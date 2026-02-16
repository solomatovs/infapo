# ClickHouse — внешние интеграции

- **Credentials ClickHouse:** см. `config/users.xml`
- **Credentials MinIO:** см. `../minio/config/minio.env`
- **Credentials Redis:** без пароля

Подключение к ClickHouse:

```bash
docker exec -it ch01 clickhouse-client --user admin --password '<CLICKHOUSE_ADMIN_PASSWORD>'
```

---

## MinIO (S3)

ClickHouse использует MinIO как S3-совместимое хранилище для чтения/записи данных.

### Архитектура

```
ClickHouse (ch01/ch02) --> MinIO (minio:9000)
                              |
                         bucket: clickhouse-data
```

- **Endpoint:** `http://minio:9000`
- **Bucket:** `clickhouse-data`
- **Конфиг:** `config/config.d/s3.xml`

### Конфигурация

#### Storage policies (определены в `config.d/s3.xml`)

| Policy       | Описание                                        |
| ------------ | ----------------------------------------------- |
| `s3_main`    | Все данные хранятся в MinIO                     |
| `s3_tiered`  | Горячие данные на локальном диске, холодные в MinIO (move_factor=0.2) |

#### S3 disk

```xml
<s3_minio>
    <type>s3</type>
    <endpoint>http://minio:9000/clickhouse-data/</endpoint>
    <access_key_id>admin</access_key_id>
    <secret_access_key>...</secret_access_key>
</s3_minio>
```

### Первоначальная настройка

#### 1. Создать bucket в MinIO

```bash
docker exec minio mc alias set local http://localhost:9000 admin <MINIO_ROOT_PASSWORD>
docker exec minio mc mb local/clickhouse-data
docker exec minio mc mb local/clickhouse-export
```

#### 2. Перезапустить ClickHouse

```bash
cd /app/docker/compose/clickhouse
docker compose up -d ch01 ch02
```

#### 3. Проверить подключение

```bash
docker exec ch01 clickhouse-client --user admin --password '<CLICKHOUSE_ADMIN_PASSWORD>' \
  --query "SELECT * FROM system.disks WHERE name = 's3_minio' FORMAT Pretty"
```

### Тестовые запросы

#### 1. Запись CSV в MinIO через s3() функцию

```sql
INSERT INTO FUNCTION s3(
    'http://minio:9000/clickhouse-export/test.csv',
    'admin', '<MINIO_ROOT_PASSWORD>',
    'CSV'
)
SELECT number AS id,
       concat('user_', toString(number)) AS name,
       now() - toIntervalDay(number) AS created_at
FROM numbers(100);
```

#### 2. Чтение CSV из MinIO

```sql
SELECT *
FROM s3(
    'http://minio:9000/clickhouse-export/test.csv',
    'admin', '<MINIO_ROOT_PASSWORD>',
    'CSV',
    'id UInt64, name String, created_at DateTime'
)
LIMIT 10;
```

#### 3. Запись/чтение Parquet

```sql
-- Записать в Parquet
INSERT INTO FUNCTION s3(
    'http://minio:9000/clickhouse-export/test.parquet',
    'admin', '<MINIO_ROOT_PASSWORD>',
    'Parquet'
)
SELECT number AS id,
       concat('product_', toString(number)) AS product,
       round(rand() % 10000 / 100, 2) AS price,
       toDate('2025-01-01') + number AS sale_date
FROM numbers(1000);

-- Прочитать Parquet
SELECT count(), min(price), max(price), avg(price)
FROM s3(
    'http://minio:9000/clickhouse-export/test.parquet',
    'admin', '<MINIO_ROOT_PASSWORD>',
    'Parquet'
);
```

#### 4. Таблица с движком S3

```sql
CREATE TABLE s3_logs (
    timestamp DateTime,
    level     String,
    message   String
) ENGINE = S3(
    'http://minio:9000/clickhouse-export/logs/data.csv',
    'admin', '<MINIO_ROOT_PASSWORD>',
    'CSV'
);

INSERT INTO s3_logs VALUES
    (now(), 'INFO', 'Application started'),
    (now(), 'WARN', 'High memory usage'),
    (now(), 'ERROR', 'Connection timeout');

SELECT * FROM s3_logs;
```

#### 5. MergeTree таблица с S3 storage policy

```sql
CREATE TABLE events_s3 (
    event_date Date,
    event_type String,
    user_id    UInt64,
    payload    String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_type, user_id)
SETTINGS storage_policy = 's3_main';

INSERT INTO events_s3
SELECT toDate('2025-01-01') + (number % 365) AS event_date,
       ['click', 'view', 'purchase', 'signup'][1 + number % 4] AS event_type,
       number % 10000 AS user_id,
       concat('{"action":"', toString(number), '"}') AS payload
FROM numbers(100000);

-- Проверить что данные в MinIO
SELECT disk_name, partition, sum(rows), formatReadableSize(sum(bytes_on_disk))
FROM system.parts
WHERE table = 'events_s3' AND active
GROUP BY disk_name, partition
ORDER BY partition;
```

#### 6. MergeTree с tiered storage (горячий/холодный)

```sql
CREATE TABLE events_tiered (
    event_date Date,
    event_type String,
    user_id    UInt64,
    payload    String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_type, user_id)
TTL event_date + INTERVAL 30 DAY TO VOLUME 'cold'
SETTINGS storage_policy = 's3_tiered';

INSERT INTO events_tiered
SELECT toDate('2024-06-01') + (number % 365) AS event_date,
       ['click', 'view', 'purchase'][1 + number % 3] AS event_type,
       number % 5000 AS user_id,
       '{}' AS payload
FROM numbers(50000);

-- Проверить распределение по дискам
SELECT disk_name, partition, sum(rows), formatReadableSize(sum(bytes_on_disk))
FROM system.parts
WHERE table = 'events_tiered' AND active
GROUP BY disk_name, partition
ORDER BY partition;
```

#### 7. Glob-паттерны — чтение нескольких файлов

```sql
INSERT INTO FUNCTION s3('http://minio:9000/clickhouse-export/daily/2025-01-01.csv', 'admin', '<MINIO_ROOT_PASSWORD>', 'CSV')
SELECT 1 AS id, 'day1' AS label;

INSERT INTO FUNCTION s3('http://minio:9000/clickhouse-export/daily/2025-01-02.csv', 'admin', '<MINIO_ROOT_PASSWORD>', 'CSV')
SELECT 2 AS id, 'day2' AS label;

SELECT *
FROM s3(
    'http://minio:9000/clickhouse-export/daily/*.csv',
    'admin', '<MINIO_ROOT_PASSWORD>',
    'CSV',
    'id UInt64, label String'
);
```

### Проверка S3

```sql
SELECT name, type, path, free_space, total_space
FROM system.disks
WHERE type = 's3';

SELECT policy_name, volume_name, disks
FROM system.storage_policies
WHERE policy_name LIKE 's3%';
```

### Очистка тестовых данных (S3)

```sql
DROP TABLE IF EXISTS s3_logs;
DROP TABLE IF EXISTS events_s3;
DROP TABLE IF EXISTS events_tiered;
```

```bash
docker exec minio mc rm --recursive --force local/clickhouse-export/
```

---

## Redis

ClickHouse может читать и писать данные в Redis через table engine и dictionaries.

### Архитектура

```
ClickHouse (ch01/ch02) --> Redis (redis:6379)
```

- **Host:** `redis`
- **Port:** `6379`
- **Password:** нет
- **Конфиг:** дополнительная настройка не требуется

### Способы интеграции

| Способ | Назначение | Формат хранения |
| --- | --- | --- |
| `Redis` table engine | Постоянная таблица, данные хранятся в Redis | **RowBinary** (внутренний формат ClickHouse) |
| `CREATE DICTIONARY` с Redis source | Чтение существующих данных из Redis для JOIN | Нативные Redis strings и hashes |

> **Важно:** Redis table engine хранит данные в бинарном формате ClickHouse (RowBinary), а **не** как обычные Redis строки или хеши. Данные, записанные через Redis engine, нельзя прочитать обычным `redis-cli`, и наоборот — существующие Redis данные нельзя прочитать через Redis engine. Для чтения существующих Redis данных используйте **Redis Dictionary**.

> **Важно:** Redis table engine сканирует **все** ключи в указанной Redis database (db_index). Используйте **отдельный db_index** для каждой таблицы, чтобы избежать конфликтов с другими данными.

### Параметры Redis engine

```
ENGINE = Redis('host:port'[, db_index[, password[, pool_size]]])
```

| Параметр | Описание | По умолчанию |
| --- | --- | --- |
| `host:port` | Адрес Redis сервера | (обязательный) |
| `db_index` | Номер Redis database (0-15) | `0` |
| `password` | Пароль Redis (пустая строка если без пароля) | `''` |
| `pool_size` | Размер пула соединений | `16` |

### Тестовые запросы

#### 1. Подготовка — записать данные в Redis

```bash
docker exec redis redis-cli HSET product:1 name "Laptop" price "1500" category "electronics"
docker exec redis redis-cli HSET product:2 name "Book" price "20" category "books"
docker exec redis redis-cli HSET product:3 name "Phone" price "800" category "electronics"
docker exec redis redis-cli SET user:1 "Alice"
docker exec redis redis-cli SET user:2 "Bob"
docker exec redis redis-cli SET user:3 "Charlie"
```

#### 2. Redis table engine — хранилище данных

```sql
-- Redis engine использует отдельный db_index (2), чтобы не конфликтовать с другими данными
CREATE TABLE redis_products (
    key      String,
    name     String,
    price    String,
    category String
) ENGINE = Redis('redis:6379', 2)
PRIMARY KEY (key);

-- Вставить данные (хранятся в RowBinary формате в Redis db 2)
INSERT INTO redis_products VALUES
    ('1', 'Laptop', '1500', 'electronics'),
    ('2', 'Book', '20', 'books'),
    ('3', 'Phone', '800', 'electronics');

-- Прочитать все данные
SELECT * FROM redis_products;

-- Вставить новые данные
INSERT INTO redis_products VALUES
    ('4', 'Tablet', '600', 'electronics'),
    ('5', 'Pen', '2', 'stationery');

-- Проверить что данные появились
SELECT * FROM redis_products WHERE key IN ('4', '5');
```

#### 3. Redis table engine — key-value конфиг

```sql
-- Таблица для key-value хранения (отдельный db_index 3)
CREATE TABLE redis_config (
    key   String,
    value String
) ENGINE = Redis('redis:6379', 3)
PRIMARY KEY (key);

-- Вставить конфигурационные значения
INSERT INTO redis_config VALUES
    ('app:version', '2.1.0'),
    ('app:env', 'production'),
    ('app:max_connections', '100');

-- Прочитать
SELECT * FROM redis_config;
```

#### 4. Redis Dictionary — чтение Redis hash (hash_map)

```sql
-- Словарь для чтения существующих Redis hash ключей
-- hash_map требует 2 ключа: Redis key (полный, с префиксом) + имя поля
-- Одна value-колонка: значение поля
CREATE DICTIONARY redis_product_dict (
    key    String,
    field  String,
    value  String
) PRIMARY KEY key, field
SOURCE(REDIS(
    host 'redis'
    port 6379
    db_index 0
    storage_type 'hash_map'
    key_prefix 'product:'
))
LIFETIME(MIN 30 MAX 60)
LAYOUT(COMPLEX_KEY_HASHED());

-- Прочитать поля хеша product:1
-- Ключ в dictGet нужно указывать полностью (с префиксом)
SELECT
    dictGet('redis_product_dict', 'value', tuple('product:1', 'name'))     AS name,
    dictGet('redis_product_dict', 'value', tuple('product:1', 'price'))    AS price,
    dictGet('redis_product_dict', 'value', tuple('product:1', 'category')) AS category;
```

#### 5. Redis Dictionary — чтение Redis строк (simple)

```sql
-- Словарь для чтения существующих Redis string ключей
CREATE DICTIONARY redis_user_dict (
    key   String,
    value String
) PRIMARY KEY key
SOURCE(REDIS(
    host 'redis'
    port 6379
    db_index 0
    storage_type 'simple'
    key_prefix 'user:'
))
LIFETIME(MIN 30 MAX 60)
LAYOUT(COMPLEX_KEY_HASHED());

-- Прочитать строковые ключи (ключ указывается полностью, с префиксом)
SELECT
    dictGet('redis_user_dict', 'value', tuple('user:1')) AS user1,
    dictGet('redis_user_dict', 'value', tuple('user:2')) AS user2;
```

#### 6. JOIN через Redis Dictionary

```sql
-- Использовать словарь для обогащения данных из ClickHouse
SELECT
    number + 1 AS event_id,
    (number % 3) + 1 AS product_id,
    dictGet('redis_product_dict', 'value',
        tuple(concat('product:', toString((number % 3) + 1)), 'name')) AS product_name,
    dictGet('redis_product_dict', 'value',
        tuple(concat('product:', toString((number % 3) + 1)), 'price')) AS price
FROM numbers(6);
```

#### 7. Запись метрик из ClickHouse в Redis

```sql
-- Таблица для записи метрик (отдельный db_index 4)
CREATE TABLE redis_metrics (
    key   String,
    value String
) ENGINE = Redis('redis:6379', 4)
PRIMARY KEY (key);

-- Записать агрегированные метрики
INSERT INTO redis_metrics
SELECT
    concat('metric:', name) AS key,
    toString(value) AS value
FROM (
    SELECT 'total_events' AS name, count() AS value FROM system.events
    UNION ALL
    SELECT 'uptime_seconds', uptime()
    UNION ALL
    SELECT 'current_databases', count() FROM system.databases
);

SELECT * FROM redis_metrics;
```

### Проверка Redis

```bash
# Проверить данные в Redis напрямую (db 0 — нативные данные)
docker exec redis redis-cli -n 0 KEYS '*'
docker exec redis redis-cli -n 0 HGETALL product:1
docker exec redis redis-cli -n 0 GET user:1
```

### Очистка тестовых данных (Redis)

```sql
DROP TABLE IF EXISTS redis_products;
DROP TABLE IF EXISTS redis_config;
DROP TABLE IF EXISTS redis_metrics;
DROP DICTIONARY IF EXISTS redis_product_dict;
DROP DICTIONARY IF EXISTS redis_user_dict;
```

```bash
docker exec redis redis-cli DEL product:1 product:2 product:3
docker exec redis redis-cli DEL user:1 user:2 user:3
```
