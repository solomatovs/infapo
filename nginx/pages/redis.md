# Redis — работа из ClickHouse

- **Host:** `redis:6379`
- **Password:** `<REDIS_ADMIN_PASSWORD>`

> **Redis table engine** хранит данные в формате RowBinary. Данные, записанные через engine, нельзя прочитать через `redis-cli`, и наоборот. Для чтения нативных Redis данных используйте **Redis Dictionary**.

> **Redis table engine** сканирует все ключи в указанной database. Используйте **отдельный db_index** для каждой таблицы.

---

## Redis table engine — хранилище данных

```sql
-- Отдельный db_index (2), чтобы не конфликтовать с другими данными
DROP TABLE IF EXISTS redis_products;

CREATE TABLE redis_products (
    key      String,
    name     String,
    price    String,
    category String
) ENGINE = Redis('redis:6379', 2, '<REDIS_ADMIN_PASSWORD>')
PRIMARY KEY (key);

INSERT INTO redis_products VALUES
    ('1', 'Laptop', '1500', 'electronics'),
    ('2', 'Book', '20', 'books'),
    ('3', 'Phone', '800', 'electronics');

SELECT * FROM redis_products;

SELECT * FROM redis_products WHERE key IN ('1', '3');
```

## Redis table engine — key-value конфиг

```sql
-- Отдельный db_index (3)
DROP TABLE IF EXISTS redis_config;

CREATE TABLE redis_config (
    key   String,
    value String
) ENGINE = Redis('redis:6379', 3, '<REDIS_ADMIN_PASSWORD>')
PRIMARY KEY (key);

INSERT INTO redis_config VALUES
    ('app:version', '2.1.0'),
    ('app:env', 'production'),
    ('app:max_connections', '100');

SELECT * FROM redis_config;
```

## Redis Dictionary — чтение hash (hash_map)

```sql
-- Словарь для чтения существующих Redis hash ключей из db 0
DROP DICTIONARY IF EXISTS redis_product_dict;

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
    password '<REDIS_ADMIN_PASSWORD>'
))
LIFETIME(MIN 30 MAX 60)
LAYOUT(COMPLEX_KEY_HASHED());

SELECT
    dictGet('redis_product_dict', 'value', tuple('product:1', 'name'))     AS name,
    dictGet('redis_product_dict', 'value', tuple('product:1', 'price'))    AS price,
    dictGet('redis_product_dict', 'value', tuple('product:1', 'category')) AS category;
```

## Redis Dictionary — чтение строк (simple)

```sql
DROP DICTIONARY IF EXISTS redis_user_dict;

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
    password '<REDIS_ADMIN_PASSWORD>'
))
LIFETIME(MIN 30 MAX 60)
LAYOUT(COMPLEX_KEY_HASHED());

SELECT
    dictGet('redis_user_dict', 'value', tuple('user:1')) AS user1,
    dictGet('redis_user_dict', 'value', tuple('user:2')) AS user2;
```

## JOIN через Redis Dictionary

```sql
SELECT
    number + 1 AS event_id,
    (number % 3) + 1 AS product_id,
    dictGet('redis_product_dict', 'value',
        tuple(concat('product:', toString((number % 3) + 1)), 'name')) AS product_name,
    dictGet('redis_product_dict', 'value',
        tuple(concat('product:', toString((number % 3) + 1)), 'price')) AS price
FROM numbers(6);
```

## Запись метрик из ClickHouse в Redis

```sql
-- Отдельный db_index (4)
DROP TABLE IF EXISTS redis_metrics;

CREATE TABLE redis_metrics (
    key   String,
    value String
) ENGINE = Redis('redis:6379', 4, '<REDIS_ADMIN_PASSWORD>')
PRIMARY KEY (key);

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

## Очистка

```sql
DROP TABLE IF EXISTS redis_products;
DROP TABLE IF EXISTS redis_config;
DROP TABLE IF EXISTS redis_metrics;
DROP DICTIONARY IF EXISTS redis_product_dict;
DROP DICTIONARY IF EXISTS redis_user_dict;
```
