# MinIO (S3) — работа из ClickHouse

- **Endpoint:** `http://minio:9000`
- **Bucket:** `clickhouse-data`, `clickhouse-export`
- **Credentials MinIO:** см. `../minio/config/minio.env`
- **Storage policies:** `s3_main` (всё в MinIO), `s3_tiered` (горячий/холодный)

---

## Запись CSV

```sql
INSERT INTO FUNCTION s3(
    'http://minio:9000/clickhouse-export/test.csv',
    'admin', '<MINIO_ROOT_PASSWORD>',
    'CSV'
)
SETTINGS s3_truncate_on_insert = 1
SELECT number AS id,
       concat('user_', toString(number)) AS name,
       now() - toIntervalDay(number) AS created_at
FROM numbers(100);
```

## Чтение CSV

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

## Запись/чтение Parquet

```sql
-- Записать
INSERT INTO FUNCTION s3(
    'http://minio:9000/clickhouse-export/test.parquet',
    'admin', '<MINIO_ROOT_PASSWORD>',
    'Parquet'
)
SETTINGS s3_truncate_on_insert = 1
SELECT number AS id,
       concat('product_', toString(number)) AS product,
       round(rand() % 10000 / 100, 2) AS price,
       toDate('2025-01-01') + number AS sale_date
FROM numbers(1000);

-- Прочитать
SELECT count(), min(price), max(price), avg(price)
FROM s3(
    'http://minio:9000/clickhouse-export/test.parquet',
    'admin', '<MINIO_ROOT_PASSWORD>',
    'Parquet'
);
```

## Таблица с движком S3

```sql
DROP TABLE IF EXISTS s3_logs;

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

## MergeTree с S3 storage policy

```sql
DROP TABLE IF EXISTS events_s3;

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

## MergeTree с tiered storage (горячий/холодный)

```sql
DROP TABLE IF EXISTS events_tiered;

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

## Glob-паттерны — чтение нескольких файлов

```sql
INSERT INTO FUNCTION s3('http://minio:9000/clickhouse-export/daily/2025-01-01.csv', 'admin', '<MINIO_ROOT_PASSWORD>', 'CSV')
SETTINGS s3_truncate_on_insert = 1
SELECT 1 AS id, 'day1' AS label;

INSERT INTO FUNCTION s3('http://minio:9000/clickhouse-export/daily/2025-01-02.csv', 'admin', '<MINIO_ROOT_PASSWORD>', 'CSV')
SETTINGS s3_truncate_on_insert = 1
SELECT 2 AS id, 'day2' AS label;

SELECT *
FROM s3(
    'http://minio:9000/clickhouse-export/daily/*.csv',
    'admin', '<MINIO_ROOT_PASSWORD>',
    'CSV',
    'id UInt64, label String'
);
```

## Проверка S3

```sql
SELECT name, type, path, free_space, total_space
FROM system.disks
WHERE type = 's3';

SELECT policy_name, volume_name, disks
FROM system.storage_policies
WHERE policy_name LIKE 's3%';
```

## Очистка

```sql
DROP TABLE IF EXISTS s3_logs;
DROP TABLE IF EXISTS events_s3;
DROP TABLE IF EXISTS events_tiered;
```
