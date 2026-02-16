# Kafka — работа из ClickHouse

- **Broker:** `kafka:9092`
- **Авторизация:** SASL/PLAIN (`<KAFKA_ADMIN_USER>` / `<KAFKA_ADMIN_PASSWORD>`)
- **Управление топиками:** [/boroda/kafka-ui](/boroda/kafka-ui)
- **Автосоздание топиков:** включено (`num.partitions=3`)

> `SELECT` из Kafka-таблицы потребляет сообщения. Для постоянного хранения используйте Materialized View.

---

## Kafka + MV + MergeTree

```sql
DROP VIEW IF EXISTS events_mv;
DROP TABLE IF EXISTS kafka_events;
DROP TABLE IF EXISTS events;

CREATE TABLE kafka_events (
    user_id  UInt64,
    event    String,
    ts       DateTime
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'test-events',
    kafka_group_name = 'clickhouse-events',
    kafka_format = 'JSONEachRow',
    kafka_security_protocol = 'SASL_PLAINTEXT',
    kafka_sasl_mechanism = 'PLAIN',
    kafka_sasl_username = '<KAFKA_ADMIN_USER>',
    kafka_sasl_password = '<KAFKA_ADMIN_PASSWORD>';

CREATE TABLE events (
    user_id  UInt64,
    event    String,
    ts       DateTime
) ENGINE = MergeTree()
ORDER BY (ts, user_id);

CREATE MATERIALIZED VIEW events_mv TO events AS
SELECT * FROM kafka_events;

SELECT * FROM events;
```

## Фильтрация

```sql
DROP VIEW IF EXISTS purchases_mv;

CREATE MATERIALIZED VIEW purchases_mv TO events AS
SELECT * FROM kafka_events
WHERE event = 'purchase';
```

## Агрегация

```sql
DROP VIEW IF EXISTS events_hourly_mv;
DROP TABLE IF EXISTS events_hourly;

CREATE TABLE events_hourly (
    hour     DateTime,
    event    String,
    count    UInt64
) ENGINE = SummingMergeTree()
ORDER BY (hour, event);

CREATE MATERIALIZED VIEW events_hourly_mv TO events_hourly AS
SELECT
    toStartOfHour(ts) AS hour,
    event,
    count() AS count
FROM kafka_events
GROUP BY hour, event;

SELECT * FROM events_hourly;
```

## Несколько consumer-потоков

```sql
DROP TABLE IF EXISTS kafka_events_parallel;

CREATE TABLE kafka_events_parallel (
    user_id  UInt64,
    event    String,
    ts       DateTime
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'test-events',
    kafka_group_name = 'clickhouse-parallel',
    kafka_format = 'JSONEachRow',
    kafka_security_protocol = 'SASL_PLAINTEXT',
    kafka_sasl_mechanism = 'PLAIN',
    kafka_sasl_username = '<KAFKA_ADMIN_USER>',
    kafka_sasl_password = '<KAFKA_ADMIN_PASSWORD>',
    kafka_num_consumers = 3;
```

## Пропуск невалидных сообщений

```sql
DROP TABLE IF EXISTS kafka_events_safe;

CREATE TABLE kafka_events_safe (
    user_id  UInt64,
    event    String,
    ts       DateTime
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'test-events',
    kafka_group_name = 'clickhouse-safe',
    kafka_format = 'JSONEachRow',
    kafka_security_protocol = 'SASL_PLAINTEXT',
    kafka_sasl_mechanism = 'PLAIN',
    kafka_sasl_username = '<KAFKA_ADMIN_USER>',
    kafka_sasl_password = '<KAFKA_ADMIN_PASSWORD>',
    kafka_skip_broken_messages = 10;
```

## CSV формат

```sql
DROP VIEW IF EXISTS csv_mv;
DROP TABLE IF EXISTS kafka_csv;
DROP TABLE IF EXISTS csv_data;

CREATE TABLE kafka_csv (
    id     UInt64,
    name   String,
    amount Float64
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'test-csv',
    kafka_group_name = 'clickhouse-csv',
    kafka_format = 'CSV',
    kafka_security_protocol = 'SASL_PLAINTEXT',
    kafka_sasl_mechanism = 'PLAIN',
    kafka_sasl_username = '<KAFKA_ADMIN_USER>',
    kafka_sasl_password = '<KAFKA_ADMIN_PASSWORD>';

CREATE TABLE csv_data (
    id     UInt64,
    name   String,
    amount Float64
) ENGINE = MergeTree()
ORDER BY id;

CREATE MATERIALIZED VIEW csv_mv TO csv_data AS
SELECT * FROM kafka_csv;

SELECT * FROM csv_data;
```

## Виртуальные колонки

```sql
DROP VIEW IF EXISTS events_with_meta_mv;
DROP TABLE IF EXISTS events_with_meta;

CREATE TABLE events_with_meta (
    user_id         UInt64,
    event           String,
    ts              DateTime,
    kafka_topic     String,
    kafka_partition UInt64,
    kafka_offset    UInt64,
    kafka_timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (ts, user_id);

CREATE MATERIALIZED VIEW events_with_meta_mv TO events_with_meta AS
SELECT
    user_id,
    event,
    ts,
    _topic          AS kafka_topic,
    _partition      AS kafka_partition,
    _offset         AS kafka_offset,
    _timestamp      AS kafka_timestamp
FROM kafka_events;
```

| Колонка | Тип | Описание |
| --- | --- | --- |
| `_topic` | String | Имя топика |
| `_partition` | UInt64 | Номер партиции |
| `_offset` | UInt64 | Offset сообщения |
| `_timestamp` | DateTime | Timestamp из Kafka |
| `_key` | String | Ключ сообщения |

## Очистка

```sql
DROP VIEW IF EXISTS events_mv;
DROP VIEW IF EXISTS events_with_meta_mv;
DROP VIEW IF EXISTS purchases_mv;
DROP VIEW IF EXISTS events_hourly_mv;
DROP VIEW IF EXISTS csv_mv;
DROP TABLE IF EXISTS kafka_events;
DROP TABLE IF EXISTS kafka_events_parallel;
DROP TABLE IF EXISTS kafka_events_safe;
DROP TABLE IF EXISTS kafka_csv;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS events_with_meta;
DROP TABLE IF EXISTS events_hourly;
DROP TABLE IF EXISTS csv_data;
```
