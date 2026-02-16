# Quotes Pipeline — котировки через Kafka + OHLC

- **Kafka broker (внутри Docker):** `kafka:9092` (SASL_PLAINTEXT)
- **Kafka broker (внешний):** `95.217.61.39:9094` (SASL_SSL)
- **Топики:** `quotes` (реал-тайм), `quotes_history` (история с timestamp)
- **Управление топиками:** [/boroda/kafka-ui](/boroda/kafka-ui)

---

## Архитектура

```
                        Kafka                            ClickHouse
                   ┌──────────────┐
  RT provider ───► │ quotes       │
                   └──────┬───────┘
                          │
                          ▼
                   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
                   │ kafka_quotes │───►│ mv_kafka_quotes_to_quotes │───►│              │
                   └──────────────┘    └──────────────┘    │              │    ┌──────────────┐
                                                           │ quotes │───►│mv_quotes_to_ohlc│
                   ┌──────────────────────┐    ┌────────── │              │    └──────┬───────┘
                   │ kafka_quotes_history  │───►│quotes_   ││              │           │
                   └──────────┬───────────┘    │history_mv│└──────────────┘    ┌──────▼───────┐
                              │                └──────────┘                    │ ohlc  │
                              │                                               └──────────────┘
                   ┌──────────┴───────────┐
  History loader ► │ quotes_history       │
                   └──────────────────────┘
```

### Объекты pipeline (7 штук)

| Объект | Тип | Описание |
|--------|-----|----------|
| `kafka_quotes` | Kafka Engine | Consumer из топика `quotes` (RT) |
| `kafka_quotes_history` | Kafka Engine | Consumer из топика `quotes_history` |
| `mv_kafka_quotes_to_quotes` | Materialized View | RT тики → `quotes` (timestamp из `_timestamp_ms`) |
| `mv_kafka_quotes_history_to_quotes` | Materialized View | History тики → `quotes` (timestamp из `ts_ms`) |
| `quotes` | ReplacingMergeTree | Хранилище тиков, дедупликация по `(symbol, ts)` |
| `mv_quotes_to_ohlc` | Materialized View | Тики → OHLC свечи (10 таймфреймов) |
| `ohlc` | AggregatingMergeTree | Хранилище OHLC свечей |

### Consumer groups в Kafka

| Consumer Group | Таблица | Топик |
|---------------|---------|-------|
| `clickhouse_quotes` | `kafka_quotes` | `quotes` |
| `clickhouse_quotes_history` | `kafka_quotes_history` | `quotes_history` |

> **Offset'ы** хранятся в Kafka (`__consumer_offsets`), а не в ClickHouse.
> DETACH TABLE и DROP TABLE **не удаляют** offset'ы — потребление возобновляется с того же места.
> Только явное удаление consumer group из Kafka сбрасывает offset'ы.

---

## Формат данных

Все сообщения в JSON (`JSONEachRow`). ClickHouse Kafka engine настроен с `kafka_skip_broken_messages = 10` — невалидные сообщения пропускаются без остановки pipeline.

**Реал-тайм** (топик `quotes`) — timestamp берётся из Kafka (`_timestamp_ms`):
```json
{"symbol":"EURUSD","bid":1.1154,"ask":1.1156}
```

**История** (топик `quotes_history`) — timestamp явно в сообщении:
```json
{"symbol":"EURUSD","bid":1.1154,"ask":1.1156,"ts_ms":1771149600000}
```

> `ts_ms` — epoch milliseconds.

---

## Переменные (задать перед выполнением скриптов)

```bash
# ─── Подключения ───
CH_CONTAINER=ch01
CH_USER='<CLICKHOUSE_USER>'
CH_PASSWORD='<CLICKHOUSE_PASSWORD>'

KAFKA_CONTAINER=kafka
KAFKA_BROKER='kafka:9092'
KAFKA_USER='<KAFKA_USER>'
KAFKA_PASSWORD='<KAFKA_PASSWORD>'

# ─── Топики Kafka ───
TOPIC_RT=quotes
TOPIC_HISTORY=quotes_history

# ─── Consumer Groups ───
CG_RT=clickhouse_quotes
CG_HISTORY=clickhouse_quotes_history

# ─── Таблицы ClickHouse ───
TABLE_QUOTES=quotes
TABLE_OHLC=ohlc
TABLE_KAFKA_RT=kafka_quotes
TABLE_KAFKA_HISTORY=kafka_quotes_history

# ─── Materialized Views ───
MV_KAFKA_RT=mv_kafka_quotes_to_quotes
MV_KAFKA_HISTORY=mv_kafka_quotes_history_to_quotes
MV_OHLC=mv_quotes_to_ohlc
```

---

## 1. drop_and_create — Пересоздание инфраструктуры

Полное пересоздание pipeline с нуля. **Все данные теряются.**

Порядок строгий: DROP ClickHouse → удаление consumer groups и топиков → создание топиков → CREATE ClickHouse → DETACH Kafka.

```bash
# ─── 1. ClickHouse: DROP (MVs → Kafka Engine → Storage) ───
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
DROP VIEW IF EXISTS $MV_OHLC;
DROP VIEW IF EXISTS $MV_KAFKA_RT;
DROP VIEW IF EXISTS $MV_KAFKA_HISTORY;
DROP TABLE IF EXISTS $TABLE_KAFKA_RT;
DROP TABLE IF EXISTS $TABLE_KAFKA_HISTORY;
DROP TABLE IF EXISTS $TABLE_QUOTES;
DROP TABLE IF EXISTS $TABLE_OHLC;
CHSQL

# ─── 2. Kafka: удалить consumer groups и топики ───
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties \
  --delete --group $CG_RT 2>/dev/null || true

docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties \
  --delete --group $CG_HISTORY 2>/dev/null || true

docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties \
  --delete --topic $TOPIC_RT 2>/dev/null || true

docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties \
  --delete --topic $TOPIC_HISTORY 2>/dev/null || true

# ─── 3. Kafka: создать топики ───
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties \
  --create --topic $TOPIC_RT --partitions 1 --replication-factor 1

docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties \
  --create --topic $TOPIC_HISTORY --partitions 1 --replication-factor 1

# ─── 4. ClickHouse: CREATE (Storage → Kafka Engine → MVs) + DETACH ───
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
CREATE TABLE $TABLE_QUOTES (
    ts     DateTime64(3),
    symbol String,
    bid    Float64,
    ask    Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (symbol, ts);

CREATE TABLE $TABLE_OHLC (
    tf     LowCardinality(String),
    symbol LowCardinality(String),
    ts     DateTime64(3, 'UTC'),
    open   AggregateFunction(argMin, Float64, DateTime64(3, 'UTC')),
    high   AggregateFunction(max, Float64),
    low    AggregateFunction(min, Float64),
    close  AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
    volume AggregateFunction(count)
) ENGINE = AggregatingMergeTree()
ORDER BY (tf, symbol, ts);

CREATE TABLE $TABLE_KAFKA_RT (
    symbol String,
    bid    Float64,
    ask    Float64
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = '$KAFKA_BROKER',
    kafka_topic_list = '$TOPIC_RT',
    kafka_group_name = '$CG_RT',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10,
    kafka_security_protocol = 'SASL_PLAINTEXT',
    kafka_sasl_mechanism = 'PLAIN',
    kafka_sasl_username = '$KAFKA_USER',
    kafka_sasl_password = '$KAFKA_PASSWORD';

CREATE TABLE $TABLE_KAFKA_HISTORY (
    symbol String,
    bid    Float64,
    ask    Float64,
    ts_ms  UInt64
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = '$KAFKA_BROKER',
    kafka_topic_list = '$TOPIC_HISTORY',
    kafka_group_name = '$CG_HISTORY',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10,
    kafka_security_protocol = 'SASL_PLAINTEXT',
    kafka_sasl_mechanism = 'PLAIN',
    kafka_sasl_username = '$KAFKA_USER',
    kafka_sasl_password = '$KAFKA_PASSWORD';

CREATE MATERIALIZED VIEW $MV_KAFKA_RT TO $TABLE_QUOTES AS
SELECT
    coalesce(_timestamp_ms, now64(3)) AS ts,
    symbol, bid, ask
FROM $TABLE_KAFKA_RT;

CREATE MATERIALIZED VIEW $MV_KAFKA_HISTORY TO $TABLE_QUOTES AS
SELECT fromUnixTimestamp64Milli(ts_ms) AS ts, symbol, bid, ask
FROM $TABLE_KAFKA_HISTORY;

CREATE MATERIALIZED VIEW $MV_OHLC TO $TABLE_OHLC AS
SELECT tf, symbol, bucket AS ts, open, high, low, close, volume
FROM (
    SELECT
        tf, symbol,
        fromUnixTimestamp64Milli(
            intDiv(toUnixTimestamp64Milli(ts), interval_ms) * interval_ms
        ) AS bucket,
        argMinState(bid, ts) AS open,
        maxState(bid) AS high,
        minState(bid) AS low,
        argMaxState(bid, ts) AS close,
        countState() AS volume
    FROM $TABLE_QUOTES
    ARRAY JOIN
        [1000, 60000, 300000, 900000, 1800000, 3600000,
         14400000, 86400000, 604800000, 31536000000] AS interval_ms,
        ['1s', '1m', '5m', '15m', '30m', '1h',
         '4h', '1d', '1w', '1y'] AS tf
    GROUP BY tf, symbol, bucket
);

DETACH TABLE $TABLE_KAFKA_RT;
DETACH TABLE $TABLE_KAFKA_HISTORY;
CHSQL
```

> **Почему CREATE Kafka → CREATE MV → DETACH Kafka?**
> MV нельзя создать на detached таблицу (`UNKNOWN_TABLE`).
> После DETACH MVs остаются на месте и будут работать после ATTACH.

---

## 2. create_if_not_exists — Идемпотентная инициализация

Досоздание отсутствующих объектов без затрагивания существующих. Безопасно запускать повторно.

> **Особенность:** если Kafka таблица в detached состоянии, `CREATE IF NOT EXISTS` её не увидит и упадёт с ошибкой.
> Поэтому паттерн: создать топики → ATTACH (ignore error) → CREATE IF NOT EXISTS → DETACH.

```bash
# ─── 1. Kafka: создать топики (если не существуют) ───
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties \
  --create --if-not-exists --topic $TOPIC_RT --partitions 1 --replication-factor 1

docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties \
  --create --if-not-exists --topic $TOPIC_HISTORY --partitions 1 --replication-factor 1

# ─── 2. ClickHouse: ATTACH Kafka таблицы (если в detached — иначе CREATE IF NOT EXISTS не увидит) ───
docker exec $CH_CONTAINER clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
  --query "ATTACH TABLE $TABLE_KAFKA_RT" 2>/dev/null || true
docker exec $CH_CONTAINER clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
  --query "ATTACH TABLE $TABLE_KAFKA_HISTORY" 2>/dev/null || true

# ─── 3. ClickHouse: CREATE IF NOT EXISTS (Storage → Kafka Engine → MVs) ───
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
CREATE TABLE IF NOT EXISTS $TABLE_QUOTES (
    ts     DateTime64(3),
    symbol String,
    bid    Float64,
    ask    Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (symbol, ts);

CREATE TABLE IF NOT EXISTS $TABLE_OHLC (
    tf     LowCardinality(String),
    symbol LowCardinality(String),
    ts     DateTime64(3, 'UTC'),
    open   AggregateFunction(argMin, Float64, DateTime64(3, 'UTC')),
    high   AggregateFunction(max, Float64),
    low    AggregateFunction(min, Float64),
    close  AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
    volume AggregateFunction(count)
) ENGINE = AggregatingMergeTree()
ORDER BY (tf, symbol, ts);

CREATE TABLE IF NOT EXISTS $TABLE_KAFKA_RT (
    symbol String,
    bid    Float64,
    ask    Float64
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = '$KAFKA_BROKER',
    kafka_topic_list = '$TOPIC_RT',
    kafka_group_name = '$CG_RT',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10,
    kafka_security_protocol = 'SASL_PLAINTEXT',
    kafka_sasl_mechanism = 'PLAIN',
    kafka_sasl_username = '$KAFKA_USER',
    kafka_sasl_password = '$KAFKA_PASSWORD';

CREATE TABLE IF NOT EXISTS $TABLE_KAFKA_HISTORY (
    symbol String,
    bid    Float64,
    ask    Float64,
    ts_ms  UInt64
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = '$KAFKA_BROKER',
    kafka_topic_list = '$TOPIC_HISTORY',
    kafka_group_name = '$CG_HISTORY',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10,
    kafka_security_protocol = 'SASL_PLAINTEXT',
    kafka_sasl_mechanism = 'PLAIN',
    kafka_sasl_username = '$KAFKA_USER',
    kafka_sasl_password = '$KAFKA_PASSWORD';

CREATE MATERIALIZED VIEW IF NOT EXISTS $MV_KAFKA_RT TO $TABLE_QUOTES AS
SELECT
    coalesce(_timestamp_ms, now64(3)) AS ts,
    symbol, bid, ask
FROM $TABLE_KAFKA_RT;

CREATE MATERIALIZED VIEW IF NOT EXISTS $MV_KAFKA_HISTORY TO $TABLE_QUOTES AS
SELECT fromUnixTimestamp64Milli(ts_ms) AS ts, symbol, bid, ask
FROM $TABLE_KAFKA_HISTORY;

CREATE MATERIALIZED VIEW IF NOT EXISTS $MV_OHLC TO $TABLE_OHLC AS
SELECT tf, symbol, bucket AS ts, open, high, low, close, volume
FROM (
    SELECT
        tf, symbol,
        fromUnixTimestamp64Milli(
            intDiv(toUnixTimestamp64Milli(ts), interval_ms) * interval_ms
        ) AS bucket,
        argMinState(bid, ts) AS open,
        maxState(bid) AS high,
        minState(bid) AS low,
        argMaxState(bid, ts) AS close,
        countState() AS volume
    FROM $TABLE_QUOTES
    ARRAY JOIN
        [1000, 60000, 300000, 900000, 1800000, 3600000,
         14400000, 86400000, 604800000, 31536000000] AS interval_ms,
        ['1s', '1m', '5m', '15m', '30m', '1h',
         '4h', '1d', '1w', '1y'] AS tf
    GROUP BY tf, symbol, bucket
);

-- ═══ DETACH Kafka (pipeline создан, но не потребляет) ═══
DETACH TABLE $TABLE_KAFKA_RT;
DETACH TABLE $TABLE_KAFKA_HISTORY;
CHSQL
```

---

## 3. enable / disable — Потребление из Kafka

Управление потреблением через `DETACH TABLE` / `ATTACH TABLE` на Kafka engine таблицах.
MVs при этом остаются на месте — трогать их не нужно.

**Включить всё:**

```bash
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
ATTACH TABLE $TABLE_KAFKA_RT;
ATTACH TABLE $TABLE_KAFKA_HISTORY;
CHSQL
```

**Выключить всё:**

```bash
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
DETACH TABLE $TABLE_KAFKA_RT;
DETACH TABLE $TABLE_KAFKA_HISTORY;
CHSQL
```

**Только RT (kafka_quotes):**

```bash
# Включить
docker exec $CH_CONTAINER clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
  --query "ATTACH TABLE $TABLE_KAFKA_RT"

# Выключить
docker exec $CH_CONTAINER clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
  --query "DETACH TABLE $TABLE_KAFKA_RT"
```

**Только History (kafka_quotes_history):**

```bash
# Включить
docker exec $CH_CONTAINER clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
  --query "ATTACH TABLE $TABLE_KAFKA_HISTORY"

# Выключить
docker exec $CH_CONTAINER clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
  --query "DETACH TABLE $TABLE_KAFKA_HISTORY"
```

**Проверка состояния:**

```bash
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
SELECT name, engine
FROM system.tables
WHERE database = currentDatabase()
  AND name IN ('$TABLE_KAFKA_RT', '$TABLE_KAFKA_HISTORY',
               '$MV_KAFKA_RT', '$MV_KAFKA_HISTORY',
               '$MV_OHLC', '$TABLE_QUOTES', '$TABLE_OHLC')
ORDER BY name;
CHSQL
```

> Detached таблицы **не отображаются** в `system.tables`.
> Если `kafka_quotes` нет в списке — потребление RT остановлено.

### Offset'ы сохраняются

| Операция | Offset'ы | Данные |
|----------|----------|--------|
| `DETACH TABLE` → `ATTACH TABLE` | Сохраняются | Накопившиеся прочитаются |
| `DROP TABLE` → `CREATE TABLE` (тот же `kafka_group_name`) | Сохраняются | Накопившиеся прочитаются |
| Удаление consumer group из Kafka | **Сбрасываются** | Всё перечитается с начала (`auto.offset.reset=earliest`) |

---

## 4. reload_data — Перезагрузка истории

Процедура замены исторических данных за диапазон через Kafka.

### Предпосылки

- OHLC MV (`mv_quotes_to_ohlc`) срабатывает на **каждый** INSERT в `quotes`, включая данные из Kafka
- `AggregatingMergeTree` **мержит** агрегаты, а не заменяет — загрузка поверх существующих данных удвоит `volume`
- Поэтому перед загрузкой нужно очистить и тики, и OHLC за перезагружаемый диапазон

```bash
# ─── Параметры ───
FROM='2026-02-15 10:00:00'
TO='2026-02-15 13:00:00'
SYMBOL=EURUSD

# ─── 1. Остановить потребление ───
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
DETACH TABLE $TABLE_KAFKA_RT;
DETACH TABLE $TABLE_KAFKA_HISTORY;
CHSQL

# ─── 2. Загрузить исторические данные в Kafka ───
# Файл data.jsonl — JSONEachRow, по одному тику на строку.
# Тестовый файл: /boroda/data.jsonl
#
# Отправить в Kafka:
cat data.jsonl | docker exec -i $KAFKA_CONTAINER /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic $TOPIC_HISTORY \
  --producer.config /etc/kafka/client.properties

# ─── 3. Очистить диапазон в ClickHouse ───
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
DETACH TABLE $MV_OHLC;
DELETE FROM $TABLE_QUOTES WHERE ts >= '$FROM' AND ts < '$TO' AND symbol = '$SYMBOL';
DELETE FROM $TABLE_OHLC WHERE ts >= '$FROM' AND ts < '$TO' AND symbol = '$SYMBOL';
ATTACH TABLE $MV_OHLC;
CHSQL

# ─── 4. Включить потребление истории ───
docker exec $CH_CONTAINER clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
  --query "ATTACH TABLE $TABLE_KAFKA_HISTORY"

# ─── 5. Дождаться загрузки (LAG=0) ───
echo "Ожидание загрузки данных... (LAG должен стать 0)"
sleep 5
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties \
  --describe --group $CG_HISTORY

# ─── 6. Выключить history, включить RT ───
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
DETACH TABLE $TABLE_KAFKA_HISTORY;
ATTACH TABLE $TABLE_KAFKA_RT;
CHSQL

# ─── 7. Проверить результат ───
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
SELECT symbol, count(), min(ts), max(ts)
FROM $TABLE_QUOTES FINAL
WHERE ts >= '$FROM' AND ts < '$TO'
GROUP BY symbol ORDER BY symbol;

SELECT tf, count() AS candles
FROM $TABLE_OHLC
WHERE ts >= '$FROM' AND ts < '$TO'
GROUP BY tf ORDER BY tf;
CHSQL
```

> **Без фильтра по символу:** убрать `AND symbol = '$SYMBOL'` из DELETE — будут перезалиты все символы.

---

## Запросы к OHLC

Таблица `ohlc` использует `AggregatingMergeTree` — все SELECT **обязаны** использовать `-Merge` комбинаторы + `GROUP BY`:

```sql
-- Свечи по символу и таймфрейму
SELECT
    ts,
    argMinMerge(open)  AS open,
    maxMerge(high)     AS high,
    minMerge(low)      AS low,
    argMaxMerge(close) AS close,
    countMerge(volume) AS volume
FROM ohlc
WHERE tf = '1h' AND symbol = 'EURUSD'
GROUP BY ts
ORDER BY ts;
```

### С фильтром по времени

```sql
-- Часовые свечи за период
SELECT
    ts,
    argMinMerge(open)  AS open,
    maxMerge(high)     AS high,
    minMerge(low)      AS low,
    argMaxMerge(close) AS close,
    countMerge(volume) AS volume
FROM ohlc
WHERE tf = '1h' AND symbol = 'EURUSD'
  AND ts >= '2026-02-15 10:00:00' AND ts < '2026-02-16 00:00:00'
GROUP BY ts
ORDER BY ts;
```

### Последние N свечей

```sql
-- Последние 10 пятиминутных свечей
SELECT
    ts,
    argMinMerge(open)  AS open,
    maxMerge(high)     AS high,
    minMerge(low)      AS low,
    argMaxMerge(close) AS close,
    countMerge(volume) AS volume
FROM ohlc
WHERE tf = '5m' AND symbol = 'EURUSD'
  AND ts >= now() - INTERVAL 1 HOUR
GROUP BY ts
ORDER BY ts DESC
LIMIT 10;
```

### Все символы за период

```sql
-- Дневные свечи всех символов
SELECT
    symbol, ts,
    argMinMerge(open)  AS open,
    maxMerge(high)     AS high,
    minMerge(low)      AS low,
    argMaxMerge(close) AS close,
    countMerge(volume) AS volume
FROM ohlc
WHERE tf = '1d'
  AND ts >= '2026-02-01' AND ts < '2026-03-01'
GROUP BY symbol, ts
ORDER BY symbol, ts;
```

### Grafana (ClickHouse datasource)

```sql
SELECT
    ts AS time,
    argMinMerge(open)  AS open,
    maxMerge(high)     AS high,
    minMerge(low)      AS low,
    argMaxMerge(close) AS close,
    countMerge(volume) AS volume
FROM ohlc
WHERE tf = ${timeframe:sqlstring}
  AND symbol = ${symbol:sqlstring}
  AND $__timeFilter(ts)
GROUP BY ts
ORDER BY ts
```

> `${timeframe:sqlstring}` и `${symbol:sqlstring}` — переменные Grafana dashboard.
> `$__timeFilter(ts)` — макрос Grafana для фильтра по выбранному временному диапазону.

### Статистика

```sql
-- Количество свечей по таймфреймам
SELECT tf, count() AS candles
FROM ohlc
GROUP BY tf
ORDER BY tf;

-- Количество тиков по символам
SELECT symbol, count(), min(ts), max(ts)
FROM quotes FINAL
GROUP BY symbol
ORDER BY symbol;
```

---

## Проверочные запросы

```sql
-- Последние тики (проверка RT pipeline)
SELECT * FROM quotes FINAL
WHERE ts >= now() - INTERVAL 1 MINUTE
ORDER BY ts DESC LIMIT 5;
```

### Тест RT pipeline

```bash
# Отправить тестовый тик
docker exec $KAFKA_CONTAINER bash -c \
  'echo '"'"'{"symbol":"EURUSD","bid":1.11540,"ask":1.11560}'"'"' | /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic '"$TOPIC_RT"' \
    --producer.config /etc/kafka/client.properties'
```

```sql
-- Через 3-5 секунд проверить
SELECT * FROM quotes FINAL
WHERE ts >= now() - INTERVAL 1 MINUTE
ORDER BY ts DESC LIMIT 5;
```

---

## Полный ребилд OHLC

Пересчитать все OHLC свечи из существующих тиков (без потери тиков):

```bash
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
DETACH TABLE $MV_OHLC;
TRUNCATE TABLE $TABLE_OHLC;

INSERT INTO $TABLE_OHLC
SELECT tf, symbol,
    fromUnixTimestamp64Milli(intDiv(toUnixTimestamp64Milli(ts), interval_ms) * interval_ms) AS bucket,
    argMinState(bid, ts) AS open, maxState(bid) AS high,
    minState(bid) AS low, argMaxState(bid, ts) AS close, countState() AS volume
FROM $TABLE_QUOTES FINAL
ARRAY JOIN
    [1000, 60000, 300000, 900000, 1800000, 3600000,
     14400000, 86400000, 604800000, 31536000000] AS interval_ms,
    ['1s', '1m', '5m', '15m', '30m', '1h',
     '4h', '1d', '1w', '1y'] AS tf
GROUP BY tf, symbol, bucket;

ATTACH TABLE $MV_OHLC;
CHSQL
```

---

## Управление Kafka-топиками

```bash
# Список топиков
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/client.properties \
  --list

# Информация о топике
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/client.properties \
  --describe --topic $TOPIC_RT

# Список consumer groups
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/client.properties \
  --list

# Offset'ы consumer group
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/client.properties \
  --describe --group $CG_RT

# Отправить тестовое сообщение
docker exec $KAFKA_CONTAINER bash -c \
  'echo '"'"'{"symbol":"EURUSD","bid":1.11540,"ask":1.11560}'"'"' | /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic '"$TOPIC_RT"' \
    --producer.config /etc/kafka/client.properties'

# Прочитать сообщения из топика
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --consumer.config /etc/kafka/client.properties \
  --topic $TOPIC_RT --from-beginning --max-messages 5
```

---

## Полная очистка

```bash
# ─── ClickHouse: удалить все объекты pipeline ───
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
DROP VIEW IF EXISTS $MV_OHLC;
DROP VIEW IF EXISTS $MV_KAFKA_RT;
DROP VIEW IF EXISTS $MV_KAFKA_HISTORY;
DROP TABLE IF EXISTS $TABLE_KAFKA_RT;
DROP TABLE IF EXISTS $TABLE_KAFKA_HISTORY;
DROP TABLE IF EXISTS $TABLE_QUOTES;
DROP TABLE IF EXISTS $TABLE_OHLC;
CHSQL

# ─── Kafka: удалить топики и consumer groups ───
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties \
  --delete --topic $TOPIC_RT 2>/dev/null || true

docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties \
  --delete --topic $TOPIC_HISTORY 2>/dev/null || true

docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties \
  --delete --group $CG_RT 2>/dev/null || true

docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties \
  --delete --group $CG_HISTORY 2>/dev/null || true
```
