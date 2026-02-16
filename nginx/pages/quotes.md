# Quotes Pipeline — котировки через Kafka + OHLC

- **Kafka broker (внутри Docker):** `kafka:9092` (SASL_PLAINTEXT)
- **Kafka broker (внешний):** `95.217.61.39:9094` (SASL_SSL)
- **Топик:** `quotes`
- **Управление топиками:** [/boroda/kafka-ui](/boroda/kafka-ui)

---

## Архитектура

```
                              ═══════════════════════════════════════════════════════════
                                                       KAFKA
                              ═══════════════════════════════════════════════════════════

                                                  ┌─────────────────┐
                                      provider ►  │     quotes      │
                                                  └────────┬────────┘
                                      топик, JSON {symbol, bid, ask, ts_ms?}
                                                           │
                              ═══════════════════════════════════════════════════════════
                                                      CLICKHOUSE
                              ═══════════════════════════════════════════════════════════
                                                           │
                                                           ▼
                                                  ┌─────────────────┐
                                                  │  kafka_quotes   │
                                                  └────────┬────────┘
                                     Kafka Engine — consumer (SASL_PLAINTEXT)
                                                           │
                                                           ▼
                                            ┌──────────────────────────────┐
                                            │ mv_kafka_quotes_to_quotes    │
                                            └──────────────┬───────────────┘
                                     coalesce(ts_ms, _timestamp_ms, now64(3))
                                                           │
                                                           ▼
                                                  ┌─────────────────┐
                                                  │     quotes      │
                                                  └────────┬────────┘
                                 ReplacingMergeTree — дедупликация по (symbol, ts)
                                                           │
                                                           ▼
                                                ┌───────────────────┐
                                                │ mv_quotes_to_ohlc │
                                                └─────────┬─────────┘
                                   ARRAY JOIN 10 таймфреймов → агрегация OHLC
                                                          │
                                                          ▼
                                                  ┌─────────────┐
                                                  │    ohlc     │
                                                  └─────────────┘
                                        AggregatingMergeTree — OHLC свечи

                              ═══════════════════════════════════════════════════════════
```

### Объекты pipeline (5 штук)

| Объект | Тип | Описание |
|--------|-----|----------|
| `kafka_quotes` | Kafka Engine | Consumer из топика `quotes` |
| `mv_kafka_quotes_to_quotes` | Materialized View | Тики → `quotes` (timestamp из `ts_ms` / `_timestamp_ms` / `now`) |
| `quotes` | ReplacingMergeTree | Хранилище тиков, дедупликация по `(symbol, ts)` |
| `mv_quotes_to_ohlc` | Materialized View | Тики → OHLC свечи (10 таймфреймов) |
| `ohlc` | AggregatingMergeTree | Хранилище OHLC свечей |

### Consumer group в Kafka

| Consumer Group | Таблица | Топик |
|---------------|---------|-------|
| `clickhouse_quotes` | `kafka_quotes` | `quotes` |

> **Offset'ы** хранятся в Kafka (`__consumer_offsets`), а не в ClickHouse.
> DETACH TABLE и DROP TABLE **не удаляют** offset'ы — потребление возобновляется с того же места.
> Только явное удаление consumer group из Kafka сбрасывает offset'ы.

---

## Формат данных

Все сообщения в JSON (`JSONEachRow`). Поле `ts_ms` опционально — если не указано, timestamp берётся из Kafka metadata.

**Без timestamp** — используется `_timestamp_ms` от Kafka (момент отправки):
```json
{"symbol":"EURUSD","bid":1.1154,"ask":1.1156}
```

**С timestamp** — используется явное значение `ts_ms` (epoch milliseconds):
```json
{"symbol":"EURUSD","bid":1.1154,"ask":1.1156,"ts_ms":1771149600000}
```

> Приоритет: `ts_ms` → `_timestamp_ms` (Kafka) → `now64(3)`.
> ClickHouse Kafka engine настроен с `kafka_skip_broken_messages = 10` — невалидные сообщения пропускаются без остановки pipeline.

---

## Переменные (задать перед выполнением скриптов)

```bash
# ─── Подключения ───
CH_CONTAINER=ch01
CH_USER='<>'
CH_PASSWORD='<CLICKHOUSE_PASSWORD>'

KAFKA_CONTAINER=kafka
KAFKA_BROKER='kafka:9092'
KAFKA_USER='<KAFKA_USER>'
KAFKA_PASSWORD='<KAFKA_PASSWORD>'

# ─── Топик Kafka ───
TOPIC=quotes

# ─── Consumer Group ───
CG=clickhouse_quotes

# ─── Таблицы ClickHouse ───
TABLE_QUOTES=quotes
TABLE_OHLC=ohlc
TABLE_KAFKA=kafka_quotes

# ─── Materialized Views ───
MV_KAFKA=mv_kafka_quotes_to_quotes
MV_OHLC=mv_quotes_to_ohlc
```

---

## 1. backup_and_create — Пересоздание инфраструктуры с бекапом

Пересоздание pipeline с нуля. Таблицы с данными (`quotes`, `ohlc`) бекапятся через `EXCHANGE TABLES` — данные сохраняются в `*_bak_YYYYMMDD_HHMMSS`.

Порядок: DROP зависимостей → бекап данных → удаление Kafka → CREATE pipeline.

```bash
BAK_SUFFIX="_bak_$(date -u +%Y%m%d_%H%M%S)"

# ─── 1. ClickHouse: ATTACH detached + DROP зависимостей (MV + Kafka Engine) ───
# DROP IF EXISTS не видит detached таблицы — сначала ATTACH всё, что могло быть detached
for TBL in $TABLE_KAFKA $MV_OHLC $MV_KAFKA; do
  ERR=$(docker exec $CH_CONTAINER clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
    --query "ATTACH TABLE $TBL" 2>&1) || \
  echo "$ERR" | grep -qE "already exists|does.?n.?t exist" || \
  { echo "ATTACH $TBL failed: $ERR" >&2; false; }
done

docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
DROP VIEW IF EXISTS $MV_OHLC;
DROP VIEW IF EXISTS $MV_KAFKA;
DROP TABLE IF EXISTS $TABLE_KAFKA;
CHSQL

# ─── 2. Бекап таблиц с данными (CREATE дубликат → EXCHANGE) ───
# После EXCHANGE: оригинал пустой (готов к использованию), бекап хранит данные
for TBL in $TABLE_QUOTES $TABLE_OHLC; do
  EXISTS=$(docker exec $CH_CONTAINER clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
    --query "EXISTS TABLE $TBL")
  if [ "$EXISTS" = "1" ]; then
    docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
CREATE TABLE ${TBL}${BAK_SUFFIX} AS $TBL;
EXCHANGE TABLES $TBL AND ${TBL}${BAK_SUFFIX};
CHSQL
    echo "Бекап: $TBL → ${TBL}${BAK_SUFFIX}"
  fi
done

# ─── 3. Kafka: удалить и пересоздать топик ───
docker exec -i $KAFKA_CONTAINER bash << KAFKA
OPTS="--bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties"
/opt/kafka/bin/kafka-consumer-groups.sh \$OPTS --delete --group $CG 2>/dev/null || true
/opt/kafka/bin/kafka-topics.sh \$OPTS --delete --topic $TOPIC 2>/dev/null || true
sleep 2
/opt/kafka/bin/kafka-topics.sh \$OPTS --create --if-not-exists --topic $TOPIC --partitions 1 --replication-factor 1
KAFKA

# ─── 4. ClickHouse: CREATE IF NOT EXISTS (Storage) + CREATE (Kafka Engine → MV) ───
# Storage таблицы уже существуют (пустые после EXCHANGE), IF NOT EXISTS — для первого запуска
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

CREATE TABLE $TABLE_KAFKA (
    symbol String,
    bid    Float64,
    ask    Float64,
    ts_ms  Nullable(UInt64)
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = '$KAFKA_BROKER',
    kafka_topic_list = '$TOPIC',
    kafka_group_name = '$CG',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10,
    kafka_security_protocol = 'SASL_PLAINTEXT',
    kafka_sasl_mechanism = 'PLAIN',
    kafka_sasl_username = '$KAFKA_USER',
    kafka_sasl_password = '$KAFKA_PASSWORD';

CREATE MATERIALIZED VIEW $MV_KAFKA TO $TABLE_QUOTES AS
SELECT
    coalesce(
        fromUnixTimestamp64Milli(ts_ms),
        _timestamp_ms,
        now64(3)
    ) AS ts,
    symbol, bid, ask
FROM $TABLE_KAFKA;

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
CHSQL
```

> **EXCHANGE TABLES** — атомарная подмена: `quotes` ↔ `quotes_bak_*`. В момент обмена таблица всегда существует (в отличие от RENAME), конкурентные запросы не получат `UNKNOWN_TABLE`.
>
> **Бекапы** остаются в базе как обычные таблицы. Посмотреть: `SELECT name FROM system.tables WHERE name LIKE '%_bak_%'`. Удалить: `DROP TABLE quotes_bak_20260216_120000`.
>
> **Почему CREATE Kafka → CREATE MV, а не наоборот?**
> MV нельзя создать на detached таблицу (`UNKNOWN_TABLE`).
> После DETACH MV остаётся на месте и будет работать после ATTACH.

---

## 2. create_if_not_exists — Идемпотентная инициализация

Досоздание отсутствующих объектов без затрагивания существующих. Безопасно запускать повторно.

> **Особенность:** если Kafka таблица в detached состоянии, `CREATE IF NOT EXISTS` её не увидит и упадёт с ошибкой.
> Поэтому паттерн: создать топик → ATTACH (ignore error) → CREATE IF NOT EXISTS.

```bash
# ─── 1. Kafka: создать топик (если не существует) ───
docker exec -i $KAFKA_CONTAINER bash << KAFKA
OPTS="--bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties"
/opt/kafka/bin/kafka-topics.sh \$OPTS --create --if-not-exists --topic $TOPIC --partitions 1 --replication-factor 1
KAFKA

# ─── 2. ClickHouse: ATTACH Kafka таблица (если в detached — иначе CREATE IF NOT EXISTS не увидит) ───
ERR=$(docker exec $CH_CONTAINER clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
  --query "ATTACH TABLE $TABLE_KAFKA" 2>&1) || \
echo "$ERR" | grep -qE "already exists|does.?n.?t exist" || \
{ echo "ATTACH $TABLE_KAFKA failed: $ERR" >&2; false; }

# ─── 3. ClickHouse: CREATE IF NOT EXISTS (Storage → Kafka Engine → MV) ───
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

CREATE TABLE IF NOT EXISTS $TABLE_KAFKA (
    symbol String,
    bid    Float64,
    ask    Float64,
    ts_ms  Nullable(UInt64)
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = '$KAFKA_BROKER',
    kafka_topic_list = '$TOPIC',
    kafka_group_name = '$CG',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10,
    kafka_security_protocol = 'SASL_PLAINTEXT',
    kafka_sasl_mechanism = 'PLAIN',
    kafka_sasl_username = '$KAFKA_USER',
    kafka_sasl_password = '$KAFKA_PASSWORD';

CREATE MATERIALIZED VIEW IF NOT EXISTS $MV_KAFKA TO $TABLE_QUOTES AS
SELECT
    coalesce(
        fromUnixTimestamp64Milli(ts_ms),
        _timestamp_ms,
        now64(3)
    ) AS ts,
    symbol, bid, ask
FROM $TABLE_KAFKA;

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
CHSQL
```

---

## 3. enable / disable — Потребление из Kafka

Управление потреблением через `DETACH TABLE` / `ATTACH TABLE` на Kafka engine таблице.
MV при этом остаётся на месте — трогать его не нужно.

**Включить:**

```bash
ERR=$(docker exec $CH_CONTAINER clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
  --query "ATTACH TABLE $TABLE_KAFKA" 2>&1) || \
echo "$ERR" | grep -q "already exists" || \
{ echo "ATTACH $TABLE_KAFKA failed: $ERR" >&2; false; }
```

**Выключить:**

```bash
ERR=$(docker exec $CH_CONTAINER clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
  --query "DETACH TABLE $TABLE_KAFKA" 2>&1) || \
echo "$ERR" | grep -qE "does.?n.?t exist" || \
{ echo "DETACH $TABLE_KAFKA failed: $ERR" >&2; false; }
```

**Проверка состояния:**

```bash
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
-- Таблицы pipeline (detached не отображаются)
SELECT name, engine, total_rows, total_bytes
FROM system.tables
WHERE database = currentDatabase()
  AND name IN ('$TABLE_KAFKA', '$MV_KAFKA',
               '$MV_OHLC', '$TABLE_QUOTES', '$TABLE_OHLC')
ORDER BY name
FORMAT PrettyCompactMonoBlock;

-- Kafka consumers: offset'ы, статус, ошибки
SELECT
    table,
    assignments.topic,
    assignments.partition_id,
    assignments.current_offset,
    last_poll_time,
    num_messages_read,
    last_commit_time,
    num_commits,
    is_currently_used,
    exceptions.time,
    exceptions.text
FROM system.kafka_consumers
WHERE database = currentDatabase()
FORMAT Vertical;
CHSQL

# Kafka-side: LAG по consumer group
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties \
  --describe --group $CG 2>/dev/null || echo "Consumer group $CG не найден (ещё не было потребления)"
```

> **Detached** таблицы **не отображаются** в `system.tables` — если `kafka_quotes` нет в списке, потребление остановлено.
> **`system.kafka_consumers`** показывает только attached Kafka таблицы. `exceptions.text` содержит последние ошибки подключения к брокеру.
> **Consumer group** появляется в Kafka только после первого коммита offset'ов. Если `kafka_quotes` только что создана — группы ещё нет.

### Offset'ы сохраняются

| Операция | Offset'ы | Данные |
|----------|----------|--------|
| `DETACH TABLE` → `ATTACH TABLE` | Сохраняются | Накопившиеся прочитаются |
| `DROP TABLE` → `CREATE TABLE` (тот же `kafka_group_name`) | Сохраняются | Накопившиеся прочитаются |
| Удаление consumer group из Kafka | **Сбрасываются** | Всё перечитается с начала (`auto.offset.reset=earliest`) |

### Проверка pipeline (smoke test)

Сквозная проверка: включить потребитель → сгенерировать тики → убедиться, что данные прошли через весь pipeline (Kafka → ClickHouse → OHLC).

```bash
# ─── Параметры ───
FROM="$(date -u +%Y-%m-%d) 00:00:00"
TO="$(date -u +'%Y-%m-%d %H:%M:%S')"
SYMBOL=EURUSD

# ─── 1. Включить потребление ───
ERR=$(docker exec $CH_CONTAINER clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
  --query "ATTACH TABLE $TABLE_KAFKA" 2>&1) || \
echo "$ERR" | grep -q "already exists" || \
{ echo "ATTACH $TABLE_KAFKA failed: $ERR" >&2; false; }

# ─── 2. Сгенерировать и отправить тики в Kafka (с ts_ms) ───
FROM_SEC=$(date -ud "$FROM" +%s)
TO_SEC=$(date -ud "$TO" +%s)
TICKS=$(( (TO_SEC - FROM_SEC) / 60 ))
echo "Генерация $TICKS тиков ($SYMBOL, $FROM → $TO)..."

for i in $(seq 0 $((TICKS - 1))); do
  TS_MS=$(( (FROM_SEC + i * 60) * 1000 ))
  SHIFT=$(( RANDOM % 100 ))
  printf '{"symbol":"%s","bid":1.%05d,"ask":1.%05d,"ts_ms":%d}\n' \
    "$SYMBOL" $((11540 + SHIFT)) $((11560 + SHIFT)) "$TS_MS"
done | docker exec -i $KAFKA_CONTAINER /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic $TOPIC \
  --producer.config /etc/kafka/client.properties

# ─── 3. Дождаться загрузки ───
sleep 5

# ─── 4. Статус: Kafka consumer group ───
echo "=== Kafka consumer group ==="
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties \
  --describe --group $CG

# ─── 5. Статус: ClickHouse consumers + таблицы ───
echo "=== ClickHouse pipeline ==="
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
SELECT name, engine, total_rows, total_bytes
FROM system.tables
WHERE database = currentDatabase()
  AND name IN ('$TABLE_KAFKA', '$MV_KAFKA',
               '$MV_OHLC', '$TABLE_QUOTES', '$TABLE_OHLC')
ORDER BY name
FORMAT PrettyCompactMonoBlock;

SELECT
    table,
    assignments.topic,
    assignments.current_offset,
    num_messages_read,
    is_currently_used,
    exceptions.text
FROM system.kafka_consumers
WHERE database = currentDatabase()
FORMAT Vertical;
CHSQL

# ─── 6. Данные: quotes + ohlc ───
echo "=== Данные ==="
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
SELECT symbol, count() AS ticks, min(ts) AS first, max(ts) AS last
FROM $TABLE_QUOTES FINAL
GROUP BY symbol ORDER BY symbol;

SELECT tf, count() AS candles
FROM $TABLE_OHLC
GROUP BY tf ORDER BY tf;
CHSQL
```

---

## 4. history_reload — Перезагрузка истории

Замена исторических данных за диапазон **без остановки потребления**. RT-тики продолжают поступать по всем символам.

### Почему не нужно останавливать потребитель

| Факт | Следствие |
|------|-----------|
| `DELETE` — lightweight, помечает строки | Не блокирует INSERT |
| MV срабатывает только на `INSERT` | DELETE не дублирует агрегаты |
| RT-тики имеют текущий timestamp | Не пересекаются с историческим диапазоном |

### Предпосылки

- `AggregatingMergeTree` **мержит** агрегаты — загрузка поверх существующих данных удвоит `volume`, поэтому перед загрузкой нужно очистить и тики, и OHLC

> **Нюанс:** между DELETE и моментом, когда новые данные пройдут через pipeline, в OHLC будет кратковременный пробел за очищенный диапазон.

```bash
# ─── Параметры ───
# Перезагрузить: весь вчерашний день + первая половина сегодняшних данных
TODAY=$(date -u +%Y-%m-%d)
YESTERDAY=$(date -ud "$TODAY - 1 day" +%Y-%m-%d)
NOW_SEC=$(date -u +%s)
TODAY_START_SEC=$(date -ud "$TODAY 00:00:00" +%s)
MID_SEC=$(( (TODAY_START_SEC + NOW_SEC) / 2 ))

FROM="$YESTERDAY 00:00:00"
TO=$(date -ud @$MID_SEC +'%Y-%m-%d %H:%M:%S')
SYMBOL=EURUSD

# ─── 1. Включить потребление (если выключено) ───
ERR=$(docker exec $CH_CONTAINER clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
  --query "ATTACH TABLE $TABLE_KAFKA" 2>&1) || \
echo "$ERR" | grep -q "already exists" || \
{ echo "ATTACH $TABLE_KAFKA failed: $ERR" >&2; false; }

# ─── 2. Очистить диапазон в quotes и ohlc ───
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
DELETE FROM $TABLE_QUOTES WHERE ts >= '$FROM' AND ts < '$TO' AND symbol = '$SYMBOL';
DELETE FROM $TABLE_OHLC WHERE ts >= '$FROM' AND ts < '$TO' AND symbol = '$SYMBOL';
CHSQL

# ─── 3. Сгенерировать и загрузить тестовые данные в Kafka ───
# 1 тик в минуту, bid ~1.11540 с random walk, ask = bid + 0.00020
FROM_SEC=$(date -ud "$FROM" +%s)
TO_SEC=$(date -ud "$TO" +%s)
TICKS=$(( (TO_SEC - FROM_SEC) / 60 ))
echo "Генерация $TICKS тиков ($SYMBOL, $FROM → $TO)..."

for i in $(seq 0 $((TICKS - 1))); do
  TS_MS=$(( (FROM_SEC + i * 60) * 1000 ))
  SHIFT=$(( RANDOM % 100 ))
  printf '{"symbol":"%s","bid":1.%05d,"ask":1.%05d,"ts_ms":%d}\n' \
    "$SYMBOL" $((11540 + SHIFT)) $((11560 + SHIFT)) "$TS_MS"
done | docker exec -i $KAFKA_CONTAINER /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic $TOPIC \
  --producer.config /etc/kafka/client.properties

# ─── 4. Дождаться загрузки (LAG=0) ───
echo "Ожидание загрузки данных... (LAG должен стать 0)"
sleep 5
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties \
  --describe --group $CG

# ─── 5. Проверить результат ───
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

## 5. rebuild_ohlc — Перерасчёт OHLC из quotes

Пересчёт OHLC свечей из существующих тиков — по сути ручной запуск того же SELECT, что выполняет `mv_quotes_to_ohlc`, но с произвольными фильтрами.

### Когда использовать

- Исправили тики в `quotes` и нужно пересчитать свечи
- Повреждены/удалены данные в `ohlc`, а тики на месте
- Нужно пересчитать только один символ или таймфрейм
- Полный ребилд всех свечей с нуля

```bash
# ─── Параметры (тот же период, что в history_reload) ───
TODAY=$(date -u +%Y-%m-%d)
YESTERDAY=$(date -ud "$TODAY - 1 day" +%Y-%m-%d)
NOW_SEC=$(date -u +%s)
TODAY_START_SEC=$(date -ud "$TODAY 00:00:00" +%s)
MID_SEC=$(( (TODAY_START_SEC + NOW_SEC) / 2 ))

FROM="$YESTERDAY 00:00:00"
TO=$(date -ud @$MID_SEC +'%Y-%m-%d %H:%M:%S')
SYMBOL=EURUSD

# ─── 0. Убедиться, что Kafka-потребитель включен ───
ERR=$(docker exec $CH_CONTAINER clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
  --query "ATTACH TABLE $TABLE_KAFKA" 2>&1) || \
echo "$ERR" | grep -q "already exists" || \
{ echo "ATTACH $TABLE_KAFKA failed: $ERR" >&2; false; }

# ─── 1. Испортить OHLC (удалить 5m и 15m свечи за период) ───
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
DELETE FROM $TABLE_OHLC
WHERE ts >= '$FROM' AND ts < '$TO' AND symbol = '$SYMBOL'
  AND tf IN ('5m', '15m');

SELECT tf, count() AS candles FROM $TABLE_OHLC
WHERE ts >= '$FROM' AND ts < '$TO' AND symbol = '$SYMBOL'
GROUP BY tf ORDER BY tf;
CHSQL

echo ""
echo "^^^ 5m и 15m должны отсутствовать"
echo ""

# ─── 2. Пересчёт ───
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
DELETE FROM $TABLE_OHLC WHERE ts >= '$FROM' AND ts < '$TO' AND symbol = '$SYMBOL';

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
WHERE ts >= '$FROM' AND ts < '$TO' AND symbol = '$SYMBOL'
GROUP BY tf, symbol, bucket;
CHSQL

# ─── 3. Проверить — 5m и 15m должны вернуться ───
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
SELECT tf, count() AS candles FROM $TABLE_OHLC
WHERE ts >= '$FROM' AND ts < '$TO' AND symbol = '$SYMBOL'
GROUP BY tf ORDER BY tf;
CHSQL
```

> **Почему без DETACH MV?** DETACH `mv_quotes_to_ohlc` на время rebuild'а предотвратил бы задвоение `volume` (MV + ручной INSERT для одного бакета). Но MV не умеет «догонять» — тики, поступившие в `quotes` пока MV detached, никогда не попадут в OHLC. На практике риск задвоения минимален: RT-тики имеют текущий timestamp и не попадают в исторический диапазон rebuild'а.
>
> **Полный ребилд:** убрать WHERE из DELETE и INSERT (или заменить DELETE на `TRUNCATE TABLE $TABLE_OHLC`).

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
-- OHLC свечи (панель Candlestick)
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

-- Bid / Ask тики (панель Timeseries, два запроса)
SELECT ts AS time, 'bid' AS metric, bid AS value
FROM quotes FINAL
WHERE symbol = ${symbol:sqlstring} AND $__timeFilter(ts)
ORDER BY ts

SELECT ts AS time, 'ask' AS metric, ask AS value
FROM quotes FINAL
WHERE symbol = ${symbol:sqlstring} AND $__timeFilter(ts)
ORDER BY ts

-- Spread (панель Timeseries)
SELECT ts AS time, ask - bid AS spread
FROM quotes FINAL
WHERE symbol = ${symbol:sqlstring} AND $__timeFilter(ts)
ORDER BY ts
```

> `${timeframe:sqlstring}` и `${symbol:sqlstring}` — переменные Grafana dashboard.
> `$__timeFilter(ts)` — макрос Grafana для фильтра по выбранному временному диапазону.
> Запросы к `quotes` используют `FINAL` для дедупликации (`ReplacingMergeTree`).

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
-- Последние тики
SELECT * FROM quotes FINAL
WHERE ts >= now() - INTERVAL 1 MINUTE
ORDER BY ts DESC LIMIT 5;
```

### Тест pipeline

```bash
# Отправить тик без timestamp (ts берётся из Kafka)
docker exec $KAFKA_CONTAINER bash -c \
  'echo '"'"'{"symbol":"EURUSD","bid":1.11540,"ask":1.11560}'"'"' | /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic '"$TOPIC"' \
    --producer.config /etc/kafka/client.properties'

# Отправить тик с явным timestamp
docker exec $KAFKA_CONTAINER bash -c \
  'echo '"'"'{"symbol":"EURUSD","bid":1.11540,"ask":1.11560,"ts_ms":1771149600000}'"'"' | /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic '"$TOPIC"' \
    --producer.config /etc/kafka/client.properties'
```

```sql
-- Через 3-5 секунд проверить
SELECT * FROM quotes FINAL
WHERE ts >= now() - INTERVAL 1 MINUTE
ORDER BY ts DESC LIMIT 5;
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
  --describe --topic $TOPIC

# Список consumer groups
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/client.properties \
  --list

# Offset'ы consumer group
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/client.properties \
  --describe --group $CG

# Отправить тестовое сообщение
docker exec $KAFKA_CONTAINER bash -c \
  'echo '"'"'{"symbol":"EURUSD","bid":1.11540,"ask":1.11560}'"'"' | /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic '"$TOPIC"' \
    --producer.config /etc/kafka/client.properties'

# Прочитать сообщения из топика
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --consumer.config /etc/kafka/client.properties \
  --topic $TOPIC --from-beginning --max-messages 5
```

---

## Полная очистка

```bash
# ─── ClickHouse: удалить все объекты pipeline ───
docker exec -i $CH_CONTAINER clickhouse-client -n --user "$CH_USER" --password "$CH_PASSWORD" << CHSQL
DROP VIEW IF EXISTS $MV_OHLC;
DROP VIEW IF EXISTS $MV_KAFKA;
DROP TABLE IF EXISTS $TABLE_KAFKA;
DROP TABLE IF EXISTS $TABLE_QUOTES;
DROP TABLE IF EXISTS $TABLE_OHLC;
CHSQL

# ─── Kafka: удалить топик и consumer group ───
docker exec -i $KAFKA_CONTAINER bash << KAFKA
OPTS="--bootstrap-server localhost:9092 --command-config /etc/kafka/client.properties"
/opt/kafka/bin/kafka-topics.sh \$OPTS --delete --topic $TOPIC 2>/dev/null || true
/opt/kafka/bin/kafka-consumer-groups.sh \$OPTS --delete --group $CG 2>/dev/null || true
KAFKA
```
