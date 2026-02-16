# Утилиты для управления Quotes Pipeline

Три утилиты для управления pipeline котировок:

- **quote-ch** — управление ClickHouse (таблицы, MV, очистка данных)
- **quote-kafka** — управление Kafka топиками и consumer groups
- **quote-gen** — генерация и отправка котировок

---

## quote-ch

Управление ClickHouse pipeline: создание/пересоздание таблиц, MV, очистка данных.

### init — идемпотентная инициализация

Создаёт все объекты (CREATE IF NOT EXISTS). Безопасно запускать повторно.

```bash
$ ./quote-ch init --host 95.217.61.39 --port 8443 \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K' \
    --kafka-broker '95.217.61.39:9094' \
    --kafka-user admin --kafka-password 'Tn8yB3cJ6fE2wQ5K'

Quote CH
  server  : https://95.217.61.39:8443
  command : init
  kafka   : 95.217.61.39:9094
  topics  : quotes, quotes_history

  mode: CREATE IF NOT EXISTS (idempotent)

  --- Storage tables ---
  CREATE quotes ... OK
  CREATE ohlc ... OK

  --- Kafka engine tables ---
  CREATE kafka_quotes ... OK
  CREATE kafka_quotes_history ... OK


  --- Materialized views ---
  CREATE mv_kafka_quotes_to_quotes ... OK
  CREATE mv_kafka_quotes_history_to_quotes ... OK
  CREATE mv_quotes_to_ohlc ... OK

  Done. Pipeline initialized.
```

### recreate — пересоздание

#### Полное пересоздание (DROP + CREATE, все данные будут потеряны!)

```bash
$ ./quote-ch recreate --host 95.217.61.39 --port 8443 \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K' \
    --kafka-broker '95.217.61.39:9094' \
    --kafka-user admin --kafka-password 'Tn8yB3cJ6fE2wQ5K'

Quote CH
  server  : https://95.217.61.39:8443
  command : recreate
  kafka   : 95.217.61.39:9094
  topics  : quotes, quotes_history

  scope: ALL objects (full recreate)

  --- Drop ---
  DROP mv_quotes_to_ohlc ... OK
  DROP mv_kafka_quotes_to_quotes ... OK
  DROP mv_kafka_quotes_history_to_quotes ... OK
  DROP kafka_quotes ... OK
  DROP kafka_quotes_history ... OK

  DROP quotes ... OK
  DROP ohlc ... OK

  --- Create ---
  CREATE quotes ... OK
  CREATE ohlc ... OK
  CREATE kafka_quotes ... OK
  CREATE kafka_quotes_history ... OK

  CREATE mv_kafka_quotes_to_quotes ... OK
  CREATE mv_kafka_quotes_history_to_quotes ... OK
  CREATE mv_quotes_to_ohlc ... OK

  Done. Objects recreated.
```

#### Пересоздать только MV (без потери данных)

```bash
$ ./quote-ch recreate --host 95.217.61.39 --port 8443 \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K' \
    --kafka-broker '95.217.61.39:9094' \
    --kafka-user admin --kafka-password 'Tn8yB3cJ6fE2wQ5K' \
    --only-mv

Quote CH
  server  : https://95.217.61.39:8443
  command : recreate
  kafka   : 95.217.61.39:9094
  topics  : quotes, quotes_history

  scope: materialized views only

  --- Drop ---
  DROP mv_quotes_to_ohlc ... OK
  DROP mv_kafka_quotes_to_quotes ... OK
  DROP mv_kafka_quotes_history_to_quotes ... OK

  --- Create ---
  CREATE mv_kafka_quotes_to_quotes ... OK
  CREATE mv_kafka_quotes_history_to_quotes ... OK
  CREATE mv_quotes_to_ohlc ... OK

  Done. Objects recreated.
```

#### Пересоздать только storage (quotes, ohlc + OHLC MV)

Kafka MVs автоматически отключаются (DETACH) и подключаются обратно (ATTACH).

```bash
$ ./quote-ch recreate --host 95.217.61.39 --port 8443 \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K' \
    --only-clickhouse

Quote CH
  server  : https://95.217.61.39:8443
  command : recreate
  kafka   : kafka:9092
  topics  : quotes, quotes_history

  scope: storage tables (quotes, ohlc) + OHLC MV

  --- Detach Kafka MVs (temporary) ---
  DETACH mv_kafka_quotes_to_quotes ... OK
  DETACH mv_kafka_quotes_history_to_quotes ... OK

  --- Drop ---
  DROP mv_quotes_to_ohlc ... OK
  DROP quotes ... OK
  DROP ohlc ... OK

  --- Create ---
  CREATE quotes ... OK
  CREATE ohlc ... OK
  CREATE mv_quotes_to_ohlc ... OK

  --- Re-attach Kafka MVs ---
  ATTACH mv_kafka_quotes_to_quotes ... OK
  ATTACH mv_kafka_quotes_history_to_quotes ... OK

  Done. Objects recreated.
```

### clean — очистка диапазона

#### Dry-run: посмотреть что будет удалено

```bash
$ ./quote-ch clean --host 95.217.61.39 --port 8443 \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K' \
    --from "2026-02-15 20:00:00" --to "2026-02-15 21:00:00" \
    --symbol EURUSD --dry-run

Quote CH
  server  : https://95.217.61.39:8443
  command : clean

  range     : 2026-02-15 20:00:00 — 2026-02-15 21:00:00
  symbol    : EURUSD
  timeframe : ALL
  target    : quotes + ohlc

  ticks to delete : 60
  ohlc to delete  : 132

  [dry-run] No changes made.
```

#### Удалить диапазон (ticks + OHLC)

MV автоматически отключается (DETACH) и подключается обратно (ATTACH).

```bash
$ ./quote-ch clean --host 95.217.61.39 --port 8443 \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K' \
    --from "2026-02-15 20:00:00" --to "2026-02-15 21:00:00" \
    --symbol EURUSD

Quote CH
  server  : https://95.217.61.39:8443
  command : clean

  range     : 2026-02-15 20:00:00 — 2026-02-15 21:00:00
  symbol    : EURUSD
  timeframe : ALL
  target    : quotes + ohlc

  ticks to delete : 60
  ohlc to delete  : 132

  [1/4] DETACH mv_quotes_to_ohlc ... OK
  [2/4] DELETE FROM quotes WHERE ts >= '2026-02-15 20:00:00' AND ts < '2026-02-15 21:00:00' AND symbol = 'EURUSD' ... OK
  [3/4] DELETE FROM ohlc WHERE ts >= '2026-02-15 20:00:00' AND ts < '2026-02-15 21:00:00' AND symbol = 'EURUSD' ... OK
  ATTACH mv_quotes_to_ohlc ... OK

  Done. Range deleted.
```

#### Удалить только OHLC за конкретный таймфрейм

```bash
$ ./quote-ch clean --host 95.217.61.39 --port 8443 \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K' \
    --from "2026-02-15 20:00:00" --to "2026-02-15 21:00:00" \
    --symbol EURUSD --timeframe 1m --only-ohlc --dry-run

Quote CH
  server  : https://95.217.61.39:8443
  command : clean

  range     : 2026-02-15 20:00:00 — 2026-02-15 21:00:00
  symbol    : EURUSD
  timeframe : 1m
  target    : ohlc only

  ohlc to delete  : 0

  [dry-run] No changes made.
```

### drop — полная очистка

Удаляет все объекты pipeline: MV, Kafka engine tables, storage tables.

```bash
$ ./quote-ch drop --host 95.217.61.39 --port 8443 \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K'

Quote CH
  server  : https://95.217.61.39:8443
  command : drop

  --- Drop all objects ---
  DROP mv_quotes_to_ohlc ... OK
  DROP mv_kafka_quotes_to_quotes ... OK
  DROP mv_kafka_quotes_history_to_quotes ... OK
  DROP kafka_quotes ... OK
  DROP kafka_quotes_history ... OK

  DROP quotes ... OK
  DROP ohlc ... OK

  Done. All objects dropped.
```

---

## quote-kafka

Управление Kafka топиками (`quotes`, `quotes_history`) и consumer groups.

### create — создание топиков

```bash
$ ./quote-kafka create --broker 95.217.61.39:9094 --tls \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K'

Quote Kafka
  broker    : 95.217.61.39:9094
  command   : create
  topics    : quotes, quotes_history
  groups    : clickhouse_quotes, clickhouse_quotes_history
  --- Create topics (if not exist) ---
  CREATE quotes ... OK
  CREATE quotes_history ... OK

  Done.
```

Повторный запуск (топики уже существуют):

```bash
$ ./quote-kafka create --broker 95.217.61.39:9094 --tls \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K'

Quote Kafka
  broker    : 95.217.61.39:9094
  command   : create
  topics    : quotes, quotes_history
  groups    : clickhouse_quotes, clickhouse_quotes_history
  --- Create topics (if not exist) ---
  CREATE quotes ... ALREADY EXISTS (skip)
  CREATE quotes_history ... ALREADY EXISTS (skip)

  Done.
```

### recreate — пересоздание топиков

```bash
$ ./quote-kafka recreate --broker 95.217.61.39:9094 --tls \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K'

Quote Kafka
  broker    : 95.217.61.39:9094
  command   : recreate
  topics    : quotes, quotes_history
  groups    : clickhouse_quotes, clickhouse_quotes_history
  --- Delete consumer groups ---
  DELETE group clickhouse_quotes ... OK
  DELETE group clickhouse_quotes_history ... OK


  --- Delete topics ---
  DELETE quotes ... OK
  DELETE quotes_history ... OK

  Waiting for deletion to propagate ... OK

  --- Create topics ---
  CREATE quotes (partitions=3, rf=1) ... OK
  CREATE quotes_history (partitions=3, rf=1) ... OK

  Done.
```

### delete — удаление

```bash
$ ./quote-kafka delete --broker 95.217.61.39:9094 --tls \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K'

Quote Kafka
  broker    : 95.217.61.39:9094
  command   : delete
  topics    : quotes, quotes_history
  groups    : clickhouse_quotes, clickhouse_quotes_history
  --- Delete topics ---
  DELETE quotes ... OK
  DELETE quotes_history ... OK

  --- Delete consumer groups ---
  DELETE group clickhouse_quotes ... OK
  DELETE group clickhouse_quotes_history ... OK


  Done.
```

### purge — очистка сообщений

Удаляет и пересоздаёт топики (сохраняя конфигурацию). Данные в ClickHouse остаются.

```bash
$ ./quote-kafka purge --broker 95.217.61.39:9094 --tls \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K'

Quote Kafka
  broker    : 95.217.61.39:9094
  command   : purge
  topics    : quotes, quotes_history
  groups    : clickhouse_quotes, clickhouse_quotes_history
  --- Delete consumer groups ---
  DELETE group clickhouse_quotes ... OK
  DELETE group clickhouse_quotes_history ... OK


  --- Delete topics ---
  DELETE quotes ... OK
  DELETE quotes_history ... OK

  Waiting for deletion to propagate ... OK

  --- Create topics ---
  CREATE quotes (partitions=3, rf=1) ... OK
  CREATE quotes_history (partitions=3, rf=1) ... OK

  Done.
```

---

## quote-gen

Генератор котировок: интерактивная отправка, автоматическая генерация, загрузка из файла.

### Генерация исторических данных в stdout

```bash
$ ./quote-gen --gen \
    --from "2026-02-15 20:00:00" --to "2026-02-15 20:00:05" \
    --symbol EURUSD --seed 42

EURUSD 1.08487 1.08507 1771185600987
EURUSD 1.08498 1.08518 1771185601750
EURUSD 1.08452 1.08472 1771185602345
EURUSD 1.08483 1.08503 1771185603176
EURUSD 1.08472 1.08492 1771185604643
Generated 5 ticks (1 symbols × 5s, interval 1000ms)
```

### Генерация всех символов

```bash
$ ./quote-gen --gen \
    --from "2026-02-15 20:00:00" --to "2026-02-15 20:00:03" --seed 42

XAUUSD 2649.49 2650.19 1771185600987
XAGUSD 31.51 31.54 1771185600750
EURUSD 1.08454 1.08474 1771185600345
GBPUSD 1.26531 1.26551 1771185600176
USDJPY 149.988 150.008 1771185600643
AUDUSD 0.65524 0.65544 1771185600967
USDCHF 0.87986 0.88006 1771185600828
USDCAD 1.36016 1.36036 1771185600653
NZDUSD 0.61521 0.61541 1771185600601
XAUUSD 2651.26 2651.96 1771185601504
XAGUSD 31.47 31.50 1771185601934
EURUSD 1.08433 1.08453 1771185601275
GBPUSD 1.26547 1.26567 1771185601861
USDJPY 150.031 150.051 1771185601919
AUDUSD 0.65519 0.65539 1771185601679
USDCHF 0.87973 0.87993 1771185601102
USDCAD 1.36056 1.36076 1771185601610
NZDUSD 0.61478 0.61498 1771185601298
XAUUSD 2653.01 2653.71 1771185602767
XAGUSD 31.44 31.47 1771185602507
EURUSD 1.08457 1.08477 1771185602387
GBPUSD 1.26522 1.26542 1771185602160
USDJPY 150.051 150.071 1771185602512
AUDUSD 0.65552 0.65572 1771185602532
USDCHF 0.87979 0.87999 1771185602919
USDCAD 1.36024 1.36044 1771185602392
NZDUSD 0.61522 0.61542 1771185602991
Generated 27 ticks (9 symbols × 3s, interval 1000ms)
```

### Генерация и отправка в Kafka

```bash
$ ./quote-gen --gen \
    --from "2026-02-15 20:00:00" --to "2026-02-15 20:01:00" \
    --symbol EURUSD --seed 42 \
    --broker 95.217.61.39:9094 --tls \
    --password 'Tn8yB3cJ6fE2wQ5K' \
    --topic quotes_history --rate 1000

EURUSD 1.08487 1.08507 1771185600987
EURUSD 1.08498 1.08518 1771185601750
EURUSD 1.08452 1.08472 1771185602345
EURUSD 1.08483 1.08503 1771185603176
EURUSD 1.08472 1.08492 1771185604643
...
Generated 60 ticks (1 symbols × 1m0s, interval 1000ms)
Sending 60 messages to Kafka...
  done: 60 sent in 714ms (84.1 msg/s)
```

### Интерактивный режим

```bash
$ ./quote-gen --broker 95.217.61.39:9094 --tls \
    --password 'Tn8yB3cJ6fE2wQ5K' \
    --symbol EURUSD

# Enter = 1 тик, число = N тиков, q = выход
> [Enter]
  EURUSD 1.08540 1.08560  → quotes  ✓
> 5
  EURUSD 1.08523 1.08543  → quotes  ✓
  EURUSD 1.08551 1.08571  → quotes  ✓
  EURUSD 1.08498 1.08518  → quotes  ✓
  EURUSD 1.08512 1.08532  → quotes  ✓
  EURUSD 1.08534 1.08554  → quotes  ✓
> q
```

### Загрузка из файла

```bash
$ ./quote-gen --broker 95.217.61.39:9094 --tls \
    --password 'Tn8yB3cJ6fE2wQ5K' \
    --topic quotes_history \
    --file data/sample.tsv --rate 500

# Формат файла (TSV): symbol bid ask ts_ms
# EURUSD 1.08540 1.08560 1771185600000
# EURUSD 1.08523 1.08543 1771185601000
# ...

Loading data/sample.tsv ...
Sending 97200 messages to Kafka...
  done: 97200 sent in 194s (500.0 msg/s)
```

---

## Типичные сценарии

### Полная пересборка pipeline с нуля

```bash
# 1. Удалить Kafka топики и consumer groups
$ ./quote-kafka delete --broker 95.217.61.39:9094 --tls \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K'

# 2. Удалить все объекты ClickHouse
$ ./quote-ch drop --host 95.217.61.39 --port 8443 \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K'

# 3. Создать Kafka топики
$ ./quote-kafka create --broker 95.217.61.39:9094 --tls \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K'

# 4. Создать все объекты ClickHouse
$ ./quote-ch init --host 95.217.61.39 --port 8443 \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K' \
    --kafka-broker '95.217.61.39:9094' \
    --kafka-user admin --kafka-password 'Tn8yB3cJ6fE2wQ5K'

# 5. Загрузить тестовые данные (9 символов × 3 часа)
$ ./quote-gen --gen \
    --from "2026-02-15 10:00:00" --to "2026-02-15 13:00:00" \
    --broker 95.217.61.39:9094 --tls \
    --password 'Tn8yB3cJ6fE2wQ5K' \
    --topic quotes_history --rate 1000
```

### Исправление OHLC (пересоздание MV без потери данных)

```bash
$ ./quote-ch recreate --host 95.217.61.39 --port 8443 \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K' \
    --kafka-broker '95.217.61.39:9094' \
    --kafka-user admin --kafka-password 'Tn8yB3cJ6fE2wQ5K' \
    --only-mv

Quote CH
  server  : https://95.217.61.39:8443
  command : recreate
  kafka   : 95.217.61.39:9094
  topics  : quotes, quotes_history

  scope: materialized views only

  --- Drop ---
  DROP mv_quotes_to_ohlc ... OK
  DROP mv_kafka_quotes_to_quotes ... OK
  DROP mv_kafka_quotes_history_to_quotes ... OK

  --- Create ---
  CREATE mv_kafka_quotes_to_quotes ... OK
  CREATE mv_kafka_quotes_history_to_quotes ... OK
  CREATE mv_quotes_to_ohlc ... OK

  Done. Objects recreated.
```

### Очистка и перезагрузка диапазона

```bash
# 1. Посмотреть что будет удалено
$ ./quote-ch clean --host 95.217.61.39 --port 8443 \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K' \
    --from "2026-02-15 20:00:00" --to "2026-02-15 21:00:00" \
    --symbol EURUSD --dry-run

Quote CH
  server  : https://95.217.61.39:8443
  command : clean

  range     : 2026-02-15 20:00:00 — 2026-02-15 21:00:00
  symbol    : EURUSD
  timeframe : ALL
  target    : quotes + ohlc

  ticks to delete : 60
  ohlc to delete  : 132

  [dry-run] No changes made.

# 2. Удалить
$ ./quote-ch clean --host 95.217.61.39 --port 8443 \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K' \
    --from "2026-02-15 20:00:00" --to "2026-02-15 21:00:00" \
    --symbol EURUSD

  ...
  [1/4] DETACH mv_quotes_to_ohlc ... OK
  [2/4] DELETE FROM quotes WHERE ... OK
  [3/4] DELETE FROM ohlc WHERE ... OK
  ATTACH mv_quotes_to_ohlc ... OK

  Done. Range deleted.

# 3. Загрузить новые данные
$ ./quote-gen --gen \
    --from "2026-02-15 20:00:00" --to "2026-02-15 21:00:00" \
    --symbol EURUSD --seed 42 \
    --broker 95.217.61.39:9094 --tls \
    --password 'Tn8yB3cJ6fE2wQ5K' \
    --topic quotes_history --rate 1000

  ...
  Generated 3600 ticks (1 symbols × 1h0m0s, interval 1000ms)
  Sending 3600 messages to Kafka...
    done: 3600 sent in 3s (1000.0 msg/s)
```

### Очистка Kafka без потери ClickHouse данных

```bash
$ ./quote-kafka purge --broker 95.217.61.39:9094 --tls \
    --user admin --password 'Tn8yB3cJ6fE2wQ5K'

Quote Kafka
  broker    : 95.217.61.39:9094
  command   : purge
  topics    : quotes, quotes_history
  groups    : clickhouse_quotes, clickhouse_quotes_history
  --- Delete consumer groups ---
  DELETE group clickhouse_quotes ... OK
  DELETE group clickhouse_quotes_history ... OK


  --- Delete topics ---
  DELETE quotes ... OK
  DELETE quotes_history ... OK

  Waiting for deletion to propagate ... OK

  --- Create topics ---
  CREATE quotes (partitions=3, rf=1) ... OK
  CREATE quotes_history (partitions=3, rf=1) ... OK

  Done.
```
