package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// ClickHouse HTTP client
// ---------------------------------------------------------------------------

type CHClient struct {
	baseURL  string
	user     string
	password string
	client   *http.Client
}

func NewCHClient(host string, port int, user, password string, useTLS bool) *CHClient {
	scheme := "http"
	if useTLS {
		scheme = "https"
	}
	return &CHClient{
		baseURL:  fmt.Sprintf("%s://%s:%d", scheme, host, port),
		user:     user,
		password: password,
		client: &http.Client{
			Timeout: 120 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		},
	}
}

func (c *CHClient) Query(query string) (string, error) {
	params := url.Values{}
	params.Set("user", c.user)
	params.Set("password", c.password)

	resp, err := c.client.Post(
		c.baseURL+"/?"+params.Encode(),
		"text/plain",
		strings.NewReader(query),
	)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	result := strings.TrimSpace(string(body))
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("ClickHouse error: %s", result)
	}
	return result, nil
}

func (c *CHClient) Exec(label, query string) error {
	fmt.Printf("  %s ... ", label)
	if _, err := c.Query(query); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return err
	}
	fmt.Println("OK")
	return nil
}

func (c *CHClient) ExistsTable(name string) bool {
	result, err := c.Query(fmt.Sprintf(
		"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '%s'", name))
	return err == nil && result == "1"
}

func (c *CHClient) AttachWithRetry(table string) error {
	fmt.Printf("  ATTACH %s ... ", table)
	for i := 0; i < 10; i++ {
		if i > 0 {
			time.Sleep(time.Duration(i) * time.Second)
		}
		_, err := c.Query(fmt.Sprintf("ATTACH TABLE %s", table))
		if err == nil {
			fmt.Println("OK")
			return nil
		}
		if !strings.Contains(err.Error(), "still used by some query") {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			return err
		}
		fmt.Printf("retry %d... ", i+1)
	}
	fmt.Fprintln(os.Stderr, "ERROR: timed out")
	return fmt.Errorf("timed out waiting for ATTACH %s", table)
}

// EnsureDroppedView handles views that may be in detached state.
// Tries ATTACH first (in case detached), then DROP.
func (c *CHClient) EnsureDroppedView(name string) {
	// Try to attach in case it's detached (ignore errors)
	c.Query(fmt.Sprintf("ATTACH TABLE %s", name))
	c.Exec("DROP "+name, fmt.Sprintf("DROP VIEW IF EXISTS %s", name))
}

// EnsureDroppedTable handles tables that may be in detached state.
func (c *CHClient) EnsureDroppedTable(name string) {
	c.Query(fmt.Sprintf("ATTACH TABLE %s", name))
	c.Exec("DROP "+name, fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
}

// ---------------------------------------------------------------------------
// SQL helpers
// ---------------------------------------------------------------------------

func kafkaSettings(broker, topic, group, kafkaUser, kafkaPassword string) string {
	return fmt.Sprintf(`SETTINGS
    kafka_broker_list = '%s',
    kafka_topic_list = '%s',
    kafka_group_name = '%s',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10,
    kafka_security_protocol = 'SASL_PLAINTEXT',
    kafka_sasl_mechanism = 'PLAIN',
    kafka_sasl_username = '%s',
    kafka_sasl_password = '%s'`,
		broker, topic, group, kafkaUser, kafkaPassword)
}

func parseTime(s string) (time.Time, error) {
	layouts := []string{
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04",
		"2006-01-02 15:04",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse %q (expected: 2006-01-02 15:04:05)", s)
}

// ---------------------------------------------------------------------------
// init — idempotent creation (CREATE IF NOT EXISTS)
// ---------------------------------------------------------------------------

func runInit(ch *CHClient, kafkaBroker, kafkaUser, kafkaPassword, topicRT, topicHistory string) {
	fmt.Println("  mode: CREATE IF NOT EXISTS (idempotent)")
	fmt.Println()

	// Storage tables
	fmt.Println("  --- Storage tables ---")

	if err := ch.Exec("CREATE quotes", `
CREATE TABLE IF NOT EXISTS quotes (
    ts     DateTime64(3),
    symbol String,
    bid    Float64,
    ask    Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (symbol, ts)`); err != nil {
		os.Exit(1)
	}

	if err := ch.Exec("CREATE ohlc", `
CREATE TABLE IF NOT EXISTS ohlc (
    tf     LowCardinality(String),
    symbol LowCardinality(String),
    ts     DateTime64(3, 'UTC'),
    open   AggregateFunction(argMin, Float64, DateTime64(3, 'UTC')),
    high   AggregateFunction(max, Float64),
    low    AggregateFunction(min, Float64),
    close  AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
    volume AggregateFunction(count)
) ENGINE = AggregatingMergeTree()
ORDER BY (tf, symbol, ts)`); err != nil {
		os.Exit(1)
	}
	fmt.Println()

	// Kafka engine tables
	// Pattern: ATTACH (ignore error) → CREATE IF NOT EXISTS → DETACH
	// If table is already detached, CREATE IF NOT EXISTS won't see it,
	// so we ATTACH first to make it visible, then CREATE if missing.
	fmt.Println("  --- Kafka engine tables ---")

	// kafka_quotes: ATTACH → CREATE IF NOT EXISTS
	ch.Query("ATTACH TABLE kafka_quotes") // ignore error
	if err := ch.Exec("CREATE kafka_quotes", fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS kafka_quotes (
    symbol String,
    bid    Float64,
    ask    Float64
) ENGINE = Kafka()
%s`, kafkaSettings(kafkaBroker, topicRT, "clickhouse_quotes", kafkaUser, kafkaPassword))); err != nil {
		os.Exit(1)
	}

	// kafka_quotes_history: ATTACH → CREATE IF NOT EXISTS
	ch.Query("ATTACH TABLE kafka_quotes_history") // ignore error
	if err := ch.Exec("CREATE kafka_quotes_history", fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS kafka_quotes_history (
    symbol String,
    bid    Float64,
    ask    Float64,
    ts_ms  UInt64
) ENGINE = Kafka()
%s`, kafkaSettings(kafkaBroker, topicHistory, "clickhouse_quotes_history", kafkaUser, kafkaPassword))); err != nil {
		os.Exit(1)
	}

	// kafka_quotes_history_producer: ATTACH → CREATE IF NOT EXISTS (no detach, needed for writes)
	ch.Query("ATTACH TABLE kafka_quotes_history_producer") // ignore error
	if err := ch.Exec("CREATE kafka_quotes_history_producer", fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS kafka_quotes_history_producer (
    symbol String,
    bid    Float64,
    ask    Float64,
    ts_ms  UInt64
) ENGINE = Kafka()
%s`, kafkaSettings(kafkaBroker, topicHistory, "clickhouse_quotes_history-producer", kafkaUser, kafkaPassword))); err != nil {
		os.Exit(1)
	}
	fmt.Println()

	// Materialized views
	// Kafka tables must be attached for MV creation (MV references Kafka table).
	fmt.Println("  --- Materialized views ---")

	if err := ch.Exec("CREATE mv_kafka_quotes_to_quotes", `
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kafka_quotes_to_quotes TO quotes AS
SELECT
    coalesce(_timestamp_ms, now64(3)) AS ts,
    symbol, bid, ask
FROM kafka_quotes`); err != nil {
		os.Exit(1)
	}

	if err := ch.Exec("CREATE mv_kafka_quotes_history_to_quotes", `
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kafka_quotes_history_to_quotes TO quotes AS
SELECT fromUnixTimestamp64Milli(ts_ms) AS ts, symbol, bid, ask
FROM kafka_quotes_history`); err != nil {
		os.Exit(1)
	}

	if err := ch.Exec("CREATE mv_quotes_to_ohlc", `
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_quotes_to_ohlc TO ohlc AS
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
    FROM quotes
    ARRAY JOIN
        [1000, 60000, 300000, 900000, 1800000, 3600000,
         14400000, 86400000, 604800000, 31536000000] AS interval_ms,
        ['1s', '1m', '5m', '15m', '30m', '1h',
         '4h', '1d', '1w', '1y'] AS tf
    GROUP BY tf, symbol, bucket
)`); err != nil {
		os.Exit(1)
	}
	fmt.Println()

	// Detach Kafka consumer tables (pipeline created but not consuming)
	fmt.Println("  --- Detach Kafka consumers ---")
	ch.Exec("DETACH kafka_quotes", "DETACH TABLE kafka_quotes")
	ch.Exec("DETACH kafka_quotes_history", "DETACH TABLE kafka_quotes_history")

	fmt.Println()
	fmt.Println("  Done. Pipeline initialized (Kafka consumers detached).")
	fmt.Println("  To enable: ATTACH TABLE kafka_quotes; ATTACH TABLE kafka_quotes_history;")
}

// ---------------------------------------------------------------------------
// recreate — DROP + CREATE (with optional filters)
// ---------------------------------------------------------------------------

func runRecreate(ch *CHClient, kafkaBroker, kafkaUser, kafkaPassword, topicRT, topicHistory string,
	onlyKafka, onlyClickhouse, onlyMV bool) {

	if onlyKafka {
		fmt.Println("  scope: Kafka engine tables + Kafka MVs")
	} else if onlyClickhouse {
		fmt.Println("  scope: storage tables (quotes, ohlc) + OHLC MV")
	} else if onlyMV {
		fmt.Println("  scope: materialized views only")
	} else {
		fmt.Println("  scope: ALL objects (full recreate)")
	}
	fmt.Println()

	// --only-clickhouse: Kafka MVs write into quotes, so we must
	// DETACH them temporarily and re-ATTACH after recreating storage.
	if onlyClickhouse {
		fmt.Println("  --- Detach Kafka MVs (temporary) ---")
		for _, mv := range []string{"mv_kafka_quotes_to_quotes", "mv_kafka_quotes_history_to_quotes"} {
			if ch.ExistsTable(mv) {
				if err := ch.Exec("DETACH "+mv,
					fmt.Sprintf("DETACH TABLE %s", mv)); err != nil {
					os.Exit(1)
				}
			}
		}
		fmt.Println()
	}

	// --- Drop phase ---
	fmt.Println("  --- Drop ---")

	// OHLC MV: drop when touching clickhouse storage or all MVs
	if !onlyKafka {
		ch.EnsureDroppedView("mv_quotes_to_ohlc")
	}
	// Kafka MVs: drop when touching kafka objects or all MVs (not --only-clickhouse)
	if !onlyClickhouse {
		ch.EnsureDroppedView("mv_kafka_quotes_to_quotes")
		ch.EnsureDroppedView("mv_kafka_quotes_history_to_quotes")
	}
	// Kafka engine tables
	if !onlyClickhouse && !onlyMV {
		ch.EnsureDroppedTable("kafka_quotes")
		ch.EnsureDroppedTable("kafka_quotes_history")
		ch.EnsureDroppedTable("kafka_quotes_history_producer")
	}
	// Storage tables
	if !onlyKafka && !onlyMV {
		ch.EnsureDroppedTable("quotes")
		ch.EnsureDroppedTable("ohlc")
	}
	fmt.Println()

	// --- Create phase ---
	fmt.Println("  --- Create ---")

	// Storage tables
	if !onlyKafka && !onlyMV {
		if err := ch.Exec("CREATE quotes", `
CREATE TABLE quotes (
    ts     DateTime64(3),
    symbol String,
    bid    Float64,
    ask    Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (symbol, ts)`); err != nil {
			os.Exit(1)
		}

		if err := ch.Exec("CREATE ohlc", `
CREATE TABLE ohlc (
    tf     LowCardinality(String),
    symbol LowCardinality(String),
    ts     DateTime64(3, 'UTC'),
    open   AggregateFunction(argMin, Float64, DateTime64(3, 'UTC')),
    high   AggregateFunction(max, Float64),
    low    AggregateFunction(min, Float64),
    close  AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
    volume AggregateFunction(count)
) ENGINE = AggregatingMergeTree()
ORDER BY (tf, symbol, ts)`); err != nil {
			os.Exit(1)
		}
	}

	// Kafka engine tables
	if !onlyClickhouse && !onlyMV {
		if err := ch.Exec("CREATE kafka_quotes", fmt.Sprintf(`
CREATE TABLE kafka_quotes (
    symbol String,
    bid    Float64,
    ask    Float64
) ENGINE = Kafka()
%s`, kafkaSettings(kafkaBroker, topicRT, "clickhouse_quotes", kafkaUser, kafkaPassword))); err != nil {
			os.Exit(1)
		}

		if err := ch.Exec("CREATE kafka_quotes_history", fmt.Sprintf(`
CREATE TABLE kafka_quotes_history (
    symbol String,
    bid    Float64,
    ask    Float64,
    ts_ms  UInt64
) ENGINE = Kafka()
%s`, kafkaSettings(kafkaBroker, topicHistory, "clickhouse_quotes_history", kafkaUser, kafkaPassword))); err != nil {
			os.Exit(1)
		}

		if err := ch.Exec("CREATE kafka_quotes_history_producer", fmt.Sprintf(`
CREATE TABLE kafka_quotes_history_producer (
    symbol String,
    bid    Float64,
    ask    Float64,
    ts_ms  UInt64
) ENGINE = Kafka()
%s`, kafkaSettings(kafkaBroker, topicHistory, "clickhouse_quotes_history-producer", kafkaUser, kafkaPassword))); err != nil {
			os.Exit(1)
		}
	}

	// Kafka MVs: create when touching kafka objects or all MVs (not --only-clickhouse)
	if !onlyClickhouse {
		if err := ch.Exec("CREATE mv_kafka_quotes_to_quotes", `
CREATE MATERIALIZED VIEW mv_kafka_quotes_to_quotes TO quotes AS
SELECT
    coalesce(_timestamp_ms, now64(3)) AS ts,
    symbol, bid, ask
FROM kafka_quotes`); err != nil {
			os.Exit(1)
		}

		if err := ch.Exec("CREATE mv_kafka_quotes_history_to_quotes", `
CREATE MATERIALIZED VIEW mv_kafka_quotes_history_to_quotes TO quotes AS
SELECT fromUnixTimestamp64Milli(ts_ms) AS ts, symbol, bid, ask
FROM kafka_quotes_history`); err != nil {
			os.Exit(1)
		}
	}

	// OHLC MV: create when touching clickhouse storage or all MVs
	if !onlyKafka {
		if err := ch.Exec("CREATE mv_quotes_to_ohlc", `
CREATE MATERIALIZED VIEW mv_quotes_to_ohlc TO ohlc AS
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
    FROM quotes
    ARRAY JOIN
        [1000, 60000, 300000, 900000, 1800000, 3600000,
         14400000, 86400000, 604800000, 31536000000] AS interval_ms,
        ['1s', '1m', '5m', '15m', '30m', '1h',
         '4h', '1d', '1w', '1y'] AS tf
    GROUP BY tf, symbol, bucket
)`); err != nil {
			os.Exit(1)
		}
	}

	// --only-clickhouse: re-attach Kafka MVs that were detached at the start
	if onlyClickhouse {
		fmt.Println()
		fmt.Println("  --- Re-attach Kafka MVs ---")
		for _, mv := range []string{"mv_kafka_quotes_to_quotes", "mv_kafka_quotes_history_to_quotes"} {
			if err := ch.AttachWithRetry(mv); err != nil {
				fmt.Fprintf(os.Stderr, "  WARNING: %s not re-attached! Run manually: ATTACH TABLE %s\n", mv, mv)
			}
		}
	}

	// Full recreate or --only-kafka: detach Kafka consumer tables
	// Pipeline is created but not consuming until explicit ATTACH.
	if !onlyClickhouse && !onlyMV {
		fmt.Println()
		fmt.Println("  --- Detach Kafka consumers ---")
		ch.Exec("DETACH kafka_quotes", "DETACH TABLE kafka_quotes")
		ch.Exec("DETACH kafka_quotes_history", "DETACH TABLE kafka_quotes_history")
	}

	fmt.Println()
	fmt.Println("  Done. Objects recreated.")
	if !onlyClickhouse && !onlyMV {
		fmt.Println("  Kafka consumers are detached. To enable:")
		fmt.Println("    ATTACH TABLE kafka_quotes;")
		fmt.Println("    ATTACH TABLE kafka_quotes_history;")
	}
}

// ---------------------------------------------------------------------------
// clean — delete range of ticks and/or OHLC
// ---------------------------------------------------------------------------

func runClean(ch *CHClient, fromStr, toStr, symbol, timeframe string,
	onlyTicks, onlyOhlc, dryRun bool) {

	cleanTicks := !onlyOhlc
	cleanOhlc := !onlyTicks

	ticksWhere := fmt.Sprintf("ts >= '%s' AND ts < '%s'", fromStr, toStr)
	ohlcWhere := fmt.Sprintf("ts >= '%s' AND ts < '%s'", fromStr, toStr)

	if symbol != "" {
		ticksWhere += fmt.Sprintf(" AND symbol = '%s'", symbol)
		ohlcWhere += fmt.Sprintf(" AND symbol = '%s'", symbol)
	}
	if timeframe != "" {
		ohlcWhere += fmt.Sprintf(" AND tf = '%s'", timeframe)
	}

	fmt.Printf("  range     : %s — %s\n", fromStr, toStr)
	if symbol != "" {
		fmt.Printf("  symbol    : %s\n", symbol)
	} else {
		fmt.Printf("  symbol    : ALL\n")
	}
	if timeframe != "" {
		fmt.Printf("  timeframe : %s\n", timeframe)
	} else {
		fmt.Printf("  timeframe : ALL\n")
	}
	if onlyTicks {
		fmt.Printf("  target    : quotes only\n")
	} else if onlyOhlc {
		fmt.Printf("  target    : ohlc only\n")
	} else {
		fmt.Printf("  target    : quotes + ohlc\n")
	}
	fmt.Println()

	// Count rows
	ticksCount := "0"
	ohlcCount := "0"

	if cleanTicks {
		var err error
		ticksCount, err = ch.Query(fmt.Sprintf("SELECT count() FROM quotes FINAL WHERE %s", ticksWhere))
		if err != nil {
			fmt.Fprintf(os.Stderr, "error counting ticks: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("  ticks to delete : %s\n", ticksCount)
	}

	if cleanOhlc {
		var err error
		ohlcCount, err = ch.Query(fmt.Sprintf("SELECT count() FROM ohlc WHERE %s", ohlcWhere))
		if err != nil {
			fmt.Fprintf(os.Stderr, "error counting ohlc: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("  ohlc to delete  : %s\n", ohlcCount)
	}
	fmt.Println()

	if dryRun {
		fmt.Println("  [dry-run] No changes made.")
		return
	}

	if ticksCount == "0" && ohlcCount == "0" {
		fmt.Println("  Nothing to delete.")
		return
	}

	// Steps
	totalSteps := 0
	if cleanOhlc {
		totalSteps += 2 // detach + attach MV
	}
	if cleanTicks {
		totalSteps++
	}
	if cleanOhlc {
		totalSteps++ // delete ohlc
	}
	step := 0

	// Detach OHLC MV
	if cleanOhlc {
		step++
		fmt.Printf("  [%d/%d] DETACH mv_quotes_to_ohlc ... ", step, totalSteps)
		if _, err := ch.Query("DETACH TABLE IF EXISTS mv_quotes_to_ohlc"); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("OK")
	}

	// Delete ticks
	if cleanTicks {
		step++
		if ticksCount != "0" {
			fmt.Printf("  [%d/%d] DELETE FROM quotes WHERE %s ... ", step, totalSteps, ticksWhere)
			if _, err := ch.Query(fmt.Sprintf("DELETE FROM quotes WHERE %s", ticksWhere)); err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
				if cleanOhlc {
					ch.Query("ATTACH TABLE mv_quotes_to_ohlc")
				}
				os.Exit(1)
			}
			fmt.Println("OK")
		} else {
			fmt.Printf("  [%d/%d] quotes — nothing to delete\n", step, totalSteps)
		}
	}

	// Delete OHLC
	if cleanOhlc {
		step++
		if ohlcCount != "0" {
			fmt.Printf("  [%d/%d] DELETE FROM ohlc WHERE %s ... ", step, totalSteps, ohlcWhere)
			if _, err := ch.Query(fmt.Sprintf("DELETE FROM ohlc WHERE %s", ohlcWhere)); err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
				ch.Query("ATTACH TABLE mv_quotes_to_ohlc")
				os.Exit(1)
			}
			fmt.Println("OK")
		} else {
			fmt.Printf("  [%d/%d] ohlc — nothing to delete\n", step, totalSteps)
		}

		// Re-attach
		if err := ch.AttachWithRetry("mv_quotes_to_ohlc"); err != nil {
			fmt.Fprintln(os.Stderr, "  WARNING: MV not re-attached! Run manually: ATTACH TABLE mv_quotes_to_ohlc")
			os.Exit(1)
		}
	}

	fmt.Println()
	fmt.Println("  Done. Range deleted.")
}

// ---------------------------------------------------------------------------
// drop — full cleanup
// ---------------------------------------------------------------------------

func runDrop(ch *CHClient) {
	fmt.Println("  --- Drop all objects ---")

	// Views (may be in detached state)
	ch.EnsureDroppedView("mv_quotes_to_ohlc")
	ch.EnsureDroppedView("mv_kafka_quotes_to_quotes")
	ch.EnsureDroppedView("mv_kafka_quotes_history_to_quotes")

	// Kafka engine tables
	ch.EnsureDroppedTable("kafka_quotes")
	ch.EnsureDroppedTable("kafka_quotes_history")
	ch.EnsureDroppedTable("kafka_quotes_history_producer")

	// Storage tables
	ch.EnsureDroppedTable("quotes")
	ch.EnsureDroppedTable("ohlc")

	fmt.Println()
	fmt.Println("  Done. All objects dropped.")
}

// ---------------------------------------------------------------------------
// usage
// ---------------------------------------------------------------------------

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: quote-ch <command> [options]

Commands:
  init       Create pipeline objects (idempotent — IF NOT EXISTS)
  recreate   Drop and recreate objects (--only-kafka, --only-clickhouse, --only-mv)
  clean      Delete ticks/OHLC range (--from, --to, --symbol, --timeframe)
  drop       Drop all pipeline objects

Connection:
  --host           ClickHouse host (default: 127.0.0.1)
  --port           ClickHouse HTTP(S) port (default: 8443)
  --user           ClickHouse user (required)
  --password       ClickHouse password (required)
  --tls            Use HTTPS (default: true)

Kafka (for init/recreate):
  --kafka-broker   Kafka broker (default: kafka:9092)
  --kafka-user     Kafka SASL user
  --kafka-password Kafka SASL password
  --topic-rt       Realtime topic (default: quotes)
  --topic-history  History topic (default: quotes_history)

Recreate filters:
  --only-kafka       Only Kafka engine tables + Kafka MVs
  --only-clickhouse  Only storage tables + OHLC MV
  --only-mv          Only materialized views

Clean options:
  --from        Start of range (required)
  --to          End of range (required)
  --symbol      Symbol filter (default: all)
  --timeframe   OHLC timeframe filter (default: all)
  --only-ticks  Only clean quotes
  --only-ohlc   Only clean ohlc
  --dry-run     Show plan without deleting

Examples:
  # --- init ---

  # Create all objects (idempotent)
  quote-ch init --host 95.217.61.39 --port 8443 \
    --user admin --password '<CLICKHOUSE_PASSWORD>' \
    --kafka-broker '95.217.61.39:9094' \
    --kafka-user admin --kafka-password '<KAFKA_PASSWORD>'

  # --- recreate ---

  # Recreate everything (DROP + CREATE, data lost!)
  quote-ch recreate --host 95.217.61.39 --port 8443 \
    --user admin --password '<CLICKHOUSE_PASSWORD>' \
    --kafka-broker '95.217.61.39:9094' \
    --kafka-user admin --kafka-password '<KAFKA_PASSWORD>'

  # Recreate only MVs (no data loss)
  quote-ch recreate --host 95.217.61.39 --port 8443 \
    --user admin --password '<CLICKHOUSE_PASSWORD>' \
    --kafka-broker '95.217.61.39:9094' \
    --kafka-user admin --kafka-password '<KAFKA_PASSWORD>' \
    --only-mv

  # Recreate only storage (quotes, ohlc + OHLC MV)
  quote-ch recreate --host 95.217.61.39 --port 8443 \
    --user admin --password '<CLICKHOUSE_PASSWORD>' \
    --only-clickhouse

  # Recreate only Kafka engine tables + Kafka MVs
  quote-ch recreate --host 95.217.61.39 --port 8443 \
    --user admin --password '<CLICKHOUSE_PASSWORD>' \
    --kafka-broker '95.217.61.39:9094' \
    --kafka-user admin --kafka-password '<KAFKA_PASSWORD>' \
    --only-kafka

  # --- clean ---

  # Dry-run: see what will be deleted
  quote-ch clean --host 95.217.61.39 --port 8443 \
    --user admin --password '<CLICKHOUSE_PASSWORD>' \
    --from "2026-02-15 20:00:00" --to "2026-02-15 21:00:00" \
    --symbol EURUSD --dry-run

  # Delete ticks + OHLC for symbol
  quote-ch clean --host 95.217.61.39 --port 8443 \
    --user admin --password '<CLICKHOUSE_PASSWORD>' \
    --from "2026-02-15 20:00:00" --to "2026-02-15 21:00:00" \
    --symbol EURUSD

  # Delete all symbols for range
  quote-ch clean --host 95.217.61.39 --port 8443 \
    --user admin --password '<CLICKHOUSE_PASSWORD>' \
    --from "2026-02-15 20:00:00" --to "2026-02-15 21:00:00"

  # Delete only OHLC for specific timeframe
  quote-ch clean --host 95.217.61.39 --port 8443 \
    --user admin --password '<CLICKHOUSE_PASSWORD>' \
    --from "2026-02-15 20:00:00" --to "2026-02-15 21:00:00" \
    --symbol EURUSD --timeframe 1m --only-ohlc

  # Delete only ticks (keep OHLC)
  quote-ch clean --host 95.217.61.39 --port 8443 \
    --user admin --password '<CLICKHOUSE_PASSWORD>' \
    --from "2026-02-15 20:00:00" --to "2026-02-15 21:00:00" \
    --symbol EURUSD --only-ticks

  # --- drop ---

  # Drop all pipeline objects
  quote-ch drop --host 95.217.61.39 --port 8443 \
    --user admin --password '<CLICKHOUSE_PASSWORD>'
`)
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	if cmd == "-h" || cmd == "--help" || cmd == "-help" || cmd == "help" {
		usage()
		return
	}

	fs := flag.NewFlagSet("quote-ch", flag.ExitOnError)

	// Connection
	host := fs.String("host", "127.0.0.1", "ClickHouse host")
	port := fs.Int("port", 8443, "ClickHouse HTTP(S) port")
	user := fs.String("user", "", "ClickHouse user")
	password := fs.String("password", "", "ClickHouse password")
	useTLS := fs.Bool("tls", true, "Use HTTPS")

	// Kafka
	kafkaBroker := fs.String("kafka-broker", "kafka:9092", "Kafka broker")
	kafkaUser := fs.String("kafka-user", "", "Kafka SASL user")
	kafkaPassword := fs.String("kafka-password", "", "Kafka SASL password")
	topicRT := fs.String("topic-rt", "quotes", "Realtime topic name")
	topicHistory := fs.String("topic-history", "quotes_history", "History topic name")

	// Recreate filters
	onlyKafka := fs.Bool("only-kafka", false, "Only Kafka engine tables + Kafka MVs")
	onlyClickhouse := fs.Bool("only-clickhouse", false, "Only storage tables + OHLC MV")
	onlyMV := fs.Bool("only-mv", false, "Only materialized views")

	// Clean options
	from := fs.String("from", "", "Start of range")
	to := fs.String("to", "", "End of range")
	symbol := fs.String("symbol", "", "Symbol filter")
	timeframe := fs.String("timeframe", "", "OHLC timeframe filter")
	onlyTicks := fs.Bool("only-ticks", false, "Only clean quotes")
	onlyOhlc := fs.Bool("only-ohlc", false, "Only clean ohlc")
	dryRun := fs.Bool("dry-run", false, "Show plan without deleting")

	fs.Parse(os.Args[2:])

	// Validate connection
	if *user == "" || *password == "" {
		fmt.Fprintln(os.Stderr, "error: --user and --password are required")
		fmt.Fprintln(os.Stderr)
		usage()
		os.Exit(1)
	}

	scheme := "http"
	if *useTLS {
		scheme = "https"
	}

	fmt.Println("Quote CH")
	fmt.Printf("  server  : %s://%s:%d\n", scheme, *host, *port)
	fmt.Printf("  command : %s\n", cmd)

	ch := NewCHClient(*host, *port, *user, *password, *useTLS)

	// Test connection
	if _, err := ch.Query("SELECT 1"); err != nil {
		fmt.Fprintf(os.Stderr, "error: cannot connect to ClickHouse: %v\n", err)
		os.Exit(1)
	}

	switch cmd {
	case "init":
		if *kafkaUser == "" || *kafkaPassword == "" {
			fmt.Fprintln(os.Stderr, "error: --kafka-user and --kafka-password are required for init")
			os.Exit(1)
		}
		fmt.Printf("  kafka   : %s\n", *kafkaBroker)
		fmt.Printf("  topics  : %s, %s\n", *topicRT, *topicHistory)
		fmt.Println()
		runInit(ch, *kafkaBroker, *kafkaUser, *kafkaPassword, *topicRT, *topicHistory)

	case "recreate":
		// Validate mutually exclusive filters
		cnt := 0
		if *onlyKafka {
			cnt++
		}
		if *onlyClickhouse {
			cnt++
		}
		if *onlyMV {
			cnt++
		}
		if cnt > 1 {
			fmt.Fprintln(os.Stderr, "error: --only-kafka, --only-clickhouse, --only-mv are mutually exclusive")
			os.Exit(1)
		}

		needKafka := !*onlyClickhouse && !*onlyMV
		if needKafka && (*kafkaUser == "" || *kafkaPassword == "") {
			fmt.Fprintln(os.Stderr, "error: --kafka-user and --kafka-password are required (or use --only-clickhouse / --only-mv)")
			os.Exit(1)
		}
		fmt.Printf("  kafka   : %s\n", *kafkaBroker)
		fmt.Printf("  topics  : %s, %s\n", *topicRT, *topicHistory)
		fmt.Println()
		runRecreate(ch, *kafkaBroker, *kafkaUser, *kafkaPassword, *topicRT, *topicHistory,
			*onlyKafka, *onlyClickhouse, *onlyMV)

	case "clean":
		if *from == "" || *to == "" {
			fmt.Fprintln(os.Stderr, "error: --from and --to are required")
			os.Exit(1)
		}
		if *onlyTicks && *onlyOhlc {
			fmt.Fprintln(os.Stderr, "error: --only-ticks and --only-ohlc are mutually exclusive")
			os.Exit(1)
		}

		fromTime, err := parseTime(*from)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: --from: %v\n", err)
			os.Exit(1)
		}
		toTime, err := parseTime(*to)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: --to: %v\n", err)
			os.Exit(1)
		}
		if !toTime.After(fromTime) {
			fmt.Fprintln(os.Stderr, "error: --to must be after --from")
			os.Exit(1)
		}

		fromStr := fromTime.Format("2006-01-02 15:04:05")
		toStr := toTime.Format("2006-01-02 15:04:05")

		fmt.Println()
		runClean(ch, fromStr, toStr, *symbol, *timeframe, *onlyTicks, *onlyOhlc, *dryRun)

	case "drop":
		fmt.Println()
		runDrop(ch)

	default:
		fmt.Fprintf(os.Stderr, "error: unknown command %q\n\n", cmd)
		usage()
		os.Exit(1)
	}
}
