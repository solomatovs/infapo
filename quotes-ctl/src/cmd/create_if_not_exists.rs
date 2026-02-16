use crate::clickhouse::ChClient;
use crate::config::Config;
use crate::kafka_admin::KafkaAdmin;

pub async fn run(cfg: &Config) -> Result<(), String> {
    let ch = ChClient::new(cfg);
    let kafka = KafkaAdmin::new(cfg);

    // ─── 1. Kafka: create topic (if not exists) ───
    println!("=== Step 1: Kafka — create topic (if not exists) ===");
    kafka.create_topic(&cfg.topic, 1, 1).await?;
    println!("  Topic '{}': ok", cfg.topic);

    // ─── 2. ATTACH Kafka table (if detached — otherwise CREATE IF NOT EXISTS won't see it) ───
    println!("=== Step 2: ATTACH Kafka table (if detached) ===");
    match ch.exec(&format!("ATTACH TABLE {}", cfg.table_kafka)).await {
        Ok(_) => println!("  ATTACH {}: ok", cfg.table_kafka),
        Err(err) => {
            if err.contains("already exists")
                || err.contains("doesn't exist")
                || err.contains("does not exist")
            {
                println!("  ATTACH {}: skipped (expected)", cfg.table_kafka);
            } else {
                return Err(format!("ATTACH {} failed: {err}", cfg.table_kafka));
            }
        }
    }

    // ─── 3. CREATE IF NOT EXISTS (Storage → Kafka Engine → MV) ───
    println!("=== Step 3: CREATE IF NOT EXISTS ===");

    let statements = build_create_statements(cfg);
    for (i, (name, sql)) in statements.iter().enumerate() {
        ch.exec(sql)
            .await
            .map_err(|e| format!("CREATE step {} ({name}) failed: {e}", i + 1))?;
        println!("  {name}: ok");
    }

    println!("=== Done ===");
    Ok(())
}

fn build_create_statements(cfg: &Config) -> Vec<(&'static str, String)> {
    vec![
        (
            "quotes",
            format!(
                r#"CREATE TABLE IF NOT EXISTS {table_quotes} (
    ts     DateTime64(3),
    symbol String,
    bid    Float64,
    ask    Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (symbol, ts)"#,
                table_quotes = cfg.table_quotes,
            ),
        ),
        (
            "ohlc",
            format!(
                r#"CREATE TABLE IF NOT EXISTS {table_ohlc} (
    tf     LowCardinality(String),
    symbol LowCardinality(String),
    ts     DateTime64(3, 'UTC'),
    open   AggregateFunction(argMin, Float64, DateTime64(3, 'UTC')),
    high   AggregateFunction(max, Float64),
    low    AggregateFunction(min, Float64),
    close  AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
    volume AggregateFunction(count)
) ENGINE = AggregatingMergeTree()
ORDER BY (tf, symbol, ts)"#,
                table_ohlc = cfg.table_ohlc,
            ),
        ),
        (
            "kafka_quotes",
            format!(
                r#"CREATE TABLE IF NOT EXISTS {table_kafka} (
    symbol String,
    bid    Float64,
    ask    Float64,
    ts_ms  Nullable(UInt64)
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = '{kafka_broker}',
    kafka_topic_list = '{topic}',
    kafka_group_name = '{cg}',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10,
    kafka_security_protocol = '{security_protocol}',
    kafka_sasl_mechanism = 'PLAIN',
    kafka_sasl_username = '{kafka_user}',
    kafka_sasl_password = '{kafka_password}'"#,
                table_kafka = cfg.table_kafka,
                kafka_broker = cfg.ch_kafka_broker,
                topic = cfg.topic,
                cg = cfg.consumer_group,
                security_protocol = cfg.ch_kafka_security_protocol,
                kafka_user = cfg.kafka_user,
                kafka_password = cfg.kafka_password,
            ),
        ),
        (
            "mv_kafka_to_quotes",
            format!(
                r#"CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_kafka} TO {table_quotes} AS
SELECT
    coalesce(
        fromUnixTimestamp64Milli(ts_ms),
        _timestamp_ms,
        now64(3)
    ) AS ts,
    symbol, bid, ask
FROM {table_kafka}"#,
                mv_kafka = cfg.mv_kafka,
                table_quotes = cfg.table_quotes,
                table_kafka = cfg.table_kafka,
            ),
        ),
        (
            "mv_quotes_to_ohlc",
            format!(
                r#"CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_ohlc} TO {table_ohlc} AS
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
    FROM {table_quotes}
    ARRAY JOIN
        [1000, 60000, 300000, 900000, 1800000, 3600000,
         14400000, 86400000, 604800000, 31536000000] AS interval_ms,
        ['1s', '1m', '5m', '15m', '30m', '1h',
         '4h', '1d', '1w', '1y'] AS tf
    GROUP BY tf, symbol, bucket
)"#,
                mv_ohlc = cfg.mv_ohlc,
                table_ohlc = cfg.table_ohlc,
                table_quotes = cfg.table_quotes,
            ),
        ),
    ]
}
