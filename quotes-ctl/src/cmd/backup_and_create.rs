use crate::clickhouse::ChClient;
use crate::config::Config;
use crate::kafka_admin::KafkaAdmin;

pub async fn run(cfg: &Config) -> Result<(), String> {
    let ch = ChClient::new(cfg);
    let kafka = KafkaAdmin::new(cfg);
    let bak_suffix = format!("_bak_{}", utc_now_suffix());

    // ─── 1. ATTACH detached + DROP dependencies (MV + Kafka Engine) ───
    println!("=== Step 1: ATTACH detached tables, DROP dependencies ===");

    for tbl in [&cfg.table_kafka, &cfg.mv_ohlc, &cfg.mv_kafka] {
        let sql = format!("ATTACH TABLE {tbl}");
        match ch.exec(&sql).await {
            Ok(_) => println!("  ATTACH {tbl}: ok"),
            Err(err) => {
                if err.contains("already exists")
                    || err.contains("doesn't exist")
                    || err.contains("does not exist")
                {
                    println!("  ATTACH {tbl}: skipped (expected)");
                } else {
                    return Err(format!("ATTACH {tbl} failed: {err}"));
                }
            }
        }
    }

    // ClickHouse HTTP API executes one statement per request — send them one by one
    for sql in [
        format!("DROP VIEW IF EXISTS {}", cfg.mv_ohlc),
        format!("DROP VIEW IF EXISTS {}", cfg.mv_kafka),
        format!("DROP TABLE IF EXISTS {}", cfg.table_kafka),
    ] {
        ch.exec(&sql)
            .await
            .map_err(|e| format!("DROP failed: {e}"))?;
    }
    println!("  DROP views/kafka table: ok");

    // ─── 2. Backup data tables (CREATE AS + EXCHANGE) ───
    println!("=== Step 2: Backup data tables ===");

    for tbl in [&cfg.table_quotes, &cfg.table_ohlc] {
        let exists = ch
            .exec(&format!("EXISTS TABLE {tbl}"))
            .await
            .unwrap_or_default();

        if exists.trim() == "1" {
            let bak_name = format!("{tbl}{bak_suffix}");
            ch.exec(&format!("CREATE TABLE {bak_name} AS {tbl}"))
                .await
                .map_err(|e| format!("CREATE backup {bak_name} failed: {e}"))?;
            ch.exec(&format!("EXCHANGE TABLES {tbl} AND {bak_name}"))
                .await
                .map_err(|e| format!("EXCHANGE {tbl} <-> {bak_name} failed: {e}"))?;
            println!("  Backup: {tbl} -> {bak_name}");
        } else {
            println!("  {tbl}: does not exist, skip backup");
        }
    }

    // ─── 3. Kafka: delete topic + recreate ───
    println!("=== Step 3: Kafka — recreate topic ===");

    // Consumer group deletion is not supported by rskafka.
    // After topic deletion+recreation, old offsets become irrelevant.
    println!("  Note: consumer group '{}' — old offsets reset by topic recreation", cfg.consumer_group);

    kafka.delete_topic(&cfg.topic).await?;
    println!("  Deleted topic '{}' (or did not exist)", cfg.topic);

    // Small delay for Kafka to process deletion
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    kafka.create_topic(&cfg.topic, 1, 1).await?;
    println!("  Created topic '{}'", cfg.topic);

    // ─── 4. CREATE pipeline ───
    println!("=== Step 4: CREATE pipeline ===");

    // Each statement sent separately (CH HTTP API = one statement per request)
    let statements = build_create_statements(cfg);
    for (i, sql) in statements.iter().enumerate() {
        ch.exec(sql)
            .await
            .map_err(|e| format!("CREATE step {} failed: {e}", i + 1))?;
    }
    println!("  Pipeline created: ok");

    println!("=== Done ===");
    Ok(())
}

fn build_create_statements(cfg: &Config) -> Vec<String> {
    vec![
        // 1. quotes storage
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
        // 2. ohlc storage
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
        // 3. Kafka engine table
        format!(
            r#"CREATE TABLE {table_kafka} (
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
        // 4. MV: kafka -> quotes
        format!(
            r#"CREATE MATERIALIZED VIEW {mv_kafka} TO {table_quotes} AS
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
        // 5. MV: quotes -> ohlc
        format!(
            r#"CREATE MATERIALIZED VIEW {mv_ohlc} TO {table_ohlc} AS
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
    ]
}

/// Generate UTC timestamp suffix like 20260216_143052
fn utc_now_suffix() -> String {
    use std::time::SystemTime;
    let secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let h = time_of_day / 3600;
    let m = (time_of_day % 3600) / 60;
    let s = time_of_day % 60;

    // civil_from_days (Howard Hinnant algorithm)
    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d_val = doy - (153 * mp + 2) / 5 + 1;
    let m_val = if mp < 10 { mp + 3 } else { mp - 9 };
    let y_val = if m_val <= 2 { y + 1 } else { y };

    format!("{y_val:04}{m_val:02}{d_val:02}_{h:02}{m:02}{s:02}")
}
