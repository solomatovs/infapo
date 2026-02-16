use crate::clickhouse::ChClient;
use crate::config::Config;

pub async fn run(cfg: &Config) -> Result<(), String> {
    let ch = ChClient::new(cfg);

    // 1. Pipeline tables
    println!("=== Pipeline tables ===");
    let sql = format!(
        "SELECT name, engine, total_rows, total_bytes
FROM system.tables
WHERE database = currentDatabase()
  AND name IN ('{table_kafka}', '{mv_kafka}',
               '{mv_ohlc}', '{table_quotes}', '{table_ohlc}')
ORDER BY name
FORMAT PrettyCompactMonoBlock",
        table_kafka = cfg.table_kafka,
        mv_kafka = cfg.mv_kafka,
        mv_ohlc = cfg.mv_ohlc,
        table_quotes = cfg.table_quotes,
        table_ohlc = cfg.table_ohlc,
    );
    let out = ch.exec(&sql).await.unwrap_or_else(|e| format!("Error: {e}"));
    if out.trim().is_empty() {
        println!("  (no pipeline tables found)");
    } else {
        println!("{out}");
    }

    // 2. Kafka consumers
    println!("=== Kafka consumers ===");
    let sql = "SELECT
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
FORMAT Vertical";
    let out = ch.exec(sql).await.unwrap_or_else(|e| format!("Error: {e}"));
    if out.trim().is_empty() {
        println!("  (no active consumers — kafka table is likely detached)");
    } else {
        println!("{out}");
    }

    // 3. Data summary
    println!("=== Data: quotes ===");
    let sql = format!(
        "SELECT symbol, count() AS ticks, min(ts) AS first, max(ts) AS last
FROM {table_quotes} FINAL
GROUP BY symbol ORDER BY symbol
FORMAT PrettyCompactMonoBlock",
        table_quotes = cfg.table_quotes,
    );
    let out = ch.exec(&sql).await.unwrap_or_else(|e| format!("Error: {e}"));
    if out.trim().is_empty() {
        println!("  (empty)");
    } else {
        println!("{out}");
    }

    println!("=== Data: ohlc ===");
    let sql = format!(
        "SELECT tf, count() AS candles
FROM {table_ohlc}
GROUP BY tf ORDER BY tf
FORMAT PrettyCompactMonoBlock",
        table_ohlc = cfg.table_ohlc,
    );
    let out = ch.exec(&sql).await.unwrap_or_else(|e| format!("Error: {e}"));
    if out.trim().is_empty() {
        println!("  (empty)");
    } else {
        println!("{out}");
    }

    Ok(())
}
