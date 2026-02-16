use crate::clickhouse::ChClient;
use crate::config::Config;
use crate::kafka_admin::KafkaAdmin;

use clap::Args;

#[derive(Args, Clone, Debug)]
pub struct HistoryReloadArgs {
    /// Start of range (UTC), e.g. "2026-02-15 00:00:00"
    #[arg(long)]
    pub from: String,

    /// End of range (UTC), e.g. "2026-02-16 00:00:00"
    #[arg(long)]
    pub to: String,

    /// Symbol, e.g. EURUSD
    #[arg(long)]
    pub symbol: String,
}

pub async fn run(cfg: &Config, args: &HistoryReloadArgs) -> Result<(), String> {
    let ch = ChClient::new(cfg);
    let kafka = KafkaAdmin::new(cfg);

    println!(
        "=== History reload: {} from {} to {} ===",
        args.symbol, args.from, args.to
    );

    // 1. Enable consumer
    println!("Step 1: Enable Kafka consumer...");
    match ch
        .exec(&format!("ATTACH TABLE {}", cfg.table_kafka))
        .await
    {
        Ok(_) => println!("  ATTACH {}: ok", cfg.table_kafka),
        Err(err) => {
            if err.contains("already exists") {
                println!("  Already enabled");
            } else {
                return Err(format!("ATTACH {} failed: {err}", cfg.table_kafka));
            }
        }
    }

    // 2. Delete range from quotes and ohlc
    println!("Step 2: Delete range from quotes and ohlc...");
    ch.exec(&format!(
        "DELETE FROM {} WHERE ts >= '{}' AND ts < '{}' AND symbol = '{}'",
        cfg.table_quotes, args.from, args.to, args.symbol
    ))
    .await
    .map_err(|e| format!("DELETE quotes failed: {e}"))?;
    println!("  DELETE {}: ok", cfg.table_quotes);

    ch.exec(&format!(
        "DELETE FROM {} WHERE ts >= '{}' AND ts < '{}' AND symbol = '{}'",
        cfg.table_ohlc, args.from, args.to, args.symbol
    ))
    .await
    .map_err(|e| format!("DELETE ohlc failed: {e}"))?;
    println!("  DELETE {}: ok", cfg.table_ohlc);

    // 3. Generate ticks and send to Kafka
    println!("Step 3: Generate ticks and send to Kafka...");
    let from_sec = parse_utc_datetime(&args.from)?;
    let to_sec = parse_utc_datetime(&args.to)?;
    if to_sec <= from_sec {
        return Err("--to must be after --from".to_string());
    }

    let ticks = ((to_sec - from_sec) / 60) as usize;
    println!("  Generating {ticks} ticks ({}, {} -> {})...", args.symbol, args.from, args.to);

    let mut messages: Vec<(Vec<u8>, chrono::DateTime<chrono::Utc>)> = Vec::with_capacity(ticks);
    for i in 0..ticks {
        let ts_sec = from_sec + (i as i64) * 60;
        let ts_ms = ts_sec * 1000;
        // Deterministic pseudo-random shift 0..99
        let shift = ((i as u64).wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407) >> 33) % 100;
        let bid_frac = 11540 + shift;
        let ask_frac = 11560 + shift;
        let json = format!(
            r#"{{"symbol":"{}","bid":1.{:05},"ask":1.{:05},"ts_ms":{}}}"#,
            args.symbol, bid_frac, ask_frac, ts_ms
        );
        let ts_chrono = chrono::DateTime::from_timestamp(ts_sec, 0)
            .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
        messages.push((json.into_bytes(), ts_chrono));
    }

    kafka.produce_messages(&cfg.topic, messages).await?;
    println!("  Sent {ticks} ticks to Kafka topic '{}'", cfg.topic);

    // 4. Wait for pipeline
    println!("Step 4: Waiting for pipeline to process (5s)...");
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // 5. Verify
    println!("=== Result ===");
    let sql = format!(
        "SELECT symbol, count() AS ticks, min(ts) AS first, max(ts) AS last FROM {} FINAL WHERE ts >= '{}' AND ts < '{}' AND symbol = '{}' GROUP BY symbol ORDER BY symbol FORMAT PrettyCompactMonoBlock",
        cfg.table_quotes, args.from, args.to, args.symbol
    );
    let out = ch.exec(&sql).await.unwrap_or_else(|e| format!("Error: {e}"));
    if out.trim().is_empty() {
        println!("  quotes: (empty — data may not have been ingested yet)");
    } else {
        println!("  quotes:");
        println!("{out}");
    }

    let sql = format!(
        "SELECT tf, count() AS candles FROM {} WHERE ts >= '{}' AND ts < '{}' AND symbol = '{}' GROUP BY tf ORDER BY tf FORMAT PrettyCompactMonoBlock",
        cfg.table_ohlc, args.from, args.to, args.symbol
    );
    let out = ch.exec(&sql).await.unwrap_or_else(|e| format!("Error: {e}"));
    if out.trim().is_empty() {
        println!("  ohlc: (empty — data may not have been ingested yet)");
    } else {
        println!("  ohlc:");
        println!("{out}");
    }

    println!("=== Done ===");
    Ok(())
}

/// Parse "YYYY-MM-DD HH:MM:SS" to epoch seconds (UTC).
fn parse_utc_datetime(s: &str) -> Result<i64, String> {
    let s = s.trim();
    if s.len() < 19 {
        return Err(format!("Invalid datetime format: '{s}' (expected YYYY-MM-DD HH:MM:SS)"));
    }
    let year: i64 = s[0..4].parse().map_err(|_| format!("bad year in '{s}'"))?;
    let month: u32 = s[5..7].parse().map_err(|_| format!("bad month in '{s}'"))?;
    let day: u32 = s[8..10].parse().map_err(|_| format!("bad day in '{s}'"))?;
    let hour: u32 = s[11..13].parse().map_err(|_| format!("bad hour in '{s}'"))?;
    let min: u32 = s[14..16].parse().map_err(|_| format!("bad minute in '{s}'"))?;
    let sec: u32 = s[17..19].parse().map_err(|_| format!("bad second in '{s}'"))?;

    // Days from civil date (Howard Hinnant algorithm)
    let (y, m) = if month <= 2 {
        (year - 1, month + 9)
    } else {
        (year, month - 3)
    };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * (m as u64) + 2) / 5 + (day as u64) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era as i64 * 146097 + doe as i64 - 719468;

    Ok(days * 86400 + hour as i64 * 3600 + min as i64 * 60 + sec as i64)
}
