use crate::clickhouse::ChClient;
use crate::config::Config;

use clap::Args;

#[derive(Args, Clone, Debug)]
pub struct RebuildOhlcArgs {
    /// Start of range (UTC), e.g. "2026-02-15 00:00:00". If omitted — full rebuild.
    #[arg(long)]
    pub from: Option<String>,

    /// End of range (UTC), e.g. "2026-02-16 00:00:00". If omitted — full rebuild.
    #[arg(long)]
    pub to: Option<String>,

    /// Filter by symbol, e.g. EURUSD. If omitted — all symbols.
    #[arg(long)]
    pub symbol: Option<String>,
}

pub async fn run(cfg: &Config, args: &RebuildOhlcArgs) -> Result<(), String> {
    let ch = ChClient::new(cfg);

    let has_range = args.from.is_some() || args.to.is_some();
    let has_symbol = args.symbol.is_some();

    // Build WHERE clause
    let mut conditions: Vec<String> = Vec::new();
    if let Some(from) = &args.from {
        conditions.push(format!("ts >= '{from}'"));
    }
    if let Some(to) = &args.to {
        conditions.push(format!("ts < '{to}'"));
    }
    if let Some(symbol) = &args.symbol {
        conditions.push(format!("symbol = '{symbol}'"));
    }
    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    // Description
    let scope = match (has_range, has_symbol) {
        (true, true) => format!(
            "{} from {} to {}",
            args.symbol.as_deref().unwrap_or("?"),
            args.from.as_deref().unwrap_or("*"),
            args.to.as_deref().unwrap_or("*"),
        ),
        (true, false) => format!(
            "all symbols from {} to {}",
            args.from.as_deref().unwrap_or("*"),
            args.to.as_deref().unwrap_or("*"),
        ),
        (false, true) => format!("{} (full)", args.symbol.as_deref().unwrap_or("?")),
        (false, false) => "FULL REBUILD (all symbols, all time)".to_string(),
    };
    println!("=== Rebuild OHLC: {scope} ===");

    // 1. Delete existing OHLC for the range
    println!("Step 1: Delete OHLC...");
    if conditions.is_empty() {
        ch.exec(&format!("TRUNCATE TABLE {}", cfg.table_ohlc))
            .await
            .map_err(|e| format!("TRUNCATE ohlc failed: {e}"))?;
    } else {
        ch.exec(&format!(
            "DELETE FROM {table_ohlc} {where_clause}",
            table_ohlc = cfg.table_ohlc,
        ))
        .await
        .map_err(|e| format!("DELETE ohlc failed: {e}"))?;
    }
    println!("  Deleted: ok");

    // 2. Rebuild from quotes
    println!("Step 2: Rebuild from quotes...");
    let sql = format!(
        r#"INSERT INTO {table_ohlc}
SELECT tf, symbol,
    fromUnixTimestamp64Milli(intDiv(toUnixTimestamp64Milli(ts), interval_ms) * interval_ms) AS bucket,
    argMinState(bid, ts) AS open, maxState(bid) AS high,
    minState(bid) AS low, argMaxState(bid, ts) AS close, countState() AS volume
FROM {table_quotes} FINAL
ARRAY JOIN
    [1000, 60000, 300000, 900000, 1800000, 3600000,
     14400000, 86400000, 604800000, 31536000000] AS interval_ms,
    ['1s', '1m', '5m', '15m', '30m', '1h',
     '4h', '1d', '1w', '1y'] AS tf
{where_clause}
GROUP BY tf, symbol, bucket"#,
        table_ohlc = cfg.table_ohlc,
        table_quotes = cfg.table_quotes,
    );
    ch.exec(&sql)
        .await
        .map_err(|e| format!("INSERT ohlc failed: {e}"))?;
    println!("  Rebuilt: ok");

    // 3. Verify
    println!("=== Result ===");
    let verify_where = if conditions.is_empty() {
        String::new()
    } else {
        // reuse same conditions but change table context
        format!("WHERE {}", conditions.join(" AND "))
    };
    let sql = format!(
        "SELECT tf, count() AS candles FROM {table_ohlc} {verify_where} GROUP BY tf ORDER BY tf FORMAT PrettyCompactMonoBlock",
        table_ohlc = cfg.table_ohlc,
    );
    let out = ch.exec(&sql).await.unwrap_or_else(|e| format!("Error: {e}"));
    if out.trim().is_empty() {
        println!("  (no candles — quotes table may be empty for this range)");
    } else {
        println!("{out}");
    }

    println!("=== Done ===");
    Ok(())
}
