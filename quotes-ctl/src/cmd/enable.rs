use crate::clickhouse::ChClient;
use crate::config::Config;

pub async fn run(cfg: &Config) -> Result<(), String> {
    let ch = ChClient::new(cfg);

    println!("Enabling Kafka consumer (ATTACH {})...", cfg.table_kafka);
    match ch.exec(&format!("ATTACH TABLE {}", cfg.table_kafka)).await {
        Ok(_) => println!("  ATTACH {}: ok", cfg.table_kafka),
        Err(err) => {
            if err.contains("already exists") {
                println!("  Already enabled");
            } else {
                return Err(format!("ATTACH {} failed: {err}", cfg.table_kafka));
            }
        }
    }

    Ok(())
}
