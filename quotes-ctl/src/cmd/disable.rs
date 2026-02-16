use crate::clickhouse::ChClient;
use crate::config::Config;

pub async fn run(cfg: &Config) -> Result<(), String> {
    let ch = ChClient::new(cfg);

    println!("Disabling Kafka consumer (DETACH {})...", cfg.table_kafka);
    match ch.exec(&format!("DETACH TABLE {}", cfg.table_kafka)).await {
        Ok(_) => println!("  DETACH {}: ok", cfg.table_kafka),
        Err(err) => {
            if err.contains("doesn't exist") || err.contains("does not exist") {
                println!("  Already disabled");
            } else {
                return Err(format!("DETACH {} failed: {err}", cfg.table_kafka));
            }
        }
    }

    Ok(())
}
