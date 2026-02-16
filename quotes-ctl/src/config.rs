use clap::Args;

#[derive(Args, Clone, Debug)]
pub struct Config {
    // ─── ClickHouse (HTTP API) ───
    #[arg(long, default_value = "127.0.0.1", env = "CH_HOST")]
    pub ch_host: String,

    #[arg(long, default_value_t = 8123, env = "CH_PORT")]
    pub ch_port: u16,

    #[arg(long, env = "CH_USER")]
    pub ch_user: String,

    #[arg(long, env = "CH_PASSWORD")]
    pub ch_password: String,

    #[arg(long, default_value = "default", env = "CH_DATABASE")]
    pub ch_database: String,

    /// Use HTTPS for ClickHouse connection
    #[arg(long, default_value_t = false, env = "CH_TLS")]
    pub ch_tls: bool,

    // ─── Kafka ───
    #[arg(long, default_value = "127.0.0.1:9092", env = "KAFKA_BROKER")]
    pub kafka_broker: String,

    #[arg(long, env = "KAFKA_USER")]
    pub kafka_user: String,

    #[arg(long, env = "KAFKA_PASSWORD")]
    pub kafka_password: String,

    /// Use TLS for Kafka connection
    #[arg(long, default_value_t = false, env = "KAFKA_TLS")]
    pub kafka_tls: bool,

    // ─── Topic & Consumer Group ───
    #[arg(long, default_value = "quotes", env = "TOPIC")]
    pub topic: String,

    #[arg(long, default_value = "clickhouse_quotes", env = "CG")]
    pub consumer_group: String,

    // ─── Kafka broker seen from ClickHouse (for Kafka Engine SETTINGS) ───
    /// Kafka broker address as seen from ClickHouse (e.g. kafka:9092 inside Docker)
    #[arg(long, default_value = "kafka:9092", env = "CH_KAFKA_BROKER")]
    pub ch_kafka_broker: String,

    /// Kafka security protocol for ClickHouse Kafka Engine
    #[arg(long, default_value = "SASL_PLAINTEXT", env = "CH_KAFKA_SECURITY_PROTOCOL")]
    pub ch_kafka_security_protocol: String,

    // ─── Tables ───
    #[arg(long, default_value = "quotes", env = "TABLE_QUOTES")]
    pub table_quotes: String,

    #[arg(long, default_value = "ohlc", env = "TABLE_OHLC")]
    pub table_ohlc: String,

    #[arg(long, default_value = "kafka_quotes", env = "TABLE_KAFKA")]
    pub table_kafka: String,

    // ─── Materialized Views ───
    #[arg(long, default_value = "mv_kafka_quotes_to_quotes", env = "MV_KAFKA")]
    pub mv_kafka: String,

    #[arg(long, default_value = "mv_quotes_to_ohlc", env = "MV_OHLC")]
    pub mv_ohlc: String,
}
