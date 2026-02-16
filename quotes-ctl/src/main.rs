mod clickhouse;
mod cmd;
mod config;
mod kafka_admin;

use clap::{Parser, Subcommand};
use cmd::generate::GenArgs;
use cmd::history_reload::HistoryReloadArgs;
use cmd::rebuild_ohlc::RebuildOhlcArgs;
use config::Config;

#[derive(Parser)]
#[command(name = "quotes-ctl", about = "Quotes pipeline management tool")]
struct Cli {
    #[command(flatten)]
    config: Config,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Drop + recreate pipeline with backup of data tables (quotes, ohlc → *_bak_TIMESTAMP)
    BackupAndCreate,

    /// Idempotent init — create missing objects without touching existing ones
    CreateIfNotExists,

    /// Enable Kafka consumption (ATTACH kafka engine table)
    Enable,

    /// Disable Kafka consumption (DETACH kafka engine table)
    Disable,

    /// Show pipeline state: tables, kafka consumers, data summary
    Status,

    /// Reload historical data for a symbol/range: DELETE + generate ticks + send to Kafka
    HistoryReload(HistoryReloadArgs),

    /// Rebuild OHLC candles from quotes (DELETE + INSERT). Supports --from/--to/--symbol filters
    RebuildOhlc(RebuildOhlcArgs),

    /// Quote generator: interactive, auto (--rate), generate (--generate), file (--file)
    Gen(GenArgs),
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let result = match &cli.command {
        Commands::BackupAndCreate => cmd::backup_and_create::run(&cli.config).await,
        Commands::CreateIfNotExists => cmd::create_if_not_exists::run(&cli.config).await,
        Commands::Enable => cmd::enable::run(&cli.config).await,
        Commands::Disable => cmd::disable::run(&cli.config).await,
        Commands::Status => cmd::status::run(&cli.config).await,
        Commands::HistoryReload(args) => cmd::history_reload::run(&cli.config, args).await,
        Commands::RebuildOhlc(args) => cmd::rebuild_ohlc::run(&cli.config, args).await,
        Commands::Gen(args) => cmd::generate::run(&cli.config, args).await,
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
