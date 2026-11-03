mod database_process_lock;

use binance_grid_bot::config::AppConfig;
use binance_grid_bot::db::Database;
use binance_grid_bot::lifecycle::ShutdownSignals;
use binance_grid_bot::runtime::TradingRuntime;
use binance_grid_bot::server::create_managed_router;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(
    name = "binance-grid-bot",
    about = "High-performance Binance Futures Grid Trading Bot API and SQLite Storage"
)]
struct CliArgs {
    /// Path to SQLite database file
    #[arg(short, long, default_value = "data/bot.db")]
    db: PathBuf,

    /// API server host (defaults to the saved setting, or 127.0.0.1 for a new database)
    #[arg(long)]
    host: Option<String>,

    /// API server port
    #[arg(short, long)]
    port: Option<u16>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Initialize a legacy paper account with an empty position and 10000 balance.
    #[command(
        long_about = "Initialize a legacy paper account with an empty position and 10000 balance. Historical positions and balances are NOT restored. Only paper trading is affected.\n\nStop the bot before running this command, including any older version that does not honor the process lock. Use a database file with a single hard link. The strategy remains paused after restart; resume it manually after reviewing the new baseline. Existing paper snapshots and scopes without historical paper trades are rejected."
    )]
    MigrateLegacyPaper {
        /// Path to an existing SQLite database file
        #[arg(long)]
        db: PathBuf,
        /// Exact uppercase trading pair, e.g. BTCUSDT
        #[arg(long)]
        symbol: String,
        /// Type INITIALIZE-EMPTY-PAPER-ACCOUNT to confirm the empty starting state
        #[arg(long)]
        confirm: String,
    },
}

fn load_saved_config(db: &Database) -> anyhow::Result<AppConfig> {
    let mut config = db.load_config()?.unwrap_or_default();
    config.ensure_pair_list()?;
    db.save_config(&config)?;
    Ok(config)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "binance_grid_bot=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    let args = CliArgs::parse();
    if let Some(Command::MigrateLegacyPaper {
        db,
        symbol,
        confirm,
    }) = args.command
    {
        anyhow::ensure!(
            confirm == "INITIALIZE-EMPTY-PAPER-ACCOUNT",
            "Confirmation must be exactly INITIALIZE-EMPTY-PAPER-ACCOUNT"
        );
        anyhow::ensure!(
            (3..=32).contains(&symbol.len())
                && symbol
                    .bytes()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()),
            "Trading pair must contain 3–32 uppercase ASCII letters or digits"
        );
        #[cfg(not(unix))]
        anyhow::bail!("Offline paper migration requires Unix process locking");
        #[cfg(unix)]
        {
            let metadata = std::fs::symlink_metadata(&db)
                .map_err(|e| anyhow::anyhow!("Cannot inspect existing database {:?}: {}", db, e))?;
            anyhow::ensure!(
                metadata.file_type().is_file(),
                "Migration requires an existing regular database file, not a symlink"
            );
            let _process_lock = database_process_lock::DatabaseProcessLock::acquire(&db)?;
            Database::open(&db)?.initialize_legacy_paper_account(&symbol)?;
            println!("Initialized paper account for {} with an empty position and 10000 balance. Historical positions and balances were not restored. Restart the bot and manually resume the paused strategy after reviewing the baseline.", symbol);
            return Ok(());
        }
    }
    let mut signals = ShutdownSignals::register().await?;
    let signal = signals.wait();
    tokio::pin!(signal);
    let _process_lock = database_process_lock::DatabaseProcessLock::acquire(&args.db)?;
    let db = Arc::new(Database::open(&args.db)?);
    let mut config = load_saved_config(&db)?;
    // These options only select the listening address for this process (e.g. recovery).
    if let Some(host) = args.host {
        config.server.host = host;
    }
    if let Some(port) = args.port {
        config.server.port = port;
    }
    let strategies = config.strategy_configs()?;
    let has_admin = db.is_admin_password_set()?;
    info!(
        "Binance Futures Grid Bot — SQLite {:?}, {} trading pair(s)",
        args.db,
        strategies.len()
    );
    info!(
        "Admin authentication: {}",
        if has_admin {
            "PROTECTED"
        } else {
            "PENDING SETUP"
        }
    );
    for strategy in &strategies {
        info!(
            "{}: interval {}, amount {}, window {} buy / {} sell, mode {}",
            strategy.exchange.symbol,
            strategy.grid.grid_interval,
            strategy.grid.order_amount_usdc,
            strategy.grid.buy_window,
            strategy.grid.sell_window,
            binance_grid_bot::types::TradingMode::from_exchange(
                strategy.exchange.dry_run,
                strategy.exchange.is_testnet
            )
            .as_str()
        );
    }
    let addr = config.server.socket_addr()?;
    // Bind before starting any engine, including initialization-time reconciliation.
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let (startup_stop, startup_stop_rx) = watch::channel(false);
    let mut runtime = {
        let startup = TradingRuntime::start_interruptible(&config, db, startup_stop_rx);
        tokio::pin!(startup);
        tokio::select! {
            biased;
            reason = &mut signal => {
                let _ = startup_stop.send(true);
                let reason = match reason {
                    Ok(reason) => format!("Startup interrupted by {reason}"),
                    Err(error) => format!("Shutdown signal failed during startup: {error:#}"),
                };
                match startup.await {
                    Ok(mut runtime) => {
                        runtime.shutdown(Duration::from_secs(30)).await
                            .map_err(|error| error.context(reason.clone()))?;
                        return Err(anyhow::anyhow!(reason));
                    }
                    Err(error) => return Err(error.context(reason)),
                }
            }
            result = &mut startup => result?,
        }
    };
    let snapshot_task = tokio::spawn(binance_grid_bot::snapshots::run_hourly_snapshots(
        runtime.handle(),
    ));
    let states = runtime.states();
    if let Some(code) = states[0].setup_code() {
        info!(
            "首次设置管理员密码需要初始化码: {}（可在应用日志中查看）",
            code
        );
    }
    let app = create_managed_router(runtime.handle()).await;
    info!(
        "API server running at http://{} (local: http://127.0.0.1:{})",
        addr, config.server.port
    );
    let (web_stop, mut web_stop_rx) = watch::channel(false);
    let mut web_task = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            while !*web_stop_rx.borrow_and_update() {
                if web_stop_rx.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
    });
    let reason = tokio::select! {
        reason = &mut signal => reason.map(str::to_string),
        result = &mut web_task => Err(anyhow::anyhow!("Web server stopped: {:?}", result)),
        message = runtime.run() => Err(anyhow::anyhow!(message)),
    };
    info!("Stopping bot: {:?}", reason);
    snapshot_task.abort();
    let cleanup = runtime.shutdown(Duration::from_secs(30)).await;
    if let Err(error) = &cleanup {
        error!("Shutdown cleanup incomplete: {}", error);
    }
    let _ = web_stop.send(true);
    if !web_task.is_finished()
        && tokio::time::timeout(Duration::from_secs(5), &mut web_task)
            .await
            .is_err()
    {
        web_task.abort();
        let _ = web_task.await;
    }
    cleanup?;
    reason?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn startup_preserves_saved_settings_and_upgrades_single_pair() {
        let db = Database::open(":memory:").unwrap();
        let mut saved = AppConfig::default();
        saved.exchange.symbol = "BTCUSDT".into();
        saved.exchange.api_key = "saved-key".into();
        saved.grid.grid_interval = dec!(10);
        saved.server.host = "0.0.0.0".into();
        db.save_config(&saved).unwrap();
        let config = load_saved_config(&db).unwrap();
        let strategies = config.strategy_configs().unwrap();
        assert_eq!(strategies.len(), 1);
        assert_eq!(strategies[0].exchange.symbol, "BTCUSDT");
        assert_eq!(strategies[0].exchange.api_key, "saved-key");
        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(strategies[0].grid.grid_interval, dec!(10));
        assert_eq!(
            serde_json::to_value(&config).unwrap(),
            serde_json::to_value(load_saved_config(&db).unwrap()).unwrap()
        );
    }

    #[test]
    fn fresh_database_uses_defaults_without_credentials() {
        let db = Database::open(":memory:").unwrap();
        let config = load_saved_config(&db).unwrap();
        config.validate().unwrap();
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(load_saved_config(&db).unwrap().server.host, "127.0.0.1");
        assert_eq!(config.pairs.len(), 1);
        let strategy = &config.strategy_configs().unwrap()[0];
        assert!(strategy.exchange.dry_run);
        assert!(strategy.exchange.api_key.is_empty());
        assert!(strategy.exchange.api_secret.is_empty());
    }

    #[test]
    fn file_and_strategy_cli_options_are_no_longer_supported() {
        for option in [
            "--config",
            "--symbol",
            "--interval",
            "--amount",
            "--live",
            "--testnet",
            "--buy-window",
            "--sell-window",
        ] {
            assert!(CliArgs::try_parse_from(["bot", option, "value"]).is_err());
        }
    }
}
