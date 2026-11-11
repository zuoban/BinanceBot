use binance_grid_bot::config::AppConfig;
use binance_grid_bot::db::{Database, DatabaseProcessLock};
use binance_grid_bot::lifecycle::ShutdownSignals;
use binance_grid_bot::runtime::TradingRuntime;
use binance_grid_bot::server::create_managed_router;
use clap::{Parser, Subcommand};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
#[command(
    name = "binance-grid-bot",
    version,
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

    /// Administrator password for the web console (prefer the ADMIN_PASSWORD environment variable)
    #[arg(long)]
    admin_password: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    Backtest(Box<binance_grid_bot::strategy::backtest::BacktestArgs>),
    /// Initialize an empty legacy paper subledger without depositing shared funds.
    #[command(
        long_about = "Initialize a legacy paper subledger with an empty position and a 10000 accounting baseline. This does not add funds to the shared paper account. Historical positions and balances are NOT restored. Only paper trading is affected.\n\nStop the bot before running this command, including any older version that does not honor the process lock. Use a database file with a single hard link. The strategy remains paused after restart; resume it manually after reviewing the new baseline. Existing paper snapshots and scopes without historical paper trades are rejected."
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
    let loaded = db.load_config()?;
    let mut config = loaded.clone().unwrap_or_default();
    config.ensure_pair_list()?;
    if loaded
        .as_ref()
        .map(serde_json::to_value)
        .transpose()?
        .as_ref()
        != Some(&serde_json::to_value(&config)?)
    {
        db.save_config(&config)?;
    }
    Ok(config)
}

fn run_migrate_legacy_paper(db: PathBuf, symbol: String, confirm: String) -> anyhow::Result<()> {
    anyhow::ensure!(
        confirm == "INITIALIZE-EMPTY-PAPER-ACCOUNT",
        "Confirmation must be exactly INITIALIZE-EMPTY-PAPER-ACCOUNT"
    );
    binance_grid_bot::config::validate_symbol(&symbol).map_err(|_| {
        anyhow::anyhow!("Trading pair must contain 3–32 uppercase ASCII letters or digits")
    })?;
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
        let _process_lock = DatabaseProcessLock::acquire(&db)?;
        Database::open(&db)?.initialize_legacy_paper_account(&symbol)?;
        println!("Initialized paper subledger for {} with an empty position and a 10000 accounting baseline; no funds were added to the shared paper account. Historical positions and balances were not restored. Restart the bot and manually resume the paused strategy after reviewing the baseline.", symbol);
        Ok(())
    }
}

fn resolve_admin_auth(
    cli_password: Option<String>,
) -> anyhow::Result<Option<Arc<binance_grid_bot::auth::AdminAuth>>> {
    let cli_supplied = cli_password.filter(|password| !password.is_empty());
    if cli_supplied.is_some() {
        warn!(
            "--admin-password was supplied on the command line and may be visible in process listings; \
             prefer the ADMIN_PASSWORD environment variable"
        );
    }
    let admin_password = cli_supplied.or_else(|| {
        std::env::var("ADMIN_PASSWORD")
            .ok()
            .filter(|password| !password.is_empty())
    });
    match admin_password {
        Some(password) => Ok(Some(Arc::new(
            binance_grid_bot::auth::AdminAuth::from_password(&password)?,
        ))),
        None => Ok(None),
    }
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "binance_grid_bot=info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();
}

fn log_startup_summary(db_path: &std::path::Path, strategies: &[AppConfig], auth_enabled: bool) {
    info!(
        "Binance Futures Grid Bot — SQLite {:?}, {} trading pair(s)",
        db_path,
        strategies.len()
    );
    if auth_enabled {
        info!("Admin authentication: enabled (password from startup configuration)");
    } else {
        warn!(
            "Admin authentication: DISABLED (no --admin-password or ADMIN_PASSWORD); \
             the console and trading APIs are open to anyone who can reach this port"
        );
    }
    for strategy in strategies {
        info!(
            "{}: interval {}, amount {}, window {} buy / {} sell, mode {}",
            strategy.exchange.symbol,
            strategy.grid.grid_interval,
            strategy.grid.order_amount_usdc,
            strategy.grid.buy_window,
            strategy.grid.sell_window,
            strategy.exchange.trading_mode().as_str()
        );
    }
}

async fn start_runtime_until_ready<F>(
    config: &AppConfig,
    db: Arc<Database>,
    admin_auth: Option<Arc<binance_grid_bot::auth::AdminAuth>>,
    mut signal: Pin<&mut F>,
) -> anyhow::Result<TradingRuntime>
where
    F: Future<Output = anyhow::Result<&'static str>>,
{
    let (startup_stop, startup_stop_rx) = watch::channel(false);
    let startup = TradingRuntime::start_interruptible(config, db, startup_stop_rx, admin_auth);
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
                    Err(anyhow::anyhow!(reason))
                }
                Err(error) => Err(error.context(reason)),
            }
        }
        result = &mut startup => result,
    }
}

fn spawn_web_server(
    listener: tokio::net::TcpListener,
    app: axum::Router,
) -> (
    watch::Sender<bool>,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let (web_stop, mut web_stop_rx) = watch::channel(false);
    let web_task = tokio::spawn(async move {
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
    (web_stop, web_task)
}

async fn supervise_and_shutdown<F>(
    mut runtime: TradingRuntime,
    snapshot_task: tokio::task::JoinHandle<()>,
    web_stop: watch::Sender<bool>,
    mut web_task: tokio::task::JoinHandle<std::io::Result<()>>,
    mut signal: Pin<&mut F>,
) -> anyhow::Result<()>
where
    F: Future<Output = anyhow::Result<&'static str>>,
{
    let handle = runtime.handle();
    let reason = {
        let running = runtime.run();
        tokio::pin!(running);
        let (reason, finished) = tokio::select! {
            biased;
            reason = &mut signal => (reason.map(str::to_string), false),
            result = &mut web_task => (Err(anyhow::anyhow!("Web server stopped: {:?}", result)), false),
            message = &mut running => (Err(anyhow::anyhow!(message)), true),
        };
        // This gate also covers a pair being prepared or rebuilt by the active command.
        handle.request_shutdown();
        let _ = web_stop.send(true);
        // The active removal owns its target; other existing engines can stop now.
        handle.start_early_shutdown(Duration::from_secs(30)).await;
        if !finished {
            // DB workers cannot be canceled safely. Wait without a deadline for the
            // command to publish its outcome before cleaning up the final engine set.
            running.await;
        }
        reason
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = CliArgs::parse();
    if let Some(Command::Backtest(backtest)) = &args.command {
        return binance_grid_bot::strategy::backtest::run_cli(backtest).await;
    }
    if let Some(Command::MigrateLegacyPaper {
        db,
        symbol,
        confirm,
    }) = args.command
    {
        return run_migrate_legacy_paper(db, symbol, confirm);
    }
    let mut signals = ShutdownSignals::register().await?;
    let signal = signals.wait();
    tokio::pin!(signal);
    let admin_auth = resolve_admin_auth(args.admin_password)?;
    let _process_lock = DatabaseProcessLock::acquire(&args.db)?;
    let db = Arc::new(Database::open(&args.db)?);
    db.clear_all_sessions()
        .map_err(|error| error.context("Failed to clear persisted login sessions"))?;
    let mut config = load_saved_config(&db)?;
    // These options only select the listening address for this process (e.g. recovery).
    if let Some(host) = args.host {
        config.server.host = host;
    }
    if let Some(port) = args.port {
        config.server.port = port;
    }
    let strategies = config.strategy_configs()?;
    log_startup_summary(&args.db, &strategies, admin_auth.is_some());
    let addr = config.server.socket_addr()?;
    // Bind before starting any engine, including initialization-time reconciliation.
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let runtime = start_runtime_until_ready(&config, db, admin_auth, signal.as_mut()).await?;
    let snapshot_task = tokio::spawn(binance_grid_bot::snapshots::run_hourly_snapshots(
        runtime.handle(),
    ));
    let app = create_managed_router(runtime.handle()).await;
    info!(
        "API server running at http://{} (local: http://127.0.0.1:{})",
        addr, config.server.port
    );
    let (web_stop, web_task) = spawn_web_server(listener, app);
    supervise_and_shutdown(runtime, snapshot_task, web_stop, web_task, signal.as_mut()).await
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
        assert_eq!(
            strategy.exchange.trading_mode(),
            binance_grid_bot::types::TradingMode::Paper
        );
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
