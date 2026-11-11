use binance_grid_bot::{
    config::{AppConfig, ServerConfig},
    db::{Database, DatabaseProcessLock},
    lifecycle::ShutdownSignals,
    runtime::TradingRuntime,
    server::create_managed_router,
};
use clap::{Parser, Subcommand};
use rusqlite::{Connection, OpenFlags};
use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc, time::Duration};
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
    /// Probe the HTTP server health endpoint and exit non-zero on failure.
    Healthcheck {
        #[arg(long)]
        url: Option<String>,
    },
}

fn load_saved_config(db: &Database) -> anyhow::Result<AppConfig> {
    let loaded = db.load_config()?;
    let mut config = loaded.clone().unwrap_or_default();
    config.ensure_pair_list()?;
    if loaded.as_ref() != Some(&config) {
        db.save_config(&config)?;
    }
    Ok(config)
}

fn listen_sidecar_path(db_path: &std::path::Path) -> PathBuf {
    let mut os = db_path.as_os_str().to_os_string();
    os.push("-listen");
    PathBuf::from(os)
}

#[must_use = "dropping ActiveListenGuard unlinks the active listener sidecar"]
struct ActiveListenGuard(PathBuf);

impl ActiveListenGuard {
    fn record(db_path: &std::path::Path, addr: std::net::SocketAddr) -> anyhow::Result<Self> {
        let path = listen_sidecar_path(db_path);
        if std::fs::symlink_metadata(&path).is_ok_and(|m| m.file_type().is_symlink()) {
            anyhow::bail!("Active listener sidecar {path:?} must not be a symbolic link");
        }
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        use std::io::Write;
        opts.open(&path)?.write_all(addr.to_string().as_bytes())?;
        Ok(Self(path))
    }
}

impl Drop for ActiveListenGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn resolve_healthcheck_url(args: &CliArgs, url: Option<&str>) -> anyhow::Result<String> {
    if let Some(url) = url {
        return Ok(url.to_owned());
    }
    let sidecar = listen_sidecar_path(&args.db);
    let active_server = std::fs::symlink_metadata(&sidecar)
        .ok()
        .filter(|m| m.file_type().is_file() && m.len() <= 128)
        .and_then(|_| std::fs::read_to_string(&sidecar).ok())
        .and_then(|raw| raw.trim().parse::<std::net::SocketAddr>().ok())
        .map(|addr| ServerConfig {
            host: addr.ip().to_string(),
            port: addr.port(),
        });
    let mut server: ServerConfig = active_server.unwrap_or_else(|| {
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        Connection::open_with_flags(&args.db, flags)
            .and_then(|conn| {
                let _ = conn.busy_timeout(Duration::from_millis(500));
                conn.query_row("SELECT config_json FROM app_config WHERE id = 1", [], |r| {
                    r.get::<_, String>(0)
                })
            })
            .ok()
            .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
            .and_then(|mut v| serde_json::from_value(v.get_mut("server")?.take()).ok())
            .unwrap_or_default()
    });
    if let Some(host) = &args.host {
        server.host.clone_from(host);
    }
    if let Some(port) = args.port {
        server.port = port;
    }
    let mut addr = server.socket_addr()?;
    if addr.ip().is_unspecified() {
        addr.set_ip(if addr.is_ipv4() {
            std::net::Ipv4Addr::LOCALHOST.into()
        } else {
            std::net::Ipv6Addr::LOCALHOST.into()
        });
    }
    Ok(format!("http://{addr}/api/auth/status"))
}

async fn run_healthcheck(url: &str) -> anyhow::Result<()> {
    const MAX_BODY: usize = 8 * 1024;
    let parsed =
        reqwest::Url::parse(url).map_err(|_| anyhow::anyhow!("Invalid healthcheck URL"))?;
    anyhow::ensure!(
        matches!(parsed.scheme(), "http" | "https"),
        "Healthcheck URL must use http or https"
    );
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(3))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut resp = client
        .get(parsed)
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;
    let status = resp.status();
    anyhow::ensure!(status.is_success(), "Healthcheck failed: HTTP {status}");
    let too_big = resp.content_length().is_some_and(|n| n > MAX_BODY as u64);
    anyhow::ensure!(!too_big, "Healthcheck body exceeded {MAX_BODY} bytes");
    let mut total = 0usize;
    while let Some(chunk) = resp.chunk().await.map_err(reqwest::Error::without_url)? {
        total = total.saturating_add(chunk.len());
        anyhow::ensure!(total <= MAX_BODY, "Healthcheck body exceeded {MAX_BODY}");
    }
    Ok(())
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
    if let Some(Command::Healthcheck { url }) = &args.command {
        return run_healthcheck(&resolve_healthcheck_url(&args, url.as_deref())?).await;
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
    let _listen_guard = ActiveListenGuard::record(&args.db, listener.local_addr()?)?;
    let runtime = start_runtime_until_ready(&config, db, admin_auth, signal.as_mut()).await?;
    let snapshot_task = tokio::spawn(binance_grid_bot::snapshots::run_hourly_snapshots(
        runtime.handle(),
    ));
    let app = create_managed_router(runtime.handle());
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
    use binance_grid_bot::types::TradingMode;
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
        assert_eq!(config, load_saved_config(&db).unwrap());
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
        assert_eq!(strategy.exchange.trading_mode(), TradingMode::Paper);
        assert!(strategy.exchange.api_key.is_empty());
        assert!(strategy.exchange.api_secret.is_empty());
    }

    #[test]
    fn file_and_strategy_cli_options_are_no_longer_supported() {
        let opts =
            "--config --symbol --interval --amount --live --testnet --buy-window --sell-window";
        for opt in opts.split_whitespace() {
            assert!(CliArgs::try_parse_from(["bot", opt, "value"]).is_err());
        }
    }

    #[tokio::test]
    async fn healthcheck_command_resolves_bind_addresses_and_verifies_http_status() {
        use axum::{http::StatusCode, routing::get, Router};
        let check = |db: &str, extra: &str, addr: &str| {
            let cmd = format!("bot --db {db} {extra} healthcheck");
            let cli = CliArgs::try_parse_from(cmd.split_whitespace()).unwrap();
            let is_hc = matches!(cli.command, Some(Command::Healthcheck { url: None }));
            assert!(is_hc);
            let got = resolve_healthcheck_url(&cli, None).unwrap();
            assert_eq!(got, format!("http://{addr}/api/auth/status"));
        };
        check("no-such-hc.db", "", "127.0.0.1:8080");
        check("no-such-hc.db", "--host 0.0.0.0 -p 9090", "127.0.0.1:9090");
        check("no-such-hc.db", "--host [::] --port 9091", "[::1]:9091");
        struct TempDir(PathBuf);
        impl Drop for TempDir {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let dir = TempDir(std::env::temp_dir().join(format!("hc-{}", uuid::Uuid::new_v4())));
        std::fs::create_dir_all(&dir.0).unwrap();
        let db_file = dir.0.join("bot.db");
        let mut saved = AppConfig::default();
        saved.server.host = "[::]".into();
        saved.server.port = 9123;
        let db = Database::open(&db_file).unwrap();
        db.save_config(&saved).unwrap();
        drop(db);
        check(db_file.to_str().unwrap(), "", "[::1]:9123");
        let guard = ActiveListenGuard::record(&db_file, "0.0.0.0:8080".parse().unwrap()).unwrap();
        check(db_file.to_str().unwrap(), "", "127.0.0.1:8080");
        drop(guard);
        check(db_file.to_str().unwrap(), "", "[::1]:9123");
        let app = Router::new()
            .route("/ok", get(|| async { "ok" }))
            .route("/big", get(|| async { "x".repeat(8 * 1024 + 1) }))
            .route("/fail", get(|| async { StatusCode::SERVICE_UNAVAILABLE }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        run_healthcheck(&format!("http://{addr}/ok")).await.unwrap();
        for path in ["big", "fail"] {
            let bad = format!("http://{addr}/{path}");
            assert!(run_healthcheck(&bad).await.is_err());
        }
        server.abort();
    }
}
