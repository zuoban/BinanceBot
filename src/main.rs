use binance_grid_bot::config::AppConfig;
use binance_grid_bot::db::Database;
use binance_grid_bot::exchange::{BinanceFuturesClient, BinanceWsStream};
use binance_grid_bot::lifecycle::{shutdown_signal, stop_engine};
use binance_grid_bot::server::{create_router, AppState};
use binance_grid_bot::strategy::GridTradingEngine;
use binance_grid_bot::types::{BotStatus, TickerInfo};
use clap::Parser;
use rust_decimal::Decimal;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(
    name = "binance-grid-bot",
    about = "High-performance Binance Futures Grid Trading Bot with Web Dashboard and SQLite Storage"
)]
struct CliArgs {
    /// Path to SQLite database file
    #[arg(short, long, default_value = "data/bot.db")]
    db: PathBuf,

    /// Optional path to legacy configuration TOML file (if migrating from file)
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Trading symbol (e.g. SOLUSDC)
    #[arg(short, long)]
    symbol: Option<String>,

    /// Grid interval spacing (e.g. 0.1)
    #[arg(short, long)]
    interval: Option<Decimal>,

    /// Order notional amount in USDC per grid (e.g. 100)
    #[arg(short, long)]
    amount: Option<Decimal>,

    /// Pre-placed buy window order count
    #[arg(long)]
    buy_window: Option<usize>,

    /// Pre-placed sell window order count
    #[arg(long)]
    sell_window: Option<usize>,

    /// Web server dashboard host (default: 0.0.0.0 for external access)
    #[arg(long)]
    host: Option<String>,

    /// Web server dashboard port
    #[arg(short, long)]
    port: Option<u16>,

    /// Run in Live Real Trading mode (disables Paper Trading)
    #[arg(long)]
    live: bool,

    /// Use Binance Futures Testnet
    #[arg(long)]
    testnet: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "binance_grid_bot=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = CliArgs::parse();

    // Initialize SQLite database
    let db = Arc::new(Database::open(&args.db)?);

    // Load configuration from SQLite or initialize defaults
    let mut config = match db.load_config()? {
        Some(saved_cfg) => {
            info!("Loaded existing configuration from SQLite ({:?})", args.db);
            saved_cfg
        }
        None => {
            // Check if legacy config file was provided or exists
            let legacy_found = if let Some(ref path) = args.config {
                path.exists()
            } else {
                Path::new("config.toml").exists()
            };

            if legacy_found {
                let legacy_path = args
                    .config
                    .as_deref()
                    .unwrap_or_else(|| Path::new("config.toml"));
                info!(
                    "Found legacy config {:?}, importing into SQLite database...",
                    legacy_path
                );
                let loaded = AppConfig::load_from_file(legacy_path)?;
                db.save_config(&loaded)?;
                loaded
            } else {
                info!(
                    "No existing configuration found. Initialized default strategy in SQLite ({:?}): SOLUSDC, 0.1 interval, 100 USDC amount",
                    args.db
                );
                let default_cfg = AppConfig::default();
                db.save_config(&default_cfg)?;
                default_cfg
            }
        }
    };

    // Apply CLI overrides if provided
    let mut overridden = false;
    if let Some(s) = args.symbol {
        config.exchange.symbol = s;
        overridden = true;
    }
    if let Some(i) = args.interval {
        config.grid.grid_interval = i;
        overridden = true;
    }
    if let Some(a) = args.amount {
        config.grid.order_amount_usdc = a;
        overridden = true;
    }
    if let Some(bw) = args.buy_window {
        config.grid.buy_window = bw;
        overridden = true;
    }
    if let Some(sw) = args.sell_window {
        config.grid.sell_window = sw;
        overridden = true;
    }
    if let Some(h) = args.host {
        config.server.host = h;
        overridden = true;
    } else if config.server.host == "127.0.0.1" {
        // Automatically upgrade legacy loopback binding to 0.0.0.0 so server and docker deployments can be accessed externally
        info!(
            "Updating server host binding from 127.0.0.1 to 0.0.0.0 for remote/external web access"
        );
        config.server.host = "0.0.0.0".to_string();
        overridden = true;
    }
    if let Some(p) = args.port {
        config.server.port = p;
        overridden = true;
    }
    if args.live {
        config.exchange.dry_run = false;
        overridden = true;
    }
    if args.testnet {
        config.exchange.is_testnet = true;
        overridden = true;
    }

    let startup_config_error = config.validate().err();
    if overridden {
        config.validate()?;
        db.save_config(&config)?;
    }

    let has_admin_pwd = db.is_admin_password_set()?;
    if !has_admin_pwd {
        if config.server.host == "0.0.0.0" {
            info!("🛡️  【首次安全初始化】系统尚未设置管理员密码！请打开控制台 http://<服务器公网IP>:{} (本地: http://127.0.0.1:{}) 进行初始密码设置。", config.server.port, config.server.port);
        } else {
            info!("🛡️  【首次安全初始化】系统尚未设置管理员密码！请打开控制台 http://{}:{} 进行初始密码设置。", config.server.host, config.server.port);
        }
    } else {
        info!("🔒 管理员密码认证已启用");
    }

    info!("============================================================");
    info!("🚀 Binance Futures Grid Trading Robot (Rust Engine)");
    info!(
        "• Persistence:      SQLite ({:?}) - Zero config files required!",
        args.db
    );
    info!(
        "• Admin Auth:       {}",
        if has_admin_pwd {
            "PROTECTED (Password Set)"
        } else {
            "PENDING SETUP (Visit Web to initialize)"
        }
    );
    info!("• Symbol:           {}", config.exchange.symbol);
    info!("• Grid Interval:    {} USDC", config.grid.grid_interval);
    info!(
        "• Order Size:       {} USDC per grid",
        config.grid.order_amount_usdc
    );
    info!(
        "• Window Orders:    {} Buy / {} Sell",
        config.grid.buy_window, config.grid.sell_window
    );
    info!("• Order Type:       Post-Only Maker (GTX)");
    info!(
        "• Trading Mode:     {}",
        if config.exchange.dry_run {
            "PAPER TRADING (Simulation)"
        } else if config.exchange.is_testnet {
            "TESTNET"
        } else {
            "LIVE REAL"
        }
    );
    if config.server.host == "0.0.0.0" {
        info!(
            "• Dashboard Web:    http://<服务器公网IP>:{} (本地: http://127.0.0.1:{})",
            config.server.port, config.server.port
        );
    } else {
        info!(
            "• Dashboard Web:    http://{}:{}",
            config.server.host, config.server.port
        );
    }
    info!("============================================================");

    let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.port).parse()?;

    // Channels
    let (action_tx, action_rx) = mpsc::channel(32);
    let (ticker_tx, ticker_rx) = broadcast::channel::<TickerInfo>(64);
    let (market_stream_tx, market_stream_rx) =
        watch::channel((config.exchange.symbol.clone(), config.exchange.is_testnet));

    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let state = AppState::new(config.clone(), db.clone(), action_tx.clone());
    if let Some(code) = state.setup_code() {
        info!(
            "首次设置管理员密码需要初始化码: {}（可在应用日志中查看）",
            code
        );
    }
    if let Some(error) = startup_config_error {
        state
            .pause_trading_with_reason("启动时发现策略配置无效，请检查系统运行日志")
            .await?;
        state
            .add_log(
                "ERROR",
                format!("Saved configuration is invalid; trading paused: {}", error),
            )
            .await;
    }
    if !has_admin_pwd && !config.exchange.dry_run {
        state
            .pause_trading_with_reason("实盘策略等待管理员完成初始化")
            .await?;
        state
            .add_log(
                "WARN",
                "Live trading paused until administrator setup is complete",
            )
            .await;
    }

    // Spawn Binance WebSocket market data stream
    let ws_stream = BinanceWsStream::new(market_stream_rx, ticker_tx.clone());
    let ws_task = tokio::spawn(async move {
        ws_stream.run().await;
    });

    // Initialize and spawn Grid Trading Engine
    let mut engine = GridTradingEngine::new(state.clone(), client.clone(), action_rx, ticker_rx);
    engine.set_market_stream_tx(market_stream_tx);

    let signal = shutdown_signal();
    tokio::pin!(signal);
    let mut early_shutdown = None;
    let initialized = tokio::select! {
        result = engine.initialize() => result,
        reason = &mut signal => {
            early_shutdown = Some(reason?);
            state.request_shutdown();
            Ok(())
        }
    };
    if let Err(e) = initialized {
        error!("Engine initialization failed; trading paused: {}", e);
        state
            .pause_trading_with_reason("策略启动时账户或挂单对账失败，请检查系统运行日志")
            .await?;
        state
            .add_log(
                "ERROR",
                format!("Startup reconciliation failed; trading paused: {}", e),
            )
            .await;
    }
    if !config.exchange.dry_run && *state.status.read().await == BotStatus::Paused {
        action_tx
            .send(binance_grid_bot::types::BotControlAction::Pause)
            .await?;
    }

    let cny_task = tokio::spawn(state.clone().run_cny_rate_refresh());
    let history_task = tokio::spawn(state.clone().run_price_history_refresh());

    let (engine_start, started) = oneshot::channel();
    let mut engine_task = tokio::spawn(async move {
        if started.await.is_ok() {
            engine.run().await;
        }
    });

    if let Some(reason) = early_shutdown {
        info!("Received {} during initialization; stopping bot...", reason);
        let _ = engine_start.send(());
        let result = stop_engine(&state, &mut engine_task, Duration::from_secs(30)).await;
        for task in [ws_task, cny_task, history_task] {
            task.abort();
            let _ = task.await;
        }
        return result;
    }

    // Start Web Server
    let app = create_router(state.clone());
    if config.server.host == "0.0.0.0" {
        info!("🌐 Web Dashboard 运行中: 远程访问 http://<服务器公网IP>:{} | 本地访问 http://127.0.0.1:{}", config.server.port, config.server.port);
    } else {
        info!("🌐 Web Dashboard running at http://{}", addr);
    }

    // Bind before starting the engine loop so a port conflict cannot leave a
    // trading task running without its control server.
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(error) => {
            state.request_shutdown();
            let _ = engine_start.send(());
            let _ = stop_engine(&state, &mut engine_task, Duration::from_secs(30)).await;
            for task in [ws_task, cny_task, history_task] {
                task.abort();
                let _ = task.await;
            }
            return Err(error.into());
        }
    };
    let _ = engine_start.send(());
    let (web_stop, mut web_stop_rx) = watch::channel(false);
    let mut web_task = tokio::spawn(async move {
        axum::serve(listener, app)
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
        result = &mut web_task => {
            Err(anyhow::anyhow!("Web server stopped: {:?}", result))
        }
        result = &mut engine_task => {
            Err(anyhow::anyhow!("Strategy engine stopped: {:?}", result))
        }
    };
    info!("Stopping bot: {:?}", reason);
    // If the engine already exited, pause persistently without polling its
    // completed JoinHandle a second time.
    let cleanup = if engine_task.is_finished() {
        state.request_shutdown();
        state
            .pause_trading_with_reason("策略引擎退出，请核实交易所挂单")
            .await
    } else {
        stop_engine(&state, &mut engine_task, Duration::from_secs(30)).await
    };
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
    for task in [ws_task, cny_task, history_task] {
        task.abort();
        let _ = task.await;
    }
    cleanup?;
    reason?;
    Ok(())
}
