use binance_grid_bot::config::AppConfig;
use binance_grid_bot::db::Database;
use binance_grid_bot::exchange::{BinanceFuturesClient, BinanceWsStream};
use binance_grid_bot::server::{create_router, AppState};
use binance_grid_bot::strategy::GridTradingEngine;
use binance_grid_bot::types::TickerInfo;
use clap::Parser;
use rust_decimal::Decimal;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
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

    /// Web server dashboard host
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

    if overridden {
        db.save_config(&config)?;
    }

    info!("============================================================");
    info!("🚀 Binance Futures Grid Trading Robot (Rust Engine)");
    info!("• Persistence:      SQLite ({:?}) - Zero config files required!", args.db);
    info!("• Symbol:           {}", config.exchange.symbol);
    info!("• Grid Interval:    {} USDC", config.grid.grid_interval);
    info!("• Order Size:       {} USDC per grid", config.grid.order_amount_usdc);
    info!("• Window Orders:    {} Buy / {} Sell", config.grid.buy_window, config.grid.sell_window);
    info!("• Order Type:       Post-Only Maker (GTX)");
    info!("• Trading Mode:     {}", if config.exchange.dry_run { "PAPER TRADING (Simulation)" } else if config.exchange.is_testnet { "TESTNET" } else { "LIVE REAL" });
    info!("• Dashboard Web:    http://{}:{}", config.server.host, config.server.port);
    info!("============================================================");

    // Channels
    let (action_tx, action_rx) = mpsc::channel(32);
    let (ticker_tx, ticker_rx) = broadcast::channel::<TickerInfo>(64);

    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let state = AppState::new(config.clone(), db.clone(), action_tx.clone());

    // Spawn Binance WebSocket market data stream
    let ws_stream = BinanceWsStream::new(
        config.exchange.symbol.clone(),
        config.exchange.is_testnet,
        ticker_tx.clone(),
    );
    tokio::spawn(async move {
        ws_stream.run().await;
    });

    // Initialize and spawn Grid Trading Engine
    let mut engine = GridTradingEngine::new(
        state.clone(),
        client.clone(),
        action_rx,
        ticker_rx,
    );

    if let Err(e) = engine.initialize().await {
        error!("Engine initialization warning: {}", e);
    }

    tokio::spawn(async move {
        engine.run().await;
    });

    // Start Web Server
    let app = create_router(state.clone());
    let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.port).parse()?;
    info!("Web Dashboard running at http://{}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    
    // Run web server with graceful shutdown
    tokio::select! {
        res = axum::serve(listener, app) => {
            if let Err(e) = res {
                error!("Web server error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal (Ctrl+C). Stopping bot gracefully...");
        }
    }

    Ok(())
}
