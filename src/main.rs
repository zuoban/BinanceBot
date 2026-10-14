use binance_grid_bot::config::{AppConfig, GridConfig, TradingPairConfig};
use binance_grid_bot::db::Database;
use binance_grid_bot::lifecycle::shutdown_signal;
use binance_grid_bot::runtime::TradingRuntime;
use binance_grid_bot::server::{create_multi_router, create_router};
use clap::Parser;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
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

    /// Import a TOML configuration, replacing the saved SQLite configuration
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Trading symbols; repeat --symbol or use commas (e.g. SOLUSDC,BTCUSDT)
    #[arg(short, long, value_delimiter = ',')]
    symbol: Vec<String>,

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

fn apply_symbol_overrides(config: &mut AppConfig, symbols: &[String]) -> bool {
    if !symbols.is_empty() {
        let symbols: Vec<_> = symbols.iter().map(|s| s.trim().to_uppercase()).collect();
        if symbols.len() == 1 && config.pairs.is_empty() {
            config.exchange.symbol = symbols[0].clone();
        } else {
            config.pairs = symbols
                .into_iter()
                .map(|symbol| {
                    config
                        .pairs
                        .iter()
                        .find(|pair| pair.symbol == symbol)
                        .cloned()
                        .unwrap_or_else(|| TradingPairConfig::new(symbol))
                })
                .collect();
        }
        return true;
    }
    false
}

fn apply_cli_overrides(config: &mut AppConfig, args: &CliArgs) -> bool {
    let mut changed = apply_symbol_overrides(config, &args.symbol);
    let grid_changed = args.interval.is_some()
        || args.amount.is_some()
        || args.buy_window.is_some()
        || args.sell_window.is_some();
    if grid_changed {
        let overrides = |grid: &mut GridConfig| {
            if let Some(value) = args.interval {
                grid.grid_interval = value;
            }
            if let Some(value) = args.amount {
                grid.order_amount_usdc = value;
            }
            if let Some(value) = args.buy_window {
                grid.buy_window = value;
            }
            if let Some(value) = args.sell_window {
                grid.sell_window = value;
            }
        };
        overrides(&mut config.grid);
        for pair in &mut config.pairs {
            let grid = pair.grid.get_or_insert_with(|| config.grid.clone());
            overrides(grid);
        }
        changed = true;
    }
    if let Some(host) = &args.host {
        config.server.host = host.clone();
        changed = true;
    }
    if let Some(port) = args.port {
        config.server.port = port;
        changed = true;
    }
    if args.live {
        config.exchange.dry_run = false;
        for pair in &mut config.pairs {
            pair.exchange.dry_run = Some(false);
        }
        changed = true;
    }
    if args.testnet {
        config.exchange.is_testnet = true;
        for pair in &mut config.pairs {
            pair.exchange.is_testnet = Some(true);
        }
        changed = true;
    }
    changed
}

fn apply_startup_overrides(
    config: &mut AppConfig,
    args: &CliArgs,
    values: &HashMap<String, String>,
) -> anyhow::Result<bool> {
    let mut changed = false;
    if args.host.is_none()
        && !values.contains_key("SERVER_HOST")
        && config.server.host == "127.0.0.1"
    {
        config.server.host = "0.0.0.0".into();
        changed = true;
    }
    // Select CLI symbols first so their per-pair environment settings are resolved too.
    let symbols_changed = apply_symbol_overrides(config, &args.symbol);
    let mut environment = values.clone();
    if symbols_changed {
        environment.remove("TRADING_PAIRS");
    }
    changed |= config.apply_environment(&environment)?;
    changed |= apply_cli_overrides(config, args);
    Ok(changed)
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
    let db = Arc::new(Database::open(&args.db)?);
    let (mut config, imported) = if let Some(path) = &args.config {
        info!("Importing configuration from {:?}", path);
        (AppConfig::load_from_file(path)?, true)
    } else if let Some(saved) = db.load_config()? {
        info!("Loaded configuration from SQLite ({:?})", args.db);
        (saved, false)
    } else if Path::new("config.toml").exists() {
        (AppConfig::load_from_file("config.toml")?, true)
    } else {
        (AppConfig::default(), true)
    };
    let environment = std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
        .collect();
    let overridden = apply_startup_overrides(&mut config, &args, &environment)?;
    let strategies = config.strategy_configs()?;
    if imported || overridden {
        config.validate()?;
        db.save_config(&config)?;
    }
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
    let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.port).parse()?;
    // Bind before starting any engine, including initialization-time reconciliation.
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let mut runtime = TradingRuntime::start(&config, db).await?;
    let states = runtime.states();
    if let Some(code) = states[0].setup_code() {
        info!(
            "首次设置管理员密码需要初始化码: {}（可在应用日志中查看）",
            code
        );
    }
    let app = if config.pairs.is_empty() {
        create_router(states[0].clone())
    } else {
        create_multi_router(states)
    };
    info!(
        "Web Dashboard running at http://{} (local: http://127.0.0.1:{})",
        addr, config.server.port
    );
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
        reason = shutdown_signal() => reason.map(str::to_string),
        result = &mut web_task => Err(anyhow::anyhow!("Web server stopped: {:?}", result)),
        message = runtime.wait_for_exit() => Err(anyhow::anyhow!(message)),
    };
    info!("Stopping bot: {:?}", reason);
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
    fn multiple_cli_symbols_share_overrides_and_keep_individual_grid_settings() {
        let args = CliArgs::parse_from([
            "bot",
            "--symbol",
            "solusdc,BTCUSDT",
            "--symbol",
            "ETHUSDT",
            "--amount",
            "25",
        ]);
        let mut config = AppConfig::default();
        let mut btc = TradingPairConfig::new("BTCUSDT".into());
        btc.grid = Some(GridConfig {
            grid_interval: dec!(10),
            ..Default::default()
        });
        config.pairs.push(btc);
        assert!(apply_cli_overrides(&mut config, &args));
        let strategies = config.strategy_configs().unwrap();
        assert_eq!(
            strategies
                .iter()
                .map(|c| c.exchange.symbol.as_str())
                .collect::<Vec<_>>(),
            ["SOLUSDC", "BTCUSDT", "ETHUSDT"]
        );
        assert_eq!(strategies[1].grid.grid_interval, dec!(10));
        assert!(strategies
            .iter()
            .all(|c| c.grid.order_amount_usdc == dec!(25)));
        config.validate().unwrap();
    }

    #[test]
    fn single_symbol_cli_remains_compatible() {
        let args = CliArgs::parse_from(["bot", "--symbol", "BTCUSDT", "--interval", "10"]);
        let mut config = AppConfig::default();
        apply_cli_overrides(&mut config, &args);
        assert!(config.pairs.is_empty());
        assert_eq!(config.exchange.symbol, "BTCUSDT");
        assert_eq!(config.grid.grid_interval, dec!(10));
    }

    #[test]
    fn startup_cli_wins_over_env_and_resolves_the_selected_pairs_env() {
        let args = CliArgs::parse_from(["bot", "--symbol", "BTCUSDT", "--amount", "75"]);
        let mut config = AppConfig::default();
        let values = [
            ("TRADING_PAIRS", "SOLUSDC,ETHUSDT"),
            ("GRID_ORDER_AMOUNT_USDC", "50"),
            ("PAIR_BTCUSDT_GRID_INTERVAL", "10"),
            ("PAIR_BTCUSDT_GRID_ORDER_AMOUNT_USDC", "100"),
            ("SERVER_HOST", "127.0.0.1"),
        ]
        .map(|(key, value)| (key.into(), value.into()))
        .into();
        assert!(apply_startup_overrides(&mut config, &args, &values).unwrap());
        let strategies = config.strategy_configs().unwrap();
        assert_eq!(strategies.len(), 1);
        assert_eq!(strategies[0].exchange.symbol, "BTCUSDT");
        assert_eq!(strategies[0].grid.grid_interval, dec!(10));
        assert_eq!(strategies[0].grid.order_amount_usdc, dec!(75));
        assert_eq!(config.server.host, "127.0.0.1");
    }
}
