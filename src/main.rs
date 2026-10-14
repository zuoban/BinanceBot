use binance_grid_bot::config::AppConfig;
use binance_grid_bot::db::Database;
use binance_grid_bot::lifecycle::shutdown_signal;
use binance_grid_bot::runtime::TradingRuntime;
use binance_grid_bot::server::create_managed_router;
use clap::Parser;
use std::path::PathBuf;
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

    /// Web server dashboard host (default: 0.0.0.0 for external access)
    #[arg(long)]
    host: Option<String>,

    /// Web server dashboard port
    #[arg(short, long)]
    port: Option<u16>,
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
                .unwrap_or_else(|_| "binance_grid_bot=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    let args = CliArgs::parse();
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
    let mut runtime = TradingRuntime::start(&config, db).await?;
    let states = runtime.states();
    if let Some(code) = states[0].setup_code() {
        info!(
            "首次设置管理员密码需要初始化码: {}（可在应用日志中查看）",
            code
        );
    }
    let app = create_managed_router(runtime.handle()).await;
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
        message = runtime.run() => Err(anyhow::anyhow!(message)),
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
    fn startup_preserves_saved_settings_and_upgrades_single_pair() {
        let db = Database::open(":memory:").unwrap();
        let mut saved = AppConfig::default();
        saved.exchange.symbol = "BTCUSDT".into();
        saved.exchange.api_key = "saved-key".into();
        saved.grid.grid_interval = dec!(10);
        db.save_config(&saved).unwrap();
        let config = load_saved_config(&db).unwrap();
        let strategies = config.strategy_configs().unwrap();
        assert_eq!(strategies.len(), 1);
        assert_eq!(strategies[0].exchange.symbol, "BTCUSDT");
        assert_eq!(strategies[0].exchange.api_key, "saved-key");
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
