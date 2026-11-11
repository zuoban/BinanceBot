use super::*;
use crate::config::{AppConfig, TradingPairConfig};
use crate::db::Database;
use rust_decimal_macros::dec;

async fn engines() -> (GridTradingEngine, GridTradingEngine, Arc<Database>) {
    let mut config = AppConfig::default();
    config.grid.grid_interval = dec!(1);
    config.grid.buy_window = 2;
    config.grid.sell_window = 2;
    config.pairs = ["SOLUSDC", "BTCUSDT"]
        .map(|s| TradingPairConfig::new(s.into()))
        .to_vec();
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&config).unwrap();
    let commit = Arc::new(tokio::sync::Mutex::new(()));
    let account_cache = Arc::new(AccountCache::default());
    let position_cache = Arc::new(PositionCache::default());
    let market_cache = Arc::new(MarketCache::default());
    let mut engines = Vec::new();
    for (config, price) in config
        .strategy_configs()
        .unwrap()
        .into_iter()
        .zip([dec!(100.5), dec!(200.5)])
    {
        let (tx, rx) = mpsc::channel(8);
        let (_, ticker_rx) = broadcast::channel(8);
        let state = AppState::new_for_pair(config.clone(), db.clone(), tx, commit.clone());
        *state.ticker.write().await = ticker(&config.exchange.symbol, price);
        let client = Arc::new(BinanceFuturesClient::with_base_url(
            &config.exchange,
            "http://127.0.0.1:1".into(),
        ));
        engines.push(GridTradingEngine::new_with_caches(
            state,
            client,
            rx,
            ticker_rx,
            account_cache.clone(),
            position_cache.clone(),
            market_cache.clone(),
        ));
    }
    let second = engines.pop().unwrap();
    let first = engines.pop().unwrap();
    (first, second, db)
}

fn ticker(symbol: &str, price: Decimal) -> TickerInfo {
    TickerInfo {
        symbol: symbol.into(),
        last_price: price,
        mark_price: price,
        update_time: Utc::now(),
        mark_update_time: Utc::now(),
        ..Default::default()
    }
}

mod isolation_and_transitions;
mod partial_fills_and_inconsistent;
