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
        engines.push(GridTradingEngine::new(state, client, rx, ticker_rx));
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

#[tokio::test]
async fn simultaneous_fills_and_pause_are_isolated_by_pair() {
    let (mut sol, mut btc, db) = engines().await;
    tokio::join!(sol.maintain_grid_window(), btc.maintain_grid_window());
    assert_eq!(sol.state.active_orders.read().await.len(), 2);
    assert_eq!(btc.state.active_orders.read().await.len(), 2);
    // A ticker routed to the wrong engine is ignored.
    sol.handle_ticker_update(ticker("BTCUSDT", dec!(200))).await;
    assert_eq!(sol.state.ticker.read().await.last_price, dec!(100.5));
    tokio::join!(
        sol.handle_ticker_update(ticker("SOLUSDC", dec!(100))),
        btc.handle_ticker_update(ticker("BTCUSDT", dec!(200))),
    );
    for (engine, symbol) in [(&sol, "SOLUSDC"), (&btc, "BTCUSDT")] {
        let snapshot = engine.state.snapshot().await;
        assert_eq!(snapshot.stats.total_trades, 1);
        assert_eq!(snapshot.recent_trades[0].symbol, symbol);
        assert!(snapshot.active_orders.iter().all(|o| o.symbol == symbol));
        assert_eq!(
            db.get_recent_trades_for(symbol, TradingMode::Paper, 100)
                .unwrap()
                .len(),
            1
        );
    }
    sol.handle_control_action(BotControlAction::Pause).await;
    assert_eq!(*sol.state.status.read().await, BotStatus::Paused);
    assert_eq!(*btc.state.status.read().await, BotStatus::Running);
    assert!(sol.state.active_orders.read().await.is_empty());
    assert!(!btc.state.active_orders.read().await.is_empty());
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
        Some(BotStatus::Paused)
    );
    assert_eq!(
        db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
        None
    );
}

#[tokio::test]
async fn applying_two_configs_keeps_both_pairs_and_rejects_symbol_collisions() {
    let (mut sol, mut btc, db) = engines().await;
    let mut sol_config = sol.state.config.read().await.clone();
    let mut btc_config = btc.state.config.read().await.clone();
    sol_config.grid.buy_window = 3;
    btc_config.grid.buy_window = 4;
    let (sol_result, btc_result) =
        tokio::join!(sol.apply_config(sol_config), btc.apply_config(btc_config));
    sol_result.unwrap();
    btc_result.unwrap();
    let configs = db
        .load_config()
        .unwrap()
        .unwrap()
        .strategy_configs()
        .unwrap();
    assert_eq!(configs[0].grid.buy_window, 3);
    assert_eq!(configs[1].grid.buy_window, 4);
    let mut renamed = sol.state.config.read().await.clone();
    renamed.exchange.symbol = "BTCUSDT".into();
    assert!(sol
        .apply_config(renamed)
        .await
        .unwrap_err()
        .to_string()
        .contains("重命名"));
    assert_eq!(sol.state.config.read().await.exchange.symbol, "SOLUSDC");
}

#[tokio::test]
async fn position_limit_pauses_only_the_pair_that_reaches_it() {
    let (mut sol, btc, _) = engines().await;
    sol.state.config.write().await.grid.pause_on_max_position = true;
    sol.state.config.write().await.grid.max_position_usdc = Some(dec!(100));
    sol.state.position.write().await.size = dec!(1);
    assert!(sol.pause_if_position_limit_reached().await);
    assert_eq!(*sol.state.status.read().await, BotStatus::Paused);
    assert_eq!(*btc.state.status.read().await, BotStatus::Running);
}

#[tokio::test]
async fn mode_changes_keep_pause_after_database_reopen_for_live_and_testnet() {
    use axum::{routing::get, Json, Router};
    use serde_json::json;
    let app = Router::new()
        .route("/fapi/v1/time", get(|| async { Json(json!({"serverTime":Utc::now().timestamp_millis()})) }))
        .route("/fapi/v1/exchangeInfo", get(|| async {
            Json(json!({"symbols":[{"symbol":"SOLUSDC","status":"TRADING","pricePrecision":2,"quantityPrecision":2,"filters":[{"filterType":"PRICE_FILTER","minPrice":"0.01","maxPrice":"100000","tickSize":"0.01"},{"filterType":"LOT_SIZE","minQty":"0.01","maxQty":"100000","stepSize":"0.01"},{"filterType":"MIN_NOTIONAL","notional":"5"}]}]}))
        }))
        .route("/fapi/v1/ticker/price", get(|| async { Json(json!({"symbol":"SOLUSDC","price":"100","time":0})) }))
        .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
        .route("/fapi/v2/positionRisk", get(|| async {
            Json(json!([{"symbol":"SOLUSDC","positionAmt":"0","entryPrice":"0","markPrice":"100","unRealizedProfit":"0","liquidationPrice":"0","leverage":"1"}]))
        }))
        .route("/fapi/v2/account", get(|| async {
            Json(json!({"totalWalletBalance":"10000","totalMarginBalance":"10000","totalUnrealizedProfit":"0","availableBalance":"10000","assets":[{"asset":"USDC","walletBalance":"10000","marginBalance":"10000","availableBalance":"10000","unrealizedProfit":"0"}]}))
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    for (is_testnet, mode) in [(false, TradingMode::Live), (true, TradingMode::Testnet)] {
        let path = std::env::temp_dir().join(format!("mode-pause-{}.db", uuid::Uuid::new_v4()));
        let db = Arc::new(Database::open(&path).unwrap());
        let mut config = AppConfig::default();
        config.exchange.is_testnet = is_testnet;
        config.exchange.api_key = "test-key".into();
        config.exchange.api_secret = "test-secret".into();
        config.pairs = vec![TradingPairConfig::new("SOLUSDC".into())];
        db.save_config(&config).unwrap();
        // A previous unclean exit left the target mode running.
        db.save_pair_status("SOLUSDC", mode, BotStatus::Running)
            .unwrap();
        let paper = config.strategy_configs().unwrap().remove(0);
        let (tx, rx) = mpsc::channel(8);
        let (_, ticker_rx) = broadcast::channel(8);
        let state = AppState::new_for_pair(
            paper.clone(),
            db.clone(),
            tx,
            Arc::new(tokio::sync::Mutex::new(())),
        );
        state.pause_trading().await.unwrap();
        let client = Arc::new(BinanceFuturesClient::with_base_url(
            &paper.exchange,
            url.clone(),
        ));
        let mut engine = GridTradingEngine::new(state.clone(), client, rx, ticker_rx);
        let mut live = paper;
        live.exchange.dry_run = false;
        engine.apply_config(live.clone()).await.unwrap();
        assert_eq!(*state.status.read().await, BotStatus::Paused);
        // Reopen directly, without a Shutdown action that could conceal a missing status commit.
        let reopened = Arc::new(Database::open(&path).unwrap());
        let (tx, _) = mpsc::channel(8);
        let restarted = AppState::new_for_pair(
            live,
            reopened.clone(),
            tx,
            Arc::new(tokio::sync::Mutex::new(())),
        );
        assert_eq!(*restarted.status.read().await, BotStatus::Paused);
        assert_eq!(
            reopened.load_pair_status("SOLUSDC", mode).unwrap(),
            Some(BotStatus::Paused)
        );
        drop(restarted);
        drop(reopened);
        drop(engine);
        drop(state);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }
    server.abort();
}
