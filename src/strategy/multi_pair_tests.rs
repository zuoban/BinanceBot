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
async fn periodic_reconciliation_preserves_critical_budget_for_cancellation() {
    use crate::exchange::rate_limit::{RateLimiter, RequestClass};
    use axum::{routing::get, Json, Router};
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let canceled = Arc::new(AtomicBool::new(false));
    let listings = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route(
            "/fapi/v1/openOrders",
            get({
                let canceled = canceled.clone();
                let listings = listings.clone();
                move || {
                    let canceled = canceled.clone();
                    let listings = listings.clone();
                    async move {
                        listings.fetch_add(1, Ordering::SeqCst);
                        if canceled.load(Ordering::SeqCst) {
                            Json(json!([]))
                        } else {
                            Json(json!([{
                                "orderId": 123, "clientOrderId": "gb_test",
                                "symbol": "SOLUSDC", "status": "NEW", "price": "100",
                                "origQty": "1", "executedQty": "0", "side": "BUY",
                                "type": "LIMIT", "timeInForce": "GTX", "updateTime": 0
                            }]))
                        }
                    }
                }
            }),
        )
        .route(
            "/fapi/v1/order",
            axum::routing::delete({
                let canceled = canceled.clone();
                move || {
                    let canceled = canceled.clone();
                    async move {
                        canceled.store(true, Ordering::SeqCst);
                        Json(json!({}))
                    }
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (mut engine, _, _) = engines().await;
    engine.state.config.write().await.exchange.dry_run = false;
    let limiter = Arc::new(RateLimiter::default());
    limiter
        .reserve(&url, [0; 32], 1800, RequestClass::Normal)
        .unwrap();
    engine.client = Arc::new(BinanceFuturesClient::with_base_url_and_limiter(
        &engine.state.config.read().await.exchange,
        url,
        limiter,
    ));
    engine.last_orders_sync = Some(Instant::now());
    engine.sync_cycle().await;
    assert_eq!(listings.load(Ordering::SeqCst), 0);
    assert!(engine.last_orders_sync.is_none());
    assert!(engine.reconciliation_blocked);
    assert!(engine.cancel_all_orders().await);
    assert!(canceled.load(Ordering::SeqCst));
    assert_eq!(listings.load(Ordering::SeqCst), 2);
    server.abort();
}

#[tokio::test]
async fn live_pair_sync_shares_account_but_keeps_positions_and_assets_separate() {
    use axum::{extract::Query, routing::get, Json, Router};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let account_calls = Arc::new(AtomicUsize::new(0));
    let position_calls = Arc::new(AtomicUsize::new(0));
    let calls = account_calls.clone();
    let positions = position_calls.clone();
    let app = Router::new()
        .route(
            "/fapi/v2/positionRisk",
            get(move |Query(query): Query<HashMap<String, String>>| {
                let positions = positions.clone();
                async move {
                    positions.fetch_add(1, Ordering::SeqCst);
                    let symbols = match query.get("symbol") {
                        Some(symbol) => vec![symbol.as_str()],
                        None => vec!["SOLUSDC", "BTCUSDT"],
                    };
                    Json(symbols.into_iter().map(|symbol| json!({
                        "symbol": symbol, "positionSide": "BOTH",
                        "positionAmt": "0", "entryPrice": "0", "markPrice": "100",
                        "unRealizedProfit": "0", "liquidationPrice": "0", "leverage": "20"
                    })).collect::<Vec<_>>())
                }
            }),
        )
        .route(
            "/fapi/v2/account",
            get(move || {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Json(json!({
                        "totalWalletBalance": "30", "totalMarginBalance": "30",
                        "totalUnrealizedProfit": "0", "availableBalance": "30",
                        "assets": [
                            {"asset": "USDC", "walletBalance": "10", "unrealizedProfit": "0",
                             "marginBalance": "10", "availableBalance": "10"},
                            {"asset": "USDT", "walletBalance": "20", "unrealizedProfit": "0",
                             "marginBalance": "20", "availableBalance": "20"}
                        ]
                    }))
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (mut sol, mut btc, _) = engines().await;
    let cache = Arc::new(AccountCache::default());
    let position_cache = Arc::new(PositionCache::default());
    for engine in [&mut sol, &mut btc] {
        engine.client = Arc::new(BinanceFuturesClient::with_base_url(
            &engine.state.config.read().await.exchange,
            url.clone(),
        ));
        engine.set_account_cache(cache.clone());
        engine.set_position_cache(position_cache.clone());
    }
    let (sol_ok, btc_ok) = tokio::join!(
        sol.sync_account_and_position_inner(true),
        btc.sync_account_and_position_inner(true)
    );
    assert!(sol_ok && btc_ok);
    assert_eq!(account_calls.load(Ordering::SeqCst), 1);
    assert_eq!(position_calls.load(Ordering::SeqCst), 1);
    let sol_account = sol.state.account.read().await;
    let btc_account = btc.state.account.read().await;
    assert_eq!(sol_account.asset, "USDC");
    assert_eq!(sol_account.available_balance, dec!(10));
    assert_eq!(btc_account.asset, "USDT");
    assert_eq!(btc_account.available_balance, dec!(20));
    assert_eq!(sol_account.update_time, btc_account.update_time);
    assert_eq!(sol.state.position.read().await.symbol, "SOLUSDC");
    assert_eq!(btc.state.position.read().await.symbol, "BTCUSDT");
    drop(sol_account);
    drop(btc_account);
    // A critical reconciliation bypasses and invalidates the shared snapshot.
    assert!(sol.sync_account_and_position().await);
    assert_eq!(position_calls.load(Ordering::SeqCst), 2);
    assert!(btc.sync_account_and_position_inner(true).await);
    assert_eq!(position_calls.load(Ordering::SeqCst), 3);
    server.abort();
}

#[tokio::test]
async fn global_transition_cancels_old_orders_but_cannot_replenish_before_commit() {
    let (mut engine, _other, db) = engines().await;
    let state = engine.state.clone();
    engine.maintain_grid_window().await;
    assert!(!state.active_orders.read().await.is_empty());
    let gate = state.order_submission_lock.clone().write_owned().await;
    let mut global = crate::config::GlobalConfig::from_app(&*state.config.read().await);
    global.recv_window = 9000;
    let (reply, response) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::UpdateGlobalConfig {
            config: Box::new(global.clone()),
            reply,
        })
        .await;
    response.await.unwrap().unwrap();
    assert_eq!(state.config.read().await.exchange.recv_window, 9000);
    assert_eq!(
        db.load_config().unwrap().unwrap().exchange.recv_window,
        5000
    );
    assert_eq!(*state.status.read().await, BotStatus::Running);
    assert!(state.active_orders.read().await.is_empty());
    state.resume_trading().await.unwrap();
    assert!(
        !engine
            .place_grid_order(OrderSide::Buy, dec!(100), "gb_guarded_buy".into(), -1, None)
            .await
    );
    engine.maintain_grid_window().await;
    assert!(state.active_orders.read().await.is_empty());
    db.save_global_config(&global).unwrap();
    drop(gate);
    engine.maintain_grid_window().await;
    assert!(!state.active_orders.read().await.is_empty());
}

#[tokio::test]
async fn running_engine_reschedules_sync_after_global_updates_and_keeps_rejected_changes_out() {
    use axum::{routing::get, Json, Router};
    use serde_json::json;

    let (sync_tx, mut sync_rx) = mpsc::unbounded_channel();
    let app = Router::new().route(
        "/fapi/v1/premiumIndex",
        get(move || {
            let sync_tx = sync_tx.clone();
            async move {
                let _ = sync_tx.send(());
                Json(json!({"symbol": "SOLUSDC", "markPrice": "0"}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (mut engine, other_engine, db) = engines().await;
    engine.state.pause_trading().await.unwrap();
    engine
        .state
        .config
        .write()
        .await
        .exchange
        .sync_interval_secs = 1;
    let state = engine.state.clone();
    state.ticker.write().await.mark_update_time = Utc::now() - chrono::Duration::seconds(16);
    engine.client = Arc::new(BinanceFuturesClient::with_base_url(
        &state.config.read().await.exchange,
        url,
    ));
    let engine_task = tokio::spawn(async move { engine.run().await });

    let outcome = async {
        // Observe the initial real synchronization request before changing the interval.
        tokio::time::timeout(Duration::from_secs(2), sync_rx.recv())
            .await
            .unwrap()
            .unwrap();
        for seconds in [30, 1] {
            let mut config = crate::config::GlobalConfig::from_app(&*state.config.read().await);
            config.sync_interval_secs = seconds;
            let saved = config.clone();
            let (reply, response) = tokio::sync::oneshot::channel();
            state
                .action_tx
                .send(BotControlAction::UpdateGlobalConfig {
                    config: Box::new(config),
                    reply,
                })
                .await
                .unwrap();
            response.await.unwrap().unwrap();
            db.save_global_config(&saved).unwrap();
            assert_eq!(
                db.load_config()
                    .unwrap()
                    .unwrap()
                    .strategy_configs()
                    .unwrap()[0]
                    .exchange
                    .sync_interval_secs,
                seconds
            );
            if seconds == 30 {
                assert!(
                    tokio::time::timeout(Duration::from_millis(1500), sync_rx.recv())
                        .await
                        .is_err(),
                    "the old one-second timer must stop after saving a longer interval"
                );
            } else {
                tokio::time::timeout(Duration::from_secs(2), sync_rx.recv())
                    .await
                    .unwrap()
                    .unwrap();
            }
        }

        let mut rejected = crate::config::GlobalConfig::from_app(&*state.config.read().await);
        rejected.sync_interval_secs = 0;
        let (reply, response) = tokio::sync::oneshot::channel();
        state
            .action_tx
            .send(BotControlAction::UpdateGlobalConfig {
                config: Box::new(rejected),
                reply,
            })
            .await
            .unwrap();
        assert!(response.await.unwrap().is_err());
        assert_eq!(state.config.read().await.exchange.sync_interval_secs, 1);
        tokio::time::timeout(Duration::from_secs(2), sync_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            other_engine
                .state
                .config
                .read()
                .await
                .exchange
                .sync_interval_secs,
            3
        );
    };
    // Always stop the test tasks, including when an assertion fails.
    use futures_util::FutureExt;
    let result = std::panic::AssertUnwindSafe(outcome).catch_unwind().await;
    engine_task.abort();
    let _ = engine_task.await;
    server.abort();
    let _ = server.await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
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
