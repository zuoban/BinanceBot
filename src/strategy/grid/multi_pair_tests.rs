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
    engine.state.config.write().await.exchange.api_key = "live-key".into();
    engine.state.config.write().await.exchange.api_secret = "live-secret".into();
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
    engine.sync_cycle(true).await;
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
    let available_usdc = Arc::new(AtomicUsize::new(10));
    let position_calls = Arc::new(AtomicUsize::new(0));
    let calls = account_calls.clone();
    let balance = available_usdc.clone();
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
                let balance = balance.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let available = balance.load(Ordering::SeqCst).to_string();
                    Json(json!({
                        "totalWalletBalance": "30", "totalMarginBalance": "30",
                        "totalUnrealizedProfit": "0", "availableBalance": "30",
                        "assets": [
                            {"asset": "USDC", "walletBalance": "10", "unrealizedProfit": "0",
                             "marginBalance": "10", "availableBalance": available},
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
    assert!(Arc::ptr_eq(&sol.account_cache, &btc.account_cache));
    assert!(Arc::ptr_eq(&sol.position_cache, &btc.position_cache));
    assert!(Arc::ptr_eq(&sol.market_cache, &btc.market_cache));
    for engine in [&mut sol, &mut btc] {
        engine.client = Arc::new(BinanceFuturesClient::with_base_url(
            &engine.state.config.read().await.exchange,
            url.clone(),
        ));
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
    available_usdc.store(4, Ordering::SeqCst);
    assert!(sol.sync_account_and_position().await);
    assert_eq!(account_calls.load(Ordering::SeqCst), 2);
    assert_eq!(sol.state.account.read().await.available_balance, dec!(4));
    assert_eq!(position_calls.load(Ordering::SeqCst), 2);
    assert!(btc.sync_account_and_position_inner(true).await);
    assert_eq!(account_calls.load(Ordering::SeqCst), 3);
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
                Json(json!({"symbol": "SOLUSDC", "markPrice": "100"}))
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
        async fn expire_mark_after_response(state: &Arc<crate::server::state::AppState>) {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let mut ticker = state.ticker.write().await;
                    if ticker.mark_price > rust_decimal::Decimal::ZERO
                        && Utc::now() - ticker.mark_update_time < chrono::Duration::seconds(8)
                    {
                        ticker.mark_update_time = Utc::now() - chrono::Duration::seconds(16);
                        return;
                    }
                    drop(ticker);
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        // Observe the initial real synchronization request before changing the interval.
        tokio::time::timeout(Duration::from_secs(2), sync_rx.recv())
            .await
            .unwrap()
            .unwrap();
        expire_mark_after_response(&state).await;
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
                // The replacement timer has a random initial offset and may fire
                // immediately. If it does, the following tick must still use
                // the longer interval rather than the old one-second period.
                if tokio::time::timeout(Duration::from_millis(1500), sync_rx.recv())
                    .await
                    .is_ok()
                {
                    expire_mark_after_response(&state).await;
                    assert!(
                        tokio::time::timeout(Duration::from_millis(1500), sync_rx.recv())
                            .await
                            .is_err(),
                        "the old one-second timer must stop after saving a longer interval"
                    );
                }
            } else {
                tokio::time::timeout(Duration::from_secs(2), sync_rx.recv())
                    .await
                    .unwrap()
                    .unwrap();
                expire_mark_after_response(&state).await;
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
async fn mode_changes_keep_pause_after_database_reopen_for_live() {
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
    let mode = TradingMode::Live;
    let path = std::env::temp_dir().join(format!("mode-pause-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    let config = AppConfig {
        pairs: vec![TradingPairConfig::new("SOLUSDC".into())],
        ..Default::default()
    };
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
    engine.mark_retry.failed();
    engine.ticker_retry.failed();
    assert!(!engine.mark_retry.ready());
    assert!(!engine.ticker_retry.ready());
    let mut live = paper;
    live.exchange.api_key = "live-key".into();
    live.exchange.api_secret = "live-secret".into();
    engine.apply_config(live.clone()).await.unwrap();
    assert!(engine.mark_retry.ready());
    assert!(engine.ticker_retry.ready());
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
    server.abort();
}

#[tokio::test]
async fn partial_fill_refreshes_inflight_position_and_fails_closed_on_error() {
    use axum::{http::StatusCode, routing::get, Json, Router};
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::Notify;

    let calls = Arc::new(AtomicUsize::new(0));
    let stage = Arc::new(AtomicUsize::new(1));
    let fail = Arc::new(AtomicBool::new(false));
    let account_fail = Arc::new(AtomicBool::new(false));
    let loading = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let orders_requested = Arc::new(Notify::new());
    let app = Router::new()
        .route("/fapi/v2/positionRisk", get({
            let calls = calls.clone();
            let stage = stage.clone();
            let fail = fail.clone();
            let loading = loading.clone();
            let release = release.clone();
            move || {
                let calls = calls.clone();
                let stage = stage.clone();
                let fail = fail.clone();
                let loading = loading.clone();
                let release = release.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        loading.notify_one();
                        release.notified().await;
                    }
                    if fail.load(Ordering::SeqCst) {
                        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({})));
                    }
                    let size = if call == 0 { "0" } else if stage.load(Ordering::SeqCst) == 1 { "1" } else { "1.5" };
                    (StatusCode::OK, Json(json!([{
                        "symbol": "SOLUSDC", "positionSide": "BOTH", "positionAmt": size,
                        "entryPrice": "100", "markPrice": "100", "unRealizedProfit": "0",
                        "liquidationPrice": "0", "leverage": "20"
                    }])))
                }
            }
        }))
        .route("/fapi/v1/openOrders", get({
            let stage = stage.clone();
            let orders_requested = orders_requested.clone();
            move || {
                let stage = stage.clone();
                let orders_requested = orders_requested.clone();
                async move {
                    orders_requested.notify_one();
                    let filled = if stage.load(Ordering::SeqCst) == 1 { "1" } else { "1.5" };
                    Json(json!([{
                        "orderId": 123, "clientOrderId": "gb_partial", "symbol": "SOLUSDC",
                        "status": "PARTIALLY_FILLED", "price": "100", "origQty": "2",
                        "executedQty": filled, "side": "BUY", "type": "LIMIT", "timeInForce": "GTX"
                    }]))
                }
            }
        }))
        .route("/fapi/v2/account", get({
            let stage = stage.clone();
            let account_fail = account_fail.clone();
            move || {
                let stage = stage.clone();
                let account_fail = account_fail.clone();
                async move {
                    if account_fail.load(Ordering::SeqCst) {
                        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({})));
                    }
                    let balance = if stage.load(Ordering::SeqCst) == 1 { "1000" } else { "0" };
                    (StatusCode::OK, Json(json!({
                        "totalWalletBalance": "1000", "totalMarginBalance": "1000",
                        "totalUnrealizedProfit": "0", "availableBalance": balance,
                        "assets": [{"asset": "USDC", "walletBalance": "1000", "unrealizedProfit": "0",
                            "marginBalance": "1000", "availableBalance": balance}]
                    })))
                }
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (mut engine, _, _) = engines().await;
    engine.state.config.write().await.exchange.api_key = "live-key".into();
    engine.state.config.write().await.exchange.api_secret = "live-secret".into();
    engine.client = Arc::new(BinanceFuturesClient::with_base_url(
        &engine.state.config.read().await.exchange,
        url.clone(),
    ));
    let order = GridOrder {
        client_order_id: "gb_partial".into(),
        order_id: Some(123),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(2),
        amount_usdc: dec!(200),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 0,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Legacy,
        merge_sources: Vec::new(),
    };
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(order.client_order_id.clone(), order);
    let cache = engine.position_cache.clone();
    let client = engine.client.clone();
    let old_load = tokio::spawn(async move { cache.get(&client, "SOLUSDC").await.unwrap() });
    loading.notified().await;
    let sync = tokio::spawn(async move {
        let ok = engine.sync_live_orders().await;
        (engine, ok)
    });
    orders_requested.notified().await;
    release.notify_one();
    old_load.await.unwrap();
    let (mut engine, ok) = sync.await.unwrap();
    assert!(ok);
    let initial_calls = calls.load(Ordering::SeqCst);
    assert!(initial_calls >= 2);
    let orders: Vec<_> = engine
        .state
        .active_orders
        .read()
        .await
        .values()
        .cloned()
        .collect();
    assert_eq!(orders[0].quantity, dec!(1));
    assert_eq!(engine.state.position.read().await.size, dec!(1));
    assert_eq!(buy_exposure_usdc(dec!(1), dec!(100), &orders), dec!(200));

    engine.last_account_sync = Some(Instant::now());
    assert!(engine.sync_live_orders().await);
    assert_eq!(calls.load(Ordering::SeqCst), initial_calls);
    engine.account_cache.get(&engine.client).await.unwrap();
    assert_eq!(
        engine.state.account.read().await.available_balance,
        dec!(1000)
    );

    // Critical reconciliation must still refresh the position when the
    // ordinary request budget is exhausted.
    let limiter = Arc::new(crate::exchange::rate_limit::RateLimiter::default());
    limiter
        .reserve(
            &url,
            [0; 32],
            1800,
            crate::exchange::rate_limit::RequestClass::Normal,
        )
        .unwrap();
    engine.client = Arc::new(BinanceFuturesClient::with_base_url_and_limiter(
        &engine.state.config.read().await.exchange,
        url,
        limiter,
    ));

    stage.store(2, Ordering::SeqCst);
    account_fail.store(true, Ordering::SeqCst);
    assert!(!engine.sync_live_orders().await);
    assert_eq!(
        engine.state.active_orders.read().await["gb_partial"].quantity,
        dec!(1)
    );
    assert!(engine.last_account_sync.is_none());
    account_fail.store(false, Ordering::SeqCst);
    fail.store(true, Ordering::SeqCst);
    engine
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    assert!(!engine.sync_live_orders().await);
    assert_eq!(calls.load(Ordering::SeqCst), initial_calls + 2);
    assert_eq!(
        engine.state.active_orders.read().await["gb_partial"].quantity,
        dec!(1)
    );
    assert!(!engine.state.global_position_ready.load(Ordering::SeqCst));
    assert!(engine.state.position.read().await.update_time.is_none());
    assert!(engine.reconciliation_blocked);

    // Even when the position endpoint fails on first discovery, retain the
    // exchange order so cancellation can find it and reserve the original size.
    engine
        .state
        .active_orders
        .write()
        .await
        .remove("gb_partial");
    engine.state.db.delete_managed_order("gb_partial").unwrap();
    assert!(!engine.sync_live_orders().await);
    assert_eq!(
        engine.state.active_orders.read().await["gb_partial"].quantity,
        dec!(2)
    );

    fail.store(false, Ordering::SeqCst);
    assert!(engine.sync_live_orders().await);
    let orders: Vec<_> = engine
        .state
        .active_orders
        .read()
        .await
        .values()
        .cloned()
        .collect();
    assert_eq!(orders[0].quantity, dec!(0.5));
    assert_eq!(engine.state.position.read().await.size, dec!(1.5));
    assert_eq!(engine.state.account.read().await.available_balance, dec!(0));
    assert_eq!(buy_exposure_usdc(dec!(1.5), dec!(100), &orders), dec!(200));
    server.abort();
}

#[tokio::test]
async fn pause_during_partial_fill_refresh_keeps_discovered_order_recoverable() {
    use axum::{routing::get, Json, Router};
    use serde_json::json;
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let app = Router::new()
        .route(
            "/fapi/v1/openOrders",
            get(|| async {
                Json(
                    json!([{"orderId":123, "clientOrderId":"gb_pause_partial", "symbol":"SOLUSDC",
                "status":"PARTIALLY_FILLED", "price":"100", "origQty":"2", "executedQty":"1",
                "side":"BUY", "type":"LIMIT", "timeInForce":"GTX"}]),
                )
            }),
        )
        .route(
            "/fapi/v2/positionRisk",
            get({
                let started = started.clone();
                let release = release.clone();
                move || {
                    let started = started.clone();
                    let release = release.clone();
                    async move {
                        started.notify_one();
                        release.notified().await;
                        Json(
                            json!([{"symbol":"SOLUSDC", "positionSide":"BOTH", "positionAmt":"1",
                        "entryPrice":"100", "markPrice":"100", "unRealizedProfit":"0",
                        "liquidationPrice":"0", "leverage":"20"}]),
                        )
                    }
                }
            }),
        )
        .route(
            "/fapi/v2/account",
            get(|| async {
                Json(
                    json!({"totalWalletBalance":"1000", "totalMarginBalance":"1000",
                "totalUnrealizedProfit":"0", "availableBalance":"1000", "assets":[
                    {"asset":"USDC", "walletBalance":"1000", "marginBalance":"1000",
                     "availableBalance":"1000", "unrealizedProfit":"0"}]}),
                )
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (mut engine, _, db) = engines().await;
    engine.state.config.write().await.exchange.api_key = "live-key".into();
    engine.state.config.write().await.exchange.api_secret = "live-secret".into();
    engine.client = Arc::new(BinanceFuturesClient::with_base_url(
        &engine.state.config.read().await.exchange,
        url,
    ));
    let state = engine.state.clone();
    let sync = tokio::spawn(async move { engine.sync_live_orders().await });
    started.notified().await;
    state.pause_trading().await.unwrap();
    release.notify_one();
    assert!(!sync.await.unwrap());
    let active = state.active_orders.read().await;
    assert_eq!(active["gb_pause_partial"].quantity, dec!(2));
    drop(active);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].client_order_id, "gb_pause_partial");
    assert_eq!(saved[0].quantity, dec!(2));
    server.abort();
}

#[tokio::test]
async fn inconsistent_open_order_uses_individual_lookup_to_recover() {
    use axum::{http::StatusCode, routing::get, Json, Router};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    for corrected in ["CANCELED", "FILLED", "PARTIALLY_FILLED"] {
        let stage = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/fapi/v1/openOrders", get(|| async {
                Json(json!([{"orderId":123, "clientOrderId":"gb_inconsistent", "symbol":"SOLUSDC",
                    "status":"FILLED", "price":"100", "origQty":"2", "executedQty":"0",
                    "side":"BUY", "type":"LIMIT", "timeInForce":"GTX"}]))
            }))
            .route("/fapi/v1/order", get({
                let stage = stage.clone();
                move || {
                    let stage = stage.clone();
                    async move {
                        let current = stage.load(Ordering::SeqCst);
                        if current == 0 {
                            return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({})));
                        }
                        let status = if current == 1 { "FILLED" } else { corrected };
                        let executed = if current == 1 || status == "CANCELED" { "0" }
                            else if status == "FILLED" { "2" } else { "1" };
                        (StatusCode::OK, Json(json!({"orderId":123, "clientOrderId":"gb_inconsistent",
                            "symbol":"SOLUSDC", "status":status, "price":"100", "origQty":"2",
                            "executedQty":executed, "side":"BUY", "type":"LIMIT", "timeInForce":"GTX"})))
                    }
                }
            }))
            .route("/fapi/v2/positionRisk", get(|| async {
                Json(json!([{"symbol":"SOLUSDC", "positionSide":"BOTH", "positionAmt":"2",
                    "entryPrice":"100", "markPrice":"100", "unRealizedProfit":"0",
                    "liquidationPrice":"0", "leverage":"20"}]))
            }))
            .route("/fapi/v2/account", get(|| async {
                Json(json!({"totalWalletBalance":"1000", "totalMarginBalance":"1000",
                    "totalUnrealizedProfit":"0", "availableBalance":"1000", "assets":[
                        {"asset":"USDC", "walletBalance":"1000", "marginBalance":"1000",
                         "availableBalance":"1000", "unrealizedProfit":"0"}]}))
            }))
            .route("/fapi/v1/userTrades", get(|| async { Json(json!([])) }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (mut engine, _, db) = engines().await;
        engine.state.config.write().await.exchange.api_key = "live-key".into();
        engine.state.config.write().await.exchange.api_secret = "live-secret".into();
        engine.client = Arc::new(BinanceFuturesClient::with_base_url(
            &engine.state.config.read().await.exchange,
            url,
        ));

        assert!(!engine.sync_live_orders().await);
        assert_eq!(
            engine.state.active_orders.read().await["gb_inconsistent"].quantity,
            dec!(2)
        );
        assert_eq!(
            db.load_managed_orders("SOLUSDC").unwrap()[0].quantity,
            dec!(2)
        );
        stage.store(1, Ordering::SeqCst);
        assert!(!engine.sync_live_orders().await);
        assert_eq!(
            engine.state.active_orders.read().await["gb_inconsistent"].quantity,
            dec!(2)
        );
        stage.store(2, Ordering::SeqCst);
        assert!(engine.sync_live_orders().await);
        if corrected == "PARTIALLY_FILLED" {
            assert_eq!(
                engine.state.active_orders.read().await["gb_inconsistent"].quantity,
                dec!(1)
            );
            assert_eq!(engine.state.position.read().await.size, dec!(2));
        } else {
            assert!(!engine
                .state
                .active_orders
                .read()
                .await
                .contains_key("gb_inconsistent"));
            assert_eq!(
                db.get_recent_trades(10).unwrap().len(),
                usize::from(corrected == "FILLED")
            );
        }
        engine
            .state
            .config
            .write()
            .await
            .global_max_position_notional = Some(dec!(10000));
        engine.state.position.write().await.update_time = Some(Utc::now());
        engine
            .state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        engine
            .state
            .global_position_ready
            .store(true, Ordering::SeqCst);
        let global = crate::server::state::GlobalPositionLimit::new(Arc::new(
            tokio::sync::RwLock::new(vec![engine.state.clone()]),
        ));
        global.membership_ready.store(true, Ordering::SeqCst);
        let _ = engine
            .state
            .global_position_limit
            .set(Arc::downgrade(&global));
        // Even a previously corrected list entry can conceal a newer fill.
        assert!(!engine.sync_live_orders().await);
        assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
        assert!(!engine.state.global_position_ready.load(Ordering::SeqCst));
        stage.store(0, Ordering::SeqCst);
        assert!(!engine.sync_live_orders().await);
        assert!(!engine.state.global_position_ready.load(Ordering::SeqCst));
        server.abort();
    }
}

#[tokio::test]
async fn inconsistent_open_order_identity_conflicts_preserve_original_record() {
    use axum::{routing::get, Json, Router};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    for (id, symbol, side) in [
        (456, "SOLUSDC", "BUY"),
        (123, "BTCUSDT", "BUY"),
        (123, "SOLUSDC", "SELL"),
    ] {
        let lookups = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/fapi/v1/openOrders",
                get(move || async move {
                    Json(
                        json!([{"orderId":id, "clientOrderId":"gb_identity", "symbol":symbol,
                    "status":"FILLED", "price":"100", "origQty":"2", "executedQty":"0",
                    "side":side, "type":"LIMIT", "timeInForce":"GTX"}]),
                    )
                }),
            )
            .route(
                "/fapi/v1/order",
                get({
                    let lookups = lookups.clone();
                    move || {
                        let lookups = lookups.clone();
                        async move {
                            lookups.fetch_add(1, Ordering::SeqCst);
                            Json(json!({}))
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (mut engine, _, db) = engines().await;
        engine.state.config.write().await.exchange.api_key = "live-key".into();
        engine.state.config.write().await.exchange.api_secret = "live-secret".into();
        engine.client = Arc::new(BinanceFuturesClient::with_base_url(
            &engine.state.config.read().await.exchange,
            url,
        ));
        let original = GridOrder {
            client_order_id: "gb_identity".into(),
            order_id: Some(123),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 0,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Legacy,
            merge_sources: Vec::new(),
        };
        db.save_managed_order(&original).unwrap();
        // The record can exist only on disk when an exchange snapshot arrives.
        assert!(!engine.sync_live_orders().await);
        let saved = db.load_managed_orders("SOLUSDC").unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].order_id, Some(123));
        assert_eq!(saved[0].quantity, dec!(1));
        assert_eq!(saved[0].side, OrderSide::Buy);
        assert_eq!(
            engine.state.active_orders.read().await["gb_identity"].order_id,
            Some(123)
        );
        assert_eq!(lookups.load(Ordering::SeqCst), 0);
        let observations = db
            .load_identity_observations(engine.client.account_identity_key(), "SOLUSDC")
            .unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].1.order_id, id);
        assert_eq!(observations[0].1.symbol, symbol);
        assert_eq!(observations[0].1.side, side);
        assert!(engine.reconciliation_blocked);
        server.abort();
    }
}

#[tokio::test]
async fn inconsistent_open_order_preserves_existing_disk_order_metadata() {
    use axum::{routing::get, Json, Router};
    use serde_json::json;
    let app = Router::new().route(
        "/fapi/v1/openOrders",
        get(|| async {
            Json(
                json!([{"orderId":123, "clientOrderId":"gb_disk_metadata", "symbol":"SOLUSDC",
            "status":"FILLED", "price":"100", "origQty":"2", "executedQty":"0",
            "side":"BUY", "type":"LIMIT", "timeInForce":"GTX"}]),
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (mut engine, _, db) = engines().await;
    engine.state.config.write().await.exchange.api_key = "live-key".into();
    engine.state.config.write().await.exchange.api_secret = "live-secret".into();
    engine.client = Arc::new(BinanceFuturesClient::with_base_url(
        &engine.state.config.read().await.exchange,
        url,
    ));
    let original = GridOrder {
        client_order_id: "gb_disk_metadata".into(),
        order_id: Some(123),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(2),
        amount_usdc: dec!(200),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -3,
        paired_client_order_id: Some("gb_parent".into()),
        is_take_profit: true,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    db.save_managed_order(&original).unwrap();
    assert!(!engine.sync_live_orders().await);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    let active = engine.state.active_orders.read().await;
    for order in [&saved[0], &active["gb_disk_metadata"]] {
        assert_eq!(order.purpose, crate::types::OrderPurpose::Grid);
        assert_eq!(order.grid_level, -3);
        assert_eq!(order.paired_client_order_id.as_deref(), Some("gb_parent"));
        assert!(order.is_take_profit);
    }
    server.abort();
}

#[tokio::test]
async fn inconsistent_order_blocks_other_pairs_before_waiting_for_lookup() {
    use axum::{routing::get, Json, Router};
    use serde_json::json;
    use std::sync::atomic::Ordering;
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let app = Router::new()
        .route(
            "/fapi/v1/openOrders",
            get(|| async {
                Json(
                    json!([{"orderId":123, "clientOrderId":"gb_unknown", "symbol":"SOLUSDC",
                "status":"FILLED", "price":"100", "origQty":"2", "executedQty":"0",
                "side":"BUY", "type":"LIMIT", "timeInForce":"GTX"}]),
                )
            }),
        )
        .route(
            "/fapi/v1/order",
            get({
                let started = started.clone();
                let release = release.clone();
                move || {
                    let started = started.clone();
                    let release = release.clone();
                    async move {
                        started.notify_one();
                        release.notified().await;
                        Json(
                            json!({"orderId":123, "clientOrderId":"gb_unknown", "symbol":"SOLUSDC",
                        "status":"FILLED", "price":"100", "origQty":"2", "executedQty":"0",
                        "side":"BUY", "type":"LIMIT", "timeInForce":"GTX"}),
                        )
                    }
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (mut engine, other, db) = engines().await;
    for state in [&engine.state, &other.state] {
        let mut config = state.config.write().await;
        config.exchange.api_key = "live-key".into();
        config.exchange.api_secret = "live-secret".into();
        config.global_max_position_notional = Some(dec!(10000));
        drop(config);
        state.position.write().await.update_time = Some(Utc::now());
        state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        state.global_position_ready.store(true, Ordering::SeqCst);
    }
    let global =
        crate::server::state::GlobalPositionLimit::new(Arc::new(tokio::sync::RwLock::new(vec![
            engine.state.clone(),
            other.state.clone(),
        ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    let _ = engine
        .state
        .global_position_limit
        .set(Arc::downgrade(&global));
    let _ = other
        .state
        .global_position_limit
        .set(Arc::downgrade(&global));
    engine.client = Arc::new(BinanceFuturesClient::with_base_url(
        &engine.state.config.read().await.exchange,
        url,
    ));
    let state = engine.state.clone();
    let sync = tokio::spawn(async move { engine.sync_live_orders().await });
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    assert!(!state.global_position_ready.load(Ordering::SeqCst));
    assert_eq!(*other.state.status.read().await, BotStatus::Paused);
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].quantity,
        dec!(2)
    );
    release.notify_one();
    assert!(!sync.await.unwrap());
    server.abort();
}

#[tokio::test]
async fn inconsistent_individual_lookup_revokes_readiness_before_next_lookup() {
    use axum::{extract::Query, routing::get, Json, Router};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
        .route("/fapi/v1/order", get({
            let calls = calls.clone();
            let started = started.clone();
            let release = release.clone();
            move |Query(query): Query<HashMap<String, String>>| {
                let calls = calls.clone();
                let started = started.clone();
                let release = release.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::SeqCst);
                    if call > 0 {
                        started.notify_one();
                        release.notified().await;
                    }
                    let id: i64 = query["orderId"].parse().unwrap();
                    Json(json!({"orderId":id, "clientOrderId":format!("gb_query_{id}"), "symbol":"SOLUSDC",
                        "status":if call == 0 { "FILLED" } else { "NEW" }, "price":"100",
                        "origQty":"1", "executedQty":"0", "side":"BUY", "type":"LIMIT", "timeInForce":"GTX"}))
                }
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (mut engine, _, db) = engines().await;
    engine.state.config.write().await.exchange.api_key = "live-key".into();
    engine.state.config.write().await.exchange.api_secret = "live-secret".into();
    engine.client = Arc::new(BinanceFuturesClient::with_base_url(
        &engine.state.config.read().await.exchange,
        url,
    ));
    for id in [111_i64, 222] {
        let order = GridOrder {
            client_order_id: format!("gb_query_{id}"),
            order_id: Some(id),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 0,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        db.save_managed_order(&order).unwrap();
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(order.client_order_id.clone(), order);
    }
    engine
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    let state = engine.state.clone();
    let sync = tokio::spawn(async move { engine.sync_live_orders().await });
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    assert!(!state.global_position_ready.load(Ordering::SeqCst));
    assert_eq!(state.active_orders.read().await.len(), 2);
    release.notify_one();
    assert!(!sync.await.unwrap());
    server.abort();
}

#[tokio::test]
async fn paper_pairs_share_capital_pnl_and_position_margin() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let commit = Arc::new(tokio::sync::Mutex::new(()));
    let mut engines = Vec::new();
    for (symbol, direction, price) in [
        ("SOLUSDC", GridDirection::Long, dec!(100)),
        ("BNBUSDC", GridDirection::Short, dec!(200)),
    ] {
        let mut config = AppConfig::default();
        config.exchange.symbol = symbol.into();
        config.grid.direction = direction;
        let (tx, rx) = mpsc::channel(8);
        let (_, ticker_rx) = broadcast::channel(8);
        let state = AppState::new_for_pair(config.clone(), db.clone(), tx, commit.clone());
        *state.ticker.write().await = ticker(symbol, price);
        engines.push(GridTradingEngine::new(
            state,
            Arc::new(BinanceFuturesClient::new(&config.exchange)),
            rx,
            ticker_rx,
        ));
    }
    let mut short = engines.pop().unwrap();
    let mut long = engines.pop().unwrap();
    let fill = |id: &str, symbol: &str, side, price, quantity| GridOrder {
        client_order_id: id.into(),
        order_id: None,
        symbol: symbol.into(),
        side,
        price,
        quantity,
        amount_usdc: price * quantity,
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 0,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    let mut long_open = fill(
        "shared-long-open",
        "SOLUSDC",
        OrderSide::Buy,
        dec!(100),
        dec!(2),
    );
    let mut short_open = fill(
        "shared-short-open",
        "BNBUSDC",
        OrderSide::Sell,
        dec!(200),
        dec!(1),
    );
    let (first, second) = tokio::join!(
        long.on_order_filled(&mut long_open),
        short.on_order_filled(&mut short_open),
    );
    assert!(first && second);
    let account = long.state.snapshot().await.account;
    assert_eq!(account.total_wallet_balance, dec!(10000));
    assert_eq!(account.available_balance, dec!(9600));

    long.handle_ticker_update(ticker("SOLUSDC", dec!(110)))
        .await;
    short
        .handle_ticker_update(ticker("BNBUSDC", dec!(190)))
        .await;
    let account = short.state.snapshot().await.account;
    assert_eq!(account.unrealized_profit, dec!(30));
    assert_eq!(account.margin_balance, dec!(10030));
    assert_eq!(account.available_balance, dec!(9620));

    let mut long_close = fill(
        "shared-long-close",
        "SOLUSDC",
        OrderSide::Sell,
        dec!(110),
        dec!(1),
    );
    let mut short_close = fill(
        "shared-short-close",
        "BNBUSDC",
        OrderSide::Buy,
        dec!(190),
        dec!(1),
    );
    let (first, second) = tokio::join!(
        long.on_order_filled(&mut long_close),
        short.on_order_filled(&mut short_close),
    );
    assert!(first && second);
    assert!(short.on_order_filled(&mut short_close).await);

    let mut config = AppConfig::default();
    config.exchange.symbol = "BTCUSDC".into();
    let (tx, _) = mpsc::channel(8);
    let added = AppState::new_for_pair(config, db.clone(), tx, commit.clone());
    for state in [&long.state, &short.state, &added] {
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.account.total_wallet_balance, dec!(10020));
        assert_eq!(snapshot.account.unrealized_profit, dec!(10));
        assert_eq!(snapshot.account.margin_balance, dec!(10030));
        assert_eq!(snapshot.account.available_balance, dec!(9920));
        let frame = serde_json::to_value(state.account_snapshot().await).unwrap();
        assert_eq!(
            frame["snapshot"]["account"],
            serde_json::to_value(snapshot.account).unwrap()
        );
    }

    let mut config = AppConfig::default();
    config.exchange.symbol = "BTCUSDT".into();
    let (tx, _) = mpsc::channel(8);
    let other_asset = AppState::new_for_pair(config, db, tx, commit);
    let account = other_asset.snapshot().await.account;
    assert_eq!(account.asset, "USDT");
    assert_eq!(account.total_wallet_balance, dec!(10000));
    assert_eq!(account.available_balance, dec!(10000));
}
