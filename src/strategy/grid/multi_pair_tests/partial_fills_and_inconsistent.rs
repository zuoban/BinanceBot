use super::*;

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
