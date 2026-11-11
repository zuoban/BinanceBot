use super::*;

#[tokio::test]
async fn corrected_same_order_side_with_fill_uses_normal_reconciliation() {
    for fully_filled in [false, true] {
        let db = database();
        let app = Router::new()
            .route("/fapi/v1/openOrders", get(|| async {
                Json(json!([{"orderId":123,"clientOrderId":"gb_side_corrected","symbol":"SOLUSDC",
                    "status":"NEW","price":"100","origQty":"2","executedQty":"0",
                    "side":"SELL","type":"LIMIT","timeInForce":"GTX"}]))
            }))
            .route("/fapi/v1/order", get(move || async move {
                Json(json!({"orderId":123,"clientOrderId":"gb_side_corrected","symbol":"SOLUSDC",
                    "status":if fully_filled {"FILLED"} else {"PARTIALLY_FILLED"}, "price":"100","origQty":"2",
                    "executedQty":if fully_filled {"2"} else {"1"}, "side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
            }))
            .route("/fapi/v2/positionRisk", get(|| async {
                Json(json!([{"symbol":"SOLUSDC","positionAmt":"2","entryPrice":"100","markPrice":"100",
                    "unRealizedProfit":"0","liquidationPrice":"0","leverage":"20"}]))
            }))
            .route("/fapi/v2/account", get(|| async {
                Json(json!({"totalWalletBalance":"1000","totalMarginBalance":"1000","totalUnrealizedProfit":"0",
                    "availableBalance":"1000","assets":[{"asset":"USDC","walletBalance":"1000","marginBalance":"1000",
                    "availableBalance":"1000","unrealizedProfit":"0"}]}))
            }))
            .route("/fapi/v1/userTrades", get(|| async { Json(json!([])) }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut engine = engine(db.clone(), &url).await;
        let expected = GridOrder {
            client_order_id: "gb_side_corrected".into(),
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
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        db.save_managed_order(&expected).unwrap();
        let account = engine.client.account_identity_key();
        db.save_identity_observation(
            account,
            &expected,
            &crate::types::ObservedOrderIdentity {
                client_order_id: expected.client_order_id.clone(),
                symbol: expected.symbol.clone(),
                side: "SELL".into(),
                order_id: 123,
            },
        )
        .unwrap();
        assert!(engine.sync_live_orders().await);
        assert_eq!(engine.state.position.read().await.size, dec!(2));
        if fully_filled {
            assert!(!engine
                .state
                .active_orders
                .read()
                .await
                .contains_key("gb_side_corrected"));
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        } else {
            let active = engine.state.active_orders.read().await;
            assert_eq!(active["gb_side_corrected"].quantity, dec!(1));
            assert_eq!(
                active["gb_side_corrected"].status,
                OrderStatus::PartiallyFilled
            );
        }
        assert!(db
            .load_identity_observations(account, "SOLUSDC")
            .unwrap()
            .is_empty());
        server.abort();
    }
}

#[tokio::test]
async fn changed_window_restores_all_identity_observations_and_accepts_corrected_side_terminal() {
    let db = database();
    let stage = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get({
            let stage = stage.clone();
            move || { let stage = stage.clone(); async move {
                if stage.load(Ordering::SeqCst) > 0 { return Json(json!([])); }
                Json(json!([{"orderId":789,"clientOrderId":"gb_many_conflicts","symbol":"SOLUSDC",
                    "status":"NEW","price":"100","origQty":"1","executedQty":"0",
                    "side":"BUY","type":"LIMIT","timeInForce":"GTX"}]))
            }}
        }))
        .route("/fapi/v1/order", get({
            let stage = stage.clone();
            move |Query(query): Query<HashMap<String, String>>| {
                let stage = stage.clone();
                async move {
                    let id = query.get("orderId").and_then(|s| s.parse::<i64>().ok()).unwrap_or(123);
                    let status = if stage.load(Ordering::SeqCst) == 0 { "FILLED" } else { "CANCELED" };
                    Json(json!({"orderId":id,"clientOrderId":"gb_many_conflicts","symbol":"SOLUSDC",
                        "status":status,"price":"100","origQty":"1","executedQty":"0",
                        "side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
                }
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut first = engine(db.clone(), &url).await;
    let expected = GridOrder {
        client_order_id: "gb_many_conflicts".into(),
        order_id: Some(123),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -1,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    db.save_managed_order(&expected).unwrap();
    let account = first.client.account_identity_key();
    db.save_identity_observation(
        account,
        &expected,
        &crate::types::ObservedOrderIdentity {
            client_order_id: expected.client_order_id.clone(),
            symbol: expected.symbol.clone(),
            side: "SELL".into(),
            order_id: 456,
        },
    )
    .unwrap();
    assert!(!first.sync_live_orders().await);
    let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
    assert_eq!(observations.len(), 2);
    assert!(observations.iter().any(|(_, item)| item.order_id == 789));
    drop(first);
    stage.store(1, Ordering::SeqCst);
    let mut restarted = engine(db.clone(), &url).await;
    let mut exchange = restarted.state.config.read().await.exchange.clone();
    exchange.recv_window += 1000;
    restarted.state.config.write().await.exchange.recv_window = exchange.recv_window;
    restarted.client = Arc::new(BinanceFuturesClient::with_base_url(&exchange, url));
    assert_eq!(restarted.client.account_identity_key(), account);
    assert!(restarted.sync_live_orders().await);
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    server.abort();
}

#[tokio::test]
async fn identity_conflict_can_resolve_through_confirmed_terminal_lookup() {
    for filled in [false, true] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let mut engine = engine(db.clone(), &url).await;
        let order = GridOrder {
            client_order_id: "gb_terminal_identity".into(),
            order_id: Some(123),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: -1,
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
            .insert(order.client_order_id.clone(), order.clone());
        let key = (
            engine.client.account_identity_key(),
            order.client_order_id.clone(),
        );
        engine.evidence.mark_provisional(key.0, &key.1);
        engine.evidence.insert_conflict(key.0, order.clone());
        fake.orders.lock().unwrap().insert(
            order.client_order_id.clone(),
            BinanceOrderResponse {
                order_id: 123,
                client_order_id: order.client_order_id.clone(),
                symbol: order.symbol.clone(),
                status: if filled { "FILLED" } else { "CANCELED" }.into(),
                price: order.price,
                avg_price: filled.then_some(order.price),
                orig_qty: order.quantity,
                executed_qty: if filled {
                    order.quantity
                } else {
                    Decimal::ZERO
                },
                side: "BUY".into(),
                order_type: "LIMIT".into(),
                time_in_force: "GTX".into(),
                update_time: None,
            },
        );
        assert!(engine.sync_live_orders().await);
        assert!(engine.state.active_orders.read().await.is_empty());
        assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
        assert_eq!(db.get_recent_trades(10).unwrap().len(), usize::from(filled));
        assert!(engine.evidence.order_identity_conflicts.is_empty());
        server.abort();
    }
}

#[tokio::test]
async fn cancellation_confirmation_rejects_mismatched_identity() {
    for known_id in [false, true] {
        for mismatch in ["client", "symbol", "side", "order_id"] {
            if !known_id && mismatch == "order_id" {
                continue;
            }
            let db = database();
            let lookups = Arc::new(AtomicU8::new(0));
            let app = Router::new().route("/fapi/v1/order", get({
                let lookups = lookups.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    let lookups = lookups.clone();
                    async move {
                        if known_id {
                            assert_eq!(query.get("orderId").map(String::as_str), Some("123"));
                        } else {
                            assert_eq!(query.get("origClientOrderId").map(String::as_str), Some("gb_cancel_mismatch"));
                        }
                        lookups.fetch_add(1, Ordering::SeqCst);
                        Json(json!({"orderId":if mismatch == "order_id" {456} else {123},
                            "clientOrderId":if mismatch == "client" {"gb_other"} else {"gb_cancel_mismatch"},
                            "symbol":if mismatch == "symbol" {"BTCUSDC"} else {"SOLUSDC"},
                            "side":if mismatch == "side" {"SELL"} else {"BUY"},
                            "status":"CANCELED","price":"100","origQty":"1","executedQty":"0",
                            "type":"LIMIT","timeInForce":"GTX"}))
                    }
                }
            }).delete(|| async {
                (StatusCode::BAD_REQUEST, Json(json!({"code":-2011,"msg":"unknown order"})))
            }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let order = GridOrder {
                client_order_id: "gb_cancel_mismatch".into(),
                order_id: known_id.then_some(123),
                symbol: "SOLUSDC".into(),
                side: OrderSide::Buy,
                price: dec!(100),
                quantity: dec!(1),
                amount_usdc: dec!(100),
                status: OrderStatus::New,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                grid_level: -1,
                paired_client_order_id: None,
                is_take_profit: false,
                purpose: crate::types::OrderPurpose::Grid,
                merge_sources: Vec::new(),
            };
            db.save_managed_order(&order).unwrap();
            let mut engine = engine(db.clone(), &url).await;
            let account = engine.client.account_identity_key();
            assert!(
                !engine.cancel_single_order(&order).await,
                "known_id={known_id}, mismatch={mismatch}"
            );
            assert!(lookups.load(Ordering::SeqCst) > 0);
            assert!(engine.reconciliation_blocked);
            assert!(engine.last_orders_sync.is_none());
            assert!(!engine.state.global_position_ready.load(Ordering::SeqCst));
            let active = engine.state.active_orders.read().await;
            let retained = &active[&order.client_order_id];
            assert_eq!(retained.order_id, order.order_id);
            assert_eq!(retained.symbol, order.symbol);
            assert_eq!(retained.side, order.side);
            assert_eq!(retained.quantity, order.quantity);
            drop(active);
            let saved = db.load_managed_orders("SOLUSDC").unwrap();
            assert_eq!(saved.len(), 1);
            assert_eq!(saved[0].order_id, order.order_id);
            assert_eq!(saved[0].symbol, order.symbol);
            assert_eq!(saved[0].side, order.side);
            assert!(db.get_recent_trades(10).unwrap().is_empty());
            assert!(!db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .is_empty());
            server.abort();
        }
    }
}

#[tokio::test]
async fn terminal_lookup_rejects_mismatched_order_identity() {
    for known_id in [false, true] {
        for mismatch in ["client", "symbol", "side", "order_id"] {
            if !known_id && mismatch == "order_id" {
                continue;
            }
            let db = database();
            let lookups = Arc::new(AtomicU8::new(0));
            let app = Router::new()
                .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
                .route("/fapi/v1/order", get({
                    let lookups = lookups.clone();
                    move |Query(query): Query<HashMap<String, String>>| {
                        let lookups = lookups.clone();
                        async move {
                            if known_id {
                                assert_eq!(query.get("orderId").map(String::as_str), Some("123"));
                            } else {
                                assert_eq!(query.get("origClientOrderId").map(String::as_str), Some("gb_terminal_mismatch"));
                            }
                            lookups.fetch_add(1, Ordering::SeqCst);
                            Json(json!({"orderId":if mismatch == "order_id" {456} else {123},
                                "clientOrderId":if mismatch == "client" {"gb_other"} else {"gb_terminal_mismatch"},
                                "symbol":if mismatch == "symbol" {"BTCUSDC"} else {"SOLUSDC"},
                                "side":if mismatch == "side" {"SELL"} else {"BUY"},
                                "status":"CANCELED","price":"100","origQty":"1","executedQty":"0",
                                "type":"LIMIT","timeInForce":"GTX"}))
                        }
                    }
                }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let order = GridOrder {
                client_order_id: "gb_terminal_mismatch".into(),
                order_id: known_id.then_some(123),
                symbol: "SOLUSDC".into(),
                side: OrderSide::Buy,
                price: dec!(100),
                quantity: dec!(1),
                amount_usdc: dec!(100),
                status: OrderStatus::New,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                grid_level: -1,
                paired_client_order_id: None,
                is_take_profit: false,
                purpose: crate::types::OrderPurpose::Grid,
                merge_sources: Vec::new(),
            };
            db.save_managed_order(&order).unwrap();
            let mut engine = engine(db.clone(), &url).await;
            assert!(
                !engine.sync_live_orders().await,
                "known_id={known_id}, mismatch={mismatch}"
            );
            assert!(lookups.load(Ordering::SeqCst) > 0);
            assert!(engine.reconciliation_blocked);
            assert!(engine.last_orders_sync.is_none());
            assert!(!engine.state.global_position_ready.load(Ordering::SeqCst));
            let active = engine.state.active_orders.read().await;
            let retained = &active[&order.client_order_id];
            assert_eq!(retained.order_id, order.order_id);
            assert_eq!(retained.side, order.side);
            assert_eq!(retained.symbol, order.symbol);
            assert_eq!(retained.quantity, order.quantity);
            drop(active);
            let saved = db.load_managed_orders("SOLUSDC").unwrap();
            assert_eq!(saved.len(), 1);
            assert_eq!(saved[0].order_id, order.order_id);
            assert_eq!(saved[0].side, order.side);
            assert_eq!(saved[0].symbol, order.symbol);
            assert!(db.get_recent_trades(10).unwrap().is_empty());
            server.abort();
        }
    }
}

#[tokio::test]
async fn conflicting_duplicate_open_orders_block_in_both_snapshot_orders_and_after_restart() {
    for known_id in [false, true] {
        for reversed in [false, true] {
            for mismatch in ["order_id", "side", "symbol"] {
                let db = database();
                let good = json!({"orderId":123,"clientOrderId":"gb_duplicate_snapshot","symbol":"SOLUSDC",
                    "status":"NEW","price":"100","origQty":"1","executedQty":"0",
                    "side":"BUY","type":"LIMIT","timeInForce":"GTX"});
                let bad = json!({"orderId":if mismatch == "order_id" {456} else {123},
                    "clientOrderId":"gb_duplicate_snapshot",
                    "symbol":if mismatch == "symbol" {"BTCUSDC"} else {"SOLUSDC"},
                    "status":"NEW","price":"100","origQty":"1","executedQty":"0",
                    "side":if mismatch == "side" {"SELL"} else {"BUY"},
                    "type":"LIMIT","timeInForce":"GTX"});
                let snapshot = if reversed {
                    vec![bad, good]
                } else {
                    vec![good, bad]
                };
                let app = Router::new()
                    .route(
                        "/fapi/v1/openOrders",
                        get(move || {
                            let snapshot = snapshot.clone();
                            async move { Json(snapshot) }
                        }),
                    )
                    .route(
                        "/fapi/v1/order",
                        get(|| async {
                            (
                                StatusCode::SERVICE_UNAVAILABLE,
                                Json(json!({"code":-1007,"msg":"unavailable"})),
                            )
                        }),
                    );
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let url = format!("http://{}", listener.local_addr().unwrap());
                let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
                let order = GridOrder {
                    client_order_id: "gb_duplicate_snapshot".into(),
                    order_id: known_id.then_some(123),
                    symbol: "SOLUSDC".into(),
                    side: OrderSide::Buy,
                    price: dec!(100),
                    quantity: dec!(1),
                    amount_usdc: dec!(100),
                    status: OrderStatus::New,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    grid_level: -1,
                    paired_client_order_id: None,
                    is_take_profit: false,
                    purpose: crate::types::OrderPurpose::Grid,
                    merge_sources: Vec::new(),
                };
                db.save_managed_order(&order).unwrap();
                let mut first = engine(db.clone(), &url).await;
                let account = first.client.account_identity_key();
                assert!(
                    !first.sync_live_orders().await,
                    "known_id={known_id}, reversed={reversed}, mismatch={mismatch}"
                );
                for mut current in [first, engine(db.clone(), &url).await] {
                    assert!(!current.sync_live_orders().await);
                    assert!(current.reconciliation_blocked);
                    assert!(current.last_orders_sync.is_none());
                    assert!(!current.state.global_position_ready.load(Ordering::SeqCst));
                    let active = current.state.active_orders.read().await;
                    assert_eq!(active[&order.client_order_id].order_id, order.order_id);
                    assert_eq!(active[&order.client_order_id].side, order.side);
                    assert_eq!(active[&order.client_order_id].symbol, order.symbol);
                    drop(active);
                    let saved = db.load_managed_orders("SOLUSDC").unwrap();
                    assert_eq!(saved.len(), 1);
                    assert_eq!(saved[0].order_id, order.order_id);
                    let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
                    assert!(observations.iter().any(|(_, observed)| {
                        observed.client_order_id == order.client_order_id
                            && observed.order_id == if mismatch == "order_id" { 456 } else { 123 }
                            && observed.symbol
                                == if mismatch == "symbol" {
                                    "BTCUSDC"
                                } else {
                                    "SOLUSDC"
                                }
                            && observed.side == if mismatch == "side" { "SELL" } else { "BUY" }
                    }));
                }
                server.abort();
            }
        }
    }
}

#[tokio::test]
async fn partial_snapshot_cannot_be_overwritten_by_older_single_order_queries() {
    let db = database();
    let stage = Arc::new(AtomicU8::new(0));
    let queries = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get({
            let stage = stage.clone();
            move || { let stage = stage.clone(); async move {
                if stage.load(Ordering::SeqCst) != 0 { return Json(json!([])); }
                Json(json!([{"orderId":123,"clientOrderId":"gb_partial_snapshot","symbol":"SOLUSDC",
                    "status":"PARTIALLY_FILLED","price":"100","avgPrice":"100","origQty":"2","executedQty":"0.5",
                    "updateTime":2000,"side":"BUY","type":"LIMIT","timeInForce":"GTX"}]))
            }}
        }))
        .route("/fapi/v1/order", get({
            let queries = queries.clone();
            move || { let queries = queries.clone(); async move {
                queries.fetch_add(1, Ordering::SeqCst);
                Json(json!({"orderId":123,"clientOrderId":"gb_partial_snapshot","symbol":"SOLUSDC",
                    "status":"NEW","price":"100","avgPrice":"100","origQty":"2","executedQty":"0",
                    "updateTime":1000,"side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
            }}
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let expected = GridOrder {
        client_order_id: "gb_partial_snapshot".into(),
        order_id: None,
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(2),
        amount_usdc: dec!(200),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -1,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    db.save_managed_order(&expected).unwrap();
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    db.save_identity_observation(
        account,
        &expected,
        &crate::types::ObservedOrderIdentity {
            client_order_id: expected.client_order_id.clone(),
            symbol: expected.symbol.clone(),
            side: "SELL".into(),
            order_id: 123,
        },
    )
    .unwrap();
    assert!(!first.sync_live_orders().await);
    assert!(first.reconciliation_blocked);
    let responses = db
        .load_initial_order_responses(account, "SOLUSDC", &expected.client_order_id)
        .unwrap();
    assert!(responses.iter().any(|response| response.order_id == 123
        && response.status == "PARTIALLY_FILLED"
        && response.executed_qty == dec!(0.5)
        && response.update_time == Some(2000)));
    drop(first);
    stage.store(1, Ordering::SeqCst);
    let mut restarted = engine(db.clone(), &url).await;
    for _ in 0..3 {
        assert!(!restarted.sync_live_orders().await);
        assert!(restarted.reconciliation_blocked);
        assert!(restarted
            .state
            .active_orders
            .read()
            .await
            .contains_key(&expected.client_order_id));
        assert!(db.get_recent_trades(10).unwrap().is_empty());
    }
    assert!(queries.load(Ordering::SeqCst) > 0);
    assert!(db
        .load_initial_order_responses(account, "SOLUSDC", &expected.client_order_id)
        .unwrap()
        .iter()
        .any(
            |response| response.status == "PARTIALLY_FILLED" && response.executed_qty == dec!(0.5)
        ));
    assert!(!db
        .load_terminal_order_disagreements(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    server.abort();
}
