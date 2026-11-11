use super::*;

#[tokio::test]
async fn recovery_intent_in_conflicting_snapshot_is_available_to_cancel_fallback() {
    for remainder in [false, true] {
        let db = database();
        let stage = Arc::new(AtomicU8::new(0));
        let listing_failed = Arc::new(AtomicBool::new(false));
        let fallback_ids = Arc::new(Mutex::new(Vec::<String>::new()));
        let app = Router::new()
            .route("/fapi/v1/openOrders", get({
                let stage = stage.clone();
                let listing_failed = listing_failed.clone();
                move || { let stage = stage.clone(); let listing_failed = listing_failed.clone(); async move {
                    if stage.load(Ordering::SeqCst) != 0 {
                        listing_failed.store(true, Ordering::SeqCst);
                        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unavailable"})));
                    }
                    (StatusCode::OK, Json(json!([
                        {"orderId":11,"clientOrderId":"gb_intent_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                        {"orderId":12,"clientOrderId":"gb_intent_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                        {"orderId":99,"clientOrderId":"gb_intent_known","symbol":"SOLUSDC","status":"NEW","price":"101","origQty":"2","executedQty":"0","side":"SELL","type":"LIMIT","timeInForce":"GTX"}
                    ])))
                }}
            }))
            .route("/fapi/v1/order", get(|| async {
                (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unavailable"})))
            }).delete({
                let listing_failed = listing_failed.clone();
                let fallback_ids = fallback_ids.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    let listing_failed = listing_failed.clone();
                    let fallback_ids = fallback_ids.clone();
                    async move {
                        if listing_failed.load(Ordering::SeqCst) {
                            if let Some(id) = query.get("orderId") { fallback_ids.lock().unwrap().push(id.clone()); }
                        }
                        (StatusCode::BAD_REQUEST, Json(json!({"code":-2011,"msg":"unknown order"})))
                    }
                }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let intent = GridOrder {
            client_order_id: "gb_intent_known".into(),
            order_id: Some(99),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(2),
            amount_usdc: dec!(202),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 3,
            paired_client_order_id: Some("gb_intent_parent".into()),
            is_take_profit: false,
            purpose: if remainder {
                crate::types::OrderPurpose::Remainder
            } else {
                crate::types::OrderPurpose::Grid
            },
            merge_sources: Vec::new(),
        };
        if remainder {
            db.save_remainder_plan(&RemainderPlan {
                symbol: "SOLUSDC".into(),
                mode: TradingMode::Live,
                sources: Vec::new(),
                target: intent.clone(),
                phase: RemainderPhase::Submitting,
            })
            .unwrap();
        } else {
            let trade = TradeRecord {
                trade_id: "grid:gb_intent_parent".into(),
                client_order_id: "gb_intent_parent".into(),
                symbol: "SOLUSDC".into(),
                mode: TradingMode::Live,
                side: OrderSide::Buy,
                price: dec!(100),
                quantity: dec!(2),
                amount_usdc: dec!(200),
                realized_pnl: dec!(0),
                commission: dec!(0),
                pnl_verified: true,
                is_maker: true,
                timestamp: Utc::now(),
                note: "fill".into(),
            };
            db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&intent), None)
                .unwrap();
        }
        let mut engine = engine(db.clone(), &url).await;
        assert!(!engine
            .state
            .active_orders
            .read()
            .await
            .contains_key("gb_intent_known"));
        assert!(!engine.sync_live_orders().await);
        let active = engine.state.active_orders.read().await;
        let restored = &active["gb_intent_known"];
        assert_eq!(restored.order_id, Some(99));
        assert_eq!(restored.quantity, dec!(2));
        assert_eq!(restored.price, dec!(101));
        assert_eq!(restored.grid_level, 3);
        assert_eq!(
            restored.paired_client_order_id.as_deref(),
            Some("gb_intent_parent")
        );
        drop(active);
        stage.store(1, Ordering::SeqCst);
        assert!(!engine.cancel_all_orders().await);
        assert!(listing_failed.load(Ordering::SeqCst));
        assert!(
            fallback_ids.lock().unwrap().iter().any(|id| id == "99"),
            "remainder={remainder}"
        );
        server.abort();
    }
}

#[tokio::test]
async fn ordinary_order_in_conflicting_snapshot_survives_restart_and_later_fill() {
    let db = database();
    let stage = Arc::new(AtomicU8::new(0));
    let normal_queries = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get({
            let stage = stage.clone();
            move || { let stage = stage.clone(); async move {
                if stage.load(Ordering::SeqCst) != 0 { return Json(json!([])); }
                Json(json!([
                    {"orderId":11,"clientOrderId":"gb_batch_conflict","symbol":"SOLUSDC","status":"NEW",
                        "price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                    {"orderId":12,"clientOrderId":"gb_batch_conflict","symbol":"SOLUSDC","status":"NEW",
                        "price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                    {"orderId":99,"clientOrderId":"gb_batch_normal","symbol":"SOLUSDC","status":"NEW",
                        "price":"95","origQty":"2","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}
                ]))
            }}
        }))
        .route("/fapi/v1/order", get({
            let normal_queries = normal_queries.clone();
            move |Query(query): Query<HashMap<String, String>>| {
                let normal_queries = normal_queries.clone();
                async move {
                    let client = query.get("origClientOrderId").map(String::as_str);
                    let id = query.get("orderId").and_then(|id| id.parse::<i64>().ok())
                        .unwrap_or(if client == Some("gb_batch_normal") {99} else {11});
                    let normal = id == 99;
                    if normal { normal_queries.fetch_add(1, Ordering::SeqCst); }
                    Json(json!({"orderId":id,"clientOrderId":if normal {"gb_batch_normal"} else {"gb_batch_conflict"},
                        "symbol":"SOLUSDC","status":if normal {"FILLED"} else if id == 12 {"CANCELED"} else {"NEW"},
                        "price":if normal {"95"} else {"100"},"avgPrice":if normal {"95"} else {"0"},
                        "origQty":if normal {"2"} else {"1"},"executedQty":if normal {"2"} else {"0"},
                        "side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
                }
            }
        }))
        .route("/fapi/v2/positionRisk", get(|| async {
            Json(json!([{"symbol":"SOLUSDC","positionAmt":"2","entryPrice":"95","markPrice":"100",
                "unRealizedProfit":"10","liquidationPrice":"0","leverage":"1"}]))
        }))
        .route("/fapi/v2/account", get(|| async {
            Json(json!({"totalWalletBalance":"1000","totalMarginBalance":"1000","totalUnrealizedProfit":"0",
                "availableBalance":"1000","assets":[{"asset":"USDC","walletBalance":"1000","marginBalance":"1000",
                "unrealizedProfit":"0","availableBalance":"1000"}]}))
        }))
        .route("/fapi/v1/userTrades", get(|| async {
            Json(json!([{"id":1,"orderId":99,"qty":"2","realizedPnl":"0","commission":"0.01",
                "commissionAsset":"USDC","maker":true}]))
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    assert!(!first.sync_live_orders().await);
    assert!(first.reconciliation_blocked);
    let active = first.state.active_orders.read().await;
    let normal = &active["gb_batch_normal"];
    assert_eq!(normal.quantity, dec!(2));
    assert_eq!(normal.price, dec!(95));
    assert_eq!(normal.amount_usdc, dec!(190));
    drop(active);
    let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
    assert!(observations
        .iter()
        .any(
            |(expected, observed)| expected.client_order_id == "gb_batch_normal"
                && expected.quantity == dec!(2)
                && expected.price == dec!(95)
                && observed.order_id == 99
        ));
    drop(first);
    stage.store(1, Ordering::SeqCst);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(restarted.sync_live_orders().await);
    assert!(normal_queries.load(Ordering::SeqCst) > 0);
    assert!(!restarted
        .state
        .active_orders
        .read()
        .await
        .contains_key("gb_batch_normal"));
    let trades = db.get_recent_trades(10).unwrap();
    assert_eq!(trades.len(), 1);
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .iter()
        .all(|(expected, _)| expected.client_order_id != "gb_batch_normal"));
    server.abort();
}

#[tokio::test]
async fn mixed_snapshot_persistence_failure_retains_all_observed_reservations_in_memory() {
    let dir = crate::test_support::TempTestDir::new("mixed-snapshot");
    let path = dir.join("mixed-snapshot.db");
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_mixed_observations BEFORE INSERT ON order_identity_observations BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    let app = Router::new().route("/fapi/v1/openOrders", get(|| async {
        Json(json!([
            {"orderId":11,"clientOrderId":"gb_failed_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
            {"orderId":12,"clientOrderId":"gb_failed_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
            {"orderId":99,"clientOrderId":"gb_failed_normal","symbol":"SOLUSDC","status":"NEW","price":"95","origQty":"2","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}
        ]))
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut engine = engine(db.clone(), &url).await;
    assert!(!engine.sync_live_orders().await);
    assert!(engine.reconciliation_blocked);
    assert!(engine.last_orders_sync.is_none());
    let active = engine.state.active_orders.read().await;
    assert_eq!(active["gb_failed_normal"].quantity, dec!(2));
    assert_eq!(active["gb_failed_normal"].price, dec!(95));
    assert_eq!(active["gb_failed_normal"].amount_usdc, dec!(190));
    assert!(active.contains_key("gb_failed_conflict"));
    drop(active);
    server.abort();
    drop(connection);
    drop(engine);
    drop(db);
}

#[tokio::test]
async fn known_id_does_not_bypass_unique_lookup_for_unverified_order() {
    let db = database();
    let stage = Arc::new(AtomicU8::new(0));
    let client_lookups = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
        .route("/fapi/v1/order", get({
            let stage = stage.clone();
            let client_lookups = client_lookups.clone();
            move |Query(query): Query<HashMap<String, String>>| {
                let stage = stage.clone();
                let client_lookups = client_lookups.clone();
                async move {
                    if query.contains_key("origClientOrderId") {
                        client_lookups.fetch_add(1, Ordering::SeqCst);
                        if stage.load(Ordering::SeqCst) == 0 {
                            return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unavailable"})));
                        }
                    }
                    (StatusCode::OK, Json(json!({"orderId":123,"clientOrderId":"gb_known_unverified",
                        "symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0",
                        "side":"BUY","type":"LIMIT","timeInForce":"GTX"})))
                }
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut engine = engine(db.clone(), &url).await;
    let account = engine.client.account_identity_key();
    let expected = GridOrder {
        client_order_id: "gb_known_unverified".into(),
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
        purpose: crate::types::OrderPurpose::Unverified,
        merge_sources: Vec::new(),
    };
    db.save_identity_observation(
        account,
        &expected,
        &crate::types::ObservedOrderIdentity {
            client_order_id: expected.client_order_id.clone(),
            symbol: expected.symbol.clone(),
            side: "BUY".into(),
            order_id: 123,
        },
    )
    .unwrap();
    assert!(!engine.sync_live_orders().await);
    assert!(client_lookups.load(Ordering::SeqCst) > 0);
    assert!(engine.reconciliation_blocked);
    assert!(!db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    stage.store(1, Ordering::SeqCst);
    assert!(engine.sync_live_orders().await);
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    let active = engine.state.active_orders.read().await;
    assert_eq!(active["gb_known_unverified"].order_id, Some(123));
    assert_eq!(
        active["gb_known_unverified"].purpose,
        crate::types::OrderPurpose::Legacy
    );
    server.abort();
}
