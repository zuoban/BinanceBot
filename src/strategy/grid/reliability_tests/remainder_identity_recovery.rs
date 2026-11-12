use super::*;

#[tokio::test]
async fn failed_remainder_confirmation_survives_cancel_recovery_and_restart() {
    let dir = crate::test_support::TempTestDir::new("remainder-cancel-restart");
    let path = dir.join("remainder-cancel-restart.db");
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    let stage = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async {
            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unavailable"})))
        }))
        .route("/fapi/v1/order", get({
            let stage = stage.clone();
            move || { let stage = stage.clone(); async move {
                let current = stage.load(Ordering::SeqCst);
                if current == 1 {
                    return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unavailable"})));
                }
                (StatusCode::OK, Json(json!({"orderId":if current == 0 {123} else {456},
                    "clientOrderId":"gb_remainder_cancel_restart","symbol":"SOLUSDC","status":"CANCELED",
                    "price":"101","origQty":"2","executedQty":"0","side":"SELL","type":"LIMIT","timeInForce":"GTX"})))
            }}
        }).delete(|| async {
            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unavailable"})))
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let plan = RemainderPlan {
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        sources: Vec::new(),
        phase: RemainderPhase::Submitting,
        target: GridOrder {
            client_order_id: "gb_remainder_cancel_restart".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(2),
            amount_usdc: dec!(202),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Remainder,
            merge_sources: Vec::new(),
        },
    };
    db.save_remainder_plan(&plan).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_cancel_plan_confirmation BEFORE UPDATE ON remainder_plans BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let mut copy = plan.clone();
    assert!(first.adopt_remainder_target(&mut copy).await.is_err());
    drop(copy);
    assert_eq!(
        db.load_remainder_plan("SOLUSDC", TradingMode::Live)
            .unwrap()
            .unwrap()
            .target
            .order_id,
        None
    );
    assert_eq!(
        first.state.active_orders.read().await["gb_remainder_cancel_restart"].order_id,
        Some(123)
    );
    connection
        .execute_batch("DROP TRIGGER reject_cancel_plan_confirmation")
        .unwrap();
    stage.store(1, Ordering::SeqCst);
    assert!(!first.cancel_all_orders().await);
    assert_eq!(
        first.state.active_orders.read().await["gb_remainder_cancel_restart"].order_id,
        Some(123)
    );
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .iter()
        .any(
            |(expected, observed)| expected.client_order_id == "gb_remainder_cancel_restart"
                && observed.order_id == 123
        ));
    drop(first);
    stage.store(2, Ordering::SeqCst);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted.reconciliation_blocked);
    assert_eq!(
        restarted.state.active_orders.read().await["gb_remainder_cancel_restart"].order_id,
        Some(123)
    );
    let mut recovered = db
        .load_remainder_plan("SOLUSDC", TradingMode::Live)
        .unwrap()
        .unwrap();
    assert!(restarted
        .adopt_remainder_target(&mut recovered)
        .await
        .is_err());
    assert_eq!(
        restarted.state.active_orders.read().await["gb_remainder_cancel_restart"].order_id,
        Some(123)
    );
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .iter()
        .any(
            |(expected, observed)| expected.client_order_id == "gb_remainder_cancel_restart"
                && observed.order_id == 456
        ));
    server.abort();
    drop(connection);
    drop(restarted);
    drop(db);
}

#[tokio::test]
async fn remainder_confirmation_write_failure_retains_identity_barrier_after_restart() {
    let dir = crate::test_support::TempTestDir::new("remainder-confirm-fail");
    let path = dir.join("remainder-confirm-fail.db");
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    let stage = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
        .route("/fapi/v1/order", get({
            let stage = stage.clone();
            move || { let stage = stage.clone(); async move {
                Json(json!({"orderId":if stage.load(Ordering::SeqCst) == 0 {123} else {456},
                    "clientOrderId":"gb_remainder_write_fail","symbol":"SOLUSDC","status":"CANCELED",
                    "price":"101","origQty":"2","executedQty":"0","side":"SELL","type":"LIMIT","timeInForce":"GTX"}))
            }}
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let plan = RemainderPlan {
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        sources: Vec::new(),
        phase: RemainderPhase::Submitting,
        target: GridOrder {
            client_order_id: "gb_remainder_write_fail".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(2),
            amount_usdc: dec!(202),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Remainder,
            merge_sources: Vec::new(),
        },
    };
    db.save_remainder_plan(&plan).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_plan_confirmation BEFORE UPDATE ON remainder_plans BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let mut copy = plan.clone();
    assert!(first.adopt_remainder_target(&mut copy).await.is_err());
    assert!(first.reconciliation_blocked);
    assert_eq!(
        first.state.active_orders.read().await["gb_remainder_write_fail"].order_id,
        Some(123)
    );
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .iter()
        .any(
            |(expected, observed)| expected.client_order_id == "gb_remainder_write_fail"
                && observed.order_id == 123
        ));
    drop(copy);
    connection
        .execute_batch("DROP TRIGGER reject_plan_confirmation")
        .unwrap();
    let mut recovered = db
        .load_remainder_plan("SOLUSDC", TradingMode::Live)
        .unwrap()
        .unwrap();
    stage.store(1, Ordering::SeqCst);
    assert!(first.adopt_remainder_target(&mut recovered).await.is_err());
    assert_eq!(
        first.state.active_orders.read().await["gb_remainder_write_fail"].order_id,
        Some(123)
    );
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .iter()
        .any(
            |(expected, observed)| expected.client_order_id == "gb_remainder_write_fail"
                && observed.order_id == 456
        ));
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted.reconciliation_blocked);
    assert_eq!(
        restarted.state.active_orders.read().await["gb_remainder_write_fail"].order_id,
        Some(123)
    );
    server.abort();
    drop(connection);
    drop(restarted);
    drop(db);
}

#[tokio::test]
async fn cancellation_recovery_preserves_all_same_exchange_id_observations() {
    let db = database();
    let app = Router::new()
        .route(
            "/fapi/v1/openOrders",
            get(|| async {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"code":-1007,"msg":"unavailable"})),
                )
            }),
        )
        .route(
            "/fapi/v1/order",
            axum::routing::delete(|| async {
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"code":-2011,"msg":"unknown order"})),
                )
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let expected = GridOrder {
        client_order_id: "gb_cancel_merge".into(),
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
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let key = (account, expected.client_order_id.clone());
    let existing = crate::types::ObservedOrderIdentity {
        client_order_id: "gb_observed_a".into(),
        symbol: "SOLUSDC".into(),
        side: "BUY".into(),
        order_id: 456,
    };
    let pending = crate::types::ObservedOrderIdentity {
        client_order_id: "gb_observed_b".into(),
        symbol: "SOLUSDC".into(),
        side: "SELL".into(),
        order_id: 456,
    };
    db.save_identity_observation(account, &expected, &existing)
        .unwrap();
    first.evidence.insert_conflict(key.0, expected.clone());
    first.evidence.mark_provisional(key.0, &key.1);
    first
        .evidence
        .provisional_order_observations
        .entry(key.0)
        .or_default()
        .insert(key.1, vec![pending]);
    assert!(!first.cancel_all_orders().await);
    let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
    for (client, side) in [("gb_observed_a", "BUY"), ("gb_observed_b", "SELL")] {
        assert!(observations
            .iter()
            .any(|(_, observed)| observed.client_order_id == client
                && observed.symbol == "SOLUSDC"
                && observed.side == side
                && observed.order_id == 456));
    }
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.cancel_all_orders().await);
    let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
    for (client, side) in [("gb_observed_a", "BUY"), ("gb_observed_b", "SELL")] {
        assert!(observations
            .iter()
            .any(|(_, observed)| observed.client_order_id == client
                && observed.symbol == "SOLUSDC"
                && observed.side == side
                && observed.order_id == 456));
    }
    server.abort();
}

#[tokio::test]
async fn terminal_remainder_preserves_confirmed_id_against_later_mismatch() {
    let dir = crate::test_support::TempTestDir::new("remainder-terminal-id");
    let path = dir.join("remainder-terminal-id.db");
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    let stage = Arc::new(AtomicU8::new(0));
    let app = Router::new().route("/fapi/v1/order", get({
        let stage = stage.clone();
        move || { let stage = stage.clone(); async move {
            Json(json!({"orderId":if stage.load(Ordering::SeqCst) == 0 {123} else {456},
                "clientOrderId":"gb_remainder_twice","symbol":"SOLUSDC","status":"CANCELED",
                "price":"101","origQty":"2","executedQty":"0","side":"SELL","type":"LIMIT","timeInForce":"GTX"}))
        }}
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut plan = RemainderPlan {
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        sources: Vec::new(),
        phase: RemainderPhase::Submitting,
        target: GridOrder {
            client_order_id: "gb_remainder_twice".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(2),
            amount_usdc: dec!(202),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Remainder,
            merge_sources: Vec::new(),
        },
    };
    db.save_remainder_plan(&plan).unwrap();
    let mut engine = engine(db.clone(), &url).await;
    let account = engine.client.account_identity_key();
    assert!(engine.adopt_remainder_target(&mut plan).await.unwrap());
    assert!(!engine
        .state
        .active_orders
        .read()
        .await
        .contains_key("gb_remainder_twice"));
    assert_eq!(plan.target.order_id, Some(123));
    let persisted_json: String = connection
        .query_row(
            "SELECT plan_json FROM remainder_plans WHERE target_client_order_id = ?1",
            ["gb_remainder_twice"],
            |row| row.get(0),
        )
        .unwrap();
    let persisted: RemainderPlan = serde_json::from_str(&persisted_json).unwrap();
    assert_eq!(persisted.target.order_id, Some(123));
    stage.store(1, Ordering::SeqCst);
    assert!(engine.adopt_remainder_target(&mut plan).await.is_err());
    assert!(engine.reconciliation_blocked);
    assert_eq!(plan.target.order_id, Some(123));
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .iter()
        .any(
            |(expected, observed)| expected.client_order_id == "gb_remainder_twice"
                && expected.order_id == Some(123)
                && observed.order_id == 456
        ));
    server.abort();
    drop(connection);
    drop(engine);
    drop(db);
}

#[tokio::test]
async fn accepted_intent_without_order_id_survives_missing_snapshot_and_lookup() {
    for remainder in [false, true] {
        for write_fails in [false, true] {
            let dir = crate::test_support::TempTestDir::new("intent-acceptance");
            let path = dir.join("intent-acceptance.db");
            let db = Arc::new(Database::open(&path).unwrap());
            db.save_bot_status(BotStatus::Running).unwrap();
            let connection = rusqlite::Connection::open(&path).unwrap();
            if write_fails {
                connection.execute_batch("CREATE TRIGGER reject_intent_observation BEFORE INSERT ON order_identity_observations BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
            }
            let stage = Arc::new(AtomicU8::new(0));
            let missing_queries = Arc::new(AtomicU8::new(0));
            let app = Router::new()
                .route("/fapi/v1/openOrders", get({
                    let stage = stage.clone();
                    move || { let stage = stage.clone(); async move {
                        if stage.load(Ordering::SeqCst) != 0 { return Json(json!([])); }
                        Json(json!([
                            {"orderId":11,"clientOrderId":"gb_intent_accept_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                            {"orderId":12,"clientOrderId":"gb_intent_accept_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                            {"orderId":99,"clientOrderId":"gb_intent_accepted","symbol":"SOLUSDC","status":"NEW","price":"101","origQty":"2","executedQty":"0","side":"SELL","type":"LIMIT","timeInForce":"GTX"}
                        ]))
                    }}
                }))
                .route("/fapi/v1/order", get({
                    let missing_queries = missing_queries.clone();
                    move |Query(query): Query<HashMap<String, String>>| {
                        let missing_queries = missing_queries.clone();
                        async move {
                            if query.get("origClientOrderId").map(String::as_str) == Some("gb_intent_accepted") {
                                missing_queries.fetch_add(1, Ordering::SeqCst);
                                return (StatusCode::BAD_REQUEST, Json(json!({"code":-2013,"msg":"Order does not exist"})));
                            }
                            let id = query.get("orderId").and_then(|id| id.parse::<i64>().ok()).unwrap_or(11);
                            (StatusCode::OK, Json(json!({"orderId":id,
                                "clientOrderId":if id == 99 {"gb_intent_accepted"} else {"gb_intent_accept_conflict"},
                                "symbol":"SOLUSDC","status":if id == 12 {"CANCELED"} else {"NEW"},
                                "price":if id == 99 {"101"} else {"100"},"origQty":if id == 99 {"2"} else {"1"},
                                "executedQty":"0","side":if id == 99 {"SELL"} else {"BUY"},"type":"LIMIT","timeInForce":"GTX"})))
                        }
                    }
                }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let intent = GridOrder {
                client_order_id: "gb_intent_accepted".into(),
                order_id: None,
                symbol: "SOLUSDC".into(),
                side: OrderSide::Sell,
                price: dec!(101),
                quantity: dec!(2),
                amount_usdc: dec!(202),
                status: OrderStatus::New,
                created_at: Utc::now() - chrono::Duration::hours(1),
                updated_at: Utc::now(),
                grid_level: 3,
                paired_client_order_id: Some("gb_accept_parent".into()),
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
                    trade_id: "grid:gb_accept_parent".into(),
                    client_order_id: "gb_accept_parent".into(),
                    symbol: "SOLUSDC".into(),
                    mode: TradingMode::Live,
                    side: OrderSide::Buy,
                    price: dec!(100),
                    quantity: dec!(2),
                    amount_usdc: dec!(200),
                    realized_pnl: dec!(0),
                    commission: dec!(0),
                    commission_asset: None,
                    pnl_verified: true,
                    is_maker: true,
                    timestamp: Utc::now(),
                    note: "fill".into(),
                };
                db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&intent), None)
                    .unwrap();
            }
            let mut first = engine(db.clone(), &url).await;
            let account = first.client.account_identity_key();
            assert!(!first.sync_live_orders().await);
            assert!(first.reconciliation_blocked);
            assert_eq!(
                first.state.active_orders.read().await["gb_intent_accepted"].quantity,
                dec!(2)
            );
            if !write_fails {
                let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
                assert!(observations
                    .iter()
                    .any(
                        |(expected, found)| expected.client_order_id == "gb_intent_accepted"
                            && found.order_id == 99
                            && found.side == "SELL"
                    ));
                drop(first);
                stage.store(1, Ordering::SeqCst);
                let mut restarted = engine(db.clone(), &url).await;
                assert!(!restarted.sync_live_orders().await);
                assert!(missing_queries.load(Ordering::SeqCst) > 0);
                assert!(restarted.reconciliation_blocked);
                assert_eq!(
                    restarted.state.active_orders.read().await["gb_intent_accepted"].quantity,
                    dec!(2)
                );
                assert!(db
                    .load_identity_observations(account, "SOLUSDC")
                    .unwrap()
                    .iter()
                    .any(
                        |(expected, found)| expected.client_order_id == "gb_intent_accepted"
                            && found.order_id == 99
                    ));
                drop(restarted);
            } else {
                assert!(first.last_orders_sync.is_none());
                drop(first);
            }
            if remainder {
                assert!(db
                    .load_remainder_plan("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .is_some());
            } else {
                assert_eq!(
                    db.load_pair_intents("SOLUSDC", TradingMode::Live)
                        .unwrap()
                        .len(),
                    1
                );
            }
            server.abort();
            drop(connection);
            drop(db);
        }
    }
}
