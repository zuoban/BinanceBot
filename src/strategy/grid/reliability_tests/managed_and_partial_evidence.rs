use super::*;

#[tokio::test]
async fn duplicate_snapshot_details_must_agree_before_reconciliation() {
    for reversed in [false, true] {
        for mismatch in ["none", "status", "executed_qty", "orig_qty", "price"] {
            let db = database();
            let good = json!({"orderId":123,"clientOrderId":"gb_duplicate_details","symbol":"SOLUSDC",
                "status":"NEW","price":"100","origQty":"2","executedQty":"0",
                "side":"BUY","type":"LIMIT","timeInForce":"GTX"});
            let bad = json!({"orderId":123,"clientOrderId":"gb_duplicate_details","symbol":"SOLUSDC",
                "status":if mismatch == "status" {"PARTIALLY_FILLED"} else {"NEW"},
                "price":if mismatch == "price" {"101"} else {"100"},
                "origQty":if mismatch == "orig_qty" {"3"} else {"2"},
                "executedQty":if mismatch == "status" || mismatch == "executed_qty" {"1"} else {"0"},
                "side":"BUY","type":"LIMIT","timeInForce":"GTX"});
            let snapshot = if reversed {
                vec![bad, good]
            } else {
                vec![good, bad]
            };
            let app = Router::new()
                .route("/fapi/v1/openOrders", get(move || {
                    let snapshot = snapshot.clone();
                    async move { Json(snapshot) }
                }))
                .route("/fapi/v2/positionRisk", get(|| async {
                    Json(json!([{"symbol":"SOLUSDC","positionAmt":"2","entryPrice":"100","markPrice":"100",
                        "unRealizedProfit":"0","liquidationPrice":"0","leverage":"1"}]))
                }))
                .route("/fapi/v2/account", get(|| async {
                    Json(json!({"totalWalletBalance":"1000","totalMarginBalance":"1000","totalUnrealizedProfit":"0",
                        "availableBalance":"1000","assets":[{"asset":"USDC","walletBalance":"1000","marginBalance":"1000",
                        "unrealizedProfit":"0","availableBalance":"1000"}]}))
                }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let order = GridOrder {
                client_order_id: "gb_duplicate_details".into(),
                order_id: Some(123),
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
            db.save_managed_order(&order).unwrap();
            let mut engine = engine(db.clone(), &url).await;
            assert_eq!(
                engine.sync_live_orders().await,
                mismatch == "none",
                "reversed={reversed}, mismatch={mismatch}"
            );
            assert_eq!(engine.reconciliation_blocked, mismatch != "none");
            if mismatch != "none" {
                assert!(engine.last_orders_sync.is_none());
                assert!(!engine.state.global_position_ready.load(Ordering::SeqCst));
            }
            let active = engine.state.active_orders.read().await;
            assert_eq!(active.len(), 1);
            let retained = &active[&order.client_order_id];
            assert_eq!(retained.order_id, order.order_id);
            assert_eq!(retained.quantity, order.quantity);
            assert_eq!(retained.price, order.price);
            assert_eq!(retained.status, order.status);
            drop(active);
            let saved = db.load_managed_orders("SOLUSDC").unwrap();
            assert_eq!(saved.len(), 1);
            assert_eq!(saved[0].order_id, order.order_id);
            assert_eq!(saved[0].quantity, order.quantity);
            assert_eq!(saved[0].price, order.price);
            assert_eq!(saved[0].status, order.status);
            server.abort();
        }
    }
}

#[tokio::test]
async fn failed_managed_write_is_durably_recovered_when_next_snapshot_conflicts() {
    let dir = crate::test_support::TempTestDir::new("mixed-memory");
    let path = dir.join("mixed-memory.db");
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_discovered_order BEFORE INSERT ON managed_orders BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    let stage = Arc::new(AtomicU8::new(0));
    let app = Router::new().route("/fapi/v1/openOrders", get({
        let stage = stage.clone();
        move || { let stage = stage.clone(); async move {
            let mut orders = Vec::new();
            if stage.load(Ordering::SeqCst) == 0 {
                orders.push(json!({"orderId":99,"clientOrderId":"gb_memory_only","symbol":"SOLUSDC",
                    "status":"NEW","price":"95","origQty":"2","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}));
            } else {
                for id in [11, 12] {
                    orders.push(json!({"orderId":id,"clientOrderId":"gb_memory_conflict","symbol":"SOLUSDC",
                        "status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}));
                }
            }
            Json(orders)
        }}
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    assert!(!first.sync_live_orders().await);
    assert_eq!(
        first.state.active_orders.read().await["gb_memory_only"].quantity,
        dec!(2)
    );
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    connection
        .execute_batch("DROP TRIGGER reject_discovered_order")
        .unwrap();
    stage.store(1, Ordering::SeqCst);
    assert!(!first.sync_live_orders().await);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
    assert!(
        saved
            .iter()
            .any(|order| order.client_order_id == "gb_memory_only"
                && order.order_id == Some(99)
                && order.quantity == dec!(2)
                && order.price == dec!(95))
            || observations
                .iter()
                .any(
                    |(expected, found)| expected.client_order_id == "gb_memory_only"
                        && expected.quantity == dec!(2)
                        && expected.price == dec!(95)
                        && found.order_id == 99
                )
    );
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    let active = restarted.state.active_orders.read().await;
    let restored = &active["gb_memory_only"];
    assert_eq!(restored.order_id, Some(99));
    assert_eq!(restored.quantity, dec!(2));
    assert_eq!(restored.price, dec!(95));
    assert_eq!(restored.amount_usdc, dec!(190));
    drop(active);
    server.abort();
    drop(connection);
    drop(restarted);
    drop(db);
}

#[tokio::test]
async fn managed_placeholder_does_not_hide_confirmed_memory_identity_on_restart() {
    let db = database();
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async {
            Json(json!([
                {"orderId":11,"clientOrderId":"gb_placeholder_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                {"orderId":12,"clientOrderId":"gb_placeholder_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}
            ]))
        }))
        .route("/fapi/v1/order", get(|| async {
            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unavailable"})))
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let order = GridOrder {
        client_order_id: "gb_placeholder_known".into(),
        order_id: None,
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(95),
        quantity: dec!(2),
        amount_usdc: dec!(190),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -3,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    db.save_managed_order(&order).unwrap();
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let mut confirmed = order.clone();
    confirmed.order_id = Some(99);
    first
        .state
        .active_orders
        .write()
        .await
        .insert(order.client_order_id.clone(), confirmed);
    assert!(!first.sync_live_orders().await);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
    assert!(
        saved
            .iter()
            .any(|item| item.client_order_id == order.client_order_id && item.order_id == Some(99))
            || observations
                .iter()
                .any(
                    |(expected, found)| expected.client_order_id == order.client_order_id
                        && (expected.order_id == Some(99) || found.order_id == 99)
                )
    );
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    let active = restarted.state.active_orders.read().await;
    assert_eq!(active["gb_placeholder_known"].order_id, Some(99));
    assert_eq!(active["gb_placeholder_known"].quantity, dec!(2));
    server.abort();
}

#[tokio::test]
async fn memory_order_cannot_overwrite_existing_managed_identity_on_insert_if_absent() {
    for mismatch in ["symbol", "side", "order_id"] {
        let db = database();
        let app = Router::new().route("/fapi/v1/openOrders", get(|| async {
            Json(json!([
                {"orderId":11,"clientOrderId":"gb_collision_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                {"orderId":12,"clientOrderId":"gb_collision_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}
            ]))
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let durable = GridOrder {
            client_order_id: "gb_collision_memory".into(),
            order_id: Some(123),
            symbol: if mismatch == "symbol" {
                "BTCUSDC"
            } else {
                "SOLUSDC"
            }
            .into(),
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: -2,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        db.save_managed_order(&durable).unwrap();
        let mut engine = engine(db.clone(), &url).await;
        let mut memory = durable.clone();
        memory.symbol = "SOLUSDC".into();
        if mismatch == "side" {
            memory.side = OrderSide::Sell;
        }
        if mismatch == "order_id" {
            memory.order_id = Some(456);
        }
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(memory.client_order_id.clone(), memory);
        assert!(!engine.sync_live_orders().await);
        assert!(engine.reconciliation_blocked);
        assert!(engine.last_orders_sync.is_none());
        let saved = db.load_managed_orders(&durable.symbol).unwrap();
        let saved = saved
            .iter()
            .find(|item| item.client_order_id == durable.client_order_id)
            .unwrap();
        assert_eq!(saved.order_id, durable.order_id);
        assert_eq!(saved.symbol, durable.symbol);
        assert_eq!(saved.side, durable.side);
        assert_eq!(saved.grid_level, durable.grid_level);
        server.abort();
    }
}

#[tokio::test]
async fn second_lookup_partial_fill_survives_retry_and_restart() {
    for write_fails in [false, true] {
        let dir = crate::test_support::TempTestDir::new("second-response-write");
        let path = dir.join("second-response-write.db");
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        let stage = Arc::new(AtomicU8::new(0));
        let partial_queries = Arc::new(AtomicU8::new(0));
        let partial_seen = Arc::new(AtomicBool::new(false));
        let partial_client_queries = Arc::new(AtomicU8::new(0));
        let app = Router::new()
            .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
            .route("/fapi/v1/order", get({
                let stage = stage.clone();
                let partial_queries = partial_queries.clone();
                let partial_seen = partial_seen.clone();
                let partial_client_queries = partial_client_queries.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    let stage = stage.clone();
                    let partial_queries = partial_queries.clone();
                    let partial_seen = partial_seen.clone();
                    let partial_client_queries = partial_client_queries.clone();
                    async move {
                        let by_client = query.contains_key("origClientOrderId");
                        let id = if by_client {456} else {query["orderId"].parse::<i64>().unwrap()};
                        let partial = stage.load(Ordering::SeqCst) == 0 && id == 456
                            && (!by_client || partial_seen.load(Ordering::SeqCst));
                        if partial && by_client {
                            partial_client_queries.fetch_add(1, Ordering::SeqCst);
                        } else if partial {
                            partial_seen.store(true, Ordering::SeqCst);
                            partial_queries.fetch_add(1, Ordering::SeqCst);
                        }
                        let status = if id == 123 {"CANCELED"} else if partial {"PARTIALLY_FILLED"}
                            else if stage.load(Ordering::SeqCst) == 0 {"NEW"} else {"CANCELED"};
                        Json(json!({"orderId":id,"clientOrderId":"gb_second_partial","symbol":"SOLUSDC",
                            "status":status,"price":"101","avgPrice":"100","origQty":"2",
                            "executedQty":if partial {"0.5"} else {"0"},
                            "updateTime":if partial {2000} else if status == "NEW" {1000} else {3000},
                            "side":"SELL","type":"LIMIT","timeInForce":"GTX"}))
                    }
                }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let intent = GridOrder {
            client_order_id: "gb_second_partial".into(),
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
            paired_client_order_id: Some("gb_second_partial_parent".into()),
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        let trade = TradeRecord {
            trade_id: "grid:gb_second_partial_parent".into(),
            client_order_id: "gb_second_partial_parent".into(),
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
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        db.save_identity_observation(
            account,
            &intent,
            &crate::types::ObservedOrderIdentity {
                client_order_id: intent.client_order_id.clone(),
                symbol: intent.symbol.clone(),
                side: "SELL".into(),
                order_id: 123,
            },
        )
        .unwrap();
        if write_fails {
            connection.execute_batch("CREATE TRIGGER reject_second_response BEFORE INSERT ON initial_order_responses WHEN NEW.response_json LIKE '%PARTIALLY_FILLED%' BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
        }
        assert!(!first.sync_live_orders().await);
        assert!(partial_queries.load(Ordering::SeqCst) > 0);
        let client_response = first
            .client
            .lookup_order_by_client_id("SOLUSDC", "gb_second_partial")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(client_response.status, "PARTIALLY_FILLED");
        assert_eq!(client_response.executed_qty, dec!(0.5));
        assert!(partial_client_queries.load(Ordering::SeqCst) > 0);
        assert!(first.reconciliation_blocked);
        assert_eq!(
            db.load_pair_intents("SOLUSDC", TradingMode::Live)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        if write_fails {
            assert!(!db
                .load_initial_order_responses(account, "SOLUSDC", &intent.client_order_id)
                .unwrap()
                .iter()
                .any(|response| response.status == "PARTIALLY_FILLED"));
            assert!(!first.sync_live_orders().await);
            drop(first);
            let mut failed_restart = engine(db.clone(), &url).await;
            assert!(!failed_restart.sync_live_orders().await);
            assert!(failed_restart.reconciliation_blocked);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            connection
                .execute_batch("DROP TRIGGER reject_second_response")
                .unwrap();
            assert!(!failed_restart.sync_live_orders().await);
            drop(failed_restart);
        } else {
            drop(first);
        }
        let responses = db
            .load_initial_order_responses(account, "SOLUSDC", &intent.client_order_id)
            .unwrap();
        assert!(responses.iter().any(|response| response.order_id == 456
            && response.status == "NEW"
            && response.executed_qty == dec!(0)
            && response.update_time == Some(1000)));
        assert!(responses.iter().any(|response| response.order_id == 456
            && response.status == "PARTIALLY_FILLED"
            && response.executed_qty == dec!(0.5)
            && response.update_time == Some(2000)));
        stage.store(1, Ordering::SeqCst);
        let mut restarted = engine(db.clone(), &url).await;
        for _ in 0..3 {
            assert!(!restarted.sync_live_orders().await);
            assert!(restarted.reconciliation_blocked);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        }
        let disagreements = db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap();
        assert!(disagreements.iter().any(|(client, earlier, later)| client
            == &intent.client_order_id
            && earlier.order_id == 456
            && earlier.status == "PARTIALLY_FILLED"
            && earlier.executed_qty == dec!(0.5)
            && later.order_id == 456
            && later.status == "CANCELED"
            && later.executed_qty == dec!(0)));
        server.abort();
        drop(restarted);
        drop(connection);
        drop(db);
    }
}

#[tokio::test]
async fn partial_client_response_survives_failed_id_lookup_and_restart() {
    for write_fails in [false, true] {
        let dir = crate::test_support::TempTestDir::new("partial-response-write");
        let path = dir.join("partial-response-write.db");
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        let stage = Arc::new(AtomicU8::new(0));
        let failed_id_queries = Arc::new(AtomicU8::new(0));
        let saved_before_query = Arc::new(AtomicBool::new(false));
        let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
        .route("/fapi/v1/order", get({
            let stage = stage.clone();
            let failed_id_queries = failed_id_queries.clone();
            let saved_before_query = saved_before_query.clone();
            let path = path.clone();
            move |Query(query): Query<HashMap<String, String>>| {
                let stage = stage.clone();
                let failed_id_queries = failed_id_queries.clone();
                let saved_before_query = saved_before_query.clone();
                let path = path.clone();
                async move {
                    let by_client = query.contains_key("origClientOrderId");
                    let id = if by_client {456} else {query["orderId"].parse::<i64>().unwrap()};
                    let initial = stage.load(Ordering::SeqCst) == 0;
                    if initial && !by_client && id == 456 {
                        failed_id_queries.fetch_add(1, Ordering::SeqCst);
                        let conn = rusqlite::Connection::open(&path).unwrap();
                        let count: i64 = conn.query_row("SELECT COUNT(*) FROM initial_order_responses", [], |row| row.get(0)).unwrap();
                        saved_before_query.store(count > 0, Ordering::SeqCst);
                        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unavailable"})));
                    }
                    let partial = initial && by_client;
                    (StatusCode::OK, Json(json!({"orderId":id,"clientOrderId":"gb_partial_interrupted",
                        "symbol":"SOLUSDC","status":if partial {"PARTIALLY_FILLED"} else {"CANCELED"},
                        "price":"101","avgPrice":"100","origQty":"2","executedQty":if partial {"0.5"} else {"0"},
                        "updateTime":if partial {1000} else {2000},"side":"SELL","type":"LIMIT","timeInForce":"GTX"})))
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let intent = GridOrder {
            client_order_id: "gb_partial_interrupted".into(),
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
            paired_client_order_id: Some("gb_partial_interrupted_parent".into()),
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        let trade = TradeRecord {
            trade_id: "grid:gb_partial_interrupted_parent".into(),
            client_order_id: "gb_partial_interrupted_parent".into(),
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
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        db.save_identity_observation(
            account,
            &intent,
            &crate::types::ObservedOrderIdentity {
                client_order_id: intent.client_order_id.clone(),
                symbol: intent.symbol.clone(),
                side: "SELL".into(),
                order_id: 123,
            },
        )
        .unwrap();
        if write_fails {
            connection.execute_batch("CREATE TRIGGER reject_initial_response BEFORE INSERT ON initial_order_responses BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
        }
        assert!(!first.sync_live_orders().await);
        if write_fails {
            assert_eq!(failed_id_queries.load(Ordering::SeqCst), 0);
            assert!(first.reconciliation_blocked);
            assert!(!first.sync_live_orders().await);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
            drop(first);
            let mut restarted = engine(db.clone(), &url).await;
            assert!(!restarted.sync_live_orders().await);
            assert!(restarted.reconciliation_blocked);
            assert_eq!(failed_id_queries.load(Ordering::SeqCst), 0);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
            server.abort();
            drop(restarted);
            drop(connection);
            drop(db);
            continue;
        }
        assert!(failed_id_queries.load(Ordering::SeqCst) > 0);
        assert!(saved_before_query.load(Ordering::SeqCst));
        assert!(first.reconciliation_blocked);
        assert_eq!(
            db.load_pair_intents("SOLUSDC", TradingMode::Live)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        stage.store(1, Ordering::SeqCst);
        for _ in 0..2 {
            assert!(!first.sync_live_orders().await);
            assert!(first.reconciliation_blocked);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        }
        let disagreements = db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap();
        assert!(disagreements.iter().any(|(client, initial, later)| client
            == &intent.client_order_id
            && initial.order_id == 456
            && initial.status == "PARTIALLY_FILLED"
            && initial.executed_qty == dec!(0.5)
            && initial.update_time == Some(1000)
            && later.order_id == 456
            && later.status == "CANCELED"
            && later.executed_qty == dec!(0)
            && later.update_time == Some(2000)));
        drop(first);
        let mut restarted = engine(db.clone(), &url).await;
        assert!(!restarted.sync_live_orders().await);
        assert!(restarted.reconciliation_blocked);
        assert_eq!(
            db.load_pair_intents("SOLUSDC", TradingMode::Live)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        assert!(!db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap()
            .is_empty());
        server.abort();
        drop(restarted);
        drop(connection);
        drop(db);
    }
}

#[tokio::test]
async fn repeated_lookup_of_same_observed_id_preserves_disagreeing_terminal_responses() {
    let db = database();
    let queries = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
        .route("/fapi/v1/order", get({
            let queries = queries.clone();
            move |Query(query): Query<HashMap<String, String>>| {
                let queries = queries.clone();
                async move {
                    assert_eq!(query.get("orderId").map(String::as_str), Some("456"));
                    let count = queries.fetch_add(1, Ordering::SeqCst);
                    Json(json!({"orderId":456,"clientOrderId":"gb_repeated_observed_id","symbol":"SOLUSDC",
                        "status":"CANCELED","price":"100","avgPrice":"100","origQty":"2",
                        "executedQty":if count.is_multiple_of(2) {"0"} else {"0.5"},
                        "updateTime":1000,"side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
                }
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let expected = GridOrder {
        client_order_id: "gb_repeated_observed_id".into(),
        order_id: Some(123),
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
    for side in ["BUY", "SELL"] {
        db.save_identity_observation(
            account,
            &expected,
            &crate::types::ObservedOrderIdentity {
                client_order_id: expected.client_order_id.clone(),
                symbol: expected.symbol.clone(),
                side: side.into(),
                order_id: 456,
            },
        )
        .unwrap();
    }
    for _ in 0..3 {
        assert!(!first.sync_live_orders().await);
        assert!(first.reconciliation_blocked);
        assert_eq!(
            first.state.active_orders.read().await[&expected.client_order_id].order_id,
            Some(123)
        );
        assert_eq!(
            db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
            Some(123)
        );
        assert!(db.get_recent_trades(10).unwrap().is_empty());
    }
    assert!(queries.load(Ordering::SeqCst) >= 2);
    let disagreements = db
        .load_terminal_order_disagreements(account, "SOLUSDC")
        .unwrap();
    assert!(disagreements.iter().any(|(client, a, b)| {
        client == &expected.client_order_id
            && a.order_id == 456
            && b.order_id == 456
            && a.status == "CANCELED"
            && b.status == "CANCELED"
            && a.side == "BUY"
            && b.side == "BUY"
            && ((a.executed_qty == dec!(0) && b.executed_qty == dec!(0.5))
                || (a.executed_qty == dec!(0.5) && b.executed_qty == dec!(0)))
    }));
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted.reconciliation_blocked);
    assert_eq!(
        restarted.state.active_orders.read().await[&expected.client_order_id].order_id,
        Some(123)
    );
    assert!(!db
        .load_terminal_order_disagreements(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    server.abort();
}
