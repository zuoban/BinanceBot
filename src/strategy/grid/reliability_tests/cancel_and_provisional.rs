use super::*;

#[tokio::test]
async fn bulk_cancel_conflicting_fill_response_revokes_global_readiness() {
    let fake = Exchange::default();
    fake.cancel_fails.store(true, Ordering::SeqCst);
    fake.lookup_filled_zero.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    let order = GridOrder {
        client_order_id: "gb_cancel_unknown".into(),
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
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    fake.orders.lock().unwrap().insert(
        order.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 123,
            client_order_id: order.client_order_id.clone(),
            symbol: order.symbol.clone(),
            status: "NEW".into(),
            price: order.price,
            avg_price: None,
            orig_qty: order.quantity,
            executed_qty: Decimal::ZERO,
            side: "BUY".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    engine
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    assert!(!engine.cancel_all_orders().await);
    assert!(!engine.state.global_position_ready.load(Ordering::SeqCst));
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key("gb_cancel_unknown"));
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
        Some(123)
    );
    fake.orders
        .lock()
        .unwrap()
        .get_mut("gb_cancel_unknown")
        .unwrap()
        .status = "FILLED".into();
    assert!(!engine.sync_live_orders().await);
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key("gb_cancel_unknown"));
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
        Some(123)
    );
    server.abort();
}

#[tokio::test]
async fn bulk_cancel_identity_conflict_remains_blocked_after_list_disappears_and_restart() {
    let fake = Exchange::default();
    fake.cancel_fails.store(true, Ordering::SeqCst);
    fake.lookup_filled_zero.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut first = engine(db.clone(), &url).await;
    let original = GridOrder {
        client_order_id: "gb_bulk_identity".into(),
        order_id: Some(123),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -5,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    first
        .state
        .active_orders
        .write()
        .await
        .insert(original.client_order_id.clone(), original.clone());
    fake.orders.lock().unwrap().insert(
        original.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 456,
            client_order_id: original.client_order_id.clone(),
            symbol: original.symbol.clone(),
            status: "NEW".into(),
            price: original.price,
            avg_price: None,
            orig_qty: original.quantity,
            executed_qty: Decimal::ZERO,
            side: "BUY".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    assert!(!first.cancel_all_orders().await);
    let key = (
        first.client.account_identity_key(),
        original.client_order_id.clone(),
    );
    assert!(first.evidence.has_conflict(key.0, &key.1));
    assert_eq!(
        db.load_identity_observations(key.0, "SOLUSDC")
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
        Some(123)
    );
    fake.orders
        .lock()
        .unwrap()
        .get_mut("gb_bulk_identity")
        .unwrap()
        .status = "FILLED".into();
    assert!(!first.sync_live_orders().await);
    assert!(first.evidence.has_conflict(key.0, &key.1));
    assert_eq!(
        first.state.active_orders.read().await["gb_bulk_identity"].order_id,
        Some(123)
    );
    drop(first);
    fake.orders
        .lock()
        .unwrap()
        .get_mut("gb_bulk_identity")
        .unwrap()
        .status = "NEW".into();
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert_eq!(
        db.load_identity_observations(key.0, "SOLUSDC")
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
        Some(123)
    );
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap()[0].grid_level, -5);
    server.abort();
}

#[tokio::test]
async fn bulk_cancel_keeps_provisional_order_when_managed_read_or_write_fails() {
    for corrupt_row in [true, false] {
        let dir = crate::test_support::TempTestDir::new("cancel-provisional");
        let path = dir.join("cancel-provisional.db");
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let fake = Exchange::default();
        fake.cancel_fails.store(true, Ordering::SeqCst);
        fake.lookup_filled_zero.store(true, Ordering::SeqCst);
        let (url, server) = exchange_server(fake.clone()).await;
        let mut engine = engine(db.clone(), &url).await;
        let order = GridOrder {
            client_order_id: "gb_cancel_provisional".into(),
            order_id: Some(123),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: -4,
            paired_client_order_id: Some("gb_parent".into()),
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        let connection = rusqlite::Connection::open(&path).unwrap();
        if corrupt_row {
            db.save_managed_order(&order).unwrap();
            connection.execute("UPDATE managed_orders SET order_json = 'invalid' WHERE client_order_id = 'gb_cancel_provisional'", []).unwrap();
        } else {
            connection.execute_batch("CREATE TRIGGER reject_managed BEFORE INSERT ON managed_orders BEGIN SELECT RAISE(ABORT, 'write failed'); END;").unwrap();
        }
        fake.orders.lock().unwrap().insert(
            order.client_order_id.clone(),
            BinanceOrderResponse {
                order_id: 123,
                client_order_id: order.client_order_id.clone(),
                symbol: order.symbol.clone(),
                status: "NEW".into(),
                price: order.price,
                avg_price: None,
                orig_qty: order.quantity,
                executed_qty: Decimal::ZERO,
                side: "BUY".into(),
                order_type: "LIMIT".into(),
                time_in_force: "GTX".into(),
                update_time: None,
            },
        );
        assert!(!engine.cancel_all_orders().await);
        let key = (
            engine.client.account_identity_key(),
            order.client_order_id.clone(),
        );
        assert!(engine.evidence.is_provisional(key.0, &key.1));
        assert_eq!(
            engine.state.active_orders.read().await[&order.client_order_id].order_id,
            Some(123)
        );
        if corrupt_row {
            let raw: String = connection.query_row("SELECT order_json FROM managed_orders WHERE client_order_id = 'gb_cancel_provisional'", [], |row| row.get(0)).unwrap();
            assert_eq!(raw, "invalid");
            connection.execute("UPDATE managed_orders SET order_json = ?1 WHERE client_order_id = 'gb_cancel_provisional'", rusqlite::params![serde_json::to_string(&order).unwrap()]).unwrap();
        } else {
            connection
                .execute_batch("DROP TRIGGER reject_managed")
                .unwrap();
            db.save_managed_order(&order).unwrap();
        }
        assert!(!engine.cancel_all_orders().await);
        assert_eq!(
            engine.state.active_orders.read().await["gb_cancel_provisional"].purpose,
            crate::types::OrderPurpose::Grid
        );
        let saved = db.load_managed_orders("SOLUSDC").unwrap();
        assert_eq!(saved[0].purpose, crate::types::OrderPurpose::Grid);
        assert_eq!(saved[0].grid_level, -4);
        assert_eq!(
            saved[0].paired_client_order_id.as_deref(),
            Some("gb_parent")
        );
        server.abort();
        drop(connection);
        drop(engine);
        drop(db);
    }
}

#[tokio::test]
async fn inconsistent_open_order_is_retained_in_memory_when_managed_order_loading_fails() {
    for conflicting_active in [false, true] {
        let dir = crate::test_support::TempTestDir::new("uncertain-load");
        let path = dir.join("uncertain-load.db");
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let app = Router::new().route(
            "/fapi/v1/openOrders",
            get(|| async {
                Json(
                    json!([{"orderId":123, "clientOrderId":"gb_unknown_db", "symbol":"SOLUSDC",
                "status":"FILLED", "price":"100", "origQty":"2", "executedQty":"0",
                "side":"BUY", "type":"LIMIT", "timeInForce":"GTX"}]),
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut engine = engine(db.clone(), &url).await;
        if conflicting_active {
            let order = GridOrder {
                client_order_id: "gb_unknown_db".into(),
                order_id: Some(456),
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
            engine
                .state
                .active_orders
                .write()
                .await
                .insert(order.client_order_id.clone(), order);
        }
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch("DROP TABLE managed_orders")
            .unwrap();
        assert!(!engine.sync_live_orders().await);
        let active = engine.state.active_orders.read().await;
        let retained = &active["gb_unknown_db"];
        assert_eq!(
            retained.order_id,
            if conflicting_active { Some(456) } else { None }
        );
        assert_eq!(
            retained.purpose,
            if conflicting_active {
                crate::types::OrderPurpose::Legacy
            } else {
                crate::types::OrderPurpose::Unverified
            }
        );
        assert_eq!(
            retained.quantity,
            if conflicting_active { dec!(1) } else { dec!(2) }
        );
        assert!(engine.reconciliation_blocked);
        drop(active);
        let observations = db
            .load_identity_observations(engine.client.account_identity_key(), "SOLUSDC")
            .unwrap();
        assert!(observations
            .iter()
            .any(|(_, observed)| observed.client_order_id == "gb_unknown_db"
                && observed.symbol == "SOLUSDC"
                && observed.side == "BUY"
                && observed.order_id == 123));
        server.abort();
        drop(connection);
        drop(engine);
        drop(db);
    }
}

#[tokio::test]
async fn recovered_managed_metadata_survives_normal_and_terminal_snapshots() {
    for terminal in [false, true] {
        let dir = crate::test_support::TempTestDir::new("uncertain-metadata");
        let path = dir.join("uncertain-metadata.db");
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let original = GridOrder {
            client_order_id: "gb_metadata".into(),
            order_id: Some(123),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(2),
            amount_usdc: dec!(200),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: -7,
            paired_client_order_id: Some("gb_parent".into()),
            is_take_profit: true,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: vec![crate::types::OrderSource {
                client_order_id: "gb_source".into(),
                price: dec!(99),
                quantity: dec!(1),
            }],
        };
        db.save_managed_order(&original).unwrap();
        let stage = Arc::new(AtomicU8::new(0));
        let app = Router::new().route("/fapi/v1/openOrders", get({
            let stage = stage.clone();
            move || {
                let stage = stage.clone();
                async move {
                    let current = stage.load(Ordering::SeqCst);
                    if current == 2 { return Json(json!([])); }
                    Json(json!([{"orderId":123, "clientOrderId":"gb_metadata", "symbol":"SOLUSDC",
                        "status":if current == 0 { "FILLED" } else { "NEW" }, "price":"100", "origQty":"2", "executedQty":"0",
                        "side":"BUY", "type":"LIMIT", "timeInForce":"GTX"}]))
                }
            }
        })).route("/fapi/v1/order", get({
            let stage = stage.clone();
            move || {
                let stage = stage.clone();
                async move {
                    Json(json!({"orderId":123, "clientOrderId":"gb_metadata", "symbol":"SOLUSDC",
                        "status":if stage.load(Ordering::SeqCst) == 1 {"NEW"} else {"FILLED"},
                        "price":"100", "origQty":"2", "executedQty":"0",
                        "side":"BUY", "type":"LIMIT", "timeInForce":"GTX"}))
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut engine = engine(db.clone(), &url).await;
        engine.state.active_orders.write().await.clear();
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute("UPDATE managed_orders SET order_json = 'invalid' WHERE client_order_id = 'gb_metadata'", []).unwrap();
        assert!(!engine.sync_live_orders().await);
        assert_eq!(
            engine.state.active_orders.read().await["gb_metadata"].purpose,
            crate::types::OrderPurpose::Unverified
        );
        assert_eq!(
            engine.state.active_orders.read().await["gb_metadata"].order_id,
            None
        );
        let observations = db
            .load_identity_observations(engine.client.account_identity_key(), "SOLUSDC")
            .unwrap();
        assert!(observations
            .iter()
            .any(|(_, observed)| observed.client_order_id == "gb_metadata"
                && observed.symbol == "SOLUSDC"
                && observed.side == "BUY"
                && observed.order_id == 123));
        connection
            .execute(
                "UPDATE managed_orders SET order_json = ?1 WHERE client_order_id = 'gb_metadata'",
                rusqlite::params![serde_json::to_string(&original).unwrap()],
            )
            .unwrap();
        stage.store(if terminal { 2 } else { 1 }, Ordering::SeqCst);
        assert_eq!(engine.sync_live_orders().await, !terminal);
        let saved = db.load_managed_orders("SOLUSDC").unwrap();
        let active = engine.state.active_orders.read().await;
        for order in [&saved[0], &active["gb_metadata"]] {
            assert_eq!(order.purpose, crate::types::OrderPurpose::Grid);
            assert_eq!(order.grid_level, -7);
            assert_eq!(order.paired_client_order_id.as_deref(), Some("gb_parent"));
            assert!(order.is_take_profit);
            assert_eq!(order.merge_sources.len(), 1);
            assert_eq!(order.merge_sources[0].client_order_id, "gb_source");
            assert_eq!(order.merge_sources[0].price, dec!(99));
            assert_eq!(order.merge_sources[0].quantity, dec!(1));
        }
        drop(active);
        server.abort();
        drop(connection);
        drop(engine);
        drop(db);
    }
}

#[tokio::test]
async fn provisional_identity_conflict_remains_blocked_until_exchange_matches_disk() {
    for already_active in [false, true] {
        let dir = crate::test_support::TempTestDir::new("identity-conflict");
        let path = dir.join("identity-conflict.db");
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let original = GridOrder {
            client_order_id: "gb_conflict".into(),
            order_id: Some(123),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(2),
            amount_usdc: dec!(200),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: -5,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        db.save_managed_order(&original).unwrap();
        let stage = Arc::new(AtomicU8::new(0));
        let app = Router::new().route("/fapi/v1/openOrders", get({
        let stage = stage.clone();
        move || {
            let stage = stage.clone();
            async move {
                let current = stage.load(Ordering::SeqCst);
                Json(json!([{"orderId":if current == 2 {123} else {456}, "clientOrderId":"gb_conflict",
                    "symbol":"SOLUSDC", "status":if current == 0 {"FILLED"} else {"NEW"},
                    "price":"100", "origQty":"2", "executedQty":"0", "side":"BUY", "type":"LIMIT", "timeInForce":"GTX"}]))
            }
        }
    })).route("/fapi/v1/order", get({
        let stage = stage.clone();
        move |Query(query): Query<HashMap<String, String>>| { let stage = stage.clone(); async move {
            let current = stage.load(Ordering::SeqCst);
            let id = if current == 2 {
                query.get("orderId").and_then(|id| id.parse::<i64>().ok()).unwrap_or(123)
            } else {456};
            Json(json!({"orderId":id, "clientOrderId":"gb_conflict", "symbol":"SOLUSDC",
                "status":if current != 2 {"FILLED"} else if id == 456 {"CANCELED"} else {"NEW"},
                "price":"100", "origQty":"2", "executedQty":"0", "side":"BUY", "type":"LIMIT", "timeInForce":"GTX"}))
        }}
    }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut engine = engine(db.clone(), &url).await;
        if !already_active {
            engine.state.active_orders.write().await.clear();
        }
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute("UPDATE managed_orders SET order_json = 'invalid' WHERE client_order_id = 'gb_conflict'", []).unwrap();
        assert!(!engine.sync_live_orders().await);
        assert_eq!(
            engine.state.active_orders.read().await["gb_conflict"].order_id,
            if already_active { Some(123) } else { None }
        );
        assert_eq!(
            engine.state.active_orders.read().await["gb_conflict"].purpose,
            if already_active {
                crate::types::OrderPurpose::Grid
            } else {
                crate::types::OrderPurpose::Unverified
            }
        );
        let observations = db
            .load_identity_observations(engine.client.account_identity_key(), "SOLUSDC")
            .unwrap();
        assert!(observations
            .iter()
            .any(|(_, observed)| observed.client_order_id == "gb_conflict"
                && observed.symbol == "SOLUSDC"
                && observed.side == "BUY"
                && observed.order_id == 456));
        connection
            .execute(
                "UPDATE managed_orders SET order_json = ?1 WHERE client_order_id = 'gb_conflict'",
                rusqlite::params![serde_json::to_string(&original).unwrap()],
            )
            .unwrap();
        stage.store(1, Ordering::SeqCst);
        for _ in 0..2 {
            assert!(!engine.sync_live_orders().await);
            assert_eq!(
                engine.state.active_orders.read().await["gb_conflict"].order_id,
                Some(123)
            );
            assert_eq!(
                db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
                Some(123)
            );
        }
        stage.store(2, Ordering::SeqCst);
        assert!(engine.sync_live_orders().await);
        let saved = db.load_managed_orders("SOLUSDC").unwrap();
        assert_eq!(saved[0].order_id, Some(123));
        assert_eq!(saved[0].purpose, crate::types::OrderPurpose::Grid);
        assert_eq!(saved[0].grid_level, -5);
        server.abort();
        drop(connection);
        drop(engine);
        drop(db);
    }
}
