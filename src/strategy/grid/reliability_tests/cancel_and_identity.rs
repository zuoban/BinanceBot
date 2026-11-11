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
        let path =
            std::env::temp_dir().join(format!("cancel-provisional-{}.db", uuid::Uuid::new_v4()));
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
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn inconsistent_open_order_is_retained_in_memory_when_managed_order_loading_fails() {
    for conflicting_active in [false, true] {
        let path = std::env::temp_dir().join(format!("uncertain-load-{}.db", uuid::Uuid::new_v4()));
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
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn recovered_managed_metadata_survives_normal_and_terminal_snapshots() {
    for terminal in [false, true] {
        let path =
            std::env::temp_dir().join(format!("uncertain-metadata-{}.db", uuid::Uuid::new_v4()));
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
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn provisional_identity_conflict_remains_blocked_until_exchange_matches_disk() {
    for already_active in [false, true] {
        let path =
            std::env::temp_dir().join(format!("identity-conflict-{}.db", uuid::Uuid::new_v4()));
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
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn normal_open_snapshot_cannot_overwrite_disk_identity_after_restart() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let disk = GridOrder {
        client_order_id: "gb_restart_identity".into(),
        order_id: Some(123),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -6,
        paired_client_order_id: Some("gb_parent".into()),
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    db.save_managed_order(&disk).unwrap();
    fake.orders.lock().unwrap().insert(
        disk.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 456,
            client_order_id: disk.client_order_id.clone(),
            symbol: disk.symbol.clone(),
            status: "NEW".into(),
            price: disk.price,
            avg_price: None,
            orig_qty: disk.quantity,
            executed_qty: Decimal::ZERO,
            side: "BUY".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    for _ in 0..2 {
        let mut restarted = engine(db.clone(), &url).await;
        assert!(!restarted.sync_live_orders().await);
        assert_eq!(
            restarted.state.active_orders.read().await["gb_restart_identity"].order_id,
            Some(123)
        );
        assert_eq!(
            db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
            Some(123)
        );
    }
    {
        let mut orders = fake.orders.lock().unwrap();
        let mut observed = orders.remove("gb_restart_identity").unwrap();
        observed.status = "CANCELED".into();
        let mut trusted = observed.clone();
        trusted.order_id = 123;
        trusted.status = "NEW".into();
        orders.insert("observed_restart".into(), observed);
        orders.insert("gb_restart_identity".into(), trusted);
    }
    let mut restarted = engine(db.clone(), &url).await;
    assert!(restarted.sync_live_orders().await);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(saved[0].order_id, Some(123));
    assert_eq!(saved[0].grid_level, -6);
    assert_eq!(
        saved[0].paired_client_order_id.as_deref(),
        Some("gb_parent")
    );
    server.abort();
}

#[tokio::test]
async fn existing_identity_barrier_is_rebased_to_restored_disk_identity() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    let disk = GridOrder {
        client_order_id: "gb_rebased".into(),
        order_id: Some(123),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -7,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    db.save_managed_order(&disk).unwrap();
    let mut memory = disk.clone();
    memory.order_id = Some(456);
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(memory.client_order_id.clone(), memory.clone());
    let key = (
        engine.client.account_identity_key(),
        memory.client_order_id.clone(),
    );
    engine.evidence.mark_provisional(key.0, &key.1);
    engine.evidence.insert_conflict(key.0, memory);
    fake.orders.lock().unwrap().insert(
        disk.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 456,
            client_order_id: disk.client_order_id.clone(),
            symbol: disk.symbol.clone(),
            status: "NEW".into(),
            price: disk.price,
            avg_price: None,
            orig_qty: disk.quantity,
            executed_qty: Decimal::ZERO,
            side: "BUY".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    for _ in 0..2 {
        assert!(!engine.sync_live_orders().await);
        assert_eq!(
            engine.state.active_orders.read().await["gb_rebased"].order_id,
            Some(123)
        );
        assert_eq!(
            db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
            Some(123)
        );
    }
    {
        let mut orders = fake.orders.lock().unwrap();
        let mut observed = orders.remove("gb_rebased").unwrap();
        observed.status = "CANCELED".into();
        let mut trusted = observed.clone();
        trusted.order_id = 123;
        trusted.status = "NEW".into();
        orders.insert("observed_rebased".into(), observed);
        orders.insert("gb_rebased".into(), trusted);
    }
    assert!(engine.sync_live_orders().await);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(saved[0].order_id, Some(123));
    assert_eq!(saved[0].grid_level, -7);
    server.abort();
}

#[tokio::test]
async fn resolved_identity_groups_cleanup_is_atomic_across_both_tables() {
    for rejected_table in ["initial_order_responses", "order_identity_observations"] {
        let path =
            std::env::temp_dir().join(format!("identity-cleanup-{}.db", uuid::Uuid::new_v4()));
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        let queried_a = Arc::new(AtomicU8::new(0));
        let queried_b = Arc::new(AtomicU8::new(0));
        let snapshots = Arc::new(AtomicU8::new(0));
        let app = Router::new()
            .route("/fapi/v1/openOrders", get({
                let snapshots = snapshots.clone();
                move || { let snapshots = snapshots.clone(); async move {
                    snapshots.fetch_add(1, Ordering::SeqCst);
                    Json(json!([
                        {"orderId":123,"clientOrderId":"gb_cleanup_a","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                        {"orderId":234,"clientOrderId":"gb_cleanup_b","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}
                    ]))
                }}
            }))
            .route("/fapi/v1/order", get({
                let queried_a = queried_a.clone();
                let queried_b = queried_b.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    let queried_a = queried_a.clone();
                    let queried_b = queried_b.clone();
                    async move {
                        let id = match query.get("orderId") {
                            Some(id) => id.parse::<i64>().unwrap(),
                            None => match query.get("origClientOrderId").map(String::as_str) {
                                Some("gb_cleanup_a") => 123,
                                Some("gb_cleanup_b") => 234,
                                other => panic!("unexpected client order ID: {other:?}"),
                            },
                        };
                        if id == 123 { queried_a.fetch_add(1, Ordering::SeqCst); }
                        else if id == 234 { queried_b.fetch_add(1, Ordering::SeqCst); }
                        else { panic!("unexpected order ID: {id}"); }
                        Json(json!({"orderId":id,"clientOrderId":if id == 123 {"gb_cleanup_a"} else {"gb_cleanup_b"},
                            "symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0",
                            "side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
                    }
                }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        let mut original_responses = HashMap::new();
        for (client, id) in [("gb_cleanup_a", 123), ("gb_cleanup_b", 234)] {
            let expected = GridOrder {
                client_order_id: client.into(),
                order_id: Some(id),
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
            db.save_identity_observation(
                account,
                &expected,
                &crate::types::ObservedOrderIdentity {
                    client_order_id: client.into(),
                    symbol: "SOLUSDC".into(),
                    side: "SELL".into(),
                    order_id: id,
                },
            )
            .unwrap();
            let response: BinanceOrderResponse =
                serde_json::from_value(json!({"orderId":id,"clientOrderId":client,
                "symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0",
                "side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
                .unwrap();
            original_responses.insert(client.to_string(), serde_json::to_value(&response).unwrap());
            db.save_initial_order_responses(account, "SOLUSDC", client, &[response])
                .unwrap();
        }
        connection.execute_batch(&format!("CREATE TRIGGER reject_group_cleanup BEFORE DELETE ON {rejected_table} \
            WHEN OLD.client_order_id = 'gb_cleanup_b' BEGIN SELECT RAISE(ABORT, 'injected failure'); END;")).unwrap();
        assert!(!first.sync_live_orders().await, "table={rejected_table}");
        assert!(
            queried_a.load(Ordering::SeqCst) > 0,
            "first group was not verified"
        );
        assert!(
            queried_b.load(Ordering::SeqCst) > 0,
            "second group was not verified"
        );
        assert!(snapshots.load(Ordering::SeqCst) > 0);
        assert!(first.reconciliation_blocked);
        assert!(first.last_orders_sync.is_none());
        let mut first_responses = HashMap::new();
        let mut first_observations = HashMap::new();
        for client in ["gb_cleanup_a", "gb_cleanup_b"] {
            assert!(first.evidence.has_conflict(account, client));
            assert!(first.evidence.has_observations(account, client));
            assert!(first.evidence.is_provisional(account, client));
            let responses = db
                .load_initial_order_responses(account, "SOLUSDC", client)
                .unwrap();
            assert!(!responses.is_empty());
            let values: Vec<_> = responses
                .iter()
                .map(|response| serde_json::to_value(response).unwrap())
                .collect();
            assert_eq!(values[0], original_responses[client]);
            assert!(values
                .iter()
                .all(|value| value == &original_responses[client]));
            first_responses.insert(client.to_string(), values);
            let mut observations: Vec<_> = db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .into_iter()
                .filter(|(expected, _)| expected.client_order_id == client)
                .collect();
            assert_eq!(observations.len(), 2);
            observations.sort_by(|a, b| a.1.side.cmp(&b.1.side));
            for (index, side) in ["BUY", "SELL"].into_iter().enumerate() {
                assert_eq!(observations[index].1.side, side);
                assert_eq!(observations[index].1.client_order_id, client);
                assert_eq!(observations[index].1.symbol, "SOLUSDC");
                assert_eq!(
                    observations[index].1.order_id,
                    if client == "gb_cleanup_a" { 123 } else { 234 }
                );
            }
            first_observations.insert(
                client.to_string(),
                serde_json::to_value(&observations).unwrap(),
            );
        }
        drop(first);
        let mut restarted = engine(db.clone(), &url).await;
        assert!(!restarted.sync_live_orders().await);
        assert!(restarted.reconciliation_blocked);
        for client in ["gb_cleanup_a", "gb_cleanup_b"] {
            assert!(restarted.evidence.has_conflict(account, client));
            assert!(restarted.evidence.has_observations(account, client));
            let responses = db
                .load_initial_order_responses(account, "SOLUSDC", client)
                .unwrap();
            assert!(!responses.is_empty());
            let values: Vec<_> = responses
                .iter()
                .map(|response| serde_json::to_value(response).unwrap())
                .collect();
            assert!(values.starts_with(&first_responses[client]));
            assert!(values
                .iter()
                .all(|value| value == &original_responses[client]));
            let mut observations: Vec<_> = db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .into_iter()
                .filter(|(expected, _)| expected.client_order_id == client)
                .collect();
            observations.sort_by(|a, b| a.1.side.cmp(&b.1.side));
            assert_eq!(
                serde_json::to_value(&observations).unwrap(),
                first_observations[client]
            );
        }
        connection
            .execute_batch("DROP TRIGGER reject_group_cleanup")
            .unwrap();
        assert!(restarted.sync_live_orders().await);
        assert!(db
            .load_identity_observations(account, "SOLUSDC")
            .unwrap()
            .is_empty());
        for client in ["gb_cleanup_a", "gb_cleanup_b"] {
            assert!(db
                .load_initial_order_responses(account, "SOLUSDC", client)
                .unwrap()
                .is_empty());
            assert!(!restarted.evidence.has_conflict(account, client));
        }
        server.abort();
        drop(connection);
        drop(restarted);
        drop(db);
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn all_conflict_groups_record_new_observations_before_any_lookup_can_block() {
    let path = std::env::temp_dir().join(format!("identity-batch-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async {
            Json(json!([
                {"orderId":789,"clientOrderId":"gb_group_a","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                {"orderId":890,"clientOrderId":"gb_group_b","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}
            ]))
        }))
        .route("/fapi/v1/order", get(|Query(query): Query<HashMap<String, String>>| async move {
            let id = query["orderId"].parse::<i64>().unwrap();
            let client = if id == 456 || id == 789 {"gb_group_a"} else {"gb_group_b"};
            Json(json!({"orderId":id,"clientOrderId":client,"symbol":"SOLUSDC","status":"FILLED",
                "price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    for (client, expected_id, observed_id) in [("gb_group_a", 123, 456), ("gb_group_b", 234, 567)] {
        let order = GridOrder {
            client_order_id: client.into(),
            order_id: Some(expected_id),
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
        db.save_identity_observation(
            account,
            &order,
            &crate::types::ObservedOrderIdentity {
                client_order_id: client.into(),
                symbol: "SOLUSDC".into(),
                side: "BUY".into(),
                order_id: observed_id,
            },
        )
        .unwrap();
    }
    connection.execute_batch("CREATE TRIGGER reject_later_observation BEFORE INSERT ON order_identity_observations \
        WHEN NEW.observed_order_id IN (789,890) AND EXISTS (SELECT 1 FROM order_identity_observations WHERE observed_order_id IN (789,890)) \
        BEGIN SELECT RAISE(ABORT, 'batch failure'); END;").unwrap();
    assert!(!first.sync_live_orders().await);
    let observed = db.load_identity_observations(account, "SOLUSDC").unwrap();
    let ids: std::collections::HashSet<_> =
        observed.iter().map(|(_, item)| item.order_id).collect();
    assert_eq!(ids, [456, 567].into_iter().collect());
    assert!(first.reconciliation_blocked);
    assert!(!first.state.global_position_ready.load(Ordering::SeqCst));
    assert_eq!(
        first
            .evidence
            .order_identity_observations
            .values()
            .flat_map(|obs| obs.values())
            .map(Vec::len)
            .sum::<usize>(),
        4
    );
    connection
        .execute_batch("DROP TRIGGER reject_later_observation")
        .unwrap();
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert_eq!(
        db.load_identity_observations(account, "SOLUSDC")
            .unwrap()
            .len(),
        4
    );
    server.abort();
    drop(connection);
    drop(restarted);
    drop(db);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn unknown_order_id_requires_unique_client_lookup_and_survives_restart() {
    let db = database();
    let stage = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async {
            Json(json!([{"orderId":123,"clientOrderId":"gb_unknown_id","symbol":"SOLUSDC",
                "status":"NEW","price":"100","origQty":"2","executedQty":"0",
                "side":"SELL","type":"LIMIT","timeInForce":"GTX"}]))
        }))
        .route("/fapi/v1/order", get({
            let stage = stage.clone();
            move |Query(query): Query<HashMap<String, String>>| {
                let stage = stage.clone();
                async move {
                    let wrong = query.contains_key("origClientOrderId") && stage.load(Ordering::SeqCst) == 0;
                    let queried_id = query.get("orderId").and_then(|s| s.parse::<i64>().ok()).unwrap_or(123);
                    let alternate = wrong || queried_id == 456;
                    Json(json!({"orderId":if alternate {456} else {123},"clientOrderId":"gb_unknown_id","symbol":"SOLUSDC",
                        "status":if alternate {"CANCELED"} else {"PARTIALLY_FILLED"},"price":"100","origQty":"2",
                        "executedQty":if alternate {"0"} else {"1"},
                        "side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
                }
            }
        }))
        .route("/fapi/v2/positionRisk", get({
            let stage = stage.clone();
            move || { let stage = stage.clone(); async move {
                if stage.load(Ordering::SeqCst) < 2 { return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({}))); }
                (StatusCode::OK, Json(json!([{"symbol":"SOLUSDC","positionAmt":"2","entryPrice":"100","markPrice":"100",
                    "unRealizedProfit":"0","liquidationPrice":"0","leverage":"20"}])))
            }}
        }))
        .route("/fapi/v2/account", get(|| async {
            Json(json!({"totalWalletBalance":"1000","totalMarginBalance":"1000","totalUnrealizedProfit":"0",
                "availableBalance":"1000","assets":[{"asset":"USDC","walletBalance":"1000","marginBalance":"1000",
                "availableBalance":"1000","unrealizedProfit":"0"}]}))
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut first = engine(db.clone(), &url).await;
    let expected = GridOrder {
        client_order_id: "gb_unknown_id".into(),
        order_id: None,
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
    assert_eq!(
        db.load_identity_observations(account, "SOLUSDC").unwrap()[0]
            .0
            .order_id,
        None
    );
    stage.store(1, Ordering::SeqCst);
    assert!(!first.sync_live_orders().await);
    assert_eq!(
        db.load_identity_observations(account, "SOLUSDC").unwrap()[0]
            .0
            .order_id,
        Some(123)
    );
    drop(first);
    stage.store(2, Ordering::SeqCst);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(restarted.sync_live_orders().await);
    let active = restarted.state.active_orders.read().await;
    assert_eq!(active["gb_unknown_id"].order_id, Some(123));
    assert_eq!(active["gb_unknown_id"].status, OrderStatus::PartiallyFilled);
    assert_eq!(active["gb_unknown_id"].quantity, dec!(1));
    assert_eq!(restarted.state.position.read().await.size, dec!(2));
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    server.abort();
}

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
