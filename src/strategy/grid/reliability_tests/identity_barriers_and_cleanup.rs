use super::*;

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
        let dir = crate::test_support::TempTestDir::new("identity-cleanup");
        let path = dir.join("identity-cleanup.db");
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
    }
}

#[tokio::test]
async fn all_conflict_groups_record_new_observations_before_any_lookup_can_block() {
    let dir = crate::test_support::TempTestDir::new("identity-batch");
    let path = dir.join("identity-batch.db");
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
