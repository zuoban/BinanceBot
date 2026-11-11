use super::*;

#[tokio::test]
async fn persistent_duplicate_snapshot_resolves_through_unique_query_and_real_price() {
    for canceled_other in [false, true] {
        for reversed in [false, true] {
            let db = database();
            let lookups = Arc::new(AtomicU8::new(0));
            let first = json!({"orderId":123,"clientOrderId":"gb_query_duplicate","symbol":"SOLUSDC",
                "status":"NEW","price":if canceled_other {"95"} else {"150"},"origQty":if canceled_other {"2"} else {"3"},
                "executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"});
            let second = json!({"orderId":if canceled_other {456} else {123},"clientOrderId":"gb_query_duplicate","symbol":"SOLUSDC",
                "status":"NEW","price":if canceled_other {"150"} else {"120"},"origQty":if canceled_other {"3"} else {"2"},
                "executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"});
            let snapshot = if reversed {
                vec![second, first]
            } else {
                vec![first, second]
            };
            let app = Router::new()
                .route("/fapi/v1/openOrders", get(move || {
                    let snapshot = snapshot.clone();
                    async move { Json(snapshot) }
                }))
                .route("/fapi/v1/order", get({
                    let lookups = lookups.clone();
                    move |Query(query): Query<HashMap<String, String>>| {
                        let lookups = lookups.clone();
                        async move {
                            if query.contains_key("origClientOrderId") { lookups.fetch_add(1, Ordering::SeqCst); }
                            let canceled = query.get("orderId").map(String::as_str) == Some("456");
                            Json(json!({"orderId":if canceled {456} else {123},"clientOrderId":"gb_query_duplicate",
                                "symbol":"SOLUSDC","status":if canceled {"CANCELED"} else {"NEW"},
                                "price":if canceled {"150"} else {"95"},"origQty":if canceled {"3"} else {"2"},"executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
                        }
                    }
                }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let mut engine = engine(db.clone(), &url).await;
            let account = engine.client.account_identity_key();
            assert!(!engine.sync_live_orders().await);
            assert!(!db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .is_empty());
            assert_eq!(
                engine.state.active_orders.read().await["gb_query_duplicate"].price,
                dec!(150)
            );
            assert_eq!(
                engine.state.active_orders.read().await["gb_query_duplicate"].quantity,
                dec!(3)
            );
            assert!(db.get_recent_trades(10).unwrap().is_empty());
            if !canceled_other {
                for _ in 0..2 {
                    assert!(!engine.sync_live_orders().await, "reversed={reversed}");
                    assert!(engine.reconciliation_blocked);
                    assert!(db.get_recent_trades(10).unwrap().is_empty());
                }
                let responses = db
                    .load_initial_order_responses(account, "SOLUSDC", "gb_query_duplicate")
                    .unwrap();
                for (price, quantity) in [(dec!(150), dec!(3)), (dec!(120), dec!(2))] {
                    assert!(responses.iter().any(|response| response.order_id == 123
                        && response.price == price
                        && response.orig_qty == quantity));
                }
                let disagreements = db
                    .load_terminal_order_disagreements(account, "SOLUSDC")
                    .unwrap();
                assert!(disagreements.iter().any(|(client, first, second)| client
                    == "gb_query_duplicate"
                    && first.order_id == 123
                    && second.order_id == 123
                    && (first.price != second.price || first.orig_qty != second.orig_qty)));
                drop(engine);
                let mut restarted = self::engine(db.clone(), &url).await;
                assert!(!restarted.sync_live_orders().await);
                assert!(restarted.reconciliation_blocked);
                assert!(db.get_recent_trades(10).unwrap().is_empty());
                let responses = db
                    .load_initial_order_responses(account, "SOLUSDC", "gb_query_duplicate")
                    .unwrap();
                for (price, quantity) in [(dec!(150), dec!(3)), (dec!(120), dec!(2))] {
                    assert!(responses.iter().any(|response| response.order_id == 123
                        && response.price == price
                        && response.orig_qty == quantity));
                }
                assert!(!db
                    .load_terminal_order_disagreements(account, "SOLUSDC")
                    .unwrap()
                    .is_empty());
                server.abort();
                continue;
            }
            assert!(
                engine.sync_live_orders().await,
                "canceled_other={canceled_other}, reversed={reversed}"
            );
            assert!(lookups.load(Ordering::SeqCst) > 0);
            assert!(!engine.reconciliation_blocked);
            assert!(db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .is_empty());
            let active = engine.state.active_orders.read().await;
            let order = &active["gb_query_duplicate"];
            assert_eq!(order.order_id, Some(123));
            assert_eq!(order.price, dec!(95));
            assert_eq!(order.quantity, dec!(2));
            assert_eq!(order.amount_usdc, dec!(190));
            drop(active);
            let saved = db.load_managed_orders("SOLUSDC").unwrap();
            assert_eq!(saved.len(), 1);
            assert_eq!(saved[0].price, dec!(95));
            assert_eq!(saved[0].amount_usdc, dec!(190));
            assert!(db.get_recent_trades(10).unwrap().is_empty());
            server.abort();
        }
    }
}

#[tokio::test]
async fn suspicious_and_ordinary_conflicts_in_same_snapshot_are_all_recorded() {
    for invalid in [false, true] {
        let db = database();
        let mut snapshot = vec![
            json!({"orderId":456,"clientOrderId":"gb_batch_known","symbol":"SOLUSDC",
            "status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}),
            json!({"orderId":11,"clientOrderId":"gb_batch_suspicious","symbol":"SOLUSDC",
            "status":"NEW","price":"100","origQty":"1","executedQty":"0",
            "side":if invalid {"INVALID"} else {"BUY"},"type":"LIMIT","timeInForce":"GTX"}),
        ];
        if !invalid {
            snapshot.push(json!({"orderId":12,"clientOrderId":"gb_batch_suspicious","symbol":"SOLUSDC",
                "status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}));
        }
        let app = Router::new().route(
            "/fapi/v1/openOrders",
            get(move || {
                let snapshot = snapshot.clone();
                async move { Json(snapshot) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let order = GridOrder {
            client_order_id: "gb_batch_known".into(),
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
        let mut engine = engine(db.clone(), &url).await;
        let account = engine.client.account_identity_key();
        assert!(!engine.sync_live_orders().await);
        assert!(engine.reconciliation_blocked);
        let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
        assert!(observations
            .iter()
            .any(
                |(expected, found)| expected.client_order_id == "gb_batch_known"
                    && found.order_id == 456
            ));
        assert!(observations
            .iter()
            .any(
                |(expected, found)| expected.client_order_id == "gb_batch_suspicious"
                    && found.order_id == 11
            ));
        if !invalid {
            assert!(observations
                .iter()
                .any(
                    |(expected, found)| expected.client_order_id == "gb_batch_suspicious"
                        && found.order_id == 12
                ));
        }
        assert_eq!(
            db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
            Some(123)
        );
        server.abort();
    }
}

#[tokio::test]
async fn untrusted_invalid_side_can_resolve_to_uniquely_confirmed_sell() {
    let db = database();
    let stage = Arc::new(AtomicU8::new(0));
    let client_lookups = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get({
            let stage = stage.clone();
            move || { let stage = stage.clone(); async move {
                if stage.load(Ordering::SeqCst) != 0 { return Json(json!([])); }
                Json(json!([{"orderId":123,"clientOrderId":"gb_invalid_side","symbol":"SOLUSDC",
                    "status":"NEW","price":"100","origQty":"1","executedQty":"0",
                    "side":"INVALID","type":"LIMIT","timeInForce":"GTX"}]))
            }}
        }))
        .route("/fapi/v1/order", get({
            let client_lookups = client_lookups.clone();
            move |Query(query): Query<HashMap<String, String>>| {
                let client_lookups = client_lookups.clone();
                async move {
                    if query.contains_key("origClientOrderId") { client_lookups.fetch_add(1, Ordering::SeqCst); }
                    Json(json!({"orderId":123,"clientOrderId":"gb_invalid_side","symbol":"SOLUSDC",
                        "status":"NEW","price":"100","origQty":"1","executedQty":"0",
                        "side":"SELL","type":"LIMIT","timeInForce":"GTX"}))
                }
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    assert!(!first.sync_live_orders().await);
    drop(first);
    stage.store(1, Ordering::SeqCst);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(restarted.sync_live_orders().await);
    assert!(client_lookups.load(Ordering::SeqCst) > 0);
    let active = restarted.state.active_orders.read().await;
    assert_eq!(active["gb_invalid_side"].order_id, Some(123));
    assert_eq!(active["gb_invalid_side"].side, OrderSide::Sell);
    drop(active);
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    server.abort();
}

#[tokio::test]
async fn provisional_reservation_grows_without_changing_managed_metadata() {
    let db = database();
    let stage = Arc::new(AtomicU8::new(0));
    let app = Router::new().route("/fapi/v1/openOrders", get({
        let stage = stage.clone();
        move || { let stage = stage.clone(); async move {
            let later = stage.load(Ordering::SeqCst) != 0;
            Json(json!([
                {"orderId":11,"clientOrderId":"gb_growing","symbol":"SOLUSDC","status":"NEW",
                    "price":if later {"120"} else {"100"},"origQty":if later {"4"} else {"1"},
                    "executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                {"orderId":12,"clientOrderId":"gb_growing","symbol":"SOLUSDC","status":"NEW",
                    "price":if later {"130"} else {"101"},"origQty":if later {"5"} else {"2"},
                    "executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                {"orderId":123,"clientOrderId":"gb_preserved","symbol":"SOLUSDC","status":"NEW",
                    "price":"140","origQty":"6","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                {"orderId":456,"clientOrderId":"gb_preserved","symbol":"SOLUSDC","status":"NEW",
                    "price":"150","origQty":"7","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}
            ]))
        }}
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let order = GridOrder {
        client_order_id: "gb_preserved".into(),
        order_id: Some(123),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(90),
        quantity: dec!(2),
        amount_usdc: dec!(180),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -8,
        paired_client_order_id: Some("gb_parent".into()),
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    db.save_managed_order(&order).unwrap();
    let mut engine = engine(db.clone(), &url).await;
    assert!(!engine.sync_live_orders().await);
    stage.store(1, Ordering::SeqCst);
    assert!(!engine.sync_live_orders().await);
    assert!(engine.reconciliation_blocked);
    let active = engine.state.active_orders.read().await;
    let provisional = &active["gb_growing"];
    assert_eq!(provisional.order_id, None);
    assert_eq!(provisional.price, dec!(130));
    assert_eq!(provisional.quantity, dec!(5));
    assert_eq!(provisional.amount_usdc, dec!(650));
    let managed = &active["gb_preserved"];
    assert_eq!(managed.order_id, order.order_id);
    assert_eq!(managed.price, order.price);
    assert_eq!(managed.quantity, order.quantity);
    assert_eq!(managed.grid_level, order.grid_level);
    assert_eq!(managed.paired_client_order_id, order.paired_client_order_id);
    drop(active);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].price, order.price);
    assert_eq!(saved[0].quantity, order.quantity);
    assert_eq!(saved[0].grid_level, order.grid_level);
    assert_eq!(
        saved[0].paired_client_order_id,
        order.paired_client_order_id
    );
    server.abort();
}

#[tokio::test]
async fn untrusted_snapshot_identities_survive_restart_and_missing_snapshot() {
    for case in ["status", "quantity", "price", "symbol", "side"] {
        for reversed in [false, true] {
            let db = database();
            let stage = Arc::new(AtomicU8::new(0));
            let lookups = Arc::new(AtomicU8::new(0));
            let good = json!({"orderId":123,"clientOrderId":"gb_untrusted_snapshot","symbol":"SOLUSDC",
                "status":"NEW","price":"100","origQty":"2","executedQty":"0",
                "side":"BUY","type":"LIMIT","timeInForce":"GTX"});
            let bad = json!({"orderId":123,"clientOrderId":"gb_untrusted_snapshot",
                "symbol":if case == "symbol" {"BTCUSDC"} else {"SOLUSDC"},
                "status":if case == "status" {"PARTIALLY_FILLED"} else {"NEW"},
                "price":if case == "price" {"110"} else {"100"},
                "origQty":if case == "quantity" {"3"} else {"2"},
                "executedQty":if case == "status" {"1"} else {"0"},
                "side":if case == "side" {"INVALID"} else {"BUY"},"type":"LIMIT","timeInForce":"GTX"});
            let snapshot = if case == "symbol" || case == "side" {
                vec![bad]
            } else if reversed {
                vec![bad, good]
            } else {
                vec![good, bad]
            };
            let app = Router::new()
                .route(
                    "/fapi/v1/openOrders",
                    get({
                        let stage = stage.clone();
                        move || {
                            let snapshot = snapshot.clone();
                            let stage = stage.clone();
                            async move {
                                Json(if stage.load(Ordering::SeqCst) == 0 {
                                    snapshot
                                } else {
                                    vec![]
                                })
                            }
                        }
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
                                (
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    Json(json!({"code":-1007,"msg":"unavailable"})),
                                )
                            }
                        }
                    }),
                );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let mut first = engine(db.clone(), &url).await;
            let account = first.client.account_identity_key();
            assert!(
                !first.sync_live_orders().await,
                "case={case}, reversed={reversed}"
            );
            assert!(first.reconciliation_blocked);
            let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
            assert!(!observations.is_empty(), "case={case}, reversed={reversed}");
            assert!(observations
                .iter()
                .any(
                    |(_, observed)| observed.client_order_id == "gb_untrusted_snapshot"
                        && observed.order_id == 123
                        && observed.symbol
                            == if case == "symbol" {
                                "BTCUSDC"
                            } else {
                                "SOLUSDC"
                            }
                        && observed.side == if case == "side" { "INVALID" } else { "BUY" }
                ));
            drop(first);
            let mut restarted = engine(db.clone(), &url).await;
            assert!(!restarted.sync_live_orders().await);
            assert!(restarted.reconciliation_blocked);
            lookups.store(0, Ordering::SeqCst);
            stage.store(1, Ordering::SeqCst);
            assert!(!restarted.sync_live_orders().await);
            assert!(lookups.load(Ordering::SeqCst) > 0);
            assert!(restarted.reconciliation_blocked);
            assert!(restarted.last_orders_sync.is_none());
            assert!(!restarted.state.global_position_ready.load(Ordering::SeqCst));
            assert!(!db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .is_empty());
            assert_eq!(
                restarted.state.active_orders.read().await["gb_untrusted_snapshot"].order_id,
                None
            );
            server.abort();
        }
    }
}

#[tokio::test]
async fn unknown_duplicate_reservation_is_conservative_and_independent_of_input_order() {
    for indices in [[0, 1, 2, 3], [3, 2, 1, 0], [2, 0, 3, 1], [1, 3, 0, 2]] {
        let db = database();
        let records: Vec<_> = [(123, "90", "1"), (123, "100", "2"), (123, "110", "3"), (456, "105", "4")]
            .into_iter().map(|(id, price, quantity)| json!({"orderId":id,"clientOrderId":"gb_reservation_order",
                "symbol":"SOLUSDC","status":"NEW","price":price,"origQty":quantity,"executedQty":"0",
                "side":"BUY","type":"LIMIT","timeInForce":"GTX"})).collect();
        let snapshot: Vec<_> = indices
            .into_iter()
            .map(|index| records[index].clone())
            .collect();
        let app = Router::new().route(
            "/fapi/v1/openOrders",
            get(move || {
                let snapshot = snapshot.clone();
                async move { Json(snapshot) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut engine = engine(db.clone(), &url).await;
        assert!(!engine.sync_live_orders().await);
        assert!(engine.reconciliation_blocked);
        let active = engine.state.active_orders.read().await;
        let retained = &active["gb_reservation_order"];
        assert_eq!(retained.order_id, None);
        assert_eq!(retained.price, dec!(110));
        assert_eq!(retained.quantity, dec!(4));
        assert_eq!(retained.amount_usdc, dec!(440));
        drop(active);
        let observations = db
            .load_identity_observations(engine.client.account_identity_key(), "SOLUSDC")
            .unwrap();
        let ids: std::collections::HashSet<_> = observations
            .iter()
            .map(|(_, observed)| observed.order_id)
            .collect();
        assert_eq!(ids, [123, 456].into_iter().collect());
        server.abort();
    }
}

#[tokio::test]
async fn conflicting_duplicate_snapshot_does_not_adopt_unknown_order() {
    for reversed in [false, true] {
        let db = database();
        let first = json!({"orderId":123,"clientOrderId":"gb_duplicate_unknown","symbol":"SOLUSDC",
            "status":"NEW","price":"100","origQty":"1","executedQty":"0",
            "side":"BUY","type":"LIMIT","timeInForce":"GTX"});
        let second = json!({"orderId":456,"clientOrderId":"gb_duplicate_unknown","symbol":"SOLUSDC",
            "status":"NEW","price":"100","origQty":"1","executedQty":"0",
            "side":"BUY","type":"LIMIT","timeInForce":"GTX"});
        let snapshot = if reversed {
            vec![second, first]
        } else {
            vec![first, second]
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
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        assert!(!first.sync_live_orders().await);
        drop(first);
        let mut restarted = engine(db.clone(), &url).await;
        assert!(!restarted.sync_live_orders().await);
        assert!(restarted.reconciliation_blocked);
        assert!(restarted.last_orders_sync.is_none());
        assert!(!restarted.state.global_position_ready.load(Ordering::SeqCst));
        assert_eq!(
            restarted.state.active_orders.read().await["gb_duplicate_unknown"].order_id,
            None
        );
        assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
        let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
        let ids: std::collections::HashSet<_> = observations
            .iter()
            .map(|(_, observed)| observed.order_id)
            .collect();
        assert_eq!(ids, [123, 456].into_iter().collect());
        server.abort();
    }
}

#[tokio::test]
async fn switching_to_exchange_modes_requires_manual_resume_before_orders() {
    for (source, target, previously_running) in [
        (TradingMode::Paper, TradingMode::Live, false),
        (TradingMode::Paper, TradingMode::Live, true),
    ] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let mut engine = engine(db.clone(), &url).await;
        let mut initial = engine.state.config.read().await.clone();
        if source == TradingMode::Paper {
            initial.exchange.api_key.clear();
            initial.exchange.api_secret.clear();
        } else {
            initial.exchange.api_key = "live-key".into();
            initial.exchange.api_secret = "live-secret".into();
        }
        db.save_config(&initial).unwrap();
        engine
            .state
            .save_runtime_config(initial.clone())
            .await
            .unwrap();
        engine.state.resume_trading().await.unwrap();
        if previously_running {
            db.save_pair_status("SOLUSDC", target, BotStatus::Running)
                .unwrap();
        }
        let epoch = engine.state.pause_epoch();
        let mut exchange = initial;
        exchange.exchange.api_key = "live-key".into();
        exchange.exchange.api_secret = "live-secret".into();
        let (committed, _) = tokio::join!(
            engine.state.save_runtime_config(exchange.clone()),
            engine.state.resume_trading_silently(epoch)
        );
        committed.unwrap();
        assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
        assert_eq!(
            db.load_pair_status("SOLUSDC", target).unwrap(),
            Some(BotStatus::Paused)
        );
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
        let (tx, _) = mpsc::channel(1);
        let restored = AppState::new(exchange, db.clone(), tx);
        assert_eq!(*restored.status.read().await, BotStatus::Paused);
        assert!(!submit(&mut engine).await);
        assert!(fake.posts.lock().unwrap().is_empty());
        engine.state.resume_trading().await.unwrap();
        assert!(submit(&mut engine).await);
        assert_eq!(fake.posts.lock().unwrap().len(), 1);
        server.abort();
    }
}

#[tokio::test]
async fn backend_timeout_keeps_accepted_identity_and_restart_records_terminal_fill_once() {
    let fake = Exchange::default();
    fake.submission.store(1, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut first = engine(db.clone(), &url).await;
    assert!(!submit(&mut first).await);
    assert!(first.last_orders_sync.is_none());
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].order_id, Some(1));
    // Complete before the next open-orders poll, then restart.
    {
        let mut orders = fake.orders.lock().unwrap();
        let order = orders.get_mut("gb_b_reliability").unwrap();
        order.status = "FILLED".into();
        order.executed_qty = order.orig_qty;
        order.avg_price = Some(order.price);
    }
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    fake.position_unavailable.store(true, Ordering::SeqCst);
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted
        .state
        .active_orders
        .read()
        .await
        .contains_key("gb_b_reliability"));
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    fake.position_unavailable.store(false, Ordering::SeqCst);
    // Terminal reconciliation must still refresh both position and account
    // when the ordinary request budget has been exhausted.
    let limiter = Arc::new(crate::exchange::rate_limit::RateLimiter::default());
    limiter
        .reserve(
            &url,
            [0; 32],
            1800,
            crate::exchange::rate_limit::RequestClass::Normal,
        )
        .unwrap();
    restarted.client = Arc::new(BinanceFuturesClient::with_base_url_and_limiter(
        &restarted.state.config.read().await.exchange,
        url.clone(),
        limiter,
    ));
    assert!(restarted.sync_account_and_position().await);
    restarted.client = Arc::new(BinanceFuturesClient::with_base_url(
        &restarted.state.config.read().await.exchange,
        url.clone(),
    ));
    assert!(restarted.sync_live_orders().await);
    // Simulate a recorded terminal order restored alongside a pre-fill snapshot.
    fake.position_flat.store(true, Ordering::SeqCst);
    restarted
        .position_cache
        .get(&restarted.client, "SOLUSDC")
        .await
        .unwrap();
    restarted.state.position.write().await.size = dec!(0);
    let restored = saved[0].clone();
    restarted
        .state
        .active_orders
        .write()
        .await
        .insert(restored.client_order_id.clone(), restored);
    fake.position_flat.store(false, Ordering::SeqCst);
    fake.position_unavailable.store(true, Ordering::SeqCst);
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted
        .state
        .active_orders
        .read()
        .await
        .contains_key("gb_b_reliability"));
    fake.position_unavailable.store(false, Ordering::SeqCst);
    assert!(restarted.sync_live_orders().await);
    assert_eq!(restarted.state.position.read().await.size, dec!(1));
    assert_eq!(
        restarted
            .position_cache
            .get(&restarted.client, "SOLUSDC")
            .await
            .unwrap()
            .0
            .unwrap()
            .position_amt,
        dec!(1)
    );
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    assert_eq!(fake.posts.lock().unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn absent_unaccepted_intent_waits_then_retires_without_blocking_restart() {
    let fake = Exchange::default();
    fake.submission.store(2, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut first = engine(db.clone(), &url).await;
    assert!(!submit(&mut first).await);
    assert!(!first.sync_live_orders().await);
    let mut intent = db.load_managed_orders("SOLUSDC").unwrap().remove(0);
    assert!(intent.order_id.is_none());
    intent.created_at = Utc::now() - chrono::Duration::seconds(31);
    db.save_managed_order(&intent).unwrap();
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(restarted.sync_live_orders().await);
    assert!(restarted.state.active_orders.read().await.is_empty());
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    fake.submission.store(0, Ordering::SeqCst);
    assert!(
        restarted
            .place_grid_order(
                OrderSide::Buy,
                dec!(100),
                "gb_b_new_attempt".into(),
                -1,
                None
            )
            .await
    );
    assert_eq!(fake.posts.lock().unwrap().len(), 2);
    server.abort();
}

#[tokio::test]
async fn unavailable_lookup_never_releases_an_old_unknown_intent() {
    let fake = Exchange::default();
    fake.submission.store(2, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    assert!(!submit(&mut engine).await);
    engine
        .state
        .active_orders
        .write()
        .await
        .get_mut("gb_b_reliability")
        .unwrap()
        .created_at = Utc::now() - chrono::Duration::minutes(1);
    fake.lookup_unavailable.store(true, Ordering::SeqCst);
    assert!(!engine.sync_live_orders().await);
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
    assert_eq!(engine.state.active_orders.read().await.len(), 1);
    server.abort();
}

#[tokio::test]
async fn missing_already_accepted_order_is_never_retired_as_an_unsent_intent() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    assert!(submit(&mut engine).await);
    fake.orders.lock().unwrap().clear();
    engine
        .state
        .active_orders
        .write()
        .await
        .get_mut("gb_b_reliability")
        .unwrap()
        .created_at = Utc::now() - chrono::Duration::minutes(1);
    assert!(!engine.sync_live_orders().await);
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn direct_query_persists_acceptance_when_open_snapshot_omits_the_order() {
    let fake = Exchange::default();
    fake.submission.store(1, Ordering::SeqCst);
    fake.lookup_unavailable.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut first = engine(db.clone(), &url).await;
    assert!(!submit(&mut first).await);
    assert!(db.load_managed_orders("SOLUSDC").unwrap()[0]
        .order_id
        .is_none());
    fake.lookup_unavailable.store(false, Ordering::SeqCst);
    fake.hide_open_orders.store(true, Ordering::SeqCst);
    assert!(!first.sync_live_orders().await);
    let mut accepted = db.load_managed_orders("SOLUSDC").unwrap().remove(0);
    assert_eq!(accepted.order_id, Some(1));
    accepted.created_at = Utc::now() - chrono::Duration::minutes(1);
    db.save_managed_order(&accepted).unwrap();
    fake.orders.lock().unwrap().clear();
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn stale_last_or_mark_blocks_orders_even_when_account_reconciliation_succeeds() {
    for stale_mark in [false, true] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let mut engine = engine(database(), &url).await;
        {
            let mut ticker = engine.state.ticker.write().await;
            let stale = Utc::now() - chrono::Duration::seconds(16);
            if stale_mark {
                ticker.mark_update_time = stale;
            } else {
                ticker.update_time = stale;
            }
        }
        assert!(!submit(&mut engine).await);
        engine.sync_cycle(true).await;
        assert!(engine.last_account_sync.is_some());
        assert!(engine.last_orders_sync.is_some());
        assert!(fake.posts.lock().unwrap().is_empty());
        {
            let mut ticker = engine.state.ticker.write().await;
            ticker.update_time = Utc::now();
            ticker.mark_update_time = Utc::now();
        }
        engine.maintain_grid_window().await;
        assert!(!fake.posts.lock().unwrap().is_empty());
        server.abort();
    }
}

#[tokio::test]
async fn stale_remainder_plan_keeps_identity_until_fresh_prices_return() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    let now = Utc::now();
    let mut plan = RemainderPlan {
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        sources: vec![],
        phase: RemainderPhase::Submitting,
        target: GridOrder {
            client_order_id: "gb_s_stale_remainder".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(0.05),
            amount_usdc: dec!(5.05),
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
            grid_level: 1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: OrderPurpose::Remainder,
            merge_sources: vec![],
        },
    };
    db.save_remainder_plan(&plan).unwrap();
    engine.state.ticker.write().await.mark_update_time = now - chrono::Duration::seconds(16);
    assert!(engine.recover_remainder_plan().await.unwrap());
    assert!(fake.posts.lock().unwrap().is_empty());
    plan = db
        .load_remainder_plan("SOLUSDC", TradingMode::Live)
        .unwrap()
        .unwrap();
    assert_eq!(plan.phase, RemainderPhase::Submitting);
    engine.state.ticker.write().await.mark_update_time = Utc::now();
    engine.recover_remainder_plan().await.unwrap();
    assert_eq!(
        fake.posts.lock().unwrap().as_slice(),
        &["gb_s_stale_remainder"]
    );
    server.abort();
}

#[tokio::test]
async fn delayed_ticker_does_not_become_fresh_when_consumed() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let mut engine = engine(database(), &url).await;
    let previous = Utc::now() - chrono::Duration::seconds(20);
    engine.state.ticker.write().await.update_time = previous;
    let received = Utc::now() - chrono::Duration::seconds(16);
    engine
        .handle_ticker_update(TickerInfo {
            symbol: "SOLUSDC".into(),
            last_price: dec!(100.5),
            update_time: received,
            ..Default::default()
        })
        .await;
    assert_eq!(engine.state.ticker.read().await.update_time, previous);
    assert!(!submit(&mut engine).await);
    assert!(fake.posts.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn stale_mark_does_not_match_or_prune_existing_paper_orders() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    engine.state.config.write().await.exchange.api_key.clear();
    engine
        .state
        .config
        .write()
        .await
        .exchange
        .api_secret
        .clear();
    assert!(submit(&mut engine).await);
    engine.state.ticker.write().await.mark_update_time = Utc::now() - chrono::Duration::seconds(16);
    engine
        .handle_ticker_update(TickerInfo {
            symbol: "SOLUSDC".into(),
            last_price: dec!(95),
            update_time: Utc::now(),
            ..Default::default()
        })
        .await;
    engine.maintain_grid_window().await;
    assert_eq!(engine.state.active_orders.read().await.len(), 1);
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn shutdown_records_racing_fill_and_restart_stays_paused() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    assert!(submit(&mut engine).await);
    fake.fill_on_cancel.store(true, Ordering::SeqCst);
    engine.shutdown().await.unwrap();
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    assert!(engine.state.resume_trading().await.is_err());
    assert!(!submit(&mut engine).await);
    assert_eq!(fake.posts.lock().unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn failed_shutdown_preserves_orders_and_paused_status() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    assert!(submit(&mut engine).await);
    fake.cancel_fails.store(true, Ordering::SeqCst);
    assert!(engine.shutdown().await.is_err());
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
    assert_eq!(
        fake.orders.lock().unwrap()["gb_b_reliability"].status,
        "NEW"
    );
    server.abort();
}

#[tokio::test]
async fn shutdown_confirms_a_cancellation_whose_response_was_lost() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    assert!(submit(&mut engine).await);
    fake.lose_cancel_response.store(true, Ordering::SeqCst);
    engine.shutdown().await.unwrap();
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    assert_eq!(
        fake.orders.lock().unwrap()["gb_b_reliability"].status,
        "CANCELED"
    );
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    server.abort();
}

#[tokio::test]
async fn interrupted_initialization_still_reconciles_persisted_orders_on_shutdown() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut first = engine(db.clone(), &url).await;
    assert!(submit(&mut first).await);
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    restarted.state.active_orders.write().await.clear();
    fake.fill_on_cancel.store(true, Ordering::SeqCst);
    restarted.shutdown().await.unwrap();
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn shutdown_skips_queued_resume_and_configuration_then_joins_engine() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let mut engine = engine(database(), &url).await;
    let state = engine.state.clone();
    state.request_shutdown();
    let (resume_reply, resume_response) = tokio::sync::oneshot::channel();
    state
        .action_tx
        .send(BotControlAction::Resume {
            expected_mode: TradingMode::Live,
            expected_epoch: state.pause_epoch(),
            reply: resume_reply,
        })
        .await
        .unwrap();
    let (reply, response) = tokio::sync::oneshot::channel();
    let config = state.config.read().await.clone();
    state
        .action_tx
        .send(BotControlAction::UpdateConfig {
            config: Box::new(config),
            reply,
        })
        .await
        .unwrap();
    let mut task = tokio::spawn(async move { engine.run().await });
    stop_engine(&state, &mut task, Duration::from_secs(2))
        .await
        .unwrap();
    assert!(resume_response.await.unwrap().is_err());
    assert!(response.await.unwrap().is_err());
    assert!(task.is_finished());
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert!(fake.posts.lock().unwrap().is_empty());
    server.abort();
}
