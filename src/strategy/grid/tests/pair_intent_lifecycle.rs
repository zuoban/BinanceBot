use super::*;

#[test]
fn grid_level_occupied_respects_side_and_spacing() {
    let order = GridOrder {
        client_order_id: "gb_b_occupancy".into(),
        order_id: None,
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
    let orders = [order];
    assert!(grid_level_occupied(
        &orders,
        OrderSide::Buy,
        dec!(100.99),
        dec!(1),
        dec!(0.01)
    ));
    assert!(!grid_level_occupied(
        &orders,
        OrderSide::Buy,
        dec!(101),
        dec!(1),
        dec!(0.01)
    ));
    assert!(!grid_level_occupied(
        &orders,
        OrderSide::Sell,
        dec!(100),
        dec!(1),
        dec!(0.01)
    ));
    assert!(!grid_level_occupied(
        &[],
        OrderSide::Buy,
        dec!(100),
        dec!(1),
        dec!(0.01)
    ));
    assert!(grid_level_occupied(
        &orders,
        OrderSide::Buy,
        dec!(100.99),
        dec!(0.01),
        dec!(1)
    ));
    assert!(!grid_level_occupied(
        &orders,
        OrderSide::Buy,
        dec!(101),
        dec!(0.01),
        dec!(1)
    ));
}

#[test]
fn small_price_moves_do_not_duplicate_grid_orders() {
    let existing = [dec!(113.13), dec!(112.13), dec!(111.13)];
    assert!(has_nearby_grid_order(
        &existing,
        dec!(113.15),
        dec!(1),
        dec!(0.01)
    ));
    assert!(has_nearby_grid_order(
        &existing,
        dec!(112.15),
        dec!(1),
        dec!(0.01)
    ));
    assert!(!has_nearby_grid_order(
        &existing,
        dec!(114.15),
        dec!(1),
        dec!(0.01)
    ));
}

#[test]
fn managed_order_prefix_excludes_manual_orders() {
    assert!(is_grid_order("gb_b_123"));
    assert!(!is_grid_order("manual-123"));
}

#[test]
fn paired_ids_are_stable_and_bounded() {
    let parent = "gb_b_123456789012345678901234567890";
    let sell = paired_grid_client_order_id(parent, OrderSide::Sell);
    assert_eq!(sell, paired_grid_client_order_id(parent, OrderSide::Sell));
    assert_ne!(sell, paired_grid_client_order_id(parent, OrderSide::Buy));
    assert!(sell.len() <= 36);
}

#[tokio::test]
async fn order_reconciliation_failure_revokes_readiness_until_full_recovery() {
    let (config, db, _intent) = pending_pair_fixture();
    let fake = PairExchange::default();
    let (url, server) = pair_exchange_server(fake).await;
    let mut engine = pair_recovery_engine(&config, db, url).await;
    let state = engine.state.clone();
    state
        .global_position_initialized
        .store(true, std::sync::atomic::Ordering::SeqCst);
    state
        .global_position_ready
        .store(true, std::sync::atomic::Ordering::SeqCst);
    engine.report_reconciliation_state(true).await;
    assert!(!state
        .global_position_ready
        .load(std::sync::atomic::Ordering::SeqCst));
    engine.report_reconciliation_state(false).await;
    assert!(!state
        .global_position_ready
        .load(std::sync::atomic::Ordering::SeqCst));
    engine.set_global_reconciliation_ready(true).await;
    assert!(state
        .global_position_ready
        .load(std::sync::atomic::Ordering::SeqCst));
    server.abort();
}

#[tokio::test]
async fn accepted_legacy_pair_is_adopted_after_restart_without_resubmission() {
    let (config, db, intent) = pending_pair_fixture();
    let fake = PairExchange::default();
    fake.orders.lock().unwrap().insert(
        intent.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 501,
            client_order_id: intent.client_order_id.clone(),
            symbol: intent.symbol.clone(),
            status: "NEW".into(),
            price: intent.price,
            avg_price: None,
            orig_qty: intent.quantity,
            executed_qty: Decimal::ZERO,
            side: "SELL".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut restarted = pair_recovery_engine(&config, db.clone(), url).await;
    assert!(restarted.sync_live_orders().await);
    assert!(fake.posts.lock().unwrap().is_empty());
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    let stored = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(
        stored[0].paired_client_order_id,
        intent.paired_client_order_id
    );
    server.abort();
}

#[tokio::test]
async fn provisional_pair_uses_intent_metadata_for_normal_and_terminal_recovery() {
    for terminal in [false, true] {
        let (config, db, intent) = pending_pair_fixture();
        let fake = PairExchange::default();
        fake.orders.lock().unwrap().insert(
            intent.client_order_id.clone(),
            BinanceOrderResponse {
                order_id: 501,
                client_order_id: intent.client_order_id.clone(),
                symbol: intent.symbol.clone(),
                status: if terminal { "FILLED" } else { "NEW" }.into(),
                price: intent.price,
                avg_price: terminal.then_some(intent.price),
                orig_qty: intent.quantity,
                executed_qty: if terminal {
                    intent.quantity
                } else {
                    Decimal::ZERO
                },
                side: intent.side.as_str().into(),
                order_type: "LIMIT".into(),
                time_in_force: "GTX".into(),
                update_time: None,
            },
        );
        let (url, server) = pair_exchange_server(fake).await;
        let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
        let mut placeholder = intent.clone();
        placeholder.order_id = Some(501);
        placeholder.paired_client_order_id = None;
        placeholder.purpose = crate::types::OrderPurpose::Legacy;
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(placeholder.client_order_id.clone(), placeholder);
        engine.evidence.mark_provisional(
            engine.client.account_identity_key(),
            &intent.client_order_id,
        );
        assert!(engine.sync_live_orders().await);
        if terminal {
            let trades = db.get_recent_trades(10).unwrap();
            let closing = trades
                .iter()
                .find(|trade| trade.client_order_id == intent.client_order_id)
                .unwrap();
            assert!(closing.note.contains("Completed Grid Cycle"));
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                0
            );
        } else {
            let stored = db.load_managed_orders("SOLUSDC").unwrap();
            assert_eq!(
                stored[0].paired_client_order_id,
                intent.paired_client_order_id
            );
            assert_eq!(stored[0].purpose, intent.purpose);
        }
        server.abort();
    }
}

#[tokio::test]
async fn bulk_cancel_unknown_pair_preserves_intent_metadata_before_recording_fill() {
    let (config, db, intent) = pending_pair_fixture();
    let fake = PairExchange::default();
    fake.fail_cancel.store(1, Ordering::SeqCst);
    fake.filled_zero_lookup.store(1, Ordering::SeqCst);
    fake.orders.lock().unwrap().insert(
        intent.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 501,
            client_order_id: intent.client_order_id.clone(),
            symbol: intent.symbol.clone(),
            status: "NEW".into(),
            price: intent.price,
            avg_price: None,
            orig_qty: intent.quantity,
            executed_qty: Decimal::ZERO,
            side: intent.side.as_str().into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    assert!(!engine.cancel_all_orders().await);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(
        saved[0].paired_client_order_id,
        intent.paired_client_order_id
    );
    assert_eq!(saved[0].purpose, intent.purpose);
    {
        let mut orders = fake.orders.lock().unwrap();
        let order = orders.get_mut(&intent.client_order_id).unwrap();
        order.status = "FILLED".into();
        order.executed_qty = order.orig_qty;
        order.avg_price = Some(order.price);
    }
    assert!(engine.sync_live_orders().await);
    let trades = db.get_recent_trades(10).unwrap();
    let closing = trades
        .iter()
        .find(|trade| trade.client_order_id == intent.client_order_id)
        .unwrap();
    assert!(closing.note.contains("Completed Grid Cycle"));
    server.abort();
}

#[tokio::test]
async fn provisional_pair_uses_intent_metadata_before_cancel_retry() {
    let (config, db, intent) = pending_pair_fixture();
    let fake = PairExchange::default();
    fake.fail_cancel.store(1, Ordering::SeqCst);
    fake.orders.lock().unwrap().insert(
        intent.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 501,
            client_order_id: intent.client_order_id.clone(),
            symbol: intent.symbol.clone(),
            status: "NEW".into(),
            price: intent.price,
            avg_price: None,
            orig_qty: intent.quantity,
            executed_qty: Decimal::ZERO,
            side: intent.side.as_str().into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = pair_exchange_server(fake).await;
    let mut engine = pair_recovery_engine(&config, db, url).await;
    let mut placeholder = intent.clone();
    placeholder.order_id = Some(501);
    placeholder.paired_client_order_id = None;
    placeholder.purpose = OrderPurpose::Legacy;
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(placeholder.client_order_id.clone(), placeholder);
    engine.evidence.mark_provisional(
        engine.client.account_identity_key(),
        &intent.client_order_id,
    );
    assert!(!engine.cancel_all_orders().await);
    let active = engine.state.active_orders.read().await;
    assert_eq!(
        active[&intent.client_order_id].paired_client_order_id,
        intent.paired_client_order_id
    );
    assert_eq!(active[&intent.client_order_id].purpose, intent.purpose);
    server.abort();
}

#[tokio::test]
async fn partially_filled_pair_lookup_refreshes_risk_and_preserves_intent_on_failure() {
    let (config, db, intent) = pending_pair_fixture();
    let fake = PairExchange::default();
    fake.orders.lock().unwrap().insert(
        intent.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 501,
            client_order_id: intent.client_order_id.clone(),
            symbol: intent.symbol.clone(),
            status: "PARTIALLY_FILLED".into(),
            price: intent.price,
            avg_price: None,
            orig_qty: intent.quantity,
            executed_qty: dec!(0.4),
            side: intent.side.as_str().into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    engine
        .position_cache
        .get(&engine.client, "SOLUSDC")
        .await
        .unwrap();
    engine.account_cache.get(&engine.client).await.unwrap();
    fake.fail_position.store(1, Ordering::SeqCst);
    assert!(
        !engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert_eq!(
        engine.state.active_orders.read().await[&intent.client_order_id].quantity,
        dec!(1)
    );
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].quantity,
        dec!(1)
    );
    assert_eq!(
        db.load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .len(),
        1
    );
    assert!(engine.last_account_sync.is_none());

    assert!(
        engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert_eq!(
        engine.state.active_orders.read().await[&intent.client_order_id].quantity,
        dec!(0.6)
    );
    assert_eq!(engine.state.position.read().await.size, dec!(0));
    assert_eq!(fake.position_calls.load(Ordering::SeqCst), 3);
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    server.abort();
}

#[tokio::test]
async fn conflicting_intent_and_remainder_identities_survive_failed_managed_read_and_restart() {
    for remainder in [false, true] {
        let (config, seed_db, mut intent) = pending_pair_fixture();
        intent.order_id = Some(123);
        let dir = crate::test_support::TempTestDir::new("identity-observation");
        let path = dir.join("identity-observation.db");
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let trade = seed_db.get_recent_trades(10).unwrap().remove(0);
        if remainder {
            db.insert_trade(&trade).unwrap();
            intent.purpose = OrderPurpose::Remainder;
            db.save_remainder_plan(&crate::types::RemainderPlan {
                symbol: "SOLUSDC".into(),
                mode: TradingMode::Live,
                sources: Vec::new(),
                target: intent.clone(),
                phase: crate::types::RemainderPhase::Submitting,
            })
            .unwrap();
        } else {
            db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&intent), None)
                .unwrap();
        }
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute("INSERT INTO managed_orders(client_order_id,symbol,order_json,updated_at) VALUES ('gb_corrupt','SOLUSDC','invalid','0')", []).unwrap();
        let stage = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::<i64>::new()));
        let client_id = intent.client_order_id.clone();
        let app = Router::new()
                .route("/fapi/v1/openOrders", get({
                    let stage = stage.clone(); let client_id = client_id.clone();
                    move || { let stage = stage.clone(); let client_id = client_id.clone(); async move {
                        if stage.load(Ordering::SeqCst) > 0 { return Json(serde_json::json!([])); }
                        Json(serde_json::json!([{"orderId":456,"clientOrderId":client_id,"symbol":"SOLUSDC",
                            "status":"FILLED","price":"101","origQty":"1","executedQty":"0",
                            "side":"SELL","type":"LIMIT","timeInForce":"GTX"}]))
                    }}
                }))
                .route("/fapi/v1/order", get({
                    let stage = stage.clone(); let seen = seen.clone(); let client_id = client_id.clone();
                    move |Query(query): Query<HashMap<String, String>>| {
                        let stage = stage.clone(); let seen = seen.clone(); let client_id = client_id.clone();
                        async move {
                            let id = query.get("orderId").and_then(|value| value.parse::<i64>().ok()).unwrap_or(123);
                            seen.lock().unwrap().push(id);
                            if id == 123 && stage.load(Ordering::SeqCst) == 2 {
                                return (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({})));
                            }
                            let status = if id == 456 && stage.load(Ordering::SeqCst) == 1 { "FILLED" } else { "CANCELED" };
                            (axum::http::StatusCode::OK, Json(serde_json::json!({"orderId":id,"clientOrderId":client_id,"symbol":"SOLUSDC",
                                "status":status,"price":"101","origQty":"1","executedQty":"0",
                                "side":"SELL","type":"LIMIT","timeInForce":"GTX"})))
                        }
                    }
                }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut first = pair_recovery_engine(&config, db.clone(), url.clone()).await;
        assert!(!first.sync_live_orders().await);
        assert_eq!(
            first.state.active_orders.read().await[&intent.client_order_id].order_id,
            Some(123)
        );
        let account = first.client.account_identity_key();
        assert_eq!(
            first
                .evidence
                .get_observations(account, &intent.client_order_id)
                .unwrap()[0]
                .order_id,
            456
        );
        assert_eq!(
            db.load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .len(),
            1
        );
        connection
            .execute(
                "DELETE FROM managed_orders WHERE client_order_id='gb_corrupt'",
                [],
            )
            .unwrap();
        drop(first);
        let mut changed_config = config.clone();
        changed_config.exchange.recv_window += 1000;
        stage.store(1, Ordering::SeqCst);
        let mut restarted = pair_recovery_engine(&changed_config, db.clone(), url.clone()).await;
        assert_eq!(restarted.client.account_identity_key(), account);
        assert!(!restarted.sync_live_orders().await);
        assert_eq!(
            restarted.state.active_orders.read().await[&intent.client_order_id].order_id,
            Some(123)
        );
        assert!(seen.lock().unwrap().contains(&456));
        stage.store(2, Ordering::SeqCst);
        assert!(!restarted.sync_live_orders().await);
        assert_eq!(
            db.load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .len(),
            1
        );
        drop(restarted);
        stage.store(3, Ordering::SeqCst);
        let mut restarted = pair_recovery_engine(&changed_config, db.clone(), url).await;
        assert!(restarted.sync_live_orders().await);
        assert!(seen.lock().unwrap().contains(&123));
        assert!(db
            .load_identity_observations(account, "SOLUSDC")
            .unwrap()
            .is_empty());
        server.abort();
        drop(connection);
        drop(restarted);
        drop(db);
    }
}

#[tokio::test]
async fn filled_pair_with_zero_quantity_preserves_intent_until_cancel_confirmed() {
    let (config, db, intent) = pending_pair_fixture();
    let fake = PairExchange::default();
    fake.orders.lock().unwrap().insert(
        intent.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 501,
            client_order_id: intent.client_order_id.clone(),
            symbol: intent.symbol.clone(),
            status: "FILLED".into(),
            price: intent.price,
            avg_price: None,
            orig_qty: intent.quantity,
            executed_qty: Decimal::ZERO,
            side: intent.side.as_str().into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    let state = engine.state.clone();
    state.global_position_ready.store(true, Ordering::SeqCst);
    let guard = state.active_orders.write().await;
    let task = tokio::spawn(async move {
        let ok = engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await;
        (engine, ok)
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while state.global_position_ready.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(guard);
    let (mut engine, ok) = task.await.unwrap();
    assert!(!ok);
    assert_eq!(
        engine.state.active_orders.read().await[&intent.client_order_id].quantity,
        intent.quantity
    );
    assert_eq!(
        db.load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
    fake.orders
        .lock()
        .unwrap()
        .get_mut(&intent.client_order_id)
        .unwrap()
        .status = "CANCELED".into();
    assert!(
        engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn uncertain_pair_lookup_blocks_other_orders_until_resolved() {
    let (config, db, _intent) = pending_pair_fixture();
    let fake = PairExchange::default();
    fake.fail_lookup.store(1, Ordering::SeqCst);
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    assert!(
        !engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    engine.maintain_grid_window().await;
    assert!(fake.posts.lock().unwrap().is_empty());
    assert_eq!(
        db.load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .len(),
        1
    );

    assert!(
        engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert!(fake.posts.lock().unwrap().is_empty());
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    server.abort();
}
