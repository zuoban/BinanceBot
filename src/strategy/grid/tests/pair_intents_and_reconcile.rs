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
        let path =
            std::env::temp_dir().join(format!("identity-observation-{}.db", uuid::Uuid::new_v4()));
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
        let _ = std::fs::remove_file(path);
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

#[tokio::test]
async fn skipped_counter_order_does_not_leave_a_historical_lock() {
    let (mut config, db, _intent) = pending_pair_fixture();
    config.grid.grid_interval = dec!(1);
    let fake = PairExchange::default();
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url.clone()).await;
    engine.state.ticker.write().await.last_price = dec!(102);
    assert!(
        engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    assert!(fake.posts.lock().unwrap().is_empty());
    assert!(
        engine
            .place_grid_order(OrderSide::Buy, dec!(98), "gb_b_duplicate".into(), -1, None)
            .await
    );
    drop(engine);
    let mut restarted = pair_recovery_engine(&config, db.clone(), url).await;
    restarted.state.ticker.write().await.last_price = dec!(100.5);
    assert!(restarted.sync_live_orders().await);
    assert!(
        !restarted
            .place_grid_order(
                OrderSide::Buy,
                dec!(100),
                "gb_b_after_restart".into(),
                -1,
                None
            )
            .await
    );
    assert!(matches!(
        restarted.pair_submission_decision(),
        super::PairPlacementDecision::Skip(_)
    ));
    assert_eq!(fake.posts.lock().unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn counter_sells_for_different_buys_cannot_duplicate_the_same_level() {
    let (config, db, intent) = pending_pair_fixture();
    let fake = PairExchange::default();
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    engine.state.position.write().await.size = dec!(10);
    let mut existing = intent.clone();
    existing.client_order_id = "gb_s_existing_exit".into();
    existing.paired_client_order_id = Some("gb_b_existing_parent".into());
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(existing.client_order_id.clone(), existing.clone());
    assert!(
        engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    let active = engine.state.active_orders.read().await;
    assert_eq!(active.len(), 1);
    assert!(active
        .values()
        .all(|o| o.price == intent.price && !o.is_take_profit));
    assert!(reserved_sell_quantity(&active.values().cloned().collect::<Vec<_>>()) <= dec!(10));
    drop(active);
    // Draining the same journal again must not submit either exit twice.
    assert!(
        engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert!(fake.posts.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn cancel_all_clears_absent_pair_without_submitting_it() {
    let (config, db, _) = pending_pair_fixture();
    let fake = PairExchange::default();
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    assert!(engine.cancel_all_orders().await);
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    assert!(fake.posts.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn terminal_pair_fill_after_restart_keeps_cycle_metadata() {
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
            avg_price: Some(intent.price),
            orig_qty: intent.quantity,
            executed_qty: intent.quantity,
            side: "SELL".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    engine.state.pause_trading().await.unwrap();
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
    let trade = db
        .get_trade_by_client_id(&intent.client_order_id, TradingMode::Live)
        .unwrap()
        .unwrap();
    assert_eq!(trade.side, OrderSide::Sell);
    assert_eq!(
        db.load_scoped_stats("SOLUSDC", TradingMode::Live)
            .unwrap()
            .unwrap()
            .1,
        1
    );
    server.abort();
}

#[tokio::test]
async fn live_buy_restarts_with_last_price_blocked_and_only_ordinary_window_orders() {
    let (mut config, db, _) = pending_pair_fixture();
    db.clear_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
    config.grid.order_amount_usdc = dec!(2000);
    let fake = PairExchange::default();
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url.clone()).await;
    engine.state.ticker.write().await.last_price = dec!(114.5);
    let mut buy = new_pair_intent(
        &GridOrder {
            client_order_id: "gb_b_parent".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price: dec!(114),
            quantity: dec!(17.54),
            amount_usdc: dec!(1999.56),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: -1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: OrderPurpose::Grid,
            merge_sources: vec![],
        },
        OrderSide::Buy,
        dec!(114),
        dec!(17.54),
    );
    buy.paired_client_order_id = None;
    assert!(engine.on_order_filled(&mut buy).await);
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    assert!(fake.posts.lock().unwrap().is_empty());
    drop(engine);
    let mut restarted = pair_recovery_engine(&config, db.clone(), url).await;
    restarted.state.ticker.write().await.last_price = dec!(114.5);
    restarted.state.position.write().await.size = buy.quantity;
    assert!(
        !restarted
            .place_grid_order(OrderSide::Buy, dec!(114), "gb_b_repeat".into(), -1, None)
            .await
    );
    restarted.maintain_grid_window().await;
    let active = restarted.state.active_orders.read().await;
    let sell = active.values().find(|o| o.side == OrderSide::Sell).unwrap();
    assert_eq!(sell.price, dec!(115));
    assert_eq!(sell.quantity, dec!(17.39));
    assert!(sell.paired_client_order_id.is_none());
    server.abort();
}

#[tokio::test]
async fn existing_destination_order_keeps_its_identity_without_pairing() {
    let (config, db, mut existing) = pending_pair_fixture();
    db.clear_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
    existing.client_order_id = "gb_s_existing_destination".into();
    existing.order_id = Some(500);
    existing.paired_client_order_id = None;
    db.save_managed_order(&existing).unwrap();
    let fake = PairExchange::default();
    fake.orders.lock().unwrap().insert(
        existing.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 500,
            client_order_id: existing.client_order_id.clone(),
            symbol: "SOLUSDC".into(),
            status: "NEW".into(),
            price: dec!(101),
            avg_price: None,
            orig_qty: dec!(1),
            executed_qty: dec!(0),
            side: "SELL".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url.clone()).await;
    engine.state.ticker.write().await.last_price = dec!(100.5);
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(existing.client_order_id.clone(), existing.clone());
    let mut buy = existing.clone();
    buy.client_order_id = "gb_b_destination_parent".into();
    buy.order_id = None;
    buy.side = OrderSide::Buy;
    buy.price = dec!(100);
    buy.quantity = dec!(0.46);
    buy.amount_usdc = buy.price * buy.quantity;
    assert!(engine.on_order_filled(&mut buy).await);
    let intents = db.load_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
    assert!(intents.is_empty());
    drop(engine);
    let mut restarted = pair_recovery_engine(&config, db.clone(), url).await;
    restarted.state.ticker.write().await.last_price = dec!(100.5);
    assert!(restarted.sync_live_orders().await);
    assert!(fake.posts.lock().unwrap().is_empty());
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    let managed = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(managed.len(), 1);
    assert_eq!(managed[0].paired_client_order_id.as_deref(), None);
    assert!(
        !restarted
            .place_grid_order(OrderSide::Buy, dec!(100), "gb_b_repeat".into(), -1, None)
            .await
    );
    server.abort();
}

#[tokio::test]
async fn already_recorded_terminal_counter_order_retires_its_pending_intent() {
    let (config, db, intent) = pending_pair_fixture();
    db.insert_trade(&TradeRecord {
        trade_id: format!("grid:{}", intent.client_order_id),
        client_order_id: intent.client_order_id.clone(),
        symbol: intent.symbol.clone(),
        mode: TradingMode::Live,
        side: OrderSide::Sell,
        price: intent.price,
        quantity: intent.quantity,
        amount_usdc: intent.amount_usdc,
        realized_pnl: dec!(1),
        commission: dec!(0),
        pnl_verified: true,
        is_maker: true,
        timestamp: Utc::now(),
        note: "already reconciled".into(),
    })
    .unwrap();
    let fake = PairExchange::default();
    fake.orders.lock().unwrap().insert(
        intent.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 500,
            client_order_id: intent.client_order_id.clone(),
            symbol: intent.symbol.clone(),
            status: "FILLED".into(),
            price: intent.price,
            avg_price: Some(intent.price),
            orig_qty: intent.quantity,
            executed_qty: intent.quantity,
            side: "SELL".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    assert!(
        engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    assert!(fake.posts.lock().unwrap().is_empty());
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 2);
    server.abort();
}

#[tokio::test]
async fn repeated_live_fill_records_one_trade_without_a_pair_intent() {
    let (config, db, _) = pending_pair_fixture();
    db.clear_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
    let fake = PairExchange::default();
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    engine.state.ticker.write().await.last_price = dec!(102.1);
    let price = dec!(102);
    let quantity = engine
        .state
        .rules
        .read()
        .await
        .calculate_quantity(price, config.grid.order_amount_usdc)
        .unwrap();
    let mut order = GridOrder {
        client_order_id: "gb_s_replayed".into(),
        order_id: None,
        symbol: "SOLUSDC".into(),
        side: OrderSide::Sell,
        price,
        quantity,
        amount_usdc: price * quantity,
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 1,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Legacy,
        merge_sources: Vec::new(),
    };
    assert!(engine.on_order_filled(&mut order).await);
    assert!(engine.on_order_filled(&mut order).await);
    assert!(fake.posts.lock().unwrap().is_empty());
    assert_eq!(
        db.get_recent_trades_for("SOLUSDC", TradingMode::Live, 10)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        db.load_scoped_stats("SOLUSDC", TradingMode::Live)
            .unwrap()
            .unwrap()
            .0,
        2
    );
    let intents = db.load_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
    assert!(intents.is_empty());
    server.abort();
}

#[tokio::test]
async fn unresolved_order_blocks_replenishment() {
    let app = Router::new()
        .route(
            "/fapi/v1/openOrders",
            get(|| async { Json(Vec::<BinanceOrderResponse>::new()) }),
        )
        .route(
            "/fapi/v1/order",
            get(|Query(query): Query<HashMap<String, String>>| async move {
                if query.get("orderId").is_some_and(|id| id == "43") {
                    let order = BinanceOrderResponse {
                        order_id: 43,
                        client_order_id: "gb_b_filled".into(),
                        symbol: "SOLUSDC".into(),
                        status: "FILLED".into(),
                        price: dec!(100),
                        avg_price: Some(dec!(100)),
                        orig_qty: dec!(1),
                        executed_qty: dec!(1),
                        side: "BUY".into(),
                        order_type: "LIMIT".into(),
                        time_in_force: "GTX".into(),
                        update_time: None,
                    };
                    (
                        axum::http::StatusCode::OK,
                        Json(serde_json::to_value(order).unwrap()),
                    )
                } else {
                    (
                        axum::http::StatusCode::SERVICE_UNAVAILABLE,
                        Json(serde_json::json!({})),
                    )
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db.clone(), action_tx);
    let order = GridOrder {
        client_order_id: "gb_b_unresolved".into(),
        order_id: Some(42),
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
        purpose: crate::types::OrderPurpose::Legacy,
        merge_sources: Vec::new(),
    };
    let mut filled = order.clone();
    filled.client_order_id = "gb_b_filled".into();
    filled.order_id = Some(43);
    {
        let mut active = state.active_orders.write().await;
        active.insert(order.client_order_id.clone(), order);
        active.insert(filled.client_order_id.clone(), filled);
    }
    let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);

    assert!(!engine.sync_live_orders().await);
    assert!(engine.last_orders_sync.is_none());
    assert_eq!(state.active_orders.read().await.len(), 2);
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn external_pause_is_not_overridden_by_internal_resume() {
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db.clone(), action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    let active_guard = state.active_orders.write().await;
    let cancellation = tokio::spawn(async move { engine.cancel_all_orders().await });

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while state.pause_epoch() == 0 || db.load_bot_status().unwrap() != Some(BotStatus::Paused) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    state.pause_trading().await.unwrap();
    drop(active_guard);

    assert!(cancellation.await.unwrap());
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
}

#[tokio::test]
async fn cancel_all_only_touches_managed_exchange_orders() {
    #[derive(Clone)]
    struct FakeExchange {
        fetches: Arc<AtomicUsize>,
        canceled: Arc<Mutex<Vec<String>>>,
    }
    let fake = FakeExchange {
        fetches: Arc::new(AtomicUsize::new(0)),
        canceled: Arc::new(Mutex::new(Vec::new())),
    };
    let app =
        Router::new()
            .route(
                "/fapi/v1/openOrders",
                get(|State(fake): State<FakeExchange>| async move {
                    let order = |id, client_id: &str| BinanceOrderResponse {
                        order_id: id,
                        client_order_id: client_id.into(),
                        symbol: "SOLUSDC".into(),
                        status: "NEW".into(),
                        price: dec!(100),
                        avg_price: None,
                        orig_qty: dec!(1),
                        executed_qty: dec!(0),
                        side: "BUY".into(),
                        order_type: "LIMIT".into(),
                        time_in_force: "GTX".into(),
                        update_time: None,
                    };
                    let orders = if fake.fetches.fetch_add(1, Ordering::SeqCst) == 0 {
                        vec![order(99, "manual-order"), order(42, "gb_b_owned")]
                    } else {
                        Vec::new()
                    };
                    Json(orders)
                }),
            )
            .route(
                "/fapi/v1/order",
                delete(
                    |State(fake): State<FakeExchange>,
                     Query(query): Query<HashMap<String, String>>| async move {
                        fake.canceled.lock().unwrap().push(query["orderId"].clone());
                        Json(serde_json::json!({}))
                    },
                ),
            )
            .with_state(fake.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    let mut engine = GridTradingEngine::new(state, client, action_rx, ticker_rx);

    assert!(engine.cancel_all_orders().await);
    assert_eq!(*fake.canceled.lock().unwrap(), vec!["42"]);
    server.abort();
}

#[tokio::test]
async fn switching_live_to_paper_cancels_live_orders_before_commit() {
    #[derive(Clone)]
    struct FakeExchange {
        fetches: Arc<AtomicUsize>,
        cancellations: Arc<AtomicUsize>,
    }
    let fake = FakeExchange {
        fetches: Arc::new(AtomicUsize::new(0)),
        cancellations: Arc::new(AtomicUsize::new(0)),
    };
    let app = Router::new()
            .route("/fapi/v1/exchangeInfo", get(|| async {
                Json(serde_json::json!({"symbols":[{"symbol":"SOLUSDC","status":"TRADING","pricePrecision":2,"quantityPrecision":2,"filters":[{"filterType":"PRICE_FILTER","minPrice":"0.01","maxPrice":"100000","tickSize":"0.01"},{"filterType":"LOT_SIZE","minQty":"0.01","maxQty":"100000","stepSize":"0.01"},{"filterType":"MIN_NOTIONAL","notional":"5"}]}]}))
            }))
            .route("/fapi/v1/ticker/price", get(|| async {
                Json(serde_json::json!({"symbol":"SOLUSDC","price":"100","time":0}))
            }))
            .route("/fapi/v1/openOrders", get(|State(fake): State<FakeExchange>| async move {
                if fake.fetches.fetch_add(1, Ordering::SeqCst) == 0 {
                    Json(serde_json::json!([{"orderId":42,"clientOrderId":"gb_b_live","symbol":"SOLUSDC","status":"NEW","price":"99","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX","updateTime":0}]))
                } else {
                    Json(serde_json::json!([]))
                }
            }))
            .route("/fapi/v1/order", delete(|State(fake): State<FakeExchange>| async move {
                fake.cancellations.fetch_add(1, Ordering::SeqCst);
                Json(serde_json::json!({}))
            }))
            .with_state(fake.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    let mut paper = config;
    paper.exchange.api_key.clear();
    paper.exchange.api_secret.clear();

    engine.apply_config(paper).await.unwrap();
    assert_eq!(fake.cancellations.load(Ordering::SeqCst), 1);
    assert!(state.config.read().await.exchange.is_paper());
    server.abort();
}
