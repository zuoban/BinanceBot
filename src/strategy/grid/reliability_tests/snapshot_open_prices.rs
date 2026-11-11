use super::*;

async fn assert_managed_price(engine: &GridTradingEngine, db: &Database, price: Decimal) {
    let active = engine.state.active_orders.read().await;
    let order = &active["gb_price_update"];
    assert_eq!(order.price, price);
    assert_eq!(order.quantity, dec!(1.25));
    assert_eq!(order.amount_usdc, price * dec!(1.25));
    assert_eq!(order.grid_level, -6);
    assert_eq!(order.paired_client_order_id.as_deref(), Some("gb_parent"));
    drop(active);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].price, price);
    assert_eq!(saved[0].quantity, dec!(1.25));
    assert_eq!(saved[0].amount_usdc, price * dec!(1.25));
    assert_eq!(saved[0].grid_level, -6);
    assert_eq!(
        saved[0].paired_client_order_id.as_deref(),
        Some("gb_parent")
    );
}

#[tokio::test]
async fn managed_open_price_updates_are_durable_and_zero_blocks_until_corrected() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let (order, response) = managed_price_fixture();
    db.save_managed_order(&order).unwrap();
    fake.orders
        .lock()
        .unwrap()
        .insert(order.client_order_id.clone(), response);
    let mut first = engine(db.clone(), &url).await;

    for price in [dec!(102.375), dec!(98.0625)] {
        fake.orders
            .lock()
            .unwrap()
            .get_mut(&order.client_order_id)
            .unwrap()
            .price = price;
        assert!(first.sync_live_orders().await);
        assert_managed_price(&first, &db, price).await;
    }
    drop(first);

    fake.orders
        .lock()
        .unwrap()
        .get_mut(&order.client_order_id)
        .unwrap()
        .price = Decimal::ZERO;
    let mut restarted = engine(db.clone(), &url).await;
    let account = restarted.client.account_identity_key();
    assert_managed_price(&restarted, &db, dec!(98.0625)).await;
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted.reconciliation_blocked);
    assert_managed_price(&restarted, &db, dec!(98.0625)).await;

    fake.orders
        .lock()
        .unwrap()
        .get_mut(&order.client_order_id)
        .unwrap()
        .price = dec!(101.875);
    assert!(restarted.sync_live_orders().await);
    assert_managed_price(&restarted, &db, dec!(101.875)).await;
    assert!(db
        .load_terminal_order_disagreements(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    drop(restarted);
    let mut recovered = engine(db.clone(), &url).await;
    assert_managed_price(&recovered, &db, dec!(101.875)).await;
    assert!(recovered.sync_live_orders().await);
    assert_managed_price(&recovered, &db, dec!(101.875)).await;
    assert!(db
        .load_terminal_order_disagreements(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    server.abort();
}

#[tokio::test]
async fn unchanged_managed_snapshot_skips_writes_but_changed_price_requires_persistence() {
    let dir = crate::test_support::TempTestDir::new("managed-dirty");
    let path = dir.join("managed-dirty.db");
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let (order, response) = managed_price_fixture();
    db.save_managed_order(&order).unwrap();
    let fake = Exchange::default();
    fake.orders
        .lock()
        .unwrap()
        .insert(order.client_order_id.clone(), response);
    let (url, server) = exchange_server(fake.clone()).await;
    let mut engine = engine(db.clone(), &url).await;
    assert!(engine.sync_live_orders().await);
    let saved = db.load_managed_orders("SOLUSDC").unwrap().pop().unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_unchanged BEFORE INSERT ON managed_orders \
         BEGIN SELECT RAISE(ABORT, 'unexpected managed order write'); END;",
        )
        .unwrap();
    for _ in 0..3 {
        assert!(engine.sync_live_orders().await);
    }
    let unchanged = db.load_managed_orders("SOLUSDC").unwrap().pop().unwrap();
    assert_eq!(unchanged.updated_at, saved.updated_at);

    fake.orders
        .lock()
        .unwrap()
        .get_mut(&order.client_order_id)
        .unwrap()
        .price = dec!(102.375);
    assert!(!engine.sync_live_orders().await);
    assert!(engine.reconciliation_blocked);
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].price,
        saved.price
    );
    connection
        .execute_batch("DROP TRIGGER reject_unchanged;")
        .unwrap();
    assert!(engine.sync_live_orders().await);
    assert_managed_price(&engine, &db, dec!(102.375)).await;
    server.abort();
    drop(engine);
    drop(connection);
    drop(db);
}

#[tokio::test]
async fn managed_open_price_write_failure_blocks_and_recovers_on_retry_after_restart() {
    let dir = crate::test_support::TempTestDir::new("managed-price");
    let path = dir.join("managed-price.db");
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let (order, mut response) = managed_price_fixture();
    db.save_managed_order(&order).unwrap();
    response.price = dec!(102.375);
    let fake = Exchange::default();
    fake.orders
        .lock()
        .unwrap()
        .insert(order.client_order_id.clone(), response);
    let (url, server) = exchange_server(fake).await;
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_managed_price BEFORE INSERT ON managed_orders \
         BEGIN SELECT RAISE(ABORT, 'managed price write failed'); END;",
        )
        .unwrap();

    let mut first = engine(db.clone(), &url).await;
    assert!(!first.sync_live_orders().await);
    assert!(first.reconciliation_blocked);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(saved[0].price, order.price);
    assert_eq!(saved[0].amount_usdc, order.amount_usdc);
    drop(first);

    let mut restarted = engine(db.clone(), &url).await;
    assert_managed_price(&restarted, &db, order.price).await;
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted.reconciliation_blocked);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(saved[0].price, order.price);
    assert_eq!(saved[0].amount_usdc, order.amount_usdc);
    connection
        .execute_batch("DROP TRIGGER reject_managed_price;")
        .unwrap();
    assert!(restarted.sync_live_orders().await);
    assert_managed_price(&restarted, &db, dec!(102.375)).await;
    drop(restarted);
    let mut recovered = engine(db.clone(), &url).await;
    assert_managed_price(&recovered, &db, dec!(102.375)).await;
    assert!(recovered.sync_live_orders().await);
    assert_managed_price(&recovered, &db, dec!(102.375)).await;
    server.abort();
    drop(recovered);
    drop(connection);
    drop(db);
}

#[tokio::test]
async fn managed_open_price_change_affects_opening_order_limit_on_live() {
    for changed in [false, true] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let (mut order, mut response) = managed_price_fixture();
        order.price = dec!(90.125);
        order.amount_usdc = order.price * order.quantity;
        response.price = if changed { dec!(120.375) } else { order.price };
        db.save_managed_order(&order).unwrap();
        fake.orders
            .lock()
            .unwrap()
            .insert(order.client_order_id.clone(), response.clone());
        let mut engine = engine(db.clone(), &url).await;
        {
            let mut config = engine.state.config.write().await;
            config.grid.order_amount_usdc = dec!(10);
            config.grid.max_position_usdc = Some(dec!(245));
            config.grid.pause_on_max_position = false;
        }
        let exchange = engine.state.config.read().await.exchange.clone();
        engine.client = Arc::new(BinanceFuturesClient::with_base_url(&exchange, url.clone()));
        assert!(engine.sync_live_orders().await);
        assert_managed_price(&engine, &db, response.price).await;
        assert_eq!(submit(&mut engine).await, !changed, "changed={changed}");
        assert_eq!(fake.posts.lock().unwrap().len(), usize::from(!changed));
        server.abort();
    }
}

#[tokio::test]
async fn malformed_open_price_blocks_without_replacing_durable_order_and_recovers() {
    let db = database();
    let (order, _) = managed_price_fixture();
    db.save_managed_order(&order).unwrap();
    let corrected = Arc::new(AtomicBool::new(false));
    let app = Router::new().route("/fapi/v1/openOrders", get({
        let corrected = corrected.clone();
        move || {
            let corrected = corrected.clone();
            async move {
                Json(json!([{"orderId":123,"clientOrderId":"gb_price_update",
                    "symbol":"SOLUSDC","status":"NEW",
                    "price":if corrected.load(Ordering::SeqCst) {"102.375"} else {"not-a-price"},
                    "origQty":"1.25","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}]))
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut first = engine(db.clone(), &url).await;
    assert!(!first.sync_live_orders().await);
    assert!(first.reconciliation_blocked);
    assert_managed_price(&first, &db, order.price).await;
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert_managed_price(&restarted, &db, order.price).await;
    corrected.store(true, Ordering::SeqCst);
    assert!(restarted.sync_live_orders().await);
    assert_managed_price(&restarted, &db, dec!(102.375)).await;
    server.abort();
}

#[tokio::test]
async fn unchanged_open_price_waits_for_active_order_lock_without_losing_readiness() {
    for partial in [false, true] {
        unchanged_price_lock_contention(partial, false, false, true).await;
    }
}

#[tokio::test]
async fn empty_snapshot_temporarily_blocks_and_disabled_global_protection_does_not() {
    unchanged_price_lock_contention(false, false, true, true).await;
    unchanged_price_lock_contention(true, false, false, false).await;
}

#[tokio::test]
async fn cancelled_snapshot_lock_wait_revokes_global_readiness() {
    for (partial, empty) in [(true, false), (false, false), (false, true)] {
        unchanged_price_lock_contention(partial, true, empty, true).await;
    }
}

async fn unchanged_price_lock_contention(partial: bool, cancel: bool, empty: bool, enabled: bool) {
    let db = database();
    let (mut order, mut response) = managed_price_fixture();
    if partial {
        order.quantity = dec!(0.75);
        order.amount_usdc = order.price * order.quantity;
        order.status = OrderStatus::PartiallyFilled;
        response.status = "PARTIALLY_FILLED".into();
        response.executed_qty = dec!(0.5);
    }
    if !empty {
        db.save_managed_order(&order).unwrap();
    }
    let requested = Arc::new(tokio::sync::Notify::new());
    let release_response = Arc::new(tokio::sync::Notify::new());
    let app = Router::new().route(
        "/fapi/v1/openOrders",
        get({
            let requested = requested.clone();
            let release_response = release_response.clone();
            move || {
                let requested = requested.clone();
                let release_response = release_response.clone();
                let response = response.clone();
                async move {
                    requested.notify_one();
                    release_response.notified().await;
                    Json(if empty { Vec::new() } else { vec![response] })
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let second = engine(database(), &url).await;
    second.state.config.write().await.exchange.symbol = "BTCUSDC".into();
    second.state.ticker.write().await.symbol = "BTCUSDC".into();
    let mut engine = engine(db.clone(), &url).await;
    let state = engine.state.clone();
    for pair in [&state, &second.state] {
        pair.config.write().await.global_max_position_notional = enabled.then_some(dec!(10000));
        pair.position.write().await.update_time = Some(Utc::now());
        pair.global_position_initialized
            .store(true, Ordering::SeqCst);
        pair.global_position_ready.store(true, Ordering::SeqCst);
    }
    let global =
        crate::server::state::GlobalPositionLimit::new(Arc::new(tokio::sync::RwLock::new(vec![
            state.clone(),
            second.state.clone(),
        ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    for pair in [&state, &second.state] {
        let _ = pair.global_position_limit.set(Arc::downgrade(&global));
    }
    assert!(!global.check().await);
    assert!(second.state.try_order_submission().is_some());
    let mut sync = tokio::spawn(async move {
        let result = engine.sync_live_orders().await;
        (engine, result)
    });
    requested.notified().await;
    let active_lock = state.active_orders.write().await;
    release_response.notify_one();
    assert!(tokio::time::timeout(Duration::from_millis(100), &mut sync)
        .await
        .is_err());
    assert!(state.global_position_ready.load(Ordering::SeqCst));
    assert_eq!(*state.status.read().await, BotStatus::Running);
    assert!(second.state.global_position_ready.load(Ordering::SeqCst));
    assert_eq!(*second.state.status.read().await, BotStatus::Running);
    if enabled {
        assert!(second.state.try_order_submission().is_none());
    } else {
        assert!(second.state.try_order_submission().is_some());
    }
    if cancel {
        sync.abort();
        assert!(sync.await.err().unwrap().is_cancelled());
        assert!(!state.global_position_ready.load(Ordering::SeqCst));
        assert!(second.state.global_position_ready.load(Ordering::SeqCst));
        assert!(second.state.try_order_submission().is_none());
        drop(active_lock);
        assert!(global.check().await);
        assert!(second.state.try_order_submission().is_none());
        server.abort();
        return;
    }
    drop(active_lock);
    let (engine, success) = sync.await.unwrap();
    assert!(success);
    assert!(!engine.reconciliation_blocked);
    assert!(state.global_position_ready.load(Ordering::SeqCst));
    assert_eq!(*state.status.read().await, BotStatus::Running);
    assert!(second.state.global_position_ready.load(Ordering::SeqCst));
    assert_eq!(*second.state.status.read().await, BotStatus::Running);
    assert!(second.state.try_order_submission().is_some());
    let active = state.active_orders.read().await;
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    if empty {
        assert!(active.is_empty());
        assert!(saved.is_empty());
    } else {
        let retained = &active[&order.client_order_id];
        assert_eq!(retained.price, order.price);
        assert_eq!(retained.quantity, order.quantity);
        assert_eq!(retained.amount_usdc, order.amount_usdc);
        assert_eq!(saved[0].price, order.price);
        assert_eq!(saved[0].quantity, order.quantity);
        assert_eq!(saved[0].amount_usdc, order.amount_usdc);
    }
    drop(active);
    server.abort();
}

#[tokio::test]
async fn risky_snapshots_publish_pending_before_waiting_on_active_lock_and_survive_enablement() {
    for invalid in [false, true] {
        for initially_enabled in [false, true] {
            let db = database();
            let (order, mut response) = managed_price_fixture();
            db.save_managed_order(&order).unwrap();
            if invalid {
                response.price = Decimal::ZERO;
            } else {
                response.status = "PARTIALLY_FILLED".into();
                response.executed_qty = dec!(0.5);
            }
            let requested = Arc::new(tokio::sync::Notify::new());
            let release_response = Arc::new(tokio::sync::Notify::new());
            let app = Router::new().route(
                "/fapi/v1/openOrders",
                get({
                    let requested = requested.clone();
                    let release_response = release_response.clone();
                    move || {
                        let requested = requested.clone();
                        let release_response = release_response.clone();
                        let response = response.clone();
                        async move {
                            requested.notify_one();
                            release_response.notified().await;
                            Json(vec![response])
                        }
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let mut first = engine(db, &url).await;
            let state = first.state.clone();
            let second = engine(database(), &url).await;
            let global = test_pair_global_limit(&first, &second).await;
            if !initially_enabled {
                for pair in [&state, &second.state] {
                    pair.config.write().await.global_max_position_notional = None;
                }
                assert!(!global.check().await);
            }
            let mut sync = tokio::spawn(async move { first.sync_live_orders().await });
            tokio::time::timeout(Duration::from_secs(5), requested.notified())
                .await
                .unwrap();
            let active_lock = state.active_orders.write().await;
            release_response.notify_one();
            assert!(tokio::time::timeout(Duration::from_millis(100), &mut sync)
                .await
                .is_err());
            if initially_enabled {
                assert!(
                    second.state.try_order_submission().is_none(),
                    "invalid={invalid}"
                );
            } else {
                assert!(second.state.try_order_submission().is_some());
                for pair in [&state, &second.state] {
                    pair.config.write().await.global_max_position_notional = Some(dec!(10000));
                }
                let _ = global.check().await;
                assert!(
                    second.state.try_order_submission().is_none(),
                    "invalid={invalid}"
                );
            }
            sync.abort();
            assert!(sync.await.err().unwrap().is_cancelled());
            assert!(!state.global_position_ready.load(Ordering::SeqCst));
            assert!(second.state.try_order_submission().is_none());
            drop(active_lock);
            server.abort();
        }
    }
}

#[tokio::test]
async fn valid_managed_price_update_preserves_other_pair_global_readiness() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let (order, mut response) = managed_price_fixture();
    db.save_managed_order(&order).unwrap();
    response.price = dec!(102.375);
    fake.orders
        .lock()
        .unwrap()
        .insert(order.client_order_id.clone(), response);
    let mut first = engine(db.clone(), &url).await;
    let second = engine(database(), &url).await;
    second.state.config.write().await.exchange.symbol = "BTCUSDC".into();
    second.state.ticker.write().await.symbol = "BTCUSDC".into();
    for state in [&first.state, &second.state] {
        state.config.write().await.global_max_position_notional = Some(dec!(10000));
        state.position.write().await.update_time = Some(Utc::now());
        state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        state.global_position_ready.store(true, Ordering::SeqCst);
    }
    let global =
        crate::server::state::GlobalPositionLimit::new(Arc::new(tokio::sync::RwLock::new(vec![
            first.state.clone(),
            second.state.clone(),
        ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    for state in [&first.state, &second.state] {
        let _ = state.global_position_limit.set(Arc::downgrade(&global));
    }
    assert!(!global.check().await);
    assert!(first.sync_live_orders().await);
    assert_managed_price(&first, &db, dec!(102.375)).await;
    for state in [&first.state, &second.state] {
        assert!(state.global_position_ready.load(Ordering::SeqCst));
        assert_eq!(*state.status.read().await, BotStatus::Running);
    }
    assert!(!global.check().await);
    server.abort();
}

#[tokio::test]
async fn invalid_price_snapshot_retains_other_new_order_through_disappearance_and_restart() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let (_, mut valid) = managed_price_fixture();
    valid.client_order_id = "gb_snapshot_valid_price".into();
    valid.order_id = 234;
    valid.price = dec!(99.75);
    valid.orig_qty = dec!(2.5);
    let mut invalid = valid.clone();
    invalid.client_order_id = "gb_snapshot_zero_price".into();
    invalid.order_id = 345;
    invalid.price = Decimal::ZERO;
    invalid.orig_qty = dec!(1.25);
    {
        let mut orders = fake.orders.lock().unwrap();
        orders.insert(valid.client_order_id.clone(), valid.clone());
        orders.insert(invalid.client_order_id.clone(), invalid.clone());
    }
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    assert!(!first.sync_live_orders().await);
    assert!(first.reconciliation_blocked);
    for response in [&valid, &invalid] {
        let saved = db.load_managed_orders("SOLUSDC").unwrap();
        let snapshots = db
            .load_initial_order_responses(account, "SOLUSDC", &response.client_order_id)
            .unwrap();
        let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
        assert!(
            saved
                .iter()
                .any(|order| order.client_order_id == response.client_order_id
                    && order.order_id == Some(response.order_id)
                    && order.quantity >= response.orig_qty)
                || snapshots
                    .iter()
                    .any(|found| found.order_id == response.order_id
                        && found.orig_qty == response.orig_qty
                        && found.price == response.price)
                || observations
                    .iter()
                    .any(
                        |(expected, found)| expected.client_order_id == response.client_order_id
                            && expected.quantity >= response.orig_qty
                            && found.order_id == response.order_id
                    )
        );
        let active = first.state.active_orders.read().await;
        assert!(active[&response.client_order_id].quantity >= response.orig_qty);
        assert_eq!(
            active[&response.client_order_id].order_id,
            Some(response.order_id)
        );
    }
    drop(first);
    {
        let mut orders = fake.orders.lock().unwrap();
        let filled = orders.get_mut(&valid.client_order_id).unwrap();
        filled.status = "FILLED".into();
        filled.executed_qty = filled.orig_qty;
        filled.avg_price = Some(filled.price);
    }
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted.reconciliation_blocked);
    let active = restarted.state.active_orders.read().await;
    for response in [&valid, &invalid] {
        let retained = &active[&response.client_order_id];
        assert_eq!(retained.order_id, Some(response.order_id));
        assert!(retained.quantity >= response.orig_qty);
    }
    drop(active);
    server.abort();
}

#[tokio::test]
async fn mixed_zero_snapshot_recovers_when_verified_order_price_changes() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let (_, mut valid) = managed_price_fixture();
    valid.client_order_id = "gb_mixed_repriced".into();
    valid.order_id = 234;
    valid.price = dec!(100);
    let mut invalid = valid.clone();
    invalid.client_order_id = "gb_mixed_corrected".into();
    invalid.order_id = 345;
    invalid.price = Decimal::ZERO;
    {
        let mut orders = fake.orders.lock().unwrap();
        orders.insert(valid.client_order_id.clone(), valid.clone());
        orders.insert(invalid.client_order_id.clone(), invalid.clone());
    }
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    assert!(!first.sync_live_orders().await);
    {
        let mut orders = fake.orders.lock().unwrap();
        orders.get_mut(&valid.client_order_id).unwrap().price = dec!(101);
        orders.get_mut(&invalid.client_order_id).unwrap().price = dec!(90);
    }
    assert!(first.sync_live_orders().await);
    assert!(!first.reconciliation_blocked);
    let active = first.state.active_orders.read().await;
    assert_eq!(active[&valid.client_order_id].price, dec!(101));
    assert_eq!(
        active[&valid.client_order_id].amount_usdc,
        dec!(101) * valid.orig_qty
    );
    assert_eq!(active[&invalid.client_order_id].price, dec!(90));
    drop(active);
    assert!(db
        .load_terminal_order_disagreements(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(restarted.sync_live_orders().await);
    assert!(!restarted.reconciliation_blocked);
    assert!(db
        .load_terminal_order_disagreements(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    let repriced = saved
        .iter()
        .find(|order| order.client_order_id == valid.client_order_id)
        .unwrap();
    assert_eq!(repriced.price, dec!(101));
    assert_eq!(repriced.amount_usdc, dec!(101) * valid.orig_qty);
    server.abort();
}

#[tokio::test]
async fn mixed_invalid_snapshot_partial_average_correction_survives_restart() {
    for case in [
        "partial",
        "terminal",
        "average_conflict",
        "quantity_regression",
        "time_regression",
    ] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let (_, mut partial) = managed_price_fixture();
        partial.client_order_id = "gb_mixed_partial_average".into();
        partial.order_id = 234;
        partial.status = "PARTIALLY_FILLED".into();
        partial.price = dec!(100);
        partial.orig_qty = dec!(2);
        partial.executed_qty = dec!(0.5);
        partial.avg_price = Some(if case == "average_conflict" {
            dec!(97.5)
        } else {
            Decimal::ZERO
        });
        partial.update_time = Some(1000);
        let mut invalid = partial.clone();
        invalid.client_order_id = "gb_mixed_zero_other".into();
        invalid.order_id = 345;
        invalid.status = "NEW".into();
        invalid.price = Decimal::ZERO;
        invalid.orig_qty = dec!(1.25);
        invalid.executed_qty = Decimal::ZERO;
        invalid.avg_price = None;
        {
            let mut orders = fake.orders.lock().unwrap();
            orders.insert(partial.client_order_id.clone(), partial.clone());
            orders.insert(invalid.client_order_id.clone(), invalid.clone());
        }
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        assert!(!first.sync_live_orders().await, "{case}");
        assert!(db
            .load_initial_order_responses(account, "SOLUSDC", &partial.client_order_id)
            .unwrap()
            .iter()
            .any(|found| found.status == "PARTIALLY_FILLED"
                && found.executed_qty == dec!(0.5)
                && found.avg_price == partial.avg_price));
        drop(first);
        {
            let mut orders = fake.orders.lock().unwrap();
            orders.get_mut(&invalid.client_order_id).unwrap().price = dec!(90);
            let corrected = orders.get_mut(&partial.client_order_id).unwrap();
            corrected.avg_price = Some(dec!(98.5));
            corrected.update_time = Some(if case == "time_regression" { 500 } else { 2000 });
            if case == "quantity_regression" {
                corrected.executed_qty = dec!(0.25);
            }
            if case == "terminal" {
                corrected.status = "CANCELED".into();
            }
        }
        let mut restarted = engine(db.clone(), &url).await;
        let corrected = matches!(case, "partial" | "terminal");
        assert_eq!(restarted.sync_live_orders().await, corrected, "{case}");
        let disagreements = db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap();
        if corrected {
            assert!(!restarted.reconciliation_blocked, "{case}");
            assert!(disagreements.is_empty(), "{case}");
        } else {
            assert!(restarted.reconciliation_blocked, "{case}");
            assert!(
                disagreements
                    .iter()
                    .any(|(id, _, _)| id == &partial.client_order_id),
                "{case}"
            );
        }
        drop(restarted);
        let mut again = engine(db.clone(), &url).await;
        assert_eq!(again.sync_live_orders().await, corrected, "{case}");
        assert_eq!(
            db.load_terminal_order_disagreements(account, "SOLUSDC")
                .unwrap()
                .is_empty(),
            corrected,
            "{case}"
        );
        server.abort();
    }
}
