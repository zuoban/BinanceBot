use super::*;

#[tokio::test]
async fn missing_order_blocks_other_pair_during_lookup_and_position_refresh() {
    let fake = Exchange::default();
    fake.block_lookup.store(true, Ordering::SeqCst);
    fake.block_position.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let (order, mut response) = managed_price_fixture();
    db.save_managed_order(&order).unwrap();
    response.status = "FILLED".into();
    response.executed_qty = response.orig_qty;
    response.avg_price = Some(response.price);
    fake.orders
        .lock()
        .unwrap()
        .insert(order.client_order_id.clone(), response);
    let mut first = engine(db, &url).await;
    let second = engine(database(), &url).await;
    let _global = test_pair_global_limit(&first, &second).await;
    let sync = tokio::spawn(async move { first.sync_live_orders().await });
    tokio::time::timeout(Duration::from_secs(5), fake.lookup_started.notified())
        .await
        .unwrap();
    assert!(second.state.try_order_submission().is_none());
    fake.block_lookup.store(false, Ordering::SeqCst);
    fake.release_lookup.notify_one();
    tokio::time::timeout(Duration::from_secs(5), fake.position_started.notified())
        .await
        .unwrap();
    assert!(second.state.try_order_submission().is_none());
    fake.block_position.store(false, Ordering::SeqCst);
    fake.release_position.notify_one();
    assert!(sync.await.unwrap());
    assert!(second.state.try_order_submission().is_some());
    server.abort();
}

#[tokio::test]
async fn terminal_intent_lookup_blocks_other_pair_until_risk_refresh() {
    for remainder in [false, true] {
        let fake = Exchange::default();
        fake.block_lookup.store(true, Ordering::SeqCst);
        fake.block_position.store(true, Ordering::SeqCst);
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let (mut target, mut response) = managed_price_fixture();
        target.client_order_id = "gb_gate_intent".into();
        target.order_id = Some(123);
        target.side = OrderSide::Sell;
        target.price = dec!(101);
        target.amount_usdc = target.price * target.quantity;
        target.paired_client_order_id = Some("gb_gate_parent".into());
        response.client_order_id = target.client_order_id.clone();
        response.side = "SELL".into();
        response.price = target.price;
        response.status = "FILLED".into();
        response.executed_qty = response.orig_qty;
        response.avg_price = Some(response.price);
        fake.orders
            .lock()
            .unwrap()
            .insert(target.client_order_id.clone(), response);
        if remainder {
            target.purpose = OrderPurpose::Remainder;
            db.save_remainder_plan(&RemainderPlan {
                symbol: "SOLUSDC".into(),
                mode: TradingMode::Live,
                sources: Vec::new(),
                target: target.clone(),
                phase: RemainderPhase::Submitting,
            })
            .unwrap();
        } else {
            let trade = TradeRecord {
                trade_id: "grid:gb_gate_parent".into(),
                client_order_id: "gb_gate_parent".into(),
                symbol: "SOLUSDC".into(),
                mode: TradingMode::Live,
                side: OrderSide::Buy,
                price: dec!(100),
                quantity: dec!(1.25),
                amount_usdc: dec!(125),
                realized_pnl: dec!(0),
                commission: dec!(0),
                pnl_verified: true,
                is_maker: true,
                timestamp: Utc::now(),
                note: "fill".into(),
            };
            db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&target), None)
                .unwrap();
        }
        let mut first = engine(db, &url).await;
        let second = engine(database(), &url).await;
        let _global = test_pair_global_limit(&first, &second).await;
        let handling = tokio::spawn(async move {
            if remainder {
                first.recover_remainder_plan().await.is_ok()
            } else {
                first.drain_pair_intents("SOLUSDC", TradingMode::Live).await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), fake.lookup_started.notified())
            .await
            .unwrap();
        assert!(
            second.state.try_order_submission().is_none(),
            "remainder={remainder}"
        );
        fake.block_lookup.store(false, Ordering::SeqCst);
        fake.release_lookup.notify_one();
        tokio::time::timeout(Duration::from_secs(5), fake.position_started.notified())
            .await
            .unwrap();
        assert!(
            second.state.try_order_submission().is_none(),
            "remainder={remainder}"
        );
        fake.block_position.store(false, Ordering::SeqCst);
        fake.release_position.notify_one();
        assert!(handling.await.unwrap());
        assert!(second.state.try_order_submission().is_some());
        server.abort();
    }
}

#[tokio::test]
async fn cancelled_inflight_post_immediately_blocks_other_pair() {
    let fake = Exchange::default();
    fake.block_submit.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let mut first = engine(database(), &url).await;
    let state = first.state.clone();
    let second = engine(database(), &url).await;
    let _global = test_pair_global_limit(&first, &second).await;
    let placement = tokio::spawn(async move { submit(&mut first).await });
    tokio::time::timeout(Duration::from_secs(5), fake.submit_started.notified())
        .await
        .unwrap();
    placement.abort();
    assert!(placement.await.err().unwrap().is_cancelled());
    assert!(!state.global_position_ready.load(Ordering::SeqCst));
    assert!(second.state.try_order_submission().is_none());
    fake.release_submit.notify_one();
    server.abort();
}

#[tokio::test]
async fn empty_intent_drain_skips_writer_but_new_intent_waits_for_submission() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut first = engine(db.clone(), &url).await;
    let second = engine(database(), &url).await;
    let _global = test_pair_global_limit(&first, &second).await;
    let exchange_guard = second.state.try_exchange_submission().unwrap();
    assert!(tokio::time::timeout(
        Duration::from_secs(2),
        first.drain_pair_intents("SOLUSDC", TradingMode::Live)
    )
    .await
    .unwrap());
    exchange_guard.complete();

    let (mut target, mut response) = managed_price_fixture();
    target.client_order_id = "gb_concurrent_intent".into();
    target.side = OrderSide::Sell;
    target.paired_client_order_id = Some("gb_concurrent_parent".into());
    response.client_order_id = target.client_order_id.clone();
    response.side = "SELL".into();
    response.status = "FILLED".into();
    response.executed_qty = response.orig_qty;
    response.avg_price = Some(response.price);
    fake.orders
        .lock()
        .unwrap()
        .insert(target.client_order_id.clone(), response);
    let trade = TradeRecord {
        trade_id: "grid:gb_concurrent_parent".into(),
        client_order_id: "gb_concurrent_parent".into(),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1.25),
        amount_usdc: dec!(125),
        realized_pnl: dec!(0),
        commission: dec!(0),
        pnl_verified: true,
        is_maker: true,
        timestamp: Utc::now(),
        note: "fill".into(),
    };
    db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&target), None)
        .unwrap();
    let exchange_guard = second.state.try_exchange_submission().unwrap();
    let mut drain =
        tokio::spawn(async move { first.drain_pair_intents("SOLUSDC", TradingMode::Live).await });
    assert!(tokio::time::timeout(Duration::from_millis(100), &mut drain)
        .await
        .is_err());
    assert!(second.state.try_order_submission().is_none());
    let mut concurrent = target.clone();
    concurrent.client_order_id = "gb_concurrent_second".into();
    concurrent.order_id = Some(456);
    concurrent.paired_client_order_id = Some("gb_concurrent_parent_two".into());
    let mut response = fake.orders.lock().unwrap()[&target.client_order_id].clone();
    response.client_order_id = concurrent.client_order_id.clone();
    response.order_id = 456;
    fake.orders
        .lock()
        .unwrap()
        .insert(concurrent.client_order_id.clone(), response);
    let mut second_trade = trade.clone();
    second_trade.trade_id = "grid:gb_concurrent_parent_two".into();
    second_trade.client_order_id = "gb_concurrent_parent_two".into();
    db.insert_trade_with_recovery(
        &second_trade,
        &GridStats::default(),
        Some(&concurrent),
        None,
    )
    .unwrap();
    exchange_guard.complete();
    assert!(drain.await.unwrap());
    assert!(second.state.try_order_submission().is_some());
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    server.abort();
}

#[tokio::test]
async fn immediate_fill_response_gates_other_pair_before_order_processing() {
    for remainder in [false, true] {
        let fake = Exchange::default();
        fake.fill_on_submit.store(2, Ordering::SeqCst);
        fake.block_submit.store(true, Ordering::SeqCst);
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let mut first = engine(db.clone(), &url).await;
        let second = engine(database(), &url).await;
        let _global = test_pair_global_limit(&first, &second).await;
        if remainder {
            let (mut target, _) = managed_price_fixture();
            target.client_order_id = "gb_remainder_gate".into();
            target.order_id = None;
            target.side = OrderSide::Sell;
            target.price = dec!(101);
            target.quantity = dec!(0.5);
            target.amount_usdc = target.price * target.quantity;
            target.purpose = OrderPurpose::Remainder;
            db.save_remainder_plan(&RemainderPlan {
                symbol: "SOLUSDC".into(),
                mode: TradingMode::Live,
                sources: Vec::new(),
                target,
                phase: RemainderPhase::Submitting,
            })
            .unwrap();
        }
        let state = first.state.clone();
        let mut placement = tokio::spawn(async move {
            if remainder {
                first.recover_remainder_plan().await.is_ok()
            } else {
                submit(&mut first).await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), fake.submit_started.notified())
            .await
            .unwrap();
        let active_lock = state.active_orders.write().await;
        fake.release_submit.notify_one();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut placement)
                .await
                .is_err()
        );
        assert!(
            second.state.try_order_submission().is_none(),
            "remainder={remainder}"
        );
        assert!(second.state.global_position_ready.load(Ordering::SeqCst));
        assert_eq!(*second.state.status.read().await, BotStatus::Running);
        drop(active_lock);
        assert!(placement.await.unwrap());
        assert!(second.state.try_order_submission().is_some());
        server.abort();
    }
}

#[tokio::test]
async fn fill_gate_prevents_send_after_placement_intent_is_persisted() {
    let path = std::env::temp_dir().join(format!("pre-send-gate-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let other = engine(database(), &url).await;
    let mut placing = engine(db.clone(), &url).await;
    let state = placing.state.clone();
    other.state.config.write().await.exchange.symbol = "BTCUSDC".into();
    other.state.ticker.write().await.symbol = "BTCUSDC".into();
    for pair in [&state, &other.state] {
        pair.config.write().await.global_max_position_notional = Some(dec!(10000));
        pair.position.write().await.update_time = Some(Utc::now());
        pair.global_position_initialized
            .store(true, Ordering::SeqCst);
        pair.global_position_ready.store(true, Ordering::SeqCst);
    }
    let global =
        crate::server::state::GlobalPositionLimit::new(Arc::new(tokio::sync::RwLock::new(vec![
            state.clone(),
            other.state.clone(),
        ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    for pair in [&state, &other.state] {
        let _ = pair.global_position_limit.set(Arc::downgrade(&global));
    }
    assert!(!global.check().await);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("BEGIN IMMEDIATE").unwrap();
    let placement = tokio::spawn(async move { submit(&mut placing).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!placement.is_finished());
    let ticker_lock = state.ticker.write().await;
    connection.execute_batch("COMMIT").unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let count: i64 = connection.query_row(
                "SELECT count(*) FROM managed_orders WHERE client_order_id = 'gb_b_reliability'",
                [], |row| row.get(0),
            ).unwrap();
            if count == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut gate = other.state.begin_fill_reconciliation().await;
    drop(ticker_lock);
    assert!(!placement.await.unwrap());
    assert!(fake.posts.lock().unwrap().is_empty());
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    gate.resolve();
    drop(gate);
    server.abort();
    drop(connection);
    drop(db);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn recorded_fill_refreshes_account_and_global_risk_before_other_pair_can_send() {
    for account_fails in [false, true] {
        let fake = Exchange::default();
        fake.block_account.store(true, Ordering::SeqCst);
        fake.account_unavailable
            .store(account_fails, Ordering::SeqCst);
        let (url, server) = exchange_server(fake.clone()).await;
        let first_db = database();
        let (order, mut response) = managed_price_fixture();
        first_db.save_managed_order(&order).unwrap();
        let recorded = TradeRecord {
            trade_id: format!("grid:{}", order.client_order_id),
            client_order_id: order.client_order_id.clone(),
            symbol: order.symbol.clone(),
            mode: TradingMode::Live,
            side: order.side,
            price: order.price,
            quantity: order.quantity,
            amount_usdc: order.amount_usdc,
            realized_pnl: dec!(0),
            commission: dec!(0),
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: "recorded fill".into(),
        };
        first_db.insert_trade(&recorded).unwrap();
        response.status = "FILLED".into();
        response.executed_qty = response.orig_qty;
        response.avg_price = Some(response.price);
        fake.orders
            .lock()
            .unwrap()
            .insert(order.client_order_id.clone(), response);
        let path = std::env::temp_dir().join(format!("recorded-risk-{}.db", uuid::Uuid::new_v4()));
        let second_db = Arc::new(Database::open(&path).unwrap());
        second_db.save_bot_status(BotStatus::Running).unwrap();
        let mut first = engine(first_db, &url).await;
        let mut second = engine(second_db.clone(), &url).await;
        let second_state = second.state.clone();
        let global = test_pair_global_limit(&first, &second).await;
        for pair in [&first.state, &second.state] {
            pair.config.write().await.global_max_position_notional = Some(dec!(250));
        }
        assert!(!global.check().await);
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        let placement = tokio::spawn(async move { submit(&mut second).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!placement.is_finished());
        let ticker_lock = second_state.ticker.write().await;
        connection.execute_batch("COMMIT").unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let count: i64 = connection.query_row("SELECT count(*) FROM managed_orders WHERE client_order_id='gb_b_reliability'", [], |row| row.get(0)).unwrap();
                if count == 1 { break; }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        fake.position_large.store(true, Ordering::SeqCst);
        let sync = tokio::spawn(async move { first.sync_live_orders().await });
        tokio::time::timeout(Duration::from_secs(5), fake.account_started.notified())
            .await
            .unwrap();
        drop(ticker_lock);
        assert!(!placement.await.unwrap());
        assert!(fake.posts.lock().unwrap().is_empty());
        fake.block_account.store(false, Ordering::SeqCst);
        fake.release_account.notify_one();
        let synced = sync.await.unwrap();
        if account_fails {
            assert!(!synced);
            assert!(second_state.try_order_submission().is_none());
        } else {
            assert!(global.check().await);
            assert_eq!(*second_state.status.read().await, BotStatus::Paused);
        }
        assert!(fake.posts.lock().unwrap().is_empty());
        server.abort();
        drop(connection);
        drop(second_db);
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn invalid_snapshot_preserves_higher_observed_price_and_quantity_after_restart() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let (order, mut observed) = managed_price_fixture();
    db.save_managed_order(&order).unwrap();
    observed.price = dec!(120.375);
    observed.orig_qty = dec!(2.5);
    let mut invalid = observed.clone();
    invalid.client_order_id = "gb_other_zero".into();
    invalid.order_id = 456;
    invalid.price = Decimal::ZERO;
    {
        let mut orders = fake.orders.lock().unwrap();
        orders.insert(observed.client_order_id.clone(), observed.clone());
        orders.insert(invalid.client_order_id.clone(), invalid);
    }
    let mut first = engine(db.clone(), &url).await;
    assert!(!first.sync_live_orders().await);
    assert!(first.reconciliation_blocked);
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    let active = restarted.state.active_orders.read().await;
    let retained = &active[&order.client_order_id];
    assert!(retained.price >= observed.price);
    assert!(retained.quantity >= observed.orig_qty);
    assert_eq!(retained.amount_usdc, retained.price * retained.quantity);
    drop(active);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    let retained = saved
        .iter()
        .find(|saved| saved.client_order_id == order.client_order_id)
        .unwrap();
    assert!(retained.price >= observed.price);
    assert!(retained.quantity >= observed.orig_qty);
    assert_eq!(retained.amount_usdc, retained.price * retained.quantity);
    server.abort();
}

#[tokio::test]
async fn duplicate_order_ids_with_zero_price_remain_blocked_after_snapshot_disappears() {
    for reversed in [false, true] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let (_, mut valid) = managed_price_fixture();
        valid.client_order_id = "gb_zero_duplicate".into();
        let mut invalid = valid.clone();
        invalid.order_id = 456;
        invalid.price = Decimal::ZERO;
        {
            let mut orders = fake.orders.lock().unwrap();
            let entries = if reversed {
                [invalid.clone(), valid.clone()]
            } else {
                [valid.clone(), invalid.clone()]
            };
            for (index, response) in entries.into_iter().enumerate() {
                orders.insert(index.to_string(), response);
            }
        }
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        assert!(!first.sync_live_orders().await);
        assert!(first.reconciliation_blocked);
        let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
        for id in [valid.order_id, invalid.order_id] {
            assert!(observations
                .iter()
                .any(
                    |(expected, observed)| expected.client_order_id == valid.client_order_id
                        && observed.order_id == id
                ));
        }
        drop(first);
        fake.hide_open_orders.store(true, Ordering::SeqCst);
        fake.lookup_unavailable.store(true, Ordering::SeqCst);
        let mut restarted = engine(db.clone(), &url).await;
        for _ in 0..2 {
            assert!(!restarted.sync_live_orders().await);
            assert!(restarted.reconciliation_blocked);
            let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
            for id in [valid.order_id, invalid.order_id] {
                assert!(observations
                    .iter()
                    .any(
                        |(expected, observed)| expected.client_order_id == valid.client_order_id
                            && observed.order_id == id
                    ));
            }
        }
        server.abort();
    }
}
