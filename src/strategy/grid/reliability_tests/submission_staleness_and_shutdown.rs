use super::*;

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

#[tokio::test]
async fn zero_fill_terminal_submission_does_not_leave_a_new_order() {
    for response in [3, 4] {
        let fake = Exchange::default();
        fake.fill_on_submit.store(response, Ordering::SeqCst);
        let (url, server) = exchange_server(fake).await;
        let db = database();
        let mut engine = engine(db.clone(), &url).await;
        assert!(!submit(&mut engine).await);
        assert!(engine.state.active_orders.read().await.is_empty());
        assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
        assert!(db.get_recent_trades(10).unwrap().is_empty());
        server.abort();
    }
}

#[tokio::test]
async fn filled_with_zero_quantity_remains_blocked_until_terminal_details_are_consistent() {
    for (confirmed_fill, submission) in [(false, 0), (true, 0), (false, 1), (true, 1)] {
        let fake = Exchange::default();
        fake.fill_on_submit.store(5, Ordering::SeqCst);
        fake.submission.store(submission, Ordering::SeqCst);
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let mut engine = engine(db.clone(), &url).await;
        assert!(!submit(&mut engine).await);
        let order = engine.state.active_orders.read().await["gb_b_reliability"].clone();
        assert!(!engine.cancel_single_order(&order).await);
        assert!(!engine.sync_live_orders().await);
        assert!(engine.reconciliation_blocked);
        assert!(engine.last_account_sync.is_none());
        assert!(engine
            .state
            .active_orders
            .read()
            .await
            .contains_key("gb_b_reliability"));
        assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
        assert!(db.get_recent_trades(10).unwrap().is_empty());
        {
            let mut orders = fake.orders.lock().unwrap();
            let found = orders.get_mut("gb_b_reliability").unwrap();
            if confirmed_fill {
                found.executed_qty = found.orig_qty;
                found.avg_price = Some(found.price);
            } else {
                found.status = "CANCELED".into();
            }
        }
        assert!(engine.sync_live_orders().await);
        assert!(!engine
            .state
            .active_orders
            .read()
            .await
            .contains_key("gb_b_reliability"));
        assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
        assert_eq!(
            db.get_recent_trades(10).unwrap().len(),
            usize::from(confirmed_fill)
        );
        server.abort();
    }
}

#[tokio::test]
async fn inconsistent_submission_and_timeout_lookup_block_before_active_order_lock() {
    for submission in [0, 1] {
        let fake = Exchange::default();
        fake.fill_on_submit.store(5, Ordering::SeqCst);
        fake.submission.store(submission, Ordering::SeqCst);
        fake.block_submit.store(true, Ordering::SeqCst);
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let mut engine = engine(db, &url).await;
        engine
            .state
            .global_position_ready
            .store(true, Ordering::SeqCst);
        let state = engine.state.clone();
        let task = tokio::spawn(async move { submit(&mut engine).await });
        tokio::time::timeout(Duration::from_secs(2), fake.submit_started.notified())
            .await
            .unwrap();
        let guard = state.active_orders.write().await;
        fake.release_submit.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.global_position_ready.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(guard);
        assert!(!task.await.unwrap());
        assert!(state
            .active_orders
            .read()
            .await
            .contains_key("gb_b_reliability"));
        server.abort();
    }
}
