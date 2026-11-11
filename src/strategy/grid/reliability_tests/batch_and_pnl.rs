use super::*;

#[tokio::test]
async fn mark_only_completion_protects_live_orders_before_any_ticker_or_replenishment() {
    for pause in [false, true] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let (mut pending, mut response) = managed_price_fixture();
        pending.paired_client_order_id = None;
        pending.order_id = Some(response.order_id);
        db.save_managed_order(&pending).unwrap();
        response.status = "NEW".into();
        fake.orders
            .lock()
            .unwrap()
            .insert(response.client_order_id.clone(), response);
        let mut engine = engine(db.clone(), &url).await;
        {
            let mut config = engine.state.config.write().await;
            config.grid.max_position_usdc = Some(dec!(1000));
            config.grid.pause_on_max_position = pause;
            config.grid.cancel_opening_on_max_position = !pause;
        }
        engine.state.position.write().await.size = dec!(10);
        let now = Utc::now();
        *engine.state.ticker.write().await = TickerInfo {
            symbol: "SOLUSDC".into(),
            last_price: dec!(99),
            mark_price: dec!(99),
            update_time: now - chrono::Duration::seconds(20),
            mark_update_time: now - chrono::Duration::seconds(20),
            ..Default::default()
        };
        let completion = MarkPriceCompletion {
            scope: engine.market_request_scope("SOLUSDC").await,
            previous: engine.state.ticker.read().await.clone(),
            result: Ok((dec!(101), now)),
        };
        engine.apply_mark_price_completion(completion).await;
        assert_eq!(
            *engine.state.status.read().await,
            if pause {
                BotStatus::Paused
            } else {
                BotStatus::Running
            }
        );
        assert!(engine.state.active_orders.read().await.is_empty());
        assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
        assert!(db.get_recent_trades(10).unwrap().is_empty());
        assert!(fake.posts.lock().unwrap().is_empty());
        server.abort();
    }
}

#[tokio::test]
async fn position_sync_marks_data_valid_only_after_success() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let mut engine = engine(Arc::new(Database::open(":memory:").unwrap()), &url).await;
    assert!(engine.state.position.read().await.update_time.is_none());
    assert!(engine.sync_account_and_position().await);
    assert!(engine.state.position.read().await.update_time.is_some());
    fake.position_unavailable.store(true, Ordering::SeqCst);
    assert!(!engine.sync_account_and_position().await);
    assert!(engine.state.position.read().await.update_time.is_none());
    server.abort();
}

fn seed_terminal_fills(db: &Database, fake: &Exchange, count: usize) {
    for index in 0..count {
        let (mut order, mut response) = managed_price_fixture();
        order.client_order_id = format!("gb_batch_{index}");
        order.order_id = Some(123 + index as i64);
        order.paired_client_order_id = None;
        response.client_order_id = order.client_order_id.clone();
        response.order_id = order.order_id.unwrap();
        response.status = "FILLED".into();
        response.executed_qty = response.orig_qty;
        response.avg_price = Some(response.price);
        db.save_managed_order(&order).unwrap();
        fake.orders
            .lock()
            .unwrap()
            .insert(order.client_order_id, response);
    }
}

#[tokio::test]
async fn terminal_fill_batch_refreshes_risk_once_and_verifies_pnl_outside_admission_barrier() {
    let fake = Exchange::default();
    fake.block_account.store(true, Ordering::SeqCst);
    fake.block_pnl.store(true, Ordering::SeqCst);
    *fake.realized_pnl.lock().unwrap() = dec!(1.25);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    seed_terminal_fills(&db, &fake, 6);
    let mut first = engine(db.clone(), &url).await;
    let second = engine(database(), &url).await;
    let _global = test_pair_global_limit(&first, &second).await;
    let first_state = first.state.clone();
    let sync = tokio::spawn(async move {
        let ready = first.sync_live_orders().await;
        (first, ready)
    });
    tokio::time::timeout(Duration::from_secs(5), fake.account_started.notified())
        .await
        .unwrap();
    assert!(second.state.try_order_submission().is_none());
    assert_eq!(first_state.active_orders.read().await.len(), 6);
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 6);
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    fake.block_account.store(false, Ordering::SeqCst);
    fake.release_account.notify_one();
    let (mut first, ready) = sync.await.unwrap();
    assert!(ready);
    assert_eq!(fake.position_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fake.account_calls.load(Ordering::SeqCst), 1);
    assert!(first_state.active_orders.read().await.is_empty());
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    assert_eq!(first_state.stats.read().await.pending_pnl_trades, 6);
    assert!(db
        .get_recent_trades(10)
        .unwrap()
        .iter()
        .all(|trade| !trade.pnl_verified));
    assert!(second.state.try_order_submission().is_some());

    first.reconcile_unverified_trades().await;
    tokio::time::timeout(Duration::from_secs(5), fake.pnl_started.notified())
        .await
        .unwrap();
    assert!(second.state.try_order_submission().is_some());
    assert_eq!(first_state.stats.read().await.pending_pnl_trades, 6);
    fake.block_pnl.store(false, Ordering::SeqCst);
    fake.release_pnl.notify_waiters();
    tokio::time::timeout(
        Duration::from_secs(5),
        first.pnl_reconcile_task.take().unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    let stats = first_state.stats.read().await;
    assert_eq!(stats.total_trades, 6);
    assert_eq!(stats.pending_pnl_trades, 0);
    assert_eq!(stats.total_realized_pnl, dec!(7.5));
    assert_eq!(stats.total_commission, dec!(0.06));
    drop(stats);
    assert_eq!(fake.pnl_calls.load(Ordering::SeqCst), 6);
    assert!(db
        .get_recent_trades(10)
        .unwrap()
        .iter()
        .all(|trade| trade.pnl_verified));
    assert!(first.sync_live_orders().await);
    first.reconcile_unverified_trades().await;
    assert_eq!(first_state.stats.read().await.total_trades, 6);
    assert_eq!(fake.position_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fake.account_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fake.pnl_calls.load(Ordering::SeqCst), 6);
    server.abort();
}

#[tokio::test]
async fn failed_batch_refresh_preserves_all_fill_reservations_until_critical_retry() {
    let fake = Exchange::default();
    fake.account_unavailable.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    seed_terminal_fills(&db, &fake, 3);
    let mut first = engine(db.clone(), &url).await;
    let mut second = engine(database(), &url).await;
    let _global = test_pair_global_limit(&first, &second).await;
    assert!(!first.sync_live_orders().await);
    assert!(second.state.try_order_submission().is_none());
    assert_eq!(first.state.active_orders.read().await.len(), 3);
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 3);
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    fake.account_unavailable.store(false, Ordering::SeqCst);
    assert!(first.sync_live_orders().await);
    assert_eq!(fake.position_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fake.account_calls.load(Ordering::SeqCst), 2);
    assert!(first.state.active_orders.read().await.is_empty());
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 3);
    assert_eq!(first.state.stats.read().await.pending_pnl_trades, 3);
    assert!(second.state.try_order_submission().is_none());
    first.set_global_reconciliation_ready(true).await;
    // The failed risk refresh also queued global cancellations. Fresh risk data
    // and durable fills restore readiness, but admission must wait for those
    // cancellations to finish through the engine's normal recovery path.
    assert!(second.state.try_order_submission().is_none());
    first
        .handle_control_action(BotControlAction::GlobalRiskCancel)
        .await;
    assert!(!first.state.global_cancel_pending.load(Ordering::SeqCst));
    assert!(second.state.try_order_submission().is_none());
    second
        .handle_control_action(BotControlAction::GlobalRiskCancel)
        .await;
    assert!(!second.state.global_cancel_pending.load(Ordering::SeqCst));
    assert!(second.state.try_order_submission().is_some());
    server.abort();
}

#[tokio::test]
async fn terminal_batch_keeps_original_reservations_until_every_fill_commit_is_durable() {
    let path = std::env::temp_dir().join(format!("batch-fill-commit-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let fake = Exchange::default();
    seed_terminal_fills(&db, &fake, 3);
    for response in fake.orders.lock().unwrap().values_mut() {
        response.status = "CANCELED".into();
        response.executed_qty = dec!(0.5);
    }
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_batch_fill BEFORE INSERT ON trades BEGIN SELECT RAISE(ABORT, 'fill commit failed'); END;").unwrap();
    let (url, server) = exchange_server(fake.clone()).await;
    let mut first = engine(db.clone(), &url).await;
    let second = engine(database(), &url).await;
    let _global = test_pair_global_limit(&first, &second).await;
    assert!(!first.sync_live_orders().await);
    assert!(second.state.try_order_submission().is_none());
    assert_eq!(first.state.active_orders.read().await.len(), 3);
    assert!(first
        .state
        .active_orders
        .read()
        .await
        .values()
        .all(|order| order.quantity == dec!(1.25)));
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 3);
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    assert_eq!(first.state.stats.read().await.pending_pnl_trades, 0);
    connection
        .execute_batch("DROP TRIGGER reject_batch_fill;")
        .unwrap();
    assert!(first.sync_live_orders().await);
    assert!(first.state.active_orders.read().await.is_empty());
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    let trades = db.get_recent_trades(10).unwrap();
    assert_eq!(trades.len(), 3);
    assert!(trades
        .iter()
        .all(|trade| trade.quantity == dec!(0.5) && !trade.pnl_verified));
    assert_eq!(first.state.stats.read().await.pending_pnl_trades, 3);
    server.abort();
    drop(first);
    drop(connection);
    drop(db);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn pnl_compensation_retries_failure_and_discards_results_after_history_clear() {
    let fake = Exchange::default();
    fake.pnl_unavailable.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    seed_terminal_fills(&db, &fake, 1);
    let mut first = engine(db.clone(), &url).await;
    assert!(first.sync_live_orders().await);
    first.reconcile_unverified_trades().await;
    first.pnl_retry_at = first.pnl_reconcile_task.take().unwrap().await.unwrap();
    assert!(first.pnl_retry_at.unwrap() > Instant::now() + Duration::from_secs(50));
    assert_eq!(first.state.stats.read().await.pending_pnl_trades, 1);
    assert!(!db.get_recent_trades(1).unwrap()[0].pnl_verified);
    let calls = fake.pnl_calls.load(Ordering::SeqCst);
    first.reconcile_unverified_trades().await;
    assert!(first.pnl_reconcile_task.is_none());
    assert_eq!(fake.pnl_calls.load(Ordering::SeqCst), calls);
    first.pnl_retry_at = Some(Instant::now() - Duration::from_secs(1));
    fake.pnl_unavailable.store(false, Ordering::SeqCst);
    fake.block_pnl.store(true, Ordering::SeqCst);
    first.reconcile_unverified_trades().await;
    tokio::time::timeout(Duration::from_secs(5), fake.pnl_started.notified())
        .await
        .unwrap();
    db.clear_trade_history_for_symbol("SOLUSDC").unwrap();
    first.state.refresh_current_scope().await;
    fake.block_pnl.store(false, Ordering::SeqCst);
    fake.release_pnl.notify_waiters();
    first.pnl_reconcile_task.take().unwrap().await.unwrap();
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    assert_eq!(first.state.stats.read().await.pending_pnl_trades, 0);
    assert_eq!(first.state.stats.read().await.total_realized_pnl, dec!(0));
    assert_eq!(first.state.stats.read().await.total_commission, dec!(0));
    server.abort();
}

#[tokio::test]
async fn restored_pnl_order_lookups_leave_the_critical_reserve_untouched() {
    use crate::exchange::rate_limit::RequestClass;

    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    seed_terminal_fills(&db, &fake, 6);
    let mut first = engine(db.clone(), &url).await;
    assert!(first.sync_live_orders().await);
    drop(first);
    let mut restored = engine(db.clone(), &url).await;
    assert!(restored.pnl_order_ids.is_empty());
    let limiter = Arc::new(crate::exchange::rate_limit::RateLimiter::default());
    let exchange = restored.state.config.read().await.exchange.clone();
    restored.client = Arc::new(BinanceFuturesClient::with_base_url_and_limiter(
        &exchange,
        url.clone(),
        limiter.clone(),
    ));
    limiter
        .reserve(&url, [0; 32], 1800, RequestClass::Normal)
        .unwrap();
    restored.reconcile_unverified_trades().await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while !restored.pnl_reconcile_task.as_ref().unwrap().is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    restored.reconcile_unverified_trades().await;
    assert!(restored.pnl_retry_at.unwrap() > Instant::now() + Duration::from_secs(50));
    assert!(restored.pnl_reconcile_task.is_none());
    restored.reconcile_unverified_trades().await;
    assert!(restored.pnl_reconcile_task.is_none());
    assert_eq!(restored.state.stats.read().await.pending_pnl_trades, 6);
    assert_eq!(fake.pnl_calls.load(Ordering::SeqCst), 0);
    assert!(db
        .get_recent_trades(10)
        .unwrap()
        .iter()
        .all(|trade| !trade.pnl_verified));
    // The ordinary ceiling is 1800 and the critical ceiling is 2280.
    // Every background lookup must leave all 480 emergency units available.
    restored
        .client
        .limiter()
        .reserve(&url, [0; 32], 479, RequestClass::Critical)
        .unwrap();
    let recovered = restored
        .client
        .get_order_by_client_id("SOLUSDC", "gb_batch_0")
        .await
        .unwrap();
    assert_eq!(recovered.order_id, 123);
    server.abort();
}

#[tokio::test]
async fn depleted_pnl_quota_stops_batch_dispatch_and_cools_down_without_blocking_new_fills() {
    use crate::exchange::rate_limit::RequestClass;
    use tracing_subscriber::prelude::*;

    struct WarningCounter(Arc<AtomicUsize>);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WarningCounter {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if event.metadata().level() == &tracing::Level::WARN {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    seed_terminal_fills(&db, &fake, 6);
    let mut first = engine(db.clone(), &url).await;
    let second = engine(database(), &url).await;
    let _global = test_pair_global_limit(&first, &second).await;
    assert!(first.sync_live_orders().await);
    for _ in 0..12 {
        first
            .client
            .limiter()
            .reserve(&url, [0; 32], 5, RequestClass::PnlReconciliation)
            .unwrap();
    }
    let warnings = Arc::new(AtomicUsize::new(0));
    let _logging = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(WarningCounter(warnings.clone())),
    );
    first.reconcile_unverified_trades().await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while !first.pnl_reconcile_task.as_ref().unwrap().is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    first.reconcile_unverified_trades().await;
    assert!(first.pnl_retry_at.unwrap() > Instant::now() + Duration::from_secs(50));
    assert!(first.pnl_reconcile_task.is_none());
    let rejected = warnings.load(Ordering::SeqCst);
    assert!(
        (1..=4).contains(&rejected),
        "local quota attempts must stop at the already-active requests"
    );
    for _ in 0..5 {
        first.reconcile_unverified_trades().await;
        assert!(first.pnl_reconcile_task.is_none());
    }
    assert_eq!(warnings.load(Ordering::SeqCst), rejected);
    assert_eq!(fake.pnl_calls.load(Ordering::SeqCst), 0);
    assert_eq!(first.state.stats.read().await.pending_pnl_trades, 6);
    assert!(second.state.try_order_submission().is_some());

    let (mut order, mut response) = managed_price_fixture();
    order.client_order_id = "gb_fill_during_pnl_cooldown".into();
    order.order_id = Some(999);
    order.paired_client_order_id = None;
    response.client_order_id = order.client_order_id.clone();
    response.order_id = 999;
    response.status = "FILLED".into();
    response.executed_qty = response.orig_qty;
    response.avg_price = Some(response.price);
    db.save_managed_order(&order).unwrap();
    first
        .state
        .active_orders
        .write()
        .await
        .insert(order.client_order_id.clone(), order);
    fake.orders
        .lock()
        .unwrap()
        .insert(response.client_order_id.clone(), response);
    assert!(first.sync_live_orders().await);
    first.reconcile_unverified_trades().await;
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 7);
    assert_eq!(first.state.stats.read().await.pending_pnl_trades, 7);
    assert!(first.state.active_orders.read().await.is_empty());
    assert!(first.pnl_reconcile_task.is_none());
    assert_eq!(warnings.load(Ordering::SeqCst), rejected);
    assert!(second.state.try_order_submission().is_some());

    // Replace the exhausted test window and expire the strategy timer without
    // waiting a minute; the next bounded pass must finish all pending records.
    let exchange = first.state.config.read().await.exchange.clone();
    first.client = Arc::new(BinanceFuturesClient::with_base_url(&exchange, url));
    first.pnl_retry_at = Some(Instant::now() - Duration::from_secs(1));
    first.reconcile_unverified_trades().await;
    assert!(first
        .pnl_reconcile_task
        .take()
        .unwrap()
        .await
        .unwrap()
        .is_none());
    assert_eq!(first.state.stats.read().await.pending_pnl_trades, 0);
    assert_eq!(fake.pnl_calls.load(Ordering::SeqCst), 7);
    assert!(db
        .get_recent_trades(10)
        .unwrap()
        .iter()
        .all(|trade| trade.pnl_verified));
    server.abort();
}
