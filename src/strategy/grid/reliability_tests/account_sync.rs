use super::*;

async fn ready_engine(
    url: &str,
) -> (
    GridTradingEngine,
    Arc<crate::server::state::GlobalPositionLimit>,
) {
    let mut engine = engine(database(), url).await;
    engine
        .state
        .config
        .write()
        .await
        .global_max_position_notional = Some(dec!(10000));
    assert!(engine.sync_account_and_position().await);
    engine
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    engine
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    let global =
        crate::server::state::GlobalPositionLimit::new(Arc::new(tokio::sync::RwLock::new(vec![
            engine.state.clone(),
        ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    engine
        .state
        .global_position_limit
        .set(Arc::downgrade(&global))
        .unwrap();
    assert!(!global.check().await);
    (engine, global)
}

async fn start_shared_request(
    engine: &GridTradingEngine,
    fake: &Exchange,
    position: bool,
) -> JoinHandle<()> {
    let client = engine.client.clone();
    let task = if position {
        fake.block_position.store(true, Ordering::SeqCst);
        let cache = engine.position_cache.clone();
        tokio::spawn(async move {
            cache.get(&client, "SOLUSDC").await.unwrap();
        })
    } else {
        fake.block_account.store(true, Ordering::SeqCst);
        let cache = engine.account_cache.clone();
        tokio::spawn(async move {
            cache.get(&client).await.unwrap();
        })
    };
    let started = if position {
        &fake.position_started
    } else {
        &fake.account_started
    };
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    task
}

fn release_shared_request(fake: &Exchange, position: bool) {
    if position {
        fake.block_position.store(false, Ordering::SeqCst);
        fake.release_position.notify_one();
    } else {
        fake.block_account.store(false, Ordering::SeqCst);
        fake.release_account.notify_one();
    }
}

#[tokio::test]
async fn pending_shared_sync_keeps_fresh_risk_data_but_blocks_orders_until_retry() {
    for position in [true, false] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let (mut engine, global) = ready_engine(&url).await;
        let previous_sync = engine.last_account_sync;
        let previous_position_time = engine.state.position.read().await.update_time;
        let previous_account_time = engine.state.account.read().await.update_time;
        let task = start_shared_request(&engine, &fake, position).await;

        tokio::time::timeout(Duration::from_secs(1), engine.sync_cycle(false))
            .await
            .unwrap();
        assert!(engine.account_sync_pending);
        assert_eq!(engine.last_account_sync, previous_sync);
        assert!(!engine.sync_timestamps_fresh());
        assert!(!engine.live_reconciliation_ready());
        assert!(!global.check().await);
        assert_eq!(*engine.state.status.read().await, BotStatus::Running);
        if position {
            assert_eq!(
                engine.state.position.read().await.update_time,
                previous_position_time
            );
        } else {
            assert_eq!(
                engine.state.account.read().await.update_time,
                previous_account_time
            );
        }
        assert!(!submit(&mut engine).await);
        assert!(fake.posts.lock().unwrap().is_empty());

        let gate = engine
            .state
            .order_submission_lock
            .clone()
            .write_owned()
            .await;
        release_shared_request(&fake, position);
        task.await.unwrap();
        engine.retry_pending_account_sync().await;
        assert!(!engine.account_sync_pending);
        assert!(engine.live_reconciliation_ready());
        assert!(!global.check().await);
        assert_eq!(*engine.state.status.read().await, BotStatus::Running);
        let calls = if position {
            &fake.position_calls
        } else {
            &fake.account_calls
        };
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        drop(gate);
        server.abort();
    }
}

#[tokio::test]
async fn pending_shared_sync_cannot_preserve_expired_or_invalidated_risk_data() {
    for invalidate in [false, true] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let (mut engine, global) = ready_engine(&url).await;
        if invalidate {
            engine.invalidate_account_and_position_caches();
        }
        let task = start_shared_request(&engine, &fake, false).await;
        engine.sync_cycle(false).await;
        if !invalidate {
            assert_eq!(*engine.state.status.read().await, BotStatus::Running);
            engine.last_account_sync = Some(Instant::now() - engine.sync_freshness());
            engine.retry_pending_account_sync().await;
        }
        assert!(engine.account_sync_pending);
        assert!(engine.last_account_sync.is_none());
        assert!(!engine.state.global_position_ready.load(Ordering::SeqCst));
        assert!(global.check().await);
        assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
        assert!(fake.posts.lock().unwrap().is_empty());

        release_shared_request(&fake, false);
        task.await.unwrap();
        engine.retry_pending_account_sync().await;
        assert!(!engine.account_sync_pending);
        assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
        server.abort();
    }
}

#[tokio::test]
async fn failed_shared_sync_still_invalidates_data_and_triggers_global_pause() {
    for position in [true, false] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let (mut engine, global) = ready_engine(&url).await;
        engine.invalidate_account_and_position_caches();
        if position {
            fake.position_unavailable.store(true, Ordering::SeqCst);
        } else {
            fake.account_unavailable.store(true, Ordering::SeqCst);
        }
        engine.sync_cycle(false).await;
        assert!(!engine.account_sync_pending);
        assert!(engine.last_account_sync.is_none());
        assert!(global.check().await);
        assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
        if position {
            assert!(engine.state.position.read().await.update_time.is_none());
        } else {
            assert_eq!(
                engine.state.account.read().await.update_time,
                chrono::DateTime::<Utc>::default()
            );
        }
        assert!(fake.posts.lock().unwrap().is_empty());
        server.abort();
    }
}
