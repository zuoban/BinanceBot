use super::super::*;
use crate::types::TradingMode;
use std::net::IpAddr;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

#[tokio::test]
async fn login_global_limit_is_shared_and_expires_without_reset_on_success() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _rx) = mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db, tx);
    let first = IpAddr::V4(std::net::Ipv4Addr::from(1));
    for _ in 0..10 {
        assert!(state.begin_login_attempt(first).await);
    }
    for _ in 0..10 {
        assert!(!state.begin_login_attempt(first).await);
    }
    state.clear_login_attempts(first).await;
    for i in 2..=91 {
        assert!(
            state
                .begin_login_attempt(IpAddr::V4(std::net::Ipv4Addr::from(i)))
                .await
        );
    }
    let other = IpAddr::V4(std::net::Ipv4Addr::from(100));
    assert!(!state.begin_login_attempt(other).await);
    state.clear_login_attempts(first).await;
    assert!(!state.begin_login_attempt(other).await);
    state.auth.login_failures.lock().await.global[0] = Instant::now() - Duration::from_secs(61);
    assert!(state.begin_login_attempt(other).await);
}

#[tokio::test]
async fn saturated_login_table_keeps_active_windows_until_expiry() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _rx) = mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db, tx);
    let blocked = IpAddr::V4(std::net::Ipv4Addr::from(1));
    let new_source = IpAddr::V4(std::net::Ipv4Addr::from(10_001));
    {
        let mut attempts = state.auth.login_failures.lock().await;
        for i in 1..=10_000 {
            let count = if i == 1 { 10 } else { 1 };
            attempts.by_source.insert(
                IpAddr::V4(std::net::Ipv4Addr::from(i)),
                std::iter::repeat_n(Instant::now(), count).collect(),
            );
        }
    }
    assert!(!state.begin_login_attempt(new_source).await);
    assert!(!state.begin_login_attempt(blocked).await);
    state.auth.login_failures.lock().await.by_source.insert(
        IpAddr::V4(std::net::Ipv4Addr::from(2)),
        VecDeque::from([Instant::now() - Duration::from_secs(61)]),
    );
    assert!(state.begin_login_attempt(new_source).await);
    assert!(!state.begin_login_attempt(blocked).await);
}

#[tokio::test]
async fn shutdown_global_lock_timeout_persists_pause_after_database_write_finishes() {
    let directory = crate::test_support::TempTestDir::new("shutdown-lock");
    let path = directory.join("bot.db");
    {
        let db = Arc::new(Database::open(&path).unwrap());
        let (tx, _rx) = mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db.clone(), tx);
        state.resume_trading().await.unwrap();
        let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![state.clone()])));
        state
            .global_position_limit
            .set(Arc::downgrade(&global))
            .ok()
            .unwrap();
        let guard = global.lock.lock().await;
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        let shutdown_state = state.clone();
        let mut shutdown = tokio::spawn(async move {
            let mut engine = tokio::spawn(std::future::pending::<()>());
            crate::lifecycle::stop_engine(&shutdown_state, &mut engine, Duration::from_millis(30))
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut shutdown)
                .await
                .is_err()
        );
        connection.execute_batch("COMMIT").unwrap();
        assert!(shutdown.await.unwrap().is_err());
        drop(guard);
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    drop(db);
}

#[tokio::test]
async fn shutdown_timeout_waits_for_inflight_pause_before_failure_returns() {
    let directory = crate::test_support::TempTestDir::new("shutdown-write");
    let path = directory.join("bot.db");
    {
        let db = Arc::new(Database::open(&path).unwrap());
        let (tx, _rx) = mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db, tx);
        state.resume_trading().await.unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        let shutdown_state = state.clone();
        let mut shutdown = tokio::spawn(async move {
            let mut engine = tokio::spawn(std::future::pending::<()>());
            crate::lifecycle::stop_engine(&shutdown_state, &mut engine, Duration::from_millis(30))
                .await
        });
        while state.status_update_lock.try_lock().is_ok() {
            tokio::task::yield_now().await;
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut shutdown)
                .await
                .is_err()
        );
        connection.execute_batch("COMMIT").unwrap();
        assert!(shutdown.await.unwrap().is_err());
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    drop(db);
}

#[tokio::test]
async fn cancelled_recovery_commit_finishes_before_safety_pause() {
    let directory = crate::test_support::TempTestDir::new("cancelled-commit");
    let path = directory.join("bot.db");
    {
        let db = Arc::new(Database::open(&path).unwrap());
        let (tx, _rx) = mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db, tx);
        state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        state.global_position_ready.store(true, Ordering::SeqCst);
        state.resume_trading().await.unwrap();
        state.pause_for_shutdown().await.unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        let commit_state = state.clone();
        let commit = tokio::spawn(async move { commit_state.commit_shutdown_recovery().await });
        while state.status_update_lock.try_lock().is_ok() {
            tokio::task::yield_now().await;
        }
        commit.abort();
        assert!(commit.await.unwrap_err().is_cancelled());
        let pause_state = state.clone();
        let pause = tokio::spawn(async move { pause_state.pause_trading().await });
        tokio::task::yield_now().await;
        connection.execute_batch("COMMIT").unwrap();
        assert!(pause.await.unwrap().is_err());
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Running));
    drop(db);
}

#[tokio::test]
async fn cancelled_pause_during_recovery_write_remains_paused_after_reopen() {
    let directory = crate::test_support::TempTestDir::new("shutdown-cancelled-pause");
    let path = directory.join("bot.db");
    {
        let db = Arc::new(Database::open(&path).unwrap());
        let (tx, _rx) = mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db.clone(), tx);
        state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        state.global_position_ready.store(true, Ordering::SeqCst);
        state.resume_trading().await.unwrap();
        state.pause_for_shutdown().await.unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        let commit_state = state.clone();
        let commit = tokio::spawn(async move { commit_state.commit_shutdown_recovery().await });
        // Acquiring the update lock precedes spawning the blocked write without yielding.
        while state.status_update_lock.try_lock().is_ok() {
            tokio::task::yield_now().await;
        }
        let pause_state = state.clone();
        let pause = tokio::spawn(async move { pause_state.pause_trading().await });
        tokio::task::yield_now().await;
        pause.abort();
        assert!(pause.await.unwrap_err().is_cancelled());
        connection.execute_batch("COMMIT").unwrap();
        commit.await.unwrap().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Running));
    drop(db);
}

#[tokio::test]
async fn shutdown_recovery_obeys_safety_pause_ordering() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _) = mpsc::channel(8);
    let state = AppState::new(AppConfig::default(), db.clone(), tx);
    state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    state.global_position_ready.store(true, Ordering::SeqCst);

    state.request_shutdown();
    state.pause_for_shutdown().await.unwrap();
    state.commit_shutdown_recovery().await.unwrap();
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Running));
    assert!(state.pause_trading().await.is_err());

    let (tx, _) = mpsc::channel(8);
    let state = AppState::new(AppConfig::default(), db.clone(), tx);
    state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    state.global_position_ready.store(true, Ordering::SeqCst);
    state.resume_trading().await.unwrap();
    state.request_shutdown();
    state.pause_for_shutdown().await.unwrap();
    state.pause_trading().await.unwrap();
    state.commit_shutdown_recovery().await.unwrap();
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
}

#[tokio::test]
async fn interrupted_initialization_cannot_restore_running() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _) = mpsc::channel(8);
    let state = AppState::new(AppConfig::default(), db.clone(), tx);
    state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    state.request_shutdown();
    state.pause_for_shutdown().await.unwrap();
    state.commit_shutdown_recovery().await.unwrap();
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
}

#[tokio::test]
async fn legacy_and_multi_pair_formats_preserve_pause_and_manual_resume() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let mut config = AppConfig::default();
    config.exchange.api_key = "test-key".into();
    config.exchange.api_secret = "test-secret".into();
    config.pairs = vec![crate::config::TradingPairConfig::new("SOLUSDC".into())];
    db.save_config(&config).unwrap();
    db.save_bot_status(BotStatus::Running).unwrap();
    let strategy = config.strategy_configs().unwrap().remove(0);
    let (tx, _) = mpsc::channel(8);
    let multi = AppState::new_for_pair(strategy.clone(), db.clone(), tx, Arc::new(Mutex::new(())));
    multi.pause_trading().await.unwrap();
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Running));

    db.save_config(&strategy).unwrap();
    let (tx, _) = mpsc::channel(8);
    let legacy = AppState::new(strategy.clone(), db.clone(), tx);
    assert_eq!(*legacy.status.read().await, BotStatus::Paused);
    legacy.resume_trading().await.unwrap();
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Running));
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
        Some(BotStatus::Running)
    );

    db.save_config(&config).unwrap();
    let (tx, _) = mpsc::channel(8);
    let multi = AppState::new_for_pair(strategy, db.clone(), tx, Arc::new(Mutex::new(())));
    assert_eq!(*multi.status.read().await, BotStatus::Running);
    multi.pause_trading().await.unwrap();
    assert_eq!(*multi.status.read().await, BotStatus::Paused);
}

#[tokio::test]
async fn pending_safety_pause_prevents_silent_resume() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _rx) = mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db.clone(), tx);
    state.resume_trading().await.unwrap();
    state.pause_trading_silently().await.unwrap();
    let epoch = state.pause_epoch();
    let guard = state.status_update_lock.lock().await;
    let pause_state = state.clone();
    let pause = tokio::spawn(async move { pause_state.pause_trading().await });
    while state.safety_pauses_pending.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    assert_ne!(state.pause_epoch(), epoch);
    let resume_state = state.clone();
    let resume = tokio::spawn(async move { resume_state.resume_trading_silently(epoch).await });
    drop(guard);
    pause.await.unwrap().unwrap();
    assert!(resume.await.unwrap().is_err());
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
}

#[tokio::test]
async fn manual_resume_waiting_for_status_lock_rejects_newer_pause() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _rx) = mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db.clone(), tx);
    state.pause_trading().await.unwrap();
    let config = state.config.read().await;
    let mode = config.exchange.trading_mode();
    drop(config);
    let epoch = state.pause_epoch();
    let guard = state.status_update_lock.lock().await;
    let resume_state = state.clone();
    let resume =
        tokio::spawn(async move { resume_state.resume_trading_in_mode(mode, epoch).await });
    let pause_state = state.clone();
    let pause = tokio::spawn(async move { pause_state.pause_trading().await });
    while state.pause_epoch() == epoch {
        tokio::task::yield_now().await;
    }
    drop(guard);
    assert!(resume.await.unwrap().is_err());
    pause.await.unwrap().unwrap();
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    state
        .resume_trading_in_mode(mode, state.pause_epoch())
        .await
        .unwrap();
    assert_eq!(*state.status.read().await, BotStatus::Running);
}

#[tokio::test]
async fn cancelled_running_writes_finish_before_shutdown_pause() {
    for operation in 0..3 {
        let directory = crate::test_support::TempTestDir::new("cancelled-running");
        let path = directory.join("bot.db");
        {
            let db = Arc::new(Database::open(&path).unwrap());
            let (tx, _rx) = mpsc::channel(1);
            let state = AppState::new(AppConfig::default(), db, tx);
            if operation == 2 {
                state.resume_trading().await.unwrap();
            }
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection.execute_batch("BEGIN IMMEDIATE").unwrap();
            let operation_state = state.clone();
            let task = tokio::spawn(async move {
                match operation {
                    0 => operation_state.resume_trading().await,
                    1 => {
                        operation_state
                            .resume_trading_silently(operation_state.pause_epoch())
                            .await
                    }
                    _ => {
                        let config = operation_state.config.read().await.clone();
                        operation_state.save_runtime_config(config).await
                    }
                }
            });
            while state.status_update_lock.try_lock().is_ok() {
                tokio::task::yield_now().await;
            }
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            state.request_shutdown();
            let pause_state = state.clone();
            let pause = tokio::spawn(async move { pause_state.pause_trading().await });
            while state.safety_pauses_pending.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            connection.execute_batch("COMMIT").unwrap();
            pause.await.unwrap().unwrap();
        }
        let db = Database::open(&path).unwrap();
        assert_eq!(
            db.load_bot_status().unwrap(),
            Some(BotStatus::Paused),
            "operation {operation}"
        );
        drop(db);
    }
}

#[tokio::test]
async fn a_pause_racing_a_mode_commit_is_persisted_in_the_new_mode() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let config = AppConfig {
        pairs: vec![crate::config::TradingPairConfig::new("SOLUSDC".into())],
        ..Default::default()
    };
    db.save_config(&config).unwrap();
    db.save_pair_status("SOLUSDC", TradingMode::Live, BotStatus::Running)
        .unwrap();
    let (tx, _) = mpsc::channel(8);
    let state = AppState::new_for_pair(
        config.strategy_configs().unwrap().remove(0),
        db.clone(),
        tx,
        Arc::new(Mutex::new(())),
    );
    let mut live = state.config.read().await.clone();
    live.exchange.api_key = "live-key".into();
    live.exchange.api_secret = "live-secret".into();
    let (committed, paused) = tokio::join!(state.save_runtime_config(live), state.pause_trading());
    committed.unwrap();
    paused.unwrap();
    assert!(!state.config.read().await.exchange.is_paper());
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
        Some(BotStatus::Paused)
    );
}

#[tokio::test]
async fn dirty_target_mode_pauses_until_manual_resume_without_self_guarding() {
    for legacy in [false, true] {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let mut config = AppConfig::default();
        if !legacy {
            config.pairs = vec![crate::config::TradingPairConfig::new("SOLUSDC".into())];
        }
        db.save_config(&config).unwrap();
        let config = if legacy {
            config
        } else {
            config.strategy_configs().unwrap().remove(0)
        };
        assert!(!db
            .arm_execution_guard("SOLUSDC", TradingMode::Paper)
            .unwrap());
        assert!(!db
            .arm_execution_guard("SOLUSDC", TradingMode::Live)
            .unwrap());
        let (tx, _) = mpsc::channel(8);
        let state = if legacy {
            AppState::new(config.clone(), db.clone(), tx)
        } else {
            AppState::new_for_pair(config.clone(), db.clone(), tx, Arc::new(Mutex::new(())))
        };
        state.resume_trading().await.unwrap();
        let epoch = state.pause_epoch();
        state.save_runtime_config(config.clone()).await.unwrap();
        assert_eq!(*state.status.read().await, BotStatus::Running);
        assert_eq!(state.pause_epoch(), epoch);

        let mut live = config;
        live.exchange.api_key = "live-key".into();
        live.exchange.api_secret = "live-secret".into();
        state.save_runtime_config(live.clone()).await.unwrap();
        assert_eq!(*state.status.read().await, BotStatus::Paused);
        assert_eq!(
            db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
            Some(BotStatus::Paused)
        );
        if legacy {
            assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
        }
        assert!(state.resume_trading_silently(epoch).await.is_err());
        state.save_runtime_config(live).await.unwrap();
        assert_eq!(*state.status.read().await, BotStatus::Paused);
        state.resume_trading().await.unwrap();
        assert_eq!(*state.status.read().await, BotStatus::Running);
        assert_eq!(
            db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
            Some(BotStatus::Running)
        );
    }
}

#[tokio::test]
async fn rotating_live_credentials_pauses_running_strategy_and_bumps_pause_epoch() {
    for legacy in [false, true] {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let mut base = AppConfig::default();
        base.exchange.api_key = "live-account-a".into();
        base.exchange.api_secret = "live-secret".into();
        if !legacy {
            base.pairs = vec![crate::config::TradingPairConfig::new("SOLUSDC".into())];
        }
        db.save_config(&base).unwrap();
        let account_a = if legacy {
            base.clone()
        } else {
            base.strategy_configs().unwrap().remove(0)
        };
        let (tx, _) = mpsc::channel(8);
        let state = if legacy {
            AppState::new(account_a.clone(), db.clone(), tx)
        } else {
            AppState::new_for_pair(account_a.clone(), db.clone(), tx, Arc::new(Mutex::new(())))
        };
        state.resume_trading().await.unwrap();
        assert_eq!(*state.status.read().await, BotStatus::Running);
        let epoch_before = state.pause_epoch();

        // Saving identical Live credentials preserves Running and pause_epoch.
        state.save_runtime_config(account_a.clone()).await.unwrap();
        assert_eq!(*state.status.read().await, BotStatus::Running);
        assert_eq!(state.pause_epoch(), epoch_before);

        // Rotating from Live Account A to Live Account B forces Paused, arms guard, and bumps epoch.
        let mut account_b = account_a;
        account_b.exchange.api_key = "live-account-b".into();
        state.save_runtime_config(account_b).await.unwrap();
        assert_eq!(*state.status.read().await, BotStatus::Paused);
        assert!(state.pause_epoch() > epoch_before);
        assert!(state.resume_trading_silently(epoch_before).await.is_err());
        assert_eq!(
            db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
            Some(BotStatus::Paused)
        );
        if legacy {
            assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
        }
        assert!(db
            .arm_execution_guard("SOLUSDC", TradingMode::Live)
            .unwrap());
    }
}
