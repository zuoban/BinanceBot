use super::*;
use crate::types::{GridDirection, OrderSide, TradingMode};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
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
async fn cancelled_paper_fill_finishes_publishing_before_retry_checks_history() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _rx) = mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db.clone(), tx);
    let trade = cache_test_trade("cancel-paper-fill", 1, TradingMode::Paper);
    let position = PositionInfo {
        size: dec!(1),
        entry_price: dec!(100),
        ..Default::default()
    };
    let mut account = state.account.read().await.clone();
    account.available_balance = dec!(9900);
    let position_guard = state.position.write().await;
    let revision = db.trade_data_revision();
    let committing_state = state.clone();
    let committing_trade = trade.clone();
    let commit = tokio::spawn(async move {
        committing_state
            .record_trade_with_paper_state(
                committing_trade,
                None,
                None,
                None,
                Some((position, account)),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while db.trade_data_revision() == revision {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    commit.abort();
    assert!(commit.await.unwrap_err().is_cancelled());
    assert!(state.trade_commit_lock.try_lock().is_err());
    let retry_state = state.clone();
    let retry_id = trade.client_order_id.clone();
    let retry = tokio::spawn(async move {
        retry_state
            .has_recorded_paper_order(retry_id, TradingMode::Paper)
            .await
            .unwrap()
    });
    drop(position_guard);
    assert!(tokio::time::timeout(Duration::from_secs(5), retry)
        .await
        .unwrap()
        .unwrap());
    assert_eq!(state.position.read().await.size, dec!(1));
    assert_eq!(state.position.read().await.entry_price, dec!(100));
    assert_eq!(state.account.read().await.available_balance, dec!(9900));
    assert_eq!(state.stats.read().await.total_trades, 1);
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
}

#[tokio::test]
async fn cancelled_cleanup_finishes_all_cache_resets_before_releasing_commit_lock() {
    for cleanup_kind in 0..3 {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let lock = Arc::new(Mutex::new(()));
        let mut config = AppConfig::default();
        config.exchange.symbol = "SOLUSDC".into();
        let (tx, _rx) = mpsc::channel(1);
        let first = AppState::new_for_pair(config.clone(), db.clone(), tx.clone(), lock.clone());
        config.exchange.symbol = "BTCUSDC".into();
        let second = AppState::new_for_pair(config, db.clone(), tx, lock);
        let trade = cache_test_trade("cancel-cleanup", 1, TradingMode::Paper);
        assert!(db.insert_trade(&trade).unwrap());
        first.stats.write().await.total_trades = 1;
        first.recent_trades.write().await.push_back(trade);
        second.stats.write().await.total_trades = 2;
        let stats_guard = first.stats.write().await;
        let revision = db.trade_data_revision();
        let states = vec![first.clone(), second.clone()];
        let cleanup = tokio::spawn(async move {
            match cleanup_kind {
                0 => {
                    states[0]
                        .clear_symbol_trade_history("SOLUSDC".into())
                        .await
                        .unwrap();
                }
                1 => {
                    states[0].clear_trade_history().await.unwrap();
                }
                _ => {
                    AppState::clear_selected_database_for(&states, true, false, None)
                        .await
                        .unwrap();
                }
            }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while db.trade_data_revision() == revision {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cleanup.abort();
        assert!(cleanup.await.unwrap_err().is_cancelled());
        assert!(first.trade_commit_lock.try_lock().is_err());
        drop(stats_guard);
        let _commit = tokio::time::timeout(Duration::from_secs(5), first.trade_commit_lock.lock())
            .await
            .unwrap();
        assert!(db.get_recent_trades(10).unwrap().is_empty());
        assert_eq!(first.stats.read().await.total_trades, 0);
        assert!(first.recent_trades.read().await.is_empty());
        assert_eq!(
            second.stats.read().await.total_trades,
            if cleanup_kind == 2 { 0 } else { 2 }
        );
    }
}

#[tokio::test]
async fn clearing_previous_symbol_preserves_current_symbol_cache() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let mut config = AppConfig::default();
    config.exchange.symbol = "BTCUSDC".into();
    let (tx, _rx) = mpsc::channel(1);
    let state = AppState::new(config, db.clone(), tx);
    let old = cache_test_trade("old-symbol", 1, TradingMode::Paper);
    let mut current = cache_test_trade("current-symbol", 2, TradingMode::Paper);
    current.symbol = "BTCUSDC".into();
    assert!(db.insert_trade(&old).unwrap());
    assert!(db.insert_trade(&current).unwrap());
    state.stats.write().await.total_trades = 1;
    state.recent_trades.write().await.push_back(current);
    assert_eq!(
        state
            .clear_symbol_trade_history("SOLUSDC".into())
            .await
            .unwrap()
            .0,
        1
    );
    assert_eq!(state.stats.read().await.total_trades, 1);
    assert_eq!(
        state.recent_trades.read().await[0].trade_id,
        "current-symbol"
    );
    assert_eq!(db.get_recent_trades(10).unwrap()[0].symbol, "BTCUSDC");
}

#[tokio::test]
async fn shutdown_global_lock_timeout_persists_pause_after_database_write_finishes() {
    let directory = std::env::temp_dir().join(format!("shutdown-lock-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
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
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn shutdown_timeout_waits_for_inflight_pause_before_failure_returns() {
    let directory = std::env::temp_dir().join(format!("shutdown-write-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
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
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn cancelled_recovery_commit_finishes_before_safety_pause() {
    let directory = std::env::temp_dir().join(format!("cancelled-commit-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
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
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn cancelled_pause_during_recovery_write_remains_paused_after_reopen() {
    let directory =
        std::env::temp_dir().join(format!("shutdown-cancelled-pause-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
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
    std::fs::remove_dir_all(directory).unwrap();
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
        let directory =
            std::env::temp_dir().join(format!("cancelled-running-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
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
        std::fs::remove_dir_all(directory).unwrap();
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

struct NotificationFixture {
    state: Arc<AppState>,
    requests: mpsc::UnboundedReceiver<serde_json::Value>,
    actions: mpsc::Receiver<BotControlAction>,
    url: String,
    server: tokio::task::JoinHandle<()>,
}

#[tokio::test]
async fn global_position_limit_aggregates_and_blocks_resume_until_below_limit() {
    let first = notification_fixture(false, false).await;
    let second = notification_fixture(false, false).await;
    first
        .state
        .config
        .write()
        .await
        .global_max_position_notional = Some(dec!(100));
    second
        .state
        .config
        .write()
        .await
        .global_max_position_notional = Some(dec!(100));
    first.state.position.write().await.size = dec!(-2);
    first.state.position.write().await.mark_price = dec!(20);
    first.state.ticker.write().await.mark_price = dec!(30);
    first.state.ticker.write().await.mark_update_time = Utc::now();
    second.state.position.write().await.size = dec!(2);
    second.state.ticker.write().await.last_price = dec!(20);
    second.state.ticker.write().await.update_time = Utc::now();
    let states = Arc::new(RwLock::new(vec![first.state.clone(), second.state.clone()]));
    let global = GlobalPositionLimit::new(states);
    global.membership_ready.store(true, Ordering::SeqCst);
    for state in [&first.state, &second.state] {
        state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        state.global_position_ready.store(true, Ordering::SeqCst);
        let _ = state.global_position_limit.set(Arc::downgrade(&global));
    }
    assert!(global.check().await);
    assert_eq!(*first.state.status.read().await, BotStatus::Paused);
    assert_eq!(*second.state.status.read().await, BotStatus::Paused);
    assert!(first.state.resume_trading().await.is_err());
    second.state.position.write().await.size = Decimal::ZERO;
    assert!(!global.check().await);
    assert!(first.state.resume_trading().await.is_err());
    first
        .state
        .global_cancel_pending
        .store(false, Ordering::SeqCst);
    first.state.resume_trading().await.unwrap();
}

#[tokio::test]
async fn global_position_limit_blocks_until_data_is_ready() {
    let fixture = notification_fixture(false, false).await;
    fixture
        .state
        .config
        .write()
        .await
        .global_max_position_notional = Some(Decimal::ONE);
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
    global.membership_ready.store(true, Ordering::SeqCst);
    let _ = fixture
        .state
        .global_position_limit
        .set(Arc::downgrade(&global));
    assert!(global.check().await);
    assert!(fixture.state.resume_trading().await.is_err());
    fixture
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    fixture.state.position.write().await.size = Decimal::ONE;
    assert!(global.check().await);
    fixture.state.position.write().await.mark_price = Decimal::ONE;
    fixture.state.position.write().await.update_time = Some(Utc::now());
    assert!(global.check().await);
    assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
}

#[tokio::test]
async fn initialized_member_failure_pauses_even_while_another_initializes() {
    let mut first = notification_fixture(false, false).await;
    let second = notification_fixture(false, false).await;
    first
        .state
        .config
        .write()
        .await
        .global_max_position_notional = Some(Decimal::ONE);
    first
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    first
        .state
        .global_position_ready
        .store(false, Ordering::SeqCst);
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![
        first.state.clone(),
        second.state.clone(),
    ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    assert!(global.check().await);
    assert_eq!(*first.state.status.read().await, BotStatus::Paused);
    assert_eq!(*second.state.status.read().await, BotStatus::Paused);
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(1), first.actions.recv())
            .await
            .unwrap(),
        Some(BotControlAction::GlobalRiskCancel)
    ));
}

#[tokio::test]
async fn disabled_committed_guard_skips_check_and_membership_locks() {
    let states = Arc::new(RwLock::new(Vec::new()));
    let global = GlobalPositionLimit::new(states.clone());
    global.initialize_config(crate::config::GlobalConfig::from_app(&AppConfig::default()));
    let _check = global.lock.lock().await;
    let _members = states.write().await;
    assert!(
        !tokio::time::timeout(Duration::from_secs(1), global.check())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn risk_checks_use_committed_config_during_propagation() {
    let fixture = notification_fixture(false, false).await;
    let old = crate::config::GlobalConfig::from_app(&*fixture.state.config.read().await);
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
    global.initialize_config(old);
    global.membership_ready.store(true, Ordering::SeqCst);
    fixture
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    fixture
        .state
        .config
        .write()
        .await
        .pause_on_zero_available_margin = true;
    fixture.state.account.write().await.total_wallet_balance = Decimal::ZERO;
    assert!(!global.check().await);
    assert_eq!(*fixture.state.status.read().await, BotStatus::Running);
    let new = crate::config::GlobalConfig::from_app(&*fixture.state.config.read().await);
    global.commit_config(new).await;
    assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
}

#[tokio::test]
async fn zero_margin_guard_blocks_failed_initialization_until_full_reconciliation() {
    let fixture = notification_fixture(false, false).await;
    fixture
        .state
        .config
        .write()
        .await
        .pause_on_zero_available_margin = true;
    fixture
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
    global.membership_ready.store(true, Ordering::SeqCst);
    assert!(global.check().await);
    assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
    fixture
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    assert!(!global.check().await);
}

#[tokio::test]
async fn margin_ratio_guard_pauses_only_at_or_below_threshold() {
    let fixture = notification_fixture(false, false).await;
    {
        let mut config = fixture.state.config.write().await;
        config.min_available_margin_pct = Some(dec!(10));
        // Live accounts are read as stored; paper accounts come from the shared ledger.
        config.exchange.api_key = "key".into();
        config.exchange.api_secret = "secret".into();
    }
    fixture
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    fixture
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
    global.membership_ready.store(true, Ordering::SeqCst);
    {
        let mut account = fixture.state.account.write().await;
        account.asset = "USDC".into();
        account.margin_balance = dec!(1000);
        account.available_balance = dec!(101);
        account.update_time = Utc::now();
    }
    assert!(!global.check().await);
    assert_eq!(*fixture.state.status.read().await, BotStatus::Running);
    fixture.state.account.write().await.available_balance = dec!(100);
    assert!(global.check().await);
    assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
}

#[tokio::test]
async fn zero_margin_pauses_all_and_requires_confirmed_cancellation() {
    let first = notification_fixture(false, false).await;
    let mut second = notification_fixture(false, false).await;
    for state in [&first.state, &second.state] {
        state.config.write().await.pause_on_zero_available_margin = true;
        state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        state.global_position_ready.store(true, Ordering::SeqCst);
    }
    second.state.account.write().await.total_wallet_balance = Decimal::ZERO;
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![
        first.state.clone(),
        second.state.clone(),
    ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    for state in [&first.state, &second.state] {
        let _ = state.global_position_limit.set(Arc::downgrade(&global));
    }
    assert!(global.check().await);
    assert_eq!(*first.state.status.read().await, BotStatus::Paused);
    assert_eq!(*second.state.status.read().await, BotStatus::Paused);
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(1), second.actions.recv())
            .await
            .unwrap(),
        Some(BotControlAction::GlobalRiskCancel)
    ));
    second.state.account.write().await.total_wallet_balance = dec!(1);
    assert!(second.state.resume_trading().await.is_err());
    second
        .state
        .global_cancel_pending
        .store(false, Ordering::SeqCst);
    second.state.resume_trading().await.unwrap();
}

#[tokio::test]
async fn zero_margin_guard_ignores_uninitialized_default_balance() {
    let fixture = notification_fixture(false, false).await;
    fixture
        .state
        .config
        .write()
        .await
        .pause_on_zero_available_margin = true;
    {
        let mut config = fixture.state.config.write().await;
        config.exchange.api_key = "live-key".into();
        config.exchange.api_secret = "live-secret".into();
    }
    *fixture.state.account.write().await = AccountInfo::default();
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
    global.membership_ready.store(true, Ordering::SeqCst);
    assert!(global.check().await);
    assert!(!fixture.state.global_cancel_pending.load(Ordering::SeqCst));
}

#[tokio::test]
async fn zero_margin_guard_requires_enabled_switch_and_detects_negative_balance() {
    let fixture = notification_fixture(false, false).await;
    fixture
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    fixture.state.account.write().await.total_wallet_balance = -Decimal::ONE;
    fixture
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
    global.membership_ready.store(true, Ordering::SeqCst);
    assert!(!global.check().await);
    assert_eq!(*fixture.state.status.read().await, BotStatus::Running);
    fixture
        .state
        .config
        .write()
        .await
        .pause_on_zero_available_margin = true;
    assert!(global.check().await);
    assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
}

#[tokio::test]
async fn zero_margin_guard_uses_combined_paper_positions() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let commit = Arc::new(Mutex::new(()));
    let mut states = Vec::new();
    let mut receivers = Vec::new();
    for (symbol, size, price) in [
        ("SOLUSDC", dec!(60), dec!(100)),
        ("BNBUSDC", dec!(-20), dec!(200)),
    ] {
        let mut config = AppConfig::default();
        config.exchange.symbol = symbol.into();
        config.pause_on_zero_available_margin = true;
        let (tx, rx) = mpsc::channel(8);
        let state = AppState::new_for_pair(config, db.clone(), tx, commit.clone());
        *state.position.write().await = PositionInfo {
            symbol: symbol.into(),
            size,
            entry_price: price,
            mark_price: price,
            ..Default::default()
        };
        state.account.write().await.available_balance = dec!(10000) - size.abs() * price;
        state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        state.global_position_ready.store(true, Ordering::SeqCst);
        states.push(state);
        receivers.push(rx);
    }
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(states.clone())));
    global.membership_ready.store(true, Ordering::SeqCst);
    assert!(global.check().await);
    for state in states {
        assert_eq!(*state.status.read().await, BotStatus::Paused);
        assert_eq!(
            state.current_account().await.available_balance,
            Decimal::ZERO
        );
    }
}

async fn notification_fixture(enabled: bool, reject: bool) -> NotificationFixture {
    use axum::{routing::post, Json, Router};
    let (request_tx, requests) = mpsc::unbounded_channel();
    let app = Router::new().route(
        "/botTEST/sendMessage",
        post(move |Json(payload): Json<serde_json::Value>| {
            let tx = request_tx.clone();
            async move {
                tx.send(payload).unwrap();
                Json(serde_json::json!({"ok": !reject, "description": "test rejection"}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut config = AppConfig::default();
    config.telegram = crate::config::TelegramConfig {
        enabled,
        bot_token: "TEST".into(),
        chat_id: "12345".into(),
    };
    let (action_tx, actions) = mpsc::channel(8);
    let mut state = AppState::new(
        config,
        Arc::new(Database::open(":memory:").unwrap()),
        action_tx,
    );
    Arc::get_mut(&mut state).unwrap().telegram_notifier =
        TelegramNotifier::with_base_url(reqwest::Client::new(), url.clone());
    NotificationFixture {
        state,
        requests,
        actions,
        url,
        server,
    }
}

async fn next_notification(fixture: &mut NotificationFixture) -> serde_json::Value {
    let request = tokio::time::timeout(Duration::from_secs(1), fixture.requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request["chat_id"], "12345");
    assert_eq!(request["parse_mode"], "HTML");
    request
}

async fn no_more_notifications(fixture: &mut NotificationFixture) {
    assert!(
        tokio::time::timeout(Duration::from_millis(100), fixture.requests.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn global_risk_sends_one_notification_per_trigger_across_pairs() {
    let mut first = notification_fixture(true, false).await;
    let mut second = notification_fixture(true, false).await;
    first
        .state
        .config
        .write()
        .await
        .global_max_position_notional = Some(dec!(100));
    first.state.position.write().await.size = dec!(2);
    {
        let mut ticker = first.state.ticker.write().await;
        ticker.mark_price = dec!(50);
        ticker.mark_update_time = Utc::now();
    }
    let states = Arc::new(RwLock::new(vec![first.state.clone(), second.state.clone()]));
    let global = GlobalPositionLimit::new(states);
    global.membership_ready.store(true, Ordering::SeqCst);
    for state in [&first.state, &second.state] {
        state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        state.global_position_ready.store(true, Ordering::SeqCst);
    }
    assert!(global.check().await);
    let message = next_notification(&mut first).await;
    assert!(message["text"]
        .as_str()
        .unwrap()
        .contains("全局持仓名义价值 100 已达到上限 100"));
    assert_eq!(*first.state.status.read().await, BotStatus::Paused);
    assert_eq!(*second.state.status.read().await, BotStatus::Paused);
    assert!(global.check().await);
    no_more_notifications(&mut first).await;
    no_more_notifications(&mut second).await;
    first.state.position.write().await.size = Decimal::ZERO;
    assert!(!global.check().await);
    first.state.position.write().await.size = dec!(2);
    assert!(global.check().await);
    next_notification(&mut first).await;
    no_more_notifications(&mut second).await;
    first.server.abort();
    second.server.abort();
}

#[tokio::test]
async fn global_risk_notification_reports_limit_when_another_pair_is_initializing() {
    let mut first = notification_fixture(true, false).await;
    let second = notification_fixture(true, false).await;
    first
        .state
        .config
        .write()
        .await
        .global_max_position_notional = Some(dec!(100));
    first.state.position.write().await.size = dec!(2);
    {
        let mut ticker = first.state.ticker.write().await;
        ticker.mark_price = dec!(50);
        ticker.mark_update_time = Utc::now();
    }
    first
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    first
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![
        first.state.clone(),
        second.state.clone(),
    ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    assert!(global.check().await);
    let message = next_notification(&mut first).await;
    let text = message["text"].as_str().unwrap();
    assert!(text.contains("全局持仓名义价值 100 已达到上限 100"));
    assert!(text.contains("全局风控数据不可用或已过期"));
    first.server.abort();
    second.server.abort();
}

#[tokio::test]
async fn global_risk_notification_failure_does_not_prevent_pause() {
    let mut fixture = notification_fixture(true, true).await;
    fixture
        .state
        .config
        .write()
        .await
        .pause_on_zero_available_margin = true;
    fixture.state.account.write().await.asset = "USDC".into();
    fixture.state.account.write().await.total_wallet_balance = Decimal::ZERO;
    fixture
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    fixture
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
    global.membership_ready.store(true, Ordering::SeqCst);
    let mut logs = fixture.state.ws_broadcast_tx.subscribe();
    assert!(global.check().await);
    let message = next_notification(&mut fixture).await;
    assert!(message["text"]
        .as_str()
        .unwrap()
        .contains("可用保证金已达到零"));
    assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
    assert!(fixture.state.global_cancel_pending.load(Ordering::SeqCst));
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let log = logs.recv().await.unwrap();
            if log.contains("Telegram 全局风控通知发送失败") {
                break;
            }
        }
    })
    .await
    .unwrap();
    fixture.server.abort();
}

#[tokio::test]
async fn strategy_status_changes_do_not_send_notifications() {
    let mut fixture = notification_fixture(true, false).await;
    fixture.state.pause_trading().await.unwrap();
    assert_eq!(
        fixture.state.db.load_bot_status().unwrap(),
        Some(BotStatus::Paused)
    );
    fixture.state.resume_trading().await.unwrap();
    fixture.state.pause_trading_silently().await.unwrap();
    fixture.state.pause_trading().await.unwrap();
    assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
    no_more_notifications(&mut fixture).await;
    fixture.server.abort();
}

#[tokio::test]
async fn engine_start_and_position_limit_pause_do_not_notify() {
    for at_limit in [false, true] {
        let mut fixture = notification_fixture(true, false).await;
        {
            let mut config = fixture.state.config.write().await;
            config.grid.pause_on_max_position = at_limit;
            config.grid.max_position_usdc = Some(dec!(1000));
        }
        fixture.state.position.write().await.size = dec!(10);
        {
            let mut ticker = fixture.state.ticker.write().await;
            ticker.last_price = dec!(100);
            ticker.mark_price = dec!(100);
            ticker.mark_update_time = Utc::now();
            ticker.update_time = Utc::now();
        }
        let config = fixture.state.config.read().await.clone();
        let client = Arc::new(crate::exchange::BinanceFuturesClient::with_base_url(
            &config.exchange,
            fixture.url.clone(),
        ));
        let (_, ticker_rx) = broadcast::channel(1);
        // Keep the fixture usable while moving the real action receiver into the engine.
        let (_, unused_actions) = mpsc::channel(1);
        let action_rx = std::mem::replace(&mut fixture.actions, unused_actions);
        let mut engine = crate::strategy::GridTradingEngine::new(
            fixture.state.clone(),
            client,
            action_rx,
            ticker_rx,
        );
        let task = tokio::spawn(async move { engine.run().await });
        if at_limit {
            tokio::time::timeout(Duration::from_secs(1), async {
                while *fixture.state.status.read().await != BotStatus::Paused {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        } else {
            assert_eq!(*fixture.state.status.read().await, BotStatus::Running);
        }
        no_more_notifications(&mut fixture).await;
        task.abort();
        no_more_notifications(&mut fixture).await;
        fixture.server.abort();
    }
}

async fn live_notification_fixture(direction: GridDirection) -> NotificationFixture {
    let fixture = notification_fixture(true, false).await;
    {
        let mut config = fixture.state.config.write().await;
        config.exchange.api_key = "live-key".into();
        config.exchange.api_secret = "live-secret".into();
        config.grid.direction = direction;
    }
    fixture
}

#[tokio::test]
async fn trade_notifications_wait_for_closing_pnl_and_send_each_verified_trade_once() {
    for (direction, pnl, expected) in [
        (GridDirection::Long, dec!(3.25), "+3.2500"),
        (GridDirection::Short, dec!(-2.5), "-2.5000"),
    ] {
        let mut fixture = live_notification_fixture(direction).await;
        let mut closing = cache_test_trade("closing", 1, TradingMode::Live);
        closing.side = direction.closing_side();
        let mut later = cache_test_trade("closing-later", 2, TradingMode::Live);
        later.side = direction.closing_side();
        assert!(
            fixture
                .state
                .record_trade(closing.clone(), None, None, None)
                .await
        );
        assert!(
            fixture
                .state
                .record_trade(later.clone(), None, None, None)
                .await
        );
        no_more_notifications(&mut fixture).await;

        let revision = fixture
            .state
            .db
            .symbol_trade_clear_revision(&closing.symbol);
        closing.realized_pnl = pnl;
        closing.commission = dec!(0.02);
        closing.pnl_verified = true;
        fixture
            .state
            .verify_trade_pnl(closing.clone(), revision)
            .await
            .unwrap();
        let message = next_notification(&mut fixture).await;
        let text = message["text"].as_str().unwrap();
        assert!(text.contains("｜1D｜1H</b>"));
        assert!(text.ends_with(&format!("已实现盈亏：{expected} USDC")));

        fixture
            .state
            .verify_trade_pnl(closing, revision)
            .await
            .unwrap();
        later.pnl_verified = true;
        fixture
            .state
            .verify_trade_pnl(later, revision)
            .await
            .unwrap();
        let message = next_notification(&mut fixture).await;
        let text = message["text"].as_str().unwrap();
        assert!(text.contains("｜2D｜2H</b>"));
        assert!(text.ends_with("已实现盈亏：+0.0000 USDC"));
        no_more_notifications(&mut fixture).await;
        assert_eq!(fixture.state.stats.read().await.pending_pnl_trades, 0);
        assert_eq!(fixture.state.stats.read().await.total_realized_pnl, pnl);
        fixture.server.abort();
    }
}

#[tokio::test]
async fn trade_notifications_keep_openings_immediate_without_repeating_on_pnl_verification() {
    for direction in [GridDirection::Long, GridDirection::Short] {
        let mut fixture = live_notification_fixture(direction).await;
        let mut opening = cache_test_trade("opening", 1, TradingMode::Live);
        opening.side = direction.opening_side();
        assert!(
            fixture
                .state
                .record_trade(opening.clone(), None, None, None)
                .await
        );
        let message = next_notification(&mut fixture).await;
        assert!(!message["text"].as_str().unwrap().contains("已实现盈亏"));

        let revision = fixture
            .state
            .db
            .symbol_trade_clear_revision(&opening.symbol);
        opening.pnl_verified = true;
        fixture
            .state
            .verify_trade_pnl(opening, revision)
            .await
            .unwrap();
        no_more_notifications(&mut fixture).await;
        assert_eq!(fixture.state.stats.read().await.pending_pnl_trades, 0);
        fixture.server.abort();
    }
}

#[tokio::test]
async fn trade_notifications_skip_disabled_or_cleared_pnl_verifications() {
    for clear_history in [false, true] {
        let mut fixture = live_notification_fixture(GridDirection::Long).await;
        let mut closing = cache_test_trade("closing", 1, TradingMode::Live);
        closing.side = OrderSide::Sell;
        assert!(
            fixture
                .state
                .record_trade(closing.clone(), None, None, None)
                .await
        );
        let revision = fixture
            .state
            .db
            .symbol_trade_clear_revision(&closing.symbol);
        if clear_history {
            assert_eq!(
                AppState::clear_trade_history_for(&[fixture.state.clone()])
                    .await
                    .unwrap(),
                1
            );
        } else {
            fixture.state.config.write().await.telegram.enabled = false;
        }

        closing.realized_pnl = dec!(3.25);
        closing.pnl_verified = true;
        fixture
            .state
            .verify_trade_pnl(closing, revision)
            .await
            .unwrap();
        no_more_notifications(&mut fixture).await;
        assert_eq!(fixture.state.stats.read().await.pending_pnl_trades, 0);
        let saved = fixture.state.db.get_recent_trades(1).unwrap();
        if clear_history {
            assert!(saved.is_empty());
        } else {
            assert!(saved[0].pnl_verified);
            assert_eq!(saved[0].realized_pnl, dec!(3.25));
        }
        fixture.server.abort();
    }
}

#[tokio::test]
async fn trade_notifications_capture_position_amount_after_buy_and_sell_fills() {
    let mut fixture = notification_fixture(true, false).await;
    fixture.state.position.write().await.size = dec!(9.5);
    {
        let mut ticker = fixture.state.ticker.write().await;
        ticker.last_price = dec!(100);
        ticker.mark_price = dec!(100);
        ticker.mark_update_time = Utc::now();
        ticker.update_time = Utc::now();
    }
    let config = fixture.state.config.read().await.clone();
    let client = Arc::new(crate::exchange::BinanceFuturesClient::with_base_url(
        &config.exchange,
        fixture.url.clone(),
    ));
    let (ticker_tx, ticker_rx) = broadcast::channel(4);
    let (_, unused_actions) = mpsc::channel(1);
    let action_rx = std::mem::replace(&mut fixture.actions, unused_actions);
    let mut engine = crate::strategy::GridTradingEngine::new(
        fixture.state.clone(),
        client,
        action_rx,
        ticker_rx,
    );
    let task = tokio::spawn(async move { engine.run().await });
    // The engine constructs its initial window asynchronously. Start the
    // notification deadline only after both fillable orders exist.
    let ready = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let orders = fixture.state.active_orders.read().await;
            let buy = orders
                .values()
                .any(|order| order.side == OrderSide::Buy && order.price == dec!(99.9));
            let sell = orders
                .values()
                .any(|order| order.side == OrderSide::Sell && order.price == dec!(100.1));
            if buy && sell {
                break;
            }
            drop(orders);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        ready.is_ok(),
        "initial orders missing; engine_finished={}; status={:?}; orders={:?}; logs={:?}",
        task.is_finished(),
        fixture.state.status.try_read(),
        fixture.state.active_orders.try_read(),
        fixture.state.recent_logs.try_read(),
    );
    for (price, side, size, amount) in [
        (dec!(99.9), "BUY", dec!(10.5), "1050.00"),
        (dec!(100.1), "SELL", dec!(9.51), "951.00"),
    ] {
        ticker_tx
            .send(TickerInfo {
                symbol: config.exchange.symbol.clone(),
                last_price: price,
                mark_price: dec!(100),
                mark_update_time: Utc::now(),
                update_time: Utc::now(),
                ..Default::default()
            })
            .unwrap();
        let message = match tokio::time::timeout(Duration::from_secs(1), fixture.requests.recv())
            .await
        {
            Ok(Some(message)) => message,
            result => panic!(
                "{side} notification missing: {result:?}; engine_finished={}; status={:?}; orders={:?}; trades={:?}; logs={:?}",
                task.is_finished(),
                fixture.state.status.try_read(),
                fixture.state.active_orders.try_read(),
                fixture.state.recent_trades.try_read(),
                fixture.state.recent_logs.try_read(),
            ),
        };
        assert_eq!(message["chat_id"], "12345");
        assert_eq!(message["parse_mode"], "HTML");
        let text = message["text"].as_str().unwrap();
        assert!(text.contains(side));
        assert!(text.contains(&format!("持仓金额：{amount} USDC")));
        assert_eq!(fixture.state.position.read().await.size, size);
    }
    task.abort();
    fixture.server.abort();
}

fn cache_test_trade(id: &str, seconds: i64, mode: TradingMode) -> TradeRecord {
    TradeRecord {
        trade_id: id.into(),
        client_order_id: format!("gb_b_{id}"),
        symbol: "SOLUSDC".into(),
        mode,
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        realized_pnl: Decimal::ZERO,
        commission: Decimal::ZERO,
        pnl_verified: false,
        is_maker: true,
        timestamp: chrono::DateTime::from_timestamp(1_700_000_000 + seconds, 0).unwrap(),
        note: String::new(),
    }
}

#[tokio::test]
async fn recent_trade_cache_keeps_latest_after_out_of_order_backfill_and_reload() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, _) = mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db.clone(), action_tx);
    // A permutation of 0..260 forces old and new fills to arrive in mixed order.
    for i in 0..260 {
        let timestamp = (i * 73) % 260;
        let trade = cache_test_trade(&format!("paper-{timestamp}"), timestamp, TradingMode::Paper);
        assert!(state.record_trade(trade, None, None, None).await);
    }
    // Equal timestamps use insertion order, including at the retention boundary.
    for id in ["tie-first", "tie-second"] {
        assert!(
            state
                .record_trade(
                    cache_test_trade(id, 200, TradingMode::Paper),
                    None,
                    None,
                    None
                )
                .await
        );
    }
    let cached = state.recent_trades.read().await.clone();
    assert_eq!(cached.len(), 200);
    assert_eq!(
        cached.front().unwrap().timestamp,
        cache_test_trade("x", 62, TradingMode::Paper).timestamp
    );
    assert!(cached
        .iter()
        .zip(cached.iter().skip(1))
        .all(|(a, b)| a.timestamp <= b.timestamp));
    let ids: Vec<_> = cached.iter().map(|t| t.trade_id.as_str()).collect();
    assert!(
        ids.iter().position(|id| *id == "tie-first").unwrap()
            < ids.iter().position(|id| *id == "tie-second").unwrap()
    );
    let snapshot = state.snapshot().await;
    let expected: Vec<_> = cached
        .iter()
        .rev()
        .take(50)
        .map(|t| t.trade_id.clone())
        .collect();
    assert_eq!(
        snapshot
            .recent_trades
            .iter()
            .map(|t| t.trade_id.clone())
            .collect::<Vec<_>>(),
        expected
    );

    let (tx, _) = mpsc::channel(1);
    let restarted = AppState::new(AppConfig::default(), db.clone(), tx);
    assert_eq!(
        restarted
            .recent_trades
            .read()
            .await
            .iter()
            .map(|t| &t.trade_id)
            .collect::<Vec<_>>(),
        cached.iter().map(|t| &t.trade_id).collect::<Vec<_>>()
    );
    for i in 0..205 {
        db.insert_trade(&cache_test_trade(
            &format!("live-{i}"),
            i,
            TradingMode::Live,
        ))
        .unwrap();
    }
    state.config.write().await.exchange.api_key = "live-key".into();
    state.config.write().await.exchange.api_secret = "live-secret".into();
    state.refresh_current_scope().await;
    let live = state.recent_trades.read().await;
    assert_eq!(live.len(), 200);
    assert_eq!(live.front().unwrap().trade_id, "live-5");
    assert_eq!(live.back().unwrap().trade_id, "live-204");
    drop(live);
    state.config.write().await.exchange.api_key.clear();
    state.config.write().await.exchange.api_secret.clear();
    state.refresh_current_scope().await;
    assert_eq!(
        state
            .recent_trades
            .read()
            .await
            .iter()
            .map(|t| t.trade_id.clone())
            .collect::<Vec<_>>(),
        cached
            .iter()
            .map(|t| t.trade_id.clone())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn canceled_verification_request_still_publishes_its_committed_result() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&AppConfig::default()).unwrap();
    let (tx, _) = mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db.clone(), tx);
    let mut trade = cache_test_trade("cancel-verification", 1, TradingMode::Paper);
    trade.pnl_verified = false;
    trade.realized_pnl = Decimal::ZERO;
    trade.commission = Decimal::ZERO;
    assert!(state.record_trade(trade.clone(), None, None, None).await);
    let clear_revision = db.symbol_trade_clear_revision(&trade.symbol);
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    db.set_membership_commit_hook("add", reached_tx, resume_rx);
    let writer = tokio::task::spawn_blocking({
        let db = db.clone();
        move || {
            let mut config = AppConfig::default();
            config.exchange.symbol = "ETHUSDT".into();
            db.add_pair_config(&config)
        }
    });
    reached_rx.await.unwrap();
    trade.realized_pnl = dec!(3.25);
    trade.commission = dec!(0.02);
    trade.pnl_verified = true;
    let request = tokio::spawn({
        let state = state.clone();
        async move { state.verify_trade_pnl(trade, clear_revision).await }
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while state.trade_commit_lock.try_lock().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    resume_tx.send(()).unwrap();
    writer.await.unwrap().unwrap();
    let _commit = tokio::time::timeout(Duration::from_secs(3), state.trade_commit_lock.lock())
        .await
        .unwrap();
    let stats = state.stats.read().await;
    assert_eq!(stats.total_realized_pnl, dec!(3.25));
    assert_eq!(stats.total_commission, dec!(0.02));
    assert_eq!(stats.pending_pnl_trades, 0);
    let recent = state.recent_trades.read().await;
    assert!(recent[0].pnl_verified);
    assert_eq!(recent[0].realized_pnl, dec!(3.25));
    assert_eq!(
        db.get_pnl_totals("SOLUSDC", TradingMode::Paper).unwrap(),
        (dec!(3.25), dec!(0.02), 0)
    );
}

#[tokio::test]
async fn canceled_verification_waiter_never_commits_after_the_lock_is_released() {
    use std::future::Future;
    use std::task::Poll;

    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _) = mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db.clone(), tx);
    let mut trade = cache_test_trade("cancel-waiting-verification", 1, TradingMode::Paper);
    assert!(state.record_trade(trade.clone(), None, None, None).await);
    let clear_revision = db.symbol_trade_clear_revision(&trade.symbol);
    trade.realized_pnl = dec!(3.25);
    trade.commission = dec!(0.02);
    trade.pnl_verified = true;

    let commit = state.trade_commit_lock.lock().await;
    let (waiting_tx, waiting_rx) = tokio::sync::oneshot::channel();
    let request = tokio::spawn({
        let state = state.clone();
        async move {
            let verification = state.verify_trade_pnl(trade, clear_revision);
            tokio::pin!(verification);
            std::future::poll_fn(|cx| {
                assert!(verification.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            waiting_tx.send(()).unwrap();
            verification.await
        }
    });
    waiting_rx.await.unwrap();
    tokio::task::yield_now().await;
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    drop(commit);

    let _commit = tokio::time::timeout(Duration::from_secs(3), state.trade_commit_lock.lock())
        .await
        .unwrap();
    let stats = state.stats.read().await;
    assert_eq!(stats.total_realized_pnl, Decimal::ZERO);
    assert_eq!(stats.total_commission, Decimal::ZERO);
    assert_eq!(stats.pending_pnl_trades, 1);
    let recent = state.recent_trades.read().await;
    assert!(!recent[0].pnl_verified);
    assert_eq!(recent[0].realized_pnl, Decimal::ZERO);
    assert_eq!(recent[0].commission, Decimal::ZERO);
    assert_eq!(
        db.get_pnl_totals("SOLUSDC", TradingMode::Paper).unwrap(),
        (Decimal::ZERO, Decimal::ZERO, 1)
    );
}

#[tokio::test]
async fn recent_trade_verification_preserves_cached_timestamp() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _) = mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db, tx);
    let original = cache_test_trade("verify", 1, TradingMode::Paper);
    assert!(state.record_trade(original.clone(), None, None, None).await);
    let mut verified = original.clone();
    verified.timestamp += chrono::Duration::seconds(100);
    verified.realized_pnl = dec!(3);
    let clear_revision = state.db.symbol_trade_clear_revision(&verified.symbol);
    state
        .verify_trade_pnl(verified, clear_revision)
        .await
        .unwrap();
    let cached = state.recent_trades.read().await;
    assert_eq!(cached[0].timestamp, original.timestamp);
    assert_eq!(cached[0].realized_pnl, dec!(3));
    assert!(cached[0].pnl_verified);
}

#[tokio::test]
async fn switching_mode_reloads_only_that_modes_trades() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    for (id, mode, pnl) in [
        ("paper", TradingMode::Paper, dec!(10)),
        ("live", TradingMode::Live, dec!(-2)),
    ] {
        db.insert_trade(&TradeRecord {
            trade_id: id.into(),
            client_order_id: format!("gb_b_{id}"),
            symbol: "SOLUSDC".into(),
            mode,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            realized_pnl: pnl,
            commission: Decimal::ZERO,
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: String::new(),
        })
        .unwrap();
    }
    let (action_tx, _) = mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db, action_tx);
    assert_eq!(state.stats.read().await.total_realized_pnl, dec!(10));
    assert_eq!(state.recent_trades.read().await.len(), 1);
    let snapshot = state.snapshot().await;
    let hourly = snapshot.hourly_trade_stats.unwrap();
    assert_eq!(hourly.buckets.iter().map(|b| b.buy_count).sum::<u64>(), 1);

    state.config.write().await.exchange.api_key = "live-key".into();
    state.config.write().await.exchange.api_secret = "live-secret".into();
    state.refresh_current_scope().await;
    assert_eq!(state.stats.read().await.total_realized_pnl, dec!(-2));
    let trades = state.recent_trades.read().await;
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].mode, TradingMode::Live);
    drop(trades);
    let snapshot = state.snapshot().await;
    let hourly = snapshot.hourly_trade_stats.unwrap();
    assert_eq!(hourly.buckets.iter().map(|b| b.buy_count).sum::<u64>(), 1);
}

#[tokio::test]
async fn retired_verification_is_left_for_the_replacement_state() {
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&config).unwrap();
    let commit = Arc::new(Mutex::new(()));
    let (tx, _) = mpsc::channel(1);
    let retired = AppState::new_for_pair(config.clone(), db.clone(), tx, commit.clone());
    let mut trade = cache_test_trade("retired-verification", 1, TradingMode::Paper);
    trade.pnl_verified = false;
    trade.realized_pnl = Decimal::ZERO;
    trade.commission = Decimal::ZERO;
    assert!(retired.record_trade(trade.clone(), None, None, None).await);
    let revision = db.symbol_trade_clear_revision(&trade.symbol);
    trade.realized_pnl = dec!(3.25);
    trade.commission = dec!(0.02);
    trade.pnl_verified = true;

    retired.retire();
    retired.drain_trade_commits().await;
    retired
        .verify_trade_pnl(trade.clone(), revision)
        .await
        .unwrap();
    assert_eq!(
        db.get_pnl_totals("SOLUSDC", TradingMode::Paper).unwrap(),
        (Decimal::ZERO, Decimal::ZERO, 1)
    );
    assert!(!retired.recent_trades.read().await[0].pnl_verified);

    let (tx, _) = mpsc::channel(1);
    let replacement = AppState::new_for_pair(config, db.clone(), tx, commit);
    replacement.verify_trade_pnl(trade, revision).await.unwrap();
    let stats = replacement.stats.read().await;
    assert_eq!(stats.total_realized_pnl, dec!(3.25));
    assert_eq!(stats.total_commission, dec!(0.02));
    assert_eq!(stats.pending_pnl_trades, 0);
    assert!(replacement.recent_trades.read().await[0].pnl_verified);
    assert_eq!(
        db.get_pnl_totals("SOLUSDC", TradingMode::Paper).unwrap(),
        (dec!(3.25), dec!(0.02), 0)
    );
}

#[tokio::test]
async fn global_margin_guard_fails_closed_on_decimal_overflow_without_panicking() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    config.pause_on_zero_available_margin = true;
    config.min_available_margin_pct = Some(dec!(50));
    let (tx, _rx) = mpsc::channel(4);
    let state = AppState::new(config.clone(), db, tx);
    state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    state.global_position_ready.store(true, Ordering::SeqCst);
    {
        let mut account = state.account.write().await;
        account.asset = "USDC".into();
        account.available_balance = Decimal::MAX;
        account.margin_balance = Decimal::MAX;
        account.update_time = Utc::now();
    }
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![state.clone()])));
    let _ = state.global_position_limit.set(Arc::downgrade(&global));
    global.initialize_config(crate::config::GlobalConfig::from_app(&config));
    global.membership_ready.store(true, Ordering::SeqCst);
    assert!(global.check().await);
}

#[tokio::test]
async fn record_trade_saturates_counters_and_volume_on_extreme_stats() {
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&config).unwrap();
    let (tx, _rx) = mpsc::channel(1);
    let state = AppState::new(config, db, tx);
    {
        let mut stats = state.stats.write().await;
        stats.total_trades = usize::MAX;
        stats.completed_cycles = usize::MAX;
        stats.pending_pnl_trades = usize::MAX;
        stats.total_volume_usdc = Decimal::MAX;
    }
    let mut trade = cache_test_trade("saturating-trade", 1, TradingMode::Paper);
    trade.amount_usdc = dec!(100);
    trade.pnl_verified = false;
    assert!(state.record_trade(trade, Some(dec!(1)), None, None).await);
    let stats = state.stats.read().await;
    assert_eq!(stats.total_trades, usize::MAX);
    assert_eq!(stats.completed_cycles, usize::MAX);
    assert_eq!(stats.pending_pnl_trades, usize::MAX);
    assert_eq!(stats.total_volume_usdc, Decimal::MAX);
}

#[tokio::test]
async fn live_trade_notification_uses_configured_sync_interval_for_account_freshness() {
    let mut fixture = live_notification_fixture(GridDirection::Long).await;
    fixture
        .state
        .config
        .write()
        .await
        .exchange
        .sync_interval_secs = 30;
    {
        let mut account = fixture.state.account.write().await;
        account.asset = "USDC".into();
        account.total_wallet_balance = dec!(10000);
        account.margin_balance = dec!(10123.45);
        account.available_balance = dec!(8000);
        account.update_time = Utc::now() - chrono::Duration::seconds(45);
    }
    let mut buy = cache_test_trade("long-sync-interval-buy", 1, TradingMode::Live);
    buy.side = OrderSide::Buy;
    buy.pnl_verified = true;
    assert!(fixture.state.record_trade(buy, None, None, None).await);
    let message = next_notification(&mut fixture).await;
    let text = message["text"].as_str().unwrap();
    assert!(text.contains("账户权益：10123.45 USDC"), "{text}");
    assert!(text.contains("可用余额：8000.00 USDC｜80.00%"), "{text}");
    fixture.server.abort();
}
