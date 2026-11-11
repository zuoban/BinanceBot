use super::super::*;
use super::fixture_with_database;
use crate::config::{GlobalConfig, TradingPairConfig};
use crate::db::Database;
use crate::types::{BotControlAction, BotStatus, TradingMode};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;

#[tokio::test]
async fn engine_start_fails_when_execution_guard_cannot_be_written() {
    let path =
        std::env::temp_dir().join(format!("guard-write-failure-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    let config = AppConfig::default();
    db.save_config(&config).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_guard BEFORE INSERT ON pair_execution_guard BEGIN SELECT RAISE(ABORT, 'guard write failed'); END;").unwrap();
    drop(connection);
    assert!(TradingRuntime::start_with_test_exchange(
        &config,
        db.clone(),
        "http://127.0.0.1:1".into()
    )
    .await
    .is_err());
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn later_startup_failure_cleans_up_prepared_engine_and_preserves_unstarted_metadata() {
    let app = Router::new().fallback(|| async { std::future::pending::<StatusCode>().await });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let directory =
        std::env::temp_dir().join(format!("runtime-start-failure-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("bot.db");
    let config = AppConfig {
        pairs: ["SOLUSDC", "BTCUSDT"]
            .map(|symbol| TradingPairConfig::new(symbol.into()))
            .to_vec(),
        ..Default::default()
    };
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_config(&config).unwrap();
    for symbol in ["SOLUSDC", "BTCUSDT"] {
        db.save_pair_status(symbol, TradingMode::Paper, BotStatus::Running)
            .unwrap();
    }
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_second_guard BEFORE INSERT ON pair_execution_guard WHEN NEW.symbol = 'BTCUSDT' BEGIN SELECT RAISE(ABORT, 'second guard failed'); END;").unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        TradingRuntime::start_with_test_exchange(&config, db.clone(), url),
    )
    .await
    .expect("startup cleanup timed out");
    let error = match result {
        Ok(_) => panic!("startup unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(format!("{error:#}").contains("second guard failed"));
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
        Some(BotStatus::Paused)
    );
    assert_eq!(
        db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
        Some(BotStatus::Running)
    );
    let first_guard: i64 = connection
        .query_row(
            "SELECT dirty FROM pair_execution_guard WHERE symbol = 'SOLUSDC' AND mode = 'PAPER'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let second_guards: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM pair_execution_guard WHERE symbol = 'BTCUSDT'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(first_guard, 0);
    assert_eq!(second_guards, 0);
    drop(connection);
    drop(db);
    server.abort();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn startup_already_interrupted_does_not_arm_guards_or_change_status() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let config = AppConfig {
        pairs: ["SOLUSDC", "BTCUSDT"]
            .map(|symbol| TradingPairConfig::new(symbol.into()))
            .to_vec(),
        ..Default::default()
    };
    db.save_config(&config).unwrap();
    for symbol in ["SOLUSDC", "BTCUSDT"] {
        db.save_pair_status(symbol, TradingMode::Paper, BotStatus::Running)
            .unwrap();
    }
    let (_interrupt_tx, interrupt_rx) = watch::channel(true);
    let (hook_tx, mut hook_rx) = mpsc::channel(1);
    let result = TradingRuntime::start_with_test_exchange_and_hook(
        &config,
        db.clone(),
        "http://127.0.0.1:1".into(),
        interrupt_rx,
        hook_tx,
    )
    .await;
    assert!(result.is_err());
    assert!(hook_rx.try_recv().is_err());
    for symbol in ["SOLUSDC", "BTCUSDT"] {
        assert_eq!(
            db.load_pair_status(symbol, TradingMode::Paper).unwrap(),
            Some(BotStatus::Running)
        );
        assert!(!db.arm_execution_guard(symbol, TradingMode::Paper).unwrap());
    }
}

#[tokio::test]
async fn startup_reports_cleanup_failure_and_retains_dirty_recovery_guard() {
    let app = Router::new().fallback(|| async { std::future::pending::<StatusCode>().await });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let directory = std::env::temp_dir().join(format!(
        "runtime-start-cleanup-failure-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("bot.db");
    let config = AppConfig {
        pairs: ["SOLUSDC", "BTCUSDT"]
            .map(|symbol| TradingPairConfig::new(symbol.into()))
            .to_vec(),
        ..Default::default()
    };
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_config(&config).unwrap();
    db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Running)
        .unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_second_guard BEFORE INSERT ON pair_execution_guard WHEN NEW.symbol = 'BTCUSDT' BEGIN SELECT RAISE(ABORT, 'second guard failed'); END;
        CREATE TRIGGER reject_guard_clear BEFORE UPDATE ON pair_execution_guard WHEN OLD.symbol = 'SOLUSDC' AND NEW.dirty = 0 BEGIN SELECT RAISE(ABORT, 'guard clear failed'); END;").unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        TradingRuntime::start_with_test_exchange(&config, db.clone(), url),
    )
    .await
    .expect("startup cleanup timed out");
    let error = match result {
        Ok(_) => panic!("startup unexpectedly succeeded"),
        Err(error) => format!("{error:#}"),
    };
    assert!(error.contains("second guard failed"), "{error}");
    assert!(error.contains("guard clear failed"), "{error}");
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
        Some(BotStatus::Paused)
    );
    let dirty: i64 = connection
        .query_row(
            "SELECT dirty FROM pair_execution_guard WHERE symbol = 'SOLUSDC' AND mode = 'PAPER'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(dirty, 1);
    drop(connection);
    drop(db);
    server.abort();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn interrupted_startup_cleans_every_prepared_engine_and_leaves_unstarted_pair_untouched() {
    let app = Router::new().fallback(|| async { std::future::pending::<StatusCode>().await });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let directory = std::env::temp_dir().join(format!(
        "runtime-start-interrupted-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("bot.db");
    let config = AppConfig {
        pairs: ["SOLUSDC", "BTCUSDT", "ETHUSDT"]
            .map(|symbol| TradingPairConfig::new(symbol.into()))
            .to_vec(),
        ..Default::default()
    };
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_config(&config).unwrap();
    for symbol in ["SOLUSDC", "BTCUSDT", "ETHUSDT"] {
        db.save_pair_status(symbol, TradingMode::Paper, BotStatus::Running)
            .unwrap();
    }
    let (interrupt_tx, interrupt_rx) = watch::channel(false);
    let (hook_tx, mut hook_rx) = mpsc::channel(1);
    let startup_db = db.clone();
    let startup = tokio::spawn(async move {
        TradingRuntime::start_with_test_exchange_and_hook(
            &config,
            startup_db,
            url,
            interrupt_rx,
            hook_tx,
        )
        .await
    });
    for expected in ["SOLUSDC", "BTCUSDT"] {
        let (symbol, ack) = tokio::time::timeout(Duration::from_secs(5), hook_rx.recv())
            .await
            .unwrap()
            .expect("startup ended before preparing expected pair");
        assert_eq!(symbol, expected);
        if expected == "BTCUSDT" {
            interrupt_tx.send(true).unwrap();
        }
        ack.send(()).unwrap();
    }
    let result = tokio::time::timeout(Duration::from_secs(10), startup)
        .await
        .expect("interrupted startup cleanup timed out")
        .unwrap();
    assert!(result.is_err());
    let connection = rusqlite::Connection::open(&path).unwrap();
    for symbol in ["SOLUSDC", "BTCUSDT"] {
        assert_eq!(
            db.load_pair_status(symbol, TradingMode::Paper).unwrap(),
            Some(BotStatus::Paused)
        );
        let dirty: i64 = connection
            .query_row(
                "SELECT dirty FROM pair_execution_guard WHERE symbol = ?1 AND mode = 'PAPER'",
                [symbol],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(dirty, 0, "{symbol} guard was not cleared after cleanup");
    }
    assert_eq!(
        db.load_pair_status("ETHUSDT", TradingMode::Paper).unwrap(),
        Some(BotStatus::Running)
    );
    let unstarted_guards: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM pair_execution_guard WHERE symbol = 'ETHUSDT'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(unstarted_guards, 0);
    drop(connection);
    drop(db);
    server.abort();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn interrupt_during_pair_preparation_blocks_orders_and_cleans_up_guards() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let order_submissions = Arc::new(AtomicUsize::new(0));
    let observed_orders = order_submissions.clone();
    let app = Router::new().fallback(move |request: Request<Body>| {
        let observed_orders = observed_orders.clone();
        async move {
            if request.method() == axum::http::Method::POST
                && request.uri().path() == "/fapi/v1/order"
            {
                observed_orders.fetch_add(1, Ordering::SeqCst);
            }
            if request.method() == axum::http::Method::GET
                && request.uri().path() == "/fapi/v1/openOrders"
            {
                return (StatusCode::OK, "[]");
            }
            std::future::pending::<(StatusCode, &'static str)>().await
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let directory = std::env::temp_dir().join(format!(
        "runtime-prepare-interrupted-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("bot.db");
    let mut config = AppConfig {
        pairs: ["SOLUSDC", "BTCUSDT", "ETHUSDT"]
            .map(|symbol| TradingPairConfig::new(symbol.into()))
            .to_vec(),
        ..Default::default()
    };
    config.exchange.api_key = "test-key".into();
    config.exchange.api_secret = "test-secret".into();
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_config(&config).unwrap();
    for symbol in ["SOLUSDC", "BTCUSDT", "ETHUSDT"] {
        db.save_pair_status(symbol, TradingMode::Live, BotStatus::Running)
            .unwrap();
    }
    let (interrupt_tx, interrupt_rx) = watch::channel(false);
    let (hook_tx, mut hook_rx) = mpsc::channel(1);
    let startup_db = db.clone();
    let startup = tokio::spawn(async move {
        TradingRuntime::start_with_test_exchange_and_prepare_hook(
            &config,
            startup_db,
            url,
            interrupt_rx,
            hook_tx,
        )
        .await
    });
    for expected in ["SOLUSDC", "BTCUSDT"] {
        let (symbol, ack) = tokio::time::timeout(Duration::from_secs(5), hook_rx.recv())
            .await
            .unwrap()
            .expect("startup ended before reaching prepare hook");
        assert_eq!(symbol, expected);
        if expected == "BTCUSDT" {
            interrupt_tx.send(true).unwrap();
        }
        ack.send(()).unwrap();
    }
    let result = tokio::time::timeout(Duration::from_secs(10), startup)
        .await
        .expect("interrupted startup cleanup timed out")
        .unwrap();
    assert!(result.is_err());
    assert!(
        hook_rx.try_recv().is_err(),
        "unstarted pair reached prepare hook"
    );
    assert_eq!(order_submissions.load(Ordering::SeqCst), 0);
    let connection = rusqlite::Connection::open(&path).unwrap();
    for symbol in ["SOLUSDC", "BTCUSDT"] {
        assert_eq!(
            db.load_pair_status(symbol, TradingMode::Live).unwrap(),
            Some(BotStatus::Paused)
        );
        let dirty: i64 = connection
            .query_row(
                "SELECT dirty FROM pair_execution_guard WHERE symbol = ?1 AND mode = 'LIVE'",
                [symbol],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(dirty, 0, "{symbol} guard was not cleared after cleanup");
    }
    assert_eq!(
        db.load_pair_status("ETHUSDT", TradingMode::Live).unwrap(),
        Some(BotStatus::Running)
    );
    let unstarted_guards: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM pair_execution_guard WHERE symbol = 'ETHUSDT'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(unstarted_guards, 0);
    drop(connection);
    drop(db);
    server.abort();
    std::fs::remove_dir_all(directory).unwrap();
}

async fn wait_for_runtime_initialization(runtime: &TradingRuntime) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if runtime.states().iter().all(|state| {
                state
                    .global_position_initialized
                    .load(std::sync::atomic::Ordering::SeqCst)
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("runtime initialization timed out");
    for state in runtime.states() {
        assert!(state
            .global_position_ready
            .load(std::sync::atomic::Ordering::SeqCst));
    }
}

#[tokio::test]
async fn restart_restores_each_pair_after_clean_shutdown() {
    // Paper mode tolerates market-data outages; the local endpoint guarantees that
    // startup never contacts Binance and no market price can trigger paper orders.
    let app = Router::new().fallback(|| async { StatusCode::SERVICE_UNAVAILABLE });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let directory = std::env::temp_dir().join(format!("runtime-restart-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("bot.db");
    let config = AppConfig {
        pairs: ["SOLUSDC", "BTCUSDT"]
            .map(|symbol| TradingPairConfig::new(symbol.into()))
            .to_vec(),
        ..Default::default()
    };
    assert!(config.exchange.is_paper());
    {
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_config(&config).unwrap();
        let mut runtime = TradingRuntime::start_with_test_exchange(&config, db, url.clone())
            .await
            .unwrap();
        wait_for_runtime_initialization(&runtime).await;
        let states = runtime.states();
        states[0].resume_trading().await.unwrap();
        states[1].pause_trading().await.unwrap();
        drop(states);
        runtime.shutdown(Duration::from_secs(10)).await.unwrap();
    }
    {
        let db = Arc::new(Database::open(&path).unwrap());
        assert_eq!(
            db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
            Some(BotStatus::Running)
        );
        assert_eq!(
            db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
            Some(BotStatus::Paused)
        );
        let mut runtime = TradingRuntime::start_with_test_exchange(&config, db, url)
            .await
            .unwrap();
        wait_for_runtime_initialization(&runtime).await;
        let states = runtime.states();
        assert_eq!(*states[0].status.read().await, BotStatus::Running);
        assert_eq!(*states[1].status.read().await, BotStatus::Paused);
        drop(states);
        runtime.shutdown(Duration::from_secs(10)).await.unwrap();
    }
    server.abort();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn failed_runtime_shutdown_keeps_running_pairs_paused_after_reopen() {
    let directory =
        std::env::temp_dir().join(format!("runtime-failed-shutdown-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("bot.db");
    {
        let db = Arc::new(Database::open(&path).unwrap());
        let mut runtime = fixture_with_database(db, true, false);
        for state in runtime.states() {
            state
                .global_position_initialized
                .store(true, std::sync::atomic::Ordering::SeqCst);
            state
                .global_position_ready
                .store(true, std::sync::atomic::Ordering::SeqCst);
            state.resume_trading().await.unwrap();
        }
        assert!(runtime.shutdown(Duration::from_secs(2)).await.is_err());
    }
    {
        let db = Database::open(&path).unwrap();
        for symbol in ["SOLUSDC", "BTCUSDT"] {
            assert_eq!(
                db.load_pair_status(symbol, TradingMode::Paper).unwrap(),
                Some(BotStatus::Paused)
            );
        }
    }
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn manual_pause_before_recovery_commit_remains_paused_after_reopen() {
    for pause_before_commit in [true, false] {
        let directory =
            std::env::temp_dir().join(format!("runtime-pause-order-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("bot.db");
        {
            let db = Arc::new(Database::open(&path).unwrap());
            let (tx, _rx) = mpsc::channel(1);
            let config = AppConfig::default();
            db.save_config(&config).unwrap();
            let state = AppState::new(config, db, tx);
            state
                .global_position_initialized
                .store(true, std::sync::atomic::Ordering::SeqCst);
            state
                .global_position_ready
                .store(true, std::sync::atomic::Ordering::SeqCst);
            state.resume_trading().await.unwrap();
            state.pause_for_shutdown().await.unwrap();
            if pause_before_commit {
                state.pause_trading().await.unwrap();
            }
            state.commit_shutdown_recovery().await.unwrap();
            if !pause_before_commit {
                assert!(state.pause_trading().await.is_err());
            }
        }
        {
            let db = Database::open(&path).unwrap();
            assert_eq!(
                db.load_bot_status().unwrap(),
                Some(if pause_before_commit {
                    BotStatus::Paused
                } else {
                    BotStatus::Running
                })
            );
        }
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[tokio::test]
async fn shutdown_during_initialization_does_not_restore_running_status() {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let entered = Arc::new(std::sync::Mutex::new(Some(entered_tx)));
    let app = Router::new().fallback(move || {
        let entered = entered.clone();
        async move {
            if let Some(sender) = entered.lock().unwrap().take() {
                let _ = sender.send(());
            }
            std::future::pending::<StatusCode>().await
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let directory =
        std::env::temp_dir().join(format!("runtime-interrupted-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("bot.db");
    let config = AppConfig::default();
    {
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_config(&config).unwrap();
        db.save_bot_status(BotStatus::Running).unwrap();
        let mut runtime = TradingRuntime::start_with_test_exchange(&config, db, url)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .unwrap()
            .unwrap();
        runtime.shutdown(Duration::from_secs(5)).await.unwrap();
    }
    {
        let db = Database::open(&path).unwrap();
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    }
    server.abort();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn concurrent_safety_pause_prevents_shutdown_recovery() {
    let directory =
        std::env::temp_dir().join(format!("runtime-concurrent-pause-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("bot.db");
    {
        let db = Arc::new(Database::open(&path).unwrap());
        let config = AppConfig::default();
        db.save_config(&config).unwrap();
        let lock = Arc::new(Mutex::new(()));
        let (tx, mut rx) = mpsc::channel(1);
        let state = AppState::new(config.clone(), db.clone(), tx);
        state
            .global_position_initialized
            .store(true, std::sync::atomic::Ordering::SeqCst);
        state
            .global_position_ready
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let (received_tx, received_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let engine = tokio::spawn(async move {
            let BotControlAction::Shutdown { reply, .. } = rx.recv().await.unwrap() else {
                panic!("expected shutdown")
            };
            received_tx.send(()).unwrap();
            release_rx.await.unwrap();
            reply.send(Ok(())).unwrap();
        });
        let (shutdown_tx, _) = watch::channel(false);
        let pair = PairTask {
            state: state.clone(),
            engine,
            engine_joined: false,
            recovery_failed: false,
            shutdown_tx,
            background: vec![],
        };
        let mut runtime = TradingRuntime::from_pairs(
            vec![pair],
            db,
            lock,
            config.server.clone(),
            GlobalConfig::from_app(&config),
            None,
        );
        state.resume_trading().await.unwrap();
        let shutdown = tokio::spawn(async move { runtime.shutdown(Duration::from_secs(5)).await });
        received_rx.await.unwrap();
        state.pause_trading().await.unwrap();
        release_tx.send(()).unwrap();
        shutdown.await.unwrap().unwrap();
    }
    {
        let db = Database::open(&path).unwrap();
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    }
    std::fs::remove_dir_all(directory).unwrap();
}
