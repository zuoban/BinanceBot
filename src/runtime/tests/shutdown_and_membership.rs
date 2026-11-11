use super::super::*;
use super::{
    exchange_info, fixture, fixture_with_database, fixture_with_shutdown_observer,
    fixture_with_unresponsive_engine, mock_exchange, payload, request, valid_filters,
};
use crate::config::{GlobalConfig, TradingPairConfig};
use crate::db::Database;
use crate::server::create_managed_router;
use crate::types::{BotControlAction, BotStatus, TradingMode};
use axum::http::StatusCode;
use axum::Router;
use serde_json::json;

#[tokio::test]
async fn startup_registers_engines_for_early_shutdown_timeout() {
    let (entered_tx, entered_rx) = oneshot::channel();
    let entered = Arc::new(std::sync::Mutex::new(Some(entered_tx)));
    let app = Router::new().fallback(move || {
        let entered = entered.clone();
        async move {
            if let Some(tx) = entered.lock().unwrap().take() {
                let _ = tx.send(());
            }
            std::future::pending::<StatusCode>().await
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let db = Arc::new(Database::open(":memory:").unwrap());
    let config = AppConfig {
        pairs: vec![TradingPairConfig::new("SOLUSDC".into())],
        ..Default::default()
    };
    db.save_config(&config).unwrap();
    let mut runtime = TradingRuntime::start_with_test_exchange(&config, db, url)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(!runtime.pairs[0].engine.is_finished());

    let handle = runtime.handle();
    let state = runtime.pairs[0].state.clone();
    let guard = state.status.write().await;
    handle.request_shutdown();
    handle.start_early_shutdown(Duration::from_millis(50)).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while !runtime.pairs[0].engine.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("early shutdown did not abort the startup engine");
    drop(guard);
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        runtime.shutdown(Duration::from_secs(5)),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("SOLUSDC"), "{error}");
    server.abort();
}

#[tokio::test]
async fn cancelled_exit_monitor_never_repolls_consumed_engine() {
    let mut runtime = fixture();
    let state = runtime.pairs[0].state.clone();
    let config_guard = state.config.write().await;
    runtime.pairs[0].engine.abort();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !runtime.pairs[0].engine.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), runtime.wait_for_exit())
            .await
            .is_err()
    );
    drop(config_guard);

    let reason = tokio::time::timeout(Duration::from_secs(5), runtime.wait_for_exit())
        .await
        .unwrap();
    assert!(reason.contains("SOLUSDC"), "{reason}");
    assert!(runtime.pairs[0].engine_joined);
    let reason = tokio::time::timeout(Duration::from_secs(5), runtime.wait_for_exit())
        .await
        .expect("joined engine exit was not reported");
    assert!(reason.contains("SOLUSDC"), "{reason}");
    let error = runtime.remove_pair("SOLUSDC").await.unwrap_err();
    assert!(error.to_string().contains("策略引擎已退出"), "{error}");
    assert_eq!(runtime.states().len(), 2);
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        runtime.shutdown(Duration::from_secs(5)),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("SOLUSDC"), "{error}");
}

#[tokio::test]
async fn early_shutdown_reports_engine_already_joined_by_run() {
    let mut runtime = fixture();
    runtime.pairs[0].engine.abort();
    let _ = (&mut runtime.pairs[0].engine).await;
    let (exit_tx, exit_rx) = oneshot::channel::<()>();
    runtime.pairs[0].engine = tokio::spawn(async move {
        let _ = exit_rx.await;
    });
    runtime.handle.register_engine(&runtime.pairs[0]);
    let handle = runtime.handle();
    let reason = {
        let run = runtime.run();
        tokio::pin!(run);
        tokio::select! {
            biased;
            reason = &mut run => panic!("runtime exited before engine was released: {reason}"),
            _ = tokio::task::yield_now() => {}
        }
        exit_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), &mut run)
            .await
            .unwrap()
    };
    assert!(reason.contains("SOLUSDC"), "{reason}");
    assert!(runtime.pairs[0].engine_joined);
    handle.request_shutdown();
    handle.start_early_shutdown(Duration::from_secs(5)).await;
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        runtime.shutdown(Duration::from_secs(5)),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("SOLUSDC"), "{error}");
}

#[tokio::test]
async fn early_shutdown_aborts_unresponsive_engine_before_membership_commit_returns() {
    for operation in ["add", "remove"] {
        let directory = std::env::temp_dir().join(format!(
            "runtime-early-timeout-{operation}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&directory).unwrap();
        let db = Arc::new(Database::open(directory.join("bot.db")).unwrap());
        let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
        let mut runtime =
            fixture_with_unresponsive_engine(db.clone(), false, false, Some(observed_tx), true);
        let (url, server) =
            mock_exchange(StatusCode::OK, exchange_info("TRADING", valid_filters())).await;
        runtime.test_exchange_url = Some(url);
        let states = runtime.states();
        if operation == "remove" {
            states[0].pause_trading().await.unwrap();
        }
        let mut added = db
            .load_config()
            .unwrap()
            .unwrap()
            .strategy_configs()
            .unwrap()
            .remove(0);
        added.exchange.symbol = "ETHUSDT".into();
        let abort = runtime.pairs[1].engine.abort_handle();
        let handle = runtime.handle();
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        db.set_membership_commit_hook(
            if operation == "add" {
                "add_after"
            } else {
                "remove_after"
            },
            reached_tx,
            resume_rx,
        );
        let manager = tokio::spawn(async move {
            let reason = runtime.run().await;
            (reason, runtime)
        });
        let command_handle = handle.clone();
        let command = tokio::spawn(async move {
            if operation == "add" {
                command_handle.add_pair(added).await
            } else {
                command_handle.remove_pair("SOLUSDC".into()).await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), reached_rx)
            .await
            .unwrap()
            .unwrap();
        if operation == "remove" {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), observed_rx.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                "SOLUSDC"
            );
        }
        handle.request_shutdown();
        handle.start_early_shutdown(Duration::from_millis(50)).await;
        // Pause persistence shares this deadline, so abort may precede delivery of
        // the shutdown command. Termination must not depend on observing that message.
        tokio::time::timeout(Duration::from_secs(5), async {
            while !abort.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("unresponsive engine was not aborted while membership commit was held");
        assert!(!command.is_finished());
        assert!(!manager.is_finished());
        resume_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), command)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let (reason, mut runtime) = tokio::time::timeout(Duration::from_secs(10), manager)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reason, "Runtime shutdown requested");
        let error = tokio::time::timeout(
            Duration::from_secs(10),
            runtime.shutdown(Duration::from_secs(5)),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(error.to_string().contains("BTCUSDT"), "{error}");
        assert_eq!(*states[1].status.read().await, BotStatus::Paused);
        drop(runtime);
        drop(handle);
        drop(states);
        drop(db);
        server.abort();
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[tokio::test]
async fn shutdown_stops_all_engines_while_removal_waits_for_membership_lock() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut runtime = fixture_with_shutdown_observer(db.clone(), false, false, Some(observed_tx));
    let handle = runtime.handle();
    let membership = handle.membership_lock.lock().await;
    let (waiting_tx, waiting_rx) = oneshot::channel();
    let removal = tokio::spawn(async move {
        let result = {
            let removal = runtime.remove_pair("SOLUSDC");
            tokio::pin!(removal);
            tokio::select! {
                biased;
                result = &mut removal => panic!("removal unexpectedly completed: {result:?}"),
                _ = tokio::task::yield_now() => {}
            }
            waiting_tx.send(()).unwrap();
            removal.await
        };
        (result, runtime)
    });
    tokio::time::timeout(Duration::from_secs(5), waiting_rx)
        .await
        .expect("removal did not reach the membership lock")
        .unwrap();

    handle.request_shutdown();
    handle.start_early_shutdown(Duration::from_secs(30)).await;
    let mut observed = Vec::new();
    for _ in 0..2 {
        observed.push(
            tokio::time::timeout(Duration::from_secs(5), observed_rx.recv())
                .await
                .expect("engine did not begin shutdown while membership was locked")
                .unwrap(),
        );
    }
    observed.sort();
    assert_eq!(observed, vec!["BTCUSDT", "SOLUSDC"]);
    assert!(!removal.is_finished());
    assert!(observed_rx.try_recv().is_err());

    drop(membership);
    let (result, mut runtime) = tokio::time::timeout(Duration::from_secs(5), removal)
        .await
        .expect("removal did not finish after unlocking membership")
        .unwrap();
    assert_eq!(
        result.unwrap_err().to_string(),
        "程序正在退出，无法修改交易对列表"
    );
    runtime.shutdown(Duration::from_secs(5)).await.unwrap();
    assert!(runtime.pairs.iter().all(|pair| pair.engine.is_finished()));
    assert!(observed_rx.try_recv().is_err());
    let symbols: Vec<_> = db
        .load_config()
        .unwrap()
        .unwrap()
        .pairs
        .into_iter()
        .map(|pair| pair.symbol)
        .collect();
    assert_eq!(symbols, vec!["SOLUSDC", "BTCUSDT"]);
}

#[tokio::test]
async fn early_shutdown_cleans_existing_engines_during_membership_commit() {
    for operation in ["add", "remove"] {
        let directory = std::env::temp_dir().join(format!(
            "runtime-early-{operation}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("bot.db");
        let db = Arc::new(Database::open(&path).unwrap());
        let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
        let mut runtime =
            fixture_with_shutdown_observer(db.clone(), false, false, Some(observed_tx));
        let (url, server) =
            mock_exchange(StatusCode::OK, exchange_info("TRADING", valid_filters())).await;
        runtime.test_exchange_url = Some(url);
        let states = runtime.states();
        if operation == "remove" {
            states[0].pause_trading().await.unwrap();
        }
        let mut added = db
            .load_config()
            .unwrap()
            .unwrap()
            .strategy_configs()
            .unwrap()
            .remove(0);
        added.exchange.symbol = "ETHUSDT".into();
        let handle = runtime.handle();
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        db.set_membership_commit_hook(
            if operation == "add" {
                "add_after"
            } else {
                "remove_after"
            },
            reached_tx,
            resume_rx,
        );
        let manager = tokio::spawn(async move {
            let reason = runtime.run().await;
            (reason, runtime)
        });
        let command_handle = handle.clone();
        let command = tokio::spawn(async move {
            if operation == "add" {
                command_handle.add_pair(added).await
            } else {
                command_handle.remove_pair("SOLUSDC".into()).await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), reached_rx)
            .await
            .unwrap()
            .unwrap();
        if operation == "remove" {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), observed_rx.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                "SOLUSDC"
            );
        }
        handle.request_shutdown();
        handle.start_early_shutdown(Duration::from_secs(30)).await;
        let count = if operation == "add" { 2 } else { 1 };
        let mut observed = Vec::new();
        for _ in 0..count {
            observed.push(
                tokio::time::timeout(Duration::from_secs(5), observed_rx.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        observed.sort();
        assert_eq!(
            observed,
            if operation == "add" {
                vec!["BTCUSDT", "SOLUSDC"]
            } else {
                vec!["BTCUSDT"]
            }
        );
        assert!(observed_rx.try_recv().is_err());
        assert!(!command.is_finished());
        assert!(!manager.is_finished());
        if operation == "remove" {
            let early = handle.early_stops.lock().await;
            assert!(early
                .iter()
                .all(|(state, _)| state.pair_scope() != Some("SOLUSDC")));
        }
        resume_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), command)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let (reason, mut runtime) = tokio::time::timeout(Duration::from_secs(10), manager)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reason, "Runtime shutdown requested");
        let added_state = runtime
            .states()
            .into_iter()
            .find(|state| state.pair_scope() == Some("ETHUSDT"));
        if operation == "add" {
            assert!(added_state.as_ref().unwrap().is_shutting_down());
        }
        runtime.shutdown(Duration::from_secs(10)).await.unwrap();
        if operation == "add" {
            assert!(runtime
                .pairs
                .iter()
                .find(|pair| pair.state.pair_scope() == Some("ETHUSDT"))
                .unwrap()
                .engine
                .is_finished());
            let connection = rusqlite::Connection::open(&path).unwrap();
            let dirty: i64 = connection.query_row(
                "SELECT dirty FROM pair_execution_guard WHERE symbol = 'ETHUSDT' AND mode = 'PAPER'",
                [], |row| row.get(0)
            ).unwrap();
            assert_eq!(dirty, 0);
        }
        assert!(observed_rx.try_recv().is_err());
        drop(added_state);
        drop(runtime);
        drop(handle);
        drop(states);
        drop(db);
        server.abort();
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[tokio::test]
async fn shutdown_during_membership_commit_finishes_command_and_preserves_restart() {
    for operation in ["add", "remove"] {
        let directory = std::env::temp_dir().join(format!(
            "runtime-shutdown-{operation}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("bot.db");
        let db = Arc::new(Database::open(&path).unwrap());
        let mut runtime = fixture_with_database(db.clone(), false, false);
        let (url, server) =
            mock_exchange(StatusCode::OK, exchange_info("TRADING", valid_filters())).await;
        runtime.test_exchange_url = Some(url.clone());
        let states = runtime.states();
        if operation == "remove" {
            states[0].pause_trading().await.unwrap();
        }
        assert!(states[1].try_order_submission().is_some());
        let mut added_config = db
            .load_config()
            .unwrap()
            .unwrap()
            .strategy_configs()
            .unwrap()
            .remove(0);
        added_config.exchange.symbol = "ETHUSDT".into();
        let handle = runtime.handle();
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        db.set_membership_commit_hook(operation, reached_tx, resume_rx);
        let manager = tokio::spawn(async move {
            let reason = runtime.run().await;
            (reason, runtime)
        });
        let command_handle = handle.clone();
        let command = tokio::spawn(async move {
            if operation == "add" {
                command_handle.add_pair(added_config).await
            } else {
                command_handle.remove_pair("SOLUSDC".into()).await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), reached_rx)
            .await
            .expect("membership commit was not reached")
            .expect("membership commit hook dropped");
        assert!(!command.is_finished());
        assert!(!manager.is_finished());

        handle.request_shutdown();
        for state in &states {
            assert!(state.is_shutting_down());
            assert!(state.try_order_submission().is_none());
        }
        assert!(!command.is_finished());
        assert!(!manager.is_finished());
        let (queued_reply, queued_result) = oneshot::channel();
        assert!(handle
            .commands
            .try_send(RuntimeCommand::Remove {
                symbol: "BTCUSDT".into(),
                reply: queued_reply,
            })
            .is_ok());
        resume_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), command)
            .await
            .expect("membership command timed out")
            .unwrap()
            .unwrap();
        let (reason, mut runtime) = tokio::time::timeout(Duration::from_secs(10), manager)
            .await
            .expect("runtime did not stop after command")
            .unwrap();
        assert_eq!(reason, "Runtime shutdown requested");
        let expected: Vec<&str> = if operation == "add" {
            vec!["SOLUSDC", "BTCUSDT", "ETHUSDT"]
        } else {
            vec!["BTCUSDT"]
        };
        assert_eq!(
            runtime
                .states()
                .iter()
                .map(|state| state.pair_scope().unwrap().to_string())
                .collect::<Vec<_>>(),
            expected
        );
        for state in runtime.states() {
            assert!(state.is_shutting_down());
            assert!(state.try_order_submission().is_none());
        }
        runtime.shutdown(Duration::from_secs(10)).await.unwrap();
        assert!(queued_result.await.unwrap().is_err());
        drop(runtime);
        drop(handle);
        drop(states);
        drop(db);

        let reopened = Arc::new(Database::open(&path).unwrap());
        let saved = reopened.load_config().unwrap().unwrap();
        assert_eq!(
            saved
                .pairs
                .iter()
                .map(|pair| pair.symbol.as_str())
                .collect::<Vec<_>>(),
            expected
        );
        let mut restarted = TradingRuntime::start_with_test_exchange(&saved, reopened.clone(), url)
            .await
            .unwrap();
        assert_eq!(
            restarted
                .states()
                .iter()
                .map(|state| state.pair_scope().unwrap().to_string())
                .collect::<Vec<_>>(),
            expected
        );
        restarted.shutdown(Duration::from_secs(10)).await.unwrap();
        drop(restarted);
        drop(reopened);
        server.abort();
        std::fs::remove_dir_all(directory).unwrap();
    }
}
#[tokio::test]
async fn shutdown_during_failed_removal_reports_error_and_preserves_pair_on_restart() {
    let directory = std::env::temp_dir().join(format!(
        "runtime-shutdown-remove-failure-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("bot.db");
    let db = Arc::new(Database::open(&path).unwrap());
    let mut runtime = fixture_with_database(db.clone(), false, false);
    let (url, server) =
        mock_exchange(StatusCode::OK, exchange_info("TRADING", valid_filters())).await;
    runtime.test_exchange_url = Some(url.clone());
    let states = runtime.states();
    states[0].pause_trading().await.unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_shutdown_remove BEFORE UPDATE ON app_config BEGIN SELECT RAISE(ABORT, 'shutdown removal failed'); END;").unwrap();
    drop(connection);
    let handle = runtime.handle();
    let (reached_tx, reached_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    db.set_membership_commit_hook("remove", reached_tx, resume_rx);
    let manager = tokio::spawn(async move {
        let reason = runtime.run().await;
        (reason, runtime)
    });
    let command_handle = handle.clone();
    let command = tokio::spawn(async move { command_handle.remove_pair("SOLUSDC".into()).await });
    tokio::time::timeout(Duration::from_secs(5), reached_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(states[1].try_order_submission().is_some());
    handle.request_shutdown();
    assert!(states[1].try_order_submission().is_none());
    assert!(!command.is_finished());
    assert!(!manager.is_finished());
    resume_tx.send(()).unwrap();
    let error = tokio::time::timeout(Duration::from_secs(10), command)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Failed to save config to SQLite database"),
        "{error:#}"
    );
    let (reason, mut runtime) = tokio::time::timeout(Duration::from_secs(10), manager)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reason, "Runtime shutdown requested");
    assert_eq!(runtime.states().len(), 2);
    for state in runtime.states() {
        assert!(state.is_shutting_down());
        assert!(state.try_order_submission().is_none());
    }
    runtime.shutdown(Duration::from_secs(10)).await.unwrap();
    drop(runtime);
    drop(handle);
    drop(states);
    drop(db);
    let reopened = Arc::new(Database::open(&path).unwrap());
    let saved = reopened.load_config().unwrap().unwrap();
    assert_eq!(
        saved
            .pairs
            .iter()
            .map(|pair| pair.symbol.as_str())
            .collect::<Vec<_>>(),
        vec!["SOLUSDC", "BTCUSDT"]
    );
    let mut restarted = TradingRuntime::start_with_test_exchange(&saved, reopened.clone(), url)
        .await
        .unwrap();
    assert_eq!(
        restarted
            .states()
            .iter()
            .map(|state| state.pair_scope().unwrap().to_string())
            .collect::<Vec<_>>(),
        vec!["SOLUSDC", "BTCUSDT"]
    );
    restarted.shutdown(Duration::from_secs(10)).await.unwrap();
    drop(restarted);
    drop(reopened);
    server.abort();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn adding_pair_rolls_back_when_guard_write_fails_and_can_be_retried() {
    let path = std::env::temp_dir().join(format!("add-guard-failure-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    let mut runtime = fixture_with_database(db.clone(), false, false);
    let (url, server) =
        mock_exchange(StatusCode::OK, exchange_info("TRADING", valid_filters())).await;
    runtime.test_exchange_url = Some(url);
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_new_guard BEFORE INSERT ON pair_execution_guard WHEN NEW.symbol = 'ETHUSDT' BEGIN SELECT RAISE(ABORT, 'guard write failed'); END;").unwrap();
    let manager = tokio::spawn(async move { runtime.run().await });

    let (_, response) = request(&router, "/api/pairs", "POST", payload("ETHUSDT"), true).await;
    assert_eq!(response["success"], false, "{response}");
    assert!(!db
        .load_config()
        .unwrap()
        .unwrap()
        .pairs
        .iter()
        .any(|pair| pair.symbol == "ETHUSDT"));
    assert_eq!(
        db.load_pair_status("ETHUSDT", TradingMode::Paper).unwrap(),
        None
    );
    assert_eq!(handle.states.read().await.len(), 2);
    connection
        .execute_batch("DROP TRIGGER reject_new_guard;")
        .unwrap();
    let (_, response) = request(&router, "/api/pairs", "POST", payload("ETHUSDT"), true).await;
    assert_eq!(response["success"], true, "{response}");
    assert!(db
        .load_config()
        .unwrap()
        .unwrap()
        .pairs
        .iter()
        .any(|pair| pair.symbol == "ETHUSDT"));
    assert_eq!(
        db.load_pair_status("ETHUSDT", TradingMode::Paper).unwrap(),
        Some(BotStatus::Paused)
    );
    assert_eq!(handle.states.read().await.len(), 3);
    manager.abort();
    server.abort();
    drop(connection);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn adding_pairs_requires_valid_exchange_rules_before_saving() {
    let valid = valid_filters();
    let mut no_price = valid.clone();
    no_price.as_array_mut().unwrap().remove(0);
    let mut no_lot = valid.clone();
    no_lot.as_array_mut().unwrap().remove(1);
    let mut zero_tick = valid.clone();
    zero_tick[0]["tickSize"] = json!("0");
    let mut negative_tick = valid.clone();
    negative_tick[0]["tickSize"] = json!("-0.01");
    let mut zero_step = valid.clone();
    zero_step[1]["stepSize"] = json!("0");
    let mut negative_step = valid.clone();
    negative_step[1]["stepSize"] = json!("-0.001");
    let mut mismatched_tick = valid.clone();
    mismatched_tick[0]["tickSize"] = json!("0.03");

    for (name, status, body) in [
        ("missing symbol", StatusCode::OK, json!({"symbols": []})),
        (
            "wrong symbol",
            StatusCode::OK,
            json!({"symbols": [{"symbol":"OTHER", "status":"TRADING", "pricePrecision":2, "quantityPrecision":3, "filters":valid.clone()}]}),
        ),
        (
            "not trading",
            StatusCode::OK,
            exchange_info("BREAK", valid.clone()),
        ),
        (
            "missing price filter",
            StatusCode::OK,
            exchange_info("TRADING", no_price),
        ),
        (
            "missing lot filter",
            StatusCode::OK,
            exchange_info("TRADING", no_lot),
        ),
        (
            "zero tick",
            StatusCode::OK,
            exchange_info("TRADING", zero_tick),
        ),
        (
            "negative tick",
            StatusCode::OK,
            exchange_info("TRADING", negative_tick),
        ),
        (
            "zero step",
            StatusCode::OK,
            exchange_info("TRADING", zero_step),
        ),
        (
            "negative step",
            StatusCode::OK,
            exchange_info("TRADING", negative_step),
        ),
        (
            "grid mismatch",
            StatusCode::OK,
            exchange_info("TRADING", mismatched_tick),
        ),
        (
            "invalid response",
            StatusCode::OK,
            json!({"symbols": "invalid"}),
        ),
        (
            "API error",
            StatusCode::BAD_REQUEST,
            json!({"code":-1121,"msg":"Invalid symbol."}),
        ),
        (
            "other API error",
            StatusCode::BAD_REQUEST,
            json!({"code":-1000,"msg":"Internal error"}),
        ),
    ] {
        let (url, server) = mock_exchange(status, body).await;
        let mut runtime = fixture();
        runtime.test_exchange_url = Some(url);
        let handle = runtime.handle();
        let router = create_managed_router(handle.clone()).await;
        let manager = tokio::spawn(async move { runtime.run().await });
        let (_, response) = request(&router, "/api/pairs", "POST", payload("ETHUSDT"), true).await;
        assert_eq!(response["success"], false, "{name}: {response}");
        if name == "API error" {
            assert!(response["message"]
                .as_str()
                .unwrap()
                .contains("交易对 ETHUSDT 不存在"));
        } else if name == "other API error" || name == "invalid response" {
            assert!(response["message"]
                .as_str()
                .unwrap()
                .contains("暂时无法验证交易对"));
        }
        let saved = handle.db.load_config().unwrap().unwrap();
        assert_eq!(saved.pairs.len(), 2, "{name}");
        assert!(
            !saved.pairs.iter().any(|pair| pair.symbol == "ETHUSDT"),
            "{name}"
        );
        assert_eq!(handle.states.read().await.len(), 2, "{name}");
        manager.abort();
        server.abort();
    }
}

#[tokio::test]
async fn adding_pair_rejects_network_failure_without_saving() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let mut runtime = fixture();
    runtime.test_exchange_url = Some(url);
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    let (_, response) = request(&router, "/api/pairs", "POST", payload("ETHUSDT"), true).await;
    assert_eq!(response["success"], false);
    assert!(response["message"]
        .as_str()
        .unwrap()
        .contains("暂时无法验证交易对"));
    let saved = handle.db.load_config().unwrap().unwrap();
    assert_eq!(saved.pairs.len(), 2);
    assert!(!saved.pairs.iter().any(|pair| pair.symbol == "ETHUSDT"));
    assert_eq!(handle.states.read().await.len(), 2);
    manager.abort();
}

#[tokio::test]
async fn shutdown_stops_all_pairs_before_waiting_and_cancels_them_concurrently() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let config = AppConfig {
        pairs: ["SOLUSDC", "BTCUSDT"]
            .map(|s| TradingPairConfig::new(s.into()))
            .to_vec(),
        ..Default::default()
    };
    db.save_config(&config).unwrap();
    let commit = Arc::new(Mutex::new(()));
    let mut receivers = Vec::new();
    let states: Vec<_> = config
        .strategy_configs()
        .unwrap()
        .into_iter()
        .map(|config| {
            let (tx, rx) = mpsc::channel(8);
            receivers.push(rx);
            AppState::new_for_pair(config, db.clone(), tx, commit.clone())
        })
        .collect();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let pairs = states
        .iter()
        .zip(receivers)
        .map(|(state, mut rx)| {
            let (shutdown_tx, _) = watch::channel(false);
            let all_states = states.clone();
            let barrier = barrier.clone();
            let engine = tokio::spawn(async move {
                let BotControlAction::Shutdown { reply, .. } = rx.recv().await.unwrap() else {
                    panic!("Expected shutdown");
                };
                assert!(all_states.iter().all(|state| state.is_shutting_down()));
                // Serial cleanup would time out here because neither engine can finish alone.
                barrier.wait().await;
                reply.send(Ok(())).unwrap();
            });
            PairTask {
                state: state.clone(),
                engine,
                engine_joined: false,
                recovery_failed: false,
                shutdown_tx,
                background: vec![],
            }
        })
        .collect();
    let mut runtime = TradingRuntime::from_pairs(
        pairs,
        db.clone(),
        commit,
        config.server.clone(),
        GlobalConfig::from_app(&config),
        None,
    );
    runtime.shutdown(Duration::from_secs(2)).await.unwrap();
    for state in states {
        let symbol = state.config.read().await.exchange.symbol.clone();
        assert_eq!(
            db.load_pair_status(&symbol, TradingMode::Paper).unwrap(),
            Some(BotStatus::Paused)
        );
        assert!(state.resume_trading().await.is_err());
    }
}

#[tokio::test]
async fn add_pair_rolls_back_config_and_guard_on_prepare_failure_or_fails_closed() {
    struct TempDir(std::path::PathBuf);
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let dir = TempDir(
        std::env::temp_dir().join(format!("runtime-add-rollback-{}", uuid::Uuid::new_v4())),
    );
    std::fs::create_dir(&dir.0).unwrap();
    let path = dir.0.join("bot.db");
    let db = Arc::new(Database::open(&path).unwrap());
    let mut runtime = fixture_with_database(db.clone(), false, false);
    runtime
        .global_position_limit
        .membership_ready
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (url, server) =
        mock_exchange(StatusCode::OK, exchange_info("TRADING", valid_filters())).await;
    runtime.test_exchange_url = Some(url);

    let mut added = db
        .load_config()
        .unwrap()
        .unwrap()
        .strategy_configs()
        .unwrap()
        .remove(0);
    added.exchange.symbol = "ETHUSDT".into();

    runtime
        .fail_next_prepare_pair
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(runtime.add_pair(added.clone()).await.is_err());
    assert!(runtime
        .global_position_limit
        .membership_ready
        .load(std::sync::atomic::Ordering::SeqCst));
    let symbols: Vec<_> = db
        .load_config()
        .unwrap()
        .unwrap()
        .pairs
        .into_iter()
        .map(|p| p.symbol)
        .collect();
    assert_eq!(symbols, vec!["SOLUSDC", "BTCUSDT"]);
    let external = rusqlite::Connection::open(&path).unwrap();
    let dirty: i64 = external
        .query_row(
            "SELECT dirty FROM pair_execution_guard WHERE symbol = 'ETHUSDT' AND mode = 'PAPER'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(dirty, 0);

    // If rollback itself fails, membership_ready must stay false.
    let (reached_tx, reached_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    db.set_membership_commit_hook("add_after", reached_tx, resume_rx);
    runtime
        .fail_next_prepare_pair
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let trigger_path = path.clone();
    let blocker = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(reached_rx)
            .unwrap();
        let conn = rusqlite::Connection::open(&trigger_path).unwrap();
        conn.execute_batch(
            "CREATE TRIGGER fail_rollback_config BEFORE UPDATE ON app_config
             BEGIN SELECT RAISE(ABORT, 'rollback failed'); END;",
        )
        .unwrap();
        drop(conn);
        resume_tx.send(()).unwrap();
    });
    assert!(runtime.add_pair(added).await.is_err());
    blocker.join().unwrap();
    assert!(!runtime
        .global_position_limit
        .membership_ready
        .load(std::sync::atomic::Ordering::SeqCst));
    external
        .execute_batch("DROP TRIGGER fail_rollback_config;")
        .unwrap();
    drop(external);
    runtime.shutdown(Duration::from_secs(5)).await.unwrap();
    server.abort();
}
