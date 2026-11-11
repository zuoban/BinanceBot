use super::super::*;
use super::{
    exchange_info, fixture, fixture_with_database, mock_exchange, payload, request, valid_filters,
};
use crate::db::Database;
use crate::server::create_managed_router;
use crate::types::{BotStatus, TradingMode};
use axum::http::StatusCode;
use serde_json::json;

#[tokio::test]
async fn adding_pair_rolls_back_when_guard_write_fails_and_can_be_retried() {
    let dir = crate::test_support::TempTestDir::new("add-guard-failure");
    let path = dir.join("bot.db");
    let db = Arc::new(Database::open(&path).unwrap());
    let mut runtime = fixture_with_database(db.clone(), false, false);
    let (url, server) =
        mock_exchange(StatusCode::OK, exchange_info("TRADING", valid_filters())).await;
    runtime.test_exchange_url = Some(url);
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone());
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
    let _ = manager.await;
    server.abort();
    let _ = server.await;
    drop(router);
    drop(handle);
    drop(connection);
    drop(db);
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
        let router = create_managed_router(handle.clone());
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
        let _ = manager.await;
        server.abort();
        let _ = server.await;
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
    let router = create_managed_router(handle.clone());
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
    let _ = manager.await;
}

#[tokio::test]
async fn add_pair_rolls_back_config_and_guard_on_prepare_failure_or_fails_closed() {
    let dir = crate::test_support::TempTestDir::new("runtime-add-rollback");
    let path = dir.join("bot.db");
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
    drop(runtime);
    drop(db);
    server.abort();
    let _ = server.await;
}
