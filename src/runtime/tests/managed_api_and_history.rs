use super::super::*;
use super::{
    exchange_info, fixture, mock_exchange, payload, request, valid_filters, versioned_payload,
};
use crate::server::create_managed_router;
use crate::types::{BotStatus, TradingMode};
use axum::http::StatusCode;
use serde_json::{json, Value};

#[tokio::test]
async fn symbols_route_shares_runtime_limiter() {
    use crate::exchange::rate_limit::RequestClass;

    let runtime = fixture();
    let handle = runtime.handle();
    let mainnet = "https://fapi.binance.com";
    let config = AppConfig::default().exchange;
    let trading_client = runtime.exchange_client(&config);
    assert!(Arc::ptr_eq(&trading_client.limiter(), &handle.rate_limiter));
    handle
        .rate_limiter
        .reserve(mainnet, [7; 32], 1800, RequestClass::Normal)
        .unwrap();
    let router = create_managed_router(handle.clone()).await;
    let (status, _) = request(
        &router,
        "/api/exchange/symbols?environment=mainnet",
        "GET",
        Value::Null,
        true,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn management_authenticates_add_remove_server_settings_and_pair_resources() {
    let runtime = fixture();
    let router = create_managed_router(runtime.handle()).await;
    for (path, method, body) in [
        ("/api/overview", "GET", Value::Null),
        ("/api/overview/history", "GET", Value::Null),
        ("/api/pairs", "POST", payload("ETHUSDT")),
        ("/api/pairs/SOLUSDC", "DELETE", json!({"confirm":true})),
        ("/api/server/config", "GET", Value::Null),
        (
            "/api/server/config",
            "POST",
            json!({"host":"127.0.0.1","port":9090}),
        ),
        ("/api/pairs/BTCUSDT/config", "GET", Value::Null),
        ("/api/global/config", "GET", Value::Null),
        ("/api/global/config", "POST", json!({"recv_window":8000})),
        ("/api/global/test-exchange", "POST", json!({})),
        ("/api/global/test-telegram", "POST", json!({})),
    ] {
        assert_eq!(
            request(&router, path, method, body, false).await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        runtime
            .handle
            .db
            .load_config()
            .unwrap()
            .unwrap()
            .pairs
            .len(),
        2
    );
}

#[tokio::test]
async fn overview_history_returns_saved_snapshots() {
    let runtime = fixture();
    let hour = chrono::Utc::now().timestamp().div_euclid(3600) * 3600;
    let group = crate::snapshots::SnapshotGroup {
        mode: TradingMode::Paper,
        asset: "USDC".into(),
        account_id: None,
        equity: Some(rust_decimal_macros::dec!(100)),
        long: Some(rust_decimal_macros::dec!(20)),
        short: Some(rust_decimal_macros::dec!(0)),
    };
    for saved_hour in [hour - 8 * 24 * 3600, hour - 6 * 24 * 3600 - 12 * 3600, hour] {
        runtime
            .handle
            .db
            .save_hourly_account_snapshot(saved_hour, std::slice::from_ref(&group))
            .unwrap();
    }
    let router = create_managed_router(runtime.handle()).await;
    let (status, body) = request(&router, "/api/overview/history", "GET", Value::Null, true).await;
    assert_eq!(status, StatusCode::OK);
    let snapshots = body["data"]["snapshots"].as_array().unwrap();
    assert_eq!(snapshots.len(), 2);
    assert_eq!(snapshots[0]["hour"], hour - 6 * 24 * 3600 - 12 * 3600);
    assert_eq!(snapshots[1]["hour"], hour);
    assert_eq!(snapshots[0]["groups"][0]["asset"], "USDC");
    let current = body["data"]["current_accounts"].as_array().unwrap();
    assert!(current.iter().any(|account| account["asset"] == "USDC"
        && account["mode"] == "PAPER"
        && account["account_id"] == "paper-shared-v1"));
    assert!(current
        .iter()
        .any(|account| account["asset"] == "USDT" && account["mode"] == "PAPER"));
}

#[tokio::test]
async fn history_waits_for_global_commit_and_lists_live_account_without_snapshots() {
    let runtime = fixture();
    let handle = runtime.handle();
    let guard = handle.membership_lock.lock().await;
    for state in runtime.states() {
        let mut config = state.config.write().await;
        config.exchange.api_key = "history-new-key".into();
        config.exchange.api_secret = "history-new-secret".into();
    }
    let mut committed = handle.db.load_config().unwrap().unwrap();
    committed.exchange.api_key = "history-new-key".into();
    committed.exchange.api_secret = "history-new-secret".into();
    handle.db.save_config(&committed).unwrap();
    let router = create_managed_router(handle.clone()).await;
    let mut request_task = tokio::spawn(async move {
        request(&router, "/api/overview/history", "GET", Value::Null, true).await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut request_task)
            .await
            .is_err()
    );
    drop(guard);
    let (_, body) = request_task.await.unwrap();
    assert!(body["data"]["snapshots"].as_array().unwrap().is_empty());
    let accounts = body["data"]["current_accounts"].as_array().unwrap();
    assert_eq!(accounts.len(), 2);
    let id = accounts[0]["account_id"].as_str().unwrap();
    assert!(uuid::Uuid::parse_str(id).is_ok());
    assert_eq!(accounts[1]["account_id"], id);
    let serialized = body.to_string();
    assert!(!serialized.contains("history-new-key"));
    assert!(!serialized.contains("history-new-secret"));
}

#[tokio::test]
async fn history_lists_current_account_for_legacy_single_pair_runtime() {
    let runtime = fixture();
    let handle = runtime.handle();
    let mut config = AppConfig::default();
    config.exchange.symbol = "BTCUSDC".into();
    handle.db.save_config(&config).unwrap();
    let (sender, _receiver) = mpsc::channel(1);
    let state = AppState::new(config, handle.db.clone(), sender);
    *handle.states.write().await = vec![state];

    let accounts = crate::snapshots::current_accounts(&handle).await.unwrap();
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].mode, TradingMode::Paper);
    assert_eq!(accounts[0].asset, "USDC");
    assert_eq!(accounts[0].account_id.as_deref(), Some("paper-shared-v1"));
    let captured = crate::snapshots::collect_hourly(&handle)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(captured[0].2.as_deref(), Some("paper-shared-v1"));
}

#[tokio::test]
async fn history_api_ignores_pair_update_lock_and_hourly_capture_skips_it() {
    let runtime = fixture();
    let handle = runtime.handle();
    let state = runtime.states()[0].clone();
    {
        let global = handle.membership_lock.lock().await;
        assert!(crate::snapshots::collect_hourly(&handle)
            .await
            .unwrap()
            .is_none());
        drop(global);
    }
    let guard = state.config_update_lock.lock().await;
    assert!(crate::snapshots::collect_hourly(&handle)
        .await
        .unwrap()
        .is_none());
    let router = create_managed_router(handle).await;
    let (_, body) = tokio::time::timeout(
        Duration::from_secs(1),
        request(&router, "/api/overview/history", "GET", Value::Null, true),
    )
    .await
    .unwrap();
    assert_eq!(
        body["data"]["current_accounts"].as_array().unwrap().len(),
        2
    );
    drop(guard);
}

#[tokio::test]
async fn overview_reads_current_managed_membership_without_restarting_router() {
    let mut runtime = fixture();
    let router = create_managed_router(runtime.handle()).await;
    let (status, response) = request(&router, "/api/overview", "GET", Value::Null, true).await;
    assert_eq!(status, StatusCode::OK);
    let pairs = response["data"]["pairs"].as_array().unwrap();
    assert_eq!(pairs.len(), 2);
    assert_eq!(pairs[0]["snapshot"]["symbol"], "SOLUSDC");
    assert_eq!(pairs[1]["snapshot"]["symbol"], "BTCUSDT");

    runtime.states()[0].pause_trading().await.unwrap();
    runtime.remove_pair("SOLUSDC").await.unwrap();
    let (status, response) = request(&router, "/api/overview", "GET", Value::Null, true).await;
    assert_eq!(status, StatusCode::OK);
    let pairs = response["data"]["pairs"].as_array().unwrap();
    assert_eq!(pairs.len(), 1);
    assert_eq!(pairs[0]["snapshot"]["symbol"], "BTCUSDT");
}

#[tokio::test]
async fn dashboard_membership_commands_update_routes_without_restarting_and_preserve_pause() {
    let (url, server) =
        mock_exchange(StatusCode::OK, exchange_info("TRADING", valid_filters())).await;
    let mut runtime = fixture();
    runtime.test_exchange_url = Some(url);
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    let (_, added) = request(&router, "/api/pairs", "POST", payload("ETHUSDT"), true).await;
    assert_eq!(added["success"], true);
    let (_, list) = request(&router, "/api/pairs", "GET", Value::Null, true).await;
    assert_eq!(list["data"].as_array().unwrap().len(), 3);
    let (_, added_config) = request(
        &router,
        "/api/pairs/ETHUSDT/config",
        "GET",
        Value::Null,
        true,
    )
    .await;
    assert_eq!(added_config["data"]["recv_window"], 5000);
    assert_eq!(added_config["data"]["sync_interval_secs"], 3);
    assert_eq!(added_config["data"]["has_api_key"], false);
    assert_eq!(
        handle
            .db
            .load_pair_status("ETHUSDT", TradingMode::Paper)
            .unwrap(),
        Some(BotStatus::Paused)
    );
    let (_, duplicate) = request(&router, "/api/pairs", "POST", payload("ETHUSDT"), true).await;
    assert_eq!(duplicate["success"], false);
    let (_, running_remove) = request(
        &router,
        "/api/pairs/SOLUSDC",
        "DELETE",
        json!({"confirm":true}),
        true,
    )
    .await;
    assert_eq!(running_remove["success"], false);
    request(
        &router,
        "/api/pairs/SOLUSDC/control",
        "POST",
        json!({"action":"pause"}),
        true,
    )
    .await;
    let (_, removed) = request(
        &router,
        "/api/pairs/SOLUSDC",
        "DELETE",
        json!({"confirm":true}),
        true,
    )
    .await;
    assert_eq!(removed["success"], true);
    assert_eq!(
        request(
            &router,
            "/api/pairs/SOLUSDC/status",
            "GET",
            Value::Null,
            true
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let (_, status) = request(&router, "/api/status", "GET", Value::Null, true).await;
    assert_eq!(status["data"]["symbol"], "BTCUSDT");
    assert_eq!(
        handle
            .db
            .load_config()
            .unwrap()
            .unwrap()
            .pairs
            .iter()
            .map(|pair| pair.symbol.as_str())
            .collect::<Vec<_>>(),
        ["BTCUSDT", "ETHUSDT"]
    );
    manager.abort();
    let _ = manager.await;
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn settings_validation_and_server_changes_preserve_independent_pair_updates() {
    let mut runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    let (_, rejected) = request(
        &router,
        "/api/global/config",
        "POST",
        json!({"recv_window":0}),
        true,
    )
    .await;
    assert_eq!(rejected["success"], false);
    let mut update = json!({"recv_window":8000,"sync_interval_secs":7});
    update["api_key"] = json!("configuration-key");
    update["api_secret"] = json!("configuration-secret");
    update["telegram_bot_token"] = json!("configuration-bot-token");
    update["telegram_chat_id"] = json!("123456");
    let (_, applied) = request(&router, "/api/global/config", "POST", update, true).await;
    assert_eq!(applied["success"], true);
    let serialized = applied.to_string();
    for secret in [
        "configuration-key",
        "configuration-secret",
        "configuration-bot-token",
    ] {
        assert!(!serialized.contains(secret));
    }
    let mut clear = json!({});
    clear["clear_api_credentials"] = json!(true);
    clear["clear_telegram_credentials"] = json!(true);
    let (_, cleared) = request(&router, "/api/global/config", "POST", clear, true).await;
    assert_eq!(cleared["data"]["has_api_key"], false);
    assert_eq!(cleared["data"]["has_telegram_bot_token"], false);
    let (_, invalid_server) = request(
        &router,
        "/api/server/config",
        "POST",
        json!({"host":"bad-host","port":0}),
        true,
    )
    .await;
    assert_eq!(invalid_server["success"], false);
    let (_, server) = request(
        &router,
        "/api/server/config",
        "POST",
        json!({"host":"::1","port":9090}),
        true,
    )
    .await;
    assert_eq!(server["data"]["saved"]["port"], 9090);
    assert_eq!(server["data"]["active"]["port"], 8080);
    let (_, updated_btc) = request(
        &router,
        "/api/pairs/BTCUSDT/config",
        "POST",
        versioned_payload(&router, "BTCUSDT").await,
        true,
    )
    .await;
    assert_eq!(updated_btc["success"], true);
    let saved = handle.db.load_config().unwrap().unwrap();
    assert_eq!(saved.server.host, "::1");
    assert_eq!(saved.server.port, 9090);
    let configs = saved.strategy_configs().unwrap();
    assert!(configs.iter().all(
        |config| config.exchange.recv_window == 8000 && config.exchange.sync_interval_secs == 7
    ));
    assert!(configs[0].exchange.api_key.is_empty());
    manager.abort();
    let _ = manager.await;
}

#[tokio::test]
async fn managed_pair_updates_require_a_current_version() {
    let runtime = fixture();
    let router = create_managed_router(runtime.handle()).await;
    for path in ["/api/pairs/BTCUSDT/config", "/api/config"] {
        let symbol = if path == "/api/config" {
            "SOLUSDC"
        } else {
            "BTCUSDT"
        };
        for baseline in [None, Some(json!(null)), Some(json!("")), Some(json!("  "))] {
            let mut body = payload(symbol);
            if let Some(baseline) = baseline {
                body["baseline_version"] = baseline;
            }
            let (status, result) = request(&router, path, "POST", body, true).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(result["message"]
                .as_str()
                .unwrap()
                .contains("baseline_version"));
        }
        let mut stale = payload(symbol);
        stale["baseline_version"] = json!("stale");
        assert_eq!(
            request(&router, path, "POST", stale, true).await.0,
            StatusCode::CONFLICT
        );
    }
}

#[tokio::test]
async fn configuration_versions_reject_concurrent_stale_forms() {
    let mut runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    for (path, field, first, second) in [
        (
            "/api/global/config",
            "sync_interval_secs",
            json!(11),
            json!(12),
        ),
        ("/api/server/config", "port", json!(9001), json!(9002)),
    ] {
        let (_, initial) = request(&router, path, "GET", json!({}), true).await;
        let version = initial["data"]["version"].as_str().unwrap();
        let mut a = if field == "port" {
            json!({"host":"127.0.0.1"})
        } else {
            json!({})
        };
        let mut b = a.clone();
        a[field] = first;
        b[field] = second;
        a["baseline_version"] = json!(version);
        b["baseline_version"] = json!(version);
        let (a, b) = tokio::join!(
            request(&router, path, "POST", a, true),
            request(&router, path, "POST", b, true)
        );
        let ((ok_status, success), (conflict_status, conflict)) = if a.1["success"] == true {
            (a, b)
        } else {
            (b, a)
        };
        assert_eq!(ok_status, StatusCode::OK);
        assert_eq!(conflict_status, StatusCode::CONFLICT);
        assert_eq!(success["success"], true);
        assert_eq!(conflict["success"], false);
        assert!(conflict["message"].as_str().unwrap().contains("刷新"));
        for blank in ["", "   "] {
            let mut bad = if field == "port" {
                json!({"host":"127.0.0.1","port":9003})
            } else {
                json!({"sync_interval_secs":13})
            };
            bad["baseline_version"] = json!(blank);
            let (bad_status, bad_body) = request(&router, path, "POST", bad, true).await;
            assert_eq!(bad_status, StatusCode::BAD_REQUEST);
            assert_eq!(bad_body["message"], "配置版本不能为空");
        }
        let (_, current) = request(&router, path, "GET", json!({}), true).await;
        assert_eq!(success["data"]["version"], current["data"]["version"]);
        assert_ne!(success["data"]["version"], version);
    }
    manager.abort();
    let _ = manager.await;
}

#[tokio::test]
async fn managed_account_stream_tracks_current_pair_membership() {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let url = format!("ws://{address}/api/accounts/ws");
    assert!(tokio_tungstenite::connect_async(&url).await.is_err());
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        "auth, token.configuration-test-token".parse().unwrap(),
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let first: Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
    assert_eq!(first["data"]["pairs"].as_array().unwrap().len(), 2);
    handle.states.write().await.pop();
    let updated = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let updated: Value = serde_json::from_str(updated.to_text().unwrap()).unwrap();
    assert_eq!(updated["data"]["pairs"].as_array().unwrap().len(), 1);
    assert_eq!(updated["data"]["pairs"][0]["snapshot"]["symbol"], "SOLUSDC");
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn managed_websockets_accept_protocol_tokens_without_bearer_headers() {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
    let runtime = fixture();
    let router = create_managed_router(runtime.handle()).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let mut request = format!("ws://{address}/api/pairs/BTCUSDT/ws")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        "auth, token.configuration-test-token".parse().unwrap(),
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Message::Text(text) = message else {
        panic!("Expected snapshot");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&text).unwrap()["data"]["symbol"],
        "BTCUSDT"
    );
    assert!(
        tokio_tungstenite::connect_async(format!("ws://{address}/api/pairs/BTCUSDT/ws"))
            .await
            .is_err()
    );
    server.abort();
    let _ = server.await;
}
