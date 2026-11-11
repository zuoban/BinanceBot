use super::super::*;
use super::{
    exchange_info, fixture, fixture_with_database, fixture_with_failures, mock_exchange, payload,
    request, valid_filters, versioned_payload,
};
use crate::db::Database;
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

#[tokio::test]
async fn global_position_limit_api_persists_validates_and_supports_legacy_updates() {
    let mut runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    let path = "/api/global/config";

    let (_, initial) = request(&router, path, "GET", Value::Null, true).await;
    assert!(initial["data"]["global_max_position_notional"].is_null());
    let (_, updated) = request(
        &router,
        path,
        "POST",
        json!({"global_max_position_notional":"1234.50"}),
        true,
    )
    .await;
    assert_eq!(updated["success"], true);
    assert_eq!(updated["data"]["global_max_position_notional"], "1234.50");
    assert_eq!(
        handle
            .db
            .load_config()
            .unwrap()
            .unwrap()
            .global_max_position_notional
            .unwrap()
            .to_string(),
        "1234.50"
    );

    for value in ["0", "-1"] {
        let (_, rejected) = request(
            &router,
            path,
            "POST",
            json!({ "global_max_position_notional": value }),
            true,
        )
        .await;
        assert_eq!(rejected["success"], false);
        assert_eq!(
            handle
                .db
                .load_config()
                .unwrap()
                .unwrap()
                .global_max_position_notional
                .unwrap()
                .to_string(),
            "1234.50"
        );
    }
    let (_, legacy) = request(&router, path, "POST", json!({"recv_window":8000}), true).await;
    assert_eq!(legacy["data"]["global_max_position_notional"], "1234.50");
    let (_, cleared) = request(
        &router,
        path,
        "POST",
        json!({ "global_max_position_notional": null }),
        true,
    )
    .await;
    assert_eq!(cleared["success"], true);
    assert!(cleared["data"]["global_max_position_notional"].is_null());
    assert!(handle
        .db
        .load_config()
        .unwrap()
        .unwrap()
        .global_max_position_notional
        .is_none());
    manager.abort();
    let _ = manager.await;
}

#[tokio::test]
async fn zero_available_margin_pause_global_api_preserves_legacy_updates() {
    let mut runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    let path = "/api/global/config";

    let (_, initial) = request(&router, path, "GET", Value::Null, true).await;
    assert_eq!(initial["data"]["pause_on_zero_available_margin"], false);
    for (payload, expected) in [
        (json!({"pause_on_zero_available_margin": true}), true),
        (json!({"recv_window": 8000}), true),
        (json!({"pause_on_zero_available_margin": false}), false),
        (json!({"recv_window": 9000}), false),
    ] {
        let (_, updated) = request(&router, path, "POST", payload, true).await;
        assert_eq!(updated["success"], true);
        assert_eq!(updated["data"]["pause_on_zero_available_margin"], expected);
        let (_, fetched) = request(&router, path, "GET", Value::Null, true).await;
        assert_eq!(fetched["data"]["pause_on_zero_available_margin"], expected);
        assert_eq!(
            handle
                .db
                .load_config()
                .unwrap()
                .unwrap()
                .pause_on_zero_available_margin,
            expected
        );
    }
    manager.abort();
    let _ = manager.await;
}

#[tokio::test]
async fn global_settings_reach_every_engine_and_new_pairs_inherit_without_overrides() {
    let (url, server) =
        mock_exchange(StatusCode::OK, exchange_info("TRADING", valid_filters())).await;
    let mut runtime = fixture();
    runtime.test_exchange_url = Some(url);
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    let mut btc = versioned_payload(&router, "BTCUSDT").await;
    btc["grid_interval"] = json!("10");
    request(&router, "/api/pairs/BTCUSDT/config", "POST", btc, true).await;
    let (_, global) = request(
        &router,
        "/api/global/config",
        "POST",
        json!({
            "api_key":"shared-key", "api_secret":"shared-secret", "recv_window":9000,
            "sync_interval_secs":9, "telegram_enabled":true,
            "telegram_bot_token":"shared-token", "telegram_chat_id":"123456"
        }),
        true,
    )
    .await;
    assert_eq!(global["success"], true);
    for secret in ["shared-key", "shared-secret", "shared-token"] {
        assert!(!global.to_string().contains(secret));
    }
    for state in handle.states.read().await.iter() {
        let config = state.config.read().await;
        assert_eq!(config.exchange.api_key, "shared-key");
        assert_eq!(config.exchange.recv_window, 9000);
        assert_eq!(config.exchange.sync_interval_secs, 9);
        assert_eq!(config.telegram.bot_token, "shared-token");
        assert!(config.telegram.enabled);
        // Switching to live mode requires a fresh manual resume.
        assert_eq!(*state.status.read().await, BotStatus::Paused);
    }
    let (_, blank) = request(
        &router,
        "/api/global/config",
        "POST",
        json!({
            "api_key":" ", "api_secret":"", "telegram_bot_token":"", "telegram_enabled":false
        }),
        true,
    )
    .await;
    assert_eq!(blank["success"], true);
    let (_, added) = request(&router, "/api/pairs", "POST", payload("ETHUSDT"), true).await;
    assert_eq!(added["success"], true);
    let (_, eth) = request(
        &router,
        "/api/pairs/ETHUSDT/config",
        "GET",
        Value::Null,
        true,
    )
    .await;
    assert_eq!(eth["data"]["has_api_key"], true);
    assert_eq!(eth["data"]["has_telegram_bot_token"], true);
    assert_eq!(eth["data"]["recv_window"], 9000);
    assert_eq!(eth["data"]["sync_interval_secs"], 9);
    let saved = handle.db.load_config().unwrap().unwrap();
    assert_eq!(saved.exchange.api_key, "shared-key");
    assert_eq!(saved.exchange.api_secret, "shared-secret");
    assert_eq!(saved.telegram.bot_token, "shared-token");
    assert!(saved
        .pairs
        .iter()
        .all(|pair| pair.exchange.api_key.is_none()
            && pair.exchange.api_secret.is_none()
            && pair.exchange.recv_window.is_none()
            && pair.exchange.sync_interval_secs.is_none()
            && pair.telegram.is_none()));
    let strategies = saved.strategy_configs().unwrap();
    assert_eq!(strategies[1].grid.grid_interval.to_string(), "10");
    assert_eq!(strategies[2].grid.grid_interval.to_string(), "0.1");
    assert!(strategies
        .iter()
        .all(|config| config.exchange.api_key == "shared-key"));
    let mut override_payload = versioned_payload(&router, "SOLUSDC").await;
    override_payload["api_key"] = json!("pair-key");
    let (_, rejected) = request(
        &router,
        "/api/pairs/SOLUSDC/config",
        "POST",
        override_payload.clone(),
        true,
    )
    .await;
    assert_eq!(rejected["success"], false);
    let (_, rejected_add) = request(&router, "/api/pairs", "POST", override_payload, true).await;
    assert_eq!(rejected_add["success"], false);
    manager.abort();
    let _ = manager.await;
    server.abort();
}

#[tokio::test]
async fn clearing_credentials_switches_every_pair_to_paper() {
    let mut runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    let (_, applied) = request(
        &router,
        "/api/global/config",
        "POST",
        json!({
            "api_key":"test-key", "api_secret":"test-secret", "telegram_enabled":true,
            "telegram_bot_token":"test-token", "telegram_chat_id":"123"
        }),
        true,
    )
    .await;
    assert_eq!(applied["success"], true);
    for state in handle.states.read().await.iter() {
        assert!(!state.config.read().await.exchange.is_paper());
    }
    let (_, rejected) = request(
        &router,
        "/api/global/config",
        "POST",
        json!({"clear_telegram_credentials":true}),
        true,
    )
    .await;
    assert_eq!(rejected["success"], false);
    let (_, cleared) = request(&router, "/api/global/config", "POST", json!({
        "clear_api_credentials":true, "clear_telegram_credentials":true, "telegram_enabled":false
    }), true).await;
    assert_eq!(cleared["success"], true);
    for state in handle.states.read().await.iter() {
        let config = state.config.read().await;
        assert!(config.exchange.api_key.is_empty());
        assert!(config.exchange.is_paper());
        assert!(config.telegram.bot_token.is_empty());
        assert!(config.telegram.chat_id.is_empty());
        assert!(!config.telegram.enabled);
    }
    let mut saved = handle.db.load_config().unwrap().unwrap();
    saved.ensure_pair_list().unwrap();
    assert!(saved.strategy_configs().unwrap().iter().all(|config| config
        .exchange
        .api_key
        .is_empty()
        && config.telegram.bot_token.is_empty()));
    manager.abort();
    let _ = manager.await;
}

#[tokio::test]
async fn global_reconciliation_failure_restores_both_changed_engines_and_saved_globals() {
    let runtime = fixture_with_failures(false, true);
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let (_, rejected) = request(
        &router,
        "/api/global/config",
        "POST",
        json!({"recv_window":8888}),
        true,
    )
    .await;
    assert_eq!(rejected["success"], false);
    assert!(rejected["message"]
        .as_str()
        .unwrap()
        .contains("已恢复原配置"));
    assert_eq!(
        handle
            .db
            .load_config()
            .unwrap()
            .unwrap()
            .exchange
            .recv_window,
        5000
    );
    for state in handle.states.read().await.iter() {
        assert_eq!(state.config.read().await.exchange.recv_window, 5000);
        assert!(state.try_order_submission().is_some());
    }
}

#[tokio::test]
async fn global_update_drains_old_submissions_before_switching_and_releases_only_after_commit() {
    let runtime = fixture();
    let handle = runtime.handle();
    let states = runtime.states();
    // An order in the second engine is still being submitted with the old credentials.
    let in_flight = states[1].try_order_submission().unwrap();
    let updating = handle.clone();
    let update = tokio::spawn(async move {
        updating
            .update_global_config(crate::types::UpdateGlobalConfigPayload {
                api_key: Some("new-account-key".into()),
                api_secret: Some("new-account-secret".into()),
                recv_window: Some(9000),
                ..Default::default()
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while states[0].try_order_submission().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!update.is_finished());
    // No pair may switch while an old-account submission remains in flight.
    for state in &states {
        assert!(state.config.read().await.exchange.api_key.is_empty());
        assert_eq!(state.config.read().await.exchange.recv_window, 5000);
    }
    // Even a manual resume cannot bypass the gate.
    states[0].resume_trading().await.unwrap();
    assert!(states[0].try_order_submission().is_none());
    assert!(handle
        .db
        .load_config()
        .unwrap()
        .unwrap()
        .exchange
        .api_key
        .is_empty());
    drop(in_flight);
    tokio::time::timeout(Duration::from_secs(2), update)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let saved = handle.db.load_config().unwrap().unwrap();
    assert_eq!(saved.exchange.api_key, "new-account-key");
    assert_eq!(saved.exchange.recv_window, 9000);
    for state in &states {
        assert_eq!(
            state.config.read().await.exchange.api_key,
            "new-account-key"
        );
        assert!(state.try_order_submission().is_some());
        // Credential changes switch the run mode, which requires a manual resume.
        assert_eq!(*state.status.read().await, BotStatus::Paused);
    }
}

#[tokio::test]
async fn enabling_global_guard_blocks_submissions_before_commit_returns() {
    let runtime = fixture();
    runtime
        .global_position_limit
        .membership_ready
        .store(true, std::sync::atomic::Ordering::SeqCst);
    for state in runtime.states() {
        state
            .global_position_initialized
            .store(true, std::sync::atomic::Ordering::SeqCst);
        state.account.write().await.available_balance = rust_decimal::Decimal::ZERO;
    }
    tokio::time::timeout(
        Duration::from_secs(2),
        runtime
            .handle()
            .update_global_config(crate::types::UpdateGlobalConfigPayload {
                pause_on_zero_available_margin: Some(true),
                ..Default::default()
            }),
    )
    .await
    .unwrap()
    .unwrap();
    for state in runtime.states() {
        assert!(state.try_order_submission().is_none());
        assert_eq!(*state.status.read().await, BotStatus::Paused);
    }
}

#[tokio::test]
async fn global_persistence_failure_rolls_back_before_allowing_new_submissions() {
    let path =
        std::env::temp_dir().join(format!("global-commit-failure-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    let runtime = fixture_with_database(db.clone(), false, false);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_global_config BEFORE UPDATE ON app_config
         WHEN json_extract(NEW.config_json, '$.exchange.recv_window') = 9000
         BEGIN SELECT RAISE(ABORT, 'simulated global persistence failure'); END;",
        )
        .unwrap();
    drop(connection);
    let error = runtime
        .handle()
        .update_global_config(crate::types::UpdateGlobalConfigPayload {
            recv_window: Some(9000),
            pause_on_zero_available_margin: Some(true),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(!runtime
        .global_position_limit
        .enabled
        .load(std::sync::atomic::Ordering::SeqCst));
    assert!(error.to_string().contains("已恢复原配置"));
    assert_eq!(
        db.load_config().unwrap().unwrap().exchange.recv_window,
        5000
    );
    for state in runtime.states() {
        assert_eq!(state.config.read().await.exchange.recv_window, 5000);
        assert!(state.try_order_submission().is_some());
    }
    drop(runtime);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn concurrent_global_patches_and_pair_updates_preserve_shared_and_independent_settings() {
    let runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let mut pair = versioned_payload(&router, "BTCUSDT").await;
    pair["buy_window"] = json!(3);
    let (recv, sync, pair) = tokio::join!(
        request(
            &router,
            "/api/global/config",
            "POST",
            json!({"recv_window":9000}),
            true
        ),
        request(
            &router,
            "/api/global/config",
            "POST",
            json!({"sync_interval_secs":9}),
            true
        ),
        request(&router, "/api/pairs/BTCUSDT/config", "POST", pair, true)
    );
    assert_eq!(recv.1["success"], true);
    assert_eq!(sync.1["success"], true);
    if pair.0 == StatusCode::CONFLICT {
        assert_eq!(pair.1["success"], false);
        let mut retry = versioned_payload(&router, "BTCUSDT").await;
        retry["buy_window"] = json!(3);
        let retried = request(&router, "/api/pairs/BTCUSDT/config", "POST", retry, true).await;
        assert_eq!(retried.1["success"], true);
    } else {
        assert_eq!(pair.0, StatusCode::OK);
        assert_eq!(pair.1["success"], true);
    }
    let saved = handle.db.load_config().unwrap().unwrap();
    assert_eq!(saved.exchange.recv_window, 9000);
    assert_eq!(saved.exchange.sync_interval_secs, 9);
    for state in handle.states.read().await.iter() {
        let config = state.config.read().await;
        assert_eq!(config.exchange.recv_window, 9000);
        assert_eq!(config.exchange.sync_interval_secs, 9);
    }
    assert_eq!(saved.strategy_configs().unwrap()[0].grid.buy_window, 5);
    assert_eq!(saved.strategy_configs().unwrap()[1].grid.buy_window, 3);
}

#[tokio::test]
async fn credential_probes_reject_incomplete_input_without_network_or_persistence() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let before = {
        let runtime = fixture_with_database(db.clone(), false, false);
        serde_json::to_value(runtime.handle().db.load_config().unwrap().unwrap()).unwrap()
    };
    for (path, body, expected) in [
        (
            "/api/global/test-exchange",
            json!({"api_key": "only-a-key"}),
            "请同时填写 API Key 和 Secret",
        ),
        (
            "/api/global/test-exchange",
            json!({"api_key": "k", "api_secret": "s", "recv_window": 0}),
            "请求有效窗口必须在 1-60000 毫秒之间",
        ),
        (
            "/api/global/test-telegram",
            json!({"bot_token": "123:abc"}),
            "请填写 Bot Token 和 Chat ID",
        ),
    ] {
        // Each case gets a fresh runtime, so the per-kind cooldown never interferes.
        let runtime = fixture_with_database(db.clone(), false, false);
        let router = create_managed_router(runtime.handle()).await;
        let (status, response) = request(&router, path, "POST", body, true).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response["success"], false, "{path}");
        assert!(
            response["message"].as_str().unwrap().contains(expected),
            "{path}: {response}"
        );
    }
    let after = serde_json::to_value(db.load_config().unwrap().unwrap()).unwrap();
    assert_eq!(after, before);
}

#[tokio::test]
async fn credential_probes_are_throttled_per_kind_and_independently() {
    let runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let incomplete = json!({"api_key": "only-a-key"});
    let (_, first) = request(
        &router,
        "/api/global/test-exchange",
        "POST",
        incomplete.clone(),
        true,
    )
    .await;
    assert!(first["message"].as_str().unwrap().contains("请同时填写"));
    let (_, second) = request(
        &router,
        "/api/global/test-exchange",
        "POST",
        incomplete,
        true,
    )
    .await;
    assert!(second["message"].as_str().unwrap().contains("测试过于频繁"));
    // A different probe is not blocked by the exchange cooldown.
    let (_, telegram) = request(
        &router,
        "/api/global/test-telegram",
        "POST",
        json!({"bot_token": "123:abc"}),
        true,
    )
    .await;
    assert!(telegram["message"]
        .as_str()
        .unwrap()
        .contains("请填写 Bot Token"));
}
