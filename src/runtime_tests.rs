use super::*;
use crate::config::TradingPairConfig;
use crate::server::create_managed_router;
use crate::types::{BotControlAction, BotStatus, TradingMode};
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use serde_json::{json, Value};
use tower::ServiceExt;

fn fixture() -> TradingRuntime {
    fixture_with_shutdown_failure(false)
}

fn fixture_with_shutdown_failure(fail_shutdown: bool) -> TradingRuntime {
    fixture_with_failures(fail_shutdown, false)
}

fn fixture_with_failures(fail_shutdown: bool, fail_global: bool) -> TradingRuntime {
    let db = Arc::new(Database::open(":memory:").unwrap());
    fixture_with_database(db, fail_shutdown, fail_global)
}

fn fixture_with_database(
    db: Arc<Database>,
    fail_shutdown: bool,
    fail_global: bool,
) -> TradingRuntime {
    db.create_session("configuration-test-token").unwrap();
    let config = AppConfig {
        pairs: ["SOLUSDC", "BTCUSDT"]
            .map(|symbol| TradingPairConfig::new(symbol.into()))
            .to_vec(),
        ..Default::default()
    };
    db.save_config(&config).unwrap();
    let lock = Arc::new(Mutex::new(()));
    let pairs = config
        .strategy_configs()
        .unwrap()
        .into_iter()
        .map(|config| {
            let (tx, mut rx) = mpsc::channel(8);
            let state = AppState::new_for_pair(config, db.clone(), tx, lock.clone());
            let engine_state = state.clone();
            let engine =
                tokio::spawn(async move {
                    while let Some(action) = rx.recv().await {
                        match action {
                            BotControlAction::UpdateConfig { config, reply } => {
                                let result = engine_state
                                    .save_runtime_config(*config)
                                    .await
                                    .map_err(|error| error.to_string());
                                let _ = reply.send(result);
                            }
                            BotControlAction::UpdateGlobalConfig { config, reply } => {
                                assert!(engine_state.try_order_submission().is_none(),
                                "submissions must remain blocked through propagation and rollback");
                                let reject = fail_global
                                    && config.recv_window == 8888
                                    && engine_state.pair_scope() == Some("BTCUSDT");
                                let mut updated = engine_state.config.read().await.clone();
                                config.apply_to(&mut updated);
                                let result = engine_state
                                    .save_runtime_config(updated)
                                    .await
                                    .map_err(|error| error.to_string());
                                assert!(engine_state.try_order_submission().is_none());
                                let _ = reply.send(if reject {
                                    Err("模拟保存后对账失败".into())
                                } else {
                                    result
                                });
                            }
                            BotControlAction::Shutdown { reply } => {
                                let result = if fail_shutdown {
                                    Err("撤单结果仍未确认".into())
                                } else {
                                    Ok(())
                                };
                                let _ = reply.send(result);
                                break;
                            }
                            BotControlAction::Pause | BotControlAction::CancelAll => {
                                engine_state.pause_trading().await.unwrap();
                            }
                            BotControlAction::Resume => {
                                engine_state.resume_trading().await.unwrap();
                            }
                            BotControlAction::Rebalance | BotControlAction::GlobalRiskCancel => {}
                        }
                    }
                });
            let (shutdown_tx, _) = watch::channel(false);
            PairTask {
                state,
                engine,
                shutdown_tx,
                background: vec![],
            }
        })
        .collect();
    TradingRuntime::from_pairs(
        pairs,
        db,
        lock,
        config.server.clone(),
        GlobalConfig::from_app(&config),
    )
}

#[tokio::test]
async fn failed_removal_keeps_durable_configuration_and_does_not_repoll_joined_engine() {
    let mut runtime = fixture_with_shutdown_failure(true);
    let state = runtime.states()[0].clone();
    state.pause_trading().await.unwrap();
    assert!(runtime
        .remove_pair("SOLUSDC")
        .await
        .unwrap_err()
        .to_string()
        .contains("撤单结果仍未确认"));
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
    assert_eq!(runtime.handle.states.read().await.len(), 2);
    assert!(state.is_shutting_down());
    assert!(runtime.run().await.contains("stopped"));
}

#[tokio::test]
async fn removal_preserves_at_least_one_pair_and_requires_confirmed_empty_orders() {
    let mut runtime = fixture();
    let state = runtime.states()[0].clone();
    state.pause_trading().await.unwrap();
    let order = crate::types::GridOrder {
        client_order_id: "gb_b_removal".into(),
        symbol: "SOLUSDC".into(),
        order_id: None,
        side: crate::types::OrderSide::Buy,
        price: rust_decimal_macros::dec!(100),
        quantity: rust_decimal_macros::dec!(1),
        amount_usdc: rust_decimal_macros::dec!(100),
        status: crate::types::OrderStatus::New,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        grid_level: 1,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: vec![],
    };
    state
        .active_orders
        .write()
        .await
        .insert(order.client_order_id.clone(), order);
    assert!(runtime
        .remove_pair("SOLUSDC")
        .await
        .unwrap_err()
        .to_string()
        .contains("仍有挂单"));
    assert!(!state.is_shutting_down());
    state.active_orders.write().await.clear();
    runtime.remove_pair("SOLUSDC").await.unwrap();
    assert!(runtime
        .remove_pair("BTCUSDT")
        .await
        .unwrap_err()
        .to_string()
        .contains("至少保留"));
    assert_eq!(
        runtime
            .handle
            .db
            .load_config()
            .unwrap()
            .unwrap()
            .pairs
            .len(),
        1
    );
}

async fn request(
    router: &Router,
    path: &str,
    method: &str,
    body: Value,
    authenticated: bool,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .uri(path)
        .method(method)
        .header("Content-Type", "application/json");
    if authenticated {
        request = request.header("Authorization", "Bearer configuration-test-token");
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1_000_000).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn payload(symbol: &str) -> Value {
    json!({ "symbol": symbol, "grid_interval": "0.1", "order_amount_usdc": "100", "buy_window": 5, "sell_window": 5 })
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
        equity: Some(rust_decimal_macros::dec!(100)),
        long: Some(rust_decimal_macros::dec!(20)),
        short: Some(rust_decimal_macros::dec!(0)),
    };
    runtime
        .handle
        .db
        .save_hourly_account_snapshot(hour, &[group])
        .unwrap();
    let router = create_managed_router(runtime.handle()).await;
    let (status, body) = request(&router, "/api/overview/history", "GET", Value::Null, true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["hour"], hour);
    assert_eq!(body["data"][0]["groups"][0]["asset"], "USDC");
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
    let mut runtime = fixture();
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
        payload("BTCUSDT"),
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
        let (success, conflict) = if a.1["success"] == true {
            (a.1, b.1)
        } else {
            (b.1, a.1)
        };
        assert_eq!(success["success"], true);
        assert_eq!(conflict["success"], false);
        assert!(conflict["message"].as_str().unwrap().contains("刷新"));
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
    let mut runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    let mut btc = payload("BTCUSDT");
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
        assert_eq!(*state.status.read().await, BotStatus::Running);
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
    let mut override_payload = payload("SOLUSDC");
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
}

#[tokio::test]
async fn global_clearing_validates_all_pair_modes_and_notification_credentials() {
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
    let mut live = payload("BTCUSDT");
    live["dry_run"] = json!(false);
    assert_eq!(
        request(&router, "/api/pairs/BTCUSDT/config", "POST", live, true)
            .await
            .1["success"],
        true
    );
    for clear in [
        json!({"clear_api_credentials":true}),
        json!({"clear_telegram_credentials":true}),
    ] {
        let (_, rejected) = request(&router, "/api/global/config", "POST", clear, true).await;
        assert_eq!(rejected["success"], false);
    }
    assert!(handle
        .states
        .read()
        .await
        .iter()
        .all(|state| !state.is_shutting_down()));
    let mut paper = payload("BTCUSDT");
    paper["dry_run"] = json!(true);
    request(&router, "/api/pairs/BTCUSDT/config", "POST", paper, true).await;
    let (_, cleared) = request(&router, "/api/global/config", "POST", json!({
        "clear_api_credentials":true, "clear_telegram_credentials":true, "telegram_enabled":false
    }), true).await;
    assert_eq!(cleared["success"], true);
    for state in handle.states.read().await.iter() {
        let config = state.config.read().await;
        assert!(config.exchange.api_key.is_empty());
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
        assert_eq!(*state.status.read().await, BotStatus::Running);
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
    let mut pair = payload("BTCUSDT");
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
    assert_eq!(pair.1["success"], true);
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
