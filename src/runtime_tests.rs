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
    let db = Arc::new(Database::open(":memory:").unwrap());
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
            let engine = tokio::spawn(async move {
                while let Some(action) = rx.recv().await {
                    match action {
                        BotControlAction::UpdateConfig { config, reply } => {
                            let result = engine_state
                                .save_runtime_config(*config)
                                .await
                                .map_err(|error| error.to_string());
                            let _ = reply.send(result);
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
                        BotControlAction::Rebalance => {}
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
    TradingRuntime::from_pairs(pairs, db, lock, config.server)
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
    json!({ "symbol": symbol, "grid_interval": "0.1", "order_amount_usdc": "100", "buy_window": 5, "sell_window": 5,
        "recv_window": 8000, "sync_interval_secs": 7 })
}

#[tokio::test]
async fn management_authenticates_add_remove_server_settings_and_pair_resources() {
    let runtime = fixture();
    let router = create_managed_router(runtime.handle()).await;
    for (path, method, body) in [
        ("/api/pairs", "POST", payload("ETHUSDT")),
        ("/api/pairs/SOLUSDC", "DELETE", json!({"confirm":true})),
        ("/api/server/config", "GET", Value::Null),
        (
            "/api/server/config",
            "POST",
            json!({"host":"127.0.0.1","port":9090}),
        ),
        ("/api/pairs/BTCUSDT/config", "GET", Value::Null),
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
    assert_eq!(added_config["data"]["recv_window"], 8000);
    assert_eq!(added_config["data"]["sync_interval_secs"], 7);
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
    let mut invalid = payload("SOLUSDC");
    invalid["recv_window"] = json!(0);
    let (_, rejected) = request(&router, "/api/pairs/SOLUSDC/config", "POST", invalid, true).await;
    assert_eq!(rejected["success"], false);
    let mut update = payload("SOLUSDC");
    update["api_key"] = json!("configuration-key");
    update["api_secret"] = json!("configuration-secret");
    update["telegram_bot_token"] = json!("configuration-bot-token");
    update["telegram_chat_id"] = json!("123456");
    let (_, applied) = request(&router, "/api/pairs/SOLUSDC/config", "POST", update, true).await;
    assert_eq!(applied["success"], true);
    let serialized = applied.to_string();
    for secret in [
        "configuration-key",
        "configuration-secret",
        "configuration-bot-token",
    ] {
        assert!(!serialized.contains(secret));
    }
    let mut clear = payload("SOLUSDC");
    clear["clear_api_credentials"] = json!(true);
    clear["clear_telegram_credentials"] = json!(true);
    let (_, cleared) = request(&router, "/api/pairs/SOLUSDC/config", "POST", clear, true).await;
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
