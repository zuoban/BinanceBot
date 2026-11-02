use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use binance_grid_bot::config::{AppConfig, TradingPairConfig};
use binance_grid_bot::db::Database;
use binance_grid_bot::server::{create_multi_router, create_router, AppState};
use binance_grid_bot::types::BotControlAction;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tower::ServiceExt;

fn request(body: Option<Value>) -> Request<Body> {
    let mut builder = Request::builder()
        .uri("/api/config")
        .header("Authorization", "Bearer config-test-token");
    if body.is_some() {
        builder = builder
            .method("POST")
            .header("Content-Type", "application/json");
    }
    builder
        .body(body.map_or(Body::empty(), |value| Body::from(value.to_string())))
        .unwrap()
}

async fn json_response(response: axum::response::Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

fn payload(version: &Value) -> Value {
    json!({
        "symbol": "SOLUSDC",
        "grid_interval": "0.1",
        "order_amount_usdc": "100",
        "buy_window": 5,
        "sell_window": 5,
        "baseline_version": version
    })
}

#[tokio::test]
async fn credentials_are_masked_preserved_and_explicitly_cleared() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("config-test-token").unwrap();
    let config = AppConfig::default();
    db.save_config(&config).unwrap();
    let (tx, mut rx) = mpsc::channel(8);
    let state = AppState::new(config, db.clone(), tx);
    let engine_state = state.clone();
    tokio::spawn(async move {
        while let Some(action) = rx.recv().await {
            if let BotControlAction::UpdateConfig { config, reply } = action {
                let result = engine_state
                    .db
                    .save_config(&config)
                    .map_err(|e| e.to_string());
                if result.is_ok() {
                    *engine_state.config.write().await = *config;
                }
                let _ = reply.send(result);
            }
        }
    });
    let app = create_router(state);
    let initial = json_response(app.clone().oneshot(request(None)).await.unwrap()).await;
    let mut update = payload(&initial["data"]["config_version"]);
    update["api_key"] = json!("abcd-credential-wxyz");
    update["api_secret"] = json!("secret-credential-value");
    update["telegram_enabled"] = json!(true);
    update["telegram_bot_token"] = json!("telegram-secret-token");
    update["telegram_chat_id"] = json!("123456");
    let saved = json_response(app.clone().oneshot(request(Some(update))).await.unwrap()).await;
    assert_eq!(saved["success"], true);
    assert_eq!(saved["data"]["has_api_key"], true);
    assert_eq!(saved["data"]["has_api_secret"], true);
    assert_eq!(saved["data"]["has_telegram_bot_token"], true);
    assert_eq!(saved["data"]["api_key_preview"], "abcd...wxyz");
    let serialized = saved.to_string();
    for secret in [
        "abcd-credential-wxyz",
        "secret-credential-value",
        "telegram-secret-token",
    ] {
        assert!(!serialized.contains(secret));
    }

    let mut blank = payload(&saved["data"]["config_version"]);
    blank["api_key"] = json!("  ");
    blank["api_secret"] = json!("");
    blank["telegram_bot_token"] = json!("");
    let preserved = json_response(app.clone().oneshot(request(Some(blank))).await.unwrap()).await;
    assert_eq!(preserved["success"], true);
    let persisted = db.load_config().unwrap().unwrap();
    assert_eq!(persisted.exchange.api_key, "abcd-credential-wxyz");
    assert_eq!(persisted.exchange.api_secret, "secret-credential-value");
    assert_eq!(persisted.telegram.bot_token, "telegram-secret-token");

    let mut clear = payload(&preserved["data"]["config_version"]);
    clear["clear_api_credentials"] = json!(true);
    clear["clear_telegram_credentials"] = json!(true);
    clear["telegram_enabled"] = json!(false);
    let cleared = json_response(app.clone().oneshot(request(Some(clear))).await.unwrap()).await;
    assert_eq!(cleared["success"], true);
    assert_eq!(cleared["data"]["has_api_key"], false);
    assert_eq!(cleared["data"]["has_api_secret"], false);
    assert_eq!(cleared["data"]["has_telegram_bot_token"], false);
    let persisted = db.load_config().unwrap().unwrap();
    assert!(persisted.exchange.api_key.is_empty());
    assert!(persisted.exchange.api_secret.is_empty());
    assert!(persisted.telegram.bot_token.is_empty());
    assert!(persisted.telegram.chat_id.is_empty());
}

#[tokio::test]
async fn rejected_configuration_keeps_active_and_saved_settings() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("config-test-token").unwrap();
    let config = AppConfig::default();
    db.save_config(&config).unwrap();
    let (tx, mut rx) = mpsc::channel(8);
    let state = AppState::new(config, db.clone(), tx);
    let app = create_router(state);
    let initial = json_response(app.clone().oneshot(request(None)).await.unwrap()).await;
    let before = serde_json::to_value(db.load_config().unwrap().unwrap()).unwrap();
    let mut invalid = payload(&initial["data"]["config_version"]);
    invalid["grid_interval"] = json!("0");
    let rejected = json_response(app.clone().oneshot(request(Some(invalid))).await.unwrap()).await;
    assert_eq!(rejected["success"], false);
    assert!(matches!(
        rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    let engine = tokio::spawn(async move {
        if let Some(BotControlAction::UpdateConfig { reply, .. }) = rx.recv().await {
            let _ = reply.send(Err("exchange rejected configuration".into()));
        }
    });
    let mut update = payload(&initial["data"]["config_version"]);
    update["grid_interval"] = json!("0.2");
    let response = app.clone().oneshot(request(Some(update))).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let rejected = json_response(response).await;
    assert_eq!(rejected["success"], false);
    assert_eq!(rejected["message"], "exchange rejected configuration");
    engine.await.unwrap();
    let after = json_response(app.oneshot(request(None)).await.unwrap()).await;
    assert_eq!(after["data"], initial["data"]);
    assert_eq!(
        serde_json::to_value(db.load_config().unwrap().unwrap()).unwrap(),
        before
    );
}

fn pair_request(symbol: &str, body: Option<Value>) -> Request<Body> {
    let mut builder = Request::builder()
        .uri(format!("/api/pairs/{symbol}/config"))
        .header("Authorization", "Bearer config-test-token");
    if body.is_some() {
        builder = builder
            .method("POST")
            .header("Content-Type", "application/json");
    }
    builder
        .body(body.map_or(Body::empty(), |value| Body::from(value.to_string())))
        .unwrap()
}

#[tokio::test]
async fn concurrent_pair_updates_survive_reconstruction_and_reject_stale_versions() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("config-test-token").unwrap();
    let config = AppConfig {
        pairs: ["SOLUSDC", "BTCUSDT"]
            .map(|symbol| TradingPairConfig::new(symbol.into()))
            .to_vec(),
        ..Default::default()
    };
    db.save_config(&config).unwrap();
    let lock = Arc::new(Mutex::new(()));
    let mut states = Vec::new();
    for pair_config in config.strategy_configs().unwrap() {
        let symbol = pair_config.exchange.symbol.clone();
        let (tx, mut rx) = mpsc::channel(8);
        let state = AppState::new_for_pair(pair_config, db.clone(), tx, lock.clone());
        let engine_state = state.clone();
        tokio::spawn(async move {
            while let Some(BotControlAction::UpdateConfig { config, reply }) = rx.recv().await {
                let result = engine_state
                    .db
                    .save_pair_config(&symbol, &config)
                    .map_err(|error| error.to_string());
                if result.is_ok() {
                    *engine_state.config.write().await = *config;
                }
                let _ = reply.send(result);
            }
        });
        states.push(state);
    }
    let app = create_multi_router(states);
    let sol_before = json_response(
        app.clone()
            .oneshot(pair_request("SOLUSDC", None))
            .await
            .unwrap(),
    )
    .await;
    let btc_before = json_response(
        app.clone()
            .oneshot(pair_request("BTCUSDT", None))
            .await
            .unwrap(),
    )
    .await;
    let mut sol_update = payload(&sol_before["data"]["config_version"]);
    sol_update["grid_interval"] = json!("0.2");
    let mut btc_update = payload(&btc_before["data"]["config_version"]);
    btc_update["symbol"] = json!("BTCUSDT");
    btc_update["grid_interval"] = json!("0.3");
    let (sol, btc) = tokio::join!(
        app.clone()
            .oneshot(pair_request("SOLUSDC", Some(sol_update.clone()))),
        app.clone()
            .oneshot(pair_request("BTCUSDT", Some(btc_update.clone())))
    );
    let sol = sol.unwrap();
    let btc = btc.unwrap();
    assert_eq!(sol.status(), StatusCode::OK);
    assert_eq!(btc.status(), StatusCode::OK);
    let sol = json_response(sol).await;
    let btc = json_response(btc).await;
    assert_eq!(sol["success"], true);
    assert_eq!(btc["success"], true);
    assert_eq!(sol["data"]["grid_interval"], "0.2");
    assert_eq!(btc["data"]["grid_interval"], "0.3");
    for (symbol, update) in [
        ("SOLUSDC", sol_update.clone()),
        ("BTCUSDT", btc_update.clone()),
    ] {
        let response = app
            .clone()
            .oneshot(pair_request(symbol, Some(update)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    let restored = db.load_config().unwrap().unwrap();
    let lock = Arc::new(Mutex::new(()));
    let restored_states = restored
        .strategy_configs()
        .unwrap()
        .into_iter()
        .map(|config| {
            let (tx, _rx) = mpsc::channel(8);
            AppState::new_for_pair(config, db.clone(), tx, lock.clone())
        })
        .collect();
    let restarted = create_multi_router(restored_states);
    for (symbol, interval, old_update) in [
        ("SOLUSDC", "0.2", sol_update),
        ("BTCUSDT", "0.3", btc_update),
    ] {
        let view = json_response(
            restarted
                .clone()
                .oneshot(pair_request(symbol, None))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(view["data"]["symbol"], symbol);
        assert_eq!(view["data"]["grid_interval"], interval);
        let response = restarted
            .clone()
            .oneshot(pair_request(symbol, Some(old_update)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }
}
