use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use binance_grid_bot::config::AppConfig;
use binance_grid_bot::db::Database;
use binance_grid_bot::server::{create_router, AppState};
use binance_grid_bot::types::BotControlAction;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};
use tower::ServiceExt;

#[tokio::test]
async fn test_full_auth_lifecycle() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let config = AppConfig::default();
    let (action_tx, mut action_rx) = mpsc::channel(16);
    let state = AppState::new(config, db.clone(), action_tx);
    let setup_code = state.setup_code().unwrap().to_string();
    let config_state = state.clone();
    tokio::spawn(async move {
        while let Some(action) = action_rx.recv().await {
            if let BotControlAction::UpdateConfig { config, reply } = action {
                let result = config_state
                    .db
                    .save_config(&config)
                    .map_err(|error| error.to_string());
                if result.is_ok() {
                    *config_state.config.write().await = *config;
                }
                let _ = reply.send(result);
            }
        }
    });
    let app = create_router(state);

    // 1. Initial status: should be uninitialized
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/auth/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["data"]["initialized"], false);
    assert_eq!(json["data"]["authenticated"], false);

    // 2. Accessing protected endpoint without token: should be 401 Unauthorized
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // 3. Setup with password too short (< 6 chars): should be 400 Bad Request
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/setup")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"password": "123"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // 4. Setup with valid password: should be 200 OK and return token
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/setup")
                .header("Content-Type", "application/json")
                .body(Body::from(format!(
                    r#"{{"password": "MySuperSecret123", "setup_code": "{}"}}"#,
                    setup_code
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert!(json["success"].as_bool().unwrap());
    let token = json["data"]["token"].as_str().unwrap().to_string();
    assert!(!token.is_empty());

    // 5. Subsequent setup attempt: should be rejected (cannot re-setup)
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/setup")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"password": "AnotherPassword123"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // 6. Accessing protected route with Bearer token: should now succeed!
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/config")
                .header("Authorization", format!("Bearer {}", token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Telegram credentials are required when enabled and are never echoed by the API.
    let mut config_payload = serde_json::json!({
        "symbol": "SOLUSDC",
        "grid_interval": "0.1",
        "order_amount_usdc": "100",
        "buy_window": 5,
        "sell_window": 5,
        "telegram_enabled": true
    });
    let post_config = |payload: &Value| {
        Request::builder()
            .method("POST")
            .uri("/api/config")
            .header("Authorization", format!("Bearer {}", token))
            .header("Content-Type", "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap()
    };
    let res = app
        .clone()
        .oneshot(post_config(&config_payload))
        .await
        .unwrap();
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["success"], false);

    config_payload["telegram_bot_token"] = Value::String("123:secret-token".into());
    config_payload["telegram_chat_id"] = Value::String("987654321".into());
    let res = app
        .clone()
        .oneshot(post_config(&config_payload))
        .await
        .unwrap();
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["success"], true);
    assert_eq!(json["data"]["has_telegram_bot_token"], true);
    assert_eq!(json["data"]["telegram_chat_id"], "987654321");
    assert!(!String::from_utf8_lossy(&body).contains("123:secret-token"));
    assert_eq!(
        db.load_config().unwrap().unwrap().telegram.bot_token,
        "123:secret-token"
    );
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/config")
                .header("Authorization", format!("Bearer {}", token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    assert!(!String::from_utf8_lossy(&body).contains("123:secret-token"));

    // 7. Login with wrong password: should be 401
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"password": "WrongPassword!"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // 8. Login with correct password: should succeed with a new token
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"password": "MySuperSecret123"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 9. Logout with token: should invalidate token
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/logout")
                .header("Authorization", format!("Bearer {}", token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 10. Accessing protected route with invalidated token: should now be 401!
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/config")
                .header("Authorization", format!("Bearer {}", token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[test]
fn test_default_server_binding_is_all_interfaces() {
    let config = AppConfig::default();
    assert_eq!(config.server.host, "0.0.0.0");
    assert_eq!(config.server.port, 8080);
}

#[tokio::test]
async fn websocket_authenticates_without_a_query_token() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.set_admin_password("secret-password").unwrap();
    let token = "test-session-token";
    db.create_session(token).unwrap();
    let (action_tx, mut action_rx) = mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db.clone(), action_tx);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server =
        tokio::spawn(async move { axum::serve(listener, create_router(state)).await.unwrap() });

    let mut request = format!("ws://{}/ws", address)
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        format!("auth, token.{}", token).parse().unwrap(),
    );
    let (mut socket, response) = connect_async(request).await.unwrap();
    assert_eq!(response.headers()["Sec-WebSocket-Protocol"], "auth");
    let message = socket.next().await.unwrap().unwrap();
    assert!(message
        .into_text()
        .unwrap()
        .contains("\"type\":\"snapshot\""));
    db.delete_session(token).unwrap();
    socket
        .send(tokio_tungstenite::tungstenite::Message::Text(
            "{\"action\":\"resume\"}".into(),
        ))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), action_rx.recv())
            .await
            .is_err()
    );
    server.abort();
}
