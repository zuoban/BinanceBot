use axum::body::{to_bytes, Body};
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use binance_grid_bot::auth::AdminAuth;
use binance_grid_bot::config::AppConfig;
use binance_grid_bot::db::Database;
use binance_grid_bot::server::{create_router, AppState};
use binance_grid_bot::types::BotControlAction;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};
use tower::ServiceExt;

fn state_with_password(
    db: Arc<Database>,
    action_tx: mpsc::Sender<BotControlAction>,
    password: &str,
) -> Arc<AppState> {
    AppState::new_with_admin_auth(
        AppConfig::default(),
        db,
        action_tx,
        Some(Arc::new(AdminAuth::from_password(password).unwrap())),
    )
}

#[tokio::test]
async fn test_full_auth_lifecycle() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, mut action_rx) = mpsc::channel(16);
    let state = state_with_password(db.clone(), action_tx, "correct-password");
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
    let app = create_router(state).layer(axum::Extension(ConnectInfo(
        "127.0.0.1:12345".parse::<SocketAddr>().unwrap(),
    )));

    // The backend is API-only; the separately deployed frontend serves the SPA.
    let res = app
        .clone()
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    // 1. Initial status: authentication is configured from process startup.
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
    assert_eq!(json["data"]["initialized"], true);
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

    // 3. The removed setup endpoint must not be reachable.
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/setup")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"password": "irrelevant"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    // 4. Login with wrong password: should be 401
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

    // 5. Login with the configured password: should succeed with a new token
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"password": "correct-password"}"#))
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
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let config: Value = serde_json::from_slice(&body).unwrap();

    // Telegram credentials are required when enabled and are never echoed by the API.
    let mut config_payload = serde_json::json!({
        "symbol": "SOLUSDC",
        "grid_interval": "0.1",
        "order_amount_usdc": "100",
        "buy_window": 5,
        "sell_window": 5,
        "baseline_version": config["data"]["config_version"],
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

    // 7. Logout with token: should invalidate token
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

    // 8. Accessing protected route with invalidated token: should now be 401!
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

struct AuthTestDatabase {
    db: Arc<Database>,
    external: rusqlite::Connection,
    _directory: AuthTempDir,
}

struct AuthTempDir(std::path::PathBuf);

impl Drop for AuthTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl AuthTestDatabase {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!("auth-api-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("auth.db");
        let db = Arc::new(Database::open(path.to_str().unwrap()).unwrap());
        let external = rusqlite::Connection::open(&path).unwrap();
        external
            .busy_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let mode: String = external
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode.to_lowercase(), "wal");
        Self {
            db,
            external,
            _directory: AuthTempDir(directory),
        }
    }

    fn fail_session_writes(&self, operation: &str) {
        assert!(matches!(operation, "INSERT" | "DELETE"));
        self.external
            .execute_batch(&format!(
                "CREATE TRIGGER fail_session_write BEFORE {operation} ON auth_sessions \
             BEGIN SELECT RAISE(ABORT, 'injected session failure'); END;"
            ))
            .unwrap();
    }

    fn session_count(&self) -> i64 {
        self.external
            .query_row("SELECT COUNT(*) FROM auth_sessions", [], |row| row.get(0))
            .unwrap()
    }
}

fn auth_request(path: &str, body: serde_json::Value, token: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri(path)
        .header("Content-Type", "application/json")
        .extension(ConnectInfo(
            "127.0.0.1:12345".parse::<SocketAddr>().unwrap(),
        ));
    if let Some(token) = token {
        builder = builder.header("Authorization", format!("Bearer {token}"));
    }
    builder.body(Body::from(body.to_string())).unwrap()
}

#[tokio::test]
async fn saturated_password_workers_reject_without_consuming_source_limit() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _rx) = mpsc::channel(1);
    let state = state_with_password(db, tx, "correct-password");
    let app = create_router(state.clone());
    let permits: Vec<_> = (0..4)
        .map(|_| state.try_begin_auth_work().unwrap())
        .collect();
    assert!(state.try_begin_auth_work().is_none());
    let request = || {
        auth_request(
            "/api/auth/login",
            serde_json::json!({"password": "wrong"}),
            None,
        )
    };
    for _ in 0..11 {
        assert_eq!(
            app.clone().oneshot(request()).await.unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
    drop(permits);
    assert_eq!(
        app.oneshot(request()).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn login_limit_is_scoped_to_socket_ip_and_ignores_forwarded_headers() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, _action_rx) = mpsc::channel(16);
    let app = create_router(state_with_password(db, action_tx, "correct-password"));
    let request = |address: &str, forwarded: &str, password: &str| {
        Request::builder()
            .method("POST")
            .uri("/api/auth/login")
            .header("Content-Type", "application/json")
            .header("X-Forwarded-For", forwarded)
            .header("X-Real-IP", forwarded)
            .extension(ConnectInfo(address.parse::<SocketAddr>().unwrap()))
            .body(Body::from(
                serde_json::json!({"password": password}).to_string(),
            ))
            .unwrap()
    };

    for i in 0..10 {
        let response = app
            .clone()
            .oneshot(request(
                "192.0.2.1:1000",
                &format!("198.51.100.{i}"),
                "wrong-password",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    let response = app
        .clone()
        .oneshot(request("192.0.2.2:2000", "192.0.2.1", "correct-password"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .clone()
        .oneshot(request(
            "[::ffff:192.0.2.1]:3000",
            "203.0.113.1",
            "correct-password",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);

    let missing_peer = Request::builder()
        .method("POST")
        .uri("/api/auth/login")
        .header("Content-Type", "application/json")
        .header("X-Forwarded-For", "192.0.2.3")
        .body(Body::from(r#"{"password":"correct-password"}"#))
        .unwrap();
    assert_eq!(
        app.oneshot(missing_peer).await.unwrap().status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[tokio::test]
async fn auth_status_reports_database_errors_instead_of_false_state() {
    let fixture = AuthTestDatabase::new();
    fixture.db.create_session("valid-token").unwrap();
    let (action_tx, _action_rx) = mpsc::channel(16);
    let state = state_with_password(fixture.db.clone(), action_tx, "correct-password");
    let app = create_router(state);

    let request = || {
        Request::builder()
            .uri("/api/auth/status")
            .header("Authorization", "Bearer valid-token")
            .body(Body::empty())
            .unwrap()
    };
    let response = app.clone().oneshot(request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["data"]["initialized"], true);
    assert_eq!(json["data"]["authenticated"], true);

    fixture
        .external
        .execute_batch("ALTER TABLE auth_sessions RENAME TO auth_sessions_unavailable")
        .unwrap();
    let response = app.oneshot(request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["success"], false);
    assert!(json["data"].is_null());
    assert_eq!(json["message"], "认证服务异常，请稍后重试");
}

#[tokio::test]
async fn protected_route_reports_session_read_failure_without_treating_it_as_expiration() {
    let fixture = AuthTestDatabase::new();
    fixture.db.create_session("valid-token").unwrap();
    // Never validated before the table disappears, so the session cache cannot answer for it.
    fixture.db.create_session("cold-token").unwrap();
    let (action_tx, _action_rx) = mpsc::channel(16);
    let state = state_with_password(fixture.db.clone(), action_tx, "correct-password");
    let app = create_router(state);
    let request = |token: &str| {
        Request::builder()
            .uri("/api/status")
            .header("Authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    };

    assert_eq!(
        app.clone()
            .oneshot(request("valid-token"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        app.clone()
            .oneshot(request("invalid-token"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    fixture
        .external
        .execute_batch("ALTER TABLE auth_sessions RENAME TO auth_sessions_unavailable")
        .unwrap();
    let response = app.oneshot(request("cold-token")).await.unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["success"], false);
    assert!(json["data"].is_null());
    assert_eq!(json["message"], "认证服务异常，请稍后重试");
}

#[tokio::test]
async fn expired_session_cleanup_failure_still_allows_reauthentication() {
    let fixture = AuthTestDatabase::new();
    fixture.db.create_session("expired-token").unwrap();
    fixture
        .external
        .execute(
            "UPDATE auth_sessions SET created_at = ?1 WHERE token = ?2",
            ["2000-01-01T00:00:00Z", "expired-token"],
        )
        .unwrap();
    fixture.fail_session_writes("DELETE");
    let (action_tx, _action_rx) = mpsc::channel(16);
    let state = state_with_password(fixture.db.clone(), action_tx, "correct-password");
    let app = create_router(state);
    let request = |path: &str| {
        Request::builder()
            .uri(path)
            .header("Authorization", "Bearer expired-token")
            .body(Body::empty())
            .unwrap()
    };

    let response = app
        .clone()
        .oneshot(request("/api/auth/status"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["data"]["initialized"], true);
    assert_eq!(json["data"]["authenticated"], false);
    assert_eq!(
        app.clone()
            .oneshot(request("/api/status"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(fixture.session_count(), 1);

    let response = app
        .oneshot(auth_request(
            "/api/auth/login",
            serde_json::json!({ "password": "correct-password" }),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(fixture.session_count(), 2);
}

#[tokio::test]
async fn login_session_insert_failure_returns_error_without_token() {
    let fixture = AuthTestDatabase::new();
    let (action_tx, _action_rx) = mpsc::channel(16);
    let app = create_router(state_with_password(
        fixture.db.clone(),
        action_tx,
        "correct-password",
    ));
    fixture.fail_session_writes("INSERT");

    let response = app
        .oneshot(auth_request(
            "/api/auth/login",
            serde_json::json!({
                "password": "correct-password"
            }),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert!(json.get("token").is_none());
    assert!(json["data"].get("token").is_none());
    assert_eq!(fixture.session_count(), 0);
}

#[tokio::test]
async fn login_rejects_when_the_process_has_no_password_configured() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, _action_rx) = mpsc::channel(16);
    let app = create_router(AppState::new(AppConfig::default(), db, action_tx));

    let response = app
        .oneshot(auth_request(
            "/api/auth/login",
            serde_json::json!({"password": "anything"}),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["success"], false);
    assert!(json["message"].as_str().unwrap().contains("无需登录"));
}

#[tokio::test]
async fn logout_session_delete_failure_returns_error() {
    let fixture = AuthTestDatabase::new();
    fixture.db.create_session("valid-token").unwrap();
    let (action_tx, _action_rx) = mpsc::channel(16);
    let app = create_router(state_with_password(
        fixture.db.clone(),
        action_tx,
        "correct-password",
    ));
    fixture.fail_session_writes("DELETE");

    let response = app
        .clone()
        .oneshot(auth_request(
            "/api/auth/logout",
            serde_json::json!({}),
            Some("valid-token"),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(fixture.session_count(), 1);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/config")
                .header("Authorization", "Bearer valid-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn missing_password_disables_http_authentication() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, _action_rx) = mpsc::channel(16);
    let app = create_router(AppState::new(AppConfig::default(), db, action_tx));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/auth/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["data"]["initialized"], false);
    assert_eq!(json["data"]["authenticated"], true);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn missing_password_allows_tokenless_websockets() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, _action_rx) = mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db, action_tx);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server =
        tokio::spawn(async move { axum::serve(listener, create_router(state)).await.unwrap() });

    let mut request = format!("ws://{}/ws", address)
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "auth".parse().unwrap());
    let (mut socket, response) = connect_async(request).await.unwrap();
    assert_eq!(response.headers()["Sec-WebSocket-Protocol"], "auth");
    let message = socket.next().await.unwrap().unwrap();
    assert!(message
        .into_text()
        .unwrap()
        .contains("\"type\":\"snapshot\""));
    server.abort();
}

#[test]
fn test_default_server_binding_is_loopback() {
    let config = AppConfig::default();
    assert_eq!(config.server.host, "127.0.0.1");
    assert_eq!(config.server.port, 8080);
}

#[tokio::test]
async fn websocket_authenticates_without_a_query_token() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let token = "web-socket-session-token";
    db.create_session(token).unwrap();
    let (action_tx, mut action_rx) = mpsc::channel(1);
    let state = state_with_password(db.clone(), action_tx, "correct-password");
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
    let resume = "{\"action\":\"resume\",\"expected_mode\":\"PAPER\"}";
    socket
        .send(tokio_tungstenite::tungstenite::Message::Text(resume.into()))
        .await
        .unwrap();
    let action = tokio::time::timeout(std::time::Duration::from_secs(3), action_rx.recv())
        .await
        .expect("valid resume request must reach the control channel")
        .expect("control channel closed");
    match action {
        BotControlAction::Resume {
            expected_mode,
            reply,
            ..
        } => {
            assert_eq!(expected_mode, binance_grid_bot::types::TradingMode::Paper);
            reply.send(Ok(())).unwrap();
        }
        _ => panic!("expected resume action"),
    }
    db.delete_session(token).unwrap();
    socket
        .send(tokio_tungstenite::tungstenite::Message::Text(resume.into()))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(tokio::time::Duration::from_millis(300), action_rx.recv())
            .await
            .is_err()
    );
    server.abort();
}
