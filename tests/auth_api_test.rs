use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use binance_grid_bot::config::AppConfig;
use binance_grid_bot::db::Database;
use binance_grid_bot::server::{create_router, AppState};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc;
use tower::ServiceExt;

#[tokio::test]
async fn test_full_auth_lifecycle() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let config = AppConfig::default();
    let (action_tx, _action_rx) = mpsc::channel(16);
    let state = AppState::new(config, db.clone(), action_tx);
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
                .body(Body::from(r#"{"password": "MySuperSecret123"}"#))
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
