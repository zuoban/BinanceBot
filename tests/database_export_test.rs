use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
#[path = "common/auth.rs"]
mod auth;
#[path = "common/http.rs"]
mod http;
use binance_grid_bot::config::AppConfig;
use binance_grid_bot::db::Database;
use binance_grid_bot::server::create_router;
use std::sync::Arc;
use tokio::sync::mpsc;
use tower::ServiceExt;

#[tokio::test]
async fn database_export_requires_auth_and_returns_a_private_sqlite_attachment() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, _action_rx) = mpsc::channel(1);
    let state = auth::auth_state(AppConfig::default(), db.clone(), action_tx, "test-password");
    state
        .create_session("export-test-valid-token")
        .await
        .unwrap();
    let app = create_router(state.clone());

    for token in [None, Some("export-test-invalid-token")] {
        let mut request = Request::builder().uri("/api/database/export");
        if let Some(token) = token {
            request = request.header("Authorization", format!("Bearer {token}"));
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let json = http::body_json(response).await;
        assert_eq!(json["success"], false);
    }

    let before = *state.status.read().await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/database/export")
                .header("Authorization", "Bearer export-test-valid-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["Content-Type"],
        "application/vnd.sqlite3"
    );
    assert_eq!(response.headers()["Cache-Control"], "no-store");
    assert_eq!(response.headers()["X-Content-Type-Options"], "nosniff");
    let attachment = response.headers()["Content-Disposition"].to_str().unwrap();
    assert!(attachment.starts_with("attachment; filename=\"binancebot-analysis-"));
    assert!(attachment.ends_with(".db\""));
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(bytes.starts_with(b"SQLite format 3\0"));
    assert!(!String::from_utf8_lossy(&bytes).contains("export-test-valid-token"));
    assert_eq!(*state.status.read().await, before);
    assert!(state.is_token_valid("export-test-valid-token").await);
}
