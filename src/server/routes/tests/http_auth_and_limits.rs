use super::super::*;
use super::auth_state;
use crate::config::AppConfig;
use crate::db::Database;

#[tokio::test]
async fn body_limits_reject_oversized_login_and_protected_payloads() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("body-limit-token").unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let state = auth_state(AppConfig::default(), db, tx);
    let connect_info =
        axum::extract::ConnectInfo(std::net::SocketAddr::from(([127, 0, 0, 1], 12345)));
    let router = create_router(state).layer(Extension(connect_info));

    let oversized_login = serde_json::json!({
        "password": "x".repeat(5 * 1024),
    })
    .to_string();
    let login_res = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(oversized_login))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login_res.status(), StatusCode::PAYLOAD_TOO_LARGE);

    // Authenticated API routes allow >4 KiB up to 64 KiB, then reject >64 KiB.
    let medium_control = serde_json::json!({
        "action": format!("unknown_{}", "a".repeat(8 * 1024)),
    })
    .to_string();
    let medium_res = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/api/control")
                .header("authorization", "Bearer body-limit-token")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(medium_control))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(medium_res.status(), StatusCode::OK);
    let too_many_targets = serde_json::json!({
        "targets": ["trade_history", "account_hourly_snapshots", "trade_history"],
    })
    .to_string();
    let bad_targets_res = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/api/database/export")
                .header("authorization", "Bearer body-limit-token")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(too_many_targets))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bad_targets_res.status(), StatusCode::BAD_REQUEST);

    let oversized_control = serde_json::json!({
        "action": format!("unknown_{}", "a".repeat(70 * 1024)),
    })
    .to_string();
    let oversized_res = router
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/api/control")
                .header("authorization", "Bearer body-limit-token")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(oversized_control))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(oversized_res.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn cold_token_lookups_are_bounded_and_malformed_tokens_skip_sqlite() {
    use axum::body::{to_bytes, Body};
    use axum::http::Request;

    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("hot-valid-token").unwrap();
    db.create_session("cold-valid-token").unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let state = auth_state(AppConfig::default(), db.clone(), tx);
    let app = create_router(state.clone());

    // Warm "hot-valid-token" into the in-memory session cache.
    assert_eq!(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri("/api/status")
                    .header("Authorization", "Bearer hot-valid-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let permits: Vec<_> = (0..16)
        .map(|_| state.auth.try_acquire_session_lookup_permit().unwrap())
        .collect();
    assert!(state.auth.try_acquire_session_lookup_permit().is_none());

    // Hot cached tokens still succeed without acquiring a cold lookup permit.
    assert_eq!(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri("/api/status")
                    .header("Authorization", "Bearer hot-valid-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // Cold tokens return 503 when all 16 cold lookup permits are busy.
    assert_eq!(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri("/api/status")
                    .header("Authorization", "Bearer cold-valid-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri("/api/auth/status")
                    .header("Authorization", "Bearer cold-valid-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );

    // Oversized (>128) or non-ASCII-graphic tokens are rejected before touching AuthContext.
    let oversized = "a".repeat(129);
    for bad in [oversized.as_str(), "bad token with space"] {
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/api/status")
                        .header("Authorization", format!("Bearer {bad}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let status_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/auth/status")
                    .header("Authorization", format!("Bearer {bad}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status_res.status(), StatusCode::OK);
        let bytes = to_bytes(status_res.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["data"]["authenticated"], false);
    }

    // Duplicate Authorization or X-Auth-Token headers are rejected.
    let mut dup_auth_req = Request::builder()
        .uri("/api/status")
        .body(Body::empty())
        .unwrap();
    dup_auth_req
        .headers_mut()
        .append("Authorization", "Bearer hot-valid-token".parse().unwrap());
    dup_auth_req
        .headers_mut()
        .append("Authorization", "Bearer hot-valid-token".parse().unwrap());
    assert_eq!(
        app.clone().oneshot(dup_auth_req).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let mut dup_x_req = Request::builder()
        .uri("/api/status")
        .body(Body::empty())
        .unwrap();
    dup_x_req
        .headers_mut()
        .append("X-Auth-Token", "hot-valid-token".parse().unwrap());
    dup_x_req
        .headers_mut()
        .append("X-Auth-Token", "hot-valid-token".parse().unwrap());
    assert_eq!(
        app.clone().oneshot(dup_x_req).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    drop(permits);
    assert_eq!(
        app.oneshot(
            Request::builder()
                .uri("/api/status")
                .header("Authorization", "Bearer cold-valid-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn routers_attach_security_headers_without_touching_websocket_upgrades() {
    use axum::body::Body;
    use axum::http::Request;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let db = Arc::new(Database::open(":memory:").unwrap());
    let mut config = AppConfig::default();
    config.ensure_pair_list().unwrap();
    db.save_config(&config).unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let state = AppState::new(config.clone(), db.clone(), tx);
    let mut runtime = crate::runtime::TradingRuntime::start_with_test_exchange(
        &config,
        db,
        "http://127.0.0.1:1".into(),
    )
    .await
    .unwrap();
    let managed = create_managed_router(runtime.handle());
    for router in [
        create_router(state.clone()),
        create_multi_router(vec![state.clone()]),
        create_multi_router(Vec::new()),
        managed,
    ] {
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/auth/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let headers = response.headers();
        assert_eq!(headers["Cache-Control"], "no-store");
        assert_eq!(headers["X-Content-Type-Options"], "nosniff");
        assert_eq!(headers["X-Frame-Options"], "DENY");
        assert_eq!(headers["Referrer-Policy"], "same-origin");
        assert_eq!(
            headers["Permissions-Policy"],
            "accelerometer=(), camera=(), geolocation=(), gyroscope=(), magnetometer=(), microphone=(), payment=(), usb=()"
        );
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, create_router(state)).await.unwrap();
    });
    let mut ws_req = format!("ws://{address}/ws").into_client_request().unwrap();
    ws_req
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "auth".parse().unwrap());
    let (_socket, upgrade_res) = tokio_tungstenite::connect_async(ws_req).await.unwrap();
    assert_eq!(upgrade_res.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert!(!upgrade_res.headers().contains_key("X-Frame-Options"));
    server.abort();
    let _ = runtime.shutdown(std::time::Duration::from_secs(2)).await;
}

#[tokio::test]
async fn shared_proxy_ip_successful_login_preserves_prior_failed_attempts_and_disabled_auth_is_isolated(
) {
    use axum::{body::Body, http::Request};
    let unconfigured_db = Arc::new(Database::open(":memory:").unwrap());
    let (unconfigured_tx, _unconfigured_rx) = tokio::sync::mpsc::channel(1);
    let unconfigured_state = AppState::new(AppConfig::default(), unconfigured_db, unconfigured_tx);
    let unconfigured_app = create_router(unconfigured_state.clone()).layer(axum::Extension(
        axum::extract::ConnectInfo(
            "198.51.100.10:4000"
                .parse::<std::net::SocketAddr>()
                .unwrap(),
        ),
    ));
    let disabled_probe = Request::builder()
        .method("POST")
        .uri("/api/auth/login")
        .header("Content-Type", "application/json")
        .body(Body::from(r#"{"password":"probe"}"#))
        .unwrap();
    assert_eq!(
        unconfigured_app
            .oneshot(disabled_probe)
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    {
        let failures = unconfigured_state.auth.login_failures.lock().await;
        assert!(failures.by_source.is_empty());
        assert!(failures.global.is_empty());
    }

    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let state = AppState::new_with_admin_auth(
        AppConfig::default(),
        db,
        tx,
        Some(Arc::new(
            crate::auth::AdminAuth::from_password("admin-secret-pw").unwrap(),
        )),
    );
    let proxy_addr: std::net::SocketAddr = "198.51.100.20:5000".parse().unwrap();
    let proxy_ip = proxy_addr.ip();
    let app =
        create_router(state.clone()).layer(axum::Extension(axum::extract::ConnectInfo(proxy_addr)));
    let login_req = |pw: &str| {
        Request::builder()
            .method("POST")
            .uri("/api/auth/login")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::json!({"password": pw}).to_string()))
            .unwrap()
    };

    // 5 failed attempts behind shared proxy IP
    for _ in 0..5 {
        let res = app.clone().oneshot(login_req("wrong-pw")).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }
    assert_eq!(
        state.auth.login_failures.lock().await.by_source[&proxy_ip].len(),
        5
    );

    // 1 valid login behind the same shared proxy IP rolls back ONLY its own attempt
    let ok_res = app
        .clone()
        .oneshot(login_req("admin-secret-pw"))
        .await
        .unwrap();
    assert_eq!(ok_res.status(), StatusCode::OK);
    {
        let failures = state.auth.login_failures.lock().await;
        assert_eq!(failures.by_source[&proxy_ip].len(), 5);
        assert_eq!(failures.global.len(), 5);
    }

    // 5 more failed attempts hit the 10-attempt source cap (proving prior 5 were not wiped)
    for _ in 0..5 {
        let res = app.clone().oneshot(login_req("wrong-pw")).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }
    let blocked_res = app.oneshot(login_req("wrong-pw")).await.unwrap();
    assert_eq!(blocked_res.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn trusted_proxy_x_real_ip_isolates_clients_while_untrusted_peers_cannot_spoof() {
    use axum::{body::Body, http::Request};
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let state = AppState::new_with_admin_auth(
        AppConfig::default(),
        db,
        tx,
        Some(Arc::new(
            crate::auth::AdminAuth::from_password("admin-secret-pw").unwrap(),
        )),
    );
    state
        .auth
        .set_trusted_proxies(crate::auth::TrustedProxies::parse("172.18.0.0/16").unwrap());

    let untrusted_peer: std::net::SocketAddr = "198.51.100.99:5000".parse().unwrap();
    let untrusted_app = create_router(state.clone())
        .layer(axum::Extension(axum::extract::ConnectInfo(untrusted_peer)));
    for idx in 0..10 {
        let req = Request::builder()
            .method("POST")
            .uri("/api/auth/login")
            .header("Content-Type", "application/json")
            .header("X-Real-IP", format!("203.0.113.{idx}"))
            .body(Body::from(r#"{"password":"wrong"}"#))
            .unwrap();
        assert_eq!(
            untrusted_app.clone().oneshot(req).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }
    let spoofed_11th = Request::builder()
        .method("POST")
        .uri("/api/auth/login")
        .header("Content-Type", "application/json")
        .header("X-Real-IP", "203.0.113.250")
        .body(Body::from(r#"{"password":"wrong"}"#))
        .unwrap();
    assert_eq!(
        untrusted_app.oneshot(spoofed_11th).await.unwrap().status(),
        StatusCode::TOO_MANY_REQUESTS
    );

    let trusted_peer: std::net::SocketAddr = "172.18.0.2:40000".parse().unwrap();
    let trusted_app = create_router(state.clone())
        .layer(axum::Extension(axum::extract::ConnectInfo(trusted_peer)));
    for _ in 0..10 {
        let req = Request::builder()
            .method("POST")
            .uri("/api/auth/login")
            .header("Content-Type", "application/json")
            .header("X-Real-IP", "203.0.113.10")
            .body(Body::from(r#"{"password":"wrong"}"#))
            .unwrap();
        assert_eq!(
            trusted_app.clone().oneshot(req).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }
    let blocked_attacker = Request::builder()
        .method("POST")
        .uri("/api/auth/login")
        .header("Content-Type", "application/json")
        .header("X-Real-IP", "203.0.113.10")
        .body(Body::from(r#"{"password":"admin-secret-pw"}"#))
        .unwrap();
    assert_eq!(
        trusted_app
            .clone()
            .oneshot(blocked_attacker)
            .await
            .unwrap()
            .status(),
        StatusCode::TOO_MANY_REQUESTS
    );

    let legitimate_other_client = Request::builder()
        .method("POST")
        .uri("/api/auth/login")
        .header("Content-Type", "application/json")
        .header("X-Real-IP", "203.0.113.11")
        .body(Body::from(r#"{"password":"admin-secret-pw"}"#))
        .unwrap();
    assert_eq!(
        trusted_app
            .clone()
            .oneshot(legitimate_other_client)
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let duplicate_headers = Request::builder()
        .method("POST")
        .uri("/api/auth/login")
        .header("Content-Type", "application/json")
        .header("X-Real-IP", "203.0.113.20")
        .header("X-Real-IP", "203.0.113.21")
        .body(Body::from(r#"{"password":"wrong"}"#))
        .unwrap();
    assert_eq!(
        trusted_app
            .oneshot(duplicate_headers)
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert!(state
        .auth
        .login_failures
        .lock()
        .await
        .by_source
        .contains_key(&trusted_peer.ip()));
}
