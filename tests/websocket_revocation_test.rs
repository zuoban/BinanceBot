mod common;
use common::auth;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use binance_grid_bot::auth::AdminAuth;
use binance_grid_bot::config::{AppConfig, TradingPairConfig};
use binance_grid_bot::db::Database;
use binance_grid_bot::server::{create_multi_router, create_router, AppState};
use futures_util::StreamExt;
use rust_decimal_macros::dec;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
    MaybeTlsStream, WebSocketStream,
};

fn test_auth() -> Option<Arc<AdminAuth>> {
    Some(Arc::new(AdminAuth::from_password("test-password").unwrap()))
}
use tower::ServiceExt;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn connect(address: std::net::SocketAddr, path: &str, token: &str) -> Socket {
    let mut request = format!("ws://{address}{path}")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        format!("auth, token.{token}").parse().unwrap(),
    );
    connect_async(request).await.unwrap().0
}

async fn next_json(socket: &mut Socket) -> Value {
    let frame = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .expect("timed out waiting for WebSocket data")
        .expect("WebSocket closed before sending data")
        .expect("WebSocket failed before sending data");
    serde_json::from_str(&frame.into_text().unwrap()).unwrap()
}

async fn assert_no_sensitive_data(socket: &mut Socket) {
    match tokio::time::timeout(Duration::from_millis(1500), socket.next()).await {
        Err(_) | Ok(None) | Ok(Some(Err(_))) | Ok(Some(Ok(Message::Close(_)))) => {}
        Ok(Some(Ok(message))) => panic!("revoked WebSocket received a frame: {message:?}"),
    }
}

async fn assert_account_marker(socket: &mut Socket, should_receive: bool) {
    let result = tokio::time::timeout(Duration::from_millis(2500), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Text(text))) => {
                    if text.contains("54321") {
                        return true;
                    }
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return false,
                Some(Ok(_)) => {}
            }
        }
    })
    .await
    .unwrap_or(false);
    assert_eq!(
        result, should_receive,
        "unexpected account data after revocation"
    );
}
async fn revocation_scenario(multi: bool) {
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("first-session").unwrap();
    db.create_session("other-session").unwrap();

    let (router, states, paths) = if multi {
        let config = AppConfig {
            pairs: ["SOLUSDC", "BTCUSDT"]
                .map(|symbol| TradingPairConfig::new(symbol.into()))
                .to_vec(),
            ..Default::default()
        };
        db.save_config(&config).unwrap();
        let lock = Arc::new(Mutex::new(()));
        let states: Vec<_> = config
            .strategy_configs()
            .unwrap()
            .into_iter()
            .map(|config| {
                let (tx, _rx) = mpsc::channel(8);
                AppState::new_for_pair_with_admin_auth(
                    config,
                    db.clone(),
                    tx,
                    lock.clone(),
                    test_auth(),
                )
            })
            .collect();
        (
            create_multi_router(states.clone()),
            states,
            vec![
                "/ws",
                "/api/pairs/SOLUSDC/ws",
                "/api/pairs/BTCUSDT/ws",
                "/api/accounts/ws",
            ],
        )
    } else {
        let (tx, _rx) = mpsc::channel(8);
        let state = auth::auth_state(AppConfig::default(), db.clone(), tx, "test-password");
        (
            create_router(state.clone()),
            vec![state],
            vec!["/ws", "/api/accounts/ws"],
        )
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_router = router.clone();
    let server = tokio::spawn(async move { axum::serve(listener, server_router).await.unwrap() });

    let mut first = Vec::new();
    let mut other = Vec::new();
    for path in &paths {
        let mut first_socket = connect(address, path, "first-session").await;
        let mut other_socket = connect(address, path, "other-session").await;
        let expected = if *path == "/api/accounts/ws" {
            "accounts"
        } else {
            "snapshot"
        };
        assert_eq!(next_json(&mut first_socket).await["type"], expected);
        assert_eq!(next_json(&mut other_socket).await["type"], expected);
        first.push(first_socket);
        other.push(other_socket);
    }

    // Exercise the actual broadcast path before revocation, including both pair scopes.
    for (index, state) in states.iter().enumerate() {
        let payload = json!({"type":"log", "data":{"message":format!("before-{index}")}});
        state
            .ws_broadcast_tx
            .send(payload.to_string().into())
            .unwrap();
    }
    for (index, path) in paths.iter().enumerate() {
        if *path == "/api/accounts/ws" {
            continue;
        }
        let state_index = usize::from(path.contains("BTCUSDT"));
        let expected = format!("before-{state_index}");
        assert_eq!(
            next_json(&mut first[index]).await["data"]["message"],
            expected
        );
        assert_eq!(
            next_json(&mut other[index]).await["data"]["message"],
            expected
        );
    }

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/logout")
                .header("Authorization", "Bearer first-session")
                .header("Content-Type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Logout must leave the other session usable. Previously queued account
    // frames can predate the HTTP response. Change a field afterwards so only
    // newly generated account data carries this marker.
    states[0].position.write().await.unrealized_pnl = dec!(54321);

    for (index, state) in states.iter().enumerate() {
        let payload = json!({"type":"log", "data":{"message":format!("after-{index}")}});
        state
            .ws_broadcast_tx
            .send(payload.to_string().into())
            .unwrap();
    }
    for (index, path) in paths.iter().enumerate() {
        if *path == "/api/accounts/ws" {
            continue;
        }
        let state_index = usize::from(path.contains("BTCUSDT"));
        let expected = format!("after-{state_index}");
        assert_eq!(
            next_json(&mut other[index]).await["data"]["message"],
            expected
        );
    }
    assert_account_marker(other.last_mut().unwrap(), true).await;
    for socket in &mut first[..paths.len() - 1] {
        assert_no_sensitive_data(socket).await;
    }
    assert_account_marker(first.last_mut().unwrap(), false).await;
    server.abort();
}

#[tokio::test]
async fn single_pair_logout_revokes_existing_websockets() {
    revocation_scenario(false).await;
}

#[tokio::test]
async fn multi_pair_logout_revokes_existing_websockets() {
    revocation_scenario(true).await;
}
