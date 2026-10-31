use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use binance_grid_bot::config::{AppConfig, TradingPairConfig};
use binance_grid_bot::db::Database;
use binance_grid_bot::server::{create_multi_router, AppState};
use binance_grid_bot::types::{BotControlAction, BotStatus, OrderSide, TradeRecord, TradingMode};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use rust_decimal_macros::dec;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};
use tower::ServiceExt;

fn fixture() -> (
    Arc<Database>,
    Vec<Arc<AppState>>,
    Vec<mpsc::Receiver<BotControlAction>>,
) {
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("multi-pair-token").unwrap();
    let config = AppConfig {
        pairs: ["SOLUSDC", "BTCUSDT"]
            .map(|s| TradingPairConfig::new(s.into()))
            .to_vec(),
        ..Default::default()
    };
    db.save_config(&config).unwrap();
    let lock = Arc::new(Mutex::new(()));
    let mut states = Vec::new();
    let mut receivers = Vec::new();
    for config in config.strategy_configs().unwrap() {
        let (tx, rx) = mpsc::channel(8);
        states.push(AppState::new_for_pair(config, db.clone(), tx, lock.clone()));
        receivers.push(rx);
    }
    (db, states, receivers)
}

fn request(path: &str, body: Option<Value>, authorized: bool) -> Request<Body> {
    let mut builder = Request::builder().uri(path);
    if authorized {
        builder = builder.header("Authorization", "Bearer multi-pair-token");
    }
    if body.is_some() {
        builder = builder
            .method("POST")
            .header("Content-Type", "application/json");
    }
    builder
        .body(body.map_or(Body::empty(), |value| Body::from(value.to_string())))
        .unwrap()
}

async fn response_json(response: axum::response::Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

#[tokio::test]
async fn api_routes_authenticate_and_dispatch_to_the_selected_pair_only() {
    let (db, states, mut actions) = fixture();
    let router = create_multi_router(states.clone());
    for path in [
        "/api/pairs",
        "/api/pairs/BTCUSDT/status",
        "/api/pairs/BTCUSDT/config",
    ] {
        assert_eq!(
            router
                .clone()
                .oneshot(request(path, None, false))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    let list = response_json(
        router
            .clone()
            .oneshot(request("/api/pairs", None, true))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(list["data"][0]["symbol"], "SOLUSDC");
    assert_eq!(list["data"][1]["symbol"], "BTCUSDT");
    assert_eq!(list["data"][1]["scoped"], true);
    for (path, symbol) in [
        ("/api/status", "SOLUSDC"),
        ("/api/pairs/BTCUSDT/status", "BTCUSDT"),
    ] {
        let result = response_json(
            router
                .clone()
                .oneshot(request(path, None, true))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(result["data"]["symbol"], symbol);
    }
    let result = response_json(
        router
            .clone()
            .oneshot(request(
                "/api/pairs/BTCUSDT/control",
                Some(json!({"action":"pause"})),
                true,
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(result["success"], true);
    assert!(matches!(
        actions[1].try_recv().unwrap(),
        BotControlAction::Pause
    ));
    assert!(actions[0].try_recv().is_err());
    assert_eq!(*states[0].status.read().await, BotStatus::Running);
    assert_eq!(*states[1].status.read().await, BotStatus::Paused);
    assert_eq!(
        db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
        Some(BotStatus::Paused)
    );
    assert_eq!(
        router
            .oneshot(request("/api/pairs/UNKNOWN/status", None, true))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn pair_config_versions_reject_concurrent_and_stale_updates_across_legacy_routes() {
    let (db, states, mut receivers) = fixture();
    let mut rx = receivers.remove(0);
    let engine_state = states[0].clone();
    let engine = tokio::spawn(async move {
        while let Some(BotControlAction::UpdateConfig { config, reply }) = rx.recv().await {
            tokio::task::yield_now().await;
            engine_state
                .db
                .save_pair_config("SOLUSDC", &config)
                .unwrap();
            *engine_state.config.write().await = *config;
            let _ = reply.send(Ok(()));
        }
    });
    let router = create_multi_router(states);
    let scoped = "/api/pairs/SOLUSDC/config";
    let legacy = "/api/config";
    let before = response_json(
        router
            .clone()
            .oneshot(request(scoped, None, true))
            .await
            .unwrap(),
    )
    .await;
    let legacy_before = response_json(
        router
            .clone()
            .oneshot(request(legacy, None, true))
            .await
            .unwrap(),
    )
    .await;
    let version = before["data"]["config_version"].as_str().unwrap();
    assert_eq!(version, legacy_before["data"]["config_version"]);
    let payload = |interval: &str| {
        json!({
            "symbol": "SOLUSDC", "grid_interval": interval, "order_amount_usdc": "100",
            "buy_window": 5, "sell_window": 5, "baseline_version": version
        })
    };
    let (first, second) = tokio::join!(
        router
            .clone()
            .oneshot(request(scoped, Some(payload("0.2")), true)),
        router
            .clone()
            .oneshot(request(legacy, Some(payload("0.3")), true))
    );
    let statuses = [first.unwrap().status(), second.unwrap().status()];
    assert!(statuses.contains(&StatusCode::OK));
    assert!(statuses.contains(&StatusCode::CONFLICT));
    let after = response_json(
        router
            .clone()
            .oneshot(request(legacy, None, true))
            .await
            .unwrap(),
    )
    .await;
    assert_ne!(after["data"]["config_version"], version);
    let stale = router
        .clone()
        .oneshot(request(scoped, Some(payload("0.4")), true))
        .await
        .unwrap();
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    let persisted = db
        .load_config()
        .unwrap()
        .unwrap()
        .strategy_configs()
        .unwrap()
        .remove(0);
    assert_eq!(
        persisted.grid.grid_interval.to_string(),
        after["data"]["grid_interval"].as_str().unwrap()
    );
    for path in [scoped, legacy] {
        for baseline in [None, Some(json!(null)), Some(json!("")), Some(json!("  "))] {
            let mut invalid = payload("0.5");
            if let Some(baseline) = baseline {
                invalid["baseline_version"] = baseline;
            } else {
                invalid.as_object_mut().unwrap().remove("baseline_version");
            }
            let result = router
                .clone()
                .oneshot(request(path, Some(invalid), true))
                .await
                .unwrap();
            assert_eq!(result.status(), StatusCode::BAD_REQUEST);
            assert!(response_json(result).await["message"]
                .as_str()
                .unwrap()
                .contains("baseline_version"));
        }
    }
    let (_, replacement_states, _) = fixture();
    let replacement_router = create_multi_router(replacement_states);
    let replacement = response_json(
        replacement_router
            .clone()
            .oneshot(request(scoped, None, true))
            .await
            .unwrap(),
    )
    .await;
    assert_ne!(replacement["data"]["config_version"], version);
    let old_instance = replacement_router
        .oneshot(request(scoped, Some(payload("0.4")), true))
        .await
        .unwrap();
    assert_eq!(old_instance.status(), StatusCode::CONFLICT);
    engine.abort();
}

#[tokio::test]
async fn global_cleanup_resets_all_pair_caches_without_changing_their_statuses() {
    let (db, states, _) = fixture();
    for (index, state) in states.iter().enumerate() {
        let symbol = state.config.read().await.exchange.symbol.clone();
        let trade = TradeRecord {
            trade_id: format!("multi-{index}"),
            client_order_id: format!("gb_b_multi_{index}"),
            symbol,
            mode: TradingMode::Paper,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            realized_pnl: dec!(2),
            commission: dec!(0.1),
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: String::new(),
        };
        assert!(state.record_trade(trade, None, None, None).await);
        state.position.write().await.size = dec!(1);
    }
    states[1].pause_trading().await.unwrap();
    let router = create_multi_router(states.clone());
    let result = response_json(
        router
            .oneshot(request(
                "/api/trades/clear",
                Some(json!({"confirm":true})),
                true,
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(result["data"]["deleted_trades"], 2);
    assert!(db.get_recent_trades(100).unwrap().is_empty());
    for state in &states {
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.stats.total_trades, 0);
        assert!(snapshot.recent_trades.is_empty());
        assert_eq!(snapshot.position.size, dec!(1));
    }
    assert_eq!(*states[0].status.read().await, BotStatus::Running);
    assert_eq!(*states[1].status.read().await, BotStatus::Paused);
}

#[tokio::test]
async fn websocket_snapshot_and_controls_use_the_selected_pair() {
    let (_, states, mut actions) = fixture();
    let router = create_multi_router(states);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut request = format!("ws://{address}/api/pairs/BTCUSDT/ws")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        "auth, token.multi-pair-token".parse().unwrap(),
    );
    let (mut socket, _) = connect_async(request).await.unwrap();
    let message = socket.next().await.unwrap().unwrap().into_text().unwrap();
    let snapshot: Value = serde_json::from_str(&message).unwrap();
    assert_eq!(snapshot["data"]["symbol"], "BTCUSDT");
    socket
        .send(tokio_tungstenite::tungstenite::Message::Text(
            "{\"action\":\"pause\"}".into(),
        ))
        .await
        .unwrap();
    let action = tokio::time::timeout(std::time::Duration::from_secs(2), actions[1].recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(action, BotControlAction::Pause));
    assert!(actions[0].try_recv().is_err());
    socket.close(None).await.unwrap();
    server.abort();
}
