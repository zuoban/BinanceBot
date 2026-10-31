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

fn cleanup_trade(id: &str, symbol: &str, mode: TradingMode) -> TradeRecord {
    TradeRecord {
        trade_id: format!("scoped-{id}"),
        client_order_id: format!("gb_b_scoped_{id}"),
        symbol: symbol.into(),
        mode,
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
    }
}

#[tokio::test]
async fn pair_cleanup_and_export_cover_all_modes_without_affecting_other_pairs() {
    let (db, states, _) = fixture();
    let sol = cleanup_trade("sol-paper", "SOLUSDC", TradingMode::Paper);
    let btc = cleanup_trade("btc-paper", "BTCUSDT", TradingMode::Paper);
    assert!(states[0].record_trade(sol.clone(), None, None, None).await);
    assert!(states[1].record_trade(btc.clone(), None, None, None).await);
    for (id, mode) in [
        ("sol-live", TradingMode::Live),
        ("sol-testnet", TradingMode::Testnet),
        ("sol-unknown", TradingMode::Unknown),
    ] {
        assert!(db
            .insert_trade_with_stats(&cleanup_trade(id, "SOLUSDC", mode), &Default::default())
            .unwrap());
    }
    let other_before = serde_json::to_value(states[1].snapshot().await).unwrap();
    let router = create_multi_router(states.clone());
    for (path, body) in [
        ("/api/pairs/SOLUSDC/database/export", None),
        (
            "/api/pairs/SOLUSDC/trades/clear",
            Some(json!({"confirm":true})),
        ),
    ] {
        assert_eq!(
            router
                .clone()
                .oneshot(request(path, body, false))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    for body in [json!({}), json!({"confirm":false})] {
        assert_eq!(
            router
                .clone()
                .oneshot(request("/api/pairs/SOLUSDC/trades/clear", Some(body), true))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    for path in [
        "/api/pairs/UNKNOWN/database/export",
        "/api/pairs/UNKNOWN/trades/clear",
    ] {
        let body = path.ends_with("clear").then(|| json!({"confirm":true}));
        assert_eq!(
            router
                .clone()
                .oneshot(request(path, body, true))
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
    }
    let response = router
        .clone()
        .oneshot(request("/api/pairs/SOLUSDC/database/export", None, true))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let path = std::env::temp_dir().join(format!("pair-export-{}.db", uuid::Uuid::new_v4()));
    std::fs::write(&path, bytes).unwrap();
    let exported = rusqlite::Connection::open(&path).unwrap();
    let rows: Vec<(String, String)> = exported
        .prepare("SELECT symbol, mode FROM trades ORDER BY mode")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(rows.len(), 4);
    assert!(rows.iter().all(|(symbol, _)| symbol == "SOLUSDC"));
    for mode in ["PAPER", "LIVE", "TESTNET", "UNKNOWN"] {
        assert!(rows.iter().any(|(_, value)| value == mode));
    }
    drop(exported);
    std::fs::remove_file(path).unwrap();
    let response = router
        .oneshot(request(
            "/api/pairs/SOLUSDC/trades/clear",
            Some(json!({"confirm":true})),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let result = response_json(response).await;
    assert_eq!(result["data"]["deleted_trades"], 4);
    let remaining = db.get_recent_trades(100).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].trade_id, btc.trade_id);
    let other_after = serde_json::to_value(states[1].snapshot().await).unwrap();
    assert_eq!(other_after["stats"], other_before["stats"]);
    assert_eq!(other_after["recent_trades"], other_before["recent_trades"]);
    assert!(states[0].record_trade(sol, None, None, None).await);
    assert!(states[1].record_trade(btc, None, None, None).await);
    assert_eq!(db.get_recent_trades(100).unwrap().len(), 1);
    for mode in [
        TradingMode::Paper,
        TradingMode::Live,
        TradingMode::Testnet,
        TradingMode::Unknown,
    ] {
        assert_eq!(
            db.last_fill_for("SOLUSDC", mode).unwrap(),
            Some((OrderSide::Buy, dec!(100)))
        );
    }
    assert_eq!(
        db.last_fill_for("BTCUSDT", TradingMode::Paper).unwrap(),
        Some((OrderSide::Buy, dec!(100)))
    );
}

#[tokio::test]
async fn global_database_targets_are_whitelisted_and_preserve_recovery_data() {
    let (db, states, _) = fixture();
    let sol = cleanup_trade("global-sol", "SOLUSDC", TradingMode::Paper);
    let btc = cleanup_trade("global-btc", "BTCUSDT", TradingMode::Paper);
    assert!(states[0].record_trade(sol.clone(), None, None, None).await);
    assert!(states[1].record_trade(btc.clone(), None, None, None).await);
    db.save_hourly_account_snapshot(Utc::now().timestamp(), &[])
        .unwrap();
    let router = create_multi_router(states.clone());
    let targets = json!(["trade_history", "account_hourly_snapshots"]);
    for path in ["/api/database/export", "/api/database/clear"] {
        assert_eq!(
            router
                .clone()
                .oneshot(request(
                    path,
                    Some(json!({"targets": targets, "confirm":true})),
                    false
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        for invalid in [
            json!([]),
            json!(["app_config"]),
            json!(["trade_history", "auth_sessions"]),
        ] {
            let response = router
                .clone()
                .oneshot(request(
                    path,
                    Some(json!({"targets":invalid,"confirm":true})),
                    true,
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(db.get_recent_trades(100).unwrap().len(), 2);
            assert_eq!(db.hourly_account_snapshots(0).unwrap().len(), 1);
        }
    }
    for body in [
        json!({"targets":targets}),
        json!({"targets":targets,"confirm":false}),
    ] {
        assert_eq!(
            router
                .clone()
                .oneshot(request("/api/database/clear", Some(body), true))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    let response = router
        .clone()
        .oneshot(request(
            "/api/database/export",
            Some(json!({"targets":["account_hourly_snapshots"]})),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let path =
        std::env::temp_dir().join(format!("single-target-export-{}.db", uuid::Uuid::new_v4()));
    std::fs::write(&path, bytes).unwrap();
    let exported = rusqlite::Connection::open(&path).unwrap();
    let trade_count: i64 = exported
        .query_row("SELECT COUNT(*) FROM trades", [], |row| row.get(0))
        .unwrap();
    let snapshot_count: i64 = exported
        .query_row("SELECT COUNT(*) FROM account_hourly_snapshots", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!((trade_count, snapshot_count), (0, 1));
    drop(exported);
    std::fs::remove_file(path).unwrap();
    let response = router
        .clone()
        .oneshot(request(
            "/api/database/clear",
            Some(json!({"targets":["account_hourly_snapshots"],"confirm":true})),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(db.get_recent_trades(100).unwrap().len(), 2);
    assert!(db.hourly_account_snapshots(0).unwrap().is_empty());
    db.save_hourly_account_snapshot(Utc::now().timestamp(), &[])
        .unwrap();
    let response = router
        .clone()
        .oneshot(request(
            "/api/database/export",
            Some(json!({"targets":targets})),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let path = std::env::temp_dir().join(format!("target-export-{}.db", uuid::Uuid::new_v4()));
    std::fs::write(&path, bytes).unwrap();
    let exported = rusqlite::Connection::open(&path).unwrap();
    for (table, expected) in [
        ("trades", 2),
        ("account_hourly_snapshots", 1),
        ("app_config", 0),
        ("auth_sessions", 0),
    ] {
        let count: i64 = exported
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, expected, "{table}");
    }
    drop(exported);
    std::fs::remove_file(path).unwrap();
    let response = router
        .oneshot(request(
            "/api/database/clear",
            Some(json!({"targets":targets,"confirm":true})),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let result = response_json(response).await;
    assert_eq!(result["data"]["deleted_trades"], 2);
    assert_eq!(result["data"]["deleted_account_hourly_snapshots"], 1);
    assert!(db.get_recent_trades(100).unwrap().is_empty());
    assert!(db.hourly_account_snapshots(0).unwrap().is_empty());
    assert!(db.load_config().unwrap().is_some());
    assert!(states[0].is_token_valid("multi-pair-token").await);
    for (state, trade) in states.iter().zip([sol, btc]) {
        assert_eq!(
            db.last_fill_for(&trade.symbol, trade.mode).unwrap(),
            Some((OrderSide::Buy, dec!(100)))
        );
        assert!(state.record_trade(trade, None, None, None).await);
        assert!(state.snapshot().await.recent_trades.is_empty());
    }
    assert!(db.get_recent_trades(100).unwrap().is_empty());
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
