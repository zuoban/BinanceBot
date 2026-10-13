use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use binance_grid_bot::config::AppConfig;
use binance_grid_bot::db::Database;
use binance_grid_bot::server::{create_router, AppState};
use binance_grid_bot::types::{GridStats, OrderSide, TradeRecord, TradingMode};
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::mpsc;
use tower::ServiceExt;

fn trade(id: usize, symbol: &str, mode: TradingMode) -> TradeRecord {
    TradeRecord {
        trade_id: format!("cleanup-{id}"),
        client_order_id: format!("gb_b_cleanup_{id}"),
        symbol: symbol.into(),
        mode,
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        realized_pnl: dec!(2),
        commission: dec!(0.1),
        pnl_verified: id != 0,
        is_maker: true,
        timestamp: Utc::now(),
        note: String::new(),
    }
}

#[tokio::test]
async fn cleanup_requires_auth_and_confirmation_then_clears_every_scope() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let mut stats = GridStats::default();
    for id in 0..65 {
        stats.total_trades += 1;
        stats.total_volume_usdc += dec!(100);
        db.insert_trade_with_stats(&trade(id, "SOLUSDC", TradingMode::Paper), &stats)
            .unwrap();
    }
    for (id, symbol, mode) in [
        (65, "SOLUSDC", TradingMode::Live),
        (66, "SOLUSDC", TradingMode::Testnet),
        (67, "ETHUSDT", TradingMode::Paper),
        (68, "SOLUSDC", TradingMode::Unknown),
    ] {
        db.insert_trade_with_stats(&trade(id, symbol, mode), &stats)
            .unwrap();
    }
    db.save_stats(&stats).unwrap();
    let config = AppConfig::default();
    db.save_config(&config).unwrap();
    let (action_tx, mut action_rx) = mpsc::channel(1);
    let state = AppState::new(config, db.clone(), action_tx);
    state.create_session("cleanup-token").await.unwrap();
    state.position.write().await.size = dec!(3);
    let status_before = *state.status.read().await;
    let account_before = serde_json::to_value(state.account.read().await.clone()).unwrap();
    let start_time = state.stats.read().await.start_time;
    let app = create_router(state.clone());
    // Warm the dashboard cache before cleanup; its response must invalidate it.
    assert_eq!(state.snapshot().await.stats.total_trades, 65);

    for (token, payload, expected) in [
        (None, json!({"confirm": true}), StatusCode::UNAUTHORIZED),
        (
            Some("invalid"),
            json!({"confirm": true}),
            StatusCode::UNAUTHORIZED,
        ),
        (Some("cleanup-token"), json!({}), StatusCode::BAD_REQUEST),
        (
            Some("cleanup-token"),
            json!({"confirm": false}),
            StatusCode::BAD_REQUEST,
        ),
        (
            Some("cleanup-token"),
            json!({"confirm": "true"}),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
    ] {
        let mut request = Request::builder()
            .method("POST")
            .uri("/api/trades/clear")
            .header("Content-Type", "application/json");
        if let Some(token) = token {
            request = request.header("Authorization", format!("Bearer {token}"));
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::from(payload.to_string())).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        assert_eq!(db.get_recent_trades(100).unwrap().len(), 69);
        assert_eq!(state.stats.read().await.total_trades, 65);
    }

    let mut events = state.ws_broadcast_tx.subscribe();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/trades/clear")
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer cleanup-token")
                .body(Body::from(r#"{"confirm":true}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(result["success"], true);
    assert_eq!(result["data"]["deleted_trades"], 69);
    assert_eq!(result["data"]["snapshot"]["recent_trades"], json!([]));
    assert_eq!(result["data"]["snapshot"]["stats"]["total_trades"], 0);
    assert!(result["data"]["snapshot"]["hourly_trade_stats"]["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .all(|bucket| bucket["buy_count"] == 0 && bucket["sell_count"] == 0));
    assert_eq!(state.position.read().await.size, dec!(3));
    assert_eq!(*state.status.read().await, status_before);
    assert_eq!(
        serde_json::to_value(state.account.read().await.clone()).unwrap(),
        account_before
    );
    assert_eq!(state.stats.read().await.start_time, start_time);
    assert!(db.load_config().unwrap().is_some());
    assert!(db.load_stats().unwrap().is_none());
    assert!(state.is_token_valid("cleanup-token").await);
    assert!(action_rx.try_recv().is_err());
    assert!(db.get_recent_trades(100).unwrap().is_empty());
    assert!(state.recent_trades.read().await.is_empty());
    for (symbol, mode) in [
        ("SOLUSDC", TradingMode::Paper),
        ("SOLUSDC", TradingMode::Live),
        ("SOLUSDC", TradingMode::Testnet),
        ("SOLUSDC", TradingMode::Unknown),
        ("ETHUSDT", TradingMode::Paper),
    ] {
        assert_eq!(
            db.last_fill_for(symbol, mode).unwrap(),
            Some((OrderSide::Buy, dec!(100)))
        );
    }
    let mut delayed_verification = trade(0, "SOLUSDC", TradingMode::Paper);
    delayed_verification.pnl_verified = true;
    state.verify_trade_pnl(delayed_verification).await.unwrap();
    assert_eq!(state.stats.read().await.total_realized_pnl, Decimal::ZERO);
    assert_eq!(state.stats.read().await.pending_pnl_trades, 0);
    let event: Value = serde_json::from_str(&events.try_recv().unwrap()).unwrap();
    assert_eq!(event["type"], "log");
    let event: Value = serde_json::from_str(&events.try_recv().unwrap()).unwrap();
    assert_eq!(event["type"], "snapshot");
    assert_eq!(event["data"]["recent_trades"], json!([]));

    state.config.write().await.exchange.dry_run = false;
    state.refresh_current_scope().await;
    assert_eq!(state.stats.read().await.total_trades, 0);
    state.config.write().await.exchange.dry_run = true;
    state.refresh_current_scope().await;
    // Replaying a cleared order must not reappear or alter the reset totals.
    assert!(
        state
            .record_trade(trade(0, "SOLUSDC", TradingMode::Paper), None, None, None)
            .await
    );
    assert!(state.recent_trades.read().await.is_empty());
    assert!(
        state
            .record_trade(trade(69, "SOLUSDC", TradingMode::Paper), None, None, None)
            .await
    );
    let snapshot = state.snapshot().await;
    assert_eq!(snapshot.stats.total_trades, 1);
    assert_eq!(snapshot.stats.total_realized_pnl, dec!(2));
    assert_eq!(snapshot.stats.total_commission, dec!(0.1));
    assert_eq!(snapshot.stats.total_volume_usdc, dec!(100));
    assert_eq!(snapshot.recent_trades.len(), 1);
    assert_eq!(
        snapshot
            .hourly_trade_stats
            .unwrap()
            .buckets
            .iter()
            .map(|hour| hour.buy_count + hour.sell_count)
            .sum::<u64>(),
        1
    );
    assert_eq!(
        db.get_pnl_totals("SOLUSDC", TradingMode::Live).unwrap(),
        (Decimal::ZERO, Decimal::ZERO, 0)
    );
}
