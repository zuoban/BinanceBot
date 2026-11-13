use super::*;
use crate::config::NtfyConfig;
use crate::types::{
    GridDirection, HourlyTradeCount, HourlyTradeStats, OrderSide, PositionInfo, TickerInfo,
    TradeRecord, TradingMode,
};
use chrono::Duration;
use rust_decimal_macros::dec;

fn sample_trade(side: OrderSide) -> TradeRecord {
    TradeRecord {
        trade_id: "1".into(),
        client_order_id: "order-1".into(),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        side,
        price: dec!(123.4500),
        quantity: dec!(2),
        amount_usdc: dec!(246.90),
        realized_pnl: dec!(3.25),
        commission: Decimal::ZERO,
        commission_asset: None,
        pnl_verified: true,
        is_maker: true,
        timestamp: DateTime::parse_from_rfc3339("2026-10-01T12:26:00Z")
            .unwrap()
            .with_timezone(&Utc),
        note: String::new(),
    }
}

#[test]
fn formats_ntfy_trade_risk_daily_and_test_messages_as_plain_title_and_body() {
    let trade = sample_trade(OrderSide::Sell);
    let start = trade.timestamp - Duration::hours(2);
    let stats = HourlyTradeStats {
        window_start: start,
        window_end: trade.timestamp,
        realized_pnl: Decimal::ZERO,
        pending_pnl_count: 0,
        buckets: vec![HourlyTradeCount {
            hour_start: trade.timestamp - Duration::minutes(10),
            buy_count: 4,
            sell_count: 3,
        }],
    };
    let context = TradeNotificationContext::for_trade(&trade, Some(&stats), trade.timestamp, 3)
        .with_position(
            &trade,
            &PositionInfo {
                symbol: "SOLUSDC".into(),
                size: dec!(10),
                mark_price: dec!(123.45),
                ..Default::default()
            },
            &TickerInfo::default(),
        )
        .with_direction(GridDirection::Long);

    let (title, body) = format_ntfy_trade_message(&trade, &context);
    assert_eq!(title, "🔴 SELL｜SOLUSDC｜123.45｜246.90 USDC");
    assert_eq!(
        body,
        "成交笔数：3D｜3H\n持仓金额：1234.50 USDC\n已实现盈亏：+3.2500 USDC"
    );

    let now = DateTime::parse_from_rfc3339("2026-10-11T18:26:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let (risk_title, risk_body) = format_ntfy_global_risk_message("余额 < 0 & 风控", now);
    assert_eq!(risk_title, "⚠️ 全局风控已触发");
    assert_eq!(
        risk_body,
        "原因：余额 < 0 & 风控\n时间：2026-10-12 02:26:00（北京时间）"
    );

    let (test_title, test_body) = format_ntfy_test_message(now);
    assert_eq!(test_title, "✅ Grid Console 测试消息");
    assert_eq!(
        test_body,
        "通知渠道可用。\n时间：2026-10-12 02:26:00（北京时间）"
    );

    let snapshot = crate::snapshots::DailySnapshot {
        date: "2026-11-09".into(),
        window_start: now - Duration::days(1),
        window_end: now,
        generated_at: now,
        accounts: Vec::new(),
        positions: Vec::new(),
        pair_positions: Vec::new(),
        trades: Vec::new(),
    };
    let (daily_title, daily_body) = format_ntfy_daily_snapshot_message(&snapshot);
    assert_eq!(daily_title, "📊 每日运行数据快照（2026-11-09）");
    assert!(daily_body.contains("【账户概览】\n暂无账户数据"));
    assert!(!daily_body.contains("<b>"));
}

#[test]
fn truncate_utf8_bytes_preserves_multibyte_boundaries_and_byte_budget() {
    let short = "🟢 正常短消息".to_string();
    assert_eq!(
        truncate_utf8_bytes(
            short.clone(),
            MAX_NTFY_MESSAGE_BYTES,
            MESSAGE_TRUNCATION_SUFFIX
        ),
        short
    );

    for pad in 0..8 {
        let oversized = format!("{}{}", "x".repeat(pad), "📊中文🟢".repeat(600));
        let out = truncate_utf8_bytes(oversized, MAX_NTFY_MESSAGE_BYTES, MESSAGE_TRUNCATION_SUFFIX);
        assert!(out.len() <= MAX_NTFY_MESSAGE_BYTES, "len={}", out.len());
        assert!(out.ends_with(MESSAGE_TRUNCATION_SUFFIX));
    }
}

#[tokio::test]
async fn send_message_supports_optional_bearer_auth_and_validates_inputs_and_responses() {
    use axum::{
        http::{HeaderMap, StatusCode},
        routing::post,
        Json, Router,
    };

    let notifier = NtfyNotifier::with_client(reqwest::Client::new());
    for (url, topic, token, expected) in [
        (
            "https://ntfy.sh/subpath",
            "grid_1",
            "",
            "ntfy 服务地址格式无效",
        ),
        ("ftp://ntfy.sh", "grid_1", "", "ntfy 服务地址格式无效"),
        ("https://ntfy.sh", "bad/topic", "", "ntfy Topic 格式无效"),
        ("https://ntfy.sh", "", "", "ntfy Topic 不能为空"),
        (
            "https://ntfy.sh",
            "grid_1",
            "bad token",
            "ntfy 访问令牌格式无效",
        ),
    ] {
        let cfg = NtfyConfig {
            enabled: true,
            server_url: url.into(),
            topic: topic.into(),
            token: token.into(),
        };
        let err = notifier
            .send_test_message(&cfg)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains(expected), "{err}");
    }

    let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel();
    let app = Router::new().route(
        "/",
        post(
            move |headers: HeaderMap, Json(body): Json<serde_json::Value>| {
                let tx = seen_tx.clone();
                async move {
                    let auth = headers
                        .get(axum::http::header::AUTHORIZATION)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();
                    let topic = body["topic"].as_str().unwrap_or("").to_string();
                    let _ = tx.send((auth.clone(), body));
                    match topic.as_str() {
                        "auth_topic" if auth != "Bearer tk_valid_token" => (
                            StatusCode::FORBIDDEN,
                            serde_json::json!({"code": 40301, "http": 403, "error": "unauthorized"})
                                .to_string(),
                        ),
                        "html_502" => (
                            StatusCode::BAD_GATEWAY,
                            "<!DOCTYPE html><html><body>502 Bad Gateway</body></html>".into(),
                        ),
                        "oversized" => (StatusCode::OK, "x".repeat(16 * 1024)),
                        "wrong_topic" => (
                            StatusCode::OK,
                            serde_json::json!({"id": "msg-1", "topic": "other_topic"}).to_string(),
                        ),
                        "missing_id" => (StatusCode::OK, "{}".into()),
                        _ => (
                            StatusCode::OK,
                            serde_json::json!({"id": "msg-ok", "event": "message", "topic": topic})
                                .to_string(),
                        ),
                    }
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // Public topic without token
    notifier
        .send_test_message(&NtfyConfig {
            enabled: true,
            server_url: server_url.clone(),
            topic: "public_grid".into(),
            token: String::new(),
        })
        .await
        .unwrap();
    let (auth, body) = seen_rx.recv().await.unwrap();
    assert_eq!(auth, "");
    assert_eq!(body["topic"], "public_grid");
    assert_eq!(body["priority"], 3);

    // Protected topic rejected without token, accepted with Bearer token
    let forbidden = notifier
        .send_test_message(&NtfyConfig {
            enabled: true,
            server_url: server_url.clone(),
            topic: "auth_topic".into(),
            token: String::new(),
        })
        .await
        .unwrap_err()
        .to_string();
    assert!(forbidden.contains("unauthorized"), "{forbidden}");

    notifier
        .send_global_risk_notification(
            &NtfyConfig {
                enabled: true,
                server_url: server_url.clone(),
                topic: "auth_topic".into(),
                token: "tk_valid_token".into(),
            },
            "保证金不足",
        )
        .await
        .unwrap();
    let _ = seen_rx.recv().await.unwrap();
    let (auth_ok, risk_body) = seen_rx.recv().await.unwrap();
    assert_eq!(auth_ok, "Bearer tk_valid_token");
    assert_eq!(risk_body["priority"], 4);

    // Gateway HTML, oversized, wrong topic, and missing id are all rejected cleanly
    for (topic, expected) in [
        ("html_502", "upstream HTML error response"),
        ("oversized", "ntfy returned an invalid response"),
        ("wrong_topic", "ntfy returned an invalid response"),
        ("missing_id", "ntfy returned an invalid response"),
    ] {
        let err = notifier
            .send_test_message(&NtfyConfig {
                enabled: true,
                server_url: server_url.clone(),
                topic: topic.into(),
                token: String::new(),
            })
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains(expected), "{topic}: {err}");
        assert!(!err.contains("<html"), "{topic}: {err}");
    }
    server.abort();
}
