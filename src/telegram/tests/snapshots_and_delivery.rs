use crate::config::TelegramConfig;
use crate::telegram::{format_daily_snapshot_message, truncate_telegram_html, TelegramNotifier};
use crate::types::{GridDirection, TradingMode};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[test]
fn formats_daily_snapshot_message_with_accounts_trades_pnl_and_positions() {
    let generated_at = DateTime::parse_from_rfc3339("2026-11-09T16:00:05Z")
        .unwrap()
        .with_timezone(&Utc);
    let window_end = DateTime::parse_from_rfc3339("2026-11-09T16:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let window_start = window_end - Duration::days(1);
    let snapshot = crate::snapshots::DailySnapshot {
        date: "2026-11-09".into(),
        window_start,
        window_end,
        generated_at,
        accounts: vec![crate::snapshots::DailyAccountGroup {
            mode: TradingMode::Live,
            asset: "USDC".into(),
            account_id: Some("acc-1".into()),
            count: 2,
            equity: Some(dec!(11254.48)),
            available: Some(dec!(3878.03)),
            wallet: Some(dec!(10000)),
            unrealized: Some(dec!(1254.48)),
        }],
        positions: vec![crate::snapshots::DailyPositionGroup {
            mode: TradingMode::Live,
            asset: "USDC".into(),
            count: 2,
            long: Some(dec!(2469.00)),
            short: Some(dec!(500.00)),
            total: Some(dec!(2969.00)),
        }],
        pair_positions: vec![
            crate::snapshots::DailyPositionItem {
                symbol: "SOLUSDC".into(),
                mode: TradingMode::Live,
                direction: GridDirection::Long,
                status: crate::types::BotStatus::Running,
                size: dec!(20),
                entry_price: dec!(120),
                mark_price: Some(dec!(123.45)),
                position_amount: Some(dec!(2469.00)),
                unrealized_pnl: Some(dec!(69.00)),
            },
            crate::snapshots::DailyPositionItem {
                symbol: "ETHUSDC".into(),
                mode: TradingMode::Live,
                direction: GridDirection::Short,
                status: crate::types::BotStatus::Paused,
                size: Decimal::ZERO,
                entry_price: Decimal::ZERO,
                mark_price: Some(dec!(2500)),
                position_amount: Some(Decimal::ZERO),
                unrealized_pnl: Some(Decimal::ZERO),
            },
        ],
        trades: vec![crate::snapshots::DailyTradeGroup {
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Live,
            buy_count: 12,
            sell_count: 15,
            realized_pnl: dec!(45.6789),
            pending_pnl_count: 1,
        }],
    };
    let message = format_daily_snapshot_message(&snapshot);
    assert!(message.contains("<b>📊 每日运行数据快照（2026-11-09）</b>"));
    assert!(message.contains(
        "· 实盘 USDC：权益 11254.48 USDC｜可用 3878.03 USDC（38.78%）｜浮盈 +1254.48 USDC"
    ));
    assert!(message.contains("· 成交笔数：27 笔（买 12 / 卖 15）"));
    assert!(message.contains("· 实盘 USDC：+45.6789 USDC（1 笔待同步）"));
    assert!(
        message.contains("· 实盘 USDC：总持仓 2969.00 USDC（多 2469.00 USDC / 空 500.00 USDC）")
    );
    assert!(message.contains("  - SOLUSDC（多 20）：2469.00 USDC｜浮盈 +69.00 USDC"));
    assert!(!message.contains("ETHUSDC"));
    assert!(message.contains("生成时间：2026-11-10 00:00:05（北京时间）"));
}

#[test]
fn daily_snapshot_message_saturates_extreme_trade_counts_and_pnl() {
    let now = Utc::now();
    let snapshot = crate::snapshots::DailySnapshot {
        date: "2026-11-09".into(),
        window_start: now - Duration::days(1),
        window_end: now,
        generated_at: now,
        accounts: Vec::new(),
        positions: Vec::new(),
        pair_positions: Vec::new(),
        trades: vec![
            crate::snapshots::DailyTradeGroup {
                symbol: "SOLUSDC".into(),
                mode: TradingMode::Live,
                buy_count: u64::MAX,
                sell_count: u64::MAX,
                realized_pnl: Decimal::MAX,
                pending_pnl_count: u64::MAX,
            },
            crate::snapshots::DailyTradeGroup {
                symbol: "BTCUSDC".into(),
                mode: TradingMode::Live,
                buy_count: 10,
                sell_count: 20,
                realized_pnl: Decimal::ONE,
                pending_pnl_count: 5,
            },
        ],
    };
    let message = format_daily_snapshot_message(&snapshot);
    assert!(message.contains(&format!(
        "成交笔数：{max} 笔（买 {max} / 卖 {max}）",
        max = u64::MAX
    )));
}

fn tg_cfg(bot_token: &str, chat_id: &str) -> TelegramConfig {
    TelegramConfig {
        enabled: true,
        bot_token: bot_token.into(),
        chat_id: chat_id.into(),
    }
}

#[tokio::test]
async fn send_message_validates_trims_bounds_gateway_responses_and_summarizes_errors() {
    use axum::{http::StatusCode, routing::post, Json, Router};

    let offline = TelegramNotifier::with_base_url(
        crate::exchange::client::shared_http_client(),
        "http://127.0.0.1:1".into(),
    );
    for (tok, chat, expected) in [
        (
            "123:abc/../../getMe",
            "12345",
            "Telegram Bot Token 格式无效",
        ),
        ("123:abc?foo=1", "12345", "Telegram Bot Token 格式无效"),
        ("123:abc#frag", "12345", "Telegram Bot Token 格式无效"),
        ("123:abc\\x", "12345", "Telegram Bot Token 格式无效"),
        (
            "123456:valid-token",
            "123\n456",
            "Telegram Chat ID 格式无效",
        ),
    ] {
        let err = offline
            .send_test_message(&tg_cfg(tok, chat))
            .await
            .unwrap_err();
        assert!(err.to_string().contains(expected), "{err}");
    }

    let route = format!("/bot{}/sendMessage", "123456:valid-token");
    let app = Router::new().route(
        &route,
        post(|Json(body): Json<serde_json::Value>| async move {
            let html = "<!DOCTYPE html><html><body>502 Bad Gateway</body></html>";
            let payload = if body["chat_id"] == "999" {
                format!("<html><body>{}</body></html>", "x".repeat(16 * 1024))
            } else {
                serde_json::json!({ "ok": false, "description": html }).to_string()
            };
            (StatusCode::BAD_GATEWAY, payload)
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let notifier =
        TelegramNotifier::with_base_url(crate::exchange::client::shared_http_client(), url);
    let err_502 = notifier
        .send_test_message(&tg_cfg("123456:valid-token", "999"))
        .await
        .unwrap_err()
        .to_string();
    assert!(err_502.contains("Telegram returned an invalid response (HTTP 502"));
    let padded_token = format!("  {}  ", "123456:valid-token");
    let summary_err = notifier
        .send_test_message(&tg_cfg(&padded_token, "  987654  "))
        .await
        .unwrap_err()
        .to_string();
    assert!(!summary_err.contains("<html") && summary_err.contains("upstream HTML error response"));
    server.abort();
}

fn rendered_utf16_units_and_balanced_bold(html: &str) -> (usize, bool) {
    let (mut idx, mut units, mut depth) = (0usize, 0usize, 0usize);
    while idx < html.len() {
        let rest = &html[idx..];
        if rest.starts_with("<b>") {
            depth += 1;
            idx += 3;
        } else if rest.starts_with("</b>") {
            if depth == 0 {
                return (units, false);
            }
            depth -= 1;
            idx += 4;
        } else if let Some(entity) = ["&amp;", "&lt;", "&gt;", "&quot;"]
            .into_iter()
            .find(|e| rest.starts_with(e))
        {
            units += 1;
            idx += entity.len();
        } else {
            if rest.starts_with('&') || rest.starts_with('<') {
                return (units, false);
            }
            let ch = rest.chars().next().unwrap();
            units += ch.len_utf16();
            idx += ch.len_utf8();
        }
    }
    (units, depth == 0)
}

#[test]
fn truncate_telegram_html_preserves_within_limit_and_truncates_entities_bold_and_surrogates() {
    // 4096 rendered UTF-16 units with <b>...</b>, &amp;, and emojis passes unchanged.
    let exact = format!("<b>📊🟢{}</b>{}", "&amp;".repeat(100), "a".repeat(3992));
    let (exact_units, exact_balanced) = rendered_utf16_units_and_balanced_bold(&exact);
    assert_eq!(exact_units, 4096);
    assert!(exact_balanced);
    assert_eq!(truncate_telegram_html(exact.clone()), exact);

    // Oversized inside open <b> with &amp; and surrogate-pair emojis truncates <= 4096 with balanced <b>...</b>.
    let oversized_bold = format!(
        "<b>📊 每日快照 🟢 {}</b>",
        "&amp;📊🟢&lt;&gt;&quot;".repeat(700)
    );
    let truncated = truncate_telegram_html(oversized_bold);
    assert!(truncated.ends_with("</b>\n…（已截断）"), "{truncated}");
    let (units, balanced) = rendered_utf16_units_and_balanced_bold(&truncated);
    assert!(units <= 4096, "rendered UTF-16 units {units} > 4096");
    assert!(balanced, "unbalanced HTML or split entity: {truncated}");

    // Boundary check right on &amp; and emoji surrogate pair at unit 4089.
    for filler_len in [4087, 4088, 4089] {
        let tricky = format!(
            "{}<b>&amp;🟢📊</b>{}",
            "x".repeat(filler_len),
            "y".repeat(20)
        );
        let out = truncate_telegram_html(tricky);
        let (u, ok) = rendered_utf16_units_and_balanced_bold(&out);
        assert!(u <= 4096 && ok, "filler={filler_len}, units={u}, out={out}");
    }
}
