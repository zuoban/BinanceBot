use super::*;
use crate::types::HourlyTradeCount;
use chrono::Duration;
use rust_decimal_macros::dec;

#[test]
fn global_risk_message_escapes_reason_and_uses_beijing_time() {
    let now = DateTime::parse_from_rfc3339("2026-10-11T18:26:00Z")
        .unwrap()
        .with_timezone(&Utc);
    assert_eq!(
        format_global_risk_message("余额 < 0 & 风控", now),
        "<b>⚠️ 全局风控已触发</b>\n原因：余额 &lt; 0 &amp; 风控\n时间：2026-10-12 02:26:00（北京时间）"
    );
}

#[test]
fn test_message_uses_beijing_time() {
    let now = DateTime::parse_from_rfc3339("2026-10-11T18:26:00Z")
        .unwrap()
        .with_timezone(&Utc);
    assert_eq!(
        format_test_message(now),
        "<b>✅ Grid Console 测试消息</b>\n通知渠道可用。\n时间：2026-10-12 02:26:00（北京时间）"
    );
}

fn trade(side: OrderSide) -> TradeRecord {
    TradeRecord {
        trade_id: "1".into(),
        client_order_id: "order-1".into(),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        side,
        price: dec!(123.45),
        quantity: dec!(2),
        amount_usdc: dec!(246.90),
        realized_pnl: dec!(0),
        commission: dec!(0),
        pnl_verified: true,
        is_maker: true,
        timestamp: DateTime::parse_from_rfc3339("2026-10-01T12:26:00Z")
            .unwrap()
            .with_timezone(&Utc),
        note: String::new(),
    }
}

fn account(now: DateTime<Utc>) -> AccountInfo {
    AccountInfo {
        asset: "USDC".into(),
        total_wallet_balance: dec!(10000),
        available_balance: dec!(3878.03),
        margin_balance: dec!(11254.48),
        unrealized_profit: dec!(1254.48),
        update_time: now,
    }
}

fn context(trade: &TradeRecord) -> TradeNotificationContext {
    let start = trade.timestamp - Duration::minutes(26) - Duration::hours(20);
    let stats = HourlyTradeStats {
        window_start: start,
        window_end: trade.timestamp,
        realized_pnl: Decimal::ZERO,
        pending_pnl_count: 0,
        buckets: vec![
            HourlyTradeCount {
                hour_start: start,
                buy_count: 5,
                sell_count: 3,
            },
            HourlyTradeCount {
                hour_start: trade.timestamp - Duration::minutes(26),
                buy_count: 2,
                sell_count: 1,
            },
        ],
    };
    let rate = CnyRate {
        asset: "USDC".into(),
        cny_per_unit: dec!(6.7043),
        updated_at: trade.timestamp,
    };
    TradeNotificationContext::for_trade(
        trade,
        Some(&stats),
        &account(trade.timestamp),
        Some(&rate),
        trade.timestamp,
        3,
    )
    .with_position(
        trade,
        &PositionInfo {
            symbol: trade.symbol.clone(),
            size: dec!(20),
            mark_price: dec!(123.45),
            ..Default::default()
        },
        &TickerInfo::default(),
    )
}

#[test]
fn buy_message_uses_requested_layout_and_buy_counts() {
    let trade = trade(OrderSide::Buy);
    assert_eq!(
        format_trade_message(&trade, &context(&trade)),
        "<b>🟢 BUY｜SOLUSDC｜123.45｜246.90 USDC｜7D｜2H</b>\n<b>账户权益：11254.48 USDC｜</b>≈ ¥75453.41\n持仓金额：2469.00 USDC\n可用余额：3878.03 USDC｜38.78%"
    );
}

#[test]
fn short_notifications_show_realized_pnl_on_closing_buys() {
    let mut closing = trade(OrderSide::Buy);
    closing.realized_pnl = dec!(3.25);
    let short_context = context(&closing).with_direction(GridDirection::Short);
    assert!(format_trade_message(&closing, &short_context).ends_with("已实现盈亏：+3.2500 USDC"));
    closing.pnl_verified = false;
    assert!(format_trade_message(&closing, &short_context).ends_with("已实现盈亏：待同步"));
    let opening = trade(OrderSide::Sell);
    assert!(!format_trade_message(&opening, &short_context).contains("已实现盈亏"));
}

#[test]
fn position_amount_uses_total_position_at_mark_price_in_all_modes() {
    for mode in [TradingMode::Paper, TradingMode::Live] {
        for side in [OrderSide::Buy, OrderSide::Sell] {
            let mut trade = trade(side);
            trade.mode = mode;
            trade.symbol = "SOLUSDT".into();
            let position = PositionInfo {
                symbol: trade.symbol.clone(),
                size: dec!(20),
                entry_price: dec!(50),
                mark_price: dec!(99),
                ..Default::default()
            };
            let ticker = TickerInfo {
                symbol: trade.symbol.clone(),
                mark_price: dec!(100),
                last_price: dec!(150),
                ..Default::default()
            };
            let context =
                TradeNotificationContext::default().with_position(&trade, &position, &ticker);
            assert_eq!(context.position_amount, Some(dec!(2000)));
            let message = format_trade_message(&trade, &context);
            assert!(message.contains("\n持仓金额：2000.00 USDT\n"));
            assert!(message.contains("246.90 USDT"));
        }
    }
}

#[test]
fn position_amount_handles_shorts_zero_and_missing_or_mismatched_data() {
    let mut trade = trade(OrderSide::Sell);
    let position = PositionInfo {
        symbol: trade.symbol.clone(),
        size: dec!(-2.29),
        mark_price: dec!(108.7654321),
        ..Default::default()
    };
    let context = TradeNotificationContext::default().with_position(
        &trade,
        &position,
        &TickerInfo::default(),
    );
    assert!(format_trade_message(&trade, &context).contains("持仓金额：249.07 USDC"));
    let flat = PositionInfo {
        size: Decimal::ZERO,
        mark_price: Decimal::ZERO,
        ..position.clone()
    };
    let context =
        TradeNotificationContext::default().with_position(&trade, &flat, &TickerInfo::default());
    assert!(format_trade_message(&trade, &context).contains("持仓金额：0.00 USDC"));

    for unavailable in [
        PositionInfo::default(),
        PositionInfo {
            symbol: "BTCUSDC".into(),
            ..position.clone()
        },
        PositionInfo {
            mark_price: Decimal::ZERO,
            ..position.clone()
        },
    ] {
        let context = TradeNotificationContext::default().with_position(
            &trade,
            &unavailable,
            &TickerInfo::default(),
        );
        assert_eq!(context.position_amount, None);
        assert!(format_trade_message(&trade, &context).contains("持仓金额：-- USDC"));
    }
    trade.mode = TradingMode::Paper;
    let paper = PositionInfo {
        symbol: String::new(),
        mark_price: Decimal::ZERO,
        size: dec!(2),
        ..Default::default()
    };
    let ticker = TickerInfo {
        symbol: trade.symbol.clone(),
        last_price: dec!(100),
        ..Default::default()
    };
    let context = TradeNotificationContext::default().with_position(&trade, &paper, &ticker);
    assert_eq!(context.position_amount, Some(dec!(200)));
}

#[test]
fn sell_message_uses_sell_counts_and_verified_pnl() {
    let mut trade = trade(OrderSide::Sell);
    trade.realized_pnl = dec!(-3.25);
    trade.commission = dec!(0.80);
    assert_eq!(
        format_trade_message(&trade, &context(&trade)),
        "<b>🔴 SELL｜SOLUSDC｜123.45｜246.90 USDC｜4D｜1H</b>\n<b>账户权益：11254.48 USDC｜</b>≈ ¥75453.41\n持仓金额：2469.00 USDC\n可用余额：3878.03 USDC｜38.78%\n已实现盈亏：-3.2500 USDC"
    );
    trade.realized_pnl = dec!(1.25);
    assert!(format_trade_message(&trade, &context(&trade)).ends_with("已实现盈亏：+1.2500 USDC"));
    trade.pnl_verified = false;
    assert!(format_trade_message(&trade, &context(&trade)).ends_with("已实现盈亏：待同步"));
}

#[test]
fn titles_use_execution_average_without_market_or_account_data() {
    for mode in [TradingMode::Paper, TradingMode::Live] {
        for side in [OrderSide::Buy, OrderSide::Sell] {
            let mut trade = trade(side);
            trade.mode = mode;
            trade.price = dec!(117.123456);
            trade.quantity = dec!(4.27);
            trade.amount_usdc = trade.price * trade.quantity;
            let message = format_trade_message(&trade, &TradeNotificationContext::default());
            let icon = if side == OrderSide::Buy {
                "🟢"
            } else {
                "🔴"
            };
            assert!(message.starts_with(&format!(
                "<b>{icon} {}｜SOLUSDC｜117.123456｜500.12 USDC｜--D｜--H</b>",
                side.as_str()
            )));
        }
    }
}

#[test]
fn title_preserves_small_execution_price() {
    let mut trade = trade(OrderSide::Buy);
    trade.price = dec!(0.0000123400);
    let message = format_trade_message(&trade, &TradeNotificationContext::default());
    assert!(message.starts_with("<b>🟢 BUY｜SOLUSDC｜0.00001234｜"));
}

#[test]
fn stale_or_mismatched_account_does_not_display_balances() {
    let trade = trade(OrderSide::Buy);
    for account in [
        account(trade.timestamp - Duration::seconds(30)),
        AccountInfo {
            asset: "USDT".into(),
            ..account(trade.timestamp)
        },
    ] {
        let context =
            TradeNotificationContext::for_trade(&trade, None, &account, None, trade.timestamp, 3);
        assert_eq!(context.account_equity, None);
        assert_eq!(context.available_balance, None);
        let message = format_trade_message(&trade, &context);
        assert!(message.contains("｜--D｜--H</b>"));
        assert!(message.contains("账户权益：-- USDC｜</b>--"));
        assert!(message.ends_with("可用余额：-- USDC｜--"));
    }
    let mut unknown_symbol = trade.clone();
    unknown_symbol.symbol = "BTCETH".into();
    let fallback_account = AccountInfo {
        asset: "计价资产".into(),
        ..account(trade.timestamp)
    };
    let context = TradeNotificationContext::for_trade(
        &unknown_symbol,
        None,
        &fallback_account,
        None,
        trade.timestamp,
        3,
    );
    assert_eq!(context.available_balance, None);
    assert!(format_trade_message(&unknown_symbol, &context).contains("计价资产"));
}

#[test]
fn live_trade_freshness_scales_with_sync_interval() {
    let trade = trade(OrderSide::Buy);
    let synced_45s_ago = account(trade.timestamp - Duration::seconds(45));
    let stale_under_default = TradeNotificationContext::for_trade(
        &trade,
        None,
        &synced_45s_ago,
        None,
        trade.timestamp,
        3,
    );
    assert_eq!(stale_under_default.account_equity, None);
    assert_eq!(stale_under_default.available_balance, None);

    let fresh_under_30s = TradeNotificationContext::for_trade(
        &trade,
        None,
        &synced_45s_ago,
        None,
        trade.timestamp,
        30,
    );
    assert_eq!(
        fresh_under_30s.account_equity,
        Some(synced_45s_ago.margin_balance)
    );
    assert_eq!(
        fresh_under_30s.available_balance,
        Some(synced_45s_ago.available_balance)
    );

    let expired_at_90s = TradeNotificationContext::for_trade(
        &trade,
        None,
        &account(trade.timestamp - Duration::seconds(90)),
        None,
        trade.timestamp,
        30,
    );
    assert_eq!(expired_at_90s.account_equity, None);
    assert_eq!(expired_at_90s.available_balance, None);
}

#[test]
fn paper_context_uses_shared_account_equity() {
    let mut trade = trade(OrderSide::Buy);
    trade.mode = TradingMode::Paper;
    let shared = account(trade.timestamp);
    let context =
        TradeNotificationContext::for_trade(&trade, None, &shared, None, trade.timestamp, 3);
    assert_eq!(context.account_equity, Some(shared.margin_balance));
    assert_eq!(context.available_balance, Some(shared.available_balance));
    assert_eq!(context.wallet_balance, Some(shared.total_wallet_balance));
    assert_eq!(context.cny_per_unit, None);
}

#[test]
fn ratio_handles_zero_wallet_and_zero_available_balance() {
    let trade = trade(OrderSide::Buy);
    let mut context = context(&trade);
    context.wallet_balance = Some(Decimal::ZERO);
    assert!(format_trade_message(&trade, &context).ends_with("可用余额：3878.03 USDC｜--"));
    context.wallet_balance = Some(dec!(10000));
    context.available_balance = Some(Decimal::ZERO);
    assert!(format_trade_message(&trade, &context).ends_with("可用余额：0.00 USDC｜0.00%"));
}

#[test]
fn rate_must_match_the_account_asset_and_be_recent() {
    let trade = trade(OrderSide::Buy);
    for rate in [
        CnyRate {
            asset: "USDT".into(),
            cny_per_unit: dec!(7),
            updated_at: trade.timestamp,
        },
        CnyRate {
            asset: "USDC".into(),
            cny_per_unit: dec!(7),
            updated_at: trade.timestamp - Duration::hours(1),
        },
    ] {
        let context = TradeNotificationContext::for_trade(
            &trade,
            None,
            &account(trade.timestamp),
            Some(&rate),
            trade.timestamp,
            3,
        );
        assert_eq!(context.cny_per_unit, None);
    }
}

#[test]
fn notification_escapes_symbol_in_title() {
    let mut trade = trade(OrderSide::Buy);
    trade.symbol = "<SOL>&USDC".into();
    let message = format_trade_message(&trade, &context(&trade));
    assert!(!message.contains("<SOL>"));
    assert!(message.starts_with("<b>🟢 BUY｜&lt;SOL&gt;&amp;USDC｜"));
    assert!(message.contains("246.90 USDC"));
}

#[test]
fn ordinals_survive_restart_and_reset_at_beijing_hour_and_day_boundaries() {
    let dir = crate::test_support::TempTestDir::new("telegram-counts");
    let path = dir.join("counts.db");
    let db = crate::db::Database::open(&path).unwrap();
    let mut fill = trade(OrderSide::Buy);
    let midnight = fill.timestamp - Duration::hours(20) - Duration::minutes(26);
    for (id, side, timestamp, expected) in [
        (
            "previous-day",
            OrderSide::Buy,
            midnight - Duration::nanoseconds(1),
            (1, 1),
        ),
        ("first-buy", OrderSide::Buy, midnight, (1, 1)),
        ("first-sell", OrderSide::Sell, midnight, (1, 1)),
        (
            "second-buy",
            OrderSide::Buy,
            midnight + Duration::minutes(59),
            (2, 2),
        ),
        (
            "next-hour",
            OrderSide::Buy,
            midnight + Duration::hours(1),
            (3, 1),
        ),
        (
            "same-time",
            OrderSide::Buy,
            midnight + Duration::hours(1),
            (4, 2),
        ),
        (
            "next-day",
            OrderSide::Buy,
            midnight + Duration::days(1),
            (1, 1),
        ),
    ] {
        fill.trade_id = id.into();
        fill.client_order_id = id.into();
        fill.side = side;
        fill.timestamp = timestamp;
        assert!(db.insert_trade(&fill).unwrap());
        assert!(!db.insert_trade(&fill).unwrap());
        let stats = db
            .get_hourly_trade_stats(&fill.symbol, fill.mode, timestamp)
            .unwrap();
        let context = TradeNotificationContext::for_trade(
            &fill,
            Some(&stats),
            &account(timestamp),
            None,
            timestamp,
            3,
        );
        assert_eq!(
            (context.today_count, context.hour_count),
            (Some(expected.0), Some(expected.1))
        );
    }
    drop(db);
    let reopened = crate::db::Database::open(&path).unwrap();
    // Restart within the first day's 01:00 hour and ignore other scopes.
    fill.trade_id = "after-restart".into();
    fill.client_order_id = "after-restart".into();
    fill.timestamp = midnight + Duration::hours(1) + Duration::minutes(1);
    for (id, symbol, mode) in [
        ("other-symbol", "BTCUSDC", TradingMode::Live),
        ("other-mode", "SOLUSDC", TradingMode::Paper),
    ] {
        let other = TradeRecord {
            trade_id: id.into(),
            client_order_id: id.into(),
            symbol: symbol.into(),
            mode,
            ..fill.clone()
        };
        assert!(reopened.insert_trade(&other).unwrap());
    }
    assert!(reopened.insert_trade(&fill).unwrap());
    let stats = reopened
        .get_hourly_trade_stats(&fill.symbol, fill.mode, fill.timestamp)
        .unwrap();
    let context = TradeNotificationContext::for_trade(
        &fill,
        Some(&stats),
        &account(fill.timestamp),
        None,
        fill.timestamp,
        3,
    );
    assert_eq!(context.today_count, Some(5));
    assert_eq!(context.hour_count, Some(3));
    drop(reopened);
}

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

#[tokio::test]
async fn send_message_rejects_malformed_bot_token_before_http_request() {
    let notifier = TelegramNotifier::with_base_url(
        crate::exchange::client::shared_http_client(),
        "http://127.0.0.1:1".into(),
    );
    for invalid in [
        "123:abc/../../getMe",
        "123:abc?foo=1",
        "123:abc#frag",
        "123:abc\\x",
    ] {
        let config = TelegramConfig {
            enabled: true,
            bot_token: invalid.into(),
            chat_id: "12345".into(),
        };
        let err = notifier.send_test_message(&config).await.unwrap_err();
        assert!(
            err.to_string().contains("Telegram Bot Token 格式无效"),
            "{invalid}: {err}"
        );
    }
}

#[tokio::test]
async fn send_message_rejects_oversized_gateway_response_without_unbounded_buffering() {
    use axum::{http::StatusCode, routing::post, Router};

    let oversized_html = format!("<html><body>{}</body></html>", "x".repeat(16 * 1024));
    let app = Router::new().route(
        "/bot123456:valid-token/sendMessage",
        post(move || {
            let body = oversized_html.clone();
            async move { (StatusCode::BAD_GATEWAY, body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let notifier =
        TelegramNotifier::with_base_url(crate::exchange::client::shared_http_client(), url);
    let config = TelegramConfig {
        enabled: true,
        bot_token: "123456:valid-token".into(),
        chat_id: "999".into(),
    };
    let err = notifier.send_test_message(&config).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("Telegram returned an invalid response (HTTP 502"),
        "{err}"
    );
    server.abort();
}

#[test]
fn trade_context_rejects_future_dated_cny_rate_and_live_account_timestamps() {
    let fill = trade(OrderSide::Buy);
    let now = fill.timestamp;
    let fresh_account = account(now);
    let future_rate = crate::fx::CnyRate {
        asset: "USDC".into(),
        cny_per_unit: dec!(7.12),
        updated_at: now + Duration::seconds(5),
    };
    let ctx = TradeNotificationContext::for_trade(
        &fill,
        None,
        &fresh_account,
        Some(&future_rate),
        now,
        3,
    );
    assert_eq!(ctx.cny_per_unit, None);
    assert_eq!(ctx.account_equity, Some(fresh_account.margin_balance));

    let future_account = account(now + Duration::seconds(5));
    let valid_rate = crate::fx::CnyRate {
        updated_at: now - Duration::minutes(5),
        ..future_rate
    };
    let future_acc_ctx = TradeNotificationContext::for_trade(
        &fill,
        None,
        &future_account,
        Some(&valid_rate),
        now,
        3,
    );
    assert_eq!(future_acc_ctx.account_equity, None);
    assert_eq!(future_acc_ctx.available_balance, None);
    assert_eq!(future_acc_ctx.cny_per_unit, Some(dec!(7.12)));
}
