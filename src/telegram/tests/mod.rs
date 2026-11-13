use super::*;
use crate::types::HourlyTradeCount;
use chrono::Duration;
use rust_decimal_macros::dec;

mod snapshots_and_delivery;

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

pub(super) fn trade(side: OrderSide) -> TradeRecord {
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
        commission_asset: None,
        pnl_verified: true,
        is_maker: true,
        timestamp: DateTime::parse_from_rfc3339("2026-10-01T12:26:00Z")
            .unwrap()
            .with_timezone(&Utc),
        note: String::new(),
    }
}

pub(super) fn account(now: DateTime<Utc>) -> AccountInfo {
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
        "<b>🟢 BUY｜SOLUSDC｜123.45｜246.90 USDC</b>\n成交笔数：7D｜2H\n持仓金额：2469.00 USDC"
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
            assert!(message.contains("\n持仓金额：2000.00 USDT"));
            assert!(message.contains("246.90 USDT"));
        }
    }
}

#[test]
fn position_amount_prefers_newer_position_mark_over_stale_ticker_mark() {
    let fill = trade(OrderSide::Buy);
    let now = fill.timestamp;
    let stale_ticker = TickerInfo {
        symbol: fill.symbol.clone(),
        mark_price: dec!(100),
        mark_update_time: now - Duration::seconds(30),
        last_price: dec!(150),
        update_time: now - Duration::seconds(30),
        ..Default::default()
    };
    let fresh_position = PositionInfo {
        symbol: fill.symbol.clone(),
        size: dec!(20),
        entry_price: dec!(50),
        mark_price: dec!(112.5),
        update_time: Some(now - Duration::seconds(2)),
        ..Default::default()
    };
    assert_eq!(
        fresh_position.market_valuation_price(Some(&stale_ticker)),
        dec!(112.5)
    );
    let ctx = TradeNotificationContext::for_trade(&fill, None, &account(now), None, now, 3)
        .with_position(&fill, &fresh_position, &stale_ticker);
    assert_eq!(ctx.position_amount, Some(dec!(2250)));

    // Even when both marks are fresh, the newer position mark wins over an older ticker mark.
    let older_fresh_ticker = TickerInfo {
        mark_update_time: now - Duration::seconds(5),
        update_time: now - Duration::seconds(5),
        ..stale_ticker.clone()
    };
    assert_eq!(
        fresh_position.market_valuation_price(Some(&older_fresh_ticker)),
        dec!(112.5)
    );
    let both_fresh_ctx =
        TradeNotificationContext::for_trade(&fill, None, &account(now), None, now, 3)
            .with_position(&fill, &fresh_position, &older_fresh_ticker);
    assert_eq!(both_fresh_ctx.position_amount, Some(dec!(2250)));

    // When ticker mark is newer than position mark, ticker mark wins.
    let newer_ticker = TickerInfo {
        mark_price: dec!(115),
        mark_update_time: now - Duration::seconds(1),
        update_time: now - Duration::seconds(1),
        ..stale_ticker.clone()
    };
    assert_eq!(
        fresh_position.market_valuation_price(Some(&newer_ticker)),
        dec!(115)
    );
    let newer_ticker_ctx =
        TradeNotificationContext::for_trade(&fill, None, &account(now), None, now, 3)
            .with_position(&fill, &fresh_position, &newer_ticker);
    assert_eq!(newer_ticker_ctx.position_amount, Some(dec!(2300)));

    // When ticker mark is stale and position mark is zero, a fresh last_price is used.
    let no_pos_mark = PositionInfo {
        mark_price: Decimal::ZERO,
        update_time: None,
        ..fresh_position
    };
    let fresh_last_ticker = TickerInfo {
        last_price: dec!(109),
        update_time: now - Duration::seconds(2),
        ..stale_ticker
    };
    let last_ctx = TradeNotificationContext::for_trade(&fill, None, &account(now), None, now, 3)
        .with_position(&fill, &no_pos_mark, &fresh_last_ticker);
    assert_eq!(last_ctx.position_amount, Some(dec!(2180)));
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
        "<b>🔴 SELL｜SOLUSDC｜123.45｜246.90 USDC</b>\n成交笔数：4D｜1H\n持仓金额：2469.00 USDC\n已实现盈亏：-3.2500 USDC"
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
                "<b>{icon} {}｜SOLUSDC｜117.123456｜500.12 USDC</b>\n成交笔数：--D｜--H",
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
        assert!(message.contains("\n成交笔数：--D｜--H\n"));
        assert!(!message.contains("账户权益"));
        assert!(!message.contains("可用余额"));
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
    assert_eq!(
        format_available_ratio(Some(dec!(3878.03)), Some(Decimal::ZERO)),
        "--"
    );
    assert_eq!(
        format_available_ratio(Some(Decimal::ZERO), Some(dec!(10000))),
        "0.00%"
    );
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
