use crate::config::TelegramConfig;
use crate::fx::CnyRate;
use crate::types::{AccountInfo, HourlyTradeStats, OrderSide, TradeRecord, TradingMode};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct SendMessage<'a> {
    chat_id: &'a str,
    text: String,
    parse_mode: &'static str,
}

#[derive(Deserialize)]
struct TelegramResponse {
    ok: bool,
    description: Option<String>,
}

#[derive(Debug, Default)]
pub struct TradeNotificationContext {
    pub today_count: Option<u64>,
    pub hour_count: Option<u64>,
    pub account_equity: Option<Decimal>,
    pub available_margin: Option<Decimal>,
    pub cny_per_unit: Option<Decimal>,
}

impl TradeNotificationContext {
    pub fn for_trade(
        trade: &TradeRecord,
        hourly_stats: Option<&HourlyTradeStats>,
        account: &AccountInfo,
        unrealized_pnl: Decimal,
        cny_rate: Option<&CnyRate>,
        now: DateTime<Utc>,
    ) -> Self {
        let side_count = |bucket: &crate::types::HourlyTradeCount| match trade.side {
            OrderSide::Buy => bucket.buy_count,
            OrderSide::Sell => bucket.sell_count,
        };
        let today_count = hourly_stats.map(|stats| stats.buckets.iter().map(side_count).sum());
        let hour_count = hourly_stats.and_then(|stats| stats.buckets.last().map(side_count));
        let age = now - account.update_time;
        let fresh = trade.mode == TradingMode::Paper
            || (age >= Duration::zero() && age < Duration::seconds(30));
        let matching_asset = account.asset == quote_asset(&trade.symbol);
        let account_equity =
            (fresh && matching_asset).then_some(if trade.mode == TradingMode::Paper {
                account.total_wallet_balance + unrealized_pnl
            } else {
                account.margin_balance
            });
        let available_margin = (fresh && matching_asset).then_some(account.available_balance);
        let cny_per_unit = cny_rate
            .filter(|rate| {
                rate.asset == account.asset
                    && rate.cny_per_unit > Decimal::ZERO
                    && now - rate.updated_at < Duration::hours(1)
            })
            .map(|rate| rate.cny_per_unit);
        Self {
            today_count,
            hour_count,
            account_equity,
            available_margin,
            cny_per_unit,
        }
    }
}

fn quote_asset(symbol: &str) -> &'static str {
    ["USDC", "USDT", "FDUSD", "BUSD"]
        .into_iter()
        .find(|asset| symbol.ends_with(asset))
        .unwrap_or("计价资产")
}

pub fn format_trade_message(trade: &TradeRecord, context: &TradeNotificationContext) -> String {
    let icon = match trade.side {
        OrderSide::Buy => "🟢",
        OrderSide::Sell => "🔴",
    };
    let quote = quote_asset(&trade.symbol);
    let base = trade.symbol.strip_suffix(quote).unwrap_or("");
    let average_price = trade.price.round_dp(4);
    let count = |value: Option<u64>| value.map_or_else(|| "--".to_string(), |n| n.to_string());
    let money = |value: Option<Decimal>| {
        value.map_or_else(
            || format!("-- {quote}"),
            |n| format!("{:.2} {quote}", n.round_dp(2)),
        )
    };
    let cny = context
        .account_equity
        .zip(context.cny_per_unit)
        .and_then(|(equity, rate)| equity.checked_mul(rate))
        .map_or_else(
            || "--".to_string(),
            |value| format!("≈ ¥{:.2}", value.round_dp(2)),
        );
    let ratio = context
        .available_margin
        .zip(context.account_equity)
        .filter(|(_, equity)| *equity > Decimal::ZERO)
        .and_then(|(available, equity)| available.checked_div(equity))
        .and_then(|ratio| ratio.checked_mul(Decimal::ONE_HUNDRED))
        .map_or_else(
            || "--".to_string(),
            |value| format!("{:.2}%", value.round_dp(2)),
        );
    let pnl_line = if trade.side == OrderSide::Sell {
        if trade.pnl_verified {
            format!(
                "\n已实现盈亏：{:+.4} {quote}",
                trade.realized_pnl.round_dp(4)
            )
        } else {
            "\n已实现盈亏：待同步".to_string()
        }
    } else {
        String::new()
    };
    format!(
        "<b>{icon} {direction}｜{average_price:.4}｜{quantity} {base}｜{amount:.2} {quote}｜{today}｜{hour}</b>\n<b>账户权益：{equity}｜</b>{cny}\n可用保证金：{available}｜{ratio}{pnl_line}",
        direction = trade.side.as_str(),
        quantity = trade.quantity.normalize(),
        base = escape_html(base),
        amount = trade.amount_usdc.round_dp(2),
        today = count(context.today_count),
        hour = count(context.hour_count),
        equity = money(context.account_equity),
        available = money(context.available_margin),
    )
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

pub async fn send_trade_notification(
    client: &Client,
    config: &TelegramConfig,
    trade: &TradeRecord,
    context: &TradeNotificationContext,
) -> Result<()> {
    if config.bot_token.trim().is_empty() || config.chat_id.trim().is_empty() {
        return Err(anyhow!("Bot Token or Chat ID is empty"));
    }
    let endpoint = format!(
        "https://api.telegram.org/bot{}/sendMessage",
        config.bot_token
    );
    let response = client
        .post(endpoint)
        .json(&SendMessage {
            chat_id: &config.chat_id,
            text: format_trade_message(trade, context),
            parse_mode: "HTML",
        })
        .send()
        .await
        .map_err(|err| {
            anyhow!(
                "Telegram request failed ({})",
                if err.is_timeout() {
                    "timeout"
                } else {
                    "network error"
                }
            )
        })?;

    let status = response.status();
    let body: TelegramResponse = response
        .json()
        .await
        .map_err(|_| anyhow!("Telegram returned an invalid response (HTTP {})", status))?;
    if !status.is_success() || !body.ok {
        return Err(anyhow!(
            "Telegram rejected notification (HTTP {}): {}",
            status,
            body.description
                .unwrap_or_else(|| "unknown error".to_string())
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::HourlyTradeCount;
    use rust_decimal_macros::dec;

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
            dec!(1254.48),
            Some(&rate),
            trade.timestamp,
        )
    }

    #[test]
    fn buy_message_uses_requested_layout_and_buy_counts() {
        let trade = trade(OrderSide::Buy);
        assert_eq!(
            format_trade_message(&trade, &context(&trade)),
            "<b>🟢 BUY｜123.4500｜2 SOL｜246.90 USDC｜7｜2</b>\n<b>账户权益：11254.48 USDC｜</b>≈ ¥75453.41\n可用保证金：3878.03 USDC｜34.46%"
        );
    }

    #[test]
    fn sell_message_uses_sell_counts_and_verified_pnl() {
        let mut trade = trade(OrderSide::Sell);
        trade.realized_pnl = dec!(-3.25);
        trade.commission = dec!(0.80);
        assert_eq!(
            format_trade_message(&trade, &context(&trade)),
            "<b>🔴 SELL｜123.4500｜2 SOL｜246.90 USDC｜4｜1</b>\n<b>账户权益：11254.48 USDC｜</b>≈ ¥75453.41\n可用保证金：3878.03 USDC｜34.46%\n已实现盈亏：-3.2500 USDC"
        );
        trade.realized_pnl = dec!(1.25);
        assert!(
            format_trade_message(&trade, &context(&trade)).ends_with("已实现盈亏：+1.2500 USDC")
        );
        trade.pnl_verified = false;
        assert!(format_trade_message(&trade, &context(&trade)).ends_with("已实现盈亏：待同步"));
    }

    #[test]
    fn titles_use_execution_average_without_market_or_account_data() {
        for mode in [TradingMode::Paper, TradingMode::Testnet, TradingMode::Live] {
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
                    "<b>{icon} {}｜117.1235｜4.27 SOL｜500.12 USDC｜--｜--</b>",
                    side.as_str()
                )));
            }
        }
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
            let context = TradeNotificationContext::for_trade(
                &trade,
                None,
                &account,
                Decimal::ZERO,
                None,
                trade.timestamp,
            );
            assert_eq!(context.account_equity, None);
            assert_eq!(context.available_margin, None);
            let message = format_trade_message(&trade, &context);
            assert!(message.contains("｜--｜--</b>"));
            assert!(message.contains("账户权益：-- USDC｜</b>--"));
            assert!(message.ends_with("可用保证金：-- USDC｜--"));
        }
    }

    #[test]
    fn paper_equity_includes_unrealized_pnl_and_missing_rate_stays_unknown() {
        let mut trade = trade(OrderSide::Buy);
        trade.mode = TradingMode::Paper;
        let context = TradeNotificationContext::for_trade(
            &trade,
            None,
            &account(trade.timestamp - Duration::hours(1)),
            dec!(500),
            None,
            trade.timestamp,
        );
        assert_eq!(context.account_equity, Some(dec!(10500)));
        let message = format_trade_message(&trade, &context);
        assert!(message.contains("账户权益：10500.00 USDC｜</b>--"));
        assert!(message.ends_with("可用保证金：3878.03 USDC｜36.93%"));
    }

    #[test]
    fn ratio_handles_zero_equity_and_zero_available_margin() {
        let trade = trade(OrderSide::Buy);
        let mut context = context(&trade);
        context.account_equity = Some(Decimal::ZERO);
        assert!(format_trade_message(&trade, &context).ends_with("可用保证金：3878.03 USDC｜--"));
        context.account_equity = Some(dec!(10000));
        context.available_margin = Some(Decimal::ZERO);
        assert!(format_trade_message(&trade, &context).ends_with("可用保证金：0.00 USDC｜0.00%"));
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
                Decimal::ZERO,
                Some(&rate),
                trade.timestamp,
            );
            assert_eq!(context.cny_per_unit, None);
        }
    }

    #[test]
    fn notification_escapes_quantity_asset() {
        let mut trade = trade(OrderSide::Buy);
        trade.symbol = "<SOL>&USDC".into();
        let message = format_trade_message(&trade, &context(&trade));
        assert!(!message.contains("&lt;SOL&gt;&amp;USDC"));
        assert!(message.contains("2 &lt;SOL&gt;&amp;"));
    }

    #[test]
    fn ordinals_survive_restart_and_reset_at_beijing_hour_and_day_boundaries() {
        let path =
            std::env::temp_dir().join(format!("telegram-counts-{}.db", uuid::Uuid::new_v4()));
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
                Decimal::ZERO,
                None,
                timestamp,
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
            Decimal::ZERO,
            None,
            fill.timestamp,
        );
        assert_eq!(context.today_count, Some(5));
        assert_eq!(context.hour_count, Some(3));
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }
}
