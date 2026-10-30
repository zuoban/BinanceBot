use crate::config::TelegramConfig;
use crate::fx::CnyRate;
use crate::types::{
    AccountInfo, GridDirection, HourlyTradeStats, OrderSide, PositionInfo, TickerInfo, TradeRecord,
    TradingMode,
};
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

#[derive(Clone)]
pub struct TelegramNotifier {
    client: Client,
    api_base_url: String,
}

impl TelegramNotifier {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            api_base_url: "https://api.telegram.org".into(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_base_url(client: Client, api_base_url: String) -> Self {
        Self {
            client,
            api_base_url,
        }
    }

    pub async fn send_trade_notification(
        &self,
        config: &TelegramConfig,
        trade: &TradeRecord,
        context: &TradeNotificationContext,
    ) -> Result<()> {
        self.send_message(config, format_trade_message(trade, context))
            .await
    }

    pub async fn send_global_risk_notification(
        &self,
        config: &TelegramConfig,
        reason: &str,
    ) -> Result<()> {
        self.send_message(config, format_global_risk_message(reason, Utc::now()))
            .await
    }

    async fn send_message(&self, config: &TelegramConfig, text: String) -> Result<()> {
        if config.bot_token.trim().is_empty() || config.chat_id.trim().is_empty() {
            return Err(anyhow!("Bot Token or Chat ID is empty"));
        }
        let endpoint = format!("{}/bot{}/sendMessage", self.api_base_url, config.bot_token);
        let response = self
            .client
            .post(endpoint)
            .json(&SendMessage {
                chat_id: &config.chat_id,
                text,
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
}

#[derive(Debug, Default)]
pub struct TradeNotificationContext {
    pub direction: GridDirection,
    pub today_count: Option<u64>,
    pub hour_count: Option<u64>,
    pub account_equity: Option<Decimal>,
    pub available_margin: Option<Decimal>,
    pub position_amount: Option<Decimal>,
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
            direction: GridDirection::Long,
            today_count,
            hour_count,
            account_equity,
            available_margin,
            position_amount: None,
            cny_per_unit,
        }
    }

    pub fn with_direction(mut self, direction: GridDirection) -> Self {
        self.direction = direction;
        self
    }

    pub fn with_position(
        mut self,
        trade: &TradeRecord,
        position: &PositionInfo,
        ticker: &TickerInfo,
    ) -> Self {
        self.position_amount = None;
        let matching_position = position.symbol == trade.symbol
            || (trade.mode == TradingMode::Paper && position.symbol.is_empty());
        if !matching_position {
            return self;
        }
        let matching_ticker = ticker.symbol == trade.symbol;
        let valuation_price = if matching_ticker && ticker.mark_price > Decimal::ZERO {
            ticker.mark_price
        } else if position.mark_price > Decimal::ZERO {
            position.mark_price
        } else if matching_ticker {
            ticker.last_price
        } else {
            Decimal::ZERO
        };
        self.position_amount = if position.size.is_zero() {
            Some(Decimal::ZERO)
        } else if valuation_price > Decimal::ZERO {
            position.size.abs().checked_mul(valuation_price)
        } else {
            None
        };
        self
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
    let average_price = trade.price.normalize();
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
    let pnl_line = if trade.side == context.direction.closing_side() {
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
        "<b>{icon} {direction}｜{average_price}｜{quantity} {base}｜{amount:.2} {quote}｜{today}｜{hour}</b>\n<b>账户权益：{equity}｜</b>{cny}\n持仓金额：{position_amount}\n可用保证金：{available}｜{ratio}{pnl_line}",
        direction = trade.side.as_str(),
        quantity = trade.quantity.normalize(),
        base = escape_html(base),
        amount = trade.amount_usdc.round_dp(2),
        today = count(context.today_count),
        hour = count(context.hour_count),
        equity = money(context.account_equity),
        available = money(context.available_margin),
        position_amount = money(context.position_amount),
    )
}

fn format_global_risk_message(reason: &str, now: DateTime<Utc>) -> String {
    let time = now
        .with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap())
        .format("%Y-%m-%d %H:%M:%S");
    format!(
        "<b>⚠️ 全局风控已触发</b>\n原因：{}\n时间：{time}（北京时间）",
        escape_html(reason)
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
    TelegramNotifier::new(client.clone())
        .send_trade_notification(config, trade, context)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::HourlyTradeCount;
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
            "<b>🟢 BUY｜123.45｜2 SOL｜246.90 USDC｜7｜2</b>\n<b>账户权益：11254.48 USDC｜</b>≈ ¥75453.41\n持仓金额：2469.00 USDC\n可用保证金：3878.03 USDC｜34.46%"
        );
    }

    #[test]
    fn short_notifications_show_realized_pnl_on_closing_buys() {
        let mut closing = trade(OrderSide::Buy);
        closing.realized_pnl = dec!(3.25);
        let short_context = context(&closing).with_direction(GridDirection::Short);
        assert!(
            format_trade_message(&closing, &short_context).ends_with("已实现盈亏：+3.2500 USDC")
        );
        closing.pnl_verified = false;
        assert!(format_trade_message(&closing, &short_context).ends_with("已实现盈亏：待同步"));
        let opening = trade(OrderSide::Sell);
        assert!(!format_trade_message(&opening, &short_context).contains("已实现盈亏"));
    }

    #[test]
    fn position_amount_uses_total_position_at_mark_price_in_all_modes() {
        for mode in [TradingMode::Paper, TradingMode::Testnet, TradingMode::Live] {
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
        let context = TradeNotificationContext::default().with_position(
            &trade,
            &flat,
            &TickerInfo::default(),
        );
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
            "<b>🔴 SELL｜123.45｜2 SOL｜246.90 USDC｜4｜1</b>\n<b>账户权益：11254.48 USDC｜</b>≈ ¥75453.41\n持仓金额：2469.00 USDC\n可用保证金：3878.03 USDC｜34.46%\n已实现盈亏：-3.2500 USDC"
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
                    "<b>{icon} {}｜117.123456｜4.27 SOL｜500.12 USDC｜--｜--</b>",
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
        assert!(message.starts_with("<b>🟢 BUY｜0.00001234｜"));
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
