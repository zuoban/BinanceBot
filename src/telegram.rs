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
    pub fn shared_client() -> Client {
        crate::exchange::client::shared_http_client()
    }

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

    pub(crate) async fn send_daily_snapshot_notification(
        &self,
        config: &TelegramConfig,
        snapshot: &crate::snapshots::DailySnapshot,
    ) -> Result<()> {
        self.send_message(config, format_daily_snapshot_message(snapshot))
            .await
    }

    /// Deliver a fixed probe message so credentials can be verified before they are saved.
    pub async fn send_test_message(&self, config: &TelegramConfig) -> Result<()> {
        self.send_message(config, format_test_message(Utc::now()))
            .await
    }

    async fn send_message(&self, config: &TelegramConfig, text: String) -> Result<()> {
        if config.bot_token.trim().is_empty() || config.chat_id.trim().is_empty() {
            return Err(anyhow!("Bot Token or Chat ID is empty"));
        }
        TelegramConfig::validate_bot_token(&config.bot_token)?;
        let endpoint = format!("{}/bot{}/sendMessage", self.api_base_url, config.bot_token);
        let response = self
            .client
            .post(endpoint)
            .timeout(std::time::Duration::from_secs(10))
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
    pub wallet_balance: Option<Decimal>,
    pub available_balance: Option<Decimal>,
    pub position_amount: Option<Decimal>,
    pub cny_per_unit: Option<Decimal>,
}

impl TradeNotificationContext {
    pub fn for_trade(
        trade: &TradeRecord,
        hourly_stats: Option<&HourlyTradeStats>,
        account: &AccountInfo,
        cny_rate: Option<&CnyRate>,
        now: DateTime<Utc>,
    ) -> Self {
        let select_side = |(buy, sell): (u64, u64)| match trade.side {
            OrderSide::Buy => buy,
            OrderSide::Sell => sell,
        };
        let today_count = hourly_stats.map(|stats| select_side(stats.total_counts()));
        let hour_count = hourly_stats.and_then(|stats| {
            stats
                .buckets
                .last()
                .map(|bucket| select_side((bucket.buy_count, bucket.sell_count)))
        });
        let age = now - account.update_time;
        let flat_paper = trade.mode == TradingMode::Paper
            && account.margin_balance == account.available_balance
            && account.unrealized_profit.is_zero();
        let max_age = if trade.mode == TradingMode::Paper {
            15
        } else {
            30
        };
        let fresh = flat_paper || (age >= Duration::zero() && age < Duration::seconds(max_age));
        let matching_asset =
            crate::types::quote_asset(&trade.symbol).is_some_and(|asset| account.asset == asset);
        let account_equity = (fresh && matching_asset).then_some(account.margin_balance);
        let wallet_balance = ((trade.mode == TradingMode::Paper || fresh) && matching_asset)
            .then_some(account.total_wallet_balance);
        let available_balance = (fresh && matching_asset).then_some(account.available_balance);
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
            wallet_balance,
            available_balance,
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
        let matching_ticker = (ticker.symbol == trade.symbol).then_some(ticker);
        let valuation_price = position.market_valuation_price(matching_ticker);
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

fn format_decimal_dp(value: Decimal, dp: u32, force_sign: bool) -> String {
    let mut rounded = value.round_dp(dp);
    if rounded.is_zero() {
        rounded = Decimal::ZERO;
    }
    let raw = rounded.to_string();
    let (sign, unsigned) = match raw.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None if force_sign => ("+", raw.as_str()),
        None => ("", raw.as_str()),
    };
    let dp_usize = dp as usize;
    if dp_usize == 0 {
        return format!("{sign}{unsigned}");
    }
    match unsigned.split_once('.') {
        Some((whole, frac)) if frac.len() >= dp_usize => {
            format!("{sign}{whole}.{}", &frac[..dp_usize])
        }
        Some((whole, frac)) => {
            let pad = "0".repeat(dp_usize - frac.len());
            format!("{sign}{whole}.{frac}{pad}")
        }
        None => {
            let pad = "0".repeat(dp_usize);
            format!("{sign}{unsigned}.{pad}")
        }
    }
}

pub fn format_trade_message(trade: &TradeRecord, context: &TradeNotificationContext) -> String {
    let icon = match trade.side {
        OrderSide::Buy => "🟢",
        OrderSide::Sell => "🔴",
    };
    let quote = crate::types::quote_asset(&trade.symbol).unwrap_or("计价资产");
    let average_price = trade.price.normalize();
    let count = |value: Option<u64>| value.map_or_else(|| "--".to_string(), |n| n.to_string());
    let money = |value: Option<Decimal>| {
        value.map_or_else(
            || format!("-- {quote}"),
            |n| format!("{} {quote}", format_decimal_dp(n, 2, false)),
        )
    };
    let cny = context
        .account_equity
        .zip(context.cny_per_unit)
        .and_then(|(equity, rate)| equity.checked_mul(rate))
        .map_or_else(
            || "--".to_string(),
            |value| format!("≈ ¥{}", format_decimal_dp(value, 2, false)),
        );
    let ratio = context
        .available_balance
        .zip(context.wallet_balance)
        .filter(|(_, wallet)| *wallet > Decimal::ZERO)
        .and_then(|(available, wallet)| available.checked_div(wallet))
        .and_then(|ratio| ratio.checked_mul(Decimal::ONE_HUNDRED))
        .map_or_else(
            || "--".to_string(),
            |value| format!("{}%", format_decimal_dp(value, 2, false)),
        );
    let pnl_line = if trade.side == context.direction.closing_side() {
        if trade.pnl_verified {
            format!(
                "\n已实现盈亏：{} {quote}",
                format_decimal_dp(trade.realized_pnl, 4, true)
            )
        } else {
            "\n已实现盈亏：待同步".to_string()
        }
    } else {
        String::new()
    };
    format!(
        "<b>{icon} {direction}｜{symbol}｜{average_price}｜{amount} {quote}｜{today}D｜{hour}H</b>\n<b>账户权益：{equity}｜</b>{cny}\n持仓金额：{position_amount}\n可用余额：{available}｜{ratio}{pnl_line}",
        direction = trade.side.as_str(),
        symbol = escape_html(&trade.symbol),
        amount = format_decimal_dp(trade.amount_usdc, 2, false),
        today = count(context.today_count),
        hour = count(context.hour_count),
        equity = money(context.account_equity),
        available = money(context.available_balance),
        position_amount = money(context.position_amount),
    )
}

fn format_beijing_time(now: DateTime<Utc>) -> String {
    now.with_timezone(&crate::types::beijing_offset())
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

fn format_global_risk_message(reason: &str, now: DateTime<Utc>) -> String {
    let time = format_beijing_time(now);
    format!(
        "<b>⚠️ 全局风控已触发</b>\n原因：{}\n时间：{time}（北京时间）",
        escape_html(reason)
    )
}

fn mode_label(mode: TradingMode) -> &'static str {
    match mode {
        TradingMode::Paper => "模拟盘",
        TradingMode::Live => "实盘",
        TradingMode::Unknown => "未知模式",
    }
}

pub(crate) fn format_daily_snapshot_message(snapshot: &crate::snapshots::DailySnapshot) -> String {
    let money = |value: Option<Decimal>, asset: &str| {
        value.map_or_else(
            || format!("-- {asset}"),
            |n| format!("{} {asset}", format_decimal_dp(n, 2, false)),
        )
    };
    let signed_money = |value: Option<Decimal>, asset: &str| {
        value.map_or_else(
            || format!("-- {asset}"),
            |n| format!("{} {asset}", format_decimal_dp(n, 2, true)),
        )
    };

    let account_lines = if snapshot.accounts.is_empty() {
        "暂无账户数据".to_string()
    } else {
        snapshot
            .accounts
            .iter()
            .map(|acc| {
                let asset = escape_html(&acc.asset);
                let ratio = acc
                    .available
                    .zip(acc.wallet)
                    .filter(|(_, wallet)| *wallet > Decimal::ZERO)
                    .and_then(|(available, wallet)| available.checked_div(wallet))
                    .and_then(|ratio| ratio.checked_mul(Decimal::ONE_HUNDRED))
                    .map_or_else(
                        || "--".to_string(),
                        |value| format!("{}%", format_decimal_dp(value, 2, false)),
                    );
                format!(
                    "· {mode} {asset}：权益 {equity}｜可用 {available}（{ratio}）｜浮盈 {unrealized}",
                    mode = mode_label(acc.mode),
                    equity = money(acc.equity, &asset),
                    available = money(acc.available, &asset),
                    unrealized = signed_money(acc.unrealized, &asset),
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    let total_buys = snapshot
        .trades
        .iter()
        .fold(0u64, |acc, t| acc.saturating_add(t.buy_count));
    let total_sells = snapshot
        .trades
        .iter()
        .fold(0u64, |acc, t| acc.saturating_add(t.sell_count));
    let total_trades = total_buys.saturating_add(total_sells);

    let mut pnl_groups: std::collections::BTreeMap<
        (String, String),
        (TradingMode, String, Decimal, u64),
    > = std::collections::BTreeMap::new();
    for trade in &snapshot.trades {
        let asset = crate::types::quote_asset(&trade.symbol)
            .unwrap_or(&trade.symbol)
            .to_owned();
        let entry = pnl_groups
            .entry((trade.mode.as_str().to_owned(), asset.clone()))
            .or_insert((trade.mode, asset, Decimal::ZERO, 0));
        entry.2 = entry.2.saturating_add(trade.realized_pnl);
        entry.3 = entry.3.saturating_add(trade.pending_pnl_count);
    }
    let pnl_lines = if pnl_groups.is_empty() {
        "· 0.00".to_string()
    } else {
        pnl_groups
            .into_values()
            .map(|(mode, asset, pnl, pending)| {
                let asset = escape_html(&asset);
                let formatted_pnl = format_decimal_dp(pnl, 4, true);
                if pending > 0 {
                    format!(
                        "· {mode} {asset}：{formatted_pnl} {asset}（{pending} 笔待同步）",
                        mode = mode_label(mode),
                    )
                } else {
                    format!(
                        "· {mode} {asset}：{formatted_pnl} {asset}",
                        mode = mode_label(mode),
                    )
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    let position_lines = if snapshot.positions.is_empty() {
        "暂无持仓数据".to_string()
    } else {
        let mut lines: Vec<String> = snapshot
            .positions
            .iter()
            .map(|pos| {
                let asset = escape_html(&pos.asset);
                format!(
                    "· {mode} {asset}：总持仓 {total}（多 {long} / 空 {short}）",
                    mode = mode_label(pos.mode),
                    total = money(pos.total, &asset),
                    long = money(pos.long, &asset),
                    short = money(pos.short, &asset),
                )
            })
            .collect();
        for item in snapshot.pair_positions.iter().filter(|p| !p.size.is_zero()) {
            let asset =
                escape_html(crate::types::quote_asset(&item.symbol).unwrap_or(&item.symbol));
            let side = if item.size > Decimal::ZERO {
                "多"
            } else {
                "空"
            };
            lines.push(format!(
                "  - {symbol}（{side} {qty}）：{amount}｜浮盈 {unrealized}",
                symbol = escape_html(&item.symbol),
                qty = item.size.abs().normalize(),
                amount = money(item.position_amount, &asset),
                unrealized = signed_money(item.unrealized_pnl, &asset),
            ));
        }
        lines.join("\n")
    };

    let time = format_beijing_time(snapshot.generated_at);
    format!(
        "<b>📊 每日运行数据快照（{date}）</b>\n\n<b>【账户概览】</b>\n{account_lines}\n\n<b>【昨日成交与盈亏】</b>\n· 成交笔数：{total_trades} 笔（买 {total_buys} / 卖 {total_sells}）\n· 已实现盈亏：\n{pnl_lines}\n\n<b>【持仓概览】</b>\n{position_lines}\n\n生成时间：{time}（北京时间）",
        date = escape_html(&snapshot.date),
    )
}

fn format_test_message(now: DateTime<Utc>) -> String {
    let time = format_beijing_time(now);
    format!("<b>✅ Grid Console 测试消息</b>\n通知渠道可用。\n时间：{time}（北京时间）")
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
#[path = "telegram_tests.rs"]
mod tests;
