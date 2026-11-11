use crate::config::TelegramConfig;
use crate::fx::CnyRate;
use crate::types::{
    format_decimal_dp, AccountInfo, GridDirection, HourlyTradeStats, OrderSide, PositionInfo,
    TickerInfo, TradeRecord, TradingMode,
};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

const MAX_TELEGRAM_RESPONSE_BYTES: usize = 8 * 1024;

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
pub(crate) struct TelegramNotifier {
    client: Client,
    api_base_url: String,
}

impl TelegramNotifier {
    pub(crate) fn new() -> Self {
        Self {
            client: crate::exchange::client::shared_http_client(),
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

    pub(crate) async fn send_trade_notification(
        &self,
        config: &TelegramConfig,
        trade: &TradeRecord,
        context: &TradeNotificationContext,
    ) -> Result<()> {
        self.send_message(config, format_trade_message(trade, context))
            .await
    }

    pub(crate) async fn send_global_risk_notification(
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
    pub(crate) async fn send_test_message(&self, config: &TelegramConfig) -> Result<()> {
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
        let bytes =
            crate::exchange::client::read_bounded_bytes(response, MAX_TELEGRAM_RESPONSE_BYTES + 1)
                .await
                .ok()
                .filter(|bytes| bytes.len() <= MAX_TELEGRAM_RESPONSE_BYTES)
                .ok_or_else(|| {
                    anyhow!("Telegram returned an invalid response (HTTP {})", status)
                })?;
        let body: TelegramResponse = serde_json::from_slice(&bytes)
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
pub(crate) struct TradeNotificationContext {
    pub(crate) direction: GridDirection,
    pub(crate) today_count: Option<u64>,
    pub(crate) hour_count: Option<u64>,
    pub(crate) account_equity: Option<Decimal>,
    pub(crate) wallet_balance: Option<Decimal>,
    pub(crate) available_balance: Option<Decimal>,
    pub(crate) position_amount: Option<Decimal>,
    pub(crate) cny_per_unit: Option<Decimal>,
}

impl TradeNotificationContext {
    pub(crate) fn for_trade(
        trade: &TradeRecord,
        hourly_stats: Option<&HourlyTradeStats>,
        account: &AccountInfo,
        cny_rate: Option<&CnyRate>,
        now: DateTime<Utc>,
        sync_interval_secs: u64,
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
        let flat_paper = trade.mode == TradingMode::Paper
            && account.margin_balance == account.available_balance
            && account.unrealized_profit.is_zero();
        let max_age = if trade.mode == TradingMode::Paper {
            crate::types::MARKET_PRICE_MAX_AGE_SECS
        } else {
            crate::types::max_reconciliation_age_secs(sync_interval_secs)
        };
        let fresh =
            flat_paper || crate::types::is_timestamp_fresh(account.update_time, now, max_age);
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
                    && crate::types::is_timestamp_fresh(rate.updated_at, now, 3600)
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

    pub(crate) fn with_direction(mut self, direction: GridDirection) -> Self {
        self.direction = direction;
        self
    }

    pub(crate) fn with_position(
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

fn format_available_ratio(available: Option<Decimal>, wallet: Option<Decimal>) -> String {
    available
        .zip(wallet)
        .filter(|(_, wallet)| *wallet > Decimal::ZERO)
        .and_then(|(available, wallet)| available.checked_div(wallet))
        .and_then(|ratio| ratio.checked_mul(Decimal::ONE_HUNDRED))
        .map_or_else(
            || "--".to_string(),
            |value| format!("{}%", format_decimal_dp(value, 2, false)),
        )
}

pub(crate) fn format_trade_message(
    trade: &TradeRecord,
    context: &TradeNotificationContext,
) -> String {
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
    let ratio = format_available_ratio(context.available_balance, context.wallet_balance);
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

fn format_optional_money(value: Option<Decimal>, asset: &str, force_sign: bool) -> String {
    value.map_or_else(
        || format!("-- {asset}"),
        |n| format!("{} {asset}", format_decimal_dp(n, 2, force_sign)),
    )
}

fn format_daily_account_lines(accounts: &[crate::snapshots::DailyAccountGroup]) -> String {
    if accounts.is_empty() {
        return "暂无账户数据".to_string();
    }
    accounts
        .iter()
        .map(|acc| {
            let asset = escape_html(&acc.asset);
            let ratio = format_available_ratio(acc.available, acc.wallet);
            format!(
                "· {mode} {asset}：权益 {equity}｜可用 {available}（{ratio}）｜浮盈 {unrealized}",
                mode = mode_label(acc.mode),
                equity = format_optional_money(acc.equity, &asset, false),
                available = format_optional_money(acc.available, &asset, false),
                unrealized = format_optional_money(acc.unrealized, &asset, true),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_daily_pnl_lines(trades: &[crate::snapshots::DailyTradeGroup]) -> String {
    let mut pnl_groups: std::collections::BTreeMap<
        (&'static str, &str),
        (TradingMode, Decimal, u64),
    > = std::collections::BTreeMap::new();
    for trade in trades {
        let asset = crate::types::quote_asset(&trade.symbol).unwrap_or(&trade.symbol);
        let entry = pnl_groups.entry((trade.mode.as_str(), asset)).or_insert((
            trade.mode,
            Decimal::ZERO,
            0,
        ));
        entry.1 = entry.1.saturating_add(trade.realized_pnl);
        entry.2 = entry.2.saturating_add(trade.pending_pnl_count);
    }
    if pnl_groups.is_empty() {
        return "· 0.00".to_string();
    }
    pnl_groups
        .into_iter()
        .map(|((_, asset), (mode, pnl, pending))| {
            let asset = escape_html(asset);
            let formatted_pnl = format_decimal_dp(pnl, 4, true);
            if pending > 0 {
                format!(
                    "· {mode} {asset}：{formatted_pnl} {asset}（{pending} 笔待同步）",
                    mode = mode_label(mode),
                )
            } else {
                format!(
                    "· {mode} {asset}：{formatted_pnl} {asset}",
                    mode = mode_label(mode)
                )
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_daily_position_lines(
    positions: &[crate::snapshots::DailyPositionGroup],
    pair_positions: &[crate::snapshots::DailyPositionItem],
) -> String {
    if positions.is_empty() {
        return "暂无持仓数据".to_string();
    }
    let mut lines: Vec<String> = positions
        .iter()
        .map(|pos| {
            let asset = escape_html(&pos.asset);
            format!(
                "· {mode} {asset}：总持仓 {total}（多 {long} / 空 {short}）",
                mode = mode_label(pos.mode),
                total = format_optional_money(pos.total, &asset, false),
                long = format_optional_money(pos.long, &asset, false),
                short = format_optional_money(pos.short, &asset, false),
            )
        })
        .collect();
    for item in pair_positions.iter().filter(|p| !p.size.is_zero()) {
        let asset = escape_html(crate::types::quote_asset(&item.symbol).unwrap_or(&item.symbol));
        let side = if item.size > Decimal::ZERO {
            "多"
        } else {
            "空"
        };
        lines.push(format!(
            "  - {symbol}（{side} {qty}）：{amount}｜浮盈 {unrealized}",
            symbol = escape_html(&item.symbol),
            qty = item.size.abs().normalize(),
            amount = format_optional_money(item.position_amount, &asset, false),
            unrealized = format_optional_money(item.unrealized_pnl, &asset, true),
        ));
    }
    lines.join("\n")
}

pub(crate) fn format_daily_snapshot_message(snapshot: &crate::snapshots::DailySnapshot) -> String {
    let account_lines = format_daily_account_lines(&snapshot.accounts);
    let total_buys = snapshot
        .trades
        .iter()
        .fold(0u64, |acc, t| acc.saturating_add(t.buy_count));
    let total_sells = snapshot
        .trades
        .iter()
        .fold(0u64, |acc, t| acc.saturating_add(t.sell_count));
    let total_trades = total_buys.saturating_add(total_sells);
    let pnl_lines = format_daily_pnl_lines(&snapshot.trades);
    let position_lines = format_daily_position_lines(&snapshot.positions, &snapshot.pair_positions);
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
mod tests;
