use crate::config::TelegramConfig;
use crate::types::{
    format_decimal_dp, BotStatus, GridDirection, HourlyTradeStats, OrderSide, PositionInfo,
    TickerInfo, TradeRecord, TradingMode,
};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub(crate) mod daily_snapshot;
pub(crate) use daily_snapshot::format_daily_snapshot_message;

const MAX_TELEGRAM_RESPONSE_BYTES: usize = 8 * 1024;
const MAX_TELEGRAM_UTF16_UNITS: usize = 4096;
const TRUNCATION_SUFFIX: &str = "\n…（已截断）";
const TRUNCATION_SUFFIX_UTF16_UNITS: usize = 7;

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
            client: crate::exchange::client::shared_aux_http_client(),
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

    pub(crate) async fn send_reverse_notification(
        &self,
        config: &TelegramConfig,
        notification: &ReverseNotification,
    ) -> Result<()> {
        self.send_message(config, format_reverse_message(notification))
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
        let bot_token = config.bot_token.trim();
        let chat_id = config.chat_id.trim();
        if bot_token.is_empty() || chat_id.is_empty() {
            return Err(anyhow!("Bot Token or Chat ID is empty"));
        }
        TelegramConfig::validate_bot_token(bot_token)?;
        TelegramConfig::validate_chat_id(chat_id)?;
        let text = truncate_telegram_html(text);
        let endpoint = format!("{}/bot{}/sendMessage", self.api_base_url, bot_token);
        let response = self
            .client
            .post(endpoint)
            .timeout(std::time::Duration::from_secs(10))
            .json(&SendMessage {
                chat_id,
                text,
                parse_mode: "HTML",
            })
            .send()
            .await
            .map_err(|err| {
                let kind = if err.is_timeout() {
                    "timeout"
                } else {
                    "network error"
                };
                anyhow!("Telegram request failed ({kind})")
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
            let detail = body.description.map_or_else(
                || "unknown error".to_string(),
                |desc| crate::exchange::client::summarize_non_json_body(&desc),
            );
            return Err(anyhow!(
                "Telegram rejected notification (HTTP {}): {}",
                status,
                detail
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
    pub(crate) position_amount: Option<Decimal>,
    reference_time: Option<DateTime<Utc>>,
    max_age_secs: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct ReverseNotification {
    pub(crate) symbol: String,
    pub(crate) mode: TradingMode,
    pub(crate) from_direction: GridDirection,
    pub(crate) to_direction: GridDirection,
    pub(crate) effective_direction: GridDirection,
    pub(crate) closed_qty: Decimal,
    pub(crate) close_avg_price: Option<Decimal>,
    pub(crate) opened_qty: Decimal,
    pub(crate) open_avg_price: Option<Decimal>,
    pub(crate) status: BotStatus,
    pub(crate) failure_reason: Option<String>,
    pub(crate) timestamp: DateTime<Utc>,
}

pub(crate) fn bot_status_label(status: BotStatus) -> &'static str {
    match status {
        BotStatus::Running => "运行中",
        BotStatus::Paused => "已暂停",
        BotStatus::Stopped => "已停止",
        BotStatus::Error => "异常",
    }
}

fn format_leg_execution(qty: Decimal, avg_price: Option<Decimal>) -> String {
    if qty <= Decimal::ZERO {
        return "0".to_string();
    }
    match avg_price.filter(|price| *price > Decimal::ZERO) {
        Some(price) => format!("{} @ {}", qty.normalize(), price.normalize()),
        None => qty.normalize().to_string(),
    }
}

pub(crate) fn format_reverse_plain_parts(notification: &ReverseNotification) -> (String, String) {
    let header_prefix = if notification.failure_reason.is_some() {
        "⚠️ 反手交易异常暂停"
    } else {
        "🔄 反手交易已完成"
    };
    let title = format!(
        "{header_prefix}｜{}｜{}",
        notification.symbol,
        mode_label(notification.mode)
    );
    let direction_note = if notification.effective_direction != notification.to_direction {
        format!(
            "（当前仍为{}）",
            notification.effective_direction.label_zh()
        )
    } else {
        String::new()
    };
    let position_line = if notification.closed_qty <= Decimal::ZERO
        && notification.opened_qty <= Decimal::ZERO
        && notification.failure_reason.is_none()
    {
        "空仓切换方向（未提交市价单）".to_string()
    } else {
        format!(
            "平仓 {}，反向开仓 {}",
            format_leg_execution(notification.closed_qty, notification.close_avg_price),
            format_leg_execution(notification.opened_qty, notification.open_avg_price),
        )
    };
    let failure_line = notification
        .failure_reason
        .as_deref()
        .map(|reason| format!("\n异常说明：{reason}"))
        .unwrap_or_default();
    let time = format_beijing_time(notification.timestamp);
    let body = format!(
        "方向变更：{} → {}{direction_note}\n仓位执行：{position_line}\n当前状态：{}{failure_line}\n时间：{time}（北京时间）",
        notification.from_direction.label_zh(),
        notification.to_direction.label_zh(),
        bot_status_label(notification.status),
    );
    (title, body)
}

pub(crate) fn format_reverse_message(notification: &ReverseNotification) -> String {
    let (title, body) = format_reverse_plain_parts(notification);
    format!("<b>{}</b>\n{}", escape_html(&title), escape_html(&body))
}

impl TradeNotificationContext {
    pub(crate) fn for_trade(
        trade: &TradeRecord,
        hourly_stats: Option<&HourlyTradeStats>,
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
        let max_age = if trade.mode == TradingMode::Paper {
            crate::types::MARKET_PRICE_MAX_AGE_SECS
        } else {
            crate::types::max_reconciliation_age_secs(sync_interval_secs)
        };
        Self {
            direction: GridDirection::Long,
            today_count,
            hour_count,
            position_amount: None,
            reference_time: Some(now),
            max_age_secs: max_age,
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
        let ref_time = self.reference_time.unwrap_or(trade.timestamp);
        let pos_max_age = if self.max_age_secs > 0 {
            self.max_age_secs
        } else if trade.mode == TradingMode::Paper {
            crate::types::MARKET_PRICE_MAX_AGE_SECS
        } else {
            crate::types::max_reconciliation_age_secs(3)
        };
        let valuation_price = if ticker.symbol == trade.symbol {
            let ticker_mark_fresh = ticker.mark_price > Decimal::ZERO
                && crate::types::is_timestamp_fresh(
                    ticker.mark_update_time,
                    ref_time,
                    crate::types::MARKET_PRICE_MAX_AGE_SECS,
                );
            let pos_mark_fresh = position.mark_price > Decimal::ZERO
                && position
                    .update_time
                    .is_some_and(|ts| crate::types::is_timestamp_fresh(ts, ref_time, pos_max_age));
            let ticker_last_fresh = ticker.last_price > Decimal::ZERO
                && crate::types::is_timestamp_fresh(
                    ticker.update_time,
                    ref_time,
                    crate::types::MARKET_PRICE_MAX_AGE_SECS,
                );
            if ticker_mark_fresh || pos_mark_fresh {
                position.valuation_price_with_freshness(ticker, ticker_mark_fresh, pos_mark_fresh)
            } else if ticker_last_fresh {
                ticker.last_price
            } else if ticker.mark_price > Decimal::ZERO
                && ticker.mark_update_time != DateTime::<Utc>::default()
                && (position.mark_price > Decimal::ZERO
                    || (ticker.last_price > Decimal::ZERO
                        && (ticker.update_time > ticker.mark_update_time
                            || ticker.update_time == DateTime::<Utc>::default())))
            {
                if position.mark_price > Decimal::ZERO
                    && position
                        .update_time
                        .is_none_or(|pos_ts| pos_ts >= ticker.mark_update_time)
                {
                    position.mark_price
                } else if ticker.last_price > Decimal::ZERO && position.mark_price <= Decimal::ZERO
                {
                    ticker.last_price
                } else {
                    position.market_valuation_price(Some(ticker))
                }
            } else {
                position.market_valuation_price(Some(ticker))
            }
        } else {
            position.market_valuation_price(None)
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

pub(crate) fn format_available_ratio(
    available: Option<Decimal>,
    wallet: Option<Decimal>,
) -> String {
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
    let money = |value: Option<Decimal>| format_optional_money(value, quote, false);
    let pnl_line = match (
        trade.side == context.direction.closing_side(),
        trade.pnl_verified,
    ) {
        (true, true) => format!(
            "\n已实现盈亏：{} {quote}",
            format_decimal_dp(trade.realized_pnl, 4, true)
        ),
        (true, false) => "\n已实现盈亏：待同步".to_string(),
        (false, _) => String::new(),
    };
    format!(
        "<b>{icon} {direction}｜{symbol}｜{average_price}｜{amount} {quote}</b>\n成交笔数：{today}D｜{hour}H\n持仓金额：{position_amount}{pnl_line}",
        direction = trade.side.as_str(),
        symbol = escape_html(&trade.symbol),
        amount = format_decimal_dp(trade.amount_usdc, 2, false),
        today = count(context.today_count),
        hour = count(context.hour_count),
        position_amount = money(context.position_amount),
    )
}

pub(crate) fn format_beijing_time(now: DateTime<Utc>) -> String {
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

pub(crate) fn mode_label(mode: TradingMode) -> &'static str {
    match mode {
        TradingMode::Paper => "模拟盘",
        TradingMode::Live => "实盘",
        TradingMode::Unknown => "未知模式",
    }
}

pub(crate) fn format_optional_money(
    value: Option<Decimal>,
    asset: &str,
    force_sign: bool,
) -> String {
    value.map_or_else(
        || format!("-- {asset}"),
        |n| format!("{} {asset}", format_decimal_dp(n, 2, force_sign)),
    )
}

fn format_test_message(now: DateTime<Utc>) -> String {
    let time = format_beijing_time(now);
    format!("<b>✅ Grid Console 测试消息</b>\n通知渠道可用。\n时间：{time}（北京时间）")
}

pub(super) fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

pub(crate) fn truncate_telegram_html(text: String) -> String {
    let budget = MAX_TELEGRAM_UTF16_UNITS - TRUNCATION_SUFFIX_UTF16_UNITS;
    let (mut idx, mut units, mut bold_depth) = (0usize, 0usize, 0usize);
    let (mut cut_idx, mut cut_bold_depth) = (0usize, 0usize);
    while idx < text.len() {
        let rest = &text[idx..];
        let (step, delta, open_bold) = if rest.starts_with("<b>") {
            bold_depth += 1;
            (3, 0, true)
        } else if rest.starts_with("</b>") {
            bold_depth = bold_depth.saturating_sub(1);
            (4, 0, false)
        } else if let Some(entity) = ["&amp;", "&lt;", "&gt;", "&quot;"]
            .into_iter()
            .find(|entity| rest.starts_with(entity))
        {
            (entity.len(), 1, false)
        } else {
            let ch = rest.chars().next().expect("non-empty slice");
            (ch.len_utf8(), ch.len_utf16(), false)
        };
        idx += step;
        units += delta;
        if units <= budget && !open_bold {
            cut_idx = idx;
            cut_bold_depth = bold_depth;
        }
    }
    if units <= MAX_TELEGRAM_UTF16_UNITS {
        return text;
    }
    let mut out = String::with_capacity(cut_idx + cut_bold_depth * 4 + TRUNCATION_SUFFIX.len());
    out.push_str(&text[..cut_idx]);
    for _ in 0..cut_bold_depth {
        out.push_str("</b>");
    }
    out.push_str(TRUNCATION_SUFFIX);
    out
}

#[cfg(test)]
mod tests;
