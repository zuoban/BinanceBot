use crate::config::NtfyConfig;
use crate::telegram::daily_snapshot::format_daily_snapshot_plain_body;
use crate::telegram::{
    format_beijing_time, format_optional_money, format_reverse_plain_parts, ReverseNotification,
    TradeNotificationContext,
};
use crate::types::{format_decimal_dp, OrderSide, TradeRecord};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

const MAX_NTFY_RESPONSE_BYTES: usize = 8 * 1024;
const MAX_NTFY_TITLE_BYTES: usize = 256;
const MAX_NTFY_MESSAGE_BYTES: usize = 4096;
const TITLE_TRUNCATION_SUFFIX: &str = "…";
const MESSAGE_TRUNCATION_SUFFIX: &str = "\n…（已截断）";
const DEFAULT_PRIORITY: u8 = 3;
const HIGH_PRIORITY: u8 = 4;

#[derive(Serialize)]
struct PublishMessage<'a> {
    topic: &'a str,
    title: String,
    message: String,
    priority: u8,
}

#[derive(Deserialize)]
struct NtfyResponse {
    id: Option<String>,
    topic: Option<String>,
    error: Option<String>,
}

#[derive(Clone)]
pub(crate) struct NtfyNotifier {
    client: Client,
}

impl NtfyNotifier {
    pub(crate) fn new() -> Self {
        Self {
            client: crate::exchange::client::shared_aux_http_client(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_client(client: Client) -> Self {
        Self { client }
    }

    pub(crate) async fn send_trade_notification(
        &self,
        config: &NtfyConfig,
        trade: &TradeRecord,
        context: &TradeNotificationContext,
    ) -> Result<()> {
        let (title, message) = format_ntfy_trade_message(trade, context);
        self.send_message(config, title, message, DEFAULT_PRIORITY)
            .await
    }

    pub(crate) async fn send_global_risk_notification(
        &self,
        config: &NtfyConfig,
        reason: &str,
    ) -> Result<()> {
        let (title, message) = format_ntfy_global_risk_message(reason, Utc::now());
        self.send_message(config, title, message, HIGH_PRIORITY)
            .await
    }

    pub(crate) async fn send_reverse_notification(
        &self,
        config: &NtfyConfig,
        notification: &ReverseNotification,
    ) -> Result<()> {
        let (title, message) = format_ntfy_reverse_message(notification);
        let priority = if notification.failure_reason.is_some() {
            HIGH_PRIORITY
        } else {
            DEFAULT_PRIORITY
        };
        self.send_message(config, title, message, priority).await
    }

    pub(crate) async fn send_daily_snapshot_notification(
        &self,
        config: &NtfyConfig,
        snapshot: &crate::snapshots::DailySnapshot,
    ) -> Result<()> {
        let (title, message) = format_ntfy_daily_snapshot_message(snapshot);
        self.send_message(config, title, message, DEFAULT_PRIORITY)
            .await
    }

    /// Deliver a fixed probe message so settings can be verified before they are saved.
    pub(crate) async fn send_test_message(&self, config: &NtfyConfig) -> Result<()> {
        let (title, message) = format_ntfy_test_message(Utc::now());
        self.send_message(config, title, message, DEFAULT_PRIORITY)
            .await
    }

    async fn send_message(
        &self,
        config: &NtfyConfig,
        title: String,
        message: String,
        priority: u8,
    ) -> Result<()> {
        let server_url = config.effective_server_url();
        let topic = config.topic.trim();
        let token = config.token.trim();
        if topic.is_empty() {
            return Err(anyhow!("ntfy Topic 不能为空"));
        }
        NtfyConfig::validate_server_url(server_url)?;
        NtfyConfig::validate_topic(topic)?;
        if !token.is_empty() {
            NtfyConfig::validate_token(token)?;
        }
        let endpoint = url::Url::parse(server_url).map_err(|_| anyhow!("ntfy 服务地址格式无效"))?;
        let title = truncate_utf8_bytes(title, MAX_NTFY_TITLE_BYTES, TITLE_TRUNCATION_SUFFIX);
        let message =
            truncate_utf8_bytes(message, MAX_NTFY_MESSAGE_BYTES, MESSAGE_TRUNCATION_SUFFIX);
        let mut request = self
            .client
            .post(endpoint)
            .timeout(std::time::Duration::from_secs(10));
        if !token.is_empty() {
            request = request.bearer_auth(token);
        }
        let response = request
            .json(&PublishMessage {
                topic,
                title,
                message,
                priority,
            })
            .send()
            .await
            .map_err(|err| {
                let kind = if err.is_timeout() {
                    "timeout"
                } else {
                    "network error"
                };
                anyhow!("ntfy request failed ({kind})")
            })?;
        let status = response.status();
        let bytes =
            crate::exchange::client::read_bounded_bytes(response, MAX_NTFY_RESPONSE_BYTES + 1)
                .await
                .ok()
                .filter(|bytes| bytes.len() <= MAX_NTFY_RESPONSE_BYTES)
                .ok_or_else(|| anyhow!("ntfy returned an invalid response (HTTP {})", status))?;
        if !status.is_success() {
            let detail = serde_json::from_slice::<NtfyResponse>(&bytes)
                .ok()
                .and_then(|body| body.error)
                .map(|err| crate::exchange::client::summarize_non_json_body(&err))
                .unwrap_or_else(|| {
                    crate::exchange::client::summarize_non_json_body(&String::from_utf8_lossy(
                        &bytes,
                    ))
                });
            return Err(anyhow!(
                "ntfy rejected notification (HTTP {}): {}",
                status,
                detail
            ));
        }
        let body: NtfyResponse = serde_json::from_slice(&bytes)
            .map_err(|_| anyhow!("ntfy returned an invalid response (HTTP {})", status))?;
        if let Some(err) = body.error {
            let detail = crate::exchange::client::summarize_non_json_body(&err);
            return Err(anyhow!(
                "ntfy rejected notification (HTTP {}): {}",
                status,
                detail
            ));
        }
        if body.id.as_deref().is_none_or(|id| id.trim().is_empty())
            || body
                .topic
                .as_deref()
                .is_some_and(|returned| returned != topic)
        {
            return Err(anyhow!(
                "ntfy returned an invalid response (HTTP {})",
                status
            ));
        }
        Ok(())
    }
}

pub(crate) fn format_ntfy_trade_message(
    trade: &TradeRecord,
    context: &TradeNotificationContext,
) -> (String, String) {
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
    let title = format!(
        "{icon} {direction}｜{symbol}｜{average_price}｜{amount} {quote}",
        direction = trade.side.as_str(),
        symbol = trade.symbol,
        amount = format_decimal_dp(trade.amount_usdc, 2, false),
    );
    let message = format!(
        "成交笔数：{today}D｜{hour}H\n持仓金额：{position_amount}{pnl_line}",
        today = count(context.today_count),
        hour = count(context.hour_count),
        position_amount = money(context.position_amount),
    );
    (title, message)
}

pub(crate) fn format_ntfy_global_risk_message(
    reason: &str,
    now: DateTime<Utc>,
) -> (String, String) {
    let time = format_beijing_time(now);
    (
        "⚠️ 全局风控已触发".to_string(),
        format!("原因：{reason}\n时间：{time}（北京时间）"),
    )
}

pub(crate) fn format_ntfy_reverse_message(notification: &ReverseNotification) -> (String, String) {
    format_reverse_plain_parts(notification)
}

pub(crate) fn format_ntfy_daily_snapshot_message(
    snapshot: &crate::snapshots::DailySnapshot,
) -> (String, String) {
    (
        format!("📊 每日运行数据快照（{}）", snapshot.date),
        format_daily_snapshot_plain_body(snapshot),
    )
}

pub(crate) fn format_ntfy_test_message(now: DateTime<Utc>) -> (String, String) {
    let time = format_beijing_time(now);
    (
        "✅ Grid Console 测试消息".to_string(),
        format!("通知渠道可用。\n时间：{time}（北京时间）"),
    )
}

pub(crate) fn truncate_utf8_bytes(text: String, max_bytes: usize, suffix: &str) -> String {
    if text.len() <= max_bytes {
        return text;
    }
    let budget = max_bytes.saturating_sub(suffix.len());
    let mut cut = budget.min(text.len());
    while cut > 0 && !text.is_char_boundary(cut) {
        cut -= 1;
    }
    let mut out = String::with_capacity(cut + suffix.len());
    out.push_str(&text[..cut]);
    out.push_str(suffix);
    out
}

#[cfg(test)]
mod tests;
