use super::{json_err, json_ok, ApiResponse};
use crate::config::AppConfig;
use crate::exchange::rate_limit::RateLimiter;
use crate::exchange::BinanceFuturesClient;
use crate::runtime::RuntimeHandle;
use crate::types::non_blank;
use axum::extract::State;
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
pub(super) struct TestExchangePayload {
    api_key: Option<String>,
    api_secret: Option<String>,
    recv_window: Option<u64>,
}

impl std::fmt::Debug for TestExchangePayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestExchangePayload")
            .field("api_key", &self.api_key.as_ref().map(|_| "[REDACTED]"))
            .field(
                "api_secret",
                &self.api_secret.as_ref().map(|_| "[REDACTED]"),
            )
            .field("recv_window", &self.recv_window)
            .finish()
    }
}

#[derive(Deserialize)]
pub(super) struct TestTelegramPayload {
    bot_token: Option<String>,
    chat_id: Option<String>,
}

impl std::fmt::Debug for TestTelegramPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestTelegramPayload")
            .field("bot_token", &self.bot_token.as_ref().map(|_| "[REDACTED]"))
            .field("chat_id", &self.chat_id)
            .finish()
    }
}

/// Resolve probe credentials: a non-blank override wins, otherwise the saved value is used.
pub(super) fn resolve_exchange_probe(
    saved: &crate::config::ExchangeConfig,
    payload: TestExchangePayload,
) -> Result<crate::config::ExchangeConfig, String> {
    let mut config = saved.clone();
    if let Some(key) = non_blank(payload.api_key) {
        config.api_key = key;
    }
    if let Some(secret) = non_blank(payload.api_secret) {
        config.api_secret = secret;
    }
    if let Some(window) = payload.recv_window {
        config.recv_window = window;
    }
    if config.api_key.trim().is_empty() || config.api_secret.trim().is_empty() {
        return Err("请同时填写 API Key 和 Secret（或先保存后再测试）".into());
    }
    crate::config::ExchangeConfig::validate_api_key(&config.api_key).map_err(|e| e.to_string())?;
    if !(1..=60_000).contains(&config.recv_window) {
        return Err("请求有效窗口必须在 1-60000 毫秒之间".into());
    }
    Ok(config)
}

fn resolve_telegram_probe(
    saved: &crate::config::TelegramConfig,
    payload: TestTelegramPayload,
) -> Result<crate::config::TelegramConfig, String> {
    let mut config = saved.clone();
    if let Some(token) = non_blank(payload.bot_token) {
        config.bot_token = token;
    }
    if let Some(chat_id) = non_blank(payload.chat_id) {
        config.chat_id = chat_id;
    }
    if config.bot_token.trim().is_empty() || config.chat_id.trim().is_empty() {
        return Err("请填写 Bot Token 和 Chat ID（或先保存后再测试）".into());
    }
    Ok(config)
}

/// Probes talk to the exchange with credentials the engine does not use, so they get their own
/// request budget and a short cooldown: repeated clicks must never consume the engine's weight.
pub(super) const PROBE_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(3);

#[derive(Default)]
pub(crate) struct ProbeGate(std::sync::Mutex<Option<std::time::Instant>>);

impl ProbeGate {
    #[cfg(test)]
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Err carries the whole seconds left to wait (at least one).
    pub(super) fn enter(&self, now: std::time::Instant) -> Result<(), u64> {
        let mut last = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(wait) = last
            .and_then(|at| PROBE_COOLDOWN.checked_sub(now.saturating_duration_since(at)))
            .filter(|wait| !wait.is_zero())
        {
            return Err(wait.as_secs() + 1);
        }
        *last = Some(now);
        Ok(())
    }
}

/// One gate per probe kind, owned by the runtime so tests and instances never share cooldowns.
#[derive(Default)]
pub(crate) struct ProbeGates {
    pub(crate) exchange: ProbeGate,
    pub(crate) telegram: ProbeGate,
}

fn probe_throttled(gate: &ProbeGate) -> Option<Json<ApiResponse<String>>> {
    gate.enter(std::time::Instant::now())
        .err()
        .map(|wait| json_err(format!("测试过于频繁，请 {wait} 秒后再试")))
}

async fn load_probe_config<T, P>(
    handle: &RuntimeHandle,
    gate: &ProbeGate,
    payload: P,
    resolve: impl FnOnce(&AppConfig, P) -> Result<T, String>,
) -> Result<T, Json<ApiResponse<String>>> {
    if let Some(throttled) = probe_throttled(gate) {
        return Err(throttled);
    }
    let Ok(saved) = handle.db.load_saved_config().await else {
        return Err(json_err("无法读取全局设置"));
    };
    resolve(&saved, payload).map_err(json_err)
}

/// Verify that the (possibly unsaved) API credentials can read the futures account.
/// Nothing is persisted, and failures never echo signed URLs or credentials.
pub(super) async fn test_exchange_handler(
    State(handle): State<RuntimeHandle>,
    Json(payload): Json<TestExchangePayload>,
) -> Json<ApiResponse<String>> {
    let config = match load_probe_config(
        &handle,
        &handle.probes.exchange,
        payload,
        |saved, payload| resolve_exchange_probe(&saved.exchange, payload),
    )
    .await
    {
        Ok(config) => config,
        Err(response) => return response,
    };
    let client = BinanceFuturesClient::new_with_limiter(&config, Arc::new(RateLimiter::default()));
    if let Err(error) = client.sync_server_time().await {
        return json_err(sanitized_probe_error("无法连接币安", &error));
    }
    match client.get_account().await {
        Ok(_) => json_ok("连接成功：API Key 有效，可读取合约账户".into()),
        Err(error) => json_err(sanitized_probe_error("账户验证失败", &error)),
    }
}

fn sanitized_probe_error(prefix: &str, error: &anyhow::Error) -> String {
    if error.downcast_ref::<reqwest::Error>().is_some() {
        return format!("{prefix}：网络请求失败，请检查网络后重试");
    }
    format!("{prefix}：{error}")
}

/// Send one fixed probe message with the (possibly unsaved) Telegram credentials.
pub(super) async fn test_telegram_handler(
    State(handle): State<RuntimeHandle>,
    Json(payload): Json<TestTelegramPayload>,
) -> Json<ApiResponse<String>> {
    let config = match load_probe_config(
        &handle,
        &handle.probes.telegram,
        payload,
        |saved, payload| resolve_telegram_probe(&saved.telegram, payload),
    )
    .await
    {
        Ok(config) => config,
        Err(response) => return response,
    };
    let notifier = crate::telegram::TelegramNotifier::new();
    match notifier.send_test_message(&config).await {
        Ok(()) => json_ok("测试消息已发送，请在 Telegram 中查看".into()),
        Err(error) => json_err(format!("测试消息发送失败：{error}")),
    }
}
