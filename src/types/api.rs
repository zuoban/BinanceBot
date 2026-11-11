use super::{non_blank, AccountInfo, BotStatus, GridDirection, TradeRecord, TradingMode};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingPairView {
    pub symbol: String,
    /// True when this pair has a stable /api/pairs/{symbol} route.
    pub scoped: bool,
    pub status: BotStatus,
    pub mode: TradingMode,
    pub grid_config: crate::config::GridConfigSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverviewPairView {
    pub scoped: bool,
    pub mode: TradingMode,
    pub snapshot: PairSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairSummary {
    pub symbol: String,
    pub status: BotStatus,
    pub mode: TradingMode,
    pub grid_config: crate::config::GridConfigSummary,
    pub ticker: SummaryTicker,
    pub position: SummaryPosition,
    pub stats: SummaryStats,
    pub today_trade_counts: Option<TodayTradeCounts>,
    pub account: AccountInfo,
    pub account_cny_rate: Option<crate::fx::CnyRate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryTicker {
    pub mark_price: Decimal,
    pub mark_update_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryPosition {
    pub size: Decimal,
    pub entry_price: Decimal,
    pub mark_price: Decimal,
    pub unrealized_pnl: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryStats {
    pub total_realized_pnl: Decimal,
    pub active_buy_orders: usize,
    pub active_sell_orders: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TodayTradeCounts {
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub buy: u64,
    pub sell: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverviewSnapshot {
    pub generated_at: DateTime<Utc>,
    /// Each account belongs to its pair snapshot and may represent the same wallet.
    /// Consumers must not sum account balances across pairs.
    pub pairs: Vec<OverviewPairView>,
    pub today_pnl: Option<TodayPnl>,
    pub recent_trades: Vec<TradeRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TodayPnlGroup {
    pub symbol: String,
    pub mode: TradingMode,
    pub realized_pnl: Decimal,
    pub pending_pnl_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TodayPnl {
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub groups: Vec<TodayPnlGroup>,
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct UpdateGlobalConfigPayload {
    pub baseline_version: Option<String>,
    pub recv_window: Option<u64>,
    pub sync_interval_secs: Option<u64>,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    #[serde(default)]
    pub clear_api_credentials: bool,
    #[serde(default)]
    pub clear_telegram_credentials: bool,
    pub telegram_enabled: Option<bool>,
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal_update",
        skip_serializing_if = "Option::is_none"
    )]
    pub global_max_position_notional: Option<Option<Decimal>>,
    pub pause_on_zero_available_margin: Option<bool>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal_update",
        skip_serializing_if = "Option::is_none"
    )]
    pub min_available_margin_pct: Option<Option<Decimal>>,
}

impl std::fmt::Debug for UpdateGlobalConfigPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            baseline_version,
            recv_window,
            sync_interval_secs,
            api_key,
            api_secret,
            clear_api_credentials,
            clear_telegram_credentials,
            telegram_enabled,
            telegram_bot_token,
            telegram_chat_id,
            global_max_position_notional,
            pause_on_zero_available_margin,
            min_available_margin_pct,
        } = self;
        f.debug_struct("UpdateGlobalConfigPayload")
            .field("baseline_version", baseline_version)
            .field("recv_window", recv_window)
            .field("sync_interval_secs", sync_interval_secs)
            .field("api_key", &crate::config::redact_optional_secret(api_key))
            .field(
                "api_secret",
                &crate::config::redact_optional_secret(api_secret),
            )
            .field("clear_api_credentials", clear_api_credentials)
            .field("clear_telegram_credentials", clear_telegram_credentials)
            .field("telegram_enabled", telegram_enabled)
            .field(
                "telegram_bot_token",
                &crate::config::redact_optional_secret(telegram_bot_token),
            )
            .field("telegram_chat_id", telegram_chat_id)
            .field("global_max_position_notional", global_max_position_notional)
            .field(
                "pause_on_zero_available_margin",
                pause_on_zero_available_margin,
            )
            .field("min_available_margin_pct", min_available_margin_pct)
            .finish()
    }
}

fn deserialize_optional_decimal_update<'de, D>(
    deserializer: D,
) -> Result<Option<Option<Decimal>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<Decimal>::deserialize(deserializer).map(Some)
}

impl UpdateGlobalConfigPayload {
    pub fn apply_to(self, config: &mut crate::config::GlobalConfig) {
        if let Some(value) = self.pause_on_zero_available_margin {
            config.pause_on_zero_available_margin = value;
        }
        if let Some(value) = self.min_available_margin_pct {
            config.min_available_margin_pct = value;
        }
        if let Some(value) = self.global_max_position_notional {
            config.global_max_position_notional = value;
        }
        if let Some(value) = self.recv_window {
            config.recv_window = value;
        }
        if let Some(value) = self.sync_interval_secs {
            config.sync_interval_secs = value;
        }
        if self.clear_api_credentials {
            config.api_key.clear();
            config.api_secret.clear();
        } else {
            for (target, value) in [
                (&mut config.api_key, self.api_key),
                (&mut config.api_secret, self.api_secret),
            ] {
                if let Some(value) = non_blank(value) {
                    *target = value;
                }
            }
        }
        if let Some(enabled) = self.telegram_enabled {
            config.telegram.enabled = enabled;
        }
        if self.clear_telegram_credentials {
            config.telegram.bot_token.clear();
            config.telegram.chat_id.clear();
        } else {
            for (target, value) in [
                (&mut config.telegram.bot_token, self.telegram_bot_token),
                (&mut config.telegram.chat_id, self.telegram_chat_id),
            ] {
                if let Some(value) = non_blank(value) {
                    *target = value;
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WebGlobalConfigView {
    pub version: String,
    pub pause_on_zero_available_margin: bool,
    pub min_available_margin_pct: Option<Decimal>,
    pub global_max_position_notional: Option<Decimal>,
    pub recv_window: u64,
    pub sync_interval_secs: u64,
    pub has_api_key: bool,
    pub has_api_secret: bool,
    pub api_key_preview: String,
    pub telegram_enabled: bool,
    pub has_telegram_bot_token: bool,
    pub telegram_chat_id: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct UpdateConfigPayload {
    #[serde(default)]
    pub baseline_version: Option<String>,
    pub symbol: String,
    #[serde(default)]
    pub direction: Option<GridDirection>,
    pub grid_interval: Decimal,
    pub order_amount_usdc: Decimal,
    pub buy_window: usize,
    pub sell_window: usize,
    #[serde(default)]
    pub post_only: Option<bool>,
    /// Retired: the run mode now derives from API credentials. Rejected so a
    /// stale client cannot believe it is still switching modes.
    #[serde(default)]
    pub dry_run: Option<bool>,
    /// Retired: testnet mode was removed. Only rejected so a stale client
    /// cannot silently turn its testnet configuration into live trading.
    #[serde(default)]
    pub is_testnet: Option<bool>,
    #[serde(default)]
    pub recv_window: Option<u64>,
    pub sync_interval_secs: Option<u64>,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    #[serde(default)]
    pub clear_api_credentials: bool,
    #[serde(default)]
    pub clear_telegram_credentials: bool,
    #[serde(default)]
    pub telegram_enabled: Option<bool>,
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
    pub min_price: Option<Decimal>,
    pub max_price: Option<Decimal>,
    pub max_position_usdc: Option<Decimal>,
    #[serde(default)]
    pub pause_on_max_position: Option<bool>,
    #[serde(default)]
    pub cancel_opening_on_max_position: Option<bool>,
    /// Replaces the saved value; omitted or null clears it.
    #[serde(default)]
    pub min_liquidation_distance_pct: Option<Decimal>,
}

impl std::fmt::Debug for UpdateConfigPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            baseline_version,
            symbol,
            direction,
            grid_interval,
            order_amount_usdc,
            buy_window,
            sell_window,
            post_only,
            dry_run,
            is_testnet,
            recv_window,
            sync_interval_secs,
            api_key,
            api_secret,
            clear_api_credentials,
            clear_telegram_credentials,
            telegram_enabled,
            telegram_bot_token,
            telegram_chat_id,
            min_price,
            max_price,
            max_position_usdc,
            pause_on_max_position,
            cancel_opening_on_max_position,
            min_liquidation_distance_pct,
        } = self;
        f.debug_struct("UpdateConfigPayload")
            .field("baseline_version", baseline_version)
            .field("symbol", symbol)
            .field("direction", direction)
            .field("grid_interval", grid_interval)
            .field("order_amount_usdc", order_amount_usdc)
            .field("buy_window", buy_window)
            .field("sell_window", sell_window)
            .field("post_only", post_only)
            .field("dry_run", dry_run)
            .field("is_testnet", is_testnet)
            .field("recv_window", recv_window)
            .field("sync_interval_secs", sync_interval_secs)
            .field("api_key", &crate::config::redact_optional_secret(api_key))
            .field(
                "api_secret",
                &crate::config::redact_optional_secret(api_secret),
            )
            .field("clear_api_credentials", clear_api_credentials)
            .field("clear_telegram_credentials", clear_telegram_credentials)
            .field("telegram_enabled", telegram_enabled)
            .field(
                "telegram_bot_token",
                &crate::config::redact_optional_secret(telegram_bot_token),
            )
            .field("telegram_chat_id", telegram_chat_id)
            .field("min_price", min_price)
            .field("max_price", max_price)
            .field("max_position_usdc", max_position_usdc)
            .field("pause_on_max_position", pause_on_max_position)
            .field(
                "cancel_opening_on_max_position",
                cancel_opening_on_max_position,
            )
            .field("min_liquidation_distance_pct", min_liquidation_distance_pct)
            .finish()
    }
}

impl UpdateConfigPayload {
    pub fn has_global_updates(&self) -> bool {
        self.recv_window.is_some()
            || self.sync_interval_secs.is_some()
            || self.api_key.is_some()
            || self.api_secret.is_some()
            || self.clear_api_credentials
            || self.clear_telegram_credentials
            || self.telegram_enabled.is_some()
            || self.telegram_bot_token.is_some()
            || self.telegram_chat_id.is_some()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebConfigView {
    pub config_version: String,
    pub symbol: String,
    pub direction: GridDirection,
    pub grid_interval: Decimal,
    pub order_amount_usdc: Decimal,
    pub buy_window: usize,
    pub sell_window: usize,
    pub post_only: bool,
    pub recv_window: u64,
    pub sync_interval_secs: u64,
    pub has_api_key: bool,
    pub has_api_secret: bool,
    pub api_key_preview: String,
    pub telegram_enabled: bool,
    pub has_telegram_bot_token: bool,
    pub telegram_chat_id: String,
    pub min_price: Option<Decimal>,
    pub max_price: Option<Decimal>,
    pub max_position_usdc: Option<Decimal>,
    pub pause_on_max_position: bool,
    pub cancel_opening_on_max_position: bool,
    pub min_liquidation_distance_pct: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tick_size: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_size: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_qty: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_notional: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthStatusResponse {
    pub initialized: bool,
    pub authenticated: bool,
}

#[derive(Debug, Deserialize)]
pub struct AuthLoginPayload {
    pub password: String,
}

#[derive(Debug, Serialize)]
pub struct AuthTokenResponse {
    pub token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub message: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            message: None,
        }
    }

    pub fn ok_with_message(data: T, message: impl Into<String>) -> Self {
        Self {
            success: true,
            data: Some(data),
            message: Some(message.into()),
        }
    }

    pub fn err(message: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            message: Some(message.into()),
        }
    }
}
