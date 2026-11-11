use super::{non_blank, AccountInfo, BotStatus, GridDirection, TradeRecord, TradingMode};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TradingPairView {
    pub(crate) symbol: String,
    /// True when this pair has a stable /api/pairs/{symbol} route.
    pub(crate) scoped: bool,
    pub(crate) status: BotStatus,
    pub(crate) mode: TradingMode,
    pub(crate) grid_config: crate::config::GridConfigSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OverviewPairView {
    pub(crate) scoped: bool,
    pub(crate) mode: TradingMode,
    pub(crate) snapshot: PairSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PairSummary {
    pub(crate) symbol: String,
    pub(crate) status: BotStatus,
    pub(crate) mode: TradingMode,
    #[serde(default = "super::default_sync_interval_secs")]
    pub(crate) sync_interval_secs: u64,
    pub(crate) grid_config: crate::config::GridConfigSummary,
    pub(crate) ticker: SummaryTicker,
    pub(crate) position: SummaryPosition,
    pub(crate) stats: SummaryStats,
    pub(crate) today_trade_counts: Option<TodayTradeCounts>,
    pub(crate) account: AccountInfo,
    pub(crate) account_cny_rate: Option<crate::fx::CnyRate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SummaryTicker {
    pub(crate) mark_price: Decimal,
    pub(crate) mark_update_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SummaryPosition {
    pub(crate) size: Decimal,
    pub(crate) entry_price: Decimal,
    pub(crate) mark_price: Decimal,
    pub(crate) unrealized_pnl: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SummaryStats {
    pub(crate) total_realized_pnl: Decimal,
    pub(crate) active_buy_orders: usize,
    pub(crate) active_sell_orders: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TodayTradeCounts {
    pub(crate) window_start: DateTime<Utc>,
    pub(crate) window_end: DateTime<Utc>,
    pub(crate) buy: u64,
    pub(crate) sell: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OverviewSnapshot {
    pub(crate) generated_at: DateTime<Utc>,
    /// Each account belongs to its pair snapshot and may represent the same wallet.
    /// Consumers must not sum account balances across pairs.
    pub(crate) pairs: Vec<OverviewPairView>,
    pub(crate) today_pnl: Option<TodayPnl>,
    pub(crate) recent_trades: Vec<TradeRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TodayPnlGroup {
    pub(crate) symbol: String,
    pub(crate) mode: TradingMode,
    pub(crate) realized_pnl: Decimal,
    pub(crate) pending_pnl_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TodayPnl {
    pub(crate) window_start: DateTime<Utc>,
    pub(crate) window_end: DateTime<Utc>,
    pub(crate) groups: Vec<TodayPnlGroup>,
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub(crate) struct UpdateGlobalConfigPayload {
    pub(crate) baseline_version: Option<String>,
    pub(crate) recv_window: Option<u64>,
    pub(crate) sync_interval_secs: Option<u64>,
    pub(crate) api_key: Option<String>,
    pub(crate) api_secret: Option<String>,
    #[serde(default)]
    pub(crate) clear_api_credentials: bool,
    #[serde(default)]
    pub(crate) clear_telegram_credentials: bool,
    pub(crate) telegram_enabled: Option<bool>,
    pub(crate) telegram_bot_token: Option<String>,
    pub(crate) telegram_chat_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal_update",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) global_max_position_notional: Option<Option<Decimal>>,
    pub(crate) pause_on_zero_available_margin: Option<bool>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal_update",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) min_available_margin_pct: Option<Option<Decimal>>,
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
    pub(crate) fn apply_to(self, config: &mut crate::config::GlobalConfig) {
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
            if let Some(value) = non_blank(self.telegram_bot_token) {
                config.telegram.bot_token = value;
            }
            if let Some(chat_id) = self.telegram_chat_id {
                config.telegram.chat_id = chat_id.trim().to_string();
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WebGlobalConfigView {
    pub(crate) version: String,
    pub(crate) pause_on_zero_available_margin: bool,
    pub(crate) min_available_margin_pct: Option<Decimal>,
    pub(crate) global_max_position_notional: Option<Decimal>,
    pub(crate) recv_window: u64,
    pub(crate) sync_interval_secs: u64,
    pub(crate) has_api_key: bool,
    pub(crate) has_api_secret: bool,
    pub(crate) api_key_preview: String,
    pub(crate) telegram_enabled: bool,
    pub(crate) has_telegram_bot_token: bool,
    pub(crate) telegram_chat_id: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct UpdateConfigPayload {
    #[serde(default)]
    pub(crate) baseline_version: Option<String>,
    pub(crate) symbol: String,
    #[serde(default)]
    pub(crate) direction: Option<GridDirection>,
    pub(crate) grid_interval: Decimal,
    pub(crate) order_amount_usdc: Decimal,
    pub(crate) buy_window: usize,
    pub(crate) sell_window: usize,
    #[serde(default)]
    pub(crate) post_only: Option<bool>,
    /// Retired: the run mode now derives from API credentials. Rejected so a
    /// stale client cannot believe it is still switching modes.
    #[serde(default)]
    pub(crate) dry_run: Option<bool>,
    /// Retired: testnet mode was removed. Only rejected so a stale client
    /// cannot silently turn its testnet configuration into live trading.
    #[serde(default)]
    pub(crate) is_testnet: Option<bool>,
    #[serde(default)]
    pub(crate) recv_window: Option<u64>,
    pub(crate) sync_interval_secs: Option<u64>,
    pub(crate) api_key: Option<String>,
    pub(crate) api_secret: Option<String>,
    #[serde(default)]
    pub(crate) clear_api_credentials: bool,
    #[serde(default)]
    pub(crate) clear_telegram_credentials: bool,
    #[serde(default)]
    pub(crate) telegram_enabled: Option<bool>,
    pub(crate) telegram_bot_token: Option<String>,
    pub(crate) telegram_chat_id: Option<String>,
    pub(crate) min_price: Option<Decimal>,
    pub(crate) max_price: Option<Decimal>,
    pub(crate) max_position_usdc: Option<Decimal>,
    #[serde(default)]
    pub(crate) pause_on_max_position: Option<bool>,
    #[serde(default)]
    pub(crate) cancel_opening_on_max_position: Option<bool>,
    /// Replaces the saved value; omitted or null clears it.
    #[serde(default)]
    pub(crate) min_liquidation_distance_pct: Option<Decimal>,
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
    pub(crate) fn has_global_updates(&self) -> bool {
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
pub(crate) struct WebConfigView {
    pub(crate) config_version: String,
    pub(crate) symbol: String,
    pub(crate) direction: GridDirection,
    pub(crate) grid_interval: Decimal,
    pub(crate) order_amount_usdc: Decimal,
    pub(crate) buy_window: usize,
    pub(crate) sell_window: usize,
    pub(crate) post_only: bool,
    pub(crate) recv_window: u64,
    pub(crate) sync_interval_secs: u64,
    pub(crate) has_api_key: bool,
    pub(crate) has_api_secret: bool,
    pub(crate) api_key_preview: String,
    pub(crate) telegram_enabled: bool,
    pub(crate) has_telegram_bot_token: bool,
    pub(crate) telegram_chat_id: String,
    pub(crate) min_price: Option<Decimal>,
    pub(crate) max_price: Option<Decimal>,
    pub(crate) max_position_usdc: Option<Decimal>,
    pub(crate) pause_on_max_position: bool,
    pub(crate) cancel_opening_on_max_position: bool,
    pub(crate) min_liquidation_distance_pct: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tick_size: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) step_size: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) min_qty: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) min_notional: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AuthStatusResponse {
    pub(crate) initialized: bool,
    pub(crate) authenticated: bool,
}

#[derive(Deserialize)]
pub(crate) struct AuthLoginPayload {
    pub(crate) password: String,
}

impl std::fmt::Debug for AuthLoginPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { password } = self;
        f.debug_struct("AuthLoginPayload")
            .field("password", &crate::config::redact_secret(password))
            .finish()
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct AuthTokenResponse {
    pub(crate) token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ApiResponse<T> {
    pub(crate) success: bool,
    pub(crate) data: Option<T>,
    pub(crate) message: Option<String>,
}

impl<T> ApiResponse<T> {
    pub(crate) fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            message: None,
        }
    }

    pub(crate) fn ok_with_message(data: T, message: impl Into<String>) -> Self {
        Self {
            success: true,
            data: Some(data),
            message: Some(message.into()),
        }
    }

    pub(crate) fn err(message: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            message: Some(message.into()),
        }
    }
}

pub(crate) fn ws_frame_json<T: Serialize + ?Sized>(
    kind: &'static str,
    data: &T,
) -> Option<std::sync::Arc<str>> {
    #[derive(Serialize)]
    struct WsFrame<'a, T: ?Sized> {
        #[serde(rename = "type")]
        kind: &'static str,
        data: &'a T,
    }
    serde_json::to_string(&WsFrame { kind, data })
        .ok()
        .map(std::sync::Arc::<str>::from)
}
