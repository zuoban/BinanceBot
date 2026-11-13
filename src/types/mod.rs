use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

mod order;
mod position;

pub use order::{
    GridDirection, GridOrder, OrderPurpose, OrderSide, OrderSource, OrderStatus, TradingMode,
};
pub(crate) use order::{ObservedOrderIdentity, RemainderPhase, RemainderPlan};
pub(crate) use position::{
    apply_fill_to_position, format_decimal_dp, format_truncated_decimal_dp, quote_asset,
    resolve_quote_asset, PAPER_INITIAL_BALANCE,
};
pub use position::{AccountInfo, PositionInfo, TradeRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum BotStatus {
    Running,
    Paused,
    Stopped,
    Error,
}

impl BotStatus {
    #[must_use]
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::Paused => "PAUSED",
            Self::Stopped => "STOPPED",
            Self::Error => "ERROR",
        }
    }

    pub fn from_db(s: &str) -> anyhow::Result<Self> {
        match s {
            "RUNNING" => Ok(Self::Running),
            "PAUSED" => Ok(Self::Paused),
            "STOPPED" => Ok(Self::Stopped),
            "ERROR" => Ok(Self::Error),
            _ => anyhow::bail!("Unknown bot status in database: {s:?}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridStats {
    pub total_trades: usize,
    pub completed_cycles: usize,
    /// Legacy paired grid spread estimate, separate from exchange realized PnL.
    pub total_realized_profit: Decimal,
    /// Gross realized PnL reported by Binance for this symbol's saved trades.
    pub total_realized_pnl: Decimal,
    /// Commission charged in the symbol's quote asset.
    pub total_commission: Decimal,
    #[serde(default)]
    pub non_quote_commission_trades: usize,
    pub pending_pnl_trades: usize,
    pub total_volume_usdc: Decimal,
    pub start_time: DateTime<Utc>,
    pub uptime_secs: u64,
    pub active_buy_orders: usize,
    pub active_sell_orders: usize,
}

impl Default for GridStats {
    fn default() -> Self {
        Self {
            total_trades: 0,
            completed_cycles: 0,
            total_realized_profit: Decimal::ZERO,
            total_realized_pnl: Decimal::ZERO,
            total_commission: Decimal::ZERO,
            non_quote_commission_trades: 0,
            pending_pnl_trades: 0,
            total_volume_usdc: Decimal::ZERO,
            start_time: Utc::now(),
            uptime_secs: 0,
            active_buy_orders: 0,
            active_sell_orders: 0,
        }
    }
}

pub(crate) const MARKET_PRICE_MAX_AGE_SECS: i64 = 15;

#[must_use]
#[inline]
pub(crate) const fn beijing_offset() -> chrono::FixedOffset {
    const OFFSET: chrono::FixedOffset = match chrono::FixedOffset::east_opt(8 * 3600) {
        Some(offset) => offset,
        None => panic!("invalid UTC+8 offset"),
    };
    OFFSET
}

#[must_use]
#[inline]
pub(crate) fn non_blank(value: Option<String>) -> Option<String> {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

#[must_use]
#[inline]
pub(crate) fn is_timestamp_fresh(
    updated_at: DateTime<Utc>,
    now: DateTime<Utc>,
    max_age_secs: i64,
) -> bool {
    let age = now.signed_duration_since(updated_at);
    age >= chrono::Duration::zero() && age < chrono::Duration::seconds(max_age_secs)
}

#[must_use]
#[inline]
pub(crate) fn max_reconciliation_age_secs(sync_interval_secs: u64) -> i64 {
    sync_interval_secs
        .saturating_mul(3)
        .max(30)
        .min(i64::MAX as u64) as i64
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TickerInfo {
    pub symbol: String,
    pub last_price: Decimal,
    pub mark_price: Decimal,
    pub mark_update_time: DateTime<Utc>,
    pub high_24h: Decimal,
    pub low_24h: Decimal,
    pub change_24h: Decimal,
    pub change_percent_24h: Decimal,
    pub volume_24h: Decimal,
    pub update_time: DateTime<Utc>,
}

impl TickerInfo {
    #[must_use]
    #[inline]
    pub fn mark_price_is_fresh(&self, now: DateTime<Utc>) -> bool {
        self.mark_price > Decimal::ZERO
            && is_timestamp_fresh(self.mark_update_time, now, MARKET_PRICE_MAX_AGE_SECS)
    }

    /// A positive cached mark must be fresh too; do not silently change the
    /// strategy reference to the last price when a previously available mark expires.
    #[must_use]
    #[inline]
    pub fn prices_are_fresh(&self, now: DateTime<Utc>) -> bool {
        self.last_price > Decimal::ZERO
            && is_timestamp_fresh(self.update_time, now, MARKET_PRICE_MAX_AGE_SECS)
            && (self.mark_price <= Decimal::ZERO || self.mark_price_is_fresh(now))
    }

    /// Center the grid on the mark price; use the last price until a mark is available.
    #[must_use]
    #[inline]
    pub fn grid_reference_price(&self) -> Decimal {
        if self.mark_price > Decimal::ZERO {
            self.mark_price
        } else {
            self.last_price
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlyTradeCount {
    pub hour_start: DateTime<Utc>,
    pub buy_count: u64,
    pub sell_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlyTradeStats {
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub buckets: Vec<HourlyTradeCount>,
    pub realized_pnl: Decimal,
    pub pending_pnl_count: u64,
}

impl HourlyTradeStats {
    #[must_use]
    #[inline]
    pub fn total_counts(&self) -> (u64, u64) {
        self.buckets
            .iter()
            .fold((0u64, 0u64), |(buy, sell), bucket| {
                (
                    buy.saturating_add(bucket.buy_count),
                    sell.saturating_add(bucket.sell_count),
                )
            })
    }
}

pub(super) fn default_sync_interval_secs() -> u64 {
    3
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BotSnapshot {
    #[serde(default = "chrono::Utc::now")]
    pub generated_at: DateTime<Utc>,
    pub status: BotStatus,
    pub mode: TradingMode,
    #[serde(default)]
    pub account_scope: String,
    pub symbol: String,
    #[serde(default = "default_sync_interval_secs")]
    pub sync_interval_secs: u64,
    pub ticker: TickerInfo,
    pub stats: GridStats,
    pub position: PositionInfo,
    pub account: AccountInfo,
    pub account_cny_rate: Option<crate::fx::CnyRate>,
    pub grid_config: crate::config::GridConfigSummary,
    pub active_orders: Vec<GridOrder>,
    pub recent_trades: Vec<TradeRecord>,
    pub hourly_trade_stats: Option<HourlyTradeStats>,
    pub recent_logs: Vec<LogEntry>,
}

#[derive(Debug)]
pub enum BotControlAction {
    Pause,
    /// Pause was already persisted before this action was queued.
    PauseAlreadyPersisted,
    GlobalRiskCancel,
    Resume {
        expected_mode: TradingMode,
        expected_epoch: u64,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    CancelAll,
    /// Pause was already persisted before this cancellation was queued.
    CancelAllAlreadyPersisted,
    Rebalance {
        expected_mode: TradingMode,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    Reverse {
        expected_mode: TradingMode,
        expected_direction: GridDirection,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    Shutdown {
        removing: bool,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    UpdateConfig {
        config: Box<crate::config::AppConfig>,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    UpdateGlobalConfig {
        config: Box<crate::config::GlobalConfig>,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
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

#[derive(Serialize)]
pub(crate) struct AuthTokenResponse {
    pub(crate) token: String,
}

impl std::fmt::Debug for AuthTokenResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { token } = self;
        f.debug_struct("AuthTokenResponse")
            .field("token", &crate::config::redact_secret(token))
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ApiResponse<T> {
    pub(crate) success: bool,
    pub(crate) data: Option<T>,
    pub(crate) message: Option<String>,
}

impl<T> ApiResponse<T> {
    #[must_use]
    #[inline]
    pub(crate) fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            message: None,
        }
    }

    #[must_use]
    #[inline]
    pub(crate) fn ok_with_message(data: T, message: impl Into<String>) -> Self {
        Self {
            success: true,
            data: Some(data),
            message: Some(message.into()),
        }
    }

    #[must_use]
    #[inline]
    pub(crate) fn err(message: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            message: Some(message.into()),
        }
    }
}

#[must_use]
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
mod api;
pub(crate) use api::*;

#[cfg(test)]
mod tests;
