use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    pub fn opposite(&self) -> Self {
        match self {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        }
    }
}

/// Strategy direction in Binance's one-way position mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum GridDirection {
    #[default]
    Long,
    Short,
}

impl GridDirection {
    pub fn opening_side(self) -> OrderSide {
        match self {
            Self::Long => OrderSide::Buy,
            Self::Short => OrderSide::Sell,
        }
    }

    pub fn closing_side(self) -> OrderSide {
        self.opening_side().opposite()
    }

    pub fn sign(self) -> Decimal {
        match self {
            Self::Long => Decimal::ONE,
            Self::Short => -Decimal::ONE,
        }
    }

    pub fn position_quantity(self, size: Decimal) -> Decimal {
        (size * self.sign()).max(Decimal::ZERO)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TradingMode {
    #[default]
    Unknown,
    Paper,
    Testnet,
    Live,
}

impl TradingMode {
    pub fn from_exchange(dry_run: bool, is_testnet: bool) -> Self {
        if dry_run {
            Self::Paper
        } else if is_testnet {
            Self::Testnet
        } else {
            Self::Live
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "UNKNOWN",
            Self::Paper => "PAPER",
            Self::Testnet => "TESTNET",
            Self::Live => "LIVE",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value {
            "PAPER" => Self::Paper,
            "TESTNET" => Self::Testnet,
            "LIVE" => Self::Live,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridOrder {
    pub client_order_id: String,
    pub order_id: Option<i64>,
    pub symbol: String,
    pub side: OrderSide,
    pub price: Decimal,
    pub quantity: Decimal,
    pub amount_usdc: Decimal,
    pub status: OrderStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub grid_level: i32,
    pub paired_client_order_id: Option<String>,
    pub is_take_profit: bool,
    /// Legacy orders retain unknown provenance; size alone must not classify them.
    #[serde(default)]
    pub purpose: OrderPurpose,
    #[serde(default)]
    pub merge_sources: Vec<OrderSource>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderPurpose {
    #[default]
    Legacy,
    Grid,
    TakeProfit,
    Remainder,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderSource {
    pub client_order_id: String,
    pub price: Decimal,
    pub quantity: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemainderPhase {
    Canceling,
    Submitting,
    Complete,
    Abandoned,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemainderPlan {
    pub symbol: String,
    pub mode: TradingMode,
    pub sources: Vec<GridOrder>,
    pub target: GridOrder,
    pub phase: RemainderPhase,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub trade_id: String,
    pub client_order_id: String,
    pub symbol: String,
    #[serde(default)]
    pub mode: TradingMode,
    pub side: OrderSide,
    pub price: Decimal,
    pub quantity: Decimal,
    pub amount_usdc: Decimal,
    pub realized_pnl: Decimal,
    pub commission: Decimal,
    #[serde(default)]
    pub pnl_verified: bool,
    pub is_maker: bool,
    pub timestamp: DateTime<Utc>,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PositionInfo {
    pub symbol: String,
    pub size: Decimal,
    pub entry_price: Decimal,
    pub mark_price: Decimal,
    pub unrealized_pnl: Decimal,
    pub liquidation_price: Decimal,
    pub leverage: u32,
    #[serde(default)]
    pub update_time: Option<DateTime<Utc>>,
}

pub fn quote_asset(symbol: &str) -> Option<&str> {
    ["FDUSD", "USDC", "USDT", "BUSD"]
        .into_iter()
        .find(|asset| symbol.ends_with(asset))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountInfo {
    pub asset: String,
    pub total_wallet_balance: Decimal,
    pub available_balance: Decimal,
    pub margin_balance: Decimal,
    pub unrealized_profit: Decimal,
    pub update_time: DateTime<Utc>,
}

impl Default for AccountInfo {
    fn default() -> Self {
        Self {
            asset: String::new(),
            total_wallet_balance: Decimal::ZERO,
            available_balance: Decimal::ZERO,
            margin_balance: Decimal::ZERO,
            unrealized_profit: Decimal::ZERO,
            update_time: DateTime::<Utc>::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum BotStatus {
    Running,
    Paused,
    Stopped,
    Error,
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
            pending_pnl_trades: 0,
            total_volume_usdc: Decimal::ZERO,
            start_time: Utc::now(),
            uptime_secs: 0,
            active_buy_orders: 0,
            active_sell_orders: 0,
        }
    }
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
    /// A positive cached mark must be fresh too; do not silently change the
    /// strategy reference to the last price when a previously available mark expires.
    pub fn prices_are_fresh(&self, now: DateTime<Utc>) -> bool {
        let fresh = |updated_at: DateTime<Utc>| {
            let age = now.signed_duration_since(updated_at);
            age >= chrono::Duration::zero() && age < chrono::Duration::seconds(15)
        };
        self.last_price > Decimal::ZERO
            && fresh(self.update_time)
            && (self.mark_price <= Decimal::ZERO || fresh(self.mark_update_time))
    }

    /// Center the grid on the mark price; use the last price until a mark is available.
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BotSnapshot {
    pub status: BotStatus,
    pub dry_run: bool,
    pub symbol: String,
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
    pub snapshot: BotSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverviewSnapshot {
    pub generated_at: DateTime<Utc>,
    /// Each account belongs to its pair snapshot and may represent the same wallet.
    /// Consumers must not sum account balances across pairs.
    pub pairs: Vec<OverviewPairView>,
    pub today_pnl: Option<TodayPnl>,
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

#[derive(Debug)]
pub enum BotControlAction {
    Pause,
    GlobalRiskCancel,
    Resume,
    CancelAll,
    Rebalance,
    Shutdown {
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

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
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
                if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
                    *target = value.trim().to_string();
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
            if let Some(token) = self
                .telegram_bot_token
                .filter(|value| !value.trim().is_empty())
            {
                config.telegram.bot_token = token.trim().to_string();
            }
            if let Some(chat_id) = self.telegram_chat_id {
                config.telegram.chat_id = chat_id.trim().to_string();
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WebGlobalConfigView {
    pub version: String,
    pub pause_on_zero_available_margin: bool,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateConfigPayload {
    pub symbol: String,
    #[serde(default)]
    pub direction: Option<GridDirection>,
    pub grid_interval: Decimal,
    pub order_amount_usdc: Decimal,
    pub buy_window: usize,
    pub sell_window: usize,
    #[serde(default)]
    pub post_only: Option<bool>,
    #[serde(default)]
    pub dry_run: Option<bool>,
    #[serde(default)]
    pub is_testnet: Option<bool>,
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
    pub symbol: String,
    pub direction: GridDirection,
    pub grid_interval: Decimal,
    pub order_amount_usdc: Decimal,
    pub buy_window: usize,
    pub sell_window: usize,
    pub post_only: bool,
    pub dry_run: bool,
    pub is_testnet: bool,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthStatusResponse {
    pub initialized: bool,
    pub authenticated: bool,
}

#[derive(Debug, Deserialize)]
pub struct AuthSetupPayload {
    pub password: String,
    #[serde(default)]
    pub setup_code: String,
}

#[derive(Debug, Deserialize)]
pub struct AuthLoginPayload {
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct ChangePasswordPayload {
    pub old_password: String,
    pub new_password: String,
}

#[derive(Debug, Serialize)]
pub struct AuthTokenResponse {
    pub token: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn price_freshness_checks_both_clocks_and_keeps_the_startup_fallback() {
        let now = Utc::now();
        let mut ticker = TickerInfo {
            last_price: dec!(100),
            update_time: now,
            ..Default::default()
        };
        assert!(ticker.prices_are_fresh(now));
        ticker.mark_price = dec!(101);
        assert!(!ticker.prices_are_fresh(now));
        ticker.mark_update_time = now - chrono::Duration::milliseconds(14_999);
        assert!(ticker.prices_are_fresh(now));
        ticker.mark_update_time = now - chrono::Duration::seconds(15);
        assert!(!ticker.prices_are_fresh(now));
        ticker.mark_update_time = now;
        ticker.update_time = now + chrono::Duration::milliseconds(1);
        assert!(!ticker.prices_are_fresh(now));
        ticker.update_time = now;
        ticker.last_price = Decimal::ZERO;
        assert!(!ticker.prices_are_fresh(now));
    }
}
