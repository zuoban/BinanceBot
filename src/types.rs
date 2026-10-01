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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PricePoint {
    pub timestamp: DateTime<Utc>,
    pub price: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceHistory {
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub day_start: DateTime<Utc>,
    pub midnight_price: Option<Decimal>,
    pub points: Vec<PricePoint>,
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
    pub price_history: Option<PriceHistory>,
    pub recent_logs: Vec<LogEntry>,
}

#[derive(Debug)]
pub enum BotControlAction {
    Pause,
    Resume,
    CancelAll,
    Rebalance,
    UpdateConfig {
        config: Box<crate::config::AppConfig>,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateConfigPayload {
    pub symbol: String,
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
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    #[serde(default)]
    pub telegram_enabled: Option<bool>,
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
    pub min_price: Option<Decimal>,
    pub max_price: Option<Decimal>,
    pub max_position_usdc: Option<Decimal>,
    #[serde(default = "default_true")]
    pub save_to_file: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebConfigView {
    pub symbol: String,
    pub grid_interval: Decimal,
    pub order_amount_usdc: Decimal,
    pub buy_window: usize,
    pub sell_window: usize,
    pub post_only: bool,
    pub dry_run: bool,
    pub is_testnet: bool,
    pub has_api_key: bool,
    pub has_api_secret: bool,
    pub api_key_preview: String,
    pub telegram_enabled: bool,
    pub has_telegram_bot_token: bool,
    pub telegram_chat_id: String,
    pub min_price: Option<Decimal>,
    pub max_price: Option<Decimal>,
    pub max_position_usdc: Option<Decimal>,
}

fn default_true() -> bool {
    true
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
