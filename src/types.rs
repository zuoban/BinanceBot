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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub trade_id: String,
    pub client_order_id: String,
    pub symbol: String,
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
pub struct BotSnapshot {
    pub status: BotStatus,
    pub dry_run: bool,
    pub symbol: String,
    pub ticker: TickerInfo,
    pub stats: GridStats,
    pub position: PositionInfo,
    pub account: AccountInfo,
    pub grid_config: crate::config::GridConfigSummary,
    pub active_orders: Vec<GridOrder>,
    pub recent_trades: Vec<TradeRecord>,
    pub recent_logs: Vec<LogEntry>,
}

#[derive(Debug, Clone)]
pub enum BotControlAction {
    Pause,
    Resume,
    CancelAll,
    Rebalance,
    UpdateConfig(Box<crate::config::AppConfig>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateConfigPayload {
    pub symbol: String,
    pub grid_interval: Decimal,
    pub order_amount_usdc: Decimal,
    pub buy_window: usize,
    pub sell_window: usize,
    #[serde(default = "default_true")]
    pub post_only: bool,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub is_testnet: bool,
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
