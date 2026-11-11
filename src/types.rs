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

    pub fn from_exchange_str(side: &str) -> Option<Self> {
        match side {
            "BUY" => Some(Self::Buy),
            "SELL" => Some(Self::Sell),
            _ => None,
        }
    }

    pub(crate) fn is_maker_price(self, price: Decimal, market: Decimal) -> bool {
        match self {
            Self::Buy => price < market,
            Self::Sell => price > market,
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

    pub fn closing_price_cmp(self, a: Decimal, b: Decimal) -> std::cmp::Ordering {
        match self {
            Self::Long => a.cmp(&b),
            Self::Short => b.cmp(&a),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TradingMode {
    Paper,
    Live,
    #[default]
    #[serde(other)]
    Unknown,
}

impl TradingMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "UNKNOWN",
            Self::Paper => "PAPER",
            Self::Live => "LIVE",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value {
            "PAPER" => Self::Paper,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ObservedOrderIdentity {
    pub client_order_id: String,
    pub symbol: String,
    pub side: String,
    pub order_id: i64,
}

impl ObservedOrderIdentity {
    pub(crate) fn from_response(resp: &crate::exchange::BinanceOrderResponse) -> Self {
        Self {
            client_order_id: resp.client_order_id.clone(),
            symbol: resp.symbol.clone(),
            side: resp.side.clone(),
            order_id: resp.order_id,
        }
    }

    pub(crate) fn from_order(order: &GridOrder) -> Option<Self> {
        Some(Self {
            client_order_id: order.client_order_id.clone(),
            symbol: order.symbol.clone(),
            side: order.side.as_str().to_owned(),
            order_id: order.order_id?,
        })
    }

    pub(crate) fn matches_order(&self, expected: &GridOrder) -> bool {
        self.client_order_id == expected.client_order_id
            && self.symbol == expected.symbol
            && self.side == expected.side.as_str()
            && expected.order_id.is_none_or(|id| self.order_id == id)
    }

    pub(crate) fn matches_confirmed_order(&self, expected: &GridOrder) -> bool {
        self.client_order_id == expected.client_order_id
            && self.symbol == expected.symbol
            && self.side == expected.side.as_str()
            && expected.order_id == Some(self.order_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

impl GridOrder {
    pub(crate) fn from_exchange_response(
        found: &crate::exchange::BinanceOrderResponse,
        quantity: Decimal,
        purpose: OrderPurpose,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            client_order_id: found.client_order_id.clone(),
            order_id: Some(found.order_id),
            symbol: found.symbol.clone(),
            side: OrderSide::from_exchange_str(&found.side).unwrap_or(OrderSide::Sell),
            price: found.price,
            quantity,
            amount_usdc: found.price.checked_mul(quantity).unwrap_or(Decimal::MAX),
            status: found.open_order_status(),
            created_at: timestamp,
            updated_at: timestamp,
            grid_level: 0,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose,
            merge_sources: Vec::new(),
        }
    }

    pub(crate) fn apply_terminal_fill(
        &mut self,
        response: &crate::exchange::BinanceOrderResponse,
        updated_at: DateTime<Utc>,
    ) {
        self.updated_at = updated_at;
        self.quantity = response.executed_qty;
        if let Some(price) = response
            .avg_price
            .filter(|p| *p > Decimal::ZERO)
            .or_else(|| (response.price > Decimal::ZERO).then_some(response.price))
        {
            self.price = price;
        }
        self.amount_usdc = self
            .price
            .checked_mul(self.quantity)
            .unwrap_or(Decimal::MAX);
    }

    pub(crate) fn matches_exchange_identity(
        &self,
        resp: &crate::exchange::BinanceOrderResponse,
    ) -> bool {
        self.client_order_id == resp.client_order_id
            && self.symbol == resp.symbol
            && resp.side == self.side.as_str()
            && self.order_id.is_none_or(|id| resp.order_id == id)
    }

    pub(crate) fn has_compatible_identity(&self, other: &GridOrder) -> bool {
        self.client_order_id == other.client_order_id
            && self.symbol == other.symbol
            && self.side == other.side
            && self
                .order_id
                .zip(other.order_id)
                .is_none_or(|(a, b)| a == b)
    }

    pub(crate) fn adopt_compatible_order_id(&mut self, other: &GridOrder) {
        if self.order_id.is_none() && self.has_compatible_identity(other) {
            self.order_id = other.order_id;
        }
    }

    pub(crate) fn reserve_quantity(&mut self, min_quantity: Decimal) {
        self.quantity = self.quantity.max(min_quantity);
        self.amount_usdc = self
            .price
            .checked_mul(self.quantity)
            .unwrap_or(Decimal::MAX);
    }

    pub(crate) fn reserve_max(&mut self, min_price: Decimal, min_quantity: Decimal) {
        self.price = self.price.max(min_price);
        self.reserve_quantity(min_quantity);
    }

    pub(crate) fn matches_persisted_state(&self, other: &Self) -> bool {
        self.client_order_id == other.client_order_id
            && self.order_id == other.order_id
            && self.symbol == other.symbol
            && self.side == other.side
            && self.price == other.price
            && self.quantity == other.quantity
            && self.amount_usdc == other.amount_usdc
            && self.status == other.status
            && self.created_at == other.created_at
            && self.grid_level == other.grid_level
            && self.paired_client_order_id == other.paired_client_order_id
            && self.is_take_profit == other.is_take_profit
            && self.purpose == other.purpose
            && self.merge_sources == other.merge_sources
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderPurpose {
    #[default]
    Legacy,
    Unverified,
    Grid,
    TakeProfit,
    Remainder,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderSource {
    pub client_order_id: String,
    pub price: Decimal,
    pub quantity: Decimal,
}

#[path = "types/position.rs"]
mod position;
pub(crate) use position::{
    apply_fill_to_position, format_decimal_dp, format_truncated_decimal_dp, quote_asset,
    PAPER_INITIAL_BALANCE,
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

pub(crate) const MARKET_PRICE_MAX_AGE_SECS: i64 = 15;

pub(crate) fn beijing_offset() -> chrono::FixedOffset {
    const OFFSET: chrono::FixedOffset = match chrono::FixedOffset::east_opt(8 * 3600) {
        Some(offset) => offset,
        None => panic!("invalid UTC+8 offset"),
    };
    OFFSET
}

pub(crate) fn non_blank(value: Option<String>) -> Option<String> {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

pub(crate) fn is_timestamp_fresh(
    updated_at: DateTime<Utc>,
    now: DateTime<Utc>,
    max_age_secs: i64,
) -> bool {
    let age = now.signed_duration_since(updated_at);
    age >= chrono::Duration::zero() && age < chrono::Duration::seconds(max_age_secs)
}

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
    pub fn mark_price_is_fresh(&self, now: DateTime<Utc>) -> bool {
        self.mark_price > Decimal::ZERO
            && is_timestamp_fresh(self.mark_update_time, now, MARKET_PRICE_MAX_AGE_SECS)
    }

    /// A positive cached mark must be fresh too; do not silently change the
    /// strategy reference to the last price when a previously available mark expires.
    pub fn prices_are_fresh(&self, now: DateTime<Utc>) -> bool {
        self.last_price > Decimal::ZERO
            && is_timestamp_fresh(self.update_time, now, MARKET_PRICE_MAX_AGE_SECS)
            && (self.mark_price <= Decimal::ZERO || self.mark_price_is_fresh(now))
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

impl HourlyTradeStats {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BotSnapshot {
    pub status: BotStatus,
    pub mode: TradingMode,
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

#[path = "types/api.rs"]
mod api;
pub(crate) use api::*;

#[cfg(test)]
#[path = "types_tests.rs"]
mod tests;
