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
    pub(crate) fn from_response(resp: &crate::exchange::model::BinanceOrderResponse) -> Self {
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

impl GridOrder {
    pub(crate) fn from_exchange_response(
        found: &crate::exchange::model::BinanceOrderResponse,
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
            amount_usdc: found.price * quantity,
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
        response: &crate::exchange::model::BinanceOrderResponse,
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
        self.amount_usdc = self.price * self.quantity;
    }

    pub(crate) fn matches_exchange_identity(
        &self,
        resp: &crate::exchange::model::BinanceOrderResponse,
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
        self.amount_usdc = self.price * self.quantity;
    }

    pub(crate) fn reserve_max(&mut self, min_price: Decimal, min_quantity: Decimal) {
        self.price = self.price.max(min_price);
        self.reserve_quantity(min_quantity);
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

impl RemainderPlan {
    pub fn find_order(&self, client_order_id: &str) -> Option<&GridOrder> {
        self.sources
            .iter()
            .chain(std::iter::once(&self.target))
            .find(|order| order.client_order_id == client_order_id)
    }
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

impl PositionInfo {
    pub fn position_margin(&self) -> Decimal {
        let valuation_price = if self.mark_price > Decimal::ZERO {
            self.mark_price
        } else {
            self.entry_price
        };
        self.size.abs() * valuation_price / Decimal::from(self.leverage.max(1))
    }

    /// Best-effort market valuation price using an optional ticker (`mark_price`,
    /// then `self.mark_price`, then `ticker.last_price`), without falling back to `entry_price`.
    pub fn market_valuation_price(&self, ticker: Option<&TickerInfo>) -> Decimal {
        if let Some(price) = ticker.map(|t| t.mark_price).filter(|p| *p > Decimal::ZERO) {
            price
        } else if self.mark_price > Decimal::ZERO {
            self.mark_price
        } else if let Some(price) = ticker.map(|t| t.last_price).filter(|p| *p > Decimal::ZERO) {
            price
        } else {
            Decimal::ZERO
        }
    }

    /// Strictly fresh market valuation price (`ticker.mark_price`, `self.mark_price`, or `ticker.last_price`).
    pub fn fresh_market_valuation_price(
        &self,
        ticker: &TickerInfo,
        mut is_fresh: impl FnMut(DateTime<Utc>) -> bool,
    ) -> Decimal {
        if ticker.mark_price > Decimal::ZERO && is_fresh(ticker.mark_update_time) {
            ticker.mark_price
        } else if self.mark_price > Decimal::ZERO && self.update_time.is_some_and(&mut is_fresh) {
            self.mark_price
        } else if ticker.last_price > Decimal::ZERO && is_fresh(ticker.update_time) {
            ticker.last_price
        } else {
            Decimal::ZERO
        }
    }

    /// Valuation price preferring fresh mark prices, falling back to best-effort market price or `entry_price`.
    pub fn valuation_price_with_freshness(
        &self,
        ticker: &TickerInfo,
        ticker_fresh: bool,
        position_fresh: bool,
    ) -> Decimal {
        if ticker_fresh && position_fresh {
            if self
                .update_time
                .is_some_and(|time| time > ticker.mark_update_time)
            {
                self.mark_price
            } else {
                ticker.mark_price
            }
        } else if ticker_fresh {
            ticker.mark_price
        } else if position_fresh {
            self.mark_price
        } else {
            let market_price = self.market_valuation_price(Some(ticker));
            if market_price > Decimal::ZERO {
                market_price
            } else {
                self.entry_price
            }
        }
    }
}

pub fn apply_fill_to_position(
    size: Decimal,
    entry_price: Decimal,
    side: OrderSide,
    price: Decimal,
    qty: Decimal,
) -> (Decimal, Decimal, Decimal) {
    let signed_qty = match side {
        OrderSide::Buy => qty,
        OrderSide::Sell => -qty,
    };
    let new_size = size + signed_qty;
    let same_direction = size.is_zero() || size.is_sign_positive() == signed_qty.is_sign_positive();
    if same_direction {
        let new_entry = if new_size.is_zero() {
            Decimal::ZERO
        } else {
            ((entry_price * size.abs()) + (price * qty)) / new_size.abs()
        };
        (new_size, new_entry, Decimal::ZERO)
    } else {
        let closed_qty = qty.min(size.abs());
        let direction_sign = if size > Decimal::ZERO {
            Decimal::ONE
        } else {
            -Decimal::ONE
        };
        let realized = (price - entry_price) * closed_qty * direction_sign;
        let new_entry = if new_size.is_zero() {
            Decimal::ZERO
        } else if new_size.is_sign_positive() != size.is_sign_positive() {
            price
        } else {
            entry_price
        };
        (new_size, new_entry, realized)
    }
}

pub const PAPER_INITIAL_BALANCE: Decimal = rust_decimal_macros::dec!(10000);

pub fn quote_asset(symbol: &str) -> Option<&'static str> {
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

impl AccountInfo {
    pub fn initial_paper(symbol: &str, update_time: Option<DateTime<Utc>>) -> Self {
        Self {
            asset: quote_asset(symbol).unwrap_or(symbol).to_string(),
            total_wallet_balance: PAPER_INITIAL_BALANCE,
            available_balance: PAPER_INITIAL_BALANCE,
            margin_balance: PAPER_INITIAL_BALANCE,
            unrealized_profit: Decimal::ZERO,
            update_time: update_time.unwrap_or_else(Utc::now),
        }
    }
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

pub const MARKET_PRICE_MAX_AGE_SECS: i64 = 15;

pub(crate) fn beijing_offset() -> chrono::FixedOffset {
    chrono::FixedOffset::east_opt(8 * 3600).expect("UTC+8 is a valid fixed offset")
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
        self.buckets.iter().fold((0, 0), |(buy, sell), bucket| {
            (buy + bucket.buy_count, sell + bucket.sell_count)
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
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal_update",
        skip_serializing_if = "Option::is_none"
    )]
    pub min_available_margin_pct: Option<Option<Decimal>>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

pub(crate) fn ws_frame_json<T: Serialize + ?Sized>(kind: &'static str, data: &T) -> Option<String> {
    #[derive(Serialize)]
    struct WsFrame<'a, T: ?Sized> {
        #[serde(rename = "type")]
        kind: &'static str,
        data: &'a T,
    }
    serde_json::to_string(&WsFrame { kind, data }).ok()
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

    #[test]
    fn global_config_payload_preserves_existing_chat_id_when_blank() {
        let mut global =
            crate::config::GlobalConfig::from_app(&crate::config::AppConfig::default());
        global.telegram.chat_id = "123456".into();
        UpdateGlobalConfigPayload {
            telegram_chat_id: Some("   ".into()),
            ..Default::default()
        }
        .apply_to(&mut global);
        assert_eq!(global.telegram.chat_id, "123456");

        UpdateGlobalConfigPayload {
            clear_telegram_credentials: true,
            ..Default::default()
        }
        .apply_to(&mut global);
        assert!(global.telegram.chat_id.is_empty());
    }

    #[test]
    fn initial_paper_account_falls_back_to_symbol_when_quote_suffix_is_unknown() {
        assert_eq!(AccountInfo::initial_paper("SOLUSDC", None).asset, "USDC");
        assert_eq!(AccountInfo::initial_paper("BTCETH", None).asset, "BTCETH");
    }

    #[test]
    fn terminal_fill_prefers_positive_avg_price_falls_back_to_response_price_and_updates_notional()
    {
        let created_at = Utc::now() - chrono::Duration::seconds(10);
        let updated_at = Utc::now();
        let base_order = GridOrder {
            client_order_id: "gb_fill_1".into(),
            order_id: Some(42),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(2),
            amount_usdc: dec!(200),
            status: OrderStatus::New,
            created_at,
            updated_at: created_at,
            grid_level: 1000,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        let mut response = crate::exchange::model::BinanceOrderResponse {
            order_id: 42,
            client_order_id: "gb_fill_1".into(),
            symbol: "SOLUSDC".into(),
            status: "FILLED".into(),
            price: dec!(101),
            avg_price: Some(dec!(99.5)),
            orig_qty: dec!(2),
            executed_qty: dec!(1.5),
            side: "BUY".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        };

        let mut order = base_order.clone();
        order.apply_terminal_fill(&response, updated_at);
        assert_eq!(order.updated_at, updated_at);
        assert_eq!(order.quantity, dec!(1.5));
        assert_eq!(order.price, dec!(99.5));
        assert_eq!(order.amount_usdc, dec!(149.25));

        response.avg_price = Some(Decimal::ZERO);
        let mut fallback_order = base_order.clone();
        fallback_order.apply_terminal_fill(&response, updated_at);
        assert_eq!(fallback_order.price, dec!(101));
        assert_eq!(fallback_order.amount_usdc, dec!(151.5));

        response.price = Decimal::ZERO;
        let mut keep_order = base_order;
        keep_order.apply_terminal_fill(&response, updated_at);
        assert_eq!(keep_order.price, dec!(100));
        assert_eq!(keep_order.amount_usdc, dec!(150));
    }
}
