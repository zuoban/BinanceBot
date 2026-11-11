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
    #[must_use]
    #[inline]
    pub const fn opposite(self) -> Self {
        match self {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        }
    }

    #[must_use]
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        }
    }

    #[must_use]
    #[inline]
    pub fn from_exchange_str(side: &str) -> Option<Self> {
        match side {
            "BUY" => Some(Self::Buy),
            "SELL" => Some(Self::Sell),
            _ => None,
        }
    }

    #[must_use]
    #[inline]
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
    #[must_use]
    #[inline]
    pub const fn opening_side(self) -> OrderSide {
        match self {
            Self::Long => OrderSide::Buy,
            Self::Short => OrderSide::Sell,
        }
    }

    #[must_use]
    #[inline]
    pub const fn closing_side(self) -> OrderSide {
        self.opening_side().opposite()
    }

    #[must_use]
    #[inline]
    pub fn sign(self) -> Decimal {
        match self {
            Self::Long => Decimal::ONE,
            Self::Short => -Decimal::ONE,
        }
    }

    #[must_use]
    #[inline]
    pub fn position_quantity(self, size: Decimal) -> Decimal {
        (size * self.sign()).max(Decimal::ZERO)
    }

    #[inline]
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
    #[must_use]
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "UNKNOWN",
            Self::Paper => "PAPER",
            Self::Live => "LIVE",
        }
    }

    #[must_use]
    #[inline]
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
    #[must_use]
    pub(crate) fn from_response(resp: &crate::exchange::BinanceOrderResponse) -> Self {
        Self {
            client_order_id: resp.client_order_id.clone(),
            symbol: resp.symbol.clone(),
            side: resp.side.clone(),
            order_id: resp.order_id,
        }
    }

    #[must_use]
    pub(crate) fn from_order(order: &GridOrder) -> Option<Self> {
        Some(Self {
            client_order_id: order.client_order_id.clone(),
            symbol: order.symbol.clone(),
            side: order.side.as_str().to_owned(),
            order_id: order.order_id?,
        })
    }

    #[must_use]
    #[inline]
    pub(crate) fn matches_order(&self, expected: &GridOrder) -> bool {
        self.client_order_id == expected.client_order_id
            && self.symbol == expected.symbol
            && self.side == expected.side.as_str()
            && expected.order_id.is_none_or(|id| self.order_id == id)
    }

    #[must_use]
    #[inline]
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
    #[must_use]
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

    #[must_use]
    #[inline]
    pub(crate) fn matches_exchange_identity(
        &self,
        resp: &crate::exchange::BinanceOrderResponse,
    ) -> bool {
        self.client_order_id == resp.client_order_id
            && self.symbol == resp.symbol
            && resp.side == self.side.as_str()
            && self.order_id.is_none_or(|id| resp.order_id == id)
    }

    #[must_use]
    #[inline]
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

    #[must_use]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RemainderPhase {
    Canceling,
    Submitting,
    Complete,
    Abandoned,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RemainderPlan {
    pub(crate) symbol: String,
    pub(crate) mode: TradingMode,
    pub(crate) sources: Vec<GridOrder>,
    pub(crate) target: GridOrder,
    pub(crate) phase: RemainderPhase,
}

impl RemainderPlan {
    #[must_use]
    #[inline]
    pub(crate) fn find_order(&self, client_order_id: &str) -> Option<&GridOrder> {
        self.sources
            .iter()
            .chain(std::iter::once(&self.target))
            .find(|order| order.client_order_id == client_order_id)
    }
}
