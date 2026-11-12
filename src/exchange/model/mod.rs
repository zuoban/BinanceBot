use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub(super) const ONE_WAY_POSITION_ERROR: &str =
    "策略仅支持币安单向持仓模式，请先将账户设置为单向持仓";

#[derive(Debug, Deserialize)]
pub(crate) struct BinanceServerTime {
    #[serde(rename = "serverTime")]
    pub(crate) server_time: i64,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub(crate) struct BinanceApiError {
    pub(crate) code: i64,
    pub(crate) msg: String,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct BinanceListenKeyResponse {
    #[serde(rename = "listenKey")]
    pub(crate) listen_key: String,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum BinanceBatchOrderItem {
    Order(BinanceOrderResponse),
    Error(BinanceApiError),
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum CancelBatchTarget<'a> {
    OrderIds(&'a [i64]),
    ClientOrderIds(&'a [&'a str]),
}

#[derive(Debug, Deserialize)]
pub(crate) struct BinanceExchangeInfo {
    pub(crate) symbols: Vec<BinanceSymbolInfo>,
    #[serde(rename = "rateLimits", default)]
    pub(crate) rate_limits: Vec<BinanceRateLimit>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BinanceRateLimit {
    #[serde(rename = "rateLimitType")]
    pub(crate) rate_limit_type: String,
    pub(crate) interval: String,
    #[serde(rename = "intervalNum")]
    pub(crate) interval_num: u64,
    pub(crate) limit: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BinanceSymbolInfo {
    pub(crate) symbol: String,
    pub(crate) status: String,
    #[serde(rename = "contractType", default)]
    pub(crate) contract_type: String,
    pub(crate) filters: Vec<BinanceFilter>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "filterType")]
pub(crate) enum BinanceFilter {
    #[serde(rename = "PRICE_FILTER")]
    PriceFilter {
        #[serde(rename = "minPrice")]
        min_price: Decimal,
        #[serde(rename = "maxPrice")]
        max_price: Decimal,
        #[serde(rename = "tickSize")]
        tick_size: Decimal,
    },
    #[serde(rename = "LOT_SIZE")]
    LotSize {
        #[serde(rename = "minQty")]
        min_qty: Decimal,
        #[serde(rename = "maxQty")]
        max_qty: Decimal,
        #[serde(rename = "stepSize")]
        step_size: Decimal,
    },
    #[serde(rename = "MIN_NOTIONAL")]
    MinNotional {
        #[serde(default)]
        notional: Option<Decimal>,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub(crate) struct BinanceOrderResponse {
    #[serde(rename = "orderId")]
    pub(crate) order_id: i64,
    #[serde(rename = "clientOrderId")]
    pub(crate) client_order_id: String,
    pub(crate) symbol: String,
    pub(crate) status: String,
    pub(crate) price: Decimal,
    #[serde(rename = "avgPrice", default)]
    pub(crate) avg_price: Option<Decimal>,
    #[serde(rename = "origQty")]
    pub(crate) orig_qty: Decimal,
    #[serde(rename = "executedQty")]
    pub(crate) executed_qty: Decimal,
    pub(crate) side: String,
    #[serde(rename = "type")]
    pub(crate) order_type: String,
    #[serde(rename = "timeInForce")]
    pub(crate) time_in_force: String,
    #[serde(rename = "updateTime")]
    pub(crate) update_time: Option<i64>,
}

impl BinanceOrderResponse {
    #[must_use]
    #[inline]
    pub(crate) fn is_terminal_status(status: &str) -> bool {
        matches!(
            status,
            "FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
        )
    }

    #[must_use]
    #[inline]
    pub(crate) fn is_terminal(&self) -> bool {
        Self::is_terminal_status(&self.status)
    }

    #[must_use]
    #[inline]
    pub(crate) fn is_unfilled_terminal_status(status: &str) -> bool {
        matches!(
            status,
            "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
        )
    }

    #[must_use]
    #[inline]
    pub(crate) fn is_unfilled_terminal(&self) -> bool {
        Self::is_unfilled_terminal_status(&self.status)
    }

    #[must_use]
    #[inline]
    pub(crate) fn is_known_status(status: &str) -> bool {
        matches!(
            status,
            "NEW"
                | "PARTIALLY_FILLED"
                | "FILLED"
                | "CANCELED"
                | "EXPIRED"
                | "EXPIRED_IN_MATCH"
                | "REJECTED"
        )
    }

    #[must_use]
    #[inline]
    pub(crate) fn open_order_status(&self) -> crate::types::OrderStatus {
        if self.executed_qty > Decimal::ZERO {
            crate::types::OrderStatus::PartiallyFilled
        } else {
            crate::types::OrderStatus::New
        }
    }

    #[must_use]
    #[inline]
    pub(crate) fn remaining_qty(&self) -> Decimal {
        self.orig_qty
            .checked_sub(self.executed_qty)
            .unwrap_or(Decimal::ZERO)
            .max(Decimal::ZERO)
    }

    #[must_use]
    #[inline]
    pub(crate) fn has_valid_symbol_and_side(&self, symbol: &str) -> bool {
        self.symbol == symbol && crate::types::OrderSide::from_exchange_str(&self.side).is_some()
    }

    #[must_use]
    #[inline]
    pub(crate) fn matches_identity(&self, other: &Self) -> bool {
        self.client_order_id == other.client_order_id
            && self.symbol == other.symbol
            && self.side == other.side
            && self.order_id == other.order_id
    }

    #[must_use]
    #[inline]
    pub(crate) fn matches_snapshot(&self, other: &Self) -> bool {
        self.matches_identity(other)
            && self.status == other.status
            && self.executed_qty == other.executed_qty
            && self.orig_qty == other.orig_qty
            && self.price == other.price
            && self.avg_price == other.avg_price
            && self.update_time == other.update_time
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct BinanceUserTrade {
    #[serde(rename = "realizedPnl")]
    pub(crate) realized_pnl: Decimal,
    pub(crate) commission: Decimal,
    #[serde(rename = "commissionAsset")]
    pub(crate) commission_asset: String,
    pub(crate) maker: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BinanceTickerPrice {
    pub(crate) price: Decimal,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BinanceMarkPrice {
    #[serde(rename = "markPrice")]
    pub(crate) mark_price: Decimal,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BinanceMarkPriceWithSymbol {
    pub(crate) symbol: String,
    #[serde(rename = "markPrice")]
    pub(crate) mark_price: Decimal,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct Binance24hrTicker {
    #[serde(rename = "lastPrice")]
    pub(crate) last_price: Decimal,
    #[serde(rename = "priceChange")]
    pub(crate) price_change: Decimal,
    #[serde(rename = "priceChangePercent")]
    pub(crate) price_change_percent: Decimal,
    #[serde(rename = "highPrice")]
    pub(crate) high_price: Decimal,
    #[serde(rename = "lowPrice")]
    pub(crate) low_price: Decimal,
    pub(crate) volume: Decimal,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct BinancePositionRisk {
    pub(crate) symbol: String,
    #[serde(rename = "positionSide", default)]
    pub(crate) position_side: Option<String>,
    #[serde(rename = "positionAmt")]
    pub(crate) position_amt: Decimal,
    #[serde(rename = "entryPrice")]
    pub(crate) entry_price: Decimal,
    #[serde(rename = "markPrice")]
    pub(crate) mark_price: Decimal,
    #[serde(rename = "unRealizedProfit")]
    pub(crate) un_realized_profit: Decimal,
    #[serde(rename = "liquidationPrice")]
    pub(crate) liquidation_price: Decimal,
    pub(crate) leverage: String,
}

impl BinancePositionRisk {
    #[must_use]
    #[inline]
    pub(crate) fn into_position_info(
        self,
        update_time: Option<chrono::DateTime<chrono::Utc>>,
    ) -> crate::types::PositionInfo {
        crate::types::PositionInfo {
            symbol: self.symbol,
            size: self.position_amt,
            entry_price: self.entry_price,
            mark_price: self.mark_price,
            unrealized_pnl: self.un_realized_profit,
            liquidation_price: self.liquidation_price,
            leverage: self
                .leverage
                .parse::<u32>()
                .ok()
                .filter(|&v| v > 0)
                .unwrap_or(1),
            update_time,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct BinanceAccountAsset {
    pub(crate) asset: String,
    #[serde(rename = "walletBalance")]
    pub(crate) wallet_balance: Decimal,
    #[serde(rename = "unrealizedProfit")]
    pub(crate) unrealized_profit: Decimal,
    #[serde(rename = "marginBalance")]
    pub(crate) margin_balance: Decimal,
    #[serde(rename = "availableBalance")]
    pub(crate) available_balance: Decimal,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BinanceAccountInfoResponse {
    pub(crate) assets: Vec<BinanceAccountAsset>,
}

impl BinanceAccountInfoResponse {
    /// USDⓈ-M balances are reported per asset. The account-wide totals can be zero
    /// for a USDC-margined symbol even when its USDC balance is nonzero.
    #[must_use]
    #[inline]
    pub(crate) fn margin_asset_for_symbol(&self, symbol: &str) -> Option<&BinanceAccountAsset> {
        self.assets
            .iter()
            .filter(|asset| symbol.ends_with(&asset.asset))
            .max_by_key(|asset| asset.asset.len())
    }
}

#[cfg(test)]
mod tests;
