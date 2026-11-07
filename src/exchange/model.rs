use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct BinanceServerTime {
    #[serde(rename = "serverTime")]
    pub server_time: i64,
}

#[derive(Debug, Deserialize)]
pub struct BinanceApiError {
    pub code: i64,
    pub msg: String,
}

#[derive(Debug, Deserialize)]
pub struct BinanceExchangeInfo {
    pub symbols: Vec<BinanceSymbolInfo>,
    #[serde(rename = "rateLimits", default)]
    pub rate_limits: Vec<BinanceRateLimit>,
}

#[derive(Debug, Deserialize)]
pub struct BinanceRateLimit {
    #[serde(rename = "rateLimitType")]
    pub rate_limit_type: String,
    pub interval: String,
    #[serde(rename = "intervalNum")]
    pub interval_num: u64,
    pub limit: u64,
}

#[derive(Debug, Deserialize)]
pub struct BinanceSymbolInfo {
    pub symbol: String,
    pub status: String,
    #[serde(rename = "pricePrecision")]
    pub price_precision: u32,
    #[serde(rename = "quantityPrecision")]
    pub quantity_precision: u32,
    pub filters: Vec<BinanceFilter>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "filterType")]
pub enum BinanceFilter {
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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BinanceOrderResponse {
    #[serde(rename = "orderId")]
    pub order_id: i64,
    #[serde(rename = "clientOrderId")]
    pub client_order_id: String,
    pub symbol: String,
    pub status: String,
    pub price: Decimal,
    #[serde(rename = "avgPrice", default)]
    pub avg_price: Option<Decimal>,
    #[serde(rename = "origQty")]
    pub orig_qty: Decimal,
    #[serde(rename = "executedQty")]
    pub executed_qty: Decimal,
    pub side: String,
    #[serde(rename = "type")]
    pub order_type: String,
    #[serde(rename = "timeInForce")]
    pub time_in_force: String,
    #[serde(rename = "updateTime")]
    pub update_time: Option<i64>,
}

impl BinanceOrderResponse {
    pub fn is_terminal_status(status: &str) -> bool {
        matches!(
            status,
            "FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
        )
    }

    pub fn is_terminal(&self) -> bool {
        Self::is_terminal_status(&self.status)
    }

    pub fn is_unfilled_terminal_status(status: &str) -> bool {
        matches!(
            status,
            "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
        )
    }

    pub fn is_unfilled_terminal(&self) -> bool {
        Self::is_unfilled_terminal_status(&self.status)
    }

    pub fn is_known_status(status: &str) -> bool {
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

    pub fn matches_identity(&self, other: &Self) -> bool {
        self.client_order_id == other.client_order_id
            && self.symbol == other.symbol
            && self.side == other.side
            && self.order_id == other.order_id
    }

    pub fn matches_snapshot(&self, other: &Self) -> bool {
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
pub struct BinanceUserTrade {
    #[serde(rename = "realizedPnl")]
    pub realized_pnl: Decimal,
    pub commission: Decimal,
    #[serde(rename = "commissionAsset")]
    pub commission_asset: String,
    pub maker: bool,
}

#[derive(Debug, Deserialize)]
pub struct BinanceTickerPrice {
    pub symbol: String,
    pub price: Decimal,
    pub time: i64,
}

#[derive(Debug, Deserialize)]
pub struct BinanceMarkPrice {
    #[serde(rename = "markPrice")]
    pub mark_price: Decimal,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BinanceMarkPriceWithSymbol {
    pub symbol: String,
    #[serde(rename = "markPrice")]
    pub mark_price: Decimal,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Binance24hrTicker {
    pub symbol: String,
    #[serde(rename = "lastPrice")]
    pub last_price: Decimal,
    #[serde(rename = "priceChange")]
    pub price_change: Decimal,
    #[serde(rename = "priceChangePercent")]
    pub price_change_percent: Decimal,
    #[serde(rename = "highPrice")]
    pub high_price: Decimal,
    #[serde(rename = "lowPrice")]
    pub low_price: Decimal,
    pub volume: Decimal,
    #[serde(rename = "quoteVolume")]
    pub quote_volume: Decimal,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BinancePositionRisk {
    pub symbol: String,
    #[serde(rename = "positionSide", default)]
    pub position_side: Option<String>,
    #[serde(rename = "positionAmt")]
    pub position_amt: Decimal,
    #[serde(rename = "entryPrice")]
    pub entry_price: Decimal,
    #[serde(rename = "markPrice")]
    pub mark_price: Decimal,
    #[serde(rename = "unRealizedProfit")]
    pub un_realized_profit: Decimal,
    #[serde(rename = "liquidationPrice")]
    pub liquidation_price: Decimal,
    pub leverage: String,
}

impl BinancePositionRisk {
    pub fn into_position_info(
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
            leverage: self.leverage.parse().unwrap_or(20),
            update_time,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct BinanceAccountAsset {
    pub asset: String,
    #[serde(rename = "walletBalance")]
    pub wallet_balance: Decimal,
    #[serde(rename = "unrealizedProfit")]
    pub unrealized_profit: Decimal,
    #[serde(rename = "marginBalance")]
    pub margin_balance: Decimal,
    #[serde(rename = "availableBalance")]
    pub available_balance: Decimal,
}

#[derive(Debug, Deserialize)]
pub struct BinanceAccountInfoResponse {
    #[serde(rename = "totalWalletBalance")]
    pub total_wallet_balance: Decimal,
    #[serde(rename = "totalMarginBalance")]
    pub total_margin_balance: Decimal,
    #[serde(rename = "totalUnrealizedProfit")]
    pub total_unrealized_profit: Decimal,
    #[serde(rename = "availableBalance")]
    pub available_balance: Decimal,
    pub assets: Vec<BinanceAccountAsset>,
}

impl BinanceAccountInfoResponse {
    /// USDⓈ-M balances are reported per asset. The account-wide totals can be zero
    /// for a USDC-margined symbol even when its USDC balance is nonzero.
    pub fn margin_asset_for_symbol(&self, symbol: &str) -> Option<&BinanceAccountAsset> {
        self.assets
            .iter()
            .filter(|asset| symbol.ends_with(&asset.asset))
            .max_by_key(|asset| asset.asset.len())
    }
}

/// WebSocket 24hr Mini-Ticker payload
#[derive(Debug, Deserialize)]
pub struct BinanceWs24hrTicker {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "c")]
    pub close_price: Decimal,
    #[serde(rename = "o")]
    pub open_price: Decimal,
    #[serde(rename = "h")]
    pub high_price: Decimal,
    #[serde(rename = "l")]
    pub low_price: Decimal,
    #[serde(rename = "v")]
    pub volume: Decimal,
    #[serde(rename = "q")]
    pub quote_volume: Decimal,
}

#[derive(Debug, Deserialize)]
pub struct BinanceWsMarkPrice {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "p")]
    pub mark_price: Decimal,
}

#[cfg(test)]
mod tests {
    use super::{BinanceAccountInfoResponse, BinanceMarkPrice, BinanceUserTrade};
    use rust_decimal_macros::dec;

    #[test]
    fn premium_index_uses_mark_price_field() {
        let response =
            r#"{"symbol":"SOLUSDC","markPrice":"114.56873000","indexPrice":"114.57000000"}"#;
        let mark: BinanceMarkPrice = serde_json::from_str(response).unwrap();
        assert_eq!(mark.mark_price, dec!(114.56873000));
    }

    #[test]
    fn user_trade_reads_exchange_realized_pnl_and_fee() {
        let response =
            r#"{"realizedPnl":"-3.25","commission":"0.80","commissionAsset":"USDC","maker":true}"#;
        let trade: BinanceUserTrade = serde_json::from_str(response).unwrap();
        assert_eq!(trade.realized_pnl, dec!(-3.25));
        assert_eq!(trade.commission, dec!(0.80));
        assert_eq!(trade.commission_asset, "USDC");
    }

    #[test]
    fn usdc_account_balance_comes_from_matching_asset() {
        let response = r#"{
            "totalWalletBalance":"0.00000000",
            "totalMarginBalance":"0.00000000",
            "totalUnrealizedProfit":"0.00000000",
            "availableBalance":"0.00000000",
            "assets":[{"asset":"USDT","walletBalance":"0","marginBalance":"0","availableBalance":"0","unrealizedProfit":"0"},
                      {"asset":"USDC","walletBalance":"10091.02762121","marginBalance":"9852.87142345","availableBalance":"6358.01937745","unrealizedProfit":"-238.15619776"}],
            "positions":[{"symbol":"SOLUSDC","positionAmt":"20.00"}]
        }"#;
        let account: BinanceAccountInfoResponse = serde_json::from_str(response).unwrap();
        let asset = account.margin_asset_for_symbol("SOLUSDC").unwrap();
        assert_eq!(asset.asset, "USDC");
        assert_eq!(asset.margin_balance, dec!(9852.87142345));
        assert_eq!(asset.available_balance, dec!(6358.01937745));
    }

    #[test]
    fn order_status_and_position_risk_helpers_classify_responses() {
        assert!(super::BinanceOrderResponse::is_terminal_status("FILLED"));
        assert!(!super::BinanceOrderResponse::is_unfilled_terminal_status(
            "FILLED"
        ));
        assert!(super::BinanceOrderResponse::is_unfilled_terminal_status(
            "CANCELED"
        ));
        assert!(super::BinanceOrderResponse::is_known_status(
            "PARTIALLY_FILLED"
        ));
        assert!(!super::BinanceOrderResponse::is_known_status(
            "PENDING_CANCEL"
        ));

        let risk = super::BinancePositionRisk {
            symbol: "SOLUSDC".into(),
            position_side: None,
            position_amt: dec!(3.5),
            entry_price: dec!(120),
            mark_price: dec!(125),
            un_realized_profit: dec!(17.5),
            liquidation_price: dec!(80),
            leverage: "15".into(),
        };
        let pos = risk.into_position_info(None);
        assert_eq!(pos.symbol, "SOLUSDC");
        assert_eq!(pos.size, dec!(3.5));
        assert_eq!(pos.leverage, 15);
    }
}
