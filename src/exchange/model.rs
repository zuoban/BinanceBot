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
    pub positions: Vec<BinancePositionRisk>,
}

#[derive(Debug, Deserialize)]
pub struct BinanceListenKeyResponse {
    #[serde(rename = "listenKey")]
    pub listen_key: String,
}

/// WebSocket 24hr Mini-Ticker payload
#[derive(Debug, Deserialize)]
pub struct BinanceWs24hrTicker {
    #[serde(rename = "e")]
    pub event_type: String,
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

#[cfg(test)]
mod tests {
    use super::BinanceMarkPrice;
    use rust_decimal_macros::dec;

    #[test]
    fn premium_index_uses_mark_price_field() {
        let response = r#"{"symbol":"SOLUSDC","markPrice":"114.56873000","indexPrice":"114.57000000"}"#;
        let mark: BinanceMarkPrice = serde_json::from_str(response).unwrap();
        assert_eq!(mark.mark_price, dec!(114.56873000));
    }
}
