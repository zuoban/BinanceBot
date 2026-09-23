use crate::config::ExchangeConfig;
use crate::exchange::model::*;
use crate::exchange::signature::sign_query;
use anyhow::{anyhow, Result};
use chrono::Utc;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Client;
use rust_decimal::Decimal;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;
use tracing::{debug, info};

#[derive(Debug, thiserror::Error)]
pub enum ExchangeError {
    #[error("Post-Only (GTX) order rejected as it would execute as taker: {0}")]
    PostOnlyRejected(String),

    #[error("Binance API error (code: {code}): {msg}")]
    ApiError { code: i64, msg: String },

    #[error("Network or HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("General error: {0}")]
    Other(String),
}

pub struct BinanceFuturesClient {
    base_url: String,
    pub _api_key: String,
    api_secret: String,
    client: Client,
    recv_window: u64,
    time_offset_ms: AtomicI64,
}

impl BinanceFuturesClient {
    pub fn new(config: &ExchangeConfig) -> Self {
        let base_url = if config.is_testnet {
            "https://testnet.binancefuture.com".to_string()
        } else {
            "https://fapi.binance.com".to_string()
        };

        let mut headers = HeaderMap::new();
        if !config.api_key.is_empty() {
            if let Ok(mut val) = HeaderValue::from_str(&config.api_key) {
                val.set_sensitive(true);
                headers.insert("X-MBX-APIKEY", val);
            }
        }

        let client = Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(10))
            .build()
            .expect("Failed to build HTTP client");

        Self {
            base_url,
            _api_key: config.api_key.clone(),
            api_secret: config.api_secret.clone(),
            client,
            recv_window: config.recv_window,
            time_offset_ms: AtomicI64::new(0),
        }
    }

    /// Synchronize local timestamp with Binance Futures server time
    pub async fn sync_server_time(&self) -> Result<i64> {
        let url = format!("{}/fapi/v1/time", self.base_url);
        let resp = self.client.get(&url).send().await?.error_for_status()?;
        let server_time: BinanceServerTime = resp.json().await?;

        let local_now = Utc::now().timestamp_millis();
        let offset = server_time.server_time - local_now;
        self.time_offset_ms.store(offset, Ordering::SeqCst);

        info!(
            "Synchronized time with Binance Futures: server_time={}, offset={}ms",
            server_time.server_time, offset
        );
        Ok(offset)
    }

    fn current_timestamp(&self) -> i64 {
        Utc::now().timestamp_millis() + self.time_offset_ms.load(Ordering::SeqCst)
    }

    fn sign_params(&self, params: &str) -> String {
        let signature = sign_query(&self.api_secret, params);
        format!("{}&signature={}", params, signature)
    }

    /// Fetch exchange info (symbol filters)
    pub async fn get_exchange_info(&self, symbol: Option<&str>) -> Result<BinanceExchangeInfo> {
        let url = if let Some(sym) = symbol {
            format!("{}/fapi/v1/exchangeInfo?symbol={}", self.base_url, sym)
        } else {
            format!("{}/fapi/v1/exchangeInfo", self.base_url)
        };

        let resp = self.client.get(&url).send().await?.error_for_status()?;
        let info: BinanceExchangeInfo = resp.json().await?;
        Ok(info)
    }

    /// Fetch the latest traded price.
    pub async fn get_ticker_price(&self, symbol: &str) -> Result<Decimal> {
        let url = format!("{}/fapi/v1/ticker/price?symbol={}", self.base_url, symbol);
        let resp = self.client.get(&url).send().await?.error_for_status()?;
        let ticker: BinanceTickerPrice = resp.json().await?;
        Ok(ticker.price)
    }

    /// Fetch the futures mark price, which differs from the latest traded price.
    pub async fn get_mark_price(&self, symbol: &str) -> Result<Decimal> {
        let url = format!("{}/fapi/v1/premiumIndex?symbol={}", self.base_url, symbol);
        let resp = self.client.get(&url).send().await?.error_for_status()?;
        let mark: BinanceMarkPrice = resp.json().await?;
        Ok(mark.mark_price)
    }

    /// Fetch 24-hour ticker statistics
    pub async fn get_24hr_ticker(&self, symbol: &str) -> Result<Binance24hrTicker> {
        let url = format!("{}/fapi/v1/ticker/24hr?symbol={}", self.base_url, symbol);
        let resp = self.client.get(&url).send().await?.error_for_status()?;
        let ticker: Binance24hrTicker = resp.json().await?;
        Ok(ticker)
    }

    /// Fetch open orders for symbol
    pub async fn get_open_orders(&self, symbol: &str) -> Result<Vec<BinanceOrderResponse>> {
        let ts = self.current_timestamp();
        let query = format!(
            "symbol={}&recvWindow={}&timestamp={}",
            symbol, self.recv_window, ts
        );
        let signed_query = self.sign_params(&query);
        let url = format!("{}/fapi/v1/openOrders?{}", self.base_url, signed_query);

        let resp = self.client.get(&url).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to get open orders: {} - {}", status, text));
        }

        let orders: Vec<BinanceOrderResponse> = resp.json().await?;
        Ok(orders)
    }

    /// Fetch an order's final status before treating it as a fill.
    pub async fn get_order(&self, symbol: &str, order_id: i64) -> Result<BinanceOrderResponse> {
        let ts = self.current_timestamp();
        let query = format!(
            "symbol={}&orderId={}&recvWindow={}&timestamp={}",
            symbol, order_id, self.recv_window, ts
        );
        let signed_query = self.sign_params(&query);
        let url = format!("{}/fapi/v1/order?{}", self.base_url, signed_query);
        let resp = self.client.get(&url).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to get order: {} - {}", status, body));
        }
        Ok(resp.json().await?)
    }

    /// Place an order. If post_only is true, timeInForce is set to GTX (Maker Only)
    pub async fn place_order(
        &self,
        symbol: &str,
        side: &str,
        price: &str,
        quantity: &str,
        client_order_id: &str,
        post_only: bool,
    ) -> std::result::Result<BinanceOrderResponse, ExchangeError> {
        let ts = self.current_timestamp();
        let tif = if post_only { "GTX" } else { "GTC" };

        let query = format!(
            "symbol={}&side={}&type=LIMIT&timeInForce={}&price={}&quantity={}&newClientOrderId={}&recvWindow={}&timestamp={}",
            symbol, side, tif, price, quantity, client_order_id, self.recv_window, ts
        );
        let signed_query = self.sign_params(&query);
        let url = format!("{}/fapi/v1/order?{}", self.base_url, signed_query);

        debug!("Sending Binance order request: side={}, price={}, qty={}, tif={}", side, price, quantity, tif);
        let resp = self.client.post(&url).send().await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if let Ok(err) = serde_json::from_str::<BinanceApiError>(&body) {
                if err.code == -5022 {
                    return Err(ExchangeError::PostOnlyRejected(err.msg));
                }
                return Err(ExchangeError::ApiError {
                    code: err.code,
                    msg: err.msg,
                });
            }
            return Err(ExchangeError::Other(format!(
                "HTTP error {}: {}",
                status, body
            )));
        }

        let order: BinanceOrderResponse = resp.json().await.map_err(|e| {
            ExchangeError::Other(format!("Failed to parse order response: {}", e))
        })?;
        Ok(order)
    }

    /// Cancel a single order by orderId or origClientOrderId
    pub async fn cancel_order(
        &self,
        symbol: &str,
        order_id: Option<i64>,
        client_order_id: Option<&str>,
    ) -> Result<()> {
        let ts = self.current_timestamp();
        let mut query = format!("symbol={}&recvWindow={}&timestamp={}", symbol, self.recv_window, ts);
        if let Some(id) = order_id {
            query.push_str(&format!("&orderId={}", id));
        } else if let Some(cid) = client_order_id {
            query.push_str(&format!("&origClientOrderId={}", cid));
        }

        let signed_query = self.sign_params(&query);
        let url = format!("{}/fapi/v1/order?{}", self.base_url, signed_query);

        let resp = self.client.delete(&url).send().await?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to cancel order: {}", body));
        }
        Ok(())
    }

    /// Cancel all open orders for symbol
    pub async fn cancel_all_orders(&self, symbol: &str) -> Result<()> {
        let ts = self.current_timestamp();
        let query = format!(
            "symbol={}&recvWindow={}&timestamp={}",
            symbol, self.recv_window, ts
        );
        let signed_query = self.sign_params(&query);
        let url = format!("{}/fapi/v1/allOpenOrders?{}", self.base_url, signed_query);

        let resp = self.client.delete(&url).send().await?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to cancel all orders: {}", body));
        }
        Ok(())
    }

    /// Fetch position risk information for symbol
    pub async fn get_position(&self, symbol: &str) -> Result<Option<BinancePositionRisk>> {
        let ts = self.current_timestamp();
        let query = format!(
            "symbol={}&recvWindow={}&timestamp={}",
            symbol, self.recv_window, ts
        );
        let signed_query = self.sign_params(&query);
        let url = format!("{}/fapi/v2/positionRisk?{}", self.base_url, signed_query);

        let resp = self.client.get(&url).send().await?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to get position risk: {}", body));
        }

        let positions: Vec<BinancePositionRisk> = resp.json().await?;
        let pos = positions.into_iter().find(|p| p.symbol.eq_ignore_ascii_case(symbol));
        Ok(pos)
    }

    /// Fetch account balance summary
    pub async fn get_account(&self) -> Result<BinanceAccountInfoResponse> {
        let ts = self.current_timestamp();
        let query = format!("recvWindow={}&timestamp={}", self.recv_window, ts);
        let signed_query = self.sign_params(&query);
        let url = format!("{}/fapi/v2/account?{}", self.base_url, signed_query);

        let resp = self.client.get(&url).send().await?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to get account info: {}", body));
        }

        let acc: BinanceAccountInfoResponse = resp.json().await?;
        Ok(acc)
    }

    /// Request a user data stream listenKey
    pub async fn get_listen_key(&self) -> Result<String> {
        let url = format!("{}/fapi/v1/listenKey", self.base_url);
        let resp = self.client.post(&url).send().await?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to obtain listenKey: {}", body));
        }
        let res: BinanceListenKeyResponse = resp.json().await?;
        Ok(res.listen_key)
    }

    /// Keepalive user data stream listenKey
    pub async fn keepalive_listen_key(&self) -> Result<()> {
        let url = format!("{}/fapi/v1/listenKey", self.base_url);
        let resp = self.client.put(&url).send().await?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to keepalive listenKey: {}", body));
        }
        Ok(())
    }
}
