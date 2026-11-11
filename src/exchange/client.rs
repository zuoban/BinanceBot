use crate::config::ExchangeConfig;
use crate::exchange::model::*;
use crate::exchange::rate_limit::{RateLimiter, RequestClass};
use crate::exchange::signature::{
    build_signed_url, compute_account_cache_key, identity_key, new_hmac_key, HmacSha256,
};
use anyhow::{anyhow, Result};
use chrono::Utc;
use reqwest::header::HeaderValue;
use reqwest::Client;
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

static MAINNET_TIME_OFFSET_MS: AtomicI64 = AtomicI64::new(0);
static MONOTONIC_EPOCH: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);
const REACTIVE_TIME_SYNC_THROTTLE_MS: u64 = 10_000;

fn monotonic_now_ms() -> u64 {
    (MONOTONIC_EPOCH.elapsed().as_millis() as u64).saturating_add(1)
}

pub(super) fn mainnet_time_offset_ms() -> i64 {
    MAINNET_TIME_OFFSET_MS.load(Ordering::SeqCst)
}

fn is_invalid_timestamp_body(body: &str) -> bool {
    serde_json::from_str::<BinanceApiError>(body).is_ok_and(|error| error.code == -1021)
}
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct LocalRateLimitError(String);

#[derive(Debug, thiserror::Error)]
pub(crate) enum ExchangeError {
    #[error("Post-Only (GTX) order rejected as it would execute as taker: {0}")]
    PostOnlyRejected(String),

    #[error("Binance API error (code: {code}): {msg}")]
    ApiError { code: i64, msg: String },

    #[error("Order execution status is unknown (code: {code:?}): {msg}")]
    ExecutionUnknown { code: Option<i64>, msg: String },

    #[error("Order request was rejected: {0}")]
    RequestRejected(String),

    #[error("Network or HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("General error: {0}")]
    Other(String),
}

impl ExchangeError {
    pub(crate) fn execution_status_unknown(&self) -> bool {
        matches!(
            self,
            Self::ExecutionUnknown { .. } | Self::HttpError(_) | Self::Other(_)
        )
    }
}

fn order_response_error(status: reqwest::StatusCode, body: String) -> ExchangeError {
    let api_error = serde_json::from_str::<BinanceApiError>(&body).ok();
    let code = api_error.as_ref().map(|error| error.code);
    let msg = api_error
        .as_ref()
        .map(|error| error.msg.as_str())
        .unwrap_or(&body);
    // These overload responses explicitly mean the submission failed. Other
    // server errors and backend timeouts may have accepted an order already.
    let known_failure = status == reqwest::StatusCode::SERVICE_UNAVAILABLE
        && (code == Some(-1008)
            || msg == "Service Unavailable."
            || msg == "Internal error; unable to process your request. Please try again.");
    if !known_failure
        && (matches!(code, Some(-1000 | -1001 | -1006 | -1007))
            || status == reqwest::StatusCode::REQUEST_TIMEOUT
            || status.is_server_error())
    {
        return ExchangeError::ExecutionUnknown {
            code,
            msg: msg.to_string(),
        };
    }
    match api_error {
        Some(error) if error.code == -5022 => ExchangeError::PostOnlyRejected(error.msg),
        Some(error) => ExchangeError::ApiError {
            code: error.code,
            msg: error.msg,
        },
        None if known_failure => {
            ExchangeError::RequestRejected(format!("HTTP {}: {}", status, body))
        }
        None => ExchangeError::Other(format!("HTTP error {}: {}", status, body)),
    }
}
fn parse_api_error_body(status: reqwest::StatusCode, body: String) -> anyhow::Error {
    match serde_json::from_str::<BinanceApiError>(&body) {
        Ok(error) => ExchangeError::ApiError {
            code: error.code,
            msg: error.msg,
        }
        .into(),
        Err(_) => ExchangeError::Other(format!("HTTP error {}: {}", status, body)).into(),
    }
}

async fn checked_response(response: reqwest::Response) -> Result<reqwest::Response> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let body = response.text().await.map_err(reqwest::Error::without_url)?;
    Err(parse_api_error_body(status, body))
}

pub(crate) struct NewOrderRequest<'a> {
    pub(crate) symbol: &'a str,
    pub(crate) side: &'a str,
    pub(crate) price: &'a str,
    pub(crate) quantity: &'a str,
    pub(crate) client_order_id: &'a str,
    pub(crate) post_only: bool,
    pub(crate) reduce_only: bool,
}

#[path = "fill_reconciler.rs"]
mod fill_reconciler;

use fill_reconciler::FillReconcilerState;
#[cfg(test)]
use fill_reconciler::{ExecutionFill, FillCacheCleanup, FillCacheEntry, FillProgress};

pub(crate) fn shared_http_client() -> Client {
    static CLIENT: std::sync::LazyLock<Client> = std::sync::LazyLock::new(|| {
        Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("Failed to build HTTP client")
    });
    CLIENT.clone()
}

pub struct BinanceFuturesClient {
    base_url: String,
    api_key_header: Option<HeaderValue>,
    hmac_key: HmacSha256,
    client: Client,
    recv_window: u64,
    account_cache_key: [u8; 32],
    account_identity_key: [u8; 32],
    rate_account_key: [u8; 32],
    time_offset_ms: AtomicI64,
    last_time_sync_ms: AtomicU64,
    limiter: Arc<RateLimiter>,
    fill_reconciler: std::sync::Mutex<FillReconcilerState>,
    fill_retention_seconds: AtomicU64,
}

impl BinanceFuturesClient {
    pub(crate) fn market_cache_key(&self) -> &str {
        &self.base_url
    }

    pub(crate) async fn get_all_mark_prices_for_fallback(
        &self,
    ) -> Result<Vec<BinanceMarkPriceWithSymbol>> {
        self.get_public_json(
            10,
            RequestClass::MarketFallback,
            "/fapi/v1/premiumIndex",
            &[],
        )
        .await
    }

    pub fn new(config: &ExchangeConfig) -> Self {
        Self::new_with_limiter(config, Arc::new(RateLimiter::default()))
    }

    pub(crate) fn new_with_limiter(config: &ExchangeConfig, limiter: Arc<RateLimiter>) -> Self {
        Self::with_base_url_and_limiter(config, "https://fapi.binance.com".to_string(), limiter)
    }

    pub(crate) fn with_base_url_and_limiter(
        config: &ExchangeConfig,
        base_url: String,
        limiter: Arc<RateLimiter>,
    ) -> Self {
        let api_key_header = if !config.api_key.is_empty() {
            HeaderValue::from_str(&config.api_key).ok().map(|mut val| {
                val.set_sensitive(true);
                val
            })
        } else {
            None
        };

        let account_cache_key = compute_account_cache_key(
            &base_url,
            &config.api_key,
            &config.api_secret,
            config.recv_window,
        );
        let account_identity_key = identity_key(&base_url, &config.api_key);
        let rate_account_key = Sha256::digest(config.api_key.as_bytes()).into();
        let initial_offset_ms = if base_url == "https://fapi.binance.com" {
            MAINNET_TIME_OFFSET_MS.load(Ordering::SeqCst)
        } else {
            0
        };

        Self {
            base_url,
            api_key_header,
            hmac_key: new_hmac_key(&config.api_secret),
            client: shared_http_client(),
            recv_window: config.recv_window,
            account_cache_key,
            account_identity_key,
            rate_account_key,
            time_offset_ms: AtomicI64::new(initial_offset_ms),
            last_time_sync_ms: AtomicU64::new(0),
            limiter,
            fill_reconciler: std::sync::Mutex::new(FillReconcilerState::default()),
            fill_retention_seconds: AtomicU64::new(600),
        }
    }

    /// Rebuild credentials while keeping the configured endpoint, limiter, and synced clock offset.
    pub(crate) fn reconfig(&self, config: &ExchangeConfig) -> Self {
        let rebuilt = Self::with_base_url_and_limiter(
            config,
            self.base_url.clone(),
            Arc::clone(&self.limiter),
        );
        rebuilt
            .time_offset_ms
            .store(self.time_offset_ms.load(Ordering::SeqCst), Ordering::SeqCst);
        rebuilt.last_time_sync_ms.store(
            self.last_time_sync_ms.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        rebuilt
    }

    #[cfg(test)]
    pub(crate) fn with_base_url(config: &ExchangeConfig, base_url: String) -> Self {
        Self::with_base_url_and_limiter(config, base_url, Arc::new(RateLimiter::default()))
    }

    /// Synchronize local timestamp with Binance Futures server time
    pub(crate) async fn sync_server_time(&self) -> Result<i64> {
        let before_ms = Utc::now().timestamp_millis();
        let server_time: BinanceServerTime = self
            .get_public_json(1, RequestClass::Normal, "/fapi/v1/time", &[])
            .await?;
        let after_ms = Utc::now().timestamp_millis();
        let midpoint = before_ms / 2 + after_ms / 2 + (before_ms & after_ms & 1);
        let offset = server_time.server_time.saturating_sub(midpoint);
        self.time_offset_ms.store(offset, Ordering::SeqCst);
        self.last_time_sync_ms
            .store(monotonic_now_ms(), Ordering::SeqCst);
        if self.base_url == "https://fapi.binance.com" {
            MAINNET_TIME_OFFSET_MS.store(offset, Ordering::SeqCst);
        }

        info!(
            "Synchronized time with Binance Futures: server_time={}, offset={}ms",
            server_time.server_time, offset
        );
        Ok(offset)
    }

    pub(super) async fn maybe_resync_on_invalid_timestamp(&self, body: &str) {
        if !is_invalid_timestamp_body(body) {
            return;
        }
        let now_ms = monotonic_now_ms();
        let prev_ms = self.last_time_sync_ms.load(Ordering::SeqCst);
        if prev_ms > 0 && now_ms.saturating_sub(prev_ms) < REACTIVE_TIME_SYNC_THROTTLE_MS {
            return;
        }
        if self
            .last_time_sync_ms
            .compare_exchange(prev_ms, now_ms, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        if let Err(error) = self.sync_server_time().await {
            warn!(
                "Reactive Binance server time resync after -1021 failed: {}",
                error
            );
        }
    }

    async fn checked_signed_response(
        &self,
        response: reqwest::Response,
    ) -> Result<reqwest::Response> {
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status();
        let body = response.text().await.map_err(reqwest::Error::without_url)?;
        self.maybe_resync_on_invalid_timestamp(&body).await;
        Err(parse_api_error_body(status, body))
    }

    async fn ensure_signed_success(
        &self,
        resp: reqwest::Response,
        context: &str,
        include_status: bool,
    ) -> Result<reqwest::Response> {
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        self.maybe_resync_on_invalid_timestamp(&body).await;
        if include_status {
            Err(anyhow!("{}: {} - {}", context, status, body))
        } else {
            Err(anyhow!("{}: {}", context, body))
        }
    }

    fn current_timestamp(&self) -> i64 {
        Utc::now()
            .timestamp_millis()
            .saturating_add(self.time_offset_ms.load(Ordering::SeqCst))
    }

    fn signed_url(&self, path: &str, params: &str) -> String {
        build_signed_url(
            &self.base_url,
            &self.hmac_key,
            self.recv_window,
            self.current_timestamp(),
            path,
            params,
        )
    }

    pub(crate) fn account_cache_key(&self) -> [u8; 32] {
        self.account_cache_key
    }

    // Recovery records belong to the exchange account, independent of request
    // timing settings or changes to the API secret for the same API key.
    pub(crate) fn account_identity_key(&self) -> [u8; 32] {
        self.account_identity_key
    }

    pub(crate) fn configured_account_identity(config: &ExchangeConfig) -> [u8; 32] {
        identity_key("https://fapi.binance.com", &config.api_key)
    }

    #[cfg(test)]
    pub(crate) fn limiter(&self) -> Arc<RateLimiter> {
        self.limiter.clone()
    }

    fn reserve(&self, weight: u64, class: RequestClass) -> Result<()> {
        self.limiter
            .reserve(&self.base_url, self.rate_account_key, weight, class)
            .map_err(|error| {
                tracing::warn!("{}", error);
                anyhow!(LocalRateLimitError(error))
            })
    }

    async fn send(
        &self,
        mut request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, reqwest::Error> {
        if let Some(api_key) = &self.api_key_header {
            request = request.header("X-MBX-APIKEY", api_key.clone());
        }
        let started = tracing::enabled!(tracing::Level::DEBUG).then(Instant::now);
        let request = match request.build() {
            Ok(request) => request,
            Err(error) => {
                if let Some(started) = started {
                    debug!(
                        stage = "build",
                        result = "error",
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "Exchange request finished"
                    );
                }
                return Err(error.without_url());
            }
        };
        // Only retain the path, never the signed query or the full URL.
        let request_info =
            started.map(|_| (request.method().clone(), request.url().path().to_owned()));
        let response = self.client.execute(request).await;
        if let (Some(started), Some((method, path))) = (started, request_info) {
            match &response {
                Ok(response) => {
                    debug!(%method, %path, status = response.status().as_u16(), result = if response.status().is_success() { "success" } else { "http_error" }, elapsed_ms = started.elapsed().as_millis() as u64, "Exchange request finished")
                }
                Err(_) => {
                    debug!(%method, %path, result = "transport_error", elapsed_ms = started.elapsed().as_millis() as u64, "Exchange request finished")
                }
            }
        }
        let response = response.map_err(reqwest::Error::without_url)?;
        self.limiter.observe(
            &self.base_url,
            self.rate_account_key,
            response.status(),
            response.headers(),
        );
        Ok(response)
    }

    async fn get_public_json<T: serde::de::DeserializeOwned>(
        &self,
        weight: u64,
        class: RequestClass,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<T> {
        self.reserve(weight, class)?;
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.get(&url);
        if !query.is_empty() {
            req = req.query(query);
        }
        let resp = checked_response(self.send(req).await?).await?;
        Ok(resp.json().await.map_err(reqwest::Error::without_url)?)
    }

    async fn get_signed_json<T: serde::de::DeserializeOwned>(
        &self,
        weight: u64,
        class: RequestClass,
        path: &str,
        query: &str,
        context: &str,
        include_status: bool,
    ) -> Result<T> {
        self.reserve(weight, class)?;
        let url = self.signed_url(path, query);
        let resp = self
            .ensure_signed_success(
                self.send(self.client.get(&url)).await?,
                context,
                include_status,
            )
            .await?;
        Ok(resp.json().await.map_err(reqwest::Error::without_url)?)
    }

    /// Fetch exchange info (symbol filters)
    pub(crate) async fn get_exchange_info(
        &self,
        symbol: Option<&str>,
    ) -> Result<BinanceExchangeInfo> {
        let query: &[(&str, &str)] = match symbol {
            Some(sym) => &[("symbol", sym)],
            None => &[],
        };
        let info: BinanceExchangeInfo = self
            .get_public_json(1, RequestClass::Normal, "/fapi/v1/exchangeInfo", query)
            .await?;
        self.limiter
            .update_exchange_limits(&self.base_url, &info.rate_limits);
        Ok(info)
    }

    async fn get_symbol_public<T: serde::de::DeserializeOwned>(
        &self,
        class: RequestClass,
        path: &str,
        symbol: &str,
    ) -> Result<T> {
        self.get_public_json(1, class, path, &[("symbol", symbol)])
            .await
    }

    /// Fetch the latest traded price.
    pub(crate) async fn get_ticker_price(&self, symbol: &str) -> Result<Decimal> {
        let ticker: BinanceTickerPrice = self
            .get_symbol_public(RequestClass::Normal, "/fapi/v1/ticker/price", symbol)
            .await?;
        Ok(ticker.price)
    }

    pub(crate) async fn get_ticker_price_for_fallback(&self, symbol: &str) -> Result<Decimal> {
        let ticker: BinanceTickerPrice = self
            .get_symbol_public(
                RequestClass::MarketFallback,
                "/fapi/v1/ticker/price",
                symbol,
            )
            .await?;
        Ok(ticker.price)
    }

    /// Fetch the futures mark price, which differs from the latest traded price.
    pub(crate) async fn get_mark_price(&self, symbol: &str) -> Result<Decimal> {
        let mark: BinanceMarkPrice = self
            .get_symbol_public(RequestClass::Normal, "/fapi/v1/premiumIndex", symbol)
            .await?;
        Ok(mark.mark_price)
    }

    pub(crate) async fn get_mark_price_for_fallback(&self, symbol: &str) -> Result<Decimal> {
        let mark: BinanceMarkPrice = self
            .get_symbol_public(
                RequestClass::MarketFallback,
                "/fapi/v1/premiumIndex",
                symbol,
            )
            .await?;
        Ok(mark.mark_price)
    }

    /// Fetch 24-hour ticker statistics
    pub(crate) async fn get_24hr_ticker(&self, symbol: &str) -> Result<Binance24hrTicker> {
        self.get_symbol_public(RequestClass::Normal, "/fapi/v1/ticker/24hr", symbol)
            .await
    }

    pub(crate) async fn get_24hr_ticker_for_fallback(
        &self,
        symbol: &str,
    ) -> Result<Binance24hrTicker> {
        self.get_symbol_public(RequestClass::MarketFallback, "/fapi/v1/ticker/24hr", symbol)
            .await
    }
    /// Fetch open orders for symbol
    pub(crate) async fn get_open_orders(&self, symbol: &str) -> Result<Vec<BinanceOrderResponse>> {
        self.get_open_orders_with_class(symbol, RequestClass::Critical)
            .await
    }

    pub(crate) async fn get_open_orders_for_periodic_sync(
        &self,
        symbol: &str,
    ) -> Result<Vec<BinanceOrderResponse>> {
        self.get_open_orders_with_class(symbol, RequestClass::Normal)
            .await
    }

    async fn get_open_orders_with_class(
        &self,
        symbol: &str,
        class: RequestClass,
    ) -> Result<Vec<BinanceOrderResponse>> {
        self.reserve(1, class)?;
        let url = self.signed_url("/fapi/v1/openOrders", &format!("symbol={}", symbol));

        let resp = self
            .checked_signed_response(self.send(self.client.get(&url)).await?)
            .await?;

        let orders: Vec<BinanceOrderResponse> =
            resp.json().await.map_err(reqwest::Error::without_url)?;
        Ok(orders)
    }

    /// Fetch an order's final status before treating it as a fill.
    pub(crate) async fn get_order(
        &self,
        symbol: &str,
        order_id: i64,
    ) -> Result<BinanceOrderResponse> {
        self.get_signed_json(
            1,
            RequestClass::Critical,
            "/fapi/v1/order",
            &format!("symbol={}&orderId={}", symbol, order_id),
            "Failed to get order",
            true,
        )
        .await
    }

    /// Resolve a persisted client order ID after a restart.
    pub(crate) async fn get_order_by_client_id(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<BinanceOrderResponse> {
        self.require_order_by_client_id(symbol, client_order_id, RequestClass::Critical)
            .await
    }

    /// Restore a historical order ID without consuming emergency request capacity.
    pub(crate) async fn get_order_by_client_id_for_pnl(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<BinanceOrderResponse> {
        self.require_order_by_client_id(symbol, client_order_id, RequestClass::PnlReconciliation)
            .await
    }

    async fn require_order_by_client_id(
        &self,
        symbol: &str,
        client_order_id: &str,
        class: RequestClass,
    ) -> Result<BinanceOrderResponse> {
        self.lookup_order_by_client_id_with_class(symbol, client_order_id, class)
            .await?
            .ok_or_else(|| anyhow!("Order {} was not found on Binance", client_order_id))
    }

    /// Only Binance's explicit unknown-order code means it is safe to submit this ID.
    pub(crate) async fn lookup_order_by_client_id(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<Option<BinanceOrderResponse>> {
        self.lookup_order_by_client_id_with_class(symbol, client_order_id, RequestClass::Critical)
            .await
    }

    async fn lookup_order_by_client_id_with_class(
        &self,
        symbol: &str,
        client_order_id: &str,
        class: RequestClass,
    ) -> Result<Option<BinanceOrderResponse>> {
        self.reserve(1, class)?;
        let url = self.signed_url(
            "/fapi/v1/order",
            &format!("symbol={}&origClientOrderId={}", symbol, client_order_id),
        );
        let resp = self.send(self.client.get(&url)).await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if status == reqwest::StatusCode::BAD_REQUEST
                && serde_json::from_str::<BinanceApiError>(&body)
                    .is_ok_and(|error| error.code == -2013)
            {
                return Ok(None);
            }
            self.maybe_resync_on_invalid_timestamp(&body).await;
            return Err(anyhow!(
                "Failed to resolve order {}: {} - {}",
                client_order_id,
                status,
                body
            ));
        }
        Ok(Some(
            resp.json().await.map_err(reqwest::Error::without_url)?,
        ))
    }

    /// Place an order. If post_only is true, timeInForce is set to GTX (Maker Only).
    /// reduce_only prevents a closing order from opening the opposite position.
    pub(crate) async fn place_order(
        &self,
        order: NewOrderRequest<'_>,
    ) -> std::result::Result<BinanceOrderResponse, ExchangeError> {
        self.reserve(
            0,
            if order.reduce_only {
                RequestClass::ClosingOrder
            } else {
                RequestClass::OpeningOrder
            },
        )
        .map_err(|error| ExchangeError::RequestRejected(error.to_string()))?;
        let tif = if order.post_only { "GTX" } else { "GTC" };

        let reduce_only_param = if order.reduce_only {
            "&reduceOnly=true"
        } else {
            ""
        };
        let url = self.signed_url(
            "/fapi/v1/order",
            &format!(
                "symbol={}&side={}&type=LIMIT&timeInForce={}&price={}&quantity={}&newClientOrderId={}{}",
                order.symbol,
                order.side,
                tif,
                order.price,
                order.quantity,
                order.client_order_id,
                reduce_only_param
            ),
        );

        debug!(
            "Sending Binance order request: side={}, price={}, qty={}, tif={}",
            order.side, order.price, order.quantity, tif
        );
        let resp = self.send(self.client.post(&url)).await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            self.maybe_resync_on_invalid_timestamp(&body).await;
            return Err(order_response_error(status, body));
        }

        let order: BinanceOrderResponse = resp
            .json()
            .await
            .map_err(reqwest::Error::without_url)
            .map_err(|e| ExchangeError::Other(format!("Failed to parse order response: {}", e)))?;
        Ok(order)
    }

    /// Cancel a single order by orderId or origClientOrderId
    pub(crate) async fn cancel_order(
        &self,
        symbol: &str,
        order_id: Option<i64>,
        client_order_id: Option<&str>,
    ) -> Result<()> {
        self.reserve(1, RequestClass::Critical)?;
        let mut params = format!("symbol={}", symbol);
        if let Some(id) = order_id {
            params.push_str(&format!("&orderId={}", id));
        } else if let Some(cid) = client_order_id {
            params.push_str(&format!("&origClientOrderId={}", cid));
        }

        let url = self.signed_url("/fapi/v1/order", &params);

        self.ensure_signed_success(
            self.send(self.client.delete(&url)).await?,
            "Failed to cancel order",
            false,
        )
        .await?;
        Ok(())
    }

    /// Fetch position risk information for symbol
    pub(crate) async fn get_position(&self, symbol: &str) -> Result<Option<BinancePositionRisk>> {
        let positions: Vec<BinancePositionRisk> = self
            .get_signed_json(
                5,
                RequestClass::Critical,
                "/fapi/v2/positionRisk",
                &format!("symbol={}", symbol),
                "Failed to get position risk",
                false,
            )
            .await?;
        one_way_position(positions, symbol)
    }

    /// Fetch position risk for all symbols on the account.
    pub(crate) async fn get_all_positions(&self) -> Result<Vec<BinancePositionRisk>> {
        self.get_signed_json(
            5,
            RequestClass::Normal,
            "/fapi/v2/positionRisk",
            "",
            "Failed to get position risk",
            false,
        )
        .await
    }

    /// Fetch account balance summary
    pub(crate) async fn get_account(&self) -> Result<BinanceAccountInfoResponse> {
        self.get_account_with_class(RequestClass::Normal).await
    }

    pub(crate) async fn get_account_critical(&self) -> Result<BinanceAccountInfoResponse> {
        self.get_account_with_class(RequestClass::Critical).await
    }

    async fn get_account_with_class(
        &self,
        class: RequestClass,
    ) -> Result<BinanceAccountInfoResponse> {
        self.get_signed_json(
            5,
            class,
            "/fapi/v2/account",
            "",
            "Failed to get account info",
            false,
        )
        .await
    }
}

fn one_way_position(
    positions: Vec<BinancePositionRisk>,
    symbol: &str,
) -> Result<Option<BinancePositionRisk>> {
    let mut matching = positions
        .into_iter()
        .filter(|position| position.symbol.eq_ignore_ascii_case(symbol));
    let position = matching.next();
    anyhow::ensure!(
        position.as_ref().is_none_or(|position| position
            .position_side
            .as_deref()
            .is_none_or(|side| side == "BOTH"))
            && matching.next().is_none(),
        ONE_WAY_POSITION_ERROR
    );
    Ok(position)
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;
