use crate::config::ExchangeConfig;
use crate::exchange::model::*;
use crate::exchange::rate_limit::{RateLimiter, RequestClass};
use crate::exchange::signature::sign_query;
use anyhow::{anyhow, Result};
use chrono::Utc;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Client;
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info};

#[derive(Debug, thiserror::Error)]
pub enum ExchangeError {
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
    pub fn execution_status_unknown(&self) -> bool {
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
async fn checked_response(response: reqwest::Response) -> Result<reqwest::Response> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let body = response.text().await?;
    match serde_json::from_str::<BinanceApiError>(&body) {
        Ok(error) => Err(ExchangeError::ApiError {
            code: error.code,
            msg: error.msg,
        }
        .into()),
        Err(_) => Err(ExchangeError::Other(format!("HTTP error {}: {}", status, body)).into()),
    }
}

pub struct NewOrderRequest<'a> {
    pub symbol: &'a str,
    pub side: &'a str,
    pub price: &'a str,
    pub quantity: &'a str,
    pub client_order_id: &'a str,
    pub post_only: bool,
    pub reduce_only: bool,
}

#[derive(serde::Deserialize)]
struct ExecutionFill {
    id: i64,
    #[serde(rename = "orderId")]
    order_id: i64,
    qty: Decimal,
    #[serde(flatten)]
    trade: BinanceUserTrade,
}

struct FillProgress {
    expected_qty: Decimal,
    from_id: Option<i64>,
    restarted: bool,
    fills: std::collections::HashMap<i64, ExecutionFill>,
    quantity: Decimal,
    advances: u64,
}

impl FillProgress {
    fn new(expected_qty: Decimal) -> Self {
        Self {
            expected_qty,
            from_id: None,
            restarted: false,
            fills: Default::default(),
            quantity: Decimal::ZERO,
            advances: 0,
        }
    }
}

const FILL_WAITER_ACTIVE_LEASE: Duration = Duration::from_secs(3 * 60);

struct FillCacheEntry {
    progress: Arc<tokio::sync::Mutex<FillProgress>>,
    last_used: Instant,
    stalled_attempts: u8,
}

struct FillCacheCleanup<'a> {
    cache: &'a std::sync::Mutex<std::collections::HashMap<(String, i64), FillCacheEntry>>,
    key: (String, i64),
    shared: Arc<tokio::sync::Mutex<FillProgress>>,
    progress: tokio::sync::OwnedMutexGuard<FillProgress>,
    initial_advances: u64,
    failed_query: bool,
}

impl Drop for FillCacheCleanup<'_> {
    fn drop(&mut self) {
        // Keep the progress lock until accounting and removal are complete.
        // Acquisition also happens under the cache lock, so a concurrent caller
        // cannot acquire an orphaned entry while it is being removed.
        let mut cache = self.cache.lock().unwrap();
        if let Some(entry) = cache.get_mut(&self.key) {
            if !Arc::ptr_eq(&entry.progress, &self.shared) {
                return;
            }
            if self.progress.from_id.is_none() || self.progress.fills.is_empty() {
                cache.remove(&self.key);
            } else if self.failed_query && self.progress.advances == self.initial_advances {
                entry.stalled_attempts = entry.stalled_attempts.saturating_add(1);
            } else if self.progress.advances != self.initial_advances {
                entry.stalled_attempts = 0;
            }
        }
    }
}

pub struct BinanceFuturesClient {
    base_url: String,
    pub _api_key: String,
    api_secret: String,
    client: Client,
    recv_window: u64,
    time_offset_ms: AtomicI64,
    limiter: Arc<RateLimiter>,
    fill_progress: std::sync::Mutex<std::collections::HashMap<(String, i64), FillCacheEntry>>,
    fill_waiters: std::sync::Mutex<std::collections::VecDeque<((String, i64), Instant)>>,
    fill_retention_seconds: AtomicU64,
    fill_generations: std::sync::Mutex<std::collections::HashMap<String, u64>>,
}

impl BinanceFuturesClient {
    pub(crate) fn note_pnl_reconcile_backlog(&self, pending: usize) {
        // Three records per tick. Each can make an order lookup and three
        // 10-second requests; allow two full passes and scheduling slack.
        // A later decrease in backlog must not shorten an existing lease.
        let batches = u64::try_from(pending).unwrap_or(u64::MAX).saturating_add(2) / 3;
        let seconds = batches.saturating_mul(360).saturating_add(120).max(600);
        self.fill_retention_seconds
            .fetch_max(seconds, Ordering::Relaxed);
    }

    pub(crate) fn market_cache_key(&self) -> &str {
        &self.base_url
    }

    pub(crate) async fn get_all_mark_prices_for_fallback(
        &self,
    ) -> Result<Vec<BinanceMarkPriceWithSymbol>> {
        self.reserve(10, RequestClass::MarketFallback)?;
        let url = format!("{}/fapi/v1/premiumIndex", self.base_url);
        let resp = self.send(self.client.get(&url)).await?.error_for_status()?;
        Ok(resp.json().await?)
    }

    pub fn new(config: &ExchangeConfig) -> Self {
        Self::new_with_limiter(config, Arc::new(RateLimiter::default()))
    }

    pub(crate) fn new_with_limiter(config: &ExchangeConfig, limiter: Arc<RateLimiter>) -> Self {
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
            limiter,
            fill_progress: std::sync::Mutex::new(Default::default()),
            fill_waiters: std::sync::Mutex::new(Default::default()),
            fill_retention_seconds: AtomicU64::new(600),
            fill_generations: std::sync::Mutex::new(Default::default()),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_base_url(config: &ExchangeConfig, base_url: String) -> Self {
        Self::with_base_url_and_limiter(config, base_url, Arc::new(RateLimiter::default()))
    }

    #[cfg(test)]
    pub(crate) fn with_base_url_and_limiter(
        config: &ExchangeConfig,
        base_url: String,
        limiter: Arc<RateLimiter>,
    ) -> Self {
        let mut client = Self::new_with_limiter(config, limiter);
        client.base_url = base_url;
        client
    }

    /// Synchronize local timestamp with Binance Futures server time
    pub async fn sync_server_time(&self) -> Result<i64> {
        self.reserve(1, RequestClass::Normal)?;
        let url = format!("{}/fapi/v1/time", self.base_url);
        let resp = self.send(self.client.get(&url)).await?.error_for_status()?;
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

    pub(crate) fn account_cache_key(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        for value in [
            self.base_url.as_bytes(),
            self._api_key.as_bytes(),
            self.api_secret.as_bytes(),
            &self.recv_window.to_be_bytes(),
        ] {
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value);
        }
        hash.finalize().into()
    }

    fn rate_account_key(&self) -> [u8; 32] {
        Sha256::digest(self._api_key.as_bytes()).into()
    }

    pub(crate) fn limiter(&self) -> Arc<RateLimiter> {
        self.limiter.clone()
    }

    fn reserve(&self, weight: u64, class: RequestClass) -> Result<()> {
        self.limiter
            .reserve(&self.base_url, self.rate_account_key(), weight, class)
            .map_err(|error| {
                tracing::warn!("{}", error);
                anyhow!(error)
            })
    }

    async fn send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, reqwest::Error> {
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
                return Err(error);
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
        let response = response?;
        self.limiter.observe(
            &self.base_url,
            self.rate_account_key(),
            response.status(),
            response.headers(),
        );
        Ok(response)
    }

    /// Fetch exchange info (symbol filters)
    pub async fn get_exchange_info(&self, symbol: Option<&str>) -> Result<BinanceExchangeInfo> {
        self.reserve(1, RequestClass::Normal)?;
        let url = if let Some(sym) = symbol {
            format!("{}/fapi/v1/exchangeInfo?symbol={}", self.base_url, sym)
        } else {
            format!("{}/fapi/v1/exchangeInfo", self.base_url)
        };

        let resp = checked_response(self.send(self.client.get(&url)).await?).await?;
        let info: BinanceExchangeInfo = resp.json().await?;
        self.limiter
            .update_exchange_limits(&self.base_url, &info.rate_limits);
        Ok(info)
    }

    /// Fetch the latest traded price.
    pub async fn get_ticker_price(&self, symbol: &str) -> Result<Decimal> {
        self.get_ticker_price_with_class(symbol, RequestClass::Normal)
            .await
    }

    pub(crate) async fn get_ticker_price_for_fallback(&self, symbol: &str) -> Result<Decimal> {
        self.get_ticker_price_with_class(symbol, RequestClass::MarketFallback)
            .await
    }

    async fn get_ticker_price_with_class(
        &self,
        symbol: &str,
        class: RequestClass,
    ) -> Result<Decimal> {
        self.reserve(1, class)?;
        let url = format!("{}/fapi/v1/ticker/price?symbol={}", self.base_url, symbol);
        let resp = self.send(self.client.get(&url)).await?.error_for_status()?;
        let ticker: BinanceTickerPrice = resp.json().await?;
        Ok(ticker.price)
    }

    /// Fetch the futures mark price, which differs from the latest traded price.
    pub async fn get_mark_price(&self, symbol: &str) -> Result<Decimal> {
        self.get_mark_price_with_class(symbol, RequestClass::Normal)
            .await
    }

    pub(crate) async fn get_mark_price_for_fallback(&self, symbol: &str) -> Result<Decimal> {
        self.get_mark_price_with_class(symbol, RequestClass::MarketFallback)
            .await
    }

    async fn get_mark_price_with_class(
        &self,
        symbol: &str,
        class: RequestClass,
    ) -> Result<Decimal> {
        self.reserve(1, class)?;
        let url = format!("{}/fapi/v1/premiumIndex?symbol={}", self.base_url, symbol);
        let resp = self.send(self.client.get(&url)).await?.error_for_status()?;
        let mark: BinanceMarkPrice = resp.json().await?;
        Ok(mark.mark_price)
    }

    /// Fetch 24-hour ticker statistics
    pub async fn get_24hr_ticker(&self, symbol: &str) -> Result<Binance24hrTicker> {
        self.get_24hr_ticker_with_class(symbol, RequestClass::Normal)
            .await
    }

    pub(crate) async fn get_24hr_ticker_for_fallback(
        &self,
        symbol: &str,
    ) -> Result<Binance24hrTicker> {
        self.get_24hr_ticker_with_class(symbol, RequestClass::MarketFallback)
            .await
    }

    async fn get_24hr_ticker_with_class(
        &self,
        symbol: &str,
        class: RequestClass,
    ) -> Result<Binance24hrTicker> {
        self.reserve(1, class)?;
        let url = format!("{}/fapi/v1/ticker/24hr?symbol={}", self.base_url, symbol);
        let resp = self.send(self.client.get(&url)).await?.error_for_status()?;
        let ticker: Binance24hrTicker = resp.json().await?;
        Ok(ticker)
    }

    /// Fetch open orders for symbol
    pub async fn get_open_orders(&self, symbol: &str) -> Result<Vec<BinanceOrderResponse>> {
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
        let ts = self.current_timestamp();
        let query = format!(
            "symbol={}&recvWindow={}&timestamp={}",
            symbol, self.recv_window, ts
        );
        let signed_query = self.sign_params(&query);
        let url = format!("{}/fapi/v1/openOrders?{}", self.base_url, signed_query);

        let resp = checked_response(self.send(self.client.get(&url)).await?).await?;

        let orders: Vec<BinanceOrderResponse> = resp.json().await?;
        Ok(orders)
    }

    /// Fetch an order's final status before treating it as a fill.
    pub async fn get_order(&self, symbol: &str, order_id: i64) -> Result<BinanceOrderResponse> {
        self.reserve(1, RequestClass::Critical)?;
        let ts = self.current_timestamp();
        let query = format!(
            "symbol={}&orderId={}&recvWindow={}&timestamp={}",
            symbol, order_id, self.recv_window, ts
        );
        let signed_query = self.sign_params(&query);
        let url = format!("{}/fapi/v1/order?{}", self.base_url, signed_query);
        let resp = self.send(self.client.get(&url)).await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to get order: {} - {}", status, body));
        }
        Ok(resp.json().await?)
    }

    /// Resolve a persisted client order ID after a restart.
    pub async fn get_order_by_client_id(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<BinanceOrderResponse> {
        self.lookup_order_by_client_id(symbol, client_order_id)
            .await?
            .ok_or_else(|| anyhow!("Order {} was not found on Binance", client_order_id))
    }

    /// Only Binance's explicit unknown-order code means it is safe to submit this ID.
    pub async fn lookup_order_by_client_id(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<Option<BinanceOrderResponse>> {
        self.reserve(1, RequestClass::Critical)?;
        let ts = self.current_timestamp();
        let query = format!(
            "symbol={}&origClientOrderId={}&recvWindow={}&timestamp={}",
            symbol, client_order_id, self.recv_window, ts
        );
        let url = format!(
            "{}/fapi/v1/order?{}",
            self.base_url,
            self.sign_params(&query)
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
            return Err(anyhow!(
                "Failed to resolve order {}: {} - {}",
                client_order_id,
                status,
                body
            ));
        }
        Ok(Some(resp.json().await?))
    }

    /// Binance execution records contain the exchange-calculated realized PnL and fee.
    pub async fn get_user_trades(
        &self,
        symbol: &str,
        order_id: i64,
        expected_qty: Decimal,
        clear_revision: u64,
    ) -> Result<Vec<BinanceUserTrade>> {
        if expected_qty <= Decimal::ZERO {
            return Err(anyhow!("Invalid executed quantity for order {}", order_id));
        }
        let key = (symbol.to_owned(), order_id);
        let (shared, mut progress) = {
            let mut cache = self.fill_progress.lock().unwrap();
            {
                let mut generations = self.fill_generations.lock().unwrap();
                if !generations.contains_key(symbol) && generations.len() >= 1024 {
                    return Err(anyhow!("Trade reconciliation symbol limit reached"));
                }
                let generation = generations
                    .entry(symbol.to_owned())
                    .or_insert(clear_revision);
                if clear_revision < *generation {
                    return Err(anyhow!("Trade history was cleared during reconciliation"));
                }
                if clear_revision > *generation {
                    *generation = clear_revision;
                    cache.retain(|(cached_symbol, _), _| cached_symbol != symbol);
                    self.fill_waiters
                        .lock()
                        .unwrap()
                        .retain(|((waiting_symbol, _), _)| waiting_symbol != symbol);
                }
            }
            if !cache.contains_key(&key) {
                let mut waiters = self.fill_waiters.lock().unwrap();
                let now = Instant::now();
                waiters.retain(|(_, last_seen)| {
                    now.duration_since(*last_seen)
                        < Duration::from_secs(self.fill_retention_seconds.load(Ordering::Relaxed))
                });
                if let Some((_, last_seen)) =
                    waiters.iter_mut().find(|(waiting, _)| waiting == &key)
                {
                    *last_seen = now;
                } else if cache.len() >= 8 || !waiters.is_empty() {
                    if waiters.len() >= 1024 {
                        if let Some(index) = waiters.iter().position(|(_, last_seen)| {
                            now.duration_since(*last_seen) >= FILL_WAITER_ACTIVE_LEASE
                        }) {
                            waiters.remove(index);
                        } else {
                            return Err(anyhow!("Trade reconciliation waiting queue is full"));
                        }
                    }
                    waiters.push_back((key.clone(), now));
                }
                // Keep FIFO positions for slow backlog scans, but let a recently
                // polled waiter pass an inactive head. Three minutes tolerates
                // normal one-minute reconciliation ticks and a missed tick.
                let active = waiters.iter().position(|(_, last_seen)| {
                    now.duration_since(*last_seen) < FILL_WAITER_ACTIVE_LEASE
                });
                let turn = active.is_none_or(|index| waiters[index].0 == key);
                if turn && active.is_some() && cache.len() >= 8 {
                    // Preserve healthy cursors while releasing repeated failures
                    // and cursors abandoned before the fill lookup can run.
                    let retention = Duration::from_secs(
                        self.fill_retention_seconds
                            .load(Ordering::Relaxed)
                            .max(2 * 60 * 60),
                    );
                    let victim = cache
                        .iter()
                        .filter(|(_, entry)| {
                            (entry.stalled_attempts >= 3 || entry.last_used.elapsed() >= retention)
                                && Arc::strong_count(&entry.progress) == 1
                        })
                        .min_by_key(|(_, entry)| entry.last_used)
                        .map(|(key, _)| key.clone());
                    if let Some(victim) = victim {
                        cache.remove(&victim);
                    }
                }
                if cache.len() >= 8 || !turn {
                    return Err(anyhow!(
                        "Trade reconciliation for order {} is waiting for a progress slot",
                        order_id
                    ));
                }
                if let Some(index) = active {
                    waiters.remove(index);
                }
            }
            let entry = cache.entry(key.clone()).or_insert_with(|| FillCacheEntry {
                progress: Arc::new(tokio::sync::Mutex::new(FillProgress::new(expected_qty))),
                last_used: Instant::now(),
                stalled_attempts: 0,
            });
            let shared = Arc::clone(&entry.progress);
            let progress = Arc::clone(&shared).try_lock_owned().map_err(|_| {
                anyhow!(
                    "Trade reconciliation already in progress for order {}",
                    order_id
                )
            })?;
            (shared, progress)
        };
        if progress.expected_qty != expected_qty {
            *progress = FillProgress::new(expected_qty);
        }
        let initial_advances = progress.advances;
        let mut cleanup = FillCacheCleanup {
            cache: &self.fill_progress,
            key: key.clone(),
            shared: Arc::clone(&shared),
            progress,
            initial_advances,
            failed_query: false,
        };
        let progress = &mut cleanup.progress;
        // Limit work on the live fill path. The reconciliation timer resumes the
        // saved cursor, including after a local budget or HTTP error.
        for _ in 0..3 {
            self.reserve(5, RequestClass::PnlReconciliation)?;
            let cursor = progress
                .from_id
                .map(|id| format!("&fromId={}", id))
                .unwrap_or_default();
            let query = format!(
                "symbol={}&orderId={}&limit=1000{}&recvWindow={}&timestamp={}",
                symbol,
                order_id,
                cursor,
                self.recv_window,
                self.current_timestamp()
            );
            let url = format!(
                "{}/fapi/v1/userTrades?{}",
                self.base_url,
                self.sign_params(&query)
            );
            let resp = match self.send(self.client.get(&url)).await {
                Ok(resp) => resp,
                Err(error) => {
                    cleanup.failed_query = true;
                    return Err(error.into());
                }
            };
            if !resp.status().is_success() {
                cleanup.failed_query = true;
                return Err(anyhow!(
                    "Failed to fetch trades for order {}: {}",
                    order_id,
                    resp.status()
                ));
            }
            let page: Vec<ExecutionFill> = match resp.json().await {
                Ok(page) => page,
                Err(error) => {
                    cleanup.failed_query = true;
                    return Err(error.into());
                }
            };
            if self.fill_generations.lock().unwrap().get(symbol).copied() != Some(clear_revision) {
                return Err(anyhow!("Trade history was cleared during reconciliation"));
            }
            // From here until the next await, validation either fails or advances.
            cleanup.failed_query = true;
            let page_len = page.len();
            if page_len > 1000 {
                return Err(anyhow!("Unexpected trade page size for order {}", order_id));
            }
            let mut max_id = None;
            let mut added = std::collections::HashMap::<i64, ExecutionFill>::new();
            let mut quantity = progress.quantity;
            for fill in page {
                if fill.order_id != order_id || fill.id < 0 || fill.qty <= Decimal::ZERO {
                    return Err(anyhow!("Invalid trade returned for order {}", order_id));
                }
                max_id = Some(max_id.map_or(fill.id, |id: i64| id.max(fill.id)));
                if let Some(existing) = progress.fills.get(&fill.id).or_else(|| added.get(&fill.id))
                {
                    if existing.qty != fill.qty
                        || existing.trade.realized_pnl != fill.trade.realized_pnl
                        || existing.trade.commission != fill.trade.commission
                        || existing.trade.commission_asset != fill.trade.commission_asset
                        || existing.trade.maker != fill.trade.maker
                    {
                        return Err(anyhow!(
                            "Conflicting trade {} for order {}",
                            fill.id,
                            order_id
                        ));
                    }
                } else {
                    quantity = quantity
                        .checked_add(fill.qty)
                        .ok_or_else(|| anyhow!("Trade quantity overflow for order {}", order_id))?;
                    added.insert(fill.id, fill);
                }
            }
            if quantity > expected_qty {
                return Err(anyhow!(
                    "Trade quantity exceeds executed quantity for order {}",
                    order_id
                ));
            }
            if progress.fills.len() + added.len() > 1_000_000 {
                **progress = FillProgress::new(expected_qty);
                return Err(anyhow!(
                    "Order {} exceeds the one million fill reconciliation limit",
                    order_id
                ));
            }
            if quantity == expected_qty {
                progress.fills.extend(added);
                let result = std::mem::take(&mut progress.fills)
                    .into_values()
                    .map(|fill| fill.trade)
                    .collect();
                let mut cache = self.fill_progress.lock().unwrap();
                if cache
                    .get(&key)
                    .is_some_and(|entry| Arc::ptr_eq(&entry.progress, &shared))
                {
                    cache.remove(&key);
                }
                return Ok(result);
            }
            let next = if !progress.restarted {
                Some(0)
            } else if page_len < 1000 {
                **progress = FillProgress::new(expected_qty);
                return Err(anyhow!(
                    "Incomplete trades for order {}: {} of {}",
                    order_id,
                    quantity,
                    expected_qty
                ));
            } else {
                let next = max_id
                    .and_then(|id| id.checked_add(1))
                    .ok_or_else(|| anyhow!("Invalid trade cursor for order {}", order_id))?;
                if progress.from_id.is_some_and(|cursor| next <= cursor) {
                    return Err(anyhow!(
                        "Trade cursor did not advance for order {}",
                        order_id
                    ));
                }
                Some(next)
            };
            progress.fills.extend(added);
            progress.quantity = quantity;
            progress.from_id = next;
            progress.restarted = true;
            progress.advances = progress.advances.saturating_add(1);
            cleanup.failed_query = false;
            if let Some(entry) = self.fill_progress.lock().unwrap().get_mut(&key) {
                if Arc::ptr_eq(&entry.progress, &shared) {
                    entry.last_used = Instant::now();
                    entry.stalled_attempts = 0;
                }
            }
        }
        Err(anyhow!(
            "Trade reconciliation for order {} will continue later",
            order_id
        ))
    }

    /// Place an order. If post_only is true, timeInForce is set to GTX (Maker Only).
    /// reduce_only prevents a closing order from opening the opposite position.
    pub async fn place_order(
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
        let ts = self.current_timestamp();
        let tif = if order.post_only { "GTX" } else { "GTC" };

        let reduce_only_param = if order.reduce_only {
            "&reduceOnly=true"
        } else {
            ""
        };
        let query = format!(
            "symbol={}&side={}&type=LIMIT&timeInForce={}&price={}&quantity={}&newClientOrderId={}{}&recvWindow={}&timestamp={}",
            order.symbol, order.side, tif, order.price, order.quantity, order.client_order_id, reduce_only_param, self.recv_window, ts
        );
        let signed_query = self.sign_params(&query);
        let url = format!("{}/fapi/v1/order?{}", self.base_url, signed_query);

        debug!(
            "Sending Binance order request: side={}, price={}, qty={}, tif={}",
            order.side, order.price, order.quantity, tif
        );
        let resp = self.send(self.client.post(&url)).await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(order_response_error(status, body));
        }

        let order: BinanceOrderResponse = resp
            .json()
            .await
            .map_err(|e| ExchangeError::Other(format!("Failed to parse order response: {}", e)))?;
        Ok(order)
    }

    /// Cancel a single order by orderId or origClientOrderId
    pub async fn cancel_order(
        &self,
        symbol: &str,
        order_id: Option<i64>,
        client_order_id: Option<&str>,
    ) -> Result<()> {
        self.reserve(1, RequestClass::Critical)?;
        let ts = self.current_timestamp();
        let mut query = format!(
            "symbol={}&recvWindow={}&timestamp={}",
            symbol, self.recv_window, ts
        );
        if let Some(id) = order_id {
            query.push_str(&format!("&orderId={}", id));
        } else if let Some(cid) = client_order_id {
            query.push_str(&format!("&origClientOrderId={}", cid));
        }

        let signed_query = self.sign_params(&query);
        let url = format!("{}/fapi/v1/order?{}", self.base_url, signed_query);

        let resp = self.send(self.client.delete(&url)).await?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to cancel order: {}", body));
        }
        Ok(())
    }

    /// Fetch position risk information for symbol
    pub async fn get_position(&self, symbol: &str) -> Result<Option<BinancePositionRisk>> {
        self.reserve(5, RequestClass::Critical)?;
        let ts = self.current_timestamp();
        let query = format!(
            "symbol={}&recvWindow={}&timestamp={}",
            symbol, self.recv_window, ts
        );
        let signed_query = self.sign_params(&query);
        let url = format!("{}/fapi/v2/positionRisk?{}", self.base_url, signed_query);

        let resp = self.send(self.client.get(&url)).await?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to get position risk: {}", body));
        }

        let positions: Vec<BinancePositionRisk> = resp.json().await?;
        one_way_position(positions, symbol)
    }

    /// Fetch position risk for all symbols on the account.
    pub(crate) async fn get_all_positions(&self) -> Result<Vec<BinancePositionRisk>> {
        self.reserve(5, RequestClass::Normal)?;
        let ts = self.current_timestamp();
        let query = format!("recvWindow={}&timestamp={}", self.recv_window, ts);
        let signed_query = self.sign_params(&query);
        let url = format!("{}/fapi/v2/positionRisk?{}", self.base_url, signed_query);
        let resp = self.send(self.client.get(&url)).await?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to get position risk: {}", body));
        }
        Ok(resp.json().await?)
    }

    /// Fetch account balance summary
    pub async fn get_account(&self) -> Result<BinanceAccountInfoResponse> {
        self.reserve(5, RequestClass::Normal)?;
        let ts = self.current_timestamp();
        let query = format!("recvWindow={}&timestamp={}", self.recv_window, ts);
        let signed_query = self.sign_params(&query);
        let url = format!("{}/fapi/v2/account?{}", self.base_url, signed_query);

        let resp = self.send(self.client.get(&url)).await?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to get account info: {}", body));
        }

        let acc: BinanceAccountInfoResponse = resp.json().await?;
        Ok(acc)
    }
}

fn one_way_position(
    positions: Vec<BinancePositionRisk>,
    symbol: &str,
) -> Result<Option<BinancePositionRisk>> {
    Ok(one_way_position_ref(&positions, symbol)?.cloned())
}

pub(crate) fn one_way_position_ref<'a>(
    positions: &'a [BinancePositionRisk],
    symbol: &str,
) -> Result<Option<&'a BinancePositionRisk>> {
    let mut positions = positions
        .iter()
        .filter(|position| position.symbol.eq_ignore_ascii_case(symbol));
    let position = positions.next();
    anyhow::ensure!(
        position.as_ref().is_none_or(|position| position
            .position_side
            .as_deref()
            .is_none_or(|side| side == "BOTH"))
            && positions.next().is_none(),
        "策略仅支持币安单向持仓模式，请先将账户设置为单向持仓"
    );
    Ok(position)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn backlog_retention_survives_long_rounds_and_shrinking_count() {
        let client = BinanceFuturesClient::with_base_url(
            &crate::config::AppConfig::default().exchange,
            "http://127.0.0.1:1".into(),
        );
        client.note_pnl_reconcile_backlog(33);
        client.note_pnl_reconcile_backlog(1);
        client.fill_waiters.lock().unwrap().push_back((
            ("BTCUSDT".into(), 1),
            Instant::now() - Duration::from_secs(11 * 60),
        ));
        let err = client
            .get_user_trades("BTCUSDT", 2, Decimal::ONE, 0)
            .await
            .unwrap_err();
        assert!(!err.to_string().contains("waiting"));
        assert_eq!(client.fill_waiters.lock().unwrap().front().unwrap().0 .1, 1);

        client.note_pnl_reconcile_backlog(2000);
        {
            let mut cache = client.fill_progress.lock().unwrap();
            for id in 10..18 {
                cache.insert(
                    ("ETHUSDT".into(), id),
                    FillCacheEntry {
                        progress: Arc::new(tokio::sync::Mutex::new(FillProgress::new(
                            Decimal::ONE,
                        ))),
                        last_used: Instant::now() - Duration::from_secs(3 * 60 * 60),
                        stalled_attempts: 0,
                    },
                );
            }
        }
        assert!(client
            .get_user_trades("ETHUSDT", 18, Decimal::ONE, 0)
            .await
            .is_err());
        assert!(client
            .fill_progress
            .lock()
            .unwrap()
            .contains_key(&("ETHUSDT".into(), 10)));
    }

    #[tokio::test]
    async fn inactive_waiter_is_skipped_but_keeps_its_queue_position() {
        use axum::{extract::Query, routing::get, Json, Router};
        use serde_json::json;
        use std::collections::HashMap;

        let app = Router::new().route("/fapi/v1/userTrades", get(|Query(query): Query<HashMap<String, String>>| async move {
            let order_id: i64 = query["orderId"].parse().unwrap();
            Json(json!([{ "id": order_id, "orderId": order_id, "qty": "1", "realizedPnl": "1", "commission": "0.1", "commissionAsset": "USDT", "maker": true }]))
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client =
            BinanceFuturesClient::with_base_url(&crate::config::AppConfig::default().exchange, url);
        client.note_pnl_reconcile_backlog(33);
        {
            let mut waiters = client.fill_waiters.lock().unwrap();
            waiters.push_back((
                ("BTCUSDT".into(), 1),
                Instant::now() - Duration::from_secs(181),
            ));
            waiters.push_back((
                ("BTCUSDT".into(), 2),
                Instant::now() - Duration::from_secs(60),
            ));
        }
        assert!(client
            .get_user_trades("BTCUSDT", 3, Decimal::ONE, 0)
            .await
            .unwrap_err()
            .to_string()
            .contains("waiting"));
        assert_eq!(
            client
                .get_user_trades("BTCUSDT", 2, Decimal::ONE, 0)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            client
                .get_user_trades("BTCUSDT", 1, Decimal::ONE, 0)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            client
                .get_user_trades("BTCUSDT", 3, Decimal::ONE, 0)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(client.fill_waiters.lock().unwrap().is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn full_inactive_waiting_queue_admits_a_new_order() {
        let client = BinanceFuturesClient::with_base_url(
            &crate::config::AppConfig::default().exchange,
            "http://127.0.0.1:1".into(),
        );
        {
            let mut waiters = client.fill_waiters.lock().unwrap();
            for id in 1..=1024 {
                waiters.push_back((
                    ("BTCUSDT".into(), id),
                    Instant::now() - Duration::from_secs(181),
                ));
            }
        }
        let error = client
            .get_user_trades("BTCUSDT", 2000, Decimal::ONE, 0)
            .await
            .unwrap_err()
            .to_string();
        assert!(!error.contains("waiting"));
        assert!(!error.contains("queue is full"));
        let waiters = client.fill_waiters.lock().unwrap();
        assert_eq!(waiters.len(), 1023);
        assert_eq!(waiters.front().unwrap().0 .1, 2);
    }

    #[tokio::test]
    async fn failed_progress_does_not_starve_a_new_order() {
        use axum::{
            extract::Query, http::StatusCode, response::IntoResponse, routing::get, Json, Router,
        };
        use serde_json::json;
        use std::collections::HashMap;

        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let app = Router::new().route("/fapi/v1/userTrades", get({
            let requests = Arc::clone(&requests);
            move |Query(query): Query<HashMap<String, String>>| {
                let requests = Arc::clone(&requests);
                async move {
                    let order_id: i64 = query["orderId"].parse().unwrap();
                    requests.lock().unwrap().push(query);
                    if order_id == 99 {
                        Json(json!([{ "id": 10, "orderId": order_id, "qty": "1", "realizedPnl": "1", "commission": "0.1", "commissionAsset": "USDT", "maker": true }])).into_response()
                    } else {
                        StatusCode::INTERNAL_SERVER_ERROR.into_response()
                    }
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut client =
            BinanceFuturesClient::with_base_url(&crate::config::AppConfig::default().exchange, url);
        {
            let mut cache = client.fill_progress.lock().unwrap();
            for order_id in 1..=8 {
                let fill: ExecutionFill = serde_json::from_value(json!({ "id": 1, "orderId": order_id, "qty": "1", "realizedPnl": "1", "commission": "0.1", "commissionAsset": "USDT", "maker": true })).unwrap();
                let mut progress = FillProgress::new(Decimal::from(2));
                progress.fills.insert(1, fill);
                progress.quantity = Decimal::ONE;
                progress.from_id = Some(2);
                progress.restarted = true;
                cache.insert(
                    ("BTCUSDT".into(), order_id),
                    FillCacheEntry {
                        progress: Arc::new(tokio::sync::Mutex::new(progress)),
                        last_used: Instant::now() - Duration::from_secs((9 - order_id) as u64),
                        stalled_attempts: 0,
                    },
                );
            }
        }
        for attempt in 0..3 {
            for order_id in 1..=8 {
                // Each round represents a later rate-limit window.
                client.limiter = Arc::new(RateLimiter::default());
                let error = client
                    .get_user_trades("BTCUSDT", order_id, Decimal::from(2), 0)
                    .await
                    .unwrap_err();
                assert!(error.to_string().contains("500"));
            }
            if attempt < 2 {
                let error = client
                    .get_user_trades("BTCUSDT", 99, Decimal::ONE, 0)
                    .await
                    .unwrap_err();
                assert!(error.to_string().contains("waiting"));
            }
        }
        client.limiter = Arc::new(RateLimiter::default());
        assert_eq!(
            client
                .get_user_trades("BTCUSDT", 99, Decimal::ONE, 0)
                .await
                .unwrap()
                .len(),
            1
        );
        {
            let cache = client.fill_progress.lock().unwrap();
            assert!(!cache.contains_key(&("BTCUSDT".into(), 1)));
            assert!((2..=8).all(|id| cache.contains_key(&("BTCUSDT".into(), id))));
        }
        // The evicted order starts over, rather than combining incomplete history.
        assert!(client
            .get_user_trades("BTCUSDT", 1, Decimal::from(2), 0)
            .await
            .unwrap_err()
            .to_string()
            .contains("500"));
        let requests = requests.lock().unwrap();
        assert!(requests[..24]
            .iter()
            .all(|query| query.get("fromId").map(String::as_str) == Some("2")));
        assert_eq!(requests[24]["orderId"], "99");
        assert_eq!(requests[25]["orderId"], "1");
        assert!(!requests[25].contains_key("fromId"));
        server.abort();
    }

    #[tokio::test]
    async fn abandoned_cursor_is_reclaimed_after_backlog_retention() {
        use axum::{routing::get, Json, Router};
        use serde_json::json;

        let app = Router::new().route("/fapi/v1/userTrades", get(|| async {
            Json(json!([{ "id": 9, "orderId": 99, "qty": "1", "realizedPnl": "1", "commission": "0.1", "commissionAsset": "USDT", "maker": true }]))
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client =
            BinanceFuturesClient::with_base_url(&crate::config::AppConfig::default().exchange, url);
        client
            .fill_retention_seconds
            .store(3 * 60 * 60, Ordering::Relaxed);
        {
            let mut cache = client.fill_progress.lock().unwrap();
            for order_id in 1..=8 {
                let fill: ExecutionFill = serde_json::from_value(json!({
                    "id": 1, "orderId": order_id, "qty": "1", "realizedPnl": "1",
                    "commission": "0.1", "commissionAsset": "USDT", "maker": true
                }))
                .unwrap();
                let mut progress = FillProgress::new(Decimal::from(2));
                progress.fills.insert(1, fill);
                progress.quantity = Decimal::ONE;
                progress.from_id = Some(2);
                progress.restarted = true;
                cache.insert(
                    ("BTCUSDT".into(), order_id),
                    FillCacheEntry {
                        progress: Arc::new(tokio::sync::Mutex::new(progress)),
                        last_used: if order_id == 1 {
                            Instant::now() - Duration::from_secs(2 * 60 * 60 + 30 * 60)
                        } else {
                            Instant::now()
                        },
                        stalled_attempts: 0,
                    },
                );
            }
        }
        assert!(client
            .get_user_trades("BTCUSDT", 99, Decimal::ONE, 0)
            .await
            .unwrap_err()
            .to_string()
            .contains("waiting"));
        client
            .fill_progress
            .lock()
            .unwrap()
            .get_mut(&("BTCUSDT".into(), 1))
            .unwrap()
            .last_used = Instant::now() - Duration::from_secs(3 * 60 * 60 + 1);
        assert_eq!(
            client
                .get_user_trades("BTCUSDT", 99, Decimal::ONE, 0)
                .await
                .unwrap()
                .len(),
            1
        );
        let cache = client.fill_progress.lock().unwrap();
        assert!(!cache.contains_key(&("BTCUSDT".into(), 1)));
        assert!((2..=8).all(|id| cache.contains_key(&("BTCUSDT".into(), id))));
        server.abort();
    }

    #[tokio::test]
    async fn nine_healthy_multi_page_orders_finish_without_eviction() {
        use axum::{extract::Query, routing::get, Json, Router};
        use serde_json::json;
        use std::collections::HashMap;

        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let app = Router::new().route(
            "/fapi/v1/userTrades",
            get({
                let requests = Arc::clone(&requests);
                move |Query(query): Query<HashMap<String, String>>| {
                    let requests = Arc::clone(&requests);
                    async move {
                        let order_id: i64 = query["orderId"].parse().unwrap();
                        let start = match query.get("fromId").map(String::as_str) {
                            None => 3001,
                            Some("0") => 1,
                            Some("1001") => 1001,
                            Some("2001") => 2001,
                            other => panic!("unexpected cursor: {other:?}"),
                        };
                        requests.lock().unwrap().push((order_id, start));
                        Json((start..start + 1000).map(|id| json!({
                        "id": id, "orderId": order_id, "qty": "1", "realizedPnl": "1",
                        "commission": "0.1", "commissionAsset": "USDT", "maker": true
                    })).collect::<Vec<_>>())
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut client =
            BinanceFuturesClient::with_base_url(&crate::config::AppConfig::default().exchange, url);
        for order_id in 1..=8 {
            client.limiter = Arc::new(RateLimiter::default());
            assert!(client
                .get_user_trades("BTCUSDT", order_id, Decimal::from(4000), 0)
                .await
                .unwrap_err()
                .to_string()
                .contains("continue later"));
        }
        assert!(client
            .get_user_trades("BTCUSDT", 9, Decimal::from(4000), 0)
            .await
            .unwrap_err()
            .to_string()
            .contains("waiting"));
        for order_id in 1..=8 {
            client.limiter = Arc::new(RateLimiter::default());
            assert_eq!(
                client
                    .get_user_trades("BTCUSDT", order_id, Decimal::from(4000), 0)
                    .await
                    .unwrap()
                    .len(),
                4000
            );
        }
        client.limiter = Arc::new(RateLimiter::default());
        assert!(client
            .get_user_trades("BTCUSDT", 9, Decimal::from(4000), 0)
            .await
            .unwrap_err()
            .to_string()
            .contains("continue later"));
        client.limiter = Arc::new(RateLimiter::default());
        assert_eq!(
            client
                .get_user_trades("BTCUSDT", 9, Decimal::from(4000), 0)
                .await
                .unwrap()
                .len(),
            4000
        );
        let requests = requests.lock().unwrap();
        for order_id in 1..=9 {
            assert_eq!(
                requests
                    .iter()
                    .filter(|(id, _)| *id == order_id)
                    .map(|(_, start)| *start)
                    .collect::<Vec<_>>(),
                vec![3001, 1, 1001, 2001]
            );
        }
        server.abort();
    }

    #[tokio::test]
    async fn local_budget_and_cancelled_requests_do_not_mark_progress_failed() {
        use axum::{http::StatusCode, routing::get, Router};
        use serde_json::json;

        fn populate(client: &BinanceFuturesClient) {
            let mut cache = client.fill_progress.lock().unwrap();
            for order_id in 1..=8 {
                let fill: ExecutionFill = serde_json::from_value(json!({
                    "id": 1, "orderId": order_id, "qty": "1", "realizedPnl": "1",
                    "commission": "0.1", "commissionAsset": "USDT", "maker": true
                }))
                .unwrap();
                let mut progress = FillProgress::new(Decimal::from(2));
                progress.fills.insert(1, fill);
                progress.quantity = Decimal::ONE;
                progress.from_id = Some(2);
                progress.restarted = true;
                cache.insert(
                    ("BTCUSDT".into(), order_id),
                    FillCacheEntry {
                        progress: Arc::new(tokio::sync::Mutex::new(progress)),
                        last_used: Instant::now(),
                        stalled_attempts: 0,
                    },
                );
            }
        }

        let limited = BinanceFuturesClient::with_base_url(
            &crate::config::AppConfig::default().exchange,
            "http://127.0.0.1:1".into(),
        );
        populate(&limited);
        for _ in 0..12 {
            limited.reserve(5, RequestClass::PnlReconciliation).unwrap();
        }
        for _ in 0..3 {
            assert!(limited
                .get_user_trades("BTCUSDT", 1, Decimal::from(2), 0)
                .await
                .unwrap_err()
                .to_string()
                .contains("budget"));
        }
        assert_eq!(
            limited.fill_progress.lock().unwrap()[&("BTCUSDT".into(), 1)].stalled_attempts,
            0
        );
        assert!(limited
            .get_user_trades("BTCUSDT", 9, Decimal::ONE, 0)
            .await
            .unwrap_err()
            .to_string()
            .contains("waiting"));

        let (arrived, mut received) = tokio::sync::mpsc::unbounded_channel();
        let app = Router::new().route(
            "/fapi/v1/userTrades",
            get(move || {
                let arrived = arrived.clone();
                async move {
                    arrived.send(()).unwrap();
                    std::future::pending::<StatusCode>().await
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = Arc::new(BinanceFuturesClient::with_base_url(
            &crate::config::AppConfig::default().exchange,
            url,
        ));
        populate(&client);
        for _ in 0..3 {
            let client = Arc::clone(&client);
            let task = tokio::spawn(async move {
                client
                    .get_user_trades("BTCUSDT", 1, Decimal::from(2), 0)
                    .await
            });
            tokio::time::timeout(Duration::from_secs(5), received.recv())
                .await
                .unwrap()
                .unwrap();
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        }
        assert_eq!(
            client.fill_progress.lock().unwrap()[&("BTCUSDT".into(), 1)].stalled_attempts,
            0
        );
        assert!(client
            .get_user_trades("BTCUSDT", 9, Decimal::ONE, 0)
            .await
            .unwrap_err()
            .to_string()
            .contains("waiting"));
        server.abort();
    }

    #[tokio::test]
    async fn busy_caller_cannot_interrupt_cleanup_or_failure_accounting() {
        use serde_json::json;

        let client = BinanceFuturesClient::with_base_url(
            &crate::config::AppConfig::default().exchange,
            "http://127.0.0.1:1".into(),
        );
        let key = ("BTCUSDT".to_owned(), 1);
        let shared = Arc::new(tokio::sync::Mutex::new(FillProgress::new(Decimal::from(2))));
        client.fill_progress.lock().unwrap().insert(
            key.clone(),
            FillCacheEntry {
                progress: Arc::clone(&shared),
                last_used: Instant::now(),
                stalled_attempts: 0,
            },
        );
        let cleanup = FillCacheCleanup {
            cache: &client.fill_progress,
            key: key.clone(),
            shared: Arc::clone(&shared),
            progress: Arc::clone(&shared).try_lock_owned().unwrap(),
            initial_advances: 0,
            failed_query: false,
        };
        assert!(client
            .get_user_trades("BTCUSDT", 1, Decimal::from(2), 0)
            .await
            .unwrap_err()
            .to_string()
            .contains("already in progress"));
        drop(cleanup);
        assert!(!client.fill_progress.lock().unwrap().contains_key(&key));

        let mut progress = FillProgress::new(Decimal::from(2));
        let fill: ExecutionFill = serde_json::from_value(json!({
            "id": 1, "orderId": 1, "qty": "1", "realizedPnl": "1",
            "commission": "0.1", "commissionAsset": "USDT", "maker": true
        }))
        .unwrap();
        progress.fills.insert(1, fill);
        progress.quantity = Decimal::ONE;
        progress.from_id = Some(2);
        progress.restarted = true;
        let shared = Arc::new(tokio::sync::Mutex::new(progress));
        client.fill_progress.lock().unwrap().insert(
            key.clone(),
            FillCacheEntry {
                progress: Arc::clone(&shared),
                last_used: Instant::now(),
                stalled_attempts: 0,
            },
        );
        let cleanup = FillCacheCleanup {
            cache: &client.fill_progress,
            key: key.clone(),
            shared: Arc::clone(&shared),
            progress: Arc::clone(&shared).try_lock_owned().unwrap(),
            initial_advances: 0,
            failed_query: true,
        };
        assert!(client
            .get_user_trades("BTCUSDT", 1, Decimal::from(2), 0)
            .await
            .unwrap_err()
            .to_string()
            .contains("already in progress"));
        drop(cleanup);
        assert_eq!(
            client.fill_progress.lock().unwrap()[&key].stalled_attempts,
            1
        );
    }

    #[test]
    fn old_cleanup_does_not_remove_replacement_progress() {
        let cache = std::sync::Mutex::new(std::collections::HashMap::new());
        let key = ("BTCUSDT".to_owned(), 1);
        let old = Arc::new(tokio::sync::Mutex::new(FillProgress::new(Decimal::ONE)));
        let replacement = Arc::new(tokio::sync::Mutex::new(FillProgress::new(Decimal::ONE)));
        let cleanup = FillCacheCleanup {
            cache: &cache,
            key: key.clone(),
            progress: Arc::clone(&old).try_lock_owned().unwrap(),
            shared: old,
            initial_advances: 0,
            failed_query: false,
        };
        cache.lock().unwrap().insert(
            key.clone(),
            FillCacheEntry {
                progress: Arc::clone(&replacement),
                last_used: Instant::now(),
                stalled_attempts: 0,
            },
        );
        drop(cleanup);
        assert!(Arc::ptr_eq(
            &cache.lock().unwrap()[&key].progress,
            &replacement
        ));
    }

    #[test]
    fn exchange_environment_follows_pair_configuration() {
        let mut config = crate::config::AppConfig::default().exchange;
        for (testnet, expected) in [
            (false, "https://fapi.binance.com"),
            (true, "https://testnet.binancefuture.com"),
        ] {
            config.is_testnet = testnet;
            assert_eq!(
                BinanceFuturesClient::new(&config).market_cache_key(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn exchange_info_and_open_orders_preserve_binance_error_codes() {
        use axum::{http::StatusCode, routing::get, Json, Router};
        use serde_json::json;

        let app = Router::new()
            .route(
                "/fapi/v1/exchangeInfo",
                get(|| async {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"code": -1121, "msg": "Invalid symbol."})),
                    )
                }),
            )
            .route(
                "/fapi/v1/openOrders",
                get(|| async {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"code": -1121, "msg": "Invalid symbol."})),
                    )
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = crate::config::AppConfig::default().exchange;
        let client = BinanceFuturesClient::with_base_url(&config, url);
        for error in [
            client.get_exchange_info(Some("INVALID")).await.unwrap_err(),
            client.get_open_orders("INVALID").await.unwrap_err(),
        ] {
            assert!(
                matches!(error.downcast_ref::<ExchangeError>(), Some(ExchangeError::ApiError { code: -1121, msg }) if msg == "Invalid symbol.")
            );
        }
        server.abort();
    }

    #[tokio::test]
    async fn rate_limit_response_blocks_shared_clients_then_recovers() {
        use axum::{http::StatusCode, routing::get, Json, Router};
        use serde_json::json;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new().route(
            "/fapi/v1/premiumIndex",
            get({
                let calls = calls.clone();
                move || {
                    let calls = calls.clone();
                    async move {
                        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                            (
                                StatusCode::TOO_MANY_REQUESTS,
                                [("retry-after", "1")],
                                Json(json!({})),
                            )
                        } else {
                            (
                                StatusCode::OK,
                                [("retry-after", "1")],
                                Json(json!({"markPrice":"100"})),
                            )
                        }
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = crate::config::AppConfig::default().exchange;
        let limiter = Arc::new(RateLimiter::default());
        let mut first = BinanceFuturesClient::with_base_url(&config, url.clone());
        let mut second = BinanceFuturesClient::with_base_url(&config, url);
        first.limiter = limiter.clone();
        second.limiter = limiter;
        assert!(first.get_mark_price("SOLUSDC").await.is_err());
        assert!(second.get_mark_price("BTCUSDT").await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        tokio::time::sleep(Duration::from_millis(1050)).await;
        assert_eq!(
            second.get_mark_price("BTCUSDT").await.unwrap(),
            rust_decimal_macros::dec!(100)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        server.abort();
    }

    #[tokio::test]
    async fn locally_blocked_order_is_known_not_to_have_been_submitted() {
        let config = crate::config::AppConfig::default().exchange;
        let client = BinanceFuturesClient::with_base_url(&config, "http://127.0.0.1:1".into());
        for _ in 0..100 {
            client.reserve(0, RequestClass::OpeningOrder).unwrap();
        }
        let error = client
            .place_order(NewOrderRequest {
                symbol: "SOLUSDC",
                side: "BUY",
                price: "100",
                quantity: "1",
                client_order_id: "gb_test",
                post_only: true,
                reduce_only: false,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, ExchangeError::RequestRejected(_)));
        assert!(!error.execution_status_unknown());
    }

    #[test]
    fn position_selection_accepts_one_way_short_and_rejects_hedge_mode() {
        let position = |side: &str, amount: &str| {
            serde_json::from_value::<BinancePositionRisk>(serde_json::json!({
                "symbol": "SOLUSDC", "positionSide": side, "positionAmt": amount,
                "entryPrice": "100", "markPrice": "99", "unRealizedProfit": "1",
                "liquidationPrice": "0", "leverage": "20"
            }))
            .unwrap()
        };
        let short = one_way_position(vec![position("BOTH", "-1")], "SOLUSDC")
            .unwrap()
            .unwrap();
        assert_eq!(short.position_amt, rust_decimal_macros::dec!(-1));
        assert!(one_way_position(vec![position("SHORT", "-1")], "SOLUSDC").is_err());
        assert!(one_way_position(
            vec![position("LONG", "0"), position("SHORT", "0")],
            "SOLUSDC"
        )
        .is_err());
    }

    #[test]
    fn order_errors_distinguish_uncertain_execution_from_explicit_rejection() {
        let cases = [
            (
                400,
                r#"{"code":-1006,"msg":"Execution status unknown."}"#,
                true,
            ),
            (400, r#"{"code":-1007,"msg":"Backend timeout."}"#, true),
            (
                503,
                r#"{"code":-1000,"msg":"Unknown error, please check your request or try again later."}"#,
                true,
            ),
            (
                500,
                r#"{"code":-1100,"msg":"unexpected server error"}"#,
                true,
            ),
            (408, "timeout", true),
            (
                503,
                r#"{"code":-1008,"msg":"Request throttled by system-level protection."}"#,
                false,
            ),
            (503, r#"{"code":-1000,"msg":"Service Unavailable."}"#, false),
            (503, "Service Unavailable.", false),
            (
                503,
                r#"{"code":-1000,"msg":"Internal error; unable to process your request. Please try again."}"#,
                false,
            ),
            (400, r#"{"code":-5022,"msg":"GTX rejected"}"#, false),
            (429, r#"{"code":-1003,"msg":"Too many requests"}"#, false),
        ];
        for (status, body, uncertain) in cases {
            let error =
                order_response_error(reqwest::StatusCode::from_u16(status).unwrap(), body.into());
            assert_eq!(
                error.execution_status_unknown(),
                uncertain,
                "{status}: {body}"
            );
        }
    }
}
