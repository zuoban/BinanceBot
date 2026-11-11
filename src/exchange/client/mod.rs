use crate::config::ExchangeConfig;
use crate::exchange::model::*;
use crate::exchange::rate_limit::{RateLimiter, RequestClass};
use crate::exchange::signature::{
    build_signed_url_fmt, compute_account_cache_key, identity_key, new_hmac_key, HmacSha256,
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

mod endpoints;
mod error;
mod fill_reconciler;

use error::{
    checked_response, is_invalid_timestamp_body, is_valid_client_order_id, order_response_error,
    parse_api_error_body, read_bounded_error_body,
};
pub(crate) use error::{
    read_bounded_bytes, read_bounded_json, summarize_non_json_body, ExchangeError,
    LocalRateLimitError,
};

#[cfg(test)]
use endpoints::one_way_position;
pub(crate) use endpoints::NewOrderRequest;
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
    pub(super) base_url: String,
    api_key_header: Option<HeaderValue>,
    hmac_key: HmacSha256,
    pub(super) client: Client,
    recv_window: u64,
    account_cache_key: [u8; 32],
    account_identity_key: [u8; 32],
    rate_account_key: [u8; 32],
    time_offset_ms: AtomicI64,
    last_time_sync_ms: AtomicU64,
    pub(super) limiter: Arc<RateLimiter>,
    fill_reconciler: std::sync::Mutex<FillReconcilerState>,
    pub(super) fill_retention_seconds: AtomicU64,
}

impl BinanceFuturesClient {
    #[must_use]
    #[inline]
    pub(crate) fn market_cache_key(&self) -> &str {
        &self.base_url
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

    pub(super) async fn signed_error_from_body(
        &self,
        status: reqwest::StatusCode,
        body: &str,
        context: Option<&str>,
    ) -> anyhow::Error {
        self.maybe_resync_on_invalid_timestamp(body).await;
        let err = parse_api_error_body(status, body);
        match context {
            Some(context) => {
                let detail = format!("{context} ({status}): {err}");
                err.context(detail)
            }
            None => err,
        }
    }

    async fn ensure_signed_response(
        &self,
        resp: reqwest::Response,
        context: Option<&str>,
    ) -> Result<reqwest::Response> {
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status();
        let body = read_bounded_error_body(resp).await?;
        Err(self.signed_error_from_body(status, &body, context).await)
    }

    pub(super) async fn checked_signed_response(
        &self,
        response: reqwest::Response,
    ) -> Result<reqwest::Response> {
        self.ensure_signed_response(response, None).await
    }

    pub(super) async fn ensure_signed_success(
        &self,
        resp: reqwest::Response,
        context: &str,
    ) -> Result<reqwest::Response> {
        self.ensure_signed_response(resp, Some(context)).await
    }

    fn current_timestamp(&self) -> i64 {
        Utc::now()
            .timestamp_millis()
            .saturating_add(self.time_offset_ms.load(Ordering::SeqCst))
    }

    pub(super) fn signed_url_fmt(&self, endpoint: &str, query: std::fmt::Arguments<'_>) -> String {
        build_signed_url_fmt(
            &self.base_url,
            &self.hmac_key,
            self.recv_window,
            self.current_timestamp(),
            endpoint,
            query,
        )
    }

    #[must_use]
    #[inline]
    pub(crate) fn account_cache_key(&self) -> [u8; 32] {
        self.account_cache_key
    }

    // Recovery records belong to the exchange account, independent of request
    // timing settings or changes to the API secret for the same API key.
    #[must_use]
    #[inline]
    pub(crate) fn account_identity_key(&self) -> [u8; 32] {
        self.account_identity_key
    }

    #[must_use]
    #[inline]
    pub(crate) fn configured_account_identity(config: &ExchangeConfig) -> [u8; 32] {
        identity_key("https://fapi.binance.com", &config.api_key)
    }

    #[cfg(test)]
    pub(crate) fn limiter(&self) -> Arc<RateLimiter> {
        self.limiter.clone()
    }

    pub(super) fn reserve(&self, weight: u64, class: RequestClass) -> Result<()> {
        self.limiter
            .reserve(&self.base_url, self.rate_account_key, weight, class)
            .map_err(|error| {
                tracing::warn!("{}", error);
                anyhow!(LocalRateLimitError(error))
            })
    }

    pub(super) async fn send(
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

    pub(super) async fn get_public_json<T: serde::de::DeserializeOwned>(
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
        read_bounded_json(resp, 16 * 1024 * 1024).await
    }

    pub(super) fn prepare_signed_get_url(
        &self,
        weight: u64,
        class: RequestClass,
        path: &str,
        query: std::fmt::Arguments<'_>,
    ) -> Result<String> {
        self.reserve(weight, class)?;
        Ok(self.signed_url_fmt(path, query))
    }

    pub(super) async fn get_signed_json_url<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        context: &str,
    ) -> Result<T> {
        let resp = self
            .ensure_signed_success(self.send(self.client.get(url)).await?, context)
            .await?;
        read_bounded_json(resp, 8 * 1024 * 1024).await
    }
}

#[cfg(test)]
mod tests;
