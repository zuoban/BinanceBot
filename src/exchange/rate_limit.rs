use super::model::BinanceRateLimit;
use reqwest::{header::HeaderMap, StatusCode};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tracing::warn;

const MINUTE: Duration = Duration::from_secs(60);
const TEN_SECONDS: Duration = Duration::from_secs(10);
// Conservative local ceilings leave room for other applications and emergency work.
const IP_NORMAL: u64 = 1800;
const IP_CRITICAL: u64 = 2280;
const ORDERS_NORMAL_10S: u64 = 100;
const ORDERS_CRITICAL_10S: u64 = 150;
const ORDERS_NORMAL_1M: u64 = 1000;
const ORDERS_CRITICAL_1M: u64 = 1100;

#[derive(Clone, Copy)]
pub(crate) enum RequestClass {
    Normal,
    MarketFallback,
    Critical,
    OpeningOrder,
    ClosingOrder,
}

impl RequestClass {
    fn critical(self) -> bool {
        matches!(self, Self::Critical | Self::ClosingOrder)
    }
    fn order(self) -> bool {
        matches!(self, Self::OpeningOrder | Self::ClosingOrder)
    }
}

#[derive(Default)]
struct Counter {
    events: VecDeque<(Instant, u64)>,
    local_total: u64,
    external: VecDeque<(Instant, u64)>,
}

impl Counter {
    fn prune(&mut self, now: Instant, window: Duration) {
        while self
            .events
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) >= window)
        {
            if let Some((_, weight)) = self.events.pop_front() {
                self.local_total -= weight;
            }
        }
        while self
            .external
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) >= window)
        {
            self.external.pop_front();
        }
    }

    fn local(&self) -> u64 {
        self.local_total
    }

    fn record(&mut self, now: Instant, weight: u64) {
        self.events.push_back((now, weight));
        self.local_total += weight;
    }

    fn total(&self) -> u64 {
        self.local()
            .saturating_add(self.external.front().map_or(0, |(_, count)| *count))
    }

    fn observe(&mut self, count: u64, now: Instant, window: Duration) {
        self.prune(now, window);
        let external = count.saturating_sub(self.local());
        if external > 0 {
            // Keep the maximum unexpired observation, including later smaller ones.
            while self
                .external
                .back()
                .is_some_and(|(_, count)| *count <= external)
            {
                self.external.pop_back();
            }
            self.external.push_back((now, external));
        }
    }
}

#[derive(Default)]
struct AccountLimits {
    orders_10s: Counter,
    orders_1m: Counter,
}

#[derive(Default)]
struct EndpointLimits {
    weight: Counter,
    accounts: HashMap<[u8; 32], AccountLimits>,
    blocked_until: Option<Instant>,
    ip_limit: Option<u64>,
    order_10s_limit: Option<u64>,
    order_1m_limit: Option<u64>,
}

#[derive(Default)]
pub(crate) struct RateLimiter {
    endpoints: Mutex<HashMap<String, EndpointLimits>>,
}

impl RateLimiter {
    pub fn reserve(
        &self,
        endpoint: &str,
        account: [u8; 32],
        weight: u64,
        class: RequestClass,
    ) -> Result<(), String> {
        let mut endpoints = self.endpoints.lock().unwrap();
        let now = Instant::now();
        let endpoint = endpoints.entry(endpoint.to_owned()).or_default();
        if endpoint.blocked_until.is_some_and(|until| now < until) {
            return Err("Binance rate-limit cooldown is active".into());
        }
        endpoint.weight.prune(now, MINUTE);
        let weight_limit = if class.critical() {
            IP_CRITICAL
        } else {
            IP_NORMAL
        }
        .min(endpoint.ip_limit.map_or(u64::MAX, |limit| {
            limit.saturating_mul(if class.critical() { 95 } else { 75 }) / 100
        }));
        let weight_limit = if matches!(class, RequestClass::MarketFallback) {
            weight_limit.saturating_mul(3) / 4
        } else {
            weight_limit
        };
        if endpoint.weight.total().saturating_add(weight) > weight_limit {
            return Err(format!(
                "local Binance IP request-weight budget reached ({weight_limit}/minute)"
            ));
        }
        let account = endpoint.accounts.entry(account).or_default();
        account.orders_10s.prune(now, TEN_SECONDS);
        account.orders_1m.prune(now, MINUTE);
        if class.order() {
            let limit_10s = if class.critical() {
                ORDERS_CRITICAL_10S
            } else {
                ORDERS_NORMAL_10S
            }
            .min(endpoint.order_10s_limit.map_or(u64::MAX, |limit| {
                limit.saturating_mul(if class.critical() { 95 } else { 80 }) / 100
            }));
            let limit_1m = if class.critical() {
                ORDERS_CRITICAL_1M
            } else {
                ORDERS_NORMAL_1M
            }
            .min(endpoint.order_1m_limit.map_or(u64::MAX, |limit| {
                limit.saturating_mul(if class.critical() { 95 } else { 80 }) / 100
            }));
            if account.orders_10s.total() >= limit_10s || account.orders_1m.total() >= limit_1m {
                return Err("local Binance order-rate budget reached".into());
            }
            account.orders_10s.record(now, 1);
            account.orders_1m.record(now, 1);
        }
        if weight > 0 {
            endpoint.weight.record(now, weight);
        }
        Ok(())
    }

    pub fn update_exchange_limits(&self, endpoint: &str, limits: &[BinanceRateLimit]) {
        let mut endpoints = self.endpoints.lock().unwrap();
        let endpoint = endpoints.entry(endpoint.to_owned()).or_default();
        for limit in limits.iter().filter(|limit| limit.limit > 0) {
            let slot = match (
                limit.rate_limit_type.as_str(),
                limit.interval.as_str(),
                limit.interval_num,
            ) {
                ("REQUEST_WEIGHT", "MINUTE", 1) => &mut endpoint.ip_limit,
                ("ORDERS", "SECOND", 10) => &mut endpoint.order_10s_limit,
                ("ORDERS", "MINUTE", 1) => &mut endpoint.order_1m_limit,
                _ => continue,
            };
            *slot = Some(slot.map_or(limit.limit, |old| old.min(limit.limit)));
        }
    }

    pub fn observe(
        &self,
        endpoint: &str,
        account: [u8; 32],
        status: StatusCode,
        headers: &HeaderMap,
    ) {
        let mut endpoints = self.endpoints.lock().unwrap();
        let now = Instant::now();
        let endpoint = endpoints.entry(endpoint.to_owned()).or_default();
        let value = |name| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
        };
        if let Some(weight) = value("x-mbx-used-weight-1m") {
            endpoint.weight.observe(weight, now, MINUTE);
        }
        let limits = endpoint.accounts.entry(account).or_default();
        if let Some(count) = value("x-mbx-order-count-10s") {
            limits.orders_10s.observe(count, now, TEN_SECONDS);
        }
        if let Some(count) = value("x-mbx-order-count-1m") {
            limits.orders_1m.observe(count, now, MINUTE);
        }
        if status == StatusCode::TOO_MANY_REQUESTS || status.as_u16() == 418 {
            let default = if status.as_u16() == 418 {
                3 * 24 * 60 * 60
            } else {
                60
            };
            let seconds = value("retry-after").unwrap_or(default).max(1);
            let duration = Duration::from_secs(seconds);
            let until = now
                .checked_add(duration)
                .unwrap_or(now + Duration::from_secs(3 * 24 * 60 * 60));
            endpoint.blocked_until =
                Some(endpoint.blocked_until.map_or(until, |old| old.max(until)));
            warn!(
                "Binance returned HTTP {}; requests suspended for {} seconds",
                status, seconds
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    #[test]
    fn counter_prunes_expired_events_from_its_running_total() {
        let now = Instant::now();
        let mut counter = Counter::default();
        counter.record(now - Duration::from_secs(61), 5);
        counter.record(now, 2);
        counter.prune(now, MINUTE);
        assert_eq!(counter.local(), 2);
        counter.prune(now + MINUTE, MINUTE);
        assert_eq!(counter.local(), 0);
    }

    #[test]
    fn concurrent_requests_cannot_exceed_shared_budget() {
        let limiter = std::sync::Arc::new(RateLimiter::default());
        let threads: Vec<_> = (0..20)
            .map(|_| {
                let limiter = limiter.clone();
                std::thread::spawn(move || {
                    (0..100)
                        .filter(|_| {
                            limiter
                                .reserve("main", [1; 32], 1, RequestClass::Normal)
                                .is_ok()
                        })
                        .count()
                })
            })
            .collect();
        let accepted: usize = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .sum();
        assert_eq!(accepted, IP_NORMAL as usize);
    }

    #[test]
    fn normal_weight_is_shared_by_accounts_and_reserves_capacity_for_critical_requests() {
        let limiter = RateLimiter::default();
        let endpoint = "main";
        for _ in 0..1800 {
            limiter
                .reserve(endpoint, [1; 32], 1, RequestClass::Normal)
                .unwrap();
        }
        assert!(limiter
            .reserve(endpoint, [2; 32], 1, RequestClass::Normal)
            .is_err());
        assert!(limiter
            .reserve(endpoint, [2; 32], 1, RequestClass::Critical)
            .is_ok());
        assert!(limiter
            .reserve("testnet", [1; 32], 1, RequestClass::Normal)
            .is_ok());
    }

    #[test]
    fn market_fallback_preserves_normal_budget_and_recovers_when_window_expires() {
        let limiter = RateLimiter::default();
        let fallback_limit = IP_NORMAL * 3 / 4;
        limiter
            .reserve(
                "main",
                [1; 32],
                fallback_limit,
                RequestClass::MarketFallback,
            )
            .unwrap();
        assert!(limiter
            .reserve("main", [2; 32], 1, RequestClass::MarketFallback)
            .is_err());
        assert!(limiter
            .reserve(
                "main",
                [2; 32],
                IP_NORMAL - fallback_limit,
                RequestClass::Normal
            )
            .is_ok());
        assert!(limiter
            .reserve("main", [2; 32], 1, RequestClass::Critical)
            .is_ok());
        let mut endpoints = limiter.endpoints.lock().unwrap();
        let counter = &mut endpoints.get_mut("main").unwrap().weight;
        for (time, _) in &mut counter.events {
            *time = Instant::now() - MINUTE;
        }
        drop(endpoints);
        assert!(limiter
            .reserve("main", [2; 32], 1, RequestClass::MarketFallback)
            .is_ok());
    }

    #[test]
    fn order_budget_is_account_scoped_and_preserves_reduce_only_capacity() {
        let limiter = RateLimiter::default();
        for _ in 0..100 {
            limiter
                .reserve("main", [1; 32], 0, RequestClass::OpeningOrder)
                .unwrap();
        }
        assert!(limiter
            .reserve("main", [1; 32], 0, RequestClass::OpeningOrder)
            .is_err());
        assert!(limiter
            .reserve("main", [1; 32], 0, RequestClass::ClosingOrder)
            .is_ok());
        assert!(limiter
            .reserve("main", [2; 32], 0, RequestClass::OpeningOrder)
            .is_ok());
    }

    #[test]
    fn exchange_headers_account_for_other_clients_and_reject_lower_out_of_order_counts() {
        let limiter = RateLimiter::default();
        let mut headers = HeaderMap::new();
        headers.insert("x-mbx-used-weight-1m", HeaderValue::from_static("1799"));
        headers.insert("x-mbx-order-count-10s", HeaderValue::from_static("100"));
        limiter.observe("main", [1; 32], StatusCode::OK, &headers);
        headers.insert("x-mbx-used-weight-1m", HeaderValue::from_static("1"));
        headers.insert("x-mbx-order-count-10s", HeaderValue::from_static("1"));
        limiter.observe("main", [1; 32], StatusCode::OK, &headers);
        assert!(limiter
            .reserve("main", [2; 32], 2, RequestClass::Normal)
            .is_err());
        assert!(limiter
            .reserve("main", [1; 32], 0, RequestClass::OpeningOrder)
            .is_err());
        assert!(limiter
            .reserve("main", [1; 32], 1, RequestClass::Critical)
            .is_ok());
    }

    #[test]
    fn repeated_external_usage_extends_expiry_without_accepting_lower_counts() {
        let start = Instant::now();
        let mut counter = Counter::default();
        counter.observe(1000, start, MINUTE);
        counter.observe(1000, start + Duration::from_secs(59), MINUTE);
        counter.observe(1, start + Duration::from_secs(60), MINUTE);
        counter.prune(start + Duration::from_secs(61), MINUTE);
        assert_eq!(counter.total(), 1000);
        counter.prune(start + Duration::from_secs(119), MINUTE);
        assert_eq!(counter.total(), 1);
        counter.prune(start + Duration::from_secs(120), MINUTE);
        assert_eq!(counter.total(), 0);
    }

    #[test]
    fn lower_external_usage_remains_after_older_higher_observation_expires() {
        let start = Instant::now();
        let mut counter = Counter::default();
        counter.observe(1000, start, MINUTE);
        counter.observe(900, start + Duration::from_secs(59), MINUTE);
        counter.prune(start + Duration::from_secs(60), MINUTE);
        assert_eq!(counter.total(), 900);
        counter.prune(start + Duration::from_secs(119), MINUTE);
        assert_eq!(counter.total(), 0);
    }

    #[test]
    fn rate_limit_responses_apply_cooldown_and_can_recover() {
        let limiter = RateLimiter::default();
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", HeaderValue::from_static("5"));
        limiter.observe("main", [1; 32], StatusCode::TOO_MANY_REQUESTS, &headers);
        assert!(limiter
            .reserve("main", [1; 32], 1, RequestClass::Critical)
            .is_err());
        assert!(limiter
            .reserve("testnet", [1; 32], 1, RequestClass::Normal)
            .is_ok());
        limiter
            .endpoints
            .lock()
            .unwrap()
            .get_mut("main")
            .unwrap()
            .blocked_until = Some(Instant::now() - Duration::from_secs(1));
        assert!(limiter
            .reserve("main", [1; 32], 1, RequestClass::Normal)
            .is_ok());
    }

    #[test]
    fn ban_without_retry_after_is_held_for_three_days() {
        let limiter = RateLimiter::default();
        limiter.observe(
            "main",
            [1; 32],
            StatusCode::from_u16(418).unwrap(),
            &HeaderMap::new(),
        );
        let endpoints = limiter.endpoints.lock().unwrap();
        let until = endpoints["main"].blocked_until.unwrap();
        assert!(until.duration_since(Instant::now()) > Duration::from_secs(3 * 24 * 60 * 60 - 5));
    }

    #[test]
    fn advertised_exchange_limits_reduce_local_budgets() {
        let limiter = RateLimiter::default();
        limiter.update_exchange_limits(
            "main",
            &[BinanceRateLimit {
                rate_limit_type: "REQUEST_WEIGHT".into(),
                interval: "MINUTE".into(),
                interval_num: 1,
                limit: 100,
            }],
        );
        assert!(limiter
            .reserve("main", [1; 32], 75, RequestClass::Normal)
            .is_ok());
        assert!(limiter
            .reserve("main", [1; 32], 1, RequestClass::Normal)
            .is_err());
        assert!(limiter
            .reserve("main", [1; 32], 20, RequestClass::Critical)
            .is_ok());
    }
}
