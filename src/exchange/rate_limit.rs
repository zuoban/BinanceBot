use super::model::BinanceRateLimit;
use reqwest::{header::HeaderMap, StatusCode};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tracing::warn;

const MINUTE: Duration = Duration::from_secs(60);
const TEN_SECONDS: Duration = Duration::from_secs(10);
const MAX_RETRY_AFTER_SECS: u64 = 3 * 24 * 60 * 60;
// Conservative local ceilings leave room for other applications and emergency work.
const IP_NORMAL: u64 = 1800;
const IP_CRITICAL: u64 = 2280;
const PNL_RECONCILIATION_1M: u64 = 60;
const ORDERS_NORMAL_10S: u64 = 100;
const ORDERS_CRITICAL_10S: u64 = 150;
const ORDERS_NORMAL_1M: u64 = 1000;
const ORDERS_CRITICAL_1M: u64 = 1100;

#[derive(Clone, Copy)]
pub(crate) enum RequestClass {
    Normal,
    MarketFallback,
    PnlReconciliation,
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

    fn is_empty(&self) -> bool {
        self.events.is_empty() && self.external.is_empty()
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
    pnl_reconciliation_weight: Counter,
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

fn scaled_limit(
    default_normal: u64,
    default_critical: u64,
    advertised: Option<u64>,
    normal_pct: u64,
    critical_pct: u64,
    is_critical: bool,
) -> u64 {
    let base = if is_critical {
        default_critical
    } else {
        default_normal
    };
    let pct = if is_critical {
        critical_pct
    } else {
        normal_pct
    };
    base.min(advertised.map_or(u64::MAX, |limit| limit.saturating_mul(pct) / 100))
}

impl RateLimiter {
    fn endpoint_mut<'a>(
        endpoints: &'a mut HashMap<String, EndpointLimits>,
        endpoint: &str,
    ) -> &'a mut EndpointLimits {
        if endpoints.contains_key(endpoint) {
            return endpoints.get_mut(endpoint).expect("checked key");
        }
        endpoints.entry(endpoint.to_owned()).or_default()
    }

    fn account_limits_mut(
        accounts: &mut HashMap<[u8; 32], AccountLimits>,
        account: [u8; 32],
        now: Instant,
    ) -> &mut AccountLimits {
        if accounts.len() > 64 || (accounts.len() == 64 && !accounts.contains_key(&account)) {
            accounts.retain(|key, limits| {
                if *key == account {
                    return true;
                }
                limits.orders_10s.prune(now, TEN_SECONDS);
                limits.orders_1m.prune(now, MINUTE);
                !limits.orders_10s.is_empty() || !limits.orders_1m.is_empty()
            });
        }
        accounts.entry(account).or_default()
    }

    pub(crate) fn reserve(
        &self,
        endpoint: &str,
        account: [u8; 32],
        weight: u64,
        class: RequestClass,
    ) -> Result<(), String> {
        let mut endpoints = self.endpoints.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let endpoint = Self::endpoint_mut(&mut endpoints, endpoint);
        if endpoint.blocked_until.is_some_and(|until| now < until) {
            return Err("Binance rate-limit cooldown is active".into());
        }
        endpoint.weight.prune(now, MINUTE);
        let weight_limit = scaled_limit(
            IP_NORMAL,
            IP_CRITICAL,
            endpoint.ip_limit,
            75,
            95,
            class.critical(),
        );
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
        if matches!(class, RequestClass::PnlReconciliation) {
            endpoint.pnl_reconciliation_weight.prune(now, MINUTE);
            // Keep ordinary requests viable even when Binance advertises a lower limit.
            let pnl_limit = PNL_RECONCILIATION_1M.min(weight_limit / 4);
            if endpoint
                .pnl_reconciliation_weight
                .local()
                .saturating_add(weight)
                > pnl_limit
            {
                return Err(format!(
                    "local Binance PnL reconciliation request-weight budget reached ({pnl_limit}/minute)"
                ));
            }
        }
        Self::reserve_account_order_budget(endpoint, account, now, class)?;
        if weight > 0 {
            endpoint.weight.record(now, weight);
            if matches!(class, RequestClass::PnlReconciliation) {
                endpoint.pnl_reconciliation_weight.record(now, weight);
            }
        }
        Ok(())
    }

    fn reserve_account_order_budget(
        endpoint: &mut EndpointLimits,
        account: [u8; 32],
        now: Instant,
        class: RequestClass,
    ) -> Result<(), String> {
        let account = Self::account_limits_mut(&mut endpoint.accounts, account, now);
        account.orders_10s.prune(now, TEN_SECONDS);
        account.orders_1m.prune(now, MINUTE);
        if class.order() {
            let limit_10s = scaled_limit(
                ORDERS_NORMAL_10S,
                ORDERS_CRITICAL_10S,
                endpoint.order_10s_limit,
                80,
                95,
                class.critical(),
            );
            let limit_1m = scaled_limit(
                ORDERS_NORMAL_1M,
                ORDERS_CRITICAL_1M,
                endpoint.order_1m_limit,
                80,
                95,
                class.critical(),
            );
            if account.orders_10s.total() >= limit_10s || account.orders_1m.total() >= limit_1m {
                return Err("local Binance order-rate budget reached".into());
            }
            account.orders_10s.record(now, 1);
            account.orders_1m.record(now, 1);
        }
        Ok(())
    }

    pub(crate) fn update_exchange_limits(&self, endpoint: &str, limits: &[BinanceRateLimit]) {
        let mut endpoints = self.endpoints.lock().unwrap_or_else(|e| e.into_inner());
        let endpoint = Self::endpoint_mut(&mut endpoints, endpoint);
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

    pub(crate) fn observe(
        &self,
        endpoint: &str,
        account: [u8; 32],
        status: StatusCode,
        headers: &HeaderMap,
    ) {
        let mut endpoints = self.endpoints.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let endpoint = Self::endpoint_mut(&mut endpoints, endpoint);
        let value = |name| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
        };
        if let Some(weight) = value("x-mbx-used-weight-1m") {
            endpoint.weight.observe(weight, now, MINUTE);
        }
        let limits = Self::account_limits_mut(&mut endpoint.accounts, account, now);
        if let Some(count) = value("x-mbx-order-count-10s") {
            limits.orders_10s.observe(count, now, TEN_SECONDS);
        }
        if let Some(count) = value("x-mbx-order-count-1m") {
            limits.orders_1m.observe(count, now, MINUTE);
        }
        if status == StatusCode::TOO_MANY_REQUESTS || status.as_u16() == 418 {
            let default = if status.as_u16() == 418 {
                MAX_RETRY_AFTER_SECS
            } else {
                60
            };
            let seconds = value("retry-after")
                .unwrap_or(default)
                .clamp(1, MAX_RETRY_AFTER_SECS);
            let duration = Duration::from_secs(seconds);
            let until = now
                .checked_add(duration)
                .unwrap_or(now + Duration::from_secs(MAX_RETRY_AFTER_SECS));
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
#[path = "rate_limit_tests.rs"]
mod tests;
