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
        .reserve("secondary", [1; 32], 1, RequestClass::Normal)
        .is_ok());
}

#[test]
fn pnl_budget_is_shared_across_accounts_and_preserves_other_capacity() {
    let limiter = RateLimiter::default();
    limiter
        .reserve("main", [1; 32], 40, RequestClass::PnlReconciliation)
        .unwrap();
    limiter
        .reserve("main", [2; 32], 20, RequestClass::PnlReconciliation)
        .unwrap();
    assert!(limiter
        .reserve("main", [2; 32], 1, RequestClass::PnlReconciliation)
        .is_err());
    assert!(limiter
        .reserve("secondary", [1; 32], 1, RequestClass::PnlReconciliation)
        .is_ok());
    limiter
        .reserve("main", [2; 32], IP_NORMAL - 60, RequestClass::Normal)
        .unwrap();
    assert!(limiter
        .reserve("main", [2; 32], 1, RequestClass::Normal)
        .is_err());
    assert!(limiter
        .reserve("main", [2; 32], 1, RequestClass::Critical)
        .is_ok());
}

#[test]
fn pnl_budget_expires_and_respects_the_normal_budget() {
    let limiter = RateLimiter::default();
    limiter
        .reserve("main", [1; 32], 60, RequestClass::PnlReconciliation)
        .unwrap();
    {
        let mut endpoints = limiter.endpoints.lock().unwrap();
        let endpoint = endpoints.get_mut("main").unwrap();
        for (time, _) in &mut endpoint.pnl_reconciliation_weight.events {
            *time = Instant::now() - MINUTE;
        }
    }
    limiter
        .reserve("main", [1; 32], 60, RequestClass::PnlReconciliation)
        .unwrap();

    let other = RateLimiter::default();
    other
        .reserve("main", [1; 32], IP_NORMAL - 1, RequestClass::Normal)
        .unwrap();
    assert!(other
        .reserve("main", [1; 32], 2, RequestClass::PnlReconciliation)
        .is_err());
    assert!(other
        .reserve("main", [1; 32], 1, RequestClass::PnlReconciliation)
        .is_ok());
    assert!(other
        .reserve("main", [1; 32], 1, RequestClass::Critical)
        .is_ok());
}

#[test]
fn pnl_budget_scales_down_with_advertised_exchange_limit() {
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
    limiter
        .reserve("main", [1; 32], 18, RequestClass::PnlReconciliation)
        .unwrap();
    assert!(limiter
        .reserve("main", [2; 32], 1, RequestClass::PnlReconciliation)
        .is_err());
    limiter
        .reserve("main", [2; 32], 57, RequestClass::Normal)
        .unwrap();
    assert!(limiter
        .reserve("main", [2; 32], 20, RequestClass::Critical)
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
        .reserve("secondary", [1; 32], 1, RequestClass::Normal)
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
fn epoch_sized_retry_after_clamps_to_max_retry_after_secs() {
    let limiter = RateLimiter::default();
    let mut headers = HeaderMap::new();
    headers.insert("retry-after", HeaderValue::from_static("1794393632"));
    let before = Instant::now();
    limiter.observe("main", [1; 32], StatusCode::TOO_MANY_REQUESTS, &headers);
    let endpoints = limiter.endpoints.lock().unwrap();
    let until = endpoints["main"].blocked_until.unwrap();
    let cooldown = until.duration_since(before);
    assert!(cooldown >= Duration::from_secs(MAX_RETRY_AFTER_SECS - 1));
    assert!(cooldown <= Duration::from_secs(MAX_RETRY_AFTER_SECS + 5));
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

#[test]
fn idle_account_limits_are_pruned_when_accounts_exceed_bound() {
    let limiter = RateLimiter::default();
    limiter
        .reserve("main", [200; 32], 0, RequestClass::OpeningOrder)
        .unwrap();
    for id in 0..70u8 {
        limiter
            .reserve("main", [id; 32], 1, RequestClass::Normal)
            .unwrap();
    }
    let endpoints = limiter.endpoints.lock().unwrap();
    let accounts = &endpoints["main"].accounts;
    assert!(accounts.len() <= 64, "accounts len: {}", accounts.len());
    assert!(accounts.contains_key(&[200; 32]));
    assert!(accounts.contains_key(&[69; 32]));
}
