use super::*;
use crate::config::AppConfig;
use crate::db::Database;
use futures_util::future::join_all;
use rust_decimal_macros::dec;
use std::time::Instant;

fn parameter(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|value| value.parse::<usize>().expect("invalid benchmark parameter"))
        .unwrap_or(default)
}

// This is a burst workload, not a production-capacity or exchange-limit test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit local capacity benchmark"]
async fn simulated_pair_burst() {
    let pairs = parameter("BENCH_PAIRS", 50);
    let rounds = parameter("BENCH_ROUNDS", 6);
    assert!(pairs > 0 && rounds >= 2);
    let db = Arc::new(Database::open(":memory:").unwrap());
    let trade_lock = Arc::new(tokio::sync::Mutex::new(()));
    let mut engines = Vec::with_capacity(pairs);
    let start = Instant::now();
    for i in 0..pairs {
        let mut config = AppConfig::default();
        config.exchange.symbol = format!("BENCH{i}USDT");
        config.grid.grid_interval = dec!(1);
        config.grid.buy_window = 2;
        config.grid.sell_window = 2;
        let (action_tx, action_rx) = mpsc::channel(8);
        let (_, ticker_rx) = broadcast::channel(8);
        let state =
            AppState::new_for_pair(config.clone(), db.clone(), action_tx, trade_lock.clone());
        let client = Arc::new(BinanceFuturesClient::with_base_url(
            &config.exchange,
            "http://127.0.0.1:1".into(),
        ));
        engines.push(GridTradingEngine::new(state, client, action_rx, ticker_rx));
    }
    let setup_ms = start.elapsed().as_secs_f64() * 1000.0;
    let mut samples = Vec::with_capacity(pairs * rounds);
    let mut batch_ms = Vec::with_capacity(rounds);
    for round in 0..rounds {
        let price = match round % 3 {
            0 => dec!(100.5),
            1 => dec!(99),
            _ => dec!(101),
        };
        let start = Instant::now();
        let tasks = engines.drain(..).map(|mut engine| {
            tokio::spawn(async move {
                let start = Instant::now();
                let symbol = engine.state.config.read().await.exchange.symbol.clone();
                let update = TickerInfo {
                    symbol,
                    last_price: price,
                    mark_price: price,
                    update_time: Utc::now(),
                    mark_update_time: Utc::now(),
                    ..Default::default()
                };
                engine.handle_ticker_update(update).await;
                engine.maintain_grid_window().await;
                (engine, start.elapsed().as_secs_f64() * 1000.0)
            })
        });
        for result in join_all(tasks).await {
            let (engine, elapsed) = result.unwrap();
            engines.push(engine);
            samples.push(elapsed);
        }
        batch_ms.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    let start = Instant::now();
    let snapshots = join_all(engines.iter().map(|engine| engine.state.snapshot())).await;
    let snapshot_ms = start.elapsed().as_secs_f64() * 1000.0;
    let fills: usize = snapshots
        .iter()
        .map(|snapshot| snapshot.stats.total_trades)
        .sum();
    assert!(fills > 0, "benchmark must exercise simulated fills");
    samples.sort_by(f64::total_cmp);
    let percentile = |percent: usize| samples[(samples.len() - 1) * percent / 100];
    println!(
        "pairs={pairs} rounds={rounds} setup_ms={setup_ms:.1} pair_ms_p50={:.2} pair_ms_p95={:.2} pair_ms_p99={:.2} batch_ms={batch_ms:?} snapshot_ms={snapshot_ms:.1} fills={fills}",
        percentile(50),
        percentile(95),
        percentile(99)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit local reconciliation benchmark"]
async fn simulated_live_reconciliation() {
    use crate::exchange::account_cache::AccountCache;
    use crate::exchange::market_cache::MarketCache;
    use crate::exchange::position_cache::PositionCache;
    use crate::exchange::rate_limit::RateLimiter;
    use axum::{extract::Query, routing::get, Json, Router};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let pairs = parameter("BENCH_PAIRS", 30);
    let seconds = parameter("BENCH_SECONDS", 65);
    let outage_secs = parameter("BENCH_OUTAGE_SECS", 0);
    let fail_mark = parameter("BENCH_FAIL_MARK", 0) != 0;
    let outage_start = Duration::from_secs(6);
    assert!(pairs > 0 && seconds >= 4);
    assert!(outage_secs == 0 || seconds > outage_secs + 11);
    let start = tokio::time::Instant::now();
    let outage_end = outage_start + Duration::from_secs(outage_secs as u64);
    let mark_calls = Arc::new(AtomicUsize::new(0));
    let bulk_mark_calls = Arc::new(AtomicUsize::new(0));
    let ticker_calls = Arc::new(AtomicUsize::new(0));
    let order_calls = Arc::new(AtomicUsize::new(0));
    let account_calls = Arc::new(AtomicUsize::new(0));
    let position_calls = Arc::new(AtomicUsize::new(0));
    let emergency_list_calls = Arc::new(AtomicUsize::new(0));
    let emergency_cancel_calls = Arc::new(AtomicUsize::new(0));
    let positions = Arc::new(
        (0..pairs)
            .map(|i| {
                json!({
                    "symbol": format!("BENCH{i}USDT"), "positionSide": "BOTH",
                    "positionAmt": "0", "entryPrice": "0", "markPrice": "100",
                    "unRealizedProfit": "0", "liquidationPrice": "0", "leverage": "20"
                })
            })
            .collect::<Vec<_>>(),
    );
    let app = Router::new()
        .route(
            "/fapi/v1/premiumIndex",
            get({
                let calls = mark_calls.clone();
                let bulk_calls = bulk_mark_calls.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    calls.fetch_add(1, Ordering::Relaxed);
                    let symbol = query.get("symbol").cloned();
                    if symbol.is_none() {
                        bulk_calls.fetch_add(1, Ordering::Relaxed);
                    }
                    async move {
                        let elapsed = start.elapsed();
                        if fail_mark && elapsed >= outage_start && elapsed < outage_end {
                            (
                                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                                Json(json!({"code": -1000})),
                            )
                        } else {
                            (
                                axum::http::StatusCode::OK,
                                Json(match symbol {
                                    Some(symbol) => json!({"symbol": symbol, "markPrice": "100"}),
                                    None => json!((0..pairs).map(|i| json!({
                                        "symbol": format!("BENCH{i}USDT"), "markPrice": "100"
                                    })).collect::<Vec<_>>()),
                                }),
                            )
                        }
                    }
                }
            }),
        )
        .route(
            "/fapi/v1/ticker/24hr",
            get({
                let calls = ticker_calls.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    calls.fetch_add(1, Ordering::Relaxed);
                    async move {
                        Json(json!({
                            "symbol": query.get("symbol").cloned().unwrap_or_default(),
                            "lastPrice": "100", "priceChange": "0", "priceChangePercent": "0",
                            "highPrice": "100", "lowPrice": "100", "volume": "1", "quoteVolume": "100"
                        }))
                    }
                }
            }),
        )
        .route(
            "/fapi/v1/openOrders",
            get({
                let calls = order_calls.clone();
                let emergency_calls = emergency_list_calls.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    if query
                        .get("symbol")
                        .is_some_and(|symbol| symbol == "BENCHPROBEUSDT")
                    {
                        emergency_calls.fetch_add(1, Ordering::Relaxed);
                    } else {
                        calls.fetch_add(1, Ordering::Relaxed);
                    }
                    async { Json(json!([])) }
                }
            }),
        )
        .route(
            "/fapi/v1/order",
            axum::routing::delete({
                let calls = emergency_cancel_calls.clone();
                move || {
                    calls.fetch_add(1, Ordering::Relaxed);
                    async { Json(json!({})) }
                }
            }),
        )
        .route(
            "/fapi/v2/positionRisk",
            get({
                let calls = position_calls.clone();
                let positions = positions.clone();
                move || {
                    calls.fetch_add(1, Ordering::Relaxed);
                    let positions = positions.clone();
                    async move { Json((*positions).clone()) }
                }
            }),
        )
        .route(
            "/fapi/v2/account",
            get({
                let calls = account_calls.clone();
                move || {
                    calls.fetch_add(1, Ordering::Relaxed);
                    async {
                        Json(json!({
                            "totalWalletBalance": "10000", "totalMarginBalance": "10000",
                            "totalUnrealizedProfit": "0", "availableBalance": "10000",
                            "assets": [{"asset": "USDT", "walletBalance": "10000",
                                "marginBalance": "10000", "availableBalance": "10000",
                                "unrealizedProfit": "0"}]
                        }))
                    }
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let db = Arc::new(Database::open(":memory:").unwrap());
    let trade_lock = Arc::new(tokio::sync::Mutex::new(()));
    let account_cache = Arc::new(AccountCache::default());
    let position_cache = Arc::new(PositionCache::default());
    let market_cache = Arc::new(MarketCache::default());
    let limiter = Arc::new(RateLimiter::default());
    let deadline = start + Duration::from_secs(seconds as u64);
    let mut probe_config = AppConfig::default().exchange;
    probe_config.api_key = "benchmark-key".into();
    probe_config.api_secret = "benchmark-secret".into();
    let probe = BinanceFuturesClient::with_base_url_and_limiter(
        &probe_config,
        url.clone(),
        limiter.clone(),
    );
    let emergency = tokio::spawn(async move {
        tokio::time::sleep_until(deadline).await;
        let list_ok = probe.get_open_orders("BENCHPROBEUSDT").await.is_ok();
        let cancel_ok = probe
            .cancel_order("BENCHPROBEUSDT", Some(123), None)
            .await
            .is_ok();
        (list_ok, cancel_ok)
    });
    let mut tasks = Vec::with_capacity(pairs);
    for i in 0..pairs {
        let mut config = AppConfig::default();
        config.exchange.symbol = format!("BENCH{i}USDT");
        config.exchange.dry_run = false;
        config.exchange.api_key = "benchmark-key".into();
        config.exchange.api_secret = "benchmark-secret".into();
        let (action_tx, action_rx) = mpsc::channel(8);
        let (_, ticker_rx) = broadcast::channel(8);
        let state =
            AppState::new_for_pair(config.clone(), db.clone(), action_tx, trade_lock.clone());
        let client = Arc::new(BinanceFuturesClient::with_base_url_and_limiter(
            &config.exchange,
            url.clone(),
            limiter.clone(),
        ));
        let mut engine = GridTradingEngine::new(state, client, action_rx, ticker_rx);
        engine.set_account_cache(account_cache.clone());
        engine.set_position_cache(position_cache.clone());
        engine.set_market_cache(market_cache.clone());
        tasks.push(tokio::spawn(async move {
            let offset = Duration::from_millis((i as u64 * 3000) / pairs as u64);
            let mut timer = tokio::time::interval_at(start + offset, Duration::from_secs(3));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut samples = Vec::new();
            let mut orders_ok = 0;
            let mut account_ok = 0;
            let mut outage_fresh = 0;
            let mut outage_stale = 0;
            let mut recovery_fresh = 0;
            loop {
                let scheduled = timer.tick().await;
                if scheduled >= deadline || tokio::time::Instant::now() >= deadline {
                    break;
                }
                let elapsed = start.elapsed();
                let in_outage = outage_secs > 0 && elapsed >= outage_start && elapsed < outage_end;
                if !in_outage {
                    let now = Utc::now();
                    engine
                        .handle_ticker_update(TickerInfo {
                            symbol: config.exchange.symbol.clone(),
                            last_price: dec!(100),
                            mark_price: dec!(100),
                            update_time: now,
                            mark_update_time: now,
                            ..Default::default()
                        })
                        .await;
                }
                engine.last_orders_sync = None;
                engine.last_account_sync = None;
                let cycle_start = Instant::now();
                engine.sync_cycle().await;
                samples.push(cycle_start.elapsed().as_secs_f64() * 1000.0);
                orders_ok += usize::from(engine.last_orders_sync.is_some());
                account_ok += usize::from(engine.last_account_sync.is_some());
                let fresh = engine
                    .state
                    .ticker
                    .read()
                    .await
                    .prices_are_fresh(Utc::now());
                if in_outage {
                    outage_fresh += usize::from(fresh);
                    outage_stale += usize::from(!fresh);
                } else if outage_secs > 0 && elapsed >= outage_end {
                    recovery_fresh += usize::from(fresh);
                }
            }
            (
                samples,
                orders_ok,
                account_ok,
                outage_fresh,
                outage_stale,
                recovery_fresh,
            )
        }));
    }
    let mut samples = Vec::new();
    let mut orders_ok = 0;
    let mut account_ok = 0;
    let mut outage_fresh = 0;
    let mut outage_stale = 0;
    let mut recovery_fresh = 0;
    for result in join_all(tasks).await {
        let (cycle_samples, orders, account, fresh, stale, recovered) = result.unwrap();
        samples.extend(cycle_samples);
        orders_ok += orders;
        account_ok += account;
        outage_fresh += fresh;
        outage_stale += stale;
        recovery_fresh += recovered;
    }
    let (emergency_list_ok, emergency_cancel_ok) = emergency.await.unwrap();
    server.abort();
    let cycles = samples.len();
    assert!(cycles > 0 && orders_ok > 0 && account_ok > 0);
    samples.sort_by(f64::total_cmp);
    let percentile = |percent: usize| samples[(cycles - 1) * percent / 100];
    let marks = mark_calls.load(Ordering::Relaxed);
    let bulk_marks = bulk_mark_calls.load(Ordering::Relaxed);
    if outage_secs == 0 {
        assert_eq!(marks, 0, "fresh pushed marks must not trigger REST");
    } else {
        assert!(marks > 0 && recovery_fresh > 0);
        if fail_mark && outage_secs >= 18 {
            assert!(outage_stale > 0, "expired marks must stop trading");
        }
    }
    let orders = order_calls.load(Ordering::Relaxed);
    let tickers = ticker_calls.load(Ordering::Relaxed);
    let accounts = account_calls.load(Ordering::Relaxed);
    let positions = position_calls.load(Ordering::Relaxed);
    assert_eq!(
        emergency_list_calls.load(Ordering::Relaxed),
        usize::from(emergency_list_ok)
    );
    assert_eq!(
        emergency_cancel_calls.load(Ordering::Relaxed),
        usize::from(emergency_cancel_ok)
    );
    println!(
        "pairs={pairs} seconds={seconds} outage_secs={outage_secs} fail_mark={fail_mark} cycles={cycles} orders_ok={orders_ok} account_position_ok={account_ok} mark_sent={marks} mark_bulk_sent={bulk_marks} ticker_sent={tickers} orders_sent={orders} account_sent={accounts} position_sent={positions} outage_fresh={outage_fresh} outage_stale={outage_stale} recovery_fresh={recovery_fresh} emergency_list_ok={emergency_list_ok} emergency_cancel_ok={emergency_cancel_ok} estimated_weight={} cycle_ms_p50={:.2} cycle_ms_p95={:.2} cycle_ms_p99={:.2}",
        marks + 9 * bulk_marks + tickers + orders + 5 * (accounts + positions),
        percentile(50),
        percentile(95),
        percentile(99)
    );
}
