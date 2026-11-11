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

// The directory owns the database and its SQLite sidecars; it is removed after
// all engines and database handles have been dropped, including during unwinding.
struct BenchmarkDatabase {
    directory: Option<crate::test_support::TempTestDir>,
}

impl BenchmarkDatabase {
    fn new(disk: bool) -> Self {
        Self {
            directory: disk.then(|| crate::test_support::TempTestDir::new("binancebot-benchmark")),
        }
    }

    fn open(&self) -> Database {
        match &self.directory {
            Some(directory) => Database::open(directory.join("benchmark.sqlite")).unwrap(),
            None => Database::open(":memory:").unwrap(),
        }
    }
}

fn database_mode() -> bool {
    match std::env::var("BENCH_DB").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("memory") => false,
        Ok("disk") => true,
        _ => panic!("BENCH_DB must be memory or disk"),
    }
}

#[test]
fn benchmark_disk_database_is_isolated_and_cleaned_up() {
    let directory;
    {
        let owner = BenchmarkDatabase::new(true);
        directory = owner.directory.as_ref().unwrap().path().to_path_buf();
        let db = owner.open();
        assert!(directory.join("benchmark.sqlite").exists());
        drop(db);
    }
    assert!(!directory.exists());
}

// This is a burst workload, not a production-capacity or exchange-limit test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit local capacity benchmark"]
async fn simulated_pair_burst() {
    let pairs = parameter("BENCH_PAIRS", 50);
    let rounds = parameter("BENCH_ROUNDS", 6);
    let buy_window = parameter("BENCH_BUY_WINDOW", 2);
    let sell_window = parameter("BENCH_SELL_WINDOW", 2);
    assert!(pairs > 0 && rounds >= 2);
    assert!((1..=50).contains(&buy_window) && (1..=50).contains(&sell_window));
    let disk = database_mode();
    let database = BenchmarkDatabase::new(disk);
    let db = Arc::new(database.open());
    let trade_lock = Arc::new(tokio::sync::Mutex::new(()));
    let mut engines = Vec::with_capacity(pairs);
    let start = Instant::now();
    for i in 0..pairs {
        let mut config = AppConfig::default();
        config.exchange.symbol = format!("BENCH{i}USDT");
        config.grid.grid_interval = dec!(1);
        config.grid.buy_window = buy_window;
        config.grid.sell_window = sell_window;
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
    let mut ticker_ms = Vec::with_capacity(pairs * rounds);
    let mut maintenance_ms = Vec::with_capacity(pairs * rounds);
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
                let ticker_start = Instant::now();
                engine.handle_ticker_update(update).await;
                let ticker_elapsed = ticker_start.elapsed().as_secs_f64() * 1000.0;
                let maintenance_start = Instant::now();
                engine.maintain_grid_window().await;
                let maintenance_elapsed = maintenance_start.elapsed().as_secs_f64() * 1000.0;
                (
                    engine,
                    start.elapsed().as_secs_f64() * 1000.0,
                    ticker_elapsed,
                    maintenance_elapsed,
                )
            })
        });
        for result in join_all(tasks).await {
            let (engine, elapsed, ticker_elapsed, maintenance_elapsed) = result.unwrap();
            engines.push(engine);
            samples.push(elapsed);
            ticker_ms.push(ticker_elapsed);
            maintenance_ms.push(maintenance_elapsed);
        }
        batch_ms.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    let start = Instant::now();
    let mut full_snapshot_ms = Vec::with_capacity(pairs);
    let snapshots = join_all(engines.iter().map(|engine| async {
        let start = Instant::now();
        let snapshot = engine.state.snapshot().await;
        (snapshot, start.elapsed().as_secs_f64() * 1000.0)
    }))
    .await;
    let full_snapshot_batch_ms = start.elapsed().as_secs_f64() * 1000.0;
    let mut fills = 0;
    let mut active_orders = 0;
    for (snapshot, elapsed) in snapshots {
        fills += snapshot.stats.total_trades;
        active_orders += snapshot.active_orders.len();
        full_snapshot_ms.push(elapsed);
    }
    let start = Instant::now();
    let mut account_snapshot_ms = join_all(engines.iter().map(|engine| async {
        let start = Instant::now();
        let _ = engine.state.account_snapshot().await;
        start.elapsed().as_secs_f64() * 1000.0
    }))
    .await;
    let account_snapshot_batch_ms = start.elapsed().as_secs_f64() * 1000.0;
    assert!(fills > 0, "benchmark must exercise simulated fills");
    assert!(active_orders > 0, "benchmark must retain active orders");
    println!("pairs={pairs} rounds={rounds} buy_window={buy_window} sell_window={sell_window} db={} setup_ms={setup_ms:.1} fills={fills} active_orders={active_orders} full_snapshot_batch_ms={full_snapshot_batch_ms:.3} account_snapshot_batch_ms={account_snapshot_batch_ms:.3}", if disk { "disk" } else { "memory" });
    println!(
        "nearest-rank percentiles; p95/p99 are coarse for fewer than 20/100 samples; times in ms"
    );
    crate::test_support::print_latency_percentiles("pair_total_ms", &mut samples);
    crate::test_support::print_latency_percentiles("ticker_ms", &mut ticker_ms);
    crate::test_support::print_latency_percentiles("maintenance_ms", &mut maintenance_ms);
    crate::test_support::print_latency_percentiles("batch_ms", &mut batch_ms);
    crate::test_support::print_latency_percentiles("full_snapshot_ms", &mut full_snapshot_ms);
    crate::test_support::print_latency_percentiles("account_snapshot_ms", &mut account_snapshot_ms);
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
    let disk = database_mode();
    let database = BenchmarkDatabase::new(disk);
    let db = Arc::new(database.open());
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
        let mut engine = GridTradingEngine::new_with_caches(
            state,
            client,
            action_rx,
            ticker_rx,
            account_cache.clone(),
            position_cache.clone(),
            market_cache.clone(),
        );
        tasks.push(tokio::spawn(async move {
            let offset = Duration::from_millis((i as u64 * 3000) / pairs as u64);
            let period = Duration::from_secs(3);
            let planned = usize::try_from(
                deadline
                    .saturating_duration_since(start + offset)
                    .as_nanos()
                    .div_ceil(period.as_nanos()),
            )
            .expect("planned cycle count exceeds usize");
            let mut timer = tokio::time::interval_at(start + offset, period);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut samples = Vec::new();
            let mut schedule_ms = Vec::new();
            let mut ticker_ms = Vec::new();
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
                schedule_ms.push(
                    tokio::time::Instant::now()
                        .duration_since(scheduled)
                        .as_secs_f64()
                        * 1000.0,
                );
                let elapsed = start.elapsed();
                let in_outage = outage_secs > 0 && elapsed >= outage_start && elapsed < outage_end;
                if !in_outage {
                    let ticker_start = Instant::now();
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
                    ticker_ms.push(ticker_start.elapsed().as_secs_f64() * 1000.0);
                }
                engine.last_orders_sync = None;
                engine.last_account_sync = None;
                let cycle_start = Instant::now();
                engine.sync_cycle(true).await;
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
                planned,
                samples,
                schedule_ms,
                ticker_ms,
                orders_ok,
                account_ok,
                outage_fresh,
                outage_stale,
                recovery_fresh,
            )
        }));
    }
    let mut samples = Vec::new();
    let mut schedule_ms = Vec::new();
    let mut ticker_ms = Vec::new();
    let mut planned = 0;
    let mut orders_ok = 0;
    let mut account_ok = 0;
    let mut outage_fresh = 0;
    let mut outage_stale = 0;
    let mut recovery_fresh = 0;
    for result in join_all(tasks).await {
        let (
            pair_planned,
            cycle_samples,
            schedule_samples,
            ticker_samples,
            orders,
            account,
            fresh,
            stale,
            recovered,
        ) = result.unwrap();
        planned += pair_planned;
        samples.extend(cycle_samples);
        schedule_ms.extend(schedule_samples);
        ticker_ms.extend(ticker_samples);
        orders_ok += orders;
        account_ok += account;
        outage_fresh += fresh;
        outage_stale += stale;
        recovery_fresh += recovered;
    }
    let (emergency_list_ok, emergency_cancel_ok) = emergency.await.unwrap();
    server.abort();
    let cycles = samples.len();
    let skipped = planned - cycles;
    assert!(cycles > 0 && orders_ok > 0 && account_ok > 0);
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
        "pairs={pairs} seconds={seconds} db={} outage_secs={outage_secs} fail_mark={fail_mark} planned_cycles={planned} cycles={cycles} skipped_cycles={skipped} orders_ok={orders_ok} orders_not_ok={} account_position_ok={account_ok} account_position_not_ok={} mark_sent={marks} mark_bulk_sent={bulk_marks} ticker_sent={tickers} orders_sent={orders} account_sent={accounts} position_sent={positions} outage_fresh={outage_fresh} outage_stale={outage_stale} recovery_fresh={recovery_fresh} emergency_list_ok={emergency_list_ok} emergency_cancel_ok={emergency_cancel_ok} estimated_weight={}",
        if disk { "disk" } else { "memory" }, cycles - orders_ok, cycles - account_ok,
        marks + 9 * bulk_marks + tickers + orders + 5 * (accounts + positions),
    );
    println!("planned cycles have scheduled times in [start, deadline); skipped cycles are planned cycles not started before deadline, including ticks skipped during an overrun or remaining at deadline");
    println!("nearest-rank percentiles; p95/p99 are coarse for fewer than 20/100 samples; times in ms; not_ok means sync did not report success, not necessarily a rate-limit rejection");
    crate::test_support::print_latency_percentiles("schedule_delay_ms", &mut schedule_ms);
    crate::test_support::print_latency_percentiles("ticker_ms", &mut ticker_ms);
    crate::test_support::print_latency_percentiles("sync_cycle_ms", &mut samples);
}
