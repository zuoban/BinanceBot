use super::*;
use crate::exchange::rate_limit::RateLimiter;
use crate::{config::AppConfig, db::Database};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use futures_util::future::join_all;
use serde_json::json;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};

// Each stage gets its own subscriber so concurrent pairs cannot mix their logs.
#[derive(Clone, Default)]
struct DiagnosticBuffer(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for DiagnosticBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for DiagnosticBuffer {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

async fn diagnosed<F: Future<Output = bool>>(future: F) -> (bool, String) {
    use tracing::instrument::WithSubscriber;
    let buffer = DiagnosticBuffer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_max_level(tracing::Level::WARN)
        .with_writer(buffer.clone())
        .finish();
    let success = future.with_subscriber(subscriber).await;
    let logs = String::from_utf8_lossy(&buffer.0.lock().unwrap()).into_owned();
    (success, logs)
}

fn rejection_counts(logs: &str) -> (usize, usize, usize) {
    let mut counts = (0, 0, 0);
    for line in logs
        .lines()
        .filter(|line| line.contains("::exchange::client:"))
    {
        if line.contains("local Binance IP request-weight budget reached") {
            counts.0 += 1;
        } else if line.contains("local Binance order-rate budget reached") {
            counts.1 += 1;
        } else if line.contains("Binance rate-limit cooldown is active") {
            counts.2 += 1;
        }
    }
    counts
}

fn parameter(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().expect("invalid benchmark parameter"))
        .unwrap_or(default)
}

// Only directories created by this owner are eligible for cleanup, including SQLite sidecars.
struct BenchmarkDatabase(Option<crate::test_support::TempTestDir>);

impl BenchmarkDatabase {
    fn new(disk: bool) -> Self {
        Self(disk.then(|| crate::test_support::TempTestDir::new("binancebot-orders-bench")))
    }

    fn open(&self) -> Database {
        match &self.0 {
            Some(directory) => Database::open(directory.join("benchmark.sqlite")).unwrap(),
            None => Database::open(":memory:").unwrap(),
        }
    }
}

#[derive(Clone)]
struct Exchange {
    pairs: usize,
    orders: usize,
    fills_per_round: usize,
    round: Arc<AtomicUsize>,
    open_calls: Arc<AtomicUsize>,
    lookup_calls: Arc<AtomicUsize>,
    trade_calls: Arc<AtomicUsize>,
    position_calls: Arc<AtomicUsize>,
    all_position_calls: Arc<AtomicUsize>,
    account_calls: Arc<AtomicUsize>,
    delays: ExchangeDelays,
}

// Delay is server-side time before producing a response, not measured network RTT.
#[derive(Clone, Copy)]
struct ExchangeDelays {
    open: std::time::Duration,
    lookup: std::time::Duration,
    trade: std::time::Duration,
    position: std::time::Duration,
    account: std::time::Duration,
}

impl ExchangeDelays {
    fn from_env() -> Self {
        let delay = |name| std::time::Duration::from_millis(parameter(name, 0) as u64);
        Self {
            open: delay("BENCH_ORDERS_DELAY_OPEN_MS"),
            lookup: delay("BENCH_ORDERS_DELAY_LOOKUP_MS"),
            trade: delay("BENCH_ORDERS_DELAY_TRADE_MS"),
            position: delay("BENCH_ORDERS_DELAY_POSITION_MS"),
            account: delay("BENCH_ORDERS_DELAY_ACCOUNT_MS"),
        }
    }

    async fn wait(delay: std::time::Duration) {
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
}
impl Exchange {
    fn pair(&self, symbol: &str) -> Option<usize> {
        let index = symbol
            .strip_prefix("BENCH")?
            .strip_suffix("USDT")?
            .parse::<usize>()
            .ok()?;
        (index < self.pairs).then_some(index)
    }
    fn filled(&self) -> usize {
        self.round.load(Ordering::SeqCst) * self.fills_per_round
    }
    fn order(&self, pair: usize, index: usize, filled: bool) -> BinanceOrderResponse {
        BinanceOrderResponse {
            order_id: (pair * self.orders + index + 1) as i64,
            client_order_id: format!("gb_b_{pair}_{index}"),
            symbol: format!("BENCH{pair}USDT"),
            status: if filled { "FILLED" } else { "NEW" }.into(),
            price: Decimal::from(100),
            avg_price: filled.then_some(Decimal::from(100)),
            orig_qty: Decimal::ONE,
            executed_qty: if filled { Decimal::ONE } else { Decimal::ZERO },
            side: "BUY".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTC".into(),
            update_time: Some(1_700_000_000_000 + index as i64),
        }
    }
}

// Run explicitly with --ignored --nocapture; all HTTP targets are the listener below.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit local live order reconciliation benchmark"]
async fn simulated_live_orders_and_fills() {
    let pairs = parameter("BENCH_ORDERS_PAIRS", 4);
    let orders = parameter("BENCH_ORDERS_PER_PAIR", 8);
    let rounds = parameter("BENCH_ORDERS_ROUNDS", 3);
    let fills_per_round = parameter("BENCH_ORDERS_FILLS_PER_ROUND", 1);
    assert!(pairs > 0 && rounds > 0 && fills_per_round > 0);
    let fills = rounds
        .checked_mul(fills_per_round)
        .expect("fill count overflow");
    assert!(
        fills < orders,
        "each pair must retain at least one active order"
    );
    assert!(pairs
        .checked_mul(orders)
        .is_some_and(|n| n <= i64::MAX as usize));
    let disk = match std::env::var("BENCH_ORDERS_DB").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("memory") => false,
        Ok("disk") => true,
        _ => panic!("BENCH_ORDERS_DB must be memory or disk"),
    };
    let exchange = Exchange {
        pairs,
        orders,
        fills_per_round,
        round: Arc::new(AtomicUsize::new(0)),
        open_calls: Arc::new(AtomicUsize::new(0)),
        lookup_calls: Arc::new(AtomicUsize::new(0)),
        trade_calls: Arc::new(AtomicUsize::new(0)),
        position_calls: Arc::new(AtomicUsize::new(0)),
        all_position_calls: Arc::new(AtomicUsize::new(0)),
        account_calls: Arc::new(AtomicUsize::new(0)),
        delays: ExchangeDelays::from_env(),
    };
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|State(fake): State<Exchange>, Query(query): Query<HashMap<String, String>>| async move {
            fake.open_calls.fetch_add(1, Ordering::Relaxed);
            ExchangeDelays::wait(fake.delays.open).await;
            let pair = fake.pair(query.get("symbol").map(String::as_str).unwrap_or("")).unwrap();
            Json((fake.filled()..fake.orders).map(|index| fake.order(pair, index, false)).collect::<Vec<_>>())
        }))
        .route("/fapi/v1/order", get(|State(fake): State<Exchange>, Query(query): Query<HashMap<String, String>>| async move {
            fake.lookup_calls.fetch_add(1, Ordering::Relaxed);
            ExchangeDelays::wait(fake.delays.lookup).await;
            let found = fake.pair(query.get("symbol").map(String::as_str).unwrap_or("")).and_then(|pair| {
                let id = query.get("orderId")?.parse::<usize>().ok()?;
                let index = id.checked_sub(pair * fake.orders + 1)?;
                (index < fake.filled()).then(|| fake.order(pair, index, true))
            });
            match found {
                Some(order) => (StatusCode::OK, Json(serde_json::to_value(order).unwrap())),
                None => (StatusCode::BAD_REQUEST, Json(json!({"code": -2013, "msg": "Order does not exist"}))),
            }
        }))
        .route("/fapi/v1/userTrades", get(|State(fake): State<Exchange>, Query(query): Query<HashMap<String, String>>| async move {
            fake.trade_calls.fetch_add(1, Ordering::Relaxed);
            ExchangeDelays::wait(fake.delays.trade).await;
            let order_id = query.get("orderId").unwrap().parse::<i64>().unwrap();
            Json(json!([{"id":1, "orderId":order_id, "qty":"1", "realizedPnl":"0", "commission":"0.01", "commissionAsset":"USDT", "maker":true}]))
        }))
        .route("/fapi/v2/positionRisk", get(|State(fake): State<Exchange>, Query(query): Query<HashMap<String, String>>| async move {
            fake.position_calls.fetch_add(1, Ordering::Relaxed);
            ExchangeDelays::wait(fake.delays.position).await;
            let pairs = if let Some(symbol) = query.get("symbol") {
                vec![fake.pair(symbol).unwrap()]
            } else {
                fake.all_position_calls.fetch_add(1, Ordering::Relaxed);
                (0..fake.pairs).collect()
            };
            let filled = fake.filled();
            Json(pairs.into_iter().map(|pair| json!({"symbol":format!("BENCH{pair}USDT"), "positionSide":"BOTH", "positionAmt":filled.to_string(),
                "entryPrice":if filled == 0 {"0"} else {"100"}, "markPrice":"100", "unRealizedProfit":"0", "liquidationPrice":"0", "leverage":"20"})).collect::<Vec<_>>())
        }))
        .route("/fapi/v2/account", get(|State(fake): State<Exchange>| async move {
            fake.account_calls.fetch_add(1, Ordering::Relaxed);
            ExchangeDelays::wait(fake.delays.account).await;
            let filled = Decimal::from((fake.pairs * fake.filled()) as u64);
            let wallet = Decimal::from(10000) - filled * Decimal::new(1, 2);
            let available = wallet - filled * Decimal::from(5);
            Json(json!({"totalWalletBalance":wallet.to_string(), "totalMarginBalance":wallet.to_string(), "totalUnrealizedProfit":"0", "availableBalance":available.to_string(), "assets":[{"asset":"USDT", "walletBalance":wallet.to_string(), "marginBalance":wallet.to_string(), "availableBalance":available.to_string(), "unrealizedProfit":"0"}]}))
        }))
        .with_state(exchange.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let database = BenchmarkDatabase::new(disk);
    let db = Arc::new(database.open());
    let trade_lock = Arc::new(tokio::sync::Mutex::new(()));
    let limiter = Arc::new(RateLimiter::default());
    let account_cache = Arc::new(AccountCache::default());
    let position_cache = Arc::new(PositionCache::default());
    let mut engines = Vec::with_capacity(pairs);
    for pair in 0..pairs {
        let mut config = AppConfig::default();
        config.exchange.symbol = format!("BENCH{pair}USDT");
        config.exchange.api_key = "benchmark-key".into();
        config.exchange.api_secret = "benchmark-secret".into();
        let (tx, rx) = mpsc::channel(8);
        let (_, ticks) = broadcast::channel(8);
        let state = AppState::new_for_pair(config.clone(), db.clone(), tx, trade_lock.clone());
        *state.status.write().await = BotStatus::Paused;
        let client = Arc::new(BinanceFuturesClient::with_base_url_and_limiter(
            &config.exchange,
            url.clone(),
            limiter.clone(),
        ));
        let engine = GridTradingEngine::new_with_caches(
            state,
            client,
            rx,
            ticks,
            account_cache.clone(),
            position_cache.clone(),
            Arc::new(MarketCache::default()),
        );
        engines.push(engine);
    }
    // Exclude setup and final validation reads from the measured window.
    let db_wait_samples = db.enable_benchmark_wait_samples();
    let mut order_ms = Vec::new();
    let mut account_ms = Vec::new();
    let mut pair_ms = Vec::new();
    let mut batch_ms = Vec::new();
    let mut orders_ok = 0;
    let mut account_ok = 0;
    let mut order_success_ms = Vec::new();
    let mut order_failed_ms = Vec::new();
    let mut account_success_ms = Vec::new();
    let mut account_failed_ms = Vec::new();
    let mut rejections = (0, 0, 0);
    let mut initial_critical_position_requests = 0;
    // Round zero adopts all open orders; later rounds each close a new tranche.
    for round in 0..=rounds {
        exchange.round.store(round, Ordering::SeqCst);
        let batch_start = Instant::now();
        let tasks = engines.drain(..).map(|mut engine| {
            tokio::spawn(async move {
                let pair_start = Instant::now();
                let start = Instant::now();
                let (order_ok, order_logs) = diagnosed(
                    engine.sync_live_orders_with_priority(ReconciliationPriority::Periodic),
                )
                .await;
                let order_elapsed = start.elapsed().as_secs_f64() * 1000.0;
                let start = Instant::now();
                let (account_success, account_logs) =
                    diagnosed(engine.sync_account_and_position_inner(true)).await;
                let account_elapsed = start.elapsed().as_secs_f64() * 1000.0;
                (
                    engine,
                    order_ok,
                    account_success,
                    order_elapsed,
                    account_elapsed,
                    pair_start.elapsed().as_secs_f64() * 1000.0,
                    order_logs,
                    account_logs,
                )
            })
        });
        let results = join_all(tasks).await;
        batch_ms.push(batch_start.elapsed().as_secs_f64() * 1000.0);
        for result in results {
            let (
                engine,
                order_ok,
                account_success,
                order_elapsed,
                account_elapsed,
                pair_elapsed,
                order_logs,
                account_logs,
            ) = result.expect("pair benchmark task failed");
            let symbol = engine.state.config.read().await.exchange.symbol.clone();
            for (stage, ok, logs) in [
                ("order", order_ok, &order_logs),
                ("account_position", account_success, &account_logs),
            ] {
                let counts = rejection_counts(logs);
                rejections.0 += counts.0;
                rejections.1 += counts.1;
                rejections.2 += counts.2;
                if !ok {
                    println!(
                        "sync_failure round={round} symbol={symbol} stage={stage} reason={}",
                        if logs.is_empty() {
                            "unknown (no warning/error emitted)"
                        } else {
                            "see stage logs; cause may be unknown"
                        }
                    );
                }
                if !logs.is_empty() {
                    println!("stage_logs round={round} symbol={symbol} stage={stage} success={ok}\n{logs}");
                }
            }
            if order_ok {
                order_success_ms.push(order_elapsed);
            } else {
                order_failed_ms.push(order_elapsed);
            }
            if account_success {
                account_success_ms.push(account_elapsed);
            } else {
                account_failed_ms.push(account_elapsed);
            }
            orders_ok += usize::from(order_ok);
            account_ok += usize::from(account_success);
            order_ms.push(order_elapsed);
            account_ms.push(account_elapsed);
            pair_ms.push(pair_elapsed);
            engines.push(engine);
        }
        if round == 0 {
            initial_critical_position_requests = exchange.position_calls.load(Ordering::Relaxed)
                - exchange.all_position_calls.load(Ordering::Relaxed);
        }
    }
    let cycles = pairs * (rounds + 1);
    println!("sync_results cycles={cycles} order_success={orders_ok} order_failure={} account_success={account_ok} account_failure={} local_rejections_ip_weight={} local_rejections_order_rate={} local_rejections_cooldown={}", cycles - orders_ok, cycles - account_ok, rejections.0, rejections.1, rejections.2);
    println!(
        "received_requests open={} lookup={} trade={} position={} all_position={} account={}",
        exchange.open_calls.load(Ordering::Relaxed),
        exchange.lookup_calls.load(Ordering::Relaxed),
        exchange.trade_calls.load(Ordering::Relaxed),
        exchange.position_calls.load(Ordering::Relaxed),
        exchange.all_position_calls.load(Ordering::Relaxed),
        exchange.account_calls.load(Ordering::Relaxed)
    );
    println!("mock_server_delay_ms open={} lookup={} trade={} position={} account={}; delay starts in each local handler before response generation and is not network RTT", exchange.delays.open.as_millis(), exchange.delays.lookup.as_millis(), exchange.delays.trade.as_millis(), exchange.delays.position.as_millis(), exchange.delays.account.as_millis());
    println!("SQLite timings cover benchmark rounds only: queue is submit-to-start for Tokio spawn_blocking; connection is mutex acquisition for instrumented methods, excluding SQL, SQLite file locks, and the outer trade lock. Sample recording itself adds overhead.");
    crate::test_support::print_latency_percentiles(
        "sqlite_blocking_queue_wait_ms",
        &mut db_wait_samples.queue.lock().unwrap(),
    );
    crate::test_support::print_latency_percentiles(
        "sqlite_instrumented_connection_lock_wait_ms",
        &mut db_wait_samples.connection.lock().unwrap(),
    );
    println!("Rejections count client reservation warnings, not failed stages or server responses; shared caches and multiple requests per stage prevent one-to-one attribution. Ordinary cache waits can fail after 100ms while a loader continues; slow-handler runs therefore report account stage failures. Round 0 is adoption; latency includes diagnostics and failed stages may finish early. Batch timing ends when all pair tasks finish, before result processing. Order reconciliation includes one critical risk refresh per pair with terminal fills and durable pending-PnL commits. PnL compensation is measured separately below. Local synthetic burst is not an exchange capacity estimate. Nearest-rank p95/p99 need 20/100 samples for useful tail resolution.");
    crate::test_support::print_latency_percentiles("order_success_ms", &mut order_success_ms);
    crate::test_support::print_latency_percentiles("order_failure_ms", &mut order_failed_ms);
    crate::test_support::print_latency_percentiles("account_success_ms", &mut account_success_ms);
    crate::test_support::print_latency_percentiles("account_failure_ms", &mut account_failed_ms);
    crate::test_support::print_latency_percentiles("order_reconciliation_ms", &mut order_ms);
    crate::test_support::print_latency_percentiles("account_position_ms", &mut account_ms);
    crate::test_support::print_latency_percentiles("pair_total_ms", &mut pair_ms);
    crate::test_support::print_latency_percentiles("batch_ms", &mut batch_ms);
    let pending_fills: usize = futures_util::future::join_all(
        engines
            .iter()
            .map(|engine| async { engine.state.stats.read().await.pending_pnl_trades }),
    )
    .await
    .into_iter()
    .sum();
    assert_eq!(pending_fills, pairs * fills);
    let pnl_started = Instant::now();
    futures_util::future::join_all(engines.iter_mut().map(|engine| async move {
        while engine.state.stats.read().await.pending_pnl_trades > 0 {
            let before = engine.state.stats.read().await.pending_pnl_trades;
            engine.reconcile_unverified_trades().await;
            if let Some(task) = engine.pnl_reconcile_task.take() {
                let retry_at = task.await.expect("PnL compensation task failed");
                if retry_at.is_some() {
                    engine.pnl_retry_at = retry_at;
                    break;
                }
            }
            assert!(
                engine.state.stats.read().await.pending_pnl_trades < before,
                "PnL compensation made no progress"
            );
        }
    }))
    .await;
    let pending_after: usize = futures_util::future::join_all(
        engines
            .iter()
            .map(|engine| async { engine.state.stats.read().await.pending_pnl_trades }),
    )
    .await
    .into_iter()
    .sum();
    println!(
        "pnl_compensation_ms={:.3} pending_before={pending_fills} pending_after={pending_after}; compensation stops at the shared quota and leaves durable pending rows for a later window",
        pnl_started.elapsed().as_secs_f64() * 1000.0
    );
    let mut active = 0;
    let mut persisted_fills = 0;
    let mut verified_fills = 0;
    for engine in &engines {
        let symbol = engine.state.config.read().await.exchange.symbol.clone();
        assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
        let live = engine.state.active_orders.read().await;
        assert_eq!(live.len(), orders - fills);
        assert_eq!(db.load_managed_orders(&symbol).unwrap().len(), live.len());
        active += live.len();
        assert_eq!(
            engine.state.position.read().await.size,
            Decimal::from(fills as u64)
        );
        let trades = db
            .get_recent_trades_for(&symbol, TradingMode::Live, fills + 1)
            .unwrap();
        assert_eq!(trades.len(), fills);
        let verified = trades.iter().filter(|trade| trade.pnl_verified).count();
        assert_eq!(
            engine.state.stats.read().await.pending_pnl_trades,
            trades.len() - verified
        );
        assert!(trades
            .iter()
            .filter(|trade| trade.pnl_verified)
            .all(|trade| trade.commission == Decimal::new(1, 2)));
        verified_fills += verified;
        persisted_fills += trades.len();
    }
    let open = exchange.open_calls.load(Ordering::Relaxed);
    let lookups = exchange.lookup_calls.load(Ordering::Relaxed);
    let trade_requests = exchange.trade_calls.load(Ordering::Relaxed);
    let positions = exchange.position_calls.load(Ordering::Relaxed);
    let all_positions = exchange.all_position_calls.load(Ordering::Relaxed);
    let accounts = exchange.account_calls.load(Ordering::Relaxed);
    println!("pairs={pairs} orders_per_pair={orders} rounds={rounds} fills_per_round={fills_per_round} db={} cycles={cycles} order_ok={orders_ok} order_failed={} account_ok={account_ok} account_failed={} persisted_fills={persisted_fills} verified_fills={verified_fills} pending_fills={pending_after} active_orders={active} open_requests={open} lookup_requests={lookups} trade_requests={trade_requests} position_requests={positions} all_position_requests={all_positions} account_requests={accounts}", if disk { "disk" } else { "memory" }, cycles-orders_ok, cycles-account_ok);
    assert_eq!(orders_ok, cycles);
    assert_eq!(open, cycles);
    assert_eq!(lookups, pairs * fills);
    assert_eq!(trade_requests, verified_fills);
    assert_eq!(verified_fills + pending_after, pairs * fills);
    let critical_fill_position_requests =
        positions - all_positions - initial_critical_position_requests;
    // A peer's newly observed fills can invalidate an in-flight critical read.
    // Such retries are required for freshness and are additional HTTP requests.
    println!("critical_position_requests initial={initial_critical_position_requests} terminal_fills={critical_fill_position_requests} required_observations={}", pairs * rounds);
    assert!(
        critical_fill_position_requests >= pairs * rounds,
        "every terminal-fill observation requires a fresh critical position read"
    );
    server.abort();
}
