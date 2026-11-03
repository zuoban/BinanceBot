use super::*;
use crate::{config::AppConfig, db::Database};
use axum::{extract::State, routing::get, Json, Router};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{oneshot, Semaphore};

#[derive(Clone)]
struct MockMarket {
    requests: Arc<AtomicUsize>,
    started: Arc<std::sync::Mutex<Option<oneshot::Sender<Instant>>>>,
    release: Arc<Semaphore>,
    delay: Duration,
    served: mpsc::UnboundedSender<Duration>,
}

async fn mark_price(State(mock): State<MockMarket>) -> Json<serde_json::Value> {
    let started = Instant::now();
    let number = mock.requests.fetch_add(1, Ordering::SeqCst);
    if number == 0 {
        mock.started
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(started)
            .unwrap();
        mock.release.acquire().await.unwrap().forget();
    }
    tokio::time::sleep(mock.delay).await;
    mock.served.send(started.elapsed()).unwrap();
    Json(json!({"symbol": "BTCUSDT", "markPrice": "100"}))
}

fn parameter(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .map(|value| value.parse().expect("invalid benchmark parameter"))
        .unwrap_or(default)
}

fn report(label: &str, samples: &mut [f64]) {
    samples.sort_by(f64::total_cmp);
    let percentile = |p: usize| samples[(samples.len() * p).div_ceil(100) - 1];
    println!(
        "{label}: n={} p50={:.3} p95={:.3} p99={:.3} max={:.3} ms",
        samples.len(),
        percentile(50),
        percentile(95),
        percentile(99),
        samples[samples.len() - 1]
    );
}

// Run explicitly with --ignored --nocapture. This measures a paused paper engine
// waiting for market fallback, not exchange order execution or cancellation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit local event-loop network latency benchmark"]
async fn control_during_market_request() {
    let rounds = parameter("BENCH_LOOP_ROUNDS", 10);
    let delay_ms = parameter("BENCH_LOOP_DELAY_MS", 100);
    let timeout_ms = parameter("BENCH_LOOP_TIMEOUT_MS", 10000);
    // The exchange HTTP client has a fixed 10-second request timeout; delays
    // near that limit can still time out due to scheduling and handshake time.
    assert!(
        delay_ms < 10_000,
        "BENCH_LOOP_DELAY_MS must be less than the client's 10-second timeout"
    );
    assert!(rounds > 0 && timeout_ms > delay_ms && timeout_ms > 1000);
    let timeout = Duration::from_millis(timeout_ms);
    let mut control_samples = Vec::new();
    let mut service_samples = Vec::new();
    let mut update_samples = Vec::new();
    for _ in 0..rounds {
        let (started_tx, started_rx) = oneshot::channel();
        let (served_tx, mut served_rx) = mpsc::unbounded_channel();
        let mock = MockMarket {
            requests: Arc::new(AtomicUsize::new(0)),
            started: Arc::new(std::sync::Mutex::new(Some(started_tx))),
            release: Arc::new(Semaphore::new(0)),
            delay: Duration::from_millis(delay_ms),
            served: served_tx,
        };
        let app = Router::new()
            .route("/fapi/v1/premiumIndex", get(mark_price))
            .with_state(mock.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (server_stop, server_stopped) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = server_stopped.await;
                })
                .await
                .unwrap();
        });
        let db = Arc::new(Database::open(":memory:").unwrap());
        let mut config = AppConfig::default();
        config.exchange.symbol = "BTCUSDT".into();
        config.exchange.sync_interval_secs = 1;
        let mut stored_config = config.clone();
        stored_config.pairs = vec![crate::config::TradingPairConfig::new(
            config.exchange.symbol.clone(),
        )];
        db.save_config(&stored_config)
            .expect("persist benchmark configuration");
        let (action_tx, action_rx) = mpsc::channel(8);
        let (ticker_tx, ticker_rx) = broadcast::channel(8);
        let state = AppState::new_for_pair(
            config.clone(),
            db,
            action_tx.clone(),
            Arc::new(tokio::sync::Mutex::new(())),
        );
        *state.status.write().await = BotStatus::Paused;
        let previous_mark_time = {
            let mut ticker = state.ticker.write().await;
            ticker.update_time = Utc::now();
            ticker.mark_update_time
        };
        let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
        let engine_task = tokio::spawn(async move { engine.run().await });
        let _request_started = tokio::time::timeout(timeout, started_rx)
            .await
            .expect("engine did not request local mark price")
            .expect("request notification dropped");
        let permit = action_tx.reserve().await.unwrap();
        let (reply_tx, reply_rx) = oneshot::channel();
        let enqueued = Instant::now();
        permit.send(BotControlAction::UpdateConfig {
            config: Box::new(config),
            reply: reply_tx,
        });
        // The handler cannot respond before the command has been queued.
        mock.release.add_permits(1);
        let result = tokio::time::timeout(timeout, reply_rx)
            .await
            .expect("control timed out")
            .expect("control reply dropped");
        control_samples.push(enqueued.elapsed().as_secs_f64() * 1000.0);
        result.expect("configuration update failed");
        {
            let ticker = state.ticker.read().await;
            assert_eq!(
                ticker.mark_price,
                Decimal::from(100),
                "engine fallback was not applied"
            );
            assert!(
                ticker.mark_update_time > previous_mark_time,
                "engine fallback timestamp was not updated"
            );
        }

        let mark_update_time = Utc::now();
        let update_started = Instant::now();
        ticker_tx
            .send(TickerInfo {
                symbol: "BTCUSDT".into(),
                mark_price: Decimal::from(101),
                mark_update_time,
                ..Default::default()
            })
            .expect("engine ticker receiver dropped");
        tokio::time::timeout(timeout, async {
            loop {
                {
                    let ticker = state.ticker.read().await;
                    if ticker.mark_price == Decimal::from(101)
                        && ticker.mark_update_time == mark_update_time
                    {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("engine did not apply mark price update");
        update_samples.push(update_started.elapsed().as_secs_f64() * 1000.0);

        let service = tokio::time::timeout(timeout, served_rx.recv())
            .await
            .expect("mock service timed out")
            .expect("mock service notification dropped");
        service_samples.push(service.as_secs_f64() * 1000.0);
        assert!(mock.requests.load(Ordering::SeqCst) >= 1);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        action_tx
            .send(BotControlAction::Shutdown {
                removing: false,
                reply: shutdown_tx,
            })
            .await
            .unwrap();
        tokio::time::timeout(timeout, shutdown_rx)
            .await
            .expect("shutdown timed out")
            .expect("shutdown reply dropped")
            .expect("shutdown failed");
        tokio::time::timeout(timeout, engine_task)
            .await
            .expect("engine did not stop")
            .unwrap();
        drop(ticker_tx);
        server_stop.send(()).unwrap();
        tokio::time::timeout(timeout, server)
            .await
            .expect("server did not stop")
            .unwrap();
    }
    println!("rounds={rounds} delay_ms={delay_ms} timeout_ms={timeout_ms} db=memory mode=paper workers=4");
    println!("UpdateConfig acknowledgement is configuration completion, not order cancellation; no market-update loss is measured.");
    println!("Mark update latency runs from broadcast send to observing engine state; mock service time includes the release handshake. Small samples give coarse percentiles.");
    report("control_enqueue_to_ack", &mut control_samples);
    report("mark_update_send_to_observation", &mut update_samples);
    report("mock_handler_service", &mut service_samples);
}
