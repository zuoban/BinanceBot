use super::*;

#[tokio::test]
async fn shared_http_pool_keeps_api_headers_isolated_per_request() {
    use axum::{http::HeaderMap, routing::get, Router};
    let app = Router::new().route(
        "/",
        get(|headers: HeaderMap| async move {
            headers
                .get("X-MBX-APIKEY")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("missing")
                .to_owned()
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let request = |key: &'static str| {
        let url = url.clone();
        async move {
            let mut config = crate::config::AppConfig::default().exchange;
            config.api_key = key.into();
            let client = BinanceFuturesClient::with_base_url(&config, url.clone());
            client
                .send(client.client.get(url))
                .await
                .unwrap()
                .text()
                .await
                .unwrap()
        }
    };
    let (first, second, public) =
        tokio::join!(request("first-key"), request("second-key"), request(""));
    assert_eq!(first, "first-key");
    assert_eq!(second, "second-key");
    assert_eq!(public, "missing");
    server.abort();
}

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
                    progress: Arc::new(tokio::sync::Mutex::new(FillProgress::new(Decimal::ONE))),
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

    // Pending==0 preserves elevated retention while progress/waiters remain, then resets to 600s.
    client.note_pnl_reconcile_backlog(0);
    assert!(client.fill_retention_seconds.load(Ordering::Relaxed) > 600);
    client.fill_progress.lock().unwrap().clear();
    client.fill_waiters.lock().unwrap().clear();
    client.note_pnl_reconcile_backlog(0);
    assert_eq!(client.fill_retention_seconds.load(Ordering::Relaxed), 600);
}

#[tokio::test]
async fn inactive_waiter_is_skipped_but_keeps_its_queue_position() {
    use axum::{extract::Query, routing::get, Json, Router};
    use serde_json::json;
    use std::collections::HashMap;

    let app = Router::new().route(
        "/fapi/v1/userTrades",
        get(|Query(query): Query<HashMap<String, String>>| async move {
            let order_id: i64 = query["orderId"].parse().unwrap();
            Json(json!([{ "id": order_id, "orderId": order_id, "qty": "1", "realizedPnl": "1", "commission": "0.1", "commissionAsset": "USDT", "maker": true }]))
        }),
    );
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
    let app = Router::new().route(
        "/fapi/v1/userTrades",
        get({
            let requests = Arc::clone(&requests);
            move |Query(query): Query<HashMap<String, String>>| {
                let requests = Arc::clone(&requests);
                async move {
                    let order_id: i64 = query["orderId"].parse().unwrap();
                    requests.lock().unwrap().push(query);
                    if order_id == 99 {
                        Json(json!([{ "id": 10, "orderId": order_id, "qty": "1", "realizedPnl": "1", "commission": "0.1", "commissionAsset": "USDT", "maker": true }])).into_response()
                    } else {
                        (StatusCode::INTERNAL_SERVER_ERROR, "upstream failure").into_response()
                    }
                }
            }
        }),
    );
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
            assert!(error.to_string().contains("upstream failure"));
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

    let app = Router::new().route(
        "/fapi/v1/userTrades",
        get(|| async {
            Json(json!([{ "id": 9, "orderId": 99, "qty": "1", "realizedPnl": "1", "commission": "0.1", "commissionAsset": "USDT", "maker": true }]))
        }),
    );
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
                    Json(
                        (start..start + 1000)
                            .map(|id| {
                                json!({
                                    "id": id, "orderId": order_id, "qty": "1", "realizedPnl": "1",
                                    "commission": "0.1", "commissionAsset": "USDT", "maker": true
                                })
                            })
                            .collect::<Vec<_>>(),
                    )
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
fn exchange_environment_uses_mainnet_url() {
    let config = crate::config::AppConfig::default().exchange;
    assert_eq!(
        BinanceFuturesClient::new(&config).market_cache_key(),
        "https://fapi.binance.com"
    );
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
            "/fapi/v1/ticker/price",
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
        client.get_ticker_price("INVALID").await.unwrap_err(),
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

#[tokio::test]
async fn pnl_order_lookups_share_compensation_budget_and_leave_critical_capacity() {
    use axum::{http::StatusCode, routing::get, Json, Router};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let lookups = Arc::new(AtomicUsize::new(0));
    let cancellations = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route(
            "/fapi/v1/exchangeInfo",
            get(|| async {
                Json(json!({
                    "symbols": [],
                    "rateLimits": [{
                        "rateLimitType": "REQUEST_WEIGHT", "interval": "MINUTE",
                        "intervalNum": 1, "limit": 100
                    }]
                }))
            }),
        )
        .route(
            "/fapi/v1/order",
            get({
                let lookups = lookups.clone();
                move || {
                    let lookups = lookups.clone();
                    async move {
                        lookups.fetch_add(1, Ordering::SeqCst);
                        (
                            StatusCode::BAD_REQUEST,
                            Json(json!({"code": -2013, "msg": "Order does not exist"})),
                        )
                    }
                }
            })
            .delete({
                let cancellations = cancellations.clone();
                move || {
                    let cancellations = cancellations.clone();
                    async move {
                        cancellations.fetch_add(1, Ordering::SeqCst);
                        Json(json!({}))
                    }
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client =
        BinanceFuturesClient::with_base_url(&crate::config::AppConfig::default().exchange, url);
    client.get_exchange_info(None).await.unwrap();
    let missing = client
        .get_order_by_client_id_for_pnl("SOLUSDC", "missing")
        .await
        .unwrap_err();
    assert!(missing.downcast_ref::<LocalRateLimitError>().is_none());
    assert_eq!(lookups.load(Ordering::SeqCst), 1);

    let mut limited = false;
    for _ in 0..60 {
        let error = client
            .get_order_by_client_id_for_pnl("SOLUSDC", "missing")
            .await
            .unwrap_err();
        if error.downcast_ref::<LocalRateLimitError>().is_some() {
            limited = true;
            break;
        }
    }
    assert!(limited, "historical lookups must share the PnL allowance");
    let historical_lookups = lookups.load(Ordering::SeqCst);
    assert!(historical_lookups <= 60);
    let fills = client
        .get_user_trades("SOLUSDC", 42, Decimal::ONE, 0)
        .await
        .unwrap_err();
    assert!(fills.downcast_ref::<LocalRateLimitError>().is_some());

    client
        .cancel_order("SOLUSDC", Some(42), None)
        .await
        .unwrap();
    assert!(client
        .lookup_order_by_client_id("SOLUSDC", "missing")
        .await
        .unwrap()
        .is_none());
    assert_eq!(cancellations.load(Ordering::SeqCst), 1);
    assert_eq!(lookups.load(Ordering::SeqCst), historical_lookups + 1);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn signed_and_public_http_errors_strip_request_urls() {
    use axum::{http::StatusCode, routing::get, routing::post, Router};

    let mut config = crate::config::AppConfig::default().exchange;
    config.api_key = "test-key".into();
    config.api_secret = "test-secret".into();

    let unreachable = BinanceFuturesClient::with_base_url(&config, "http://127.0.0.1:1".into());
    let transport_err = unreachable.get_open_orders("SOLUSDC").await.unwrap_err();
    let rendered = format!("{transport_err} {transport_err:?}");
    assert!(!rendered.contains("signature="), "{rendered}");
    assert!(!rendered.contains("recvWindow="), "{rendered}");

    let app = Router::new()
        .route(
            "/fapi/v1/ticker/price",
            get(|| async { StatusCode::BAD_GATEWAY }),
        )
        .route("/fapi/v1/order", post(|| async { "not-json" }))
        .route("/fapi/v2/account", get(|| async { "not-json" }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = BinanceFuturesClient::with_base_url(&config, url.clone());

    let public_err = client.get_ticker_price("SOLUSDC").await.unwrap_err();
    let public_rendered = format!("{public_err} {public_err:?}");
    assert!(!public_rendered.contains(&url), "{public_rendered}");

    let account_err = client.get_account().await.unwrap_err();
    let account_rendered = format!("{account_err} {account_err:?}");
    assert!(
        !account_rendered.contains("signature="),
        "{account_rendered}"
    );
    assert!(!account_rendered.contains(&url), "{account_rendered}");

    let order_err = client
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
    let order_rendered = format!("{order_err} {order_err:?}");
    assert!(!order_rendered.contains("signature="), "{order_rendered}");
    assert!(!order_rendered.contains(&url), "{order_rendered}");

    server.abort();
}
