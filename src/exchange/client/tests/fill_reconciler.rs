use super::super::*;

#[tokio::test]
async fn backlog_retention_survives_long_rounds_and_shrinking_count() {
    let client = BinanceFuturesClient::with_base_url(
        &crate::config::AppConfig::default().exchange,
        "http://127.0.0.1:1".into(),
    );
    client.note_pnl_reconcile_backlog(33);
    client.note_pnl_reconcile_backlog(1);
    client.fill_reconciler.lock().unwrap().waiters.push_back((
        ("BTCUSDT".into(), 1),
        Instant::now() - Duration::from_secs(11 * 60),
    ));
    let err = client
        .get_user_trades("BTCUSDT", 2, Decimal::ONE, 0)
        .await
        .unwrap_err();
    assert!(!err.to_string().contains("waiting"));
    assert_eq!(
        client
            .fill_reconciler
            .lock()
            .unwrap()
            .waiters
            .front()
            .unwrap()
            .0
             .1,
        1
    );

    client.note_pnl_reconcile_backlog(2000);
    {
        let mut state = client.fill_reconciler.lock().unwrap();
        for id in 10..18 {
            state.progress.insert(
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
        .fill_reconciler
        .lock()
        .unwrap()
        .progress
        .contains_key(&("ETHUSDT".into(), 10)));

    // Pending==0 preserves elevated retention while progress/waiters remain, then resets to 600s.
    client.note_pnl_reconcile_backlog(0);
    assert!(client.fill_retention_seconds.load(Ordering::Relaxed) > 600);
    {
        let mut state = client.fill_reconciler.lock().unwrap();
        state.progress.clear();
        state.waiters.clear();
    }
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
        let mut state = client.fill_reconciler.lock().unwrap();
        state.waiters.push_back((
            ("BTCUSDT".into(), 1),
            Instant::now() - Duration::from_secs(181),
        ));
        state.waiters.push_back((
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
    assert!(client.fill_reconciler.lock().unwrap().waiters.is_empty());
    server.abort();
}

#[tokio::test]
async fn full_inactive_waiting_queue_admits_a_new_order() {
    let client = BinanceFuturesClient::with_base_url(
        &crate::config::AppConfig::default().exchange,
        "http://127.0.0.1:1".into(),
    );
    {
        let mut state = client.fill_reconciler.lock().unwrap();
        for id in 1..=1024 {
            state.waiters.push_back((
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
    let state = client.fill_reconciler.lock().unwrap();
    assert_eq!(state.waiters.len(), 1023);
    assert_eq!(state.waiters.front().unwrap().0 .1, 2);
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
        let mut state = client.fill_reconciler.lock().unwrap();
        for order_id in 1..=8 {
            let fill: ExecutionFill = serde_json::from_value(json!({ "id": 1, "orderId": order_id, "qty": "1", "realizedPnl": "1", "commission": "0.1", "commissionAsset": "USDT", "maker": true })).unwrap();
            let mut progress = FillProgress::new(Decimal::from(2));
            progress.fills.insert(1, fill);
            progress.quantity = Decimal::ONE;
            progress.from_id = Some(2);
            progress.restarted = true;
            state.progress.insert(
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
        let state = client.fill_reconciler.lock().unwrap();
        assert!(!state.progress.contains_key(&("BTCUSDT".into(), 1)));
        assert!((2..=8).all(|id| state.progress.contains_key(&("BTCUSDT".into(), id))));
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
        let mut state = client.fill_reconciler.lock().unwrap();
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
            state.progress.insert(
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
        .fill_reconciler
        .lock()
        .unwrap()
        .progress
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
    let state = client.fill_reconciler.lock().unwrap();
    assert!(!state.progress.contains_key(&("BTCUSDT".into(), 1)));
    assert!((2..=8).all(|id| state.progress.contains_key(&("BTCUSDT".into(), id))));
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
        let mut state = client.fill_reconciler.lock().unwrap();
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
            state.progress.insert(
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
        limited.fill_reconciler.lock().unwrap().progress[&("BTCUSDT".into(), 1)].stalled_attempts,
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
        client.fill_reconciler.lock().unwrap().progress[&("BTCUSDT".into(), 1)].stalled_attempts,
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
    client.fill_reconciler.lock().unwrap().progress.insert(
        key.clone(),
        FillCacheEntry {
            progress: Arc::clone(&shared),
            last_used: Instant::now(),
            stalled_attempts: 0,
        },
    );
    let cleanup = FillCacheCleanup {
        reconciler: &client.fill_reconciler,
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
    assert!(!client
        .fill_reconciler
        .lock()
        .unwrap()
        .progress
        .contains_key(&key));

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
    client.fill_reconciler.lock().unwrap().progress.insert(
        key.clone(),
        FillCacheEntry {
            progress: Arc::clone(&shared),
            last_used: Instant::now(),
            stalled_attempts: 0,
        },
    );
    let cleanup = FillCacheCleanup {
        reconciler: &client.fill_reconciler,
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
        client.fill_reconciler.lock().unwrap().progress[&key].stalled_attempts,
        1
    );
}

#[test]
fn old_cleanup_does_not_remove_replacement_progress() {
    let reconciler = std::sync::Mutex::new(FillReconcilerState::default());
    let key = ("BTCUSDT".to_owned(), 1);
    let old = Arc::new(tokio::sync::Mutex::new(FillProgress::new(Decimal::ONE)));
    let replacement = Arc::new(tokio::sync::Mutex::new(FillProgress::new(Decimal::ONE)));
    let cleanup = FillCacheCleanup {
        reconciler: &reconciler,
        key: key.clone(),
        progress: Arc::clone(&old).try_lock_owned().unwrap(),
        shared: old,
        initial_advances: 0,
        failed_query: false,
    };
    reconciler.lock().unwrap().progress.insert(
        key.clone(),
        FillCacheEntry {
            progress: Arc::clone(&replacement),
            last_used: Instant::now(),
            stalled_attempts: 0,
        },
    );
    drop(cleanup);
    assert!(Arc::ptr_eq(
        &reconciler.lock().unwrap().progress[&key].progress,
        &replacement
    ));
}
