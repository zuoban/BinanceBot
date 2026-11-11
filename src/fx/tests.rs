use super::*;
use axum::{extract::State, routing::get, Json, Router};
use rust_decimal_macros::dec;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[tokio::test]
async fn fetches_rate_once_and_reuses_cache() {
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route(
            "/rates",
            get(|State(calls): State<Arc<AtomicUsize>>| async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Json(serde_json::json!({
                    "data": {
                        "currency": "USDC",
                        "rates": {
                            "EUR": "0.920000",
                            "JPY": "153.400000",
                            "CNY": "6.710078",
                            "GBP": "0.790000"
                        }
                    }
                }))
            }),
        )
        .with_state(calls.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/rates", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let cache = CnyRateCache::with_endpoint(endpoint);
    cache.refresh_if_due("USDC").await.unwrap();
    cache.refresh_if_due("USDC").await.unwrap();
    cache.refresh_if_due("FDUSD").await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        cache.get("USDC").await.unwrap().cny_per_unit,
        dec!(6.710078)
    );
    assert!(cache.get("FDUSD").await.is_none());

    {
        let mut entries = cache.entries.lock().await;
        let entry = entries.get_mut("USDC").unwrap();
        entry.fetched_at = Some(Instant::now() - CACHE_TTL - Duration::from_secs(1));
        entry.attempted_at = Some(Instant::now() - RETRY_DELAY - Duration::from_secs(1));
    }
    cache.refresh_if_due("USDC").await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    cache
        .entries
        .lock()
        .await
        .get_mut("USDC")
        .unwrap()
        .fetched_at = Some(Instant::now() - MAX_DISPLAY_AGE - Duration::from_secs(1));
    assert!(cache.get("USDC").await.is_none());

    server.abort();
}

#[tokio::test]
async fn rejects_missing_cny_and_strips_request_urls_from_http_errors() {
    let app = Router::new().route(
        "/secret-path",
        get(|| async { (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "boom") }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/secret-path", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let cache = CnyRateCache::with_endpoint(endpoint);
    let err = cache.refresh_if_due("USDT").await.unwrap_err().to_string();
    assert!(!err.contains("secret-path"), "{err}");
    server.abort();
}

#[tokio::test]
async fn oversized_http_200_response_is_rejected_without_oom() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/rates", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        // 1st request: unannounced (>256 KiB) streamed 200 OK body without Content-Length.
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut req_buf = [0u8; 1024];
            let _ = stream.read(&mut req_buf).await;
            let header = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"data\":{\"currency\":\"USDC\",\"rates\":{\"CNY\":\"7.1\",\"PAD\":\"";
            let _ = stream.write_all(header).await;
            let chunk = [b'A'; 8192];
            for _ in 0..40 {
                if stream.write_all(&chunk).await.is_err() {
                    break;
                }
            }
            let _ = stream.write_all(b"\"}}}").await;
        }
        // 2nd request: huge advertised Content-Length rejected immediately before reading body.
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut req_buf = [0u8; 1024];
            let _ = stream.read(&mut req_buf).await;
            let header = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1073741824\r\nConnection: close\r\n\r\n";
            let _ = stream.write_all(header).await;
        }
    });

    let cache = CnyRateCache::with_endpoint(endpoint);
    let err_stream = cache.refresh_if_due("USDC").await.unwrap_err().to_string();
    assert!(err_stream.contains("exceeded"), "{err_stream}");
    assert!(cache.get("USDC").await.is_none());

    let err_len = cache.refresh_if_due("USDT").await.unwrap_err().to_string();
    assert!(err_len.contains("exceeded"), "{err_len}");
    assert!(cache.get("USDT").await.is_none());

    server.abort();
}
