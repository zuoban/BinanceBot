use super::*;

mod fill_reconciler;
mod http_and_signing;

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
