use super::super::*;
use axum::{extract::State, routing::get, Json, Router};
use serde_json::{json, Value};
use tokio::sync::RwLock;

#[tokio::test]
async fn market_endpoints_reject_unidentified_prices_and_recover_with_matching_symbols() {
    let response = Arc::new(RwLock::new(json!({
        "symbol": "BTCUSDT", "price": "101", "markPrice": "100",
        "lastPrice": "101", "priceChange": "1", "priceChangePercent": "1",
        "highPrice": "110", "lowPrice": "90", "volume": "12"
    })));
    async fn respond(State(response): State<Arc<RwLock<Value>>>) -> Json<Value> {
        Json(response.read().await.clone())
    }
    let app = Router::new()
        .route("/fapi/v1/ticker/price", get(respond))
        .route("/fapi/v1/premiumIndex", get(respond))
        .route("/fapi/v1/ticker/24hr", get(respond))
        .with_state(response.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client =
        BinanceFuturesClient::with_base_url(&crate::config::AppConfig::default().exchange, url);

    for symbol in [Some("BTCUSDT"), Some(""), None] {
        {
            let mut body = response.write().await;
            if let Some(symbol) = symbol {
                body["symbol"] = json!(symbol);
            } else {
                body.as_object_mut().unwrap().remove("symbol");
            }
        }
        let results = [
            client.get_ticker_price("SOLUSDC").await.map(|_| ()),
            client
                .get_ticker_price_for_fallback("SOLUSDC")
                .await
                .map(|_| ()),
            client.get_mark_price("SOLUSDC").await.map(|_| ()),
            client
                .get_mark_price_for_fallback("SOLUSDC")
                .await
                .map(|_| ()),
            client.get_24hr_ticker("SOLUSDC").await.map(|_| ()),
            client
                .get_24hr_ticker_for_fallback("SOLUSDC")
                .await
                .map(|_| ()),
        ];
        for (endpoint, result) in results.into_iter().enumerate() {
            assert!(result.is_err(), "endpoint {endpoint} accepted {symbol:?}");
        }
    }

    for symbol in ["SOLUSDC", "solusdc"] {
        response.write().await["symbol"] = json!(symbol);
        assert_eq!(
            client.get_ticker_price("SOLUSDC").await.unwrap(),
            Decimal::from(101)
        );
        assert_eq!(
            client
                .get_ticker_price_for_fallback("SOLUSDC")
                .await
                .unwrap(),
            Decimal::from(101)
        );
        assert_eq!(
            client.get_mark_price("SOLUSDC").await.unwrap(),
            Decimal::from(100)
        );
        assert_eq!(
            client.get_mark_price_for_fallback("SOLUSDC").await.unwrap(),
            Decimal::from(100)
        );
        assert_eq!(
            client.get_24hr_ticker("SOLUSDC").await.unwrap().last_price,
            Decimal::from(101)
        );
        assert_eq!(
            client
                .get_24hr_ticker_for_fallback("SOLUSDC")
                .await
                .unwrap()
                .last_price,
            Decimal::from(101)
        );
    }
    server.abort();
    let _ = server.await;
}
