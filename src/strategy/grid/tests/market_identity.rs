use super::*;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

#[tokio::test]
async fn mismatched_rest_prices_preserve_staleness_and_keep_the_market_gate_closed() {
    let valid = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut app = Router::new();
    for path in [
        "/fapi/v1/premiumIndex",
        "/fapi/v1/ticker/24hr",
        "/fapi/v1/ticker/price",
    ] {
        app = app.route(
            path,
            get({
                let valid = valid.clone();
                let calls = calls.clone();
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let symbol = if valid.load(Ordering::SeqCst) {
                        "SOLUSDC"
                    } else {
                        "BTCUSDT"
                    };
                    async move {
                        Json(serde_json::json!({
                            "symbol": symbol, "price": "101", "markPrice": "100",
                            "lastPrice": "101", "priceChange": "1", "priceChangePercent": "1",
                            "highPrice": "110", "lowPrice": "90", "volume": "12"
                        }))
                    }
                }
            }),
        );
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    let symbol = &config.exchange.symbol;
    let stale = Utc::now() - chrono::Duration::seconds(30);
    {
        let mut ticker = state.ticker.write().await;
        ticker.symbol = symbol.clone();
        ticker.last_price = dec!(99);
        ticker.mark_price = dec!(98);
        ticker.update_time = stale;
        ticker.mark_update_time = stale;
    }
    engine.refresh_mark_price(symbol).await;
    engine.refresh_stale_ticker(symbol).await;
    {
        let ticker = state.ticker.read().await;
        assert_eq!(ticker.symbol, *symbol);
        assert_eq!(ticker.last_price, dec!(99));
        assert_eq!(ticker.mark_price, dec!(98));
        assert_eq!(ticker.update_time, stale);
        assert_eq!(ticker.mark_update_time, stale);
    }
    assert!(!engine.market_data_ready().await);
    assert!(!engine.mark_retry.ready());
    assert!(!engine.ticker_retry.ready());
    assert!(engine.pending_paper_ticker.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    engine.refresh_mark_price(symbol).await;
    engine.refresh_stale_ticker(symbol).await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);

    valid.store(true, Ordering::SeqCst);
    let retry = Instant::now() - Duration::from_secs(1);
    engine.mark_retry.retry_at = Some(retry);
    engine.ticker_retry.retry_at = Some(retry);
    engine.refresh_mark_price(symbol).await;
    engine.refresh_stale_ticker(symbol).await;
    assert!(engine.market_data_ready().await);
    let ticker = state.ticker.read().await;
    assert_eq!(ticker.last_price, dec!(101));
    assert_eq!(ticker.mark_price, dec!(100));
    assert!(engine.mark_retry.ready());
    assert!(engine.ticker_retry.ready());
    assert_eq!(calls.load(Ordering::SeqCst), 5);
    server.abort();
    let _ = server.await;
}
