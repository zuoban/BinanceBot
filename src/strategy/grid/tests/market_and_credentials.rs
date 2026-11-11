use super::*;

#[tokio::test]
async fn ticker_update_keeps_independent_mark_price() {
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    let mark_updated_at = Utc::now();
    {
        let mut ticker = state.ticker.write().await;
        ticker.mark_price = dec!(114.3);
        ticker.mark_update_time = mark_updated_at;
    }

    engine
        .handle_ticker_update(TickerInfo {
            symbol: config.exchange.symbol,
            last_price: dec!(114.5),
            update_time: Utc::now(),
            ..Default::default()
        })
        .await;

    let ticker = state.ticker.read().await;
    assert_eq!(ticker.last_price, dec!(114.5));
    assert_eq!(ticker.mark_price, dec!(114.3));
    assert_eq!(ticker.mark_update_time, mark_updated_at);
}

#[tokio::test]
async fn mark_update_preserves_ticker_and_does_not_fill_paper_orders() {
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    let now = Utc::now();
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(100);
        ticker.high_24h = dec!(110);
        ticker.update_time = now;
    }
    *state.status.write().await = BotStatus::Running;
    state.active_orders.write().await.insert(
        "gb_b_test".into(),
        GridOrder {
            client_order_id: "gb_b_test".into(),
            order_id: None,
            symbol: config.exchange.symbol.clone(),
            side: OrderSide::Buy,
            price: dec!(99),
            quantity: dec!(1),
            amount_usdc: dec!(99),
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
            grid_level: -1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: OrderPurpose::Grid,
            merge_sources: Vec::new(),
        },
    );
    {
        let mut position = state.position.write().await;
        position.size = dec!(1);
        position.entry_price = dec!(90);
    }
    engine
        .handle_ticker_update(TickerInfo {
            symbol: config.exchange.symbol.clone(),
            mark_price: dec!(102),
            mark_update_time: Utc::now(),
            ..Default::default()
        })
        .await;
    let ticker = state.ticker.read().await.clone();
    assert_eq!(ticker.last_price, dec!(100));
    assert_eq!(ticker.high_24h, dec!(110));
    assert_eq!(ticker.update_time, now);
    assert_eq!(ticker.mark_price, dec!(102));
    assert_eq!(state.position.read().await.unrealized_pnl, dec!(12));
    assert!(state.active_orders.read().await.contains_key("gb_b_test"));

    engine
        .handle_ticker_update(TickerInfo {
            symbol: config.exchange.symbol,
            mark_price: dec!(50),
            mark_update_time: now - chrono::Duration::seconds(1),
            ..Default::default()
        })
        .await;
    assert_eq!(state.ticker.read().await.mark_price, dec!(102));
}

#[tokio::test]
async fn mark_rest_fallback_only_refreshes_missing_or_stale_values() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let calls = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicBool::new(false));
    let app = Router::new().route(
        "/fapi/v1/premiumIndex",
        get({
            let calls = calls.clone();
            let fail = fail.clone();
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                let failed = fail.load(Ordering::SeqCst);
                async move {
                    if failed {
                        (
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            Json(serde_json::json!({"code": -1000})),
                        )
                    } else {
                        (
                            axum::http::StatusCode::OK,
                            Json(serde_json::json!({"symbol": "SOLUSDC", "markPrice": "105"})),
                        )
                    }
                }
            }
        }),
    );
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
    {
        let mut ticker = state.ticker.write().await;
        ticker.symbol = symbol.clone();
        ticker.last_price = dec!(100);
        ticker.update_time = Utc::now();
    }
    engine.refresh_mark_price(symbol).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.ticker.read().await.mark_price, dec!(105));
    engine.refresh_mark_price(symbol).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let stale = Utc::now() - chrono::Duration::seconds(16);
    state.ticker.write().await.mark_update_time = stale;
    fail.store(true, Ordering::SeqCst);
    engine.refresh_mark_price(symbol).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let ticker = state.ticker.read().await;
    assert_eq!(ticker.mark_update_time, stale);
    assert!(!ticker.prices_are_fresh(Utc::now()));
    drop(ticker);
    fail.store(false, Ordering::SeqCst);
    engine.refresh_mark_price(symbol).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(!state.ticker.read().await.prices_are_fresh(Utc::now()));
    engine.mark_retry.retry_at = Some(Instant::now() - std::time::Duration::from_secs(1));
    engine.refresh_mark_price(symbol).await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert!(state.ticker.read().await.prices_are_fresh(Utc::now()));

    state.ticker.write().await.mark_update_time = stale;
    fail.store(true, Ordering::SeqCst);
    engine.refresh_mark_price(symbol).await;
    assert!(!engine.mark_retry.ready());
    engine
        .handle_ticker_update(TickerInfo {
            symbol: symbol.clone(),
            mark_price: dec!(106),
            mark_update_time: Utc::now(),
            ..Default::default()
        })
        .await;
    assert!(engine.mark_retry.ready());
    assert!(state.ticker.read().await.prices_are_fresh(Utc::now()));
    let future = Utc::now() + chrono::Duration::seconds(30);
    state.ticker.write().await.mark_update_time = future;
    fail.store(false, Ordering::SeqCst);
    engine.refresh_mark_price(symbol).await;
    let ticker = state.ticker.read().await;
    assert_eq!(ticker.mark_price, dec!(106));
    assert_eq!(ticker.mark_update_time, future);
    assert!(!ticker.prices_are_fresh(Utc::now()));
    server.abort();
}

#[tokio::test]
async fn failed_ticker_fallback_backs_off_and_recovers_on_push_or_retry() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let calls = Arc::new(AtomicUsize::new(0));
    let available = Arc::new(AtomicBool::new(false));
    let app = Router::new()
            .route(
                "/fapi/v1/ticker/24hr",
                get({
                    let calls = calls.clone();
                    move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        async { axum::http::StatusCode::SERVICE_UNAVAILABLE }
                    }
                }),
            )
            .route(
                "/fapi/v1/ticker/price",
                get({
                    let calls = calls.clone();
                    let available = available.clone();
                    move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let available = available.load(Ordering::SeqCst);
                        async move {
                            if available {
                                (
                                    axum::http::StatusCode::OK,
                                    Json(serde_json::json!({"symbol": "SOLUSDC", "price": "101", "time": 1})),
                                )
                            } else {
                                (
                                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                                    Json(serde_json::json!({})),
                                )
                            }
                        }
                    }
                }),
            );
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

    engine.refresh_stale_ticker(symbol).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    engine.refresh_stale_ticker(symbol).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(!state.ticker.read().await.prices_are_fresh(Utc::now()));
    engine
        .handle_ticker_update(TickerInfo {
            symbol: symbol.clone(),
            last_price: dec!(100),
            update_time: Utc::now(),
            ..Default::default()
        })
        .await;
    assert!(engine.ticker_retry.ready());
    assert!(state.ticker.read().await.prices_are_fresh(Utc::now()));

    state.ticker.write().await.update_time = Utc::now() - chrono::Duration::seconds(16);
    engine.refresh_stale_ticker(symbol).await;
    assert!(!engine.ticker_retry.ready());
    available.store(true, Ordering::SeqCst);
    engine.ticker_retry.retry_at = Some(Instant::now() - std::time::Duration::from_secs(1));
    engine.refresh_stale_ticker(symbol).await;
    assert_eq!(calls.load(Ordering::SeqCst), 6);
    assert_eq!(state.ticker.read().await.last_price, dec!(101));
    assert!(state.ticker.read().await.prices_are_fresh(Utc::now()));
    server.abort();
}

#[tokio::test]
async fn delayed_mark_rest_response_does_not_overwrite_a_newer_update() {
    let requested = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let app = Router::new().route(
        "/fapi/v1/premiumIndex",
        get({
            let requested = requested.clone();
            let release = release.clone();
            move || {
                let requested = requested.clone();
                let release = release.clone();
                async move {
                    requested.notify_one();
                    release.notified().await;
                    Json(serde_json::json!({"symbol": "SOLUSDC", "markPrice": "90"}))
                }
            }
        }),
    );
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
    let ((), ()) = tokio::join!(engine.refresh_mark_price(&config.exchange.symbol), async {
        requested.notified().await;
        let mut ticker = state.ticker.write().await;
        ticker.mark_price = dec!(110);
        ticker.mark_update_time = Utc::now();
        drop(ticker);
        release.notify_one();
    });
    assert_eq!(state.ticker.read().await.mark_price, dec!(110));
    server.abort();
}

#[tokio::test]
async fn queued_mark_event_from_during_rest_request_is_not_discarded() {
    let requested = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let app = Router::new().route(
        "/fapi/v1/premiumIndex",
        get({
            let requested = requested.clone();
            let release = release.clone();
            move || {
                let requested = requested.clone();
                let release = release.clone();
                async move {
                    requested.notify_one();
                    release.notified().await;
                    Json(serde_json::json!({"symbol": "SOLUSDC", "markPrice": "90"}))
                }
            }
        }),
    );
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
    let (_, event_time) = tokio::join!(engine.refresh_mark_price(&config.exchange.symbol), async {
        requested.notified().await;
        let event_time = Utc::now();
        release.notify_one();
        event_time
    });
    engine
        .handle_ticker_update(TickerInfo {
            symbol: config.exchange.symbol,
            mark_price: dec!(110),
            mark_update_time: event_time,
            ..Default::default()
        })
        .await;
    assert_eq!(state.ticker.read().await.mark_price, dec!(110));
    server.abort();
}

#[tokio::test]
async fn market_fallbacks_have_one_request_per_type_and_apply_valid_completions() {
    let started = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
            .route("/fapi/v1/premiumIndex", get({
                let started = started.clone();
                let release = release.clone();
                let calls = calls.clone();
                move || {
                    let started = started.clone();
                    let release = release.clone();
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        started.add_permits(1);
                        release.acquire().await.unwrap().forget();
                        Json(serde_json::json!({"symbol":"SOLUSDC", "markPrice":"101"}))
                    }
                }
            }))
            .route("/fapi/v1/ticker/24hr", get({
                let started = started.clone();
                let release = release.clone();
                let calls = calls.clone();
                move || {
                    let started = started.clone();
                    let release = release.clone();
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        started.add_permits(1);
                        release.acquire().await.unwrap().forget();
                        Json(serde_json::json!({"symbol":"SOLUSDC", "lastPrice":"100", "highPrice":"110", "lowPrice":"90", "priceChange":"1", "priceChangePercent":"1", "volume":"12", "quoteVolume":"1200"}))
                    }
                }
            }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let config = AppConfig::default();
    let mut engine =
        pair_recovery_engine(&config, Arc::new(Database::open(":memory:").unwrap()), url).await;
    *engine.state.status.write().await = BotStatus::Paused;
    *engine.state.ticker.write().await = TickerInfo {
        symbol: config.exchange.symbol.clone(),
        ..Default::default()
    };
    for _ in 0..3 {
        engine
            .start_mark_price_request(&config.exchange.symbol)
            .await;
        engine.start_ticker_request(&config.exchange.symbol).await;
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), started.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(engine.state.ticker.read().await.last_price, Decimal::ZERO);
    release.add_permits(2);
    let mark = engine.mark_price_task.take().unwrap().await.unwrap();
    let ticker = engine.ticker_task.take().unwrap().await.unwrap();
    let mark_requested_at = mark.result.as_ref().unwrap().1;
    let ticker_requested_at = ticker.result.as_ref().unwrap().update_time;
    engine.apply_ticker_completion(ticker).await;
    engine.apply_mark_price_completion(mark).await;
    let ticker = engine.state.ticker.read().await;
    assert_eq!(ticker.mark_price, dec!(101));
    assert_eq!(ticker.last_price, dec!(100));
    assert_eq!(ticker.mark_update_time, mark_requested_at);
    assert_eq!(ticker.update_time, ticker_requested_at);
    assert!(ticker.prices_are_fresh(Utc::now()));
    assert_eq!(engine.state.position.read().await.unrealized_pnl, dec!(101));
    server.abort();
}

#[tokio::test]
async fn market_completions_reject_stale_requests_and_retired_or_changed_scopes() {
    for change in [
        "stale",
        "generation",
        "symbol",
        "mode",
        "account",
        "shutdown",
    ] {
        let config = AppConfig::default();
        let mut engine = pair_recovery_engine(
            &config,
            Arc::new(Database::open(":memory:").unwrap()),
            "http://127.0.0.1:1".into(),
        )
        .await;
        *engine.state.status.write().await = BotStatus::Paused;
        let scope = engine.market_request_scope(&config.exchange.symbol).await;
        let previous = engine.state.ticker.read().await.clone();
        let mut requested_at = Utc::now();
        match change {
            "stale" => requested_at -= chrono::Duration::seconds(16),
            "generation" => engine.retire_market_requests(),
            "symbol" => engine.state.config.write().await.exchange.symbol = "BTCUSDC".into(),
            "mode" => {
                let mut config = engine.state.config.write().await;
                config.exchange.api_key = "different-key".into();
                config.exchange.api_secret = "different-secret".into();
            }
            "account" => {
                let mut changed = config.exchange.clone();
                changed.api_key = "other-account".into();
                changed.api_secret = "other-secret".into();
                engine.client = Arc::new(BinanceFuturesClient::with_base_url(
                    &changed,
                    "http://127.0.0.1:1".into(),
                ));
            }
            "shutdown" => engine.state.request_shutdown(),
            _ => unreachable!(),
        }
        engine
            .apply_mark_price_completion(super::MarkPriceCompletion {
                scope: scope.clone(),
                previous: previous.clone(),
                result: Ok((dec!(70), requested_at)),
            })
            .await;
        engine
            .apply_ticker_completion(super::TickerCompletion {
                scope,
                result: Ok(TickerInfo {
                    symbol: config.exchange.symbol,
                    last_price: dec!(70),
                    update_time: requested_at,
                    ..Default::default()
                }),
            })
            .await;
        let ticker = engine.state.ticker.read().await;
        assert_eq!(ticker.last_price, previous.last_price, "{change}");
        assert_eq!(ticker.mark_price, previous.mark_price, "{change}");
        assert_eq!(ticker.update_time, previous.update_time, "{change}");
        assert_eq!(
            ticker.mark_update_time, previous.mark_update_time,
            "{change}"
        );
    }
}

#[tokio::test]
async fn ticker_fallback_matches_paper_fill_once_when_its_pending_mark_completes() {
    for interrupted_by_pause in [false, true] {
        let config = AppConfig::default();
        let mut engine = pair_recovery_engine(
            &config,
            Arc::new(Database::open(":memory:").unwrap()),
            "http://127.0.0.1:1".into(),
        )
        .await;
        *engine.state.status.write().await = BotStatus::Running;
        *engine.state.position.write().await = PositionInfo::default();
        let now = Utc::now();
        *engine.state.ticker.write().await = TickerInfo {
            symbol: config.exchange.symbol.clone(),
            // A missing mark permits startup fills against the fresh last price.
            // A previously available, stale mark must defer matching instead.
            mark_price: dec!(100),
            mark_update_time: now - chrono::Duration::seconds(16),
            ..Default::default()
        };
        let order = GridOrder {
            client_order_id: "gb_deferred_market_fill".into(),
            order_id: None,
            symbol: config.exchange.symbol.clone(),
            side: OrderSide::Buy,
            price: dec!(99),
            quantity: dec!(1),
            amount_usdc: dec!(99),
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
            grid_level: -1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(order.client_order_id.clone(), order);
        let scope = engine.market_request_scope(&config.exchange.symbol).await;
        let previous = engine.state.ticker.read().await.clone();
        engine
            .apply_ticker_completion(super::TickerCompletion {
                scope: scope.clone(),
                result: Ok(TickerInfo {
                    symbol: config.exchange.symbol,
                    last_price: dec!(98),
                    update_time: now,
                    ..Default::default()
                }),
            })
            .await;
        assert_eq!(engine.state.stats.read().await.total_trades, 0);
        assert!(engine.pending_paper_ticker.is_some());
        assert!(!engine
            .state
            .ticker
            .read()
            .await
            .prices_are_fresh(Utc::now()));
        if interrupted_by_pause {
            engine.state.pause_trading().await.unwrap();
            engine
                .state
                .resume_trading_in_mode(TradingMode::Paper, engine.state.pause_epoch())
                .await
                .unwrap();
        }
        engine
            .apply_mark_price_completion(super::MarkPriceCompletion {
                scope,
                previous,
                result: Ok((dec!(98), now)),
            })
            .await;
        assert!(engine
            .state
            .ticker
            .read()
            .await
            .prices_are_fresh(Utc::now()));
        let expected_trades = if interrupted_by_pause { 0 } else { 1 };
        assert_eq!(
            engine.state.stats.read().await.total_trades,
            expected_trades
        );
        assert_eq!(
            engine.state.position.read().await.size,
            if interrupted_by_pause {
                dec!(0)
            } else {
                dec!(1)
            }
        );
        assert_eq!(
            engine.state.position.read().await.unrealized_pnl,
            if interrupted_by_pause {
                dec!(0)
            } else {
                dec!(-1)
            }
        );
        assert!(engine.pending_paper_ticker.is_none());
        engine
            .handle_ticker_update(TickerInfo {
                symbol: "SOLUSDC".into(),
                mark_price: dec!(97),
                mark_update_time: Utc::now(),
                ..Default::default()
            })
            .await;
        assert_eq!(
            engine.state.stats.read().await.total_trades,
            expected_trades
        );
    }
}

#[test]
fn grid_client_order_ids_fit_binance_limit() {
    for (side, prefix) in [(OrderSide::Buy, "gb_b_"), (OrderSide::Sell, "gb_s_")] {
        let first = new_grid_client_order_id(side);
        let second = new_grid_client_order_id(side);

        assert!(first.starts_with(prefix));
        assert_eq!(first.len(), 35);
        assert!(first
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_'));
        assert_ne!(first, second);
    }
}

#[tokio::test]
async fn pnl_order_id_cache_survives_failed_user_trades_retry_without_order_lookup() {
    use std::sync::atomic::AtomicBool;
    let order_lookups = Arc::new(AtomicUsize::new(0));
    let fail_trades = Arc::new(AtomicBool::new(true));
    let app = Router::new()
        .route(
            "/fapi/v1/order",
            get({
                let order_lookups = Arc::clone(&order_lookups);
                move || {
                    order_lookups.fetch_add(1, Ordering::SeqCst);
                    async {
                        (
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            Json(serde_json::json!({})),
                        )
                    }
                }
            }),
        )
        .route(
            "/fapi/v1/userTrades",
            get({
                let fail_trades = Arc::clone(&fail_trades);
                move || {
                    let fail = fail_trades.load(Ordering::SeqCst);
                    async move {
                        if fail {
                            (
                                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                                Json(serde_json::json!({"msg": "busy"})),
                            )
                        } else {
                            (
                                axum::http::StatusCode::OK,
                                Json(serde_json::json!([{
                                    "id": 1,
                                    "orderId": 987,
                                    "qty": "1",
                                    "realizedPnl": "1.5",
                                    "commission": "0.02",
                                    "commissionAsset": "USDC",
                                    "maker": true
                                }])),
                            )
                        }
                    }
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    let mut filled = GridOrder {
        client_order_id: "gb_s_cached_oid".into(),
        order_id: Some(987),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Sell,
        price: dec!(101),
        quantity: dec!(1),
        amount_usdc: dec!(101),
        status: OrderStatus::Filled,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 1,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    assert!(engine.commit_order_fill(&mut filled).await);
    assert_eq!(engine.pnl_order_ids.get("gb_s_cached_oid"), Some(&987));

    engine.reconcile_unverified_trades().await;
    engine.pnl_retry_at = engine.pnl_reconcile_task.take().unwrap().await.unwrap();
    assert!(engine.pnl_retry_at.is_some());
    assert_eq!(engine.pnl_order_ids.get("gb_s_cached_oid"), Some(&987));
    assert_eq!(order_lookups.load(Ordering::SeqCst), 0);

    fail_trades.store(false, Ordering::SeqCst);
    engine.pnl_retry_at = None;
    engine.reconcile_unverified_trades().await;
    assert!(engine
        .pnl_reconcile_task
        .take()
        .unwrap()
        .await
        .unwrap()
        .is_none());
    assert_eq!(order_lookups.load(Ordering::SeqCst), 0);
    assert_eq!(engine.state.stats.read().await.pending_pnl_trades, 0);

    engine.reconcile_unverified_trades().await;
    assert!(engine.pnl_order_ids.is_empty());
    server.abort();
}

#[tokio::test]
async fn config_scope_switch_resets_blocked_flags_and_paper_window_cache() {
    let mut paper = AppConfig::default();
    paper.grid.grid_interval = dec!(1);
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (url, server) = pair_exchange_server(PairExchange::default()).await;
    let mut engine = pair_recovery_engine(&paper, db, url).await;
    engine.state.position.write().await.size = dec!(10);
    engine.maintain_grid_window().await;
    assert!(engine.paper_window_cache.is_some());
    engine.reconciliation_blocked = true;
    engine.market_data_blocked = true;

    let mut switched = paper.clone();
    switched.exchange.api_key = "live-key".into();
    switched.exchange.api_secret = "live-secret".into();
    engine.apply_config(switched).await.unwrap();
    assert!(!engine.reconciliation_blocked);
    assert!(!engine.market_data_blocked);
    assert!(engine.paper_window_cache.is_none());
    server.abort();
}

#[tokio::test]
async fn live_sync_freshness_scales_with_configured_sync_interval() {
    assert_eq!(live_sync_freshness(1), std::time::Duration::from_secs(10));
    assert_eq!(live_sync_freshness(5), std::time::Duration::from_secs(14));
    assert_eq!(live_sync_freshness(20), std::time::Duration::from_secs(44));

    let mut config = AppConfig::default();
    config.exchange.sync_interval_secs = 20;
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state, client, action_rx, ticker_rx);
    let recent = std::time::Instant::now() - std::time::Duration::from_secs(25);
    engine.last_account_sync = Some(recent);
    engine.last_orders_sync = Some(recent);
    assert!(engine.sync_timestamps_fresh());

    let expired = std::time::Instant::now() - std::time::Duration::from_secs(45);
    engine.last_account_sync = Some(expired);
    assert!(!engine.sync_timestamps_fresh());
}

#[tokio::test]
async fn paper_single_buy_window_places_closing_sell_immediately_on_fill_tick() {
    let mut config = AppConfig::default();
    config.grid.grid_interval = dec!(1);
    config.grid.order_amount_usdc = dec!(100);
    config.grid.buy_window = 1;
    config.grid.sell_window = 1;
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    *state.status.write().await = BotStatus::Running;
    {
        let mut ticker = state.ticker.write().await;
        ticker.symbol = config.exchange.symbol.clone();
        ticker.last_price = dec!(100.5);
        ticker.mark_price = dec!(100.5);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);

    engine.maintain_grid_window().await;
    {
        let active = state.active_orders.read().await;
        assert_eq!(active.len(), 1);
        let only = active.values().next().unwrap();
        assert_eq!(only.side, OrderSide::Buy);
        assert_eq!(only.price, dec!(100));
    }

    // Price crosses the single buy order, temporarily emptying active_orders.
    // That same tick must immediately place the closing sell order at 101.
    let now = Utc::now();
    engine
        .handle_ticker_update(TickerInfo {
            symbol: config.exchange.symbol,
            last_price: dec!(100),
            mark_price: dec!(100),
            update_time: now,
            mark_update_time: now,
            ..Default::default()
        })
        .await;

    assert!(state.position.read().await.size > Decimal::ZERO);
    let active = state.active_orders.read().await;
    assert!(
        active
            .values()
            .any(|o| o.side == OrderSide::Sell && o.price == dec!(101)),
        "expected immediate closing sell at 101, got: {:?}",
        active.values().collect::<Vec<_>>()
    );
}

#[derive(Clone, Default)]
struct DualAccountExchange {
    orders_a: Arc<Mutex<HashMap<String, BinanceOrderResponse>>>,
    orders_b: Arc<Mutex<HashMap<String, BinanceOrderResponse>>>,
    posts_a: Arc<Mutex<Vec<String>>>,
    posts_b: Arc<Mutex<Vec<String>>>,
    b_position_calls: Arc<AtomicUsize>,
    fail_b_second_position: Arc<AtomicUsize>,
}

async fn dual_account_server(fake: DualAccountExchange) -> (String, tokio::task::JoinHandle<()>) {
    let api_key = |headers: &axum::http::HeaderMap| {
        headers
            .get("x-mbx-apikey")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    };
    let app = Router::new()
        .route("/fapi/v1/time", get(|| async {
            Json(serde_json::json!({"serverTime": Utc::now().timestamp_millis()}))
        }))
        .route("/fapi/v1/exchangeInfo", get(|| async {
            Json(serde_json::json!({"symbols":[{"symbol":"SOLUSDC","status":"TRADING","contractType":"PERPETUAL","filters":[{"filterType":"PRICE_FILTER","minPrice":"0.01","maxPrice":"100000","tickSize":"0.01"},{"filterType":"LOT_SIZE","minQty":"0.01","maxQty":"100000","stepSize":"0.01"},{"filterType":"MIN_NOTIONAL","notional":"5"}]}]}))
        }))
        .route("/fapi/v1/ticker/price", get(|| async {
            Json(serde_json::json!({"symbol":"SOLUSDC","price":"100.5"}))
        }))
        .route("/fapi/v1/premiumIndex", get(|| async {
            Json(serde_json::json!({"symbol":"SOLUSDC","markPrice":"100.5"}))
        }))
        .route("/fapi/v1/ticker/24hr", get(|| async {
            Json(serde_json::json!({"lastPrice":"100.5","priceChange":"0","priceChangePercent":"0","highPrice":"101","lowPrice":"100","volume":"10"}))
        }))
        .route("/fapi/v2/positionRisk", get(move |headers: axum::http::HeaderMap, State(fake): State<DualAccountExchange>| async move {
            if api_key(&headers) == "account-b-key" {
                let call = fake.b_position_calls.fetch_add(1, Ordering::SeqCst);
                if call >= 1 && consume_failure(&fake.fail_b_second_position) {
                    return (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({})));
                }
                (axum::http::StatusCode::OK, Json(serde_json::json!([{"symbol":"SOLUSDC","positionAmt":"0","entryPrice":"0","markPrice":"100.5","unRealizedProfit":"0","liquidationPrice":"0","leverage":"5"}])))
            } else {
                (axum::http::StatusCode::OK, Json(serde_json::json!([{"symbol":"SOLUSDC","positionAmt":"3","entryPrice":"95","markPrice":"100.5","unRealizedProfit":"16.5","liquidationPrice":"50","leverage":"5"}])))
            }
        }))
        .route("/fapi/v2/account", get(move |headers: axum::http::HeaderMap| async move {
            let bal = if api_key(&headers) == "account-b-key" { "250" } else { "5000" };
            Json(serde_json::json!({"assets":[{"asset":"USDC","walletBalance":bal,"marginBalance":bal,"availableBalance":bal,"unrealizedProfit":"0"}]}))
        }))
        .route("/fapi/v1/openOrders", get(move |headers: axum::http::HeaderMap, State(fake): State<DualAccountExchange>| async move {
            let book = if api_key(&headers) == "account-b-key" { &fake.orders_b } else { &fake.orders_a };
            let open: Vec<_> = book.lock().unwrap().values().filter(|o| o.status == "NEW").cloned().collect();
            Json(serde_json::to_value(open).unwrap())
        }))
        .route("/fapi/v1/order", get(move |headers: axum::http::HeaderMap, State(fake): State<DualAccountExchange>, Query(query): Query<HashMap<String, String>>| async move {
            let book = if api_key(&headers) == "account-b-key" { &fake.orders_b } else { &fake.orders_a };
            let orders = book.lock().unwrap();
            let found = if let Some(cid) = query.get("origClientOrderId") {
                orders.get(cid).cloned()
            } else {
                query.get("orderId").and_then(|id| id.parse::<i64>().ok()).and_then(|id| orders.values().find(|o| o.order_id == id).cloned())
            };
            match found {
                Some(o) => (axum::http::StatusCode::OK, Json(serde_json::to_value(o).unwrap())),
                None => (axum::http::StatusCode::BAD_REQUEST, Json(serde_json::json!({"code": -2013, "msg": "Order does not exist"}))),
            }
        }).delete(move |headers: axum::http::HeaderMap, State(fake): State<DualAccountExchange>, Query(query): Query<HashMap<String, String>>| async move {
            let book = if api_key(&headers) == "account-b-key" { &fake.orders_b } else { &fake.orders_a };
            let mut orders = book.lock().unwrap();
            let id: i64 = query["orderId"].parse().unwrap();
            let order = orders.values_mut().find(|o| o.order_id == id).unwrap();
            order.status = "CANCELED".into();
            Json(serde_json::to_value(order.clone()).unwrap())
        }).post(move |headers: axum::http::HeaderMap, State(fake): State<DualAccountExchange>, Query(query): Query<HashMap<String, String>>| async move {
            let is_b = api_key(&headers) == "account-b-key";
            let (book, posts) = if is_b { (&fake.orders_b, &fake.posts_b) } else { (&fake.orders_a, &fake.posts_a) };
            let cid = query["newClientOrderId"].clone();
            posts.lock().unwrap().push(cid.clone());
            let mut orders = book.lock().unwrap();
            let next_id = orders.values().map(|o| o.order_id).max().unwrap_or(100) + 1;
            let order = BinanceOrderResponse {
                order_id: next_id,
                client_order_id: cid.clone(),
                symbol: query["symbol"].clone(),
                status: "NEW".into(),
                price: query["price"].parse().unwrap(),
                avg_price: None,
                orig_qty: query["quantity"].parse().unwrap(),
                executed_qty: dec!(0),
                side: query["side"].clone(),
                order_type: "LIMIT".into(),
                time_in_force: "GTX".into(),
                update_time: None,
            };
            orders.insert(cid, order.clone());
            Json(serde_json::to_value(order).unwrap())
        }))
        .with_state(fake);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, server)
}

#[tokio::test]
async fn live_credential_rotation_cancels_old_account_pauses_and_clears_cached_state() {
    let fake = DualAccountExchange::default();
    fake.orders_b.lock().unwrap().insert(
        "gb_b_preexisting_b".into(),
        BinanceOrderResponse {
            order_id: 900,
            client_order_id: "gb_b_preexisting_b".into(),
            symbol: "SOLUSDC".into(),
            status: "NEW".into(),
            price: dec!(99),
            avg_price: None,
            orig_qty: dec!(1),
            executed_qty: dec!(0),
            side: "BUY".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = dual_account_server(fake.clone()).await;
    let mut config_a = AppConfig::default();
    config_a.exchange.api_key = "account-a-key".into();
    config_a.exchange.api_secret = "live-secret".into();
    config_a.grid.grid_interval = dec!(1);
    config_a.grid.buy_window = 1;
    config_a.grid.sell_window = 1;

    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_runtime_status("SOLUSDC", TradingMode::Live, BotStatus::Running, true)
        .unwrap();
    let (action_tx, action_rx) = mpsc::channel(4);
    let (_ticker_tx, ticker_rx) = broadcast::channel(4);
    let state = AppState::new(config_a.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::with_base_url(
        &config_a.exchange,
        url.clone(),
    ));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    engine.initialize().await.unwrap();
    engine.maintain_grid_window().await;
    assert_eq!(*state.status.read().await, BotStatus::Running);
    assert_eq!(state.position.read().await.size, dec!(3));
    assert_eq!(state.account.read().await.total_wallet_balance, dec!(5000));
    assert!(!fake.posts_a.lock().unwrap().is_empty());

    let mut config_b = config_a.clone();
    config_b.exchange.api_key = "account-b-key".into();
    config_b.exchange.api_secret = "test-secret".into();
    engine.apply_config(config_b.clone()).await.unwrap();

    assert!(fake
        .orders_a
        .lock()
        .unwrap()
        .values()
        .all(|o| o.status == "CANCELED"));
    assert_eq!(
        fake.orders_b.lock().unwrap()["gb_b_preexisting_b"].status,
        "CANCELED"
    );
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert_eq!(state.position.read().await.size, Decimal::ZERO);
    assert_eq!(state.account.read().await.total_wallet_balance, dec!(250));
    assert!(fake.posts_b.lock().unwrap().is_empty());

    engine.maintain_grid_window().await;
    assert!(fake.posts_b.lock().unwrap().is_empty());

    let (reply, rx) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Resume {
            expected_mode: TradingMode::Live,
            expected_epoch: state.pause_epoch(),
            reply,
        })
        .await;
    rx.await.unwrap().unwrap();
    assert_eq!(*state.status.read().await, BotStatus::Running);
    assert!(!fake.posts_b.lock().unwrap().is_empty());

    // Rotating back to Account A and then to Account B with a failing post-commit sync
    // must still wipe Account A's cached position and balance rather than leaking them.
    engine.apply_config(config_a).await.unwrap();
    assert_eq!(state.position.read().await.size, dec!(3));
    fake.b_position_calls.store(0, Ordering::SeqCst);
    fake.fail_b_second_position.store(1, Ordering::SeqCst);
    assert!(engine.apply_config(config_b).await.is_err());
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert_eq!(state.position.read().await.size, Decimal::ZERO);
    assert_eq!(state.account.read().await.total_wallet_balance, dec!(250));
    server.abort();
}

#[tokio::test]
async fn live_recv_window_only_update_replaces_client_without_wiping_runtime_state() {
    use std::sync::atomic::AtomicUsize;

    let open_order_calls = Arc::new(AtomicUsize::new(0));
    let calls_ref = Arc::clone(&open_order_calls);
    let app = axum::Router::new()
        .route(
            "/fapi/v1/time",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({ "serverTime": Utc::now().timestamp_millis() }))
            }),
        )
        .route(
            "/fapi/v1/exchangeInfo",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({
                    "symbols": [{
                        "symbol": "SOLUSDC",
                        "status": "TRADING",
                        "contractType": "PERPETUAL",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "100000", "tickSize": "0.01"},
                            {"filterType": "LOT_SIZE", "minQty": "0.01", "maxQty": "100000", "stepSize": "0.01"},
                            {"filterType": "MIN_NOTIONAL", "notional": "5"}
                        ]
                    }]
                }))
            }),
        )
        .route(
            "/fapi/v1/ticker/price",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({"symbol": "SOLUSDC", "price": "100"}))
            }),
        )
        .route(
            "/fapi/v1/premiumIndex",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({"symbol": "SOLUSDC", "markPrice": "100"}))
            }),
        )
        .route(
            "/fapi/v1/ticker/24hr",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({
                    "lastPrice": "100", "priceChange": "0", "priceChangePercent": "0",
                    "highPrice": "101", "lowPrice": "99", "volume": "10"
                }))
            }),
        )
        .route(
            "/fapi/v1/openOrders",
            axum::routing::get(move || {
                let calls = Arc::clone(&calls_ref);
                async move {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    // Startup sync (0) and pre-switch cancel_all_orders (1, 2) succeed;
                    // post-commit sync (3) fails so pre-existing timestamps & blocked flag remain observable.
                    if n >= 3 {
                        (
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            axum::Json(serde_json::json!({"code": -1008, "msg": "Service Unavailable."})),
                        )
                    } else {
                        (axum::http::StatusCode::OK, axum::Json(serde_json::json!([])))
                    }
                }
            }),
        )
        .route(
            "/fapi/v2/positionRisk",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!([{
                    "symbol": "SOLUSDC", "positionSide": "BOTH", "positionAmt": "2",
                    "entryPrice": "100", "markPrice": "100", "unRealizedProfit": "0",
                    "liquidationPrice": "0", "leverage": "10"
                }]))
            }),
        )
        .route(
            "/fapi/v2/account",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({
                    "assets": [{
                        "asset": "USDC", "walletBalance": "1000", "unrealizedProfit": "0",
                        "marginBalance": "1000", "availableBalance": "1000"
                    }]
                }))
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    config.exchange.recv_window = 5_000;
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&config).unwrap();
    let (action_tx, action_rx) = mpsc::channel(4);
    let (_ticker_tx, ticker_rx) = broadcast::channel(4);
    let state = AppState::new(config.clone(), db, action_tx);
    let initial_client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    let mut engine = GridTradingEngine::new(
        state.clone(),
        Arc::clone(&initial_client),
        action_rx,
        ticker_rx,
    );
    engine.initialize().await.unwrap();

    let pos_ts = Utc::now() - chrono::Duration::seconds(3);
    let acc_ts = Utc::now() - chrono::Duration::seconds(2);
    state.position.write().await.update_time = Some(pos_ts);
    state.account.write().await.update_time = acc_ts;
    engine.pnl_order_ids.insert("gb_s_keep_oid".into(), 98765);
    engine.reconciliation_blocked = true;

    let mut updated = config.clone();
    updated.exchange.recv_window = 15_000;
    // Post-commit sync fails on call 3, returning Err after reset_runtime_scope_after_config_change ran.
    assert!(engine.apply_config(updated).await.is_err());

    assert!(!Arc::ptr_eq(&initial_client, &engine.client));
    assert_ne!(
        initial_client.account_cache_key(),
        engine.client.account_cache_key()
    );
    assert_eq!(state.position.read().await.update_time, Some(pos_ts));
    assert_eq!(state.account.read().await.update_time, acc_ts);
    assert_eq!(engine.pnl_order_ids.get("gb_s_keep_oid"), Some(&98765));
    assert!(engine.reconciliation_blocked);
    server.abort();
}
