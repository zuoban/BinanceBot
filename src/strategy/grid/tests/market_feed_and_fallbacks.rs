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
