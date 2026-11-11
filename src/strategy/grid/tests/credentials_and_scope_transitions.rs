use super::*;

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

#[tokio::test]
async fn scope_transition_blocks_submissions_without_spurious_global_risk_alert_and_fails_closed_on_error(
) {
    use super::super::config_transition::{ConfigScopeReset, ConfigTransitionFlags};

    let mut config_a = AppConfig::default();
    config_a.exchange.api_key = "account-a-key".into();
    config_a.exchange.api_secret = "live-secret".into();
    config_a.global_max_position_notional = Some(dec!(10_000));
    config_a.pause_on_zero_available_margin = true;

    let mut sibling_config = config_a.clone();
    sibling_config.exchange.symbol = "BTCUSDC".into();

    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_runtime_status("SOLUSDC", TradingMode::Live, BotStatus::Running, true)
        .unwrap();
    db.save_runtime_status("BTCUSDC", TradingMode::Live, BotStatus::Running, true)
        .unwrap();

    let unreachable_url = "http://127.0.0.1:1".to_string();
    let mut sol = pair_recovery_engine(&config_a, db.clone(), unreachable_url.clone()).await;
    let sibling = pair_recovery_engine(&sibling_config, db, unreachable_url).await;

    let now = Utc::now();
    for (pair_state, symbol) in [(&sol.state, "SOLUSDC"), (&sibling.state, "BTCUSDC")] {
        *pair_state.status.write().await = BotStatus::Running;
        {
            let mut ticker = pair_state.ticker.write().await;
            ticker.symbol = symbol.into();
            ticker.last_price = dec!(100);
            ticker.mark_price = dec!(100);
            ticker.update_time = now;
            ticker.mark_update_time = now;
        }
        {
            let mut position = pair_state.position.write().await;
            position.symbol = symbol.into();
            position.size = dec!(1);
            position.mark_price = dec!(100);
            position.update_time = Some(now);
        }
        *pair_state.account.write().await = AccountInfo {
            asset: "USDC".into(),
            total_wallet_balance: dec!(1_000),
            available_balance: dec!(1_000),
            margin_balance: dec!(1_000),
            update_time: now,
            ..Default::default()
        };
        pair_state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        pair_state
            .global_position_ready
            .store(true, Ordering::SeqCst);
    }

    let global =
        crate::server::state::GlobalPositionLimit::new(Arc::new(tokio::sync::RwLock::new(vec![
            sol.state.clone(),
            sibling.state.clone(),
        ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    for pair_state in [&sol.state, &sibling.state] {
        let _ = pair_state
            .global_position_limit
            .set(Arc::downgrade(&global));
    }
    assert!(!global.check().await);
    assert!(sol.state.try_order_submission().is_some());
    assert!(sibling.state.try_order_submission().is_some());
    sol.state.try_exchange_submission().unwrap().complete();
    sibling.state.try_exchange_submission().unwrap().complete();

    // (a) Simulate the mid-flight window after reset_runtime_scope_after_config_change
    // and before reconcile_committed_config_transition finishes network I/O.
    let mut config_b = config_a.clone();
    config_b.exchange.api_key = "account-b-key".into();
    config_b.exchange.api_secret = "test-secret".into();
    let flags = ConfigTransitionFlags::from_configs(&config_a, &config_b).unwrap();
    sol.state
        .save_runtime_config(config_b.clone())
        .await
        .unwrap();
    let new_rules = sol.state.rules.read().await.clone();
    sol.reset_runtime_scope_after_config_change(
        &config_b,
        ConfigScopeReset {
            new_client: Arc::new(sol.client.reconfig(&config_b.exchange)),
            new_rules,
            new_price: None,
            paper_state: None,
            flags,
            fetch_rules: false,
        },
    )
    .await;

    assert!(!sol.state.global_position_ready.load(Ordering::SeqCst));
    assert!(!sol.state.global_position_initialized.load(Ordering::SeqCst));
    assert!(sol.state.position.read().await.update_time.is_none());
    assert!(sol.state.account.read().await.asset.is_empty());

    sol.state.check_global_position_limit().await;
    assert!(sol.state.try_order_submission().is_none());
    assert!(sibling.state.try_order_submission().is_none());
    assert!(sol.state.try_exchange_submission().is_none());
    assert!(sibling.state.try_exchange_submission().is_none());
    assert!(!sol.state.global_cancel_pending.load(Ordering::SeqCst));
    assert!(!sibling.state.global_cancel_pending.load(Ordering::SeqCst));
    assert_eq!(*sibling.state.status.read().await, BotStatus::Running);
    assert!(sibling
        .state
        .recent_logs
        .read()
        .await
        .iter()
        .all(|entry| !entry.message.contains("全局风控")));

    // (b) When post-transition reconciliation fails against an unreachable exchange,
    // ScopeTransitionInitGuard restores global_position_initialized = true so global risk
    // fails closed and cancels/pauses across all pairs as intended.
    assert!(sol
        .reconcile_committed_config_transition(
            &config_b,
            flags.market_changed,
            flags.mode_changed,
            flags.credentials_changed,
        )
        .await
        .is_err());
    assert!(sol.state.global_position_initialized.load(Ordering::SeqCst));
    assert!(!sol.state.global_position_ready.load(Ordering::SeqCst));
    sol.state.check_global_position_limit().await;
    assert!(sol.state.global_cancel_pending.load(Ordering::SeqCst));
    assert!(sibling.state.global_cancel_pending.load(Ordering::SeqCst));
    assert_eq!(*sibling.state.status.read().await, BotStatus::Paused);
    assert!(sibling
        .state
        .recent_logs
        .read()
        .await
        .iter()
        .any(|entry| entry.message.contains("全局风控数据不可用或已过期")));
}
