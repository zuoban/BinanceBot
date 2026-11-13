use super::*;

#[derive(Clone)]
struct MarketOrderLogEntry {
    side: String,
    quantity: Decimal,
    reduce_only: bool,
}

#[derive(Clone, Default)]
struct ReverseMockExchange {
    orders: Arc<Mutex<HashMap<String, BinanceOrderResponse>>>,
    market_orders: Arc<Mutex<Vec<MarketOrderLogEntry>>>,
    position_amt: Arc<Mutex<Decimal>>,
    entry_price: Arc<Mutex<Decimal>>,
    mark_price_override: Arc<Mutex<Option<Decimal>>>,
    ticker_price_override: Arc<Mutex<Option<Decimal>>>,
    ticker_price_hits: Arc<AtomicUsize>,
    fail_reduce_market: Arc<AtomicUsize>,
    fail_non_reduce_market: Arc<AtomicUsize>,
    lose_market_response: Arc<AtomicUsize>,
    custom_market_post: Arc<Mutex<Option<BinanceOrderResponse>>>,
    custom_order_lookup: Arc<Mutex<Option<BinanceOrderResponse>>>,
    pause_on_exchange_info: Arc<Mutex<Option<Arc<AppState>>>>,
}

async fn reverse_exchange_server(
    mock: ReverseMockExchange,
) -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new()
        .route(
            "/fapi/v1/time",
            get(|| async {
                Json(serde_json::json!({"serverTime": Utc::now().timestamp_millis()}))
            }),
        )
        .route(
            "/fapi/v1/exchangeInfo",
            get(|State(mock): State<ReverseMockExchange>| async move {
                let maybe_pause = mock.pause_on_exchange_info.lock().unwrap().take();
                if let Some(target_state) = maybe_pause {
                    target_state.pause_trading().await.unwrap();
                }
                Json(serde_json::json!({
                    "symbols": [
                        {
                            "symbol": "SOLUSDC",
                            "status": "TRADING",
                            "contractType": "PERPETUAL",
                            "pricePrecision": 2,
                            "quantityPrecision": 2,
                            "filters": [
                                {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "100000", "tickSize": "0.01"},
                                {"filterType": "LOT_SIZE", "minQty": "0.01", "maxQty": "100000", "stepSize": "0.01"},
                                {"filterType": "MARKET_LOT_SIZE", "minQty": "0.10", "maxQty": "2.00", "stepSize": "0.10"},
                                {"filterType": "MIN_NOTIONAL", "notional": "5"}
                            ]
                        },
                        {
                            "symbol": "ETHUSDC",
                            "status": "TRADING",
                            "contractType": "PERPETUAL",
                            "pricePrecision": 2,
                            "quantityPrecision": 2,
                            "filters": [
                                {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "100000", "tickSize": "0.01"},
                                {"filterType": "LOT_SIZE", "minQty": "0.01", "maxQty": "100000", "stepSize": "0.01"},
                                {"filterType": "MARKET_LOT_SIZE", "minQty": "0.10", "maxQty": "2.00", "stepSize": "0.10"},
                                {"filterType": "MIN_NOTIONAL", "notional": "5"}
                            ]
                        }
                    ]
                }))
            }),
        )
        .route(
            "/fapi/v1/ticker/price",
            get(|State(mock): State<ReverseMockExchange>| async move {
                mock.ticker_price_hits.fetch_add(1, Ordering::SeqCst);
                let price = mock
                    .ticker_price_override
                    .lock()
                    .unwrap()
                    .unwrap_or(dec!(100));
                Json(serde_json::json!({
                    "symbol": "SOLUSDC",
                    "price": price.to_string(),
                    "time": Utc::now().timestamp_millis()
                }))
            }),
        )
        .route(
            "/fapi/v2/positionRisk",
            get(
                |State(mock): State<ReverseMockExchange>,
                 Query(query): Query<HashMap<String, String>>| async move {
                    let symbol = query
                        .get("symbol")
                        .cloned()
                        .unwrap_or_else(|| "SOLUSDC".to_string());
                    let (size, entry) = if symbol == "SOLUSDC" {
                        (
                            *mock.position_amt.lock().unwrap(),
                            *mock.entry_price.lock().unwrap(),
                        )
                    } else {
                        (Decimal::ZERO, Decimal::ZERO)
                    };
                    let mark = mock
                        .mark_price_override
                        .lock()
                        .unwrap()
                        .unwrap_or(dec!(100));
                    Json(serde_json::json!([{
                        "symbol": symbol,
                        "positionAmt": size.to_string(),
                        "entryPrice": entry.to_string(),
                        "markPrice": mark.to_string(),
                        "unRealizedProfit": "0",
                        "liquidationPrice": "0",
                        "leverage": "5"
                    }]))
                },
            ),
        )
        .route(
            "/fapi/v2/account",
            get(|| async {
                Json(serde_json::json!({
                    "totalWalletBalance": "10000",
                    "totalMarginBalance": "10000",
                    "totalUnrealizedProfit": "0",
                    "availableBalance": "9000",
                    "assets": [{
                        "asset": "USDC",
                        "walletBalance": "10000",
                        "marginBalance": "10000",
                        "availableBalance": "9000",
                        "unrealizedProfit": "0"
                    }]
                }))
            }),
        )
        .route(
            "/fapi/v1/userTrades",
            get(
                |State(mock): State<ReverseMockExchange>,
                 Query(query): Query<HashMap<String, String>>| async move {
                    let target_id: i64 = query["orderId"].parse().unwrap();
                    let qty = mock
                        .orders
                        .lock()
                        .unwrap()
                        .values()
                        .find(|order| order.order_id == target_id)
                        .map(|order| order.executed_qty)
                        .unwrap_or(Decimal::ONE);
                    Json(serde_json::json!([{
                        "id": target_id * 10 + 1,
                        "orderId": target_id,
                        "qty": qty.to_string(),
                        "realizedPnl": "1.25",
                        "commission": "0.05",
                        "commissionAsset": "USDC",
                        "maker": false
                    }]))
                },
            ),
        )
        .route(
            "/fapi/v1/openOrders",
            get(|State(mock): State<ReverseMockExchange>| async move {
                let open_orders: Vec<_> = mock
                    .orders
                    .lock()
                    .unwrap()
                    .values()
                    .filter(|order| matches!(order.status.as_str(), "NEW" | "PARTIALLY_FILLED"))
                    .cloned()
                    .collect();
                Json(serde_json::to_value(open_orders).unwrap())
            }),
        )
        .route(
            "/fapi/v1/order",
            get(
                |State(mock): State<ReverseMockExchange>,
                 Query(query): Query<HashMap<String, String>>| async move {
                    if let Some(mut custom) = mock.custom_order_lookup.lock().unwrap().clone() {
                        if custom.client_order_id.is_empty() {
                            if let Some(cid) = query.get("origClientOrderId") {
                                custom.client_order_id = cid.clone();
                            }
                        }
                        return (
                            axum::http::StatusCode::OK,
                            Json(serde_json::to_value(custom).unwrap()),
                        );
                    }
                    let orders = mock.orders.lock().unwrap();
                    let found = if let Some(cid) = query.get("origClientOrderId") {
                        orders.get(cid).cloned()
                    } else {
                        query
                            .get("orderId")
                            .and_then(|raw| raw.parse::<i64>().ok())
                            .and_then(|target_id| {
                                orders
                                    .values()
                                    .find(|order| order.order_id == target_id)
                                    .cloned()
                            })
                    };
                    match found {
                        Some(order) => (
                            axum::http::StatusCode::OK,
                            Json(serde_json::to_value(order).unwrap()),
                        ),
                        None => (
                            axum::http::StatusCode::BAD_REQUEST,
                            Json(serde_json::json!({"code": -2013, "msg": "Order does not exist"})),
                        ),
                    }
                },
            )
            .delete(
                |State(mock): State<ReverseMockExchange>,
                 Query(query): Query<HashMap<String, String>>| async move {
                    let mut orders = mock.orders.lock().unwrap();
                    let found = if let Some(raw_id) = query.get("orderId") {
                        let target_id: i64 = raw_id.parse().unwrap();
                        orders
                            .values_mut()
                            .find(|order| order.order_id == target_id)
                    } else {
                        let cid = &query["origClientOrderId"];
                        orders
                            .values_mut()
                            .find(|order| &order.client_order_id == cid)
                    };
                    let Some(order) = found else {
                        return (
                            axum::http::StatusCode::BAD_REQUEST,
                            Json(serde_json::json!({"code": -2011, "msg": "Unknown order sent."})),
                        );
                    };
                    order.status = "CANCELED".into();
                    (
                        axum::http::StatusCode::OK,
                        Json(serde_json::to_value(order.clone()).unwrap()),
                    )
                },
            )
            .post(
                |State(mock): State<ReverseMockExchange>,
                 Query(query): Query<HashMap<String, String>>| async move {
                    let order_type = query
                        .get("type")
                        .cloned()
                        .unwrap_or_else(|| "LIMIT".to_string());
                    let cid = query["newClientOrderId"].clone();
                    let side = query["side"].clone();
                    let qty: Decimal = query["quantity"].parse().unwrap();
                    let reduce_only = query
                        .get("reduceOnly")
                        .is_some_and(|val| val.eq_ignore_ascii_case("true"));
                    if order_type == "MARKET" {
                        if let Some(mut custom) = mock.custom_market_post.lock().unwrap().take() {
                            if custom.client_order_id.is_empty() {
                                custom.client_order_id = cid;
                            }
                            return (
                                axum::http::StatusCode::OK,
                                Json(serde_json::to_value(custom).unwrap()),
                            );
                        }
                        if reduce_only && consume_failure(&mock.fail_reduce_market) {
                            return (
                                axum::http::StatusCode::BAD_REQUEST,
                                Json(serde_json::json!({"code": -2022, "msg": "ReduceOnly Order is rejected."})),
                            );
                        }
                        if !reduce_only && consume_failure(&mock.fail_non_reduce_market) {
                            return (
                                axum::http::StatusCode::BAD_REQUEST,
                                Json(serde_json::json!({"code": -2019, "msg": "Margin is insufficient."})),
                            );
                        }
                        mock.market_orders.lock().unwrap().push(MarketOrderLogEntry {
                            side: side.clone(),
                            quantity: qty,
                            reduce_only,
                        });
                        {
                            let mut pos = mock.position_amt.lock().unwrap();
                            let mut entry = mock.entry_price.lock().unwrap();
                            let delta = if side == "BUY" { qty } else { -qty };
                            *pos += delta;
                            *entry = if pos.is_zero() {
                                Decimal::ZERO
                            } else {
                                dec!(100)
                            };
                        }
                        let mut orders = mock.orders.lock().unwrap();
                        let order_id = orders
                            .values()
                            .map(|order| order.order_id)
                            .max()
                            .unwrap_or(999)
                            + 1;
                        let filled = BinanceOrderResponse {
                            order_id,
                            client_order_id: cid.clone(),
                            symbol: query["symbol"].clone(),
                            status: "FILLED".into(),
                            price: Decimal::ZERO,
                            avg_price: Some(dec!(100)),
                            orig_qty: qty,
                            executed_qty: qty,
                            side,
                            order_type: "MARKET".into(),
                            time_in_force: "GTC".into(),
                            update_time: Some(Utc::now().timestamp_millis()),
                        };
                        orders.insert(cid, filled.clone());
                        if consume_failure(&mock.lose_market_response) {
                            return (
                                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                                Json(serde_json::json!({"code": -1000, "msg": "Timeout"})),
                            );
                        }
                        return (
                            axum::http::StatusCode::OK,
                            Json(serde_json::to_value(filled).unwrap()),
                        );
                    }
                    let price: Decimal = query["price"].parse().unwrap();
                    let mut orders = mock.orders.lock().unwrap();
                    let order_id = orders
                        .values()
                        .map(|order| order.order_id)
                        .max()
                        .unwrap_or(999)
                        + 1;
                    let created = BinanceOrderResponse {
                        order_id,
                        client_order_id: cid.clone(),
                        symbol: query["symbol"].clone(),
                        status: "NEW".into(),
                        price,
                        avg_price: None,
                        orig_qty: qty,
                        executed_qty: Decimal::ZERO,
                        side,
                        order_type: "LIMIT".into(),
                        time_in_force: "GTX".into(),
                        update_time: None,
                    };
                    orders.insert(cid, created.clone());
                    (
                        axum::http::StatusCode::OK,
                        Json(serde_json::to_value(created).unwrap()),
                    )
                },
            ),
        )
        .route(
            "/fapi/v1/batchOrders",
            delete(
                |State(mock): State<ReverseMockExchange>,
                 Query(query): Query<HashMap<String, String>>| async move {
                    let id_targets = query
                        .get("orderIdList")
                        .and_then(|raw| serde_json::from_str::<Vec<i64>>(raw).ok());
                    let cid_targets = query
                        .get("origClientOrderIdList")
                        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok());
                    let count = id_targets
                        .as_ref()
                        .map_or_else(|| cid_targets.as_ref().map_or(0, Vec::len), Vec::len);
                    let mut orders = mock.orders.lock().unwrap();
                    let mut items = Vec::with_capacity(count);
                    for idx in 0..count {
                        let found = if let Some(ref ids) = id_targets {
                            let target_id = ids[idx];
                            orders
                                .values_mut()
                                .find(|order| order.order_id == target_id)
                        } else {
                            let target_cid = &cid_targets.as_ref().unwrap()[idx];
                            orders
                                .values_mut()
                                .find(|order| &order.client_order_id == target_cid)
                        };
                        match found {
                            Some(order) if !order.is_terminal() => {
                                order.status = "CANCELED".into();
                                items.push(serde_json::to_value(order.clone()).unwrap());
                            }
                            _ => {
                                items.push(
                                    serde_json::json!({"code": -2011, "msg": "Unknown order sent."}),
                                );
                            }
                        }
                    }
                    (
                        axum::http::StatusCode::OK,
                        Json(serde_json::Value::Array(items)),
                    )
                },
            )
            .post(
                |State(mock): State<ReverseMockExchange>,
                 Query(query): Query<HashMap<String, String>>| async move {
                    #[derive(serde::Deserialize)]
                    struct WireBatchOrder {
                        symbol: String,
                        side: String,
                        price: Decimal,
                        quantity: Decimal,
                        #[serde(rename = "newClientOrderId")]
                        client_order_id: String,
                        #[serde(rename = "timeInForce")]
                        time_in_force: String,
                    }
                    let raw_items: Vec<WireBatchOrder> =
                        serde_json::from_str(&query["batchOrders"]).unwrap();
                    let mut orders = mock.orders.lock().unwrap();
                    let base_id = orders
                        .values()
                        .map(|order| order.order_id)
                        .max()
                        .unwrap_or(999)
                        + 1;
                    let mut out = Vec::with_capacity(raw_items.len());
                    for (next_id, item) in (base_id..).zip(raw_items) {
                        let created = BinanceOrderResponse {
                            order_id: next_id,
                            client_order_id: item.client_order_id.clone(),
                            symbol: item.symbol,
                            status: "NEW".into(),
                            price: item.price,
                            avg_price: None,
                            orig_qty: item.quantity,
                            executed_qty: Decimal::ZERO,
                            side: item.side,
                            order_type: "LIMIT".into(),
                            time_in_force: item.time_in_force,
                            update_time: None,
                        };
                        orders.insert(item.client_order_id, created.clone());
                        out.push(serde_json::to_value(created).unwrap());
                    }
                    (
                        axum::http::StatusCode::OK,
                        Json(serde_json::Value::Array(out)),
                    )
                },
            ),
        )
        .with_state(mock);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, server)
}

#[tokio::test]
async fn paper_reverse_flips_long_and_short_positions_and_preserves_status() {
    let mut config = AppConfig::default();
    config.grid.direction = GridDirection::Long;
    config.grid.grid_interval = dec!(1);
    config.grid.order_amount_usdc = dec!(100);
    config.grid.buy_window = 2;
    config.grid.sell_window = 2;
    config.grid.max_position_usdc = Some(dec!(1000));
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&config).unwrap();
    db.save_bot_status(BotStatus::Running).unwrap();
    let (action_tx, action_rx) = mpsc::channel(4);
    let (_ticker_tx, ticker_rx) = broadcast::channel(4);
    let state = AppState::new(config.clone(), db.clone(), action_tx);
    *state.status.write().await = BotStatus::Running;
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(100);
        ticker.mark_price = dec!(100);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }
    {
        let mut position = state.position.write().await;
        position.symbol = "SOLUSDC".into();
        position.size = dec!(1.5);
        position.entry_price = dec!(98);
        position.mark_price = dec!(100);
        position.leverage = 5;
    }
    let initial_wallet = state.account.read().await.total_wallet_balance;

    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    engine.maintain_grid_window().await;
    assert!(!state.active_orders.read().await.is_empty());

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Reverse {
            expected_mode: TradingMode::Paper,
            expected_direction: GridDirection::Long,
            reply: reply_tx,
        })
        .await;
    assert_eq!(reply_rx.await.unwrap(), Ok(()));
    assert_eq!(*state.status.read().await, BotStatus::Running);
    assert_eq!(
        state.config.read().await.grid.direction,
        GridDirection::Short
    );
    assert_eq!(
        db.load_config().unwrap().unwrap().grid.direction,
        GridDirection::Short
    );

    let short_pos = state.position.read().await.clone();
    assert_eq!(short_pos.size, dec!(-1.5));
    assert_eq!(short_pos.entry_price, dec!(100));
    assert_eq!(
        state.account.read().await.total_wallet_balance,
        initial_wallet + dec!(3)
    );
    assert!(db
        .last_fill_for("SOLUSDC", TradingMode::Paper)
        .unwrap()
        .is_none());

    let active_after_short: Vec<_> = state.active_orders.read().await.values().cloned().collect();
    assert!(active_after_short
        .iter()
        .any(|order| order.side == OrderSide::Sell && order.price == dec!(101)));
    assert!(active_after_short
        .iter()
        .any(|order| order.side == OrderSide::Buy && order.price == dec!(99)));

    engine.handle_control_action(BotControlAction::Pause).await;
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(95);
        ticker.mark_price = dec!(95);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }

    let (reply_tx2, reply_rx2) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Reverse {
            expected_mode: TradingMode::Paper,
            expected_direction: GridDirection::Short,
            reply: reply_tx2,
        })
        .await;
    assert_eq!(reply_rx2.await.unwrap(), Ok(()));
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert!(state.active_orders.read().await.is_empty());
    assert_eq!(
        state.config.read().await.grid.direction,
        GridDirection::Long
    );
    let long_pos = state.position.read().await.clone();
    assert_eq!(long_pos.size, dec!(1.5));
    assert_eq!(long_pos.entry_price, dec!(95));
    assert_eq!(
        state.account.read().await.total_wallet_balance,
        initial_wallet + dec!(10.5)
    );

    let trades = db.get_recent_trades(10).unwrap();
    assert_eq!(trades.len(), 4);
    assert!(trades
        .iter()
        .all(|trade| trade.is_reverse_trade() && !trade.is_maker));
}

#[tokio::test]
async fn paper_flat_reverse_switches_direction_without_market_orders() {
    let mut config = AppConfig::default();
    config.grid.direction = GridDirection::Long;
    config.grid.grid_interval = dec!(1);
    config.grid.order_amount_usdc = dec!(100);
    config.grid.buy_window = 2;
    config.grid.sell_window = 2;
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&config).unwrap();
    let (action_tx, action_rx) = mpsc::channel(4);
    let (_ticker_tx, ticker_rx) = broadcast::channel(4);
    let state = AppState::new(config.clone(), db.clone(), action_tx);
    *state.status.write().await = BotStatus::Running;
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(100);
        ticker.mark_price = dec!(100);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    engine.maintain_grid_window().await;
    assert!(state
        .active_orders
        .read()
        .await
        .values()
        .all(|order| order.side == OrderSide::Buy));

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Reverse {
            expected_mode: TradingMode::Paper,
            expected_direction: GridDirection::Long,
            reply: reply_tx,
        })
        .await;
    assert_eq!(reply_rx.await.unwrap(), Ok(()));
    assert_eq!(
        state.config.read().await.grid.direction,
        GridDirection::Short
    );
    assert!(state.position.read().await.size.is_zero());
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    assert!(state
        .active_orders
        .read()
        .await
        .values()
        .all(|order| order.side == OrderSide::Sell));
}

#[tokio::test]
async fn live_reverse_chunks_reduce_only_close_flips_direction_opens_and_reconciles() {
    let mock = ReverseMockExchange {
        position_amt: Arc::new(Mutex::new(dec!(3.0))),
        entry_price: Arc::new(Mutex::new(dec!(99.0))),
        ..ReverseMockExchange::default()
    };
    let (url, server) = reverse_exchange_server(mock.clone()).await;

    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    config.grid.direction = GridDirection::Long;
    config.grid.grid_interval = dec!(1);
    config.grid.order_amount_usdc = dec!(100);
    config.grid.buy_window = 1;
    config.grid.sell_window = 1;
    config.grid.max_position_usdc = Some(dec!(1000));

    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&config).unwrap();
    db.save_bot_status(BotStatus::Running).unwrap();
    let (action_tx, action_rx) = mpsc::channel(4);
    let (_ticker_tx, ticker_rx) = broadcast::channel(4);
    let state = AppState::new(config.clone(), db.clone(), action_tx);
    *state.status.write().await = BotStatus::Running;
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(100);
        ticker.mark_price = dec!(100);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }

    let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    assert!(engine.sync_live_orders().await);
    engine.sync_account_and_position().await;
    engine.load_exchange_rules("SOLUSDC").await.unwrap();
    engine.maintain_grid_window().await;
    assert!(!state.active_orders.read().await.is_empty());

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Reverse {
            expected_mode: TradingMode::Live,
            expected_direction: GridDirection::Long,
            reply: reply_tx,
        })
        .await;
    assert_eq!(reply_rx.await.unwrap(), Ok(()));
    assert_eq!(*state.status.read().await, BotStatus::Running);
    assert_eq!(
        state.config.read().await.grid.direction,
        GridDirection::Short
    );
    assert_eq!(state.position.read().await.size, dec!(-3.0));

    let market_log = mock.market_orders.lock().unwrap().clone();
    assert_eq!(market_log.len(), 4);
    assert_eq!(market_log[0].side, "SELL");
    assert_eq!(market_log[0].quantity, dec!(2.0));
    assert!(market_log[0].reduce_only);
    assert_eq!(market_log[1].side, "SELL");
    assert_eq!(market_log[1].quantity, dec!(1.0));
    assert!(market_log[1].reduce_only);
    assert_eq!(market_log[2].side, "SELL");
    assert_eq!(market_log[2].quantity, dec!(2.0));
    assert!(!market_log[2].reduce_only);
    assert_eq!(market_log[3].side, "SELL");
    assert_eq!(market_log[3].quantity, dec!(1.0));
    assert!(!market_log[3].reduce_only);

    if let Some(task) = engine.pnl_reconcile_task.take() {
        let _ = task.await;
    }
    let trades = db.get_recent_trades(10).unwrap();
    assert_eq!(trades.len(), 4);
    assert!(trades
        .iter()
        .all(|trade| trade.pnl_verified && trade.is_reverse_trade() && !trade.is_maker));

    server.abort();
}

#[tokio::test]
async fn live_reverse_halts_safely_on_open_rejection_and_recovers_lost_market_response() {
    let mock = ReverseMockExchange {
        position_amt: Arc::new(Mutex::new(dec!(1.5))),
        entry_price: Arc::new(Mutex::new(dec!(100.0))),
        fail_non_reduce_market: Arc::new(AtomicUsize::new(1)),
        ..ReverseMockExchange::default()
    };
    let (url, server) = reverse_exchange_server(mock.clone()).await;

    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    config.grid.direction = GridDirection::Long;
    config.grid.grid_interval = dec!(1);
    config.grid.order_amount_usdc = dec!(100);
    config.grid.max_position_usdc = Some(dec!(1000));

    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&config).unwrap();
    db.save_bot_status(BotStatus::Running).unwrap();
    let (action_tx, action_rx) = mpsc::channel(4);
    let (_ticker_tx, ticker_rx) = broadcast::channel(4);
    let state = AppState::new(config.clone(), db.clone(), action_tx);
    *state.status.write().await = BotStatus::Running;
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(100);
        ticker.mark_price = dec!(100);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }

    let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    engine.sync_account_and_position().await;

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Reverse {
            expected_mode: TradingMode::Live,
            expected_direction: GridDirection::Long,
            reply: reply_tx,
        })
        .await;
    let failure = reply_rx.await.unwrap().unwrap_err();
    assert!(failure.contains("反向市价开仓未完成"), "{failure}");
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    assert_eq!(
        state.config.read().await.grid.direction,
        GridDirection::Short
    );
    assert!(state.position.read().await.size.is_zero());

    *mock.position_amt.lock().unwrap() = dec!(-1.0);
    *mock.entry_price.lock().unwrap() = dec!(100.0);
    mock.lose_market_response.store(1, Ordering::SeqCst);

    let (reply_tx2, reply_rx2) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Reverse {
            expected_mode: TradingMode::Live,
            expected_direction: GridDirection::Short,
            reply: reply_tx2,
        })
        .await;
    assert_eq!(reply_rx2.await.unwrap(), Ok(()));
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert_eq!(
        state.config.read().await.grid.direction,
        GridDirection::Long
    );
    assert_eq!(state.position.read().await.size, dec!(1.0));

    server.abort();
}

#[tokio::test]
async fn reverse_rejects_stale_mode_or_direction_and_pauses_on_position_sign_mismatch() {
    let mut config = AppConfig::default();
    config.grid.direction = GridDirection::Long;
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&config).unwrap();
    let (action_tx, action_rx) = mpsc::channel(4);
    let (_ticker_tx, ticker_rx) = broadcast::channel(4);
    let state = AppState::new(config.clone(), db, action_tx);
    *state.status.write().await = BotStatus::Running;
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);

    let (mode_tx, mode_rx) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Reverse {
            expected_mode: TradingMode::Live,
            expected_direction: GridDirection::Long,
            reply: mode_tx,
        })
        .await;
    assert!(mode_rx
        .await
        .unwrap()
        .unwrap_err()
        .contains("运行模式已改变"));
    assert_eq!(*state.status.read().await, BotStatus::Running);

    let (dir_tx, dir_rx) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Reverse {
            expected_mode: TradingMode::Paper,
            expected_direction: GridDirection::Short,
            reply: dir_tx,
        })
        .await;
    assert!(dir_rx
        .await
        .unwrap()
        .unwrap_err()
        .contains("交易方向已改变"));
    assert_eq!(*state.status.read().await, BotStatus::Running);

    state.position.write().await.size = dec!(-1.0);
    let (mismatch_tx, mismatch_rx) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Reverse {
            expected_mode: TradingMode::Paper,
            expected_direction: GridDirection::Long,
            reply: mismatch_tx,
        })
        .await;
    assert!(mismatch_rx
        .await
        .unwrap()
        .unwrap_err()
        .contains("持仓方向与策略方向不一致"));
    assert_eq!(*state.status.read().await, BotStatus::Paused);
}

#[tokio::test]
async fn live_reverse_deterministic_market_rejection_preserves_global_risk_readiness() {
    let mock = ReverseMockExchange {
        position_amt: Arc::new(Mutex::new(dec!(1.0))),
        entry_price: Arc::new(Mutex::new(dec!(100.0))),
        fail_reduce_market: Arc::new(AtomicUsize::new(1)),
        fail_non_reduce_market: Arc::new(AtomicUsize::new(1)),
        ..ReverseMockExchange::default()
    };
    let (url, server) = reverse_exchange_server(mock.clone()).await;

    let mut sol_config = AppConfig::default();
    sol_config.exchange.symbol = "SOLUSDC".into();
    sol_config.exchange.api_key = "live-key".into();
    sol_config.exchange.api_secret = "live-secret".into();
    sol_config.grid.direction = GridDirection::Long;
    sol_config.global_max_position_notional = Some(dec!(5000));
    sol_config.pause_on_zero_available_margin = true;

    let mut eth_config = sol_config.clone();
    eth_config.exchange.symbol = "ETHUSDC".into();

    let sol_db = Arc::new(Database::open(":memory:").unwrap());
    sol_db.save_config(&sol_config).unwrap();
    sol_db.save_bot_status(BotStatus::Running).unwrap();
    let (sol_tx, sol_rx) = mpsc::channel(4);
    let (_sol_ticker_tx, sol_ticker_rx) = broadcast::channel(4);
    let sol_state = AppState::new(sol_config.clone(), sol_db, sol_tx);
    *sol_state.status.write().await = BotStatus::Running;

    let eth_db = Arc::new(Database::open(":memory:").unwrap());
    eth_db.save_config(&eth_config).unwrap();
    eth_db.save_bot_status(BotStatus::Running).unwrap();
    let (eth_tx, eth_rx) = mpsc::channel(4);
    let (_eth_ticker_tx, eth_ticker_rx) = broadcast::channel(4);
    let eth_state = AppState::new(eth_config.clone(), eth_db, eth_tx);
    *eth_state.status.write().await = BotStatus::Running;

    for pair_state in [&sol_state, &eth_state] {
        let mut ticker = pair_state.ticker.write().await;
        ticker.last_price = dec!(100);
        ticker.mark_price = dec!(100);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }

    let global =
        crate::server::state::GlobalPositionLimit::new(Arc::new(tokio::sync::RwLock::new(vec![
            sol_state.clone(),
            eth_state.clone(),
        ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    for pair_state in [&sol_state, &eth_state] {
        let _ = pair_state
            .global_position_limit
            .set(Arc::downgrade(&global));
    }

    let sol_client = Arc::new(BinanceFuturesClient::with_base_url(
        &sol_config.exchange,
        url.clone(),
    ));
    let mut sol_engine =
        GridTradingEngine::new(sol_state.clone(), sol_client, sol_rx, sol_ticker_rx);
    let eth_client = Arc::new(BinanceFuturesClient::with_base_url(
        &eth_config.exchange,
        url,
    ));
    let mut eth_engine =
        GridTradingEngine::new(eth_state.clone(), eth_client, eth_rx, eth_ticker_rx);

    assert!(sol_engine.sync_live_orders().await);
    assert!(sol_engine.sync_account_and_position().await);
    assert!(eth_engine.sync_live_orders().await);
    assert!(eth_engine.sync_account_and_position().await);
    for pair_state in [&sol_state, &eth_state] {
        pair_state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        pair_state
            .global_position_ready
            .store(true, Ordering::SeqCst);
    }
    assert!(!global.check().await);
    assert!(eth_state.try_order_submission().is_some());

    let (close_reject_tx, close_reject_rx) = tokio::sync::oneshot::channel();
    sol_engine
        .handle_control_action(BotControlAction::Reverse {
            expected_mode: TradingMode::Live,
            expected_direction: GridDirection::Long,
            reply: close_reject_tx,
        })
        .await;
    let close_err = close_reject_rx.await.unwrap().unwrap_err();
    assert!(close_err.contains("市价平仓未完成"), "{close_err}");
    assert!(sol_state.global_position_ready.load(Ordering::SeqCst));
    assert!(!global.check().await);
    assert_eq!(*eth_state.status.read().await, BotStatus::Running);
    assert!(!eth_state.global_cancel_pending.load(Ordering::SeqCst));
    assert!(eth_state.try_order_submission().is_some());

    let (open_reject_tx, open_reject_rx) = tokio::sync::oneshot::channel();
    sol_engine
        .handle_control_action(BotControlAction::Reverse {
            expected_mode: TradingMode::Live,
            expected_direction: GridDirection::Long,
            reply: open_reject_tx,
        })
        .await;
    let open_err = open_reject_rx.await.unwrap().unwrap_err();
    assert!(open_err.contains("反向市价开仓未完成"), "{open_err}");
    assert!(sol_state.global_position_ready.load(Ordering::SeqCst));
    assert!(!global.check().await);
    assert_eq!(*eth_state.status.read().await, BotStatus::Running);
    assert!(!eth_state.global_cancel_pending.load(Ordering::SeqCst));
    assert!(eth_state.try_order_submission().is_some());

    server.abort();
}

#[tokio::test]
async fn partially_filled_terminal_market_order_without_avg_price_blocks_reconciliation() {
    use super::super::snapshot_identities::invalid_snapshot_price;

    let unfilled_expired = BinanceOrderResponse {
        order_id: 3001,
        client_order_id: "grid_s_unfilled".into(),
        symbol: "SOLUSDC".into(),
        status: "EXPIRED".into(),
        price: Decimal::ZERO,
        avg_price: None,
        orig_qty: dec!(1.0),
        executed_qty: Decimal::ZERO,
        side: "SELL".into(),
        order_type: "MARKET".into(),
        time_in_force: "GTC".into(),
        update_time: Some(Utc::now().timestamp_millis()),
    };
    assert!(!invalid_snapshot_price(&unfilled_expired));

    let partial_expired_missing_avg = BinanceOrderResponse {
        executed_qty: dec!(0.5),
        ..unfilled_expired.clone()
    };
    assert!(invalid_snapshot_price(&partial_expired_missing_avg));

    let partial_expired_zero_avg = BinanceOrderResponse {
        status: "EXPIRED_IN_MATCH".into(),
        executed_qty: dec!(0.5),
        avg_price: Some(Decimal::ZERO),
        ..unfilled_expired.clone()
    };
    assert!(invalid_snapshot_price(&partial_expired_zero_avg));

    let partial_expired_valid_avg = BinanceOrderResponse {
        executed_qty: dec!(0.5),
        avg_price: Some(dec!(100.5)),
        ..unfilled_expired
    };
    assert!(!invalid_snapshot_price(&partial_expired_valid_avg));

    let mock = ReverseMockExchange {
        position_amt: Arc::new(Mutex::new(dec!(1.0))),
        entry_price: Arc::new(Mutex::new(dec!(98.0))),
        custom_market_post: Arc::new(Mutex::new(Some(BinanceOrderResponse {
            order_id: 3010,
            client_order_id: String::new(),
            symbol: "SOLUSDC".into(),
            status: "EXPIRED".into(),
            price: Decimal::ZERO,
            avg_price: None,
            orig_qty: dec!(1.0),
            executed_qty: dec!(0.5),
            side: "SELL".into(),
            order_type: "MARKET".into(),
            time_in_force: "GTC".into(),
            update_time: Some(Utc::now().timestamp_millis()),
        }))),
        ..ReverseMockExchange::default()
    };
    let (url, server) = reverse_exchange_server(mock).await;

    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    config.grid.direction = GridDirection::Long;

    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&config).unwrap();
    let (action_tx, action_rx) = mpsc::channel(4);
    let (_ticker_tx, ticker_rx) = broadcast::channel(4);
    let state = AppState::new(config.clone(), db.clone(), action_tx);
    *state.status.write().await = BotStatus::Running;

    let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    assert!(engine.sync_live_orders().await);
    assert!(engine.sync_account_and_position().await);

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Reverse {
            expected_mode: TradingMode::Live,
            expected_direction: GridDirection::Long,
            reply: reply_tx,
        })
        .await;
    let err = reply_rx.await.unwrap().unwrap_err();
    assert!(err.contains("市价单回执异常"), "{err}");
    assert!(engine.reconciliation_blocked);
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert!(db.get_recent_trades(10).unwrap().is_empty());

    server.abort();
}

#[tokio::test]
async fn conflicting_market_order_receipt_and_poll_preserve_durable_order_id() {
    let mock = ReverseMockExchange {
        position_amt: Arc::new(Mutex::new(dec!(1.0))),
        entry_price: Arc::new(Mutex::new(dec!(100.0))),
        custom_market_post: Arc::new(Mutex::new(Some(BinanceOrderResponse {
            order_id: 8888,
            client_order_id: String::new(),
            symbol: "SOLUSDC".into(),
            status: "FILLED".into(),
            price: Decimal::ZERO,
            avg_price: Some(dec!(100)),
            orig_qty: dec!(1.0),
            executed_qty: dec!(1.0),
            side: "BUY".into(),
            order_type: "MARKET".into(),
            time_in_force: "GTC".into(),
            update_time: Some(Utc::now().timestamp_millis()),
        }))),
        ..ReverseMockExchange::default()
    };
    let (url, server) = reverse_exchange_server(mock.clone()).await;

    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    config.grid.direction = GridDirection::Long;

    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&config).unwrap();
    let (action_tx, action_rx) = mpsc::channel(4);
    let (_ticker_tx, ticker_rx) = broadcast::channel(4);
    let state = AppState::new(config.clone(), db.clone(), action_tx);
    *state.status.write().await = BotStatus::Running;

    let client = Arc::new(BinanceFuturesClient::with_base_url(
        &config.exchange,
        url.clone(),
    ));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Reverse {
            expected_mode: TradingMode::Live,
            expected_direction: GridDirection::Long,
            reply: reply_tx,
        })
        .await;
    assert!(reply_rx.await.unwrap().is_err());
    assert!(engine.reconciliation_blocked);
    let managed_after_receipt = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(managed_after_receipt.len(), 1);
    assert_eq!(managed_after_receipt[0].order_id, None);
    let active_after_receipt: Vec<_> = state.active_orders.read().await.values().cloned().collect();
    assert_eq!(active_after_receipt.len(), 1);
    assert_eq!(active_after_receipt[0].order_id, None);

    let db2 = Arc::new(Database::open(":memory:").unwrap());
    db2.save_config(&config).unwrap();
    let (action_tx2, action_rx2) = mpsc::channel(4);
    let (_ticker_tx2, ticker_rx2) = broadcast::channel(4);
    let state2 = AppState::new(config.clone(), db2.clone(), action_tx2);
    *state2.status.write().await = BotStatus::Running;

    *mock.custom_market_post.lock().unwrap() = Some(BinanceOrderResponse {
        order_id: 2001,
        client_order_id: String::new(),
        symbol: "SOLUSDC".into(),
        status: "NEW".into(),
        price: Decimal::ZERO,
        avg_price: None,
        orig_qty: dec!(1.0),
        executed_qty: Decimal::ZERO,
        side: "SELL".into(),
        order_type: "MARKET".into(),
        time_in_force: "GTC".into(),
        update_time: Some(Utc::now().timestamp_millis()),
    });
    *mock.custom_order_lookup.lock().unwrap() = Some(BinanceOrderResponse {
        order_id: 9999,
        client_order_id: String::new(),
        symbol: "SOLUSDC".into(),
        status: "FILLED".into(),
        price: Decimal::ZERO,
        avg_price: Some(dec!(100)),
        orig_qty: dec!(1.0),
        executed_qty: dec!(1.0),
        side: "SELL".into(),
        order_type: "MARKET".into(),
        time_in_force: "GTC".into(),
        update_time: Some(Utc::now().timestamp_millis()),
    });

    let client2 = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    let mut engine2 = GridTradingEngine::new(state2.clone(), client2, action_rx2, ticker_rx2);

    let (reply_tx2, reply_rx2) = tokio::sync::oneshot::channel();
    engine2
        .handle_control_action(BotControlAction::Reverse {
            expected_mode: TradingMode::Live,
            expected_direction: GridDirection::Long,
            reply: reply_tx2,
        })
        .await;
    assert!(reply_rx2.await.unwrap().is_err());
    assert!(engine2.reconciliation_blocked);
    let managed_after_poll = db2.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(managed_after_poll.len(), 1);
    assert_eq!(managed_after_poll[0].order_id, Some(2001));
    let active_after_poll: Vec<_> = state2
        .active_orders
        .read()
        .await
        .values()
        .cloned()
        .collect();
    assert_eq!(active_after_poll.len(), 1);
    assert_eq!(active_after_poll[0].order_id, Some(2001));

    server.abort();
}

#[tokio::test]
async fn reverse_enforces_ticker_freshness_in_paper_and_live_modes() {
    let mut paper_config = AppConfig::default();
    paper_config.grid.direction = GridDirection::Long;
    let paper_db = Arc::new(Database::open(":memory:").unwrap());
    paper_db.save_config(&paper_config).unwrap();
    let (paper_tx, paper_rx) = mpsc::channel(4);
    let (_paper_ticker_tx, paper_ticker_rx) = broadcast::channel(4);
    let paper_state = AppState::new(paper_config.clone(), paper_db, paper_tx);
    *paper_state.status.write().await = BotStatus::Running;
    {
        let stale_ts = Utc::now() - chrono::Duration::seconds(30);
        let mut ticker = paper_state.ticker.write().await;
        ticker.last_price = dec!(100);
        ticker.mark_price = dec!(100);
        ticker.update_time = stale_ts;
        ticker.mark_update_time = stale_ts;
    }
    {
        let mut position = paper_state.position.write().await;
        position.symbol = "SOLUSDC".into();
        position.size = dec!(1.0);
        position.entry_price = dec!(95);
        position.leverage = 5;
    }
    let paper_client = Arc::new(BinanceFuturesClient::new(&paper_config.exchange));
    let mut paper_engine =
        GridTradingEngine::new(paper_state.clone(), paper_client, paper_rx, paper_ticker_rx);

    let (paper_reply_tx, paper_reply_rx) = tokio::sync::oneshot::channel();
    paper_engine
        .handle_control_action(BotControlAction::Reverse {
            expected_mode: TradingMode::Paper,
            expected_direction: GridDirection::Long,
            reply: paper_reply_tx,
        })
        .await;
    let paper_err = paper_reply_rx.await.unwrap().unwrap_err();
    assert!(paper_err.contains("当前行情已过期或缺失"), "{paper_err}");
    assert_eq!(paper_state.position.read().await.size, dec!(1.0));

    let mock = ReverseMockExchange {
        position_amt: Arc::new(Mutex::new(dec!(1.0))),
        entry_price: Arc::new(Mutex::new(dec!(80.0))),
        mark_price_override: Arc::new(Mutex::new(Some(Decimal::ZERO))),
        ticker_price_override: Arc::new(Mutex::new(Some(dec!(120)))),
        ..ReverseMockExchange::default()
    };
    let (url, server) = reverse_exchange_server(mock.clone()).await;

    let mut live_config = AppConfig::default();
    live_config.exchange.api_key = "live-key".into();
    live_config.exchange.api_secret = "live-secret".into();
    live_config.grid.direction = GridDirection::Long;
    live_config.grid.max_position_usdc = Some(dec!(100));

    let live_db = Arc::new(Database::open(":memory:").unwrap());
    live_db.save_config(&live_config).unwrap();
    let (live_tx, live_rx) = mpsc::channel(4);
    let (_live_ticker_tx, live_ticker_rx) = broadcast::channel(4);
    let live_state = AppState::new(live_config.clone(), live_db, live_tx);
    *live_state.status.write().await = BotStatus::Running;
    {
        let stale_ts = Utc::now() - chrono::Duration::seconds(30);
        let mut ticker = live_state.ticker.write().await;
        ticker.last_price = dec!(75);
        ticker.mark_price = dec!(75);
        ticker.update_time = stale_ts;
        ticker.mark_update_time = stale_ts;
    }

    let live_client = Arc::new(BinanceFuturesClient::with_base_url(
        &live_config.exchange,
        url,
    ));
    let mut live_engine =
        GridTradingEngine::new(live_state.clone(), live_client, live_rx, live_ticker_rx);

    let (live_reply_tx, live_reply_rx) = tokio::sync::oneshot::channel();
    live_engine
        .handle_control_action(BotControlAction::Reverse {
            expected_mode: TradingMode::Live,
            expected_direction: GridDirection::Long,
            reply: live_reply_tx,
        })
        .await;
    let live_err = live_reply_rx.await.unwrap().unwrap_err();
    assert!(
        mock.ticker_price_hits.load(Ordering::SeqCst) >= 1,
        "expected REST ticker fallback when mark and ticker prices are stale or zero"
    );
    assert!(live_err.contains("超过单交易对最大持仓限制"), "{live_err}");

    server.abort();
}

#[tokio::test]
async fn concurrent_pause_during_reverse_prevents_auto_resume() {
    let mock = ReverseMockExchange {
        position_amt: Arc::new(Mutex::new(dec!(1.0))),
        entry_price: Arc::new(Mutex::new(dec!(100.0))),
        ..ReverseMockExchange::default()
    };
    let (url, server) = reverse_exchange_server(mock.clone()).await;

    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    config.grid.direction = GridDirection::Long;
    config.grid.grid_interval = dec!(1);
    config.grid.order_amount_usdc = dec!(100);
    config.grid.buy_window = 2;
    config.grid.sell_window = 2;

    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&config).unwrap();
    db.save_bot_status(BotStatus::Running).unwrap();
    let (action_tx, action_rx) = mpsc::channel(4);
    let (_ticker_tx, ticker_rx) = broadcast::channel(4);
    let state = AppState::new(config.clone(), db.clone(), action_tx);
    *state.status.write().await = BotStatus::Running;
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(100);
        ticker.mark_price = dec!(100);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }

    *mock.pause_on_exchange_info.lock().unwrap() = Some(state.clone());

    let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Reverse {
            expected_mode: TradingMode::Live,
            expected_direction: GridDirection::Long,
            reply: reply_tx,
        })
        .await;
    let err = reply_rx.await.unwrap().unwrap_err();
    assert!(
        err.contains("反手仓位与方向已完成，但策略保持暂停"),
        "{err}"
    );
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    assert_eq!(
        state.config.read().await.grid.direction,
        GridDirection::Short
    );
    assert_eq!(state.position.read().await.size, dec!(-1.0));
    assert!(state.active_orders.read().await.is_empty());

    server.abort();
}
