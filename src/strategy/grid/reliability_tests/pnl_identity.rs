use super::*;

#[derive(Clone)]
struct PnlExchange {
    response: Arc<Mutex<BinanceOrderResponse>>,
    pnl_calls: Arc<AtomicUsize>,
}

struct PnlFixture {
    engine: GridTradingEngine,
    exchange: PnlExchange,
    server: JoinHandle<()>,
}

impl Drop for PnlFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl PnlFixture {
    async fn new() -> Self {
        let (order, mut response) = managed_price_fixture();
        response.status = "FILLED".into();
        response.executed_qty = order.quantity;
        response.avg_price = Some(order.price);
        let exchange = PnlExchange {
            response: Arc::new(Mutex::new(response)),
            pnl_calls: Arc::new(AtomicUsize::new(0)),
        };
        let app = Router::new()
            .route(
                "/fapi/v1/order",
                get(|State(exchange): State<PnlExchange>| async move {
                    Json(exchange.response.lock().unwrap().clone())
                }),
            )
            .route(
                "/fapi/v1/userTrades",
                get(
                    move |State(exchange): State<PnlExchange>,
                          Query(query): Query<HashMap<String, String>>| async move {
                        exchange.pnl_calls.fetch_add(1, Ordering::SeqCst);
                        // A matching fill quantity alone cannot prove the lookup's identity.
                        Json(json!([{
                            "id": 1, "orderId": query["orderId"].parse::<i64>().unwrap(),
                            "qty": order.quantity, "realizedPnl": "1.25", "commission": "0.01",
                            "commissionAsset": "USDC", "maker": true
                        }]))
                    },
                ),
            )
            .with_state(exchange.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let engine = engine(database(), &url).await;
        let pending = TradeRecord {
            trade_id: format!("grid:{}", order.client_order_id),
            client_order_id: order.client_order_id,
            symbol: order.symbol,
            mode: TradingMode::Live,
            side: order.side,
            price: order.price,
            quantity: order.quantity,
            amount_usdc: order.amount_usdc,
            realized_pnl: Decimal::ZERO,
            commission: Decimal::ZERO,
            commission_asset: None,
            pnl_verified: false,
            is_maker: true,
            timestamp: Utc::now(),
            note: String::new(),
        };
        assert!(engine.state.record_trade(pending, None, None, None).await);
        assert!(engine.pnl_order_ids.is_empty());
        Self {
            engine,
            exchange,
            server,
        }
    }

    async fn verify(&mut self) -> Option<Instant> {
        self.engine.reconcile_unverified_trades().await;
        tokio::time::timeout(
            Duration::from_secs(5),
            self.engine.pnl_reconcile_task.take().unwrap(),
        )
        .await
        .unwrap()
        .unwrap()
    }
}

#[tokio::test]
async fn restored_pnl_verification_rejects_untrusted_order_lookup_and_recovers() {
    type Mutation = (&'static str, fn(&mut BinanceOrderResponse));
    let mutations: &[Mutation] = &[
        ("client ID", |order| {
            order.client_order_id = "gb_foreign".into()
        }),
        ("symbol", |order| order.symbol = "BNBUSDC".into()),
        ("side", |order| order.side = "SELL".into()),
        ("unknown side", |order| order.side = "INVALID".into()),
        ("zero order ID", |order| order.order_id = 0),
        ("negative order ID", |order| order.order_id = -1),
        ("active status", |order| {
            order.status = "PARTIALLY_FILLED".into()
        }),
        ("unknown status", |order| order.status = "UNKNOWN".into()),
        ("zero original quantity", |order| {
            order.orig_qty = Decimal::ZERO
        }),
        ("overfilled quantity", |order| order.orig_qty = dec!(1)),
        ("different executed quantity", |order| {
            order.executed_qty = dec!(1)
        }),
        ("incomplete filled order", |order| order.orig_qty = dec!(2)),
    ];
    for (label, mutate) in mutations {
        let mut fixture = PnlFixture::new().await;
        let valid = fixture.exchange.response.lock().unwrap().clone();
        mutate(&mut fixture.exchange.response.lock().unwrap());
        let retry = fixture.verify().await;
        let pending = fixture.engine.state.db.get_recent_trades(10).unwrap();
        assert!(!pending[0].pnl_verified, "{label}");
        assert_eq!(pending[0].realized_pnl, Decimal::ZERO, "{label}");
        assert_eq!(pending[0].commission, Decimal::ZERO, "{label}");
        assert_eq!(
            fixture.exchange.pnl_calls.load(Ordering::SeqCst),
            0,
            "{label}"
        );
        assert!(retry.is_some(), "{label}");
        assert_eq!(
            fixture.engine.state.stats.read().await.pending_pnl_trades,
            1
        );

        *fixture.exchange.response.lock().unwrap() = valid;
        assert!(fixture.verify().await.is_none(), "{label}");
        let verified = fixture.engine.state.db.get_recent_trades(10).unwrap();
        assert!(verified[0].pnl_verified, "{label}");
        assert_eq!(verified[0].realized_pnl, dec!(1.25), "{label}");
        assert_eq!(verified[0].commission, dec!(0.01), "{label}");
        assert_eq!(
            fixture.exchange.pnl_calls.load(Ordering::SeqCst),
            1,
            "{label}"
        );
        let stats = fixture.engine.state.stats.read().await;
        assert_eq!(stats.pending_pnl_trades, 0);
        assert_eq!(stats.total_trades, 1);
        assert_eq!(stats.total_realized_pnl, dec!(1.25));
        assert_eq!(stats.total_commission, dec!(0.01));
    }
}

#[tokio::test]
async fn restored_pnl_verification_accepts_partial_terminal_fills() {
    for status in ["CANCELED", "EXPIRED", "EXPIRED_IN_MATCH"] {
        let mut fixture = PnlFixture::new().await;
        {
            let mut response = fixture.exchange.response.lock().unwrap();
            response.status = status.into();
            response.orig_qty = dec!(2);
        }
        assert!(fixture.verify().await.is_none(), "{status}");
        let verified = fixture.engine.state.db.get_recent_trades(10).unwrap();
        assert!(verified[0].pnl_verified, "{status}");
        assert_eq!(verified[0].quantity, dec!(1.25));
        assert_eq!(verified[0].realized_pnl, dec!(1.25));
    }
}

#[tokio::test]
async fn pnl_verification_records_bnb_and_mixed_fees_without_polluting_quote_commission() {
    let (bnb_order, mut bnb_resp) = managed_price_fixture();
    bnb_resp.order_id = 1001;
    bnb_resp.status = "FILLED".into();
    bnb_resp.executed_qty = bnb_order.quantity;
    bnb_resp.avg_price = Some(bnb_order.price);

    let mut mixed_order = bnb_order.clone();
    mixed_order.client_order_id = "gb_b_mixed_fee".into();
    let mut mixed_resp = bnb_resp.clone();
    mixed_resp.order_id = 1002;
    mixed_resp.client_order_id = mixed_order.client_order_id.clone();

    let bnb_qty = bnb_order.quantity;
    let app = Router::new()
        .route(
            "/fapi/v1/order",
            get(move |Query(query): Query<HashMap<String, String>>| {
                let bnb = bnb_resp.clone();
                let mixed = mixed_resp.clone();
                async move {
                    match query.get("origClientOrderId").map(String::as_str) {
                        Some(id) if id == mixed.client_order_id => Json(mixed),
                        _ => Json(bnb),
                    }
                }
            }),
        )
        .route(
            "/fapi/v1/userTrades",
            get(
                move |Query(query): Query<HashMap<String, String>>| async move {
                    let oid: i64 = query["orderId"].parse().unwrap();
                    if oid == 1001 {
                        Json(json!([{
                            "id": 11, "orderId": 1001, "qty": bnb_qty,
                            "realizedPnl": "2.50", "commission": "0.00025",
                            "commissionAsset": "BNB", "maker": true
                        }]))
                    } else {
                        Json(json!([
                            {
                                "id": 21, "orderId": 1002, "qty": "0.25",
                                "realizedPnl": "0.50", "commission": "0.00010",
                                "commissionAsset": "BNB", "maker": true
                            },
                            {
                                "id": 22, "orderId": 1002, "qty": "1.00",
                                "realizedPnl": "1.00", "commission": "0.04",
                                "commissionAsset": "USDC", "maker": false
                            }
                        ]))
                    }
                },
            ),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let db = database();
    let mut eng = engine(db.clone(), &url).await;
    for order in [&bnb_order, &mixed_order] {
        let pending = TradeRecord {
            trade_id: format!("grid:{}", order.client_order_id),
            client_order_id: order.client_order_id.clone(),
            symbol: order.symbol.clone(),
            mode: TradingMode::Live,
            side: order.side,
            price: order.price,
            quantity: order.quantity,
            amount_usdc: order.amount_usdc,
            realized_pnl: Decimal::ZERO,
            commission: Decimal::ZERO,
            commission_asset: None,
            pnl_verified: false,
            is_maker: true,
            timestamp: Utc::now(),
            note: String::new(),
        };
        assert!(eng.state.record_trade(pending, None, None, None).await);
    }

    eng.reconcile_unverified_trades().await;
    let retry = tokio::time::timeout(
        Duration::from_secs(5),
        eng.pnl_reconcile_task.take().unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(retry.is_none());

    {
        let stats = eng.state.stats.read().await;
        assert_eq!(stats.pending_pnl_trades, 0);
        assert_eq!(stats.total_realized_pnl, dec!(4.00));
        assert_eq!(stats.total_commission, dec!(0.04));
        assert_eq!(stats.non_quote_commission_trades, 2);
    }
    let saved = db.get_recent_trades(10).unwrap();
    let saved_bnb = saved
        .iter()
        .find(|t| t.client_order_id == bnb_order.client_order_id)
        .unwrap();
    assert_eq!(saved_bnb.commission, dec!(0.00025));
    assert_eq!(saved_bnb.commission_asset.as_deref(), Some("BNB"));
    let saved_mixed = saved
        .iter()
        .find(|t| t.client_order_id == mixed_order.client_order_id)
        .unwrap();
    assert_eq!(saved_mixed.commission, dec!(0.04));
    assert_eq!(saved_mixed.commission_asset.as_deref(), Some("MIXED"));
    assert!(!saved_mixed.is_maker);

    let cached = eng.state.recent_trades.read().await;
    assert_eq!(
        cached
            .iter()
            .find(|t| t.client_order_id == bnb_order.client_order_id)
            .unwrap()
            .commission_asset
            .as_deref(),
        Some("BNB")
    );
    drop(cached);

    let reopened = engine(db, &url).await;
    let restored = reopened.state.stats.read().await;
    assert_eq!(restored.total_realized_pnl, dec!(4.00));
    assert_eq!(restored.total_commission, dec!(0.04));
    assert_eq!(restored.non_quote_commission_trades, 2);
    assert_eq!(restored.pending_pnl_trades, 0);
    server.abort();
}
