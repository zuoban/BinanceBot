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
