use super::*;

#[tokio::test]
async fn pair_order_forward_fill_transition_settles_but_regressions_remain_blocked() {
    for case in [
        "forward",
        "time_regression",
        "quantity_regression",
        "terminal_reversal",
        "avg_price_change",
        "partial_to_new",
        "partial_time_back",
    ] {
        let db = database();
        let client_queries = Arc::new(AtomicU8::new(0));
        let app = Router::new()
            .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
            .route("/fapi/v1/order", get({
                let client_queries = client_queries.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    let client_queries = client_queries.clone();
                    async move {
                        let by_client = query.contains_key("origClientOrderId");
                        let first = by_client && client_queries.fetch_add(1, Ordering::SeqCst) == 0;
                        let old_identity = query.get("orderId").map(String::as_str) == Some("123");
                        let initial_terminal = first && case == "terminal_reversal";
                        let status = if old_identity {"CANCELED"} else if initial_terminal {"FILLED"}
                            else if first || case == "partial_time_back" {"PARTIALLY_FILLED"}
                            else if case == "partial_to_new" {"NEW"}
                            else if case == "terminal_reversal" || case == "avg_price_change" {"CANCELED"} else {"FILLED"};
                        let executed = if old_identity {"0"} else if initial_terminal {"2"}
                            else if case == "partial_to_new" {if first {"0.5"} else {"0"}}
                            else if case == "partial_time_back" {"0.5"}
                            else if first {if case == "quantity_regression" {"1.5"} else {"1"}}
                            else if case == "terminal_reversal" {"0"} else if case == "quantity_regression" || case == "avg_price_change" {"1"} else {"2"};
                        let time = if old_identity {500} else if first {if case == "time_regression" || case == "partial_time_back" {2000} else {1000}}
                            else if case == "time_regression" || case == "partial_time_back" {1000} else {3000};
                        Json(json!({"orderId":if old_identity {123} else {456},"clientOrderId":"gb_pair_forward_fill",
                            "symbol":"SOLUSDC","status":status,"price":"101",
                            "avgPrice":if case == "avg_price_change" {if first {"100"} else {"200"}} else {"101"},
                            "origQty":"2","executedQty":executed,"updateTime":time,
                            "side":"SELL","type":"LIMIT","timeInForce":"GTX"}))
                    }
                }
            }))
            .route("/fapi/v2/positionRisk", get(|| async {
                Json(json!([{"symbol":"SOLUSDC","positionAmt":"0","entryPrice":"0","markPrice":"100",
                    "unRealizedProfit":"0","liquidationPrice":"0","leverage":"1"}]))
            }))
            .route("/fapi/v2/account", get(|| async {
                Json(json!({"totalWalletBalance":"1000","totalMarginBalance":"1000","totalUnrealizedProfit":"0",
                    "availableBalance":"1000","assets":[{"asset":"USDC","walletBalance":"1000","marginBalance":"1000",
                    "unrealizedProfit":"0","availableBalance":"1000"}]}))
            }))
            .route("/fapi/v1/userTrades", get(|| async {
                Json(json!([{"id":1,"orderId":456,"qty":"2","realizedPnl":"0","commission":"0.01",
                    "commissionAsset":"USDC","maker":true}]))
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let intent = GridOrder {
            client_order_id: "gb_pair_forward_fill".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(2),
            amount_usdc: dec!(202),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: Some("gb_pair_forward_parent".into()),
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        let trade = TradeRecord {
            trade_id: "grid:gb_pair_forward_parent".into(),
            client_order_id: "gb_pair_forward_parent".into(),
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Live,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(2),
            amount_usdc: dec!(200),
            realized_pnl: dec!(0),
            commission: dec!(0),
            commission_asset: None,
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: "fill".into(),
        };
        db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&intent), None)
            .unwrap();
        let mut engine = engine(db.clone(), &url).await;
        let account = engine.client.account_identity_key();
        db.save_identity_observation(
            account,
            &intent,
            &crate::types::ObservedOrderIdentity {
                client_order_id: intent.client_order_id.clone(),
                symbol: intent.symbol.clone(),
                side: "SELL".into(),
                order_id: 123,
            },
        )
        .unwrap();
        let mut settled = false;
        for _ in 0..3 {
            let ready = engine.sync_live_orders().await;
            if case == "forward" && ready {
                settled = true;
                break;
            }
            assert!(!ready, "case={case}");
            assert!(engine.reconciliation_blocked);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        }
        if case == "forward" {
            assert!(settled);
            assert!(!engine.reconciliation_blocked);
            assert!(!engine
                .state
                .active_orders
                .read()
                .await
                .contains_key(&intent.client_order_id));
            assert!(db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .is_empty());
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 2);
        } else {
            assert!(!settled);
            assert!(!db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .is_empty());
            if matches!(
                case,
                "avg_price_change" | "partial_to_new" | "partial_time_back"
            ) {
                let disagreements = db
                    .load_terminal_order_disagreements(account, "SOLUSDC")
                    .unwrap();
                assert!(disagreements.iter().any(|(client, first, conflicting)| {
                    if client != &intent.client_order_id {
                        return false;
                    }
                    match case {
                        "avg_price_change" => {
                            first.executed_qty == dec!(1)
                                && conflicting.executed_qty == dec!(1)
                                && first.avg_price == Some(dec!(100))
                                && conflicting.avg_price == Some(dec!(200))
                        }
                        "partial_to_new" => {
                            first.status == "PARTIALLY_FILLED"
                                && first.executed_qty == dec!(0.5)
                                && conflicting.status == "NEW"
                                && conflicting.executed_qty == dec!(0)
                                && first.update_time == Some(1000)
                                && conflicting.update_time == Some(3000)
                        }
                        "partial_time_back" => {
                            first.status == "PARTIALLY_FILLED"
                                && first.executed_qty == dec!(0.5)
                                && conflicting.status == "PARTIALLY_FILLED"
                                && conflicting.executed_qty == dec!(0.5)
                                && first.update_time == Some(2000)
                                && conflicting.update_time == Some(1000)
                        }
                        _ => false,
                    }
                }));
                drop(engine);
                let mut restarted = self::engine(db.clone(), &url).await;
                assert!(!restarted.sync_live_orders().await);
                assert!(restarted.reconciliation_blocked);
                assert_eq!(
                    db.load_pair_intents("SOLUSDC", TradingMode::Live)
                        .unwrap()
                        .len(),
                    1
                );
                assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
                assert!(!db
                    .load_terminal_order_disagreements(account, "SOLUSDC")
                    .unwrap()
                    .is_empty());
            }
        }
        server.abort();
    }
}

#[tokio::test]
async fn pair_terminal_verification_remains_blocked_on_conflicting_results_or_later_id() {
    for case in [
        "status",
        "executed_qty",
        "price",
        "update_time",
        "first_status",
        "first_qty",
        "first_price",
        "first_update_time",
        "first_filled",
        "later_id",
    ] {
        let db = database();
        let client_queries = Arc::new(AtomicU8::new(0));
        let app = Router::new()
            .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
            .route("/fapi/v1/order", get({
                let client_queries = client_queries.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    let client_queries = client_queries.clone();
                    async move {
                        let by_client = query.contains_key("origClientOrderId");
                        let count = if by_client { client_queries.fetch_add(1, Ordering::SeqCst) } else {0};
                        let id = if by_client {
                            if case == "later_id" && count >= 2 {789} else {456}
                        } else { query["orderId"].parse::<i64>().unwrap() };
                        let inconsistent_order = !by_client && id == 456;
                        let initial_client = by_client && count == 0;
                        Json(json!({"orderId":id,"clientOrderId":"gb_pair_terminal_disagreement","symbol":"SOLUSDC",
                            "status":if initial_client && case == "first_filled" {"FILLED"}
                                else if (inconsistent_order && case == "status") || (initial_client && case == "first_status") {"EXPIRED"}
                                else {"CANCELED"},
                            "price":if (inconsistent_order && case == "price") || (initial_client && case == "first_price") {"102"} else {"101"},
                            "origQty":"2","executedQty":if initial_client && case == "first_filled" {"2"}
                                else if (inconsistent_order && case == "executed_qty") || (initial_client && case == "first_qty") {"1"} else {"0"},
                            "updateTime":if (inconsistent_order && case == "update_time") || (initial_client && case == "first_update_time") {1000} else {2000},
                            "side":"SELL","type":"LIMIT","timeInForce":"GTX"}))
                    }
                }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let intent = GridOrder {
            client_order_id: "gb_pair_terminal_disagreement".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(2),
            amount_usdc: dec!(202),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: Some("gb_pair_disagreement_parent".into()),
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        let trade = TradeRecord {
            trade_id: "grid:gb_pair_disagreement_parent".into(),
            client_order_id: "gb_pair_disagreement_parent".into(),
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Live,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(2),
            amount_usdc: dec!(200),
            realized_pnl: dec!(0),
            commission: dec!(0),
            commission_asset: None,
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: "fill".into(),
        };
        db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&intent), None)
            .unwrap();
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        db.save_identity_observation(
            account,
            &intent,
            &crate::types::ObservedOrderIdentity {
                client_order_id: intent.client_order_id.clone(),
                symbol: intent.symbol.clone(),
                side: "SELL".into(),
                order_id: 123,
            },
        )
        .unwrap();
        for round in 0..3 {
            assert!(
                !first.sync_live_orders().await,
                "case={case}, round={round}, client_queries={}",
                client_queries.load(Ordering::SeqCst)
            );
            assert!(first.reconciliation_blocked);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
            assert!(db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .iter()
                .any(
                    |(expected, found)| expected.client_order_id == intent.client_order_id
                        && (found.order_id == 456
                            || (case == "later_id" && expected.order_id == Some(456)))
                ));
        }
        if case == "later_id" {
            assert!(client_queries.load(Ordering::SeqCst) >= 3);
            assert!(db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .iter()
                .any(
                    |(expected, found)| expected.client_order_id == intent.client_order_id
                        && found.order_id == 789
                ));
        } else {
            assert!(first
                .state
                .active_orders
                .read()
                .await
                .contains_key(&intent.client_order_id));
        }
        drop(first);
        let mut restarted = engine(db.clone(), &url).await;
        assert!(!restarted.sync_live_orders().await);
        assert!(restarted.reconciliation_blocked);
        assert_eq!(
            db.load_pair_intents("SOLUSDC", TradingMode::Live)
                .unwrap()
                .len(),
            1
        );
        server.abort();
    }
}

#[tokio::test]
async fn pair_terminal_fallback_persists_new_identity_before_unique_verification() {
    for case in [
        "valid",
        "wrong_order_id",
        "wrong_client_side",
        "write_failure",
    ] {
        let dir = crate::test_support::TempTestDir::new("pair-terminal-fallback");
        let path = dir.join("pair-terminal-fallback.db");
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        let account_slot = Arc::new(Mutex::new(None::<[u8; 32]>));
        let client_queries = Arc::new(AtomicU8::new(0));
        let order_queries = Arc::new(AtomicU8::new(0));
        let durable_before_query = Arc::new(AtomicBool::new(false));
        let app = Router::new()
            .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
            .route("/fapi/v1/order", get({
                let db = db.clone();
                let account_slot = account_slot.clone();
                let client_queries = client_queries.clone();
                let order_queries = order_queries.clone();
                let durable_before_query = durable_before_query.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    let db = db.clone();
                    let account_slot = account_slot.clone();
                    let client_queries = client_queries.clone();
                    let order_queries = order_queries.clone();
                    let durable_before_query = durable_before_query.clone();
                    async move {
                        let client_query = query.contains_key("origClientOrderId");
                        let client_count = if client_query { client_queries.fetch_add(1, Ordering::SeqCst) } else {0};
                        let id = if client_query {456} else {query["orderId"].parse::<i64>().unwrap()};
                        if !client_query && id == 456 {
                            order_queries.fetch_add(1, Ordering::SeqCst);
                            let account = account_slot.lock().unwrap().unwrap();
                            let recorded = db.load_identity_observations(account, "SOLUSDC").unwrap().iter()
                                .any(|(expected, found)| expected.client_order_id == "gb_pair_fallback" && found.order_id == 456);
                            durable_before_query.store(recorded, Ordering::SeqCst);
                        }
                        let wrong_id = case == "wrong_order_id" && !client_query && id == 456;
                        let wrong_side = case == "wrong_client_side" && client_query && client_count > 0;
                        Json(json!({"orderId":if wrong_id {789} else {id},"clientOrderId":"gb_pair_fallback",
                            "symbol":"SOLUSDC","status":"CANCELED","price":"101","origQty":"2","executedQty":"0",
                            "side":if wrong_side {"BUY"} else {"SELL"},"type":"LIMIT","timeInForce":"GTX"}))
                    }
                }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let intent = GridOrder {
            client_order_id: "gb_pair_fallback".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(2),
            amount_usdc: dec!(202),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: Some("gb_pair_fallback_parent".into()),
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        let trade = TradeRecord {
            trade_id: "grid:gb_pair_fallback_parent".into(),
            client_order_id: "gb_pair_fallback_parent".into(),
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Live,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(2),
            amount_usdc: dec!(200),
            realized_pnl: dec!(0),
            commission: dec!(0),
            commission_asset: None,
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: "fill".into(),
        };
        db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&intent), None)
            .unwrap();
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        *account_slot.lock().unwrap() = Some(account);
        db.save_identity_observation(
            account,
            &intent,
            &crate::types::ObservedOrderIdentity {
                client_order_id: intent.client_order_id.clone(),
                symbol: intent.symbol.clone(),
                side: "SELL".into(),
                order_id: 123,
            },
        )
        .unwrap();
        if case == "write_failure" {
            connection.execute_batch("CREATE TRIGGER reject_fallback_identity BEFORE INSERT ON order_identity_observations WHEN NEW.observed_order_id = 456 BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
        }
        let completed = first.sync_live_orders().await;
        assert!(client_queries.load(Ordering::SeqCst) > 0);
        if case == "valid" {
            if !completed {
                drop(first);
                let mut restarted = engine(db.clone(), &url).await;
                assert!(restarted.sync_live_orders().await);
            }
            assert!(order_queries.load(Ordering::SeqCst) > 0);
            assert!(client_queries.load(Ordering::SeqCst) >= 2);
            assert!(durable_before_query.load(Ordering::SeqCst));
            assert!(db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .is_empty());
        } else {
            assert!(!completed, "case={case}");
            assert!(first.reconciliation_blocked);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
            assert!(observations.iter().any(|(_, found)| found.order_id == 123));
            if case == "write_failure" {
                assert!(!observations.iter().any(|(_, found)| found.order_id == 456));
                assert_eq!(order_queries.load(Ordering::SeqCst), 0);
            } else {
                assert!(observations.iter().any(|(_, found)| found.order_id == 456));
                assert!(durable_before_query.load(Ordering::SeqCst));
            }
            drop(first);
            let mut restarted = engine(db.clone(), &url).await;
            assert!(!restarted.sync_live_orders().await);
            assert!(restarted.reconciliation_blocked);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            assert!(restarted
                .state
                .active_orders
                .read()
                .await
                .contains_key("gb_pair_fallback"));
            drop(restarted);
        }
        server.abort();
        drop(connection);
        drop(db);
    }
}

#[tokio::test]
async fn pair_intent_rejects_different_id_on_second_lookup() {
    let db = database();
    let stage = Arc::new(AtomicU8::new(0));
    let app = Router::new().route("/fapi/v1/order", get({
        let stage = stage.clone();
        move || { let stage = stage.clone(); async move {
            let first = stage.load(Ordering::SeqCst) == 0;
            Json(json!({"orderId":if first {123} else {456},"clientOrderId":"gb_pair_twice","symbol":"SOLUSDC",
                "status":if first {"FILLED"} else {"CANCELED"},"price":"101","origQty":"2","executedQty":"0",
                "side":"SELL","type":"LIMIT","timeInForce":"GTX"}))
        }}
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let intent = GridOrder {
        client_order_id: "gb_pair_twice".into(),
        order_id: None,
        symbol: "SOLUSDC".into(),
        side: OrderSide::Sell,
        price: dec!(101),
        quantity: dec!(2),
        amount_usdc: dec!(202),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 1,
        paired_client_order_id: Some("gb_pair_twice_parent".into()),
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    let trade = TradeRecord {
        trade_id: "grid:gb_pair_twice_parent".into(),
        client_order_id: "gb_pair_twice_parent".into(),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(2),
        amount_usdc: dec!(200),
        realized_pnl: dec!(0),
        commission: dec!(0),
        commission_asset: None,
        pnl_verified: true,
        is_maker: true,
        timestamp: Utc::now(),
        note: "fill".into(),
    };
    db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&intent), None)
        .unwrap();
    let mut engine = engine(db.clone(), &url).await;
    let account = engine.client.account_identity_key();
    assert!(
        !engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert_eq!(
        engine.state.active_orders.read().await["gb_pair_twice"].order_id,
        Some(123)
    );
    stage.store(1, Ordering::SeqCst);
    assert!(
        !engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert!(engine.reconciliation_blocked);
    assert_eq!(
        engine.state.active_orders.read().await["gb_pair_twice"].order_id,
        Some(123)
    );
    assert_eq!(
        db.load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
        Some(123)
    );
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .iter()
        .any(
            |(expected, observed)| expected.client_order_id == "gb_pair_twice"
                && expected.order_id == Some(123)
                && observed.order_id == 456
        ));
    server.abort();
}
