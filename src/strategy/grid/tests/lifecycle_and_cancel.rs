use super::*;

#[tokio::test]
async fn invalid_symbol_removal_requires_empty_recovery_state() {
    let app = Router::new().fallback(|| async {
        (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"code": -1121, "msg": "Invalid symbol."})),
        )
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (config, db, order) = pending_pair_fixture();
    let mut engine = pair_recovery_engine(&config, db.clone(), url.clone()).await;
    assert!(engine.shutdown_inner(true).await.is_err());
    assert_eq!(
        db.load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .len(),
        1
    );
    db.clear_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
    db.save_managed_order(&order).unwrap();
    assert!(engine.shutdown_inner(true).await.is_err());
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
    db.delete_managed_order(&order.client_order_id).unwrap();
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(order.client_order_id.clone(), order.clone());
    assert!(engine.shutdown_inner(true).await.is_err());
    engine.state.active_orders.write().await.clear();
    db.save_remainder_plan(&crate::types::RemainderPlan {
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        sources: vec![order.clone()],
        target: order,
        phase: crate::types::RemainderPhase::Canceling,
    })
    .unwrap();
    assert!(engine.shutdown_inner(true).await.is_err());
    let empty_db = Arc::new(Database::open(":memory:").unwrap());
    let mut empty = pair_recovery_engine(&config, empty_db, url).await;
    assert!(empty.shutdown_inner(true).await.is_ok());
    server.abort();
}

#[tokio::test]
async fn invalid_symbol_removal_blocks_unreadable_recovery_state() {
    let app = Router::new().fallback(|| async {
        (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"code": -1121, "msg": "Invalid symbol."})),
        )
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = crate::test_support::TempTestDir::new("invalid-symbol-db");
    let path = dir.join("invalid-symbol.db");
    let db = Arc::new(Database::open(&path).unwrap());
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("DROP TABLE pair_intents").unwrap();
    drop(connection);
    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    assert!(engine.shutdown_inner(true).await.is_err());
    drop(engine);
    drop(db);
    server.abort();
}

#[tokio::test]
async fn removal_does_not_accept_other_exchange_errors() {
    let app = Router::new().fallback(|| async {
        (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"code": -1000, "msg": "Unknown error"})),
        )
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let mut engine = pair_recovery_engine(&config, db, url).await;
    assert!(engine.shutdown_inner(true).await.is_err());
    server.abort();
}

fn seed_paper_position(db: &Database) {
    let trade = TradeRecord {
        trade_id: "paper-switch-fill".into(),
        client_order_id: "paper-switch-order".into(),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Paper,
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        realized_pnl: dec!(0),
        commission: dec!(0),
        commission_asset: None,
        pnl_verified: true,
        is_maker: true,
        timestamp: Utc::now(),
        note: String::new(),
    };
    let position = PositionInfo {
        symbol: "SOLUSDC".into(),
        size: dec!(1),
        entry_price: dec!(100),
        ..Default::default()
    };
    let account = AccountInfo {
        asset: "USDC".into(),
        total_wallet_balance: dec!(900),
        available_balance: dec!(800),
        ..Default::default()
    };
    assert!(db
        .insert_trade_with_paper_recovery(
            &trade,
            &GridStats::default(),
            None,
            None,
            Some(&(position, account)),
        )
        .unwrap());
}

#[tokio::test]
async fn switching_away_and_back_restores_paper_position_and_balance() {
    let mut config = AppConfig::default();
    config.grid.grid_interval = dec!(1);
    let db = Arc::new(Database::open(":memory:").unwrap());
    seed_paper_position(&db);
    let fake = PairExchange::default();
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db, url).await;
    let mut live = config.clone();
    live.exchange.api_key = "live-key".into();
    live.exchange.api_secret = "live-secret".into();
    engine.apply_config(live).await.unwrap();
    assert_eq!(engine.state.position.read().await.size, dec!(0));
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(fake.orders.lock().unwrap().is_empty());
    {
        let mut ticker = engine.state.ticker.write().await;
        ticker.mark_price = dec!(100);
        ticker.mark_update_time = Utc::now();
    }
    let (reply, response) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Resume {
            expected_mode: TradingMode::Live,
            expected_epoch: engine.state.pause_epoch(),
            reply,
        })
        .await;
    response.await.unwrap().unwrap();
    let live_order_ids: Vec<_> = fake
        .orders
        .lock()
        .unwrap()
        .values()
        .filter(|order| order.status == "NEW")
        .map(|order| order.client_order_id.clone())
        .collect();
    assert!(live_order_ids.len() > 1);
    engine.apply_config(config).await.unwrap();
    {
        let orders = fake.orders.lock().unwrap();
        assert!(live_order_ids.iter().all(|id| orders
            .get(id)
            .is_some_and(|order| order.status == "CANCELED")));
        assert!(orders.values().all(|order| order.status != "NEW"));
    }
    assert_eq!(engine.state.position.read().await.size, dec!(1));
    let account = engine.state.account.read().await;
    assert_eq!(account.total_wallet_balance, dec!(900));
    assert_eq!(account.available_balance, dec!(800));
    server.abort();
}

#[tokio::test]
async fn switching_direction_rejects_nonempty_target_paper_position() {
    let mut config = AppConfig::default();
    config.exchange.api_key = "test-key".into();
    config.exchange.api_secret = "test-secret".into();
    let db = Arc::new(Database::open(":memory:").unwrap());
    seed_paper_position(&db);
    let (url, server) = pair_exchange_server(PairExchange::default()).await;
    let mut engine = pair_recovery_engine(&config, db, url).await;
    engine.state.position.write().await.size = dec!(0);
    let mut paper = config.clone();
    paper.exchange.api_key.clear();
    paper.exchange.api_secret.clear();
    paper.grid.direction = match config.grid.direction {
        GridDirection::Long => GridDirection::Short,
        GridDirection::Short => GridDirection::Long,
    };
    let error = engine.apply_config(paper).await.unwrap_err();
    assert!(error.to_string().contains("请先平掉目标账户的持仓"));
    let current = engine.state.config.read().await;
    assert!(!current.exchange.is_paper());
    assert_eq!(current.grid.direction, config.grid.direction);
    server.abort();
}

#[tokio::test]
async fn rebalance_rejects_mode_changed_after_enqueue_before_exchange_work() {
    let (mut engine, _db, fake, server) = dirty_live_switch_fixture().await;
    let (reply, result) = tokio::sync::oneshot::channel();
    engine
        .state
        .action_tx
        .send(BotControlAction::Rebalance {
            expected_mode: TradingMode::Paper,
            reply,
        })
        .await
        .unwrap();
    engine.state.config.write().await.exchange.api_key = "live-key".into();
    engine.state.config.write().await.exchange.api_secret = "live-secret".into();
    let action = engine.action_rx.recv().await.unwrap();
    engine.handle_control_action(action).await;
    assert!(result.await.unwrap().unwrap_err().contains("模式已改变"));
    assert_eq!(
        fake.orders
            .lock()
            .unwrap()
            .get("gb_b_dirty_target")
            .unwrap()
            .status,
        "NEW"
    );
    assert!(fake.posts.lock().unwrap().is_empty());
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    server.abort();
}

#[tokio::test]
async fn rebalance_rejects_pause_after_enqueue_without_placing_orders() {
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let fake = PairExchange::default();
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db, url).await;
    engine.state.resume_trading().await.unwrap();
    let (reply, result) = tokio::sync::oneshot::channel();
    engine
        .state
        .action_tx
        .send(BotControlAction::Rebalance {
            expected_mode: config.exchange.trading_mode(),
            reply,
        })
        .await
        .unwrap();
    engine.state.pause_trading().await.unwrap();
    let action = engine.action_rx.recv().await.unwrap();
    engine.handle_control_action(action).await;
    assert!(result.await.unwrap().is_err());
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(engine.state.active_orders.read().await.is_empty());
    assert!(fake.posts.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn rebalance_reports_cancellation_failure() {
    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let fake = PairExchange::default();
    fake.fail_open_orders.store(100, Ordering::SeqCst);
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db, url).await;
    engine.state.resume_trading().await.unwrap();
    let (reply, result) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Rebalance {
            expected_mode: config.exchange.trading_mode(),
            reply,
        })
        .await;
    assert!(result.await.unwrap().unwrap_err().contains("cancel"));
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(fake.posts.lock().unwrap().is_empty());
    server.abort();
}

async fn dirty_live_switch_fixture() -> (
    GridTradingEngine,
    Arc<Database>,
    PairExchange,
    tokio::task::JoinHandle<()>,
) {
    let mut paper = AppConfig::default();
    paper.grid.grid_interval = dec!(1);
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&paper).unwrap();
    db.arm_execution_guard("SOLUSDC", TradingMode::Paper)
        .unwrap();
    db.arm_execution_guard("SOLUSDC", TradingMode::Live)
        .unwrap();
    let fake = PairExchange::default();
    let order = BinanceOrderResponse {
        order_id: 777,
        client_order_id: "gb_b_dirty_target".into(),
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
    };
    fake.orders
        .lock()
        .unwrap()
        .insert(order.client_order_id.clone(), order);
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let engine = pair_recovery_engine(&paper, db.clone(), url).await;
    engine.state.resume_trading().await.unwrap();
    (engine, db, fake, server)
}

#[tokio::test]
async fn rebalance_reports_pause_during_cancellation() {
    let (mut engine, _db, fake, server) = dirty_live_switch_fixture().await;
    engine.state.config.write().await.exchange.api_key = "live-key".into();
    engine.state.config.write().await.exchange.api_secret = "live-secret".into();
    fake.block_cancel.store(1, Ordering::SeqCst);
    let state = engine.state.clone();
    let expected_mode = {
        let config = state.config.read().await;
        config.exchange.trading_mode()
    };
    let (reply, result) = tokio::sync::oneshot::channel();
    let rebuilding = tokio::spawn(async move {
        engine
            .handle_control_action(BotControlAction::Rebalance {
                expected_mode,
                reply,
            })
            .await;
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fake.cancel_started.notified(),
    )
    .await
    .unwrap();
    state.pause_trading().await.unwrap();
    fake.release_cancel.notify_one();
    rebuilding.await.unwrap();
    assert!(result.await.unwrap().is_err());
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert!(fake.posts.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn switching_to_dirty_live_cancels_recovered_orders_and_stays_paused() {
    let (mut engine, db, fake, server) = dirty_live_switch_fixture().await;
    let mut live = engine.state.config.read().await.clone();
    live.exchange.api_key = "live-key".into();
    live.exchange.api_secret = "live-secret".into();
    engine.apply_config(live).await.unwrap();

    assert!(!engine.state.config.read().await.exchange.is_paper());
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
        Some(BotStatus::Paused)
    );
    assert_eq!(
        fake.orders.lock().unwrap()["gb_b_dirty_target"].status,
        "CANCELED"
    );
    assert!(engine.state.active_orders.read().await.is_empty());
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn live_credential_switch_invalidates_old_balance_until_reconciled() {
    let mut config = AppConfig::default();
    config.exchange.api_key = "old-key".into();
    config.exchange.api_secret = "old-secret".into();
    config.grid.grid_interval = dec!(1);
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&config).unwrap();
    let fake = PairExchange::default();
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db, url).await;
    let state = engine.state.clone();
    let mut old_account = state.account.write().await;
    old_account.asset = "USDC".into();
    old_account.margin_balance = dec!(321);
    old_account.update_time = Utc::now();

    config.exchange.api_key = "new-key".into();
    config.exchange.api_secret = "new-secret".into();
    let switching = tokio::spawn(async move {
        let result = engine.apply_config(config).await;
        (engine, result)
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if state.config.read().await.exchange.api_key == "new-key" {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fake.fail_open_orders.store(100, Ordering::SeqCst);
    drop(old_account);

    let (mut engine, result) = tokio::time::timeout(std::time::Duration::from_secs(10), switching)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err());
    let snapshot = state.snapshot().await;
    assert_eq!(snapshot.account.margin_balance, Decimal::ZERO);
    assert_eq!(
        snapshot.account.update_time,
        chrono::DateTime::<Utc>::default()
    );

    fake.fail_open_orders.store(0, Ordering::SeqCst);
    assert!(engine.sync_account_and_position().await);
    let account = state.account.read().await;
    assert_eq!(account.margin_balance, dec!(10000));
    assert!(account.update_time > chrono::DateTime::<Utc>::default());
    server.abort();
}

#[tokio::test]
async fn dirty_live_cancellation_failure_keeps_pause_and_recovery_record() {
    let (mut engine, db, fake, server) = dirty_live_switch_fixture().await;
    fake.fail_cancel.store(100, Ordering::SeqCst);
    let mut live = engine.state.config.read().await.clone();
    live.exchange.api_key = "live-key".into();
    live.exchange.api_secret = "live-secret".into();
    assert!(engine.apply_config(live).await.is_err());

    assert!(!engine.state.config.read().await.exchange.is_paper());
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
        Some(BotStatus::Paused)
    );
    assert_eq!(
        fake.orders.lock().unwrap()["gb_b_dirty_target"].status,
        "NEW"
    );
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key("gb_b_dirty_target"));
    assert!(db
        .load_managed_orders("SOLUSDC")
        .unwrap()
        .iter()
        .any(|order| order.client_order_id == "gb_b_dirty_target"));
    server.abort();
}

#[tokio::test]
async fn concurrent_pause_during_dirty_live_cancellation_stays_paused() {
    let (mut engine, db, fake, server) = dirty_live_switch_fixture().await;
    fake.block_cancel.store(1, Ordering::SeqCst);
    let state = engine.state.clone();
    let mut live = state.config.read().await.clone();
    live.exchange.api_key = "live-key".into();
    live.exchange.api_secret = "live-secret".into();
    let switching = tokio::spawn(async move { engine.apply_config(live).await });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fake.cancel_started.notified(),
    )
    .await
    .unwrap();
    state.pause_trading().await.unwrap();
    fake.release_cancel.notify_one();
    switching.await.unwrap().unwrap();

    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
        Some(BotStatus::Paused)
    );
    assert_eq!(
        fake.orders.lock().unwrap()["gb_b_dirty_target"].status,
        "CANCELED"
    );
    server.abort();
}

#[tokio::test]
async fn cancellation_continues_when_managed_metadata_cannot_be_read() {
    let (config, _, mut order) = pending_pair_fixture();
    let dir = crate::test_support::TempTestDir::new("grid-cancel-fault");
    let path = dir.join("grid-cancel-fault.db");
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    order.client_order_id = "gb_b_partial_fault".into();
    order.order_id = Some(777);
    db.save_managed_order(&order).unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE managed_orders SET order_json = 'invalid' WHERE client_order_id = ?1",
        [&order.client_order_id],
    )
    .unwrap();
    let fake = PairExchange::default();
    let exchange_order = BinanceOrderResponse {
        order_id: 777,
        client_order_id: order.client_order_id.clone(),
        symbol: order.symbol.clone(),
        status: "PARTIALLY_FILLED".into(),
        price: order.price,
        avg_price: Some(order.price),
        orig_qty: order.quantity,
        executed_qty: dec!(0.5),
        side: order.side.as_str().into(),
        order_type: "LIMIT".into(),
        time_in_force: "GTX".into(),
        update_time: None,
    };
    fake.orders
        .lock()
        .unwrap()
        .insert(order.client_order_id.clone(), exchange_order.clone());
    let mut manual = exchange_order;
    manual.order_id = 778;
    manual.client_order_id = "manual_order".into();
    fake.orders
        .lock()
        .unwrap()
        .insert(manual.client_order_id.clone(), manual);
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(order.client_order_id.clone(), order.clone());

    assert!(!engine.cancel_all_orders().await);
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key(&order.client_order_id));
    assert_eq!(
        fake.orders.lock().unwrap()[&order.client_order_id].status,
        "CANCELED"
    );
    assert_eq!(
        fake.orders.lock().unwrap()["manual_order"].status,
        "PARTIALLY_FILLED"
    );
    let json: String = conn
        .query_row(
            "SELECT order_json FROM managed_orders WHERE client_order_id = ?1",
            [&order.client_order_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(json, "invalid");
    assert!(db
        .get_trade_by_client_id(&order.client_order_id, TradingMode::Live)
        .unwrap()
        .is_none());
    server.abort();
    drop(engine);
    drop(db);
    drop(conn);
}

#[tokio::test]
async fn cancellation_continues_when_exchange_listing_fails() {
    let (config, _, mut order) = pending_pair_fixture();
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    order.client_order_id = "gb_b_listing_fault".into();
    order.order_id = Some(779);
    db.save_managed_order(&order).unwrap();
    let fake = PairExchange::default();
    fake.fail_open_orders.store(100, Ordering::SeqCst);
    fake.orders.lock().unwrap().insert(
        order.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 779,
            client_order_id: order.client_order_id.clone(),
            symbol: order.symbol.clone(),
            status: "PARTIALLY_FILLED".into(),
            price: order.price,
            avg_price: Some(order.price),
            orig_qty: order.quantity,
            executed_qty: dec!(0.5),
            side: order.side.as_str().into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    assert!(!engine.cancel_all_orders().await);
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert_eq!(
        fake.orders.lock().unwrap()[&order.client_order_id].status,
        "CANCELED"
    );
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key(&order.client_order_id));
    assert!(db
        .load_managed_orders(&order.symbol)
        .unwrap()
        .iter()
        .any(|saved| saved.client_order_id == order.client_order_id));
    assert!(db
        .get_trade_by_client_id(&order.client_order_id, TradingMode::Live)
        .unwrap()
        .is_none());
    server.abort();
}

#[tokio::test]
async fn failed_initialization_restores_orders_before_initial_cancellation() {
    let (config, db, mut order) = pending_pair_fixture();
    order.client_order_id = "gb_b_startup_unknown".into();
    order.order_id = None;
    order.created_at = Utc::now();
    db.save_managed_order(&order).unwrap();
    let (url, server) = pair_exchange_server(PairExchange::default()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;

    assert!(engine.initialize().await.is_err());
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key(&order.client_order_id));
    assert!(!engine.cancel_all_orders().await);
    assert!(db
        .load_managed_orders("SOLUSDC")
        .unwrap()
        .iter()
        .any(|saved| saved.client_order_id == order.client_order_id));
    server.abort();
}

#[tokio::test]
async fn cancellation_restores_orders_even_if_initial_recovery_was_skipped() {
    let (config, db, mut order) = pending_pair_fixture();
    order.client_order_id = "gb_b_cancel_unknown".into();
    order.order_id = None;
    order.created_at = Utc::now();
    db.save_managed_order(&order).unwrap();
    let (url, server) = pair_exchange_server(PairExchange::default()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;

    assert!(!engine.cancel_all_orders().await);
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key(&order.client_order_id));
    assert!(db
        .load_managed_orders("SOLUSDC")
        .unwrap()
        .iter()
        .any(|saved| saved.client_order_id == order.client_order_id));
    server.abort();
}

#[tokio::test]
async fn unresolved_order_keeps_confirmed_fill_behind_reconciliation_barrier() {
    let (config, db, mut unresolved) = pending_pair_fixture();
    unresolved.client_order_id = "gb_b_unknown_frontier".into();
    unresolved.order_id = None;
    unresolved.created_at = Utc::now();
    let mut filled = unresolved.clone();
    filled.client_order_id = "gb_b_confirmed_frontier".into();
    db.save_managed_order(&unresolved).unwrap();
    db.save_managed_order(&filled).unwrap();
    let fake = PairExchange::default();
    fake.orders.lock().unwrap().insert(
        filled.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 777,
            client_order_id: filled.client_order_id.clone(),
            symbol: filled.symbol.clone(),
            status: "FILLED".into(),
            price: filled.price,
            avg_price: None,
            orig_qty: filled.quantity,
            executed_qty: filled.quantity,
            side: filled.side.as_str().into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: Some(Utc::now().timestamp_millis()),
        },
    );
    let (url, server) = pair_exchange_server(fake).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    {
        let mut active = engine.state.active_orders.write().await;
        active.insert(unresolved.client_order_id.clone(), unresolved);
        active.insert(filled.client_order_id.clone(), filled.clone());
    }
    assert!(!engine.sync_live_orders().await);
    assert!(engine.last_orders_sync.is_none());
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key(&filled.client_order_id));
    assert!(db
        .load_managed_orders("SOLUSDC")
        .unwrap()
        .iter()
        .any(|saved| saved.client_order_id == filled.client_order_id));
    server.abort();
}

#[test]
fn per_pair_engine_futures_remain_compact() {
    use std::mem::size_of_val;

    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::with_base_url(
        &config.exchange,
        "http://127.0.0.1:1".into(),
    ));
    let mut engine = GridTradingEngine::new(state, client, action_rx, ticker_rx);

    assert!(std::mem::size_of_val(&engine.run()) < 8_192);
    assert!(size_of_val(&engine.handle_ticker_update(TickerInfo::default())) < 8_192);
    assert!(size_of_val(&engine.initialize()) < 16_384);
    assert!(size_of_val(&engine.sync_cycle(false)) < 16_384);
    assert!(size_of_val(&engine.apply_config(config)) < 16_384);
    assert!(size_of_val(&engine.handle_control_action(BotControlAction::Pause)) < 16_384);
}

#[tokio::test]
async fn user_data_wakeup_triggers_immediate_sync_cycle_without_waiting_for_timer() {
    use std::time::Duration;
    let (mut config, db, order) = pending_pair_fixture();
    config.exchange.sync_interval_secs = 60;
    db.save_managed_order(&order).unwrap();
    let fake = PairExchange::default();
    fake.orders.lock().unwrap().insert(
        order.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 901,
            client_order_id: order.client_order_id.clone(),
            symbol: order.symbol.clone(),
            status: "CANCELED".into(),
            price: order.price,
            avg_price: None,
            orig_qty: order.quantity,
            executed_qty: Decimal::ZERO,
            side: order.side.as_str().into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: Some(Utc::now().timestamp_millis()),
        },
    );
    let (url, server) = pair_exchange_server(fake).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    *engine.state.status.write().await = BotStatus::Paused;
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(order.client_order_id.clone(), order.clone());
    let state = engine.state.clone();
    let (wakeup_tx, wakeup_rx) = tokio::sync::watch::channel(0u64);
    engine.set_user_data_rx(wakeup_rx);

    let run_task = tokio::spawn(async move {
        engine.run().await;
    });

    // Signal user-data wakeup; the canceled order should be reconciled and removed within < 1s
    // despite sync_interval_secs being 60s.
    wakeup_tx.send(1).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !state.active_orders.read().await.is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    state
        .action_tx
        .send(BotControlAction::Shutdown {
            removing: false,
            reply: ack_tx,
        })
        .await
        .unwrap();
    let _ = ack_rx.await;
    let _ = run_task.await;
    server.abort();
}

#[tokio::test]
async fn bulk_and_window_cancellation_batch_up_to_ten_orders_per_request() {
    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    config.grid.grid_interval = dec!(1);
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let fake = PairExchange::default();

    let make_order = |idx: usize, id: i64, price: Decimal| {
        let cid = format!("gb_b_batch_{idx:02}");
        let grid = GridOrder {
            client_order_id: cid.clone(),
            order_id: Some(id),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price,
            quantity: dec!(1),
            amount_usdc: price,
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: -(idx as i32) - 1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        let resp = BinanceOrderResponse {
            order_id: id,
            client_order_id: cid,
            symbol: "SOLUSDC".into(),
            status: "NEW".into(),
            price,
            avg_price: None,
            orig_qty: dec!(1),
            executed_qty: Decimal::ZERO,
            side: "BUY".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        };
        (grid, resp)
    };

    let mut seeded = Vec::new();
    for idx in 0..12 {
        let (grid, resp) = make_order(idx, 2000 + idx as i64, dec!(80) + Decimal::from(idx as u64));
        db.save_managed_order(&grid).unwrap();
        fake.orders
            .lock()
            .unwrap()
            .insert(grid.client_order_id.clone(), resp);
        seeded.push(grid);
    }

    // Order #03 already filled right before batch cancel runs; openOrders still includes it
    // only if we test window pruning, so test window pruning first with 12 orders where #03
    // filled on exchange before batch cancel arrives!
    {
        let mut orders = fake.orders.lock().unwrap();
        let filled = orders.get_mut("gb_b_batch_03").unwrap();
        filled.status = "FILLED".into();
        filled.executed_qty = dec!(1);
        filled.avg_price = Some(filled.price);
        filled.update_time = Some(Utc::now().timestamp_millis());
    }

    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    *engine.state.status.write().await = BotStatus::Running;
    for grid in &seeded {
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(grid.client_order_id.clone(), grid.clone());
    }

    assert!(engine.cancel_grid_orders(&seeded, true).await);
    // 12 orders = 1 batch of 10 + 1 batch of 2 -> 2 batch requests, 0 single cancel requests!
    assert_eq!(fake.batch_cancel_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fake.single_cancel_calls.load(Ordering::SeqCst), 0);
    assert!(engine.state.active_orders.read().await.is_empty());
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    // Order #03 (-2011 in batch response) was confirmed FILLED via lookup and recorded as a trade!
    assert!(db
        .get_trade_by_client_id("gb_b_batch_03", TradingMode::Live)
        .unwrap()
        .is_some());

    // Now seed 11 open orders (10 + 1): first 10 use batchOrders, lone 11th uses single cancel_order (2 HTTP calls total instead of 11).
    fake.batch_cancel_calls.store(0, Ordering::SeqCst);
    fake.single_cancel_calls.store(0, Ordering::SeqCst);
    for idx in 0..11 {
        let (grid, resp) = make_order(
            idx + 20,
            3000 + idx as i64,
            dec!(70) + Decimal::from(idx as u64),
        );
        db.save_managed_order(&grid).unwrap();
        fake.orders
            .lock()
            .unwrap()
            .insert(grid.client_order_id.clone(), resp);
    }
    assert!(engine.cancel_all_orders().await);
    assert_eq!(fake.batch_cancel_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fake.single_cancel_calls.load(Ordering::SeqCst), 1);
    assert!(engine.state.active_orders.read().await.is_empty());
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());

    server.abort();
}

#[tokio::test]
async fn live_window_replenishment_batches_up_to_five_orders_and_clears_post_only_rejections() {
    let (mut config, _, _) = pending_pair_fixture();
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    config.grid.buy_window = 6;
    config.grid.sell_window = 6;
    config.grid.order_amount_usdc = dec!(100);
    config.grid.grid_interval = dec!(1);
    config.grid.max_position_usdc = None;
    config.grid.min_liquidation_distance_pct = None;

    let fake = PairExchange::default();
    *fake.reject_post_only_prices.lock().unwrap() = vec![dec!(99)];
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    let info = engine
        .client
        .get_exchange_info(Some("SOLUSDC"))
        .await
        .unwrap();
    *engine.state.rules.write().await =
        crate::strategy::precision::SymbolRules::from_exchange_info(&info, "SOLUSDC").unwrap();
    *engine.state.status.write().await = BotStatus::Running;
    let now = Utc::now();
    *engine.state.ticker.write().await = TickerInfo {
        symbol: "SOLUSDC".into(),
        last_price: dec!(100),
        mark_price: dec!(100),
        update_time: now,
        mark_update_time: now,
        ..TickerInfo::default()
    };
    engine.state.position.write().await.size = dec!(10);
    engine.last_account_sync = Some(Instant::now());
    engine.last_orders_sync = Some(Instant::now());

    engine.maintain_grid_window().await;

    // 6 buys = batch(5) + single(1); 6 sells = batch(5) + single(1) -> 2 batch + 2 single calls!
    assert_eq!(fake.batch_place_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fake.single_place_calls.load(Ordering::SeqCst), 2);
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);

    // Price 99 was rejected with -5022 inside the buy batch, leaving 5 buys + 6 sells = 11 active & persisted orders.
    let active = engine.state.active_orders.read().await;
    assert_eq!(active.len(), 11);
    assert!(!active.values().any(|order| order.price == dec!(99)));
    drop(active);
    let persisted = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(persisted.len(), 11);
    assert!(!persisted.iter().any(|order| order.price == dec!(99)));

    server.abort();
}
