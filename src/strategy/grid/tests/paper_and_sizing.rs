use super::*;

#[tokio::test]
async fn buy_window_respects_position_and_open_buy_exposure() {
    let mut config = AppConfig::default();
    config.grid.grid_interval = dec!(1);
    config.grid.order_amount_usdc = dec!(100);
    config.grid.buy_window = 3;
    config.grid.sell_window = 1;
    config.grid.max_position_usdc = Some(dec!(150));
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(100);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }
    state.position.write().await.size = dec!(0.5);

    engine.maintain_grid_window().await;
    let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
    assert_eq!(
        orders
            .iter()
            .filter(|order| order.side == OrderSide::Buy)
            .count(),
        1
    );
    assert!(buy_exposure_usdc(dec!(0.5), dec!(100), &orders) <= dec!(150));

    engine.handle_control_action(BotControlAction::Pause).await;
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert!(state.active_orders.read().await.is_empty());
}

#[test]
fn exchange_fills_include_losses_and_fees() {
    let base_fills = || {
        vec![
            BinanceUserTrade {
                realized_pnl: dec!(1.20),
                commission: dec!(0.03),
                commission_asset: "USDC".into(),
                maker: true,
            },
            BinanceUserTrade {
                realized_pnl: dec!(-2.00),
                commission: dec!(0.04),
                commission_asset: "USDC".into(),
                maker: false,
            },
        ]
    };
    assert_eq!(
        aggregate_execution_pnl("SOLUSDC", &base_fills()).unwrap(),
        (dec!(-0.80), dec!(0.07), None, false)
    );

    let mut bnb_only = base_fills();
    bnb_only[0].commission = dec!(0.00012);
    bnb_only[0].commission_asset = "bnb".into();
    bnb_only[1].commission = dec!(0.00018);
    bnb_only[1].commission_asset = "BNB".into();
    assert_eq!(
        aggregate_execution_pnl("SOLUSDC", &bnb_only).unwrap(),
        (dec!(-0.80), dec!(0.00030), Some("BNB".into()), false)
    );

    let mut zero_fee_non_quote = base_fills();
    zero_fee_non_quote[0].commission = Decimal::ZERO;
    zero_fee_non_quote[0].commission_asset = "BNB".into();
    assert_eq!(
        aggregate_execution_pnl("SOLUSDC", &zero_fee_non_quote).unwrap(),
        (dec!(-0.80), dec!(0.04), None, false)
    );

    let mut mixed_quote_and_bnb = base_fills();
    mixed_quote_and_bnb[0].commission = dec!(0.00012);
    mixed_quote_and_bnb[0].commission_asset = "BNB".into();
    assert_eq!(
        aggregate_execution_pnl("SOLUSDC", &mixed_quote_and_bnb).unwrap(),
        (dec!(-0.80), dec!(0.04), Some("MIXED".into()), false)
    );

    let mut mixed_two_non_quote = base_fills();
    mixed_two_non_quote[0].commission = dec!(0.00012);
    mixed_two_non_quote[0].commission_asset = "BNB".into();
    mixed_two_non_quote[1].commission = dec!(0.02);
    mixed_two_non_quote[1].commission_asset = "BNFCR".into();
    assert_eq!(
        aggregate_execution_pnl("SOLUSDC", &mixed_two_non_quote).unwrap(),
        (dec!(-0.80), Decimal::ZERO, Some("MIXED".into()), false)
    );

    for invalid_asset in ["", "BNB FEE", "MIXED", "TOO_LONG_ASSET_NAME_123"] {
        let mut invalid = base_fills();
        invalid[0].commission_asset = invalid_asset.into();
        assert!(
            aggregate_execution_pnl("SOLUSDC", &invalid).is_err(),
            "{invalid_asset}"
        );
    }
    assert!(aggregate_execution_pnl("SOLUSDC", &[]).is_err());
}

#[tokio::test]
async fn paper_fill_persistence_failure_preserves_account_and_retry_applies_once() {
    for (direction, side, initial_size, price, expected_size, expected_pnl) in [
        (
            GridDirection::Long,
            OrderSide::Buy,
            dec!(0),
            dec!(100),
            dec!(1),
            dec!(0),
        ),
        (
            GridDirection::Long,
            OrderSide::Sell,
            dec!(2),
            dec!(110),
            dec!(1),
            dec!(10),
        ),
        (
            GridDirection::Short,
            OrderSide::Sell,
            dec!(0),
            dec!(100),
            dec!(-1),
            dec!(0),
        ),
        (
            GridDirection::Short,
            OrderSide::Buy,
            dec!(-2),
            dec!(90),
            dec!(-1),
            dec!(10),
        ),
    ] {
        let dir = crate::test_support::TempTestDir::new("paper-fill");
        let path = dir.join("paper-fill.db");
        let db = Arc::new(Database::open(&path).unwrap());
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER reject_paper_stats BEFORE INSERT ON scoped_grid_stats \
                 BEGIN SELECT RAISE(ABORT, 'reject stats'); END;",
            )
            .unwrap();
        let mut config = AppConfig::default();
        config.grid.direction = direction;
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_ticker_tx, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db.clone(), action_tx);
        let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
        {
            let mut position = state.position.write().await;
            position.size = initial_size;
            position.entry_price = if initial_size.is_zero() {
                dec!(0)
            } else {
                dec!(100)
            };
            position.mark_price = dec!(100);
            position.leverage = 10;
        }
        state.ticker.write().await.last_price = dec!(100);
        let position_before = serde_json::to_value(&*state.position.read().await).unwrap();
        let account_before = serde_json::to_value(&*state.account.read().await).unwrap();
        let stats_before = serde_json::to_value(&*state.stats.read().await).unwrap();
        let wallet_before = state.account.read().await.total_wallet_balance;
        let mut order = GridOrder {
            client_order_id: "paper-retry".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side,
            price,
            quantity: dec!(1),
            amount_usdc: price,
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Legacy,
            merge_sources: Vec::new(),
        };
        state
            .active_orders
            .write()
            .await
            .insert(order.client_order_id.clone(), order.clone());

        assert!(!engine.on_order_filled(&mut order).await);
        assert_eq!(*state.status.read().await, BotStatus::Paused);
        assert!(state
            .active_orders
            .read()
            .await
            .contains_key(&order.client_order_id));
        assert_eq!(
            serde_json::to_value(&*state.position.read().await).unwrap(),
            position_before
        );
        assert_eq!(
            serde_json::to_value(&*state.account.read().await).unwrap(),
            account_before
        );
        assert_eq!(
            serde_json::to_value(&*state.stats.read().await).unwrap(),
            stats_before
        );
        assert!(db.get_recent_trades(10).unwrap().is_empty());

        connection
            .execute_batch("DROP TRIGGER reject_paper_stats")
            .unwrap();
        assert!(engine.on_order_filled(&mut order).await);
        assert_eq!(state.position.read().await.size, expected_size);
        assert_eq!(state.position.read().await.entry_price, dec!(100));
        assert_eq!(
            state.account.read().await.total_wallet_balance,
            wallet_before + expected_pnl
        );
        assert!(!state
            .active_orders
            .read()
            .await
            .contains_key(&order.client_order_id));
        assert_eq!(state.stats.read().await.total_trades, 1);
        assert_eq!(state.stats.read().await.total_realized_pnl, expected_pnl);
        let trades = db.get_recent_trades(10).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].realized_pnl, expected_pnl);
        let position_after = serde_json::to_value(&*state.position.read().await).unwrap();
        let account_after = serde_json::to_value(&*state.account.read().await).unwrap();
        assert!(engine.on_order_filled(&mut order).await);
        assert_eq!(
            serde_json::to_value(&*state.position.read().await).unwrap(),
            position_after
        );
        assert_eq!(
            serde_json::to_value(&*state.account.read().await).unwrap(),
            account_after
        );
        assert_eq!(state.stats.read().await.total_trades, 1);
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);

        drop(engine);
        drop(state);
        drop(connection);
        drop(db);
    }
}

#[tokio::test]
async fn unpaired_paper_sell_records_realized_pnl() {
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    state.position.write().await.size = dec!(2.63);
    state.position.write().await.entry_price = dec!(114);
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(115);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }
    let mut order = GridOrder {
        client_order_id: "paper-sell".into(),
        order_id: None,
        symbol: "SOLUSDC".into(),
        side: OrderSide::Sell,
        price: dec!(115.1),
        quantity: dec!(1),
        amount_usdc: dec!(115.1),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 1,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Legacy,
        merge_sources: Vec::new(),
    };
    state
        .active_orders
        .write()
        .await
        .insert(order.client_order_id.clone(), order.clone());

    engine.on_order_filled(&mut order).await;
    let stats = state.stats.read().await;
    assert_eq!(stats.total_realized_pnl, dec!(1.1));
    assert_eq!(stats.completed_cycles, 0);
    drop(stats);
    let trades = state.db.get_recent_trades(10).unwrap();
    assert_eq!(trades[0].realized_pnl, dec!(1.1));
    assert!(trades[0].pnl_verified);
}

#[tokio::test]
async fn sell_window_places_one_remainder_and_replaces_oversized_orders() {
    let mut config = AppConfig::default();
    config.grid.grid_interval = dec!(1);
    config.grid.order_amount_usdc = dec!(2000);
    config.grid.buy_window = 1;
    config.grid.sell_window = 3;
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(115);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }
    state.position.write().await.size = dec!(19.67);
    engine.maintain_grid_window().await;
    let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
    let sell = orders
        .iter()
        .filter(|o| o.side == OrderSide::Sell)
        .min_by_key(|o| o.price)
        .unwrap();
    let expected_qty = state
        .rules
        .read()
        .await
        .calculate_quantity(sell.price, dec!(2000))
        .unwrap();
    assert_eq!(sell.quantity, expected_qty);
    assert_eq!(
        orders.iter().filter(|o| o.side == OrderSide::Sell).count(),
        2
    );
    assert_eq!(reserved_sell_quantity(&orders), dec!(19.67));
    assert_eq!(sell_quantity_available(dec!(19.67), &orders), dec!(0));

    engine.maintain_grid_window().await;
    assert_eq!(state.active_orders.read().await.len(), orders.len());

    let mut oversized = sell.clone();
    oversized.quantity = dec!(20);
    oversized.amount_usdc = oversized.price * oversized.quantity;
    state
        .active_orders
        .write()
        .await
        .insert(oversized.client_order_id.clone(), oversized);

    engine.maintain_grid_window().await;
    let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
    assert_eq!(reserved_sell_quantity(&orders), dec!(19.67));
    assert_eq!(
        orders.iter().filter(|o| o.side == OrderSide::Sell).count(),
        2
    );

    state.position.write().await.size = dec!(-1);
    engine.trim_closing_orders_to_position().await;
    let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
    assert_eq!(reserved_sell_quantity(&orders), dec!(0));
}

#[tokio::test]
async fn sell_remainder_respects_symbol_minimum_after_step_rounding() {
    let mut config = AppConfig::default();
    config.grid.grid_interval = dec!(1);
    config.grid.order_amount_usdc = dec!(2000);
    config.grid.buy_window = 0;
    config.grid.sell_window = 3;
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(115);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }
    state.rules.write().await.min_notional = dec!(50);
    state.position.write().await.size = dec!(0.429);

    engine.maintain_grid_window().await;
    assert!(state.active_orders.read().await.is_empty());

    state.position.write().await.size = dec!(0.449);
    engine.maintain_grid_window().await;
    let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].side, OrderSide::Sell);
    assert_eq!(orders[0].quantity, dec!(0.44));
    assert_eq!(orders[0].amount_usdc, dec!(51.04));

    engine.maintain_grid_window().await;
    assert_eq!(state.active_orders.read().await.len(), 1);
}

#[tokio::test]
async fn sell_window_adds_remainder_after_existing_full_orders() {
    let mut config = AppConfig::default();
    config.grid.grid_interval = dec!(1.2);
    config.grid.order_amount_usdc = dec!(3000);
    config.grid.buy_window = 0;
    config.grid.sell_window = 3;
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(122.15);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }
    state.position.write().await.size = dec!(50.45);

    let first = GridOrder {
        client_order_id: "gb_s_existing_1".into(),
        order_id: None,
        symbol: "SOLUSDC".into(),
        side: OrderSide::Sell,
        price: dec!(122.4),
        quantity: dec!(24.50),
        amount_usdc: dec!(2998.8),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 1,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Legacy,
        merge_sources: Vec::new(),
    };
    let second = GridOrder {
        client_order_id: "gb_s_existing_2".into(),
        price: dec!(123.6),
        quantity: dec!(24.27),
        amount_usdc: dec!(2999.772),
        ..first.clone()
    };
    let mut active = state.active_orders.write().await;
    active.insert(first.client_order_id.clone(), first);
    active.insert(second.client_order_id.clone(), second);
    drop(active);

    engine.maintain_grid_window().await;
    let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
    assert_eq!(orders.len(), 3);
    assert_eq!(reserved_sell_quantity(&orders), dec!(50.45));
    let remainder = orders.iter().find(|o| o.price == dec!(124.8)).unwrap();
    assert_eq!(remainder.quantity, dec!(1.68));
    assert_eq!(remainder.amount_usdc, dec!(209.664));
}

#[tokio::test]
async fn fills_record_actual_quantities_without_creating_counter_orders() {
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db.clone(), tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, rx, ticker_rx);
    state.position.write().await.size = dec!(20);
    for (id, side, price, quantity) in [
        ("gb_b_actual", OrderSide::Buy, dec!(114.24), dec!(17.50)),
        ("gb_s_actual", OrderSide::Sell, dec!(115.24), dec!(0.46)),
    ] {
        let mut order = GridOrder {
            client_order_id: id.into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side,
            price,
            quantity,
            amount_usdc: price * quantity,
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 0,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: OrderPurpose::Grid,
            merge_sources: vec![],
        };
        assert!(engine.on_order_filled(&mut order).await);
        assert_eq!(
            db.get_trade_by_client_id(id, TradingMode::Paper)
                .unwrap()
                .unwrap()
                .quantity,
            quantity
        );
        assert!(state.active_orders.read().await.is_empty());
    }
    assert_eq!(state.position.read().await.size, dec!(37.04));
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Paper)
        .unwrap()
        .is_empty());
}
