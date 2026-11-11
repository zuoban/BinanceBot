use super::*;

#[tokio::test]
async fn max_position_pause_cancels_both_sides_and_survives_restart() {
    let mut engine = paper_engine().await;
    {
        let mut config = engine.state.config.write().await;
        config.grid.max_position_usdc = Some(dec!(1000));
        config.grid.pause_on_max_position = true;
    }
    {
        let mut ticker = engine.state.ticker.write().await;
        ticker.last_price = dec!(95);
        ticker.mark_price = dec!(100);
    }
    engine.state.position.write().await.size = dec!(10);
    for pending in [
        order("limit_buy", OrderSide::Buy, dec!(94.9), dec!(1)),
        order("limit_sell", OrderSide::Sell, dec!(100.1), dec!(1)),
    ] {
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(pending.client_order_id.clone(), pending);
    }

    engine.maintain_grid_window().await;
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(orders(&engine).await.is_empty());
    assert_eq!(engine.state.position.read().await.size, dec!(10));
    assert_eq!(
        engine.state.db.load_bot_status().unwrap(),
        Some(BotStatus::Paused)
    );
    let logs = engine.state.recent_logs.read().await.len();
    engine.maintain_grid_window().await;
    assert_eq!(engine.state.recent_logs.read().await.len(), logs);

    let restored = paper_engine_with_db(engine.state.db.clone()).await;
    assert_eq!(*restored.state.status.read().await, BotStatus::Paused);
}

#[tokio::test]
async fn max_position_pause_ignores_pending_exposure_and_uses_mark_price() {
    let mut engine = paper_engine().await;
    {
        let mut config = engine.state.config.write().await;
        config.grid.max_position_usdc = Some(dec!(1000));
        config.grid.pause_on_max_position = true;
    }
    engine.state.position.write().await.size = dec!(9);
    {
        let mut ticker = engine.state.ticker.write().await;
        ticker.last_price = dec!(120);
        ticker.mark_price = dec!(100);
    }
    let pending = order("pending_limit", OrderSide::Buy, dec!(99.9), dec!(2));
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(pending.client_order_id.clone(), pending);
    assert!(!engine.pause_if_position_limit_reached().await);
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    assert_eq!(orders(&engine).await.len(), 1);

    engine.state.ticker.write().await.mark_price = dec!(112);
    engine.state.config.write().await.grid.pause_on_max_position = false;
    assert!(!engine.pause_if_position_limit_reached().await);
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
}

#[tokio::test]
async fn next_buy_over_position_limit_pauses_cancels_and_persists_without_filling() {
    let mut engine = paper_engine().await;
    {
        let mut config = engine.state.config.write().await;
        config.grid.max_position_usdc = Some(dec!(1000));
        config.grid.pause_on_max_position = true;
    }
    {
        let mut ticker = engine.state.ticker.write().await;
        ticker.last_price = dec!(100.05);
        ticker.mark_price = dec!(100);
    }
    engine.state.position.write().await.size = dec!(9);
    for pending in [
        order("near_limit_buy", OrderSide::Buy, dec!(99.8), dec!(0.5)),
        order("near_limit_sell", OrderSide::Sell, dec!(100.1), dec!(1)),
    ] {
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(pending.client_order_id.clone(), pending);
    }

    engine.maintain_grid_window().await;
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(orders(&engine).await.is_empty());
    assert_eq!(engine.state.position.read().await.size, dec!(9));
    assert!(engine.state.db.get_recent_trades(10).unwrap().is_empty());
    assert_eq!(
        engine.state.db.load_bot_status().unwrap(),
        Some(BotStatus::Paused)
    );
    assert!(engine.state.recent_logs.read().await.iter().any(|entry| {
        entry.message.contains("下一笔买单") && entry.message.contains("预计持仓名义价值 1100")
    }));
    let log_count = engine.state.recent_logs.read().await.len();
    engine.maintain_grid_window().await;
    assert_eq!(engine.state.recent_logs.read().await.len(), log_count);
    let restored = paper_engine_with_db(engine.state.db.clone()).await;
    assert_eq!(*restored.state.status.read().await, BotStatus::Paused);
}

#[tokio::test]
async fn next_buy_exactly_at_limit_is_allowed_and_pauses_after_fill() {
    let mut engine = paper_engine().await;
    {
        let mut config = engine.state.config.write().await;
        config.grid.max_position_usdc = Some(dec!(1000));
        config.grid.pause_on_max_position = true;
    }
    {
        let mut ticker = engine.state.ticker.write().await;
        ticker.last_price = dec!(100);
        ticker.mark_price = dec!(100);
    }
    engine.state.position.write().await.size = dec!(8);
    assert!(
        engine
            .place_grid_order(
                OrderSide::Buy,
                dec!(99.9),
                "gb_exact_limit".into(),
                -1,
                None
            )
            .await
    );
    // The configured 200 USDC order rounds down to 2 units (199.8 USDC at its limit price).
    assert_eq!(orders(&engine).await[0].quantity, dec!(2));
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    engine
        .handle_ticker_update(TickerInfo {
            symbol: "SOLUSDC".into(),
            last_price: dec!(99.9),
            mark_price: dec!(100),
            update_time: Utc::now(),
            mark_update_time: Utc::now(),
            ..Default::default()
        })
        .await;
    assert_eq!(engine.state.position.read().await.size, dec!(10));
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(orders(&engine).await.is_empty());
    assert_eq!(engine.state.db.get_recent_trades(10).unwrap().len(), 1);
}

#[tokio::test]
async fn pending_buys_reserve_capacity_without_pausing_the_window() {
    let mut engine = paper_engine().await;
    {
        let mut config = engine.state.config.write().await;
        config.grid.max_position_usdc = Some(dec!(1000));
        config.grid.pause_on_max_position = true;
        config.grid.buy_window = 4;
    }
    {
        let mut ticker = engine.state.ticker.write().await;
        ticker.last_price = dec!(100.05);
        ticker.mark_price = dec!(100);
    }
    engine.state.position.write().await.size = dec!(4);
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    assert_eq!(
        active
            .iter()
            .filter(|order| order.side == OrderSide::Buy)
            .count(),
        3
    );
    assert!(buy_exposure_usdc(dec!(4), dec!(100.05), &active) <= dec!(1000));
}

#[tokio::test]
async fn next_buy_over_limit_only_skips_when_pause_setting_is_disabled() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.max_position_usdc = Some(dec!(1000));
    {
        let mut ticker = engine.state.ticker.write().await;
        ticker.last_price = dec!(100.05);
        ticker.mark_price = dec!(100);
    }
    engine.state.position.write().await.size = dec!(9);
    let sell = order("disabled_limit_sell", OrderSide::Sell, dec!(100.1), dec!(1));
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(sell.client_order_id.clone(), sell);
    assert!(
        !engine
            .place_grid_order(OrderSide::Buy, dec!(99.9), "gb_skip_limit".into(), -1, None)
            .await
    );
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    assert_eq!(orders(&engine).await.len(), 1);
    assert_eq!(engine.state.position.read().await.size, dec!(9));
}

#[tokio::test]
async fn next_buy_pause_uses_mark_price_fallbacks_and_allows_short_reduction() {
    for (mark_price, position_mark_price, last_price, position_size, expected_pause) in [
        (dec!(100), dec!(90), dec!(100.05), dec!(9), true),
        (dec!(0), dec!(100), dec!(100.05), dec!(9), true),
        (dec!(0), dec!(0), dec!(100.05), dec!(9), true),
        // The limit price alone fits, but valuing the added quantity at the mark does not.
        (dec!(100), dec!(100), dec!(100.05), dec!(8.002), true),
        // A short strategy's closing buy must not cause an early pause.
        (dec!(100), dec!(100), dec!(100.05), dec!(-9), false),
    ] {
        let mut engine = paper_engine().await;
        {
            let mut config = engine.state.config.write().await;
            config.grid.max_position_usdc = Some(dec!(1000));
            config.grid.pause_on_max_position = true;
            if position_size < Decimal::ZERO {
                config.grid.direction = GridDirection::Short;
            }
        }
        {
            let mut ticker = engine.state.ticker.write().await;
            ticker.mark_price = mark_price;
            ticker.last_price = last_price;
        }
        {
            let mut position = engine.state.position.write().await;
            position.size = position_size;
            position.mark_price = position_mark_price;
        }
        let placed = engine
            .place_grid_order(
                OrderSide::Buy,
                dec!(99.9),
                "gb_valuation_limit".into(),
                -1,
                None,
            )
            .await;
        assert_eq!(placed, !expected_pause);
        assert_eq!(
            *engine.state.status.read().await,
            if expected_pause {
                BotStatus::Paused
            } else {
                BotStatus::Running
            }
        );
        assert_eq!(engine.state.position.read().await.size, position_size);
    }
}

#[tokio::test]
async fn max_position_pause_stops_paper_fill_batch_at_the_limit() {
    let mut engine = paper_engine().await;
    {
        let mut config = engine.state.config.write().await;
        config.grid.max_position_usdc = Some(dec!(1000));
        config.grid.pause_on_max_position = true;
    }
    engine.state.position.write().await.size = dec!(9);
    for pending in [
        order("limit_first", OrderSide::Buy, dec!(99), dec!(1)),
        order("limit_second", OrderSide::Buy, dec!(98), dec!(1)),
    ] {
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(pending.client_order_id.clone(), pending);
    }
    engine
        .handle_ticker_update(TickerInfo {
            symbol: "SOLUSDC".into(),
            last_price: dec!(95),
            mark_price: dec!(100),
            update_time: Utc::now(),
            mark_update_time: Utc::now(),
            ..Default::default()
        })
        .await;
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert_eq!(engine.state.position.read().await.size, dec!(10));
    assert!(orders(&engine).await.is_empty());
    let trades = engine.state.db.get_recent_trades(10).unwrap();
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].client_order_id, "gb_limit_first");

    engine
        .handle_ticker_update(TickerInfo {
            symbol: "SOLUSDC".into(),
            last_price: dec!(90),
            mark_price: dec!(90),
            update_time: Utc::now(),
            mark_update_time: Utc::now(),
            ..Default::default()
        })
        .await;
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(orders(&engine).await.is_empty());
}

#[tokio::test]
async fn max_position_pause_handles_short_positions_and_price_fallbacks() {
    for (mark_price, position_mark_price, last_price) in [
        (dec!(101), dec!(90), dec!(90)),
        (dec!(0), dec!(101), dec!(90)),
        (dec!(0), dec!(0), dec!(101)),
    ] {
        let mut engine = paper_engine().await;
        {
            let mut config = engine.state.config.write().await;
            config.grid.max_position_usdc = Some(dec!(1000));
            config.grid.pause_on_max_position = true;
        }
        {
            let mut position = engine.state.position.write().await;
            position.size = dec!(-10);
            position.mark_price = position_mark_price;
        }
        {
            let mut ticker = engine.state.ticker.write().await;
            ticker.mark_price = mark_price;
            ticker.last_price = last_price;
        }
        engine.maintain_grid_window().await;
        assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
        assert_eq!(engine.state.position.read().await.size, dec!(-10));
    }
}

#[tokio::test]
async fn enabling_max_position_pause_applies_immediately_and_is_saved() {
    let mut engine = paper_engine().await;
    engine.state.position.write().await.size = dec!(10);
    engine.state.ticker.write().await.mark_price = dec!(100);
    let mut config = engine.state.config.read().await.clone();
    config.grid.max_position_usdc = Some(dec!(1000));
    config.grid.pause_on_max_position = true;

    engine.apply_config(config).await.unwrap();
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(
        engine
            .state
            .db
            .load_config()
            .unwrap()
            .unwrap()
            .grid
            .pause_on_max_position
    );
    assert!(
        engine
            .state
            .config
            .read()
            .await
            .summary()
            .pause_on_max_position
    );
}

#[tokio::test]
async fn max_position_pause_triggers_from_price_updates_before_paper_sells() {
    let mut engine = paper_engine().await;
    {
        let mut config = engine.state.config.write().await;
        config.grid.max_position_usdc = Some(dec!(1000));
        config.grid.pause_on_max_position = true;
    }
    engine.state.position.write().await.size = dec!(10);
    engine.state.ticker.write().await.mark_price = dec!(99);
    let pending = order("sell_price_rise", OrderSide::Sell, dec!(100), dec!(1));
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(pending.client_order_id.clone(), pending);
    engine
        .handle_ticker_update(TickerInfo {
            symbol: "SOLUSDC".into(),
            last_price: dec!(101),
            mark_price: dec!(101),
            update_time: Utc::now(),
            mark_update_time: Utc::now(),
            ..Default::default()
        })
        .await;
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert_eq!(engine.state.position.read().await.size, dec!(10));
    assert!(orders(&engine).await.is_empty());
    assert!(engine.state.db.get_recent_trades(10).unwrap().is_empty());
}

#[tokio::test]
async fn max_position_pause_on_live_sync_stays_paused_when_cancellation_fails() {
    use axum::{http::StatusCode, routing::get, Json, Router};

    let pending = crate::exchange::BinanceOrderResponse {
        order_id: 42,
        client_order_id: "gb_live_position_limit".into(),
        symbol: "SOLUSDC".into(),
        status: "PARTIALLY_FILLED".into(),
        price: dec!(99),
        avg_price: Some(dec!(99)),
        orig_qty: dec!(2),
        executed_qty: dec!(1),
        side: "BUY".into(),
        order_type: "LIMIT".into(),
        time_in_force: "GTX".into(),
        update_time: None,
    };
    let app = Router::new()
        .route(
            "/fapi/v1/premiumIndex",
            get(|| async { Json(serde_json::json!({"symbol": "SOLUSDC", "markPrice": "100"})) }),
        )
        .route(
            "/fapi/v1/openOrders",
            get(move || {
                let pending = pending.clone();
                async move { Json(vec![pending]) }
            }),
        )
        .route(
            "/fapi/v2/positionRisk",
            get(|| async {
                Json(serde_json::json!([{
                    "symbol": "SOLUSDC", "positionAmt": "10", "entryPrice": "99",
                    "markPrice": "100", "unRealizedProfit": "10",
                    "liquidationPrice": "0", "leverage": "20"
                }]))
            }),
        )
        // Position synchronization alone must trigger the pause, even if balances fail.
        .route(
            "/fapi/v2/account",
            get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
        )
        .route(
            "/fapi/v1/order",
            axum::routing::delete(|| async { StatusCode::SERVICE_UNAVAILABLE }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut engine = paper_engine().await;
    {
        let mut config = engine.state.config.write().await;
        config.exchange.api_key = "live-key".into();
        config.exchange.api_secret = "live-secret".into();
        config.grid.max_position_usdc = Some(dec!(1000));
        config.grid.pause_on_max_position = true;
        engine.client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    }
    engine.state.position.write().await.size = dec!(9);
    engine.sync_cycle(true).await;
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert_eq!(engine.state.position.read().await.size, dec!(10));
    assert_eq!(
        engine.state.db.load_bot_status().unwrap(),
        Some(BotStatus::Paused)
    );
    assert_eq!(orders(&engine).await.len(), 1);
    assert!(engine.state.recent_logs.read().await.iter().any(|entry| {
        entry.level == "ERROR" && entry.message.contains("部分策略挂单可能尚未撤销")
    }));
    server.abort();
}
