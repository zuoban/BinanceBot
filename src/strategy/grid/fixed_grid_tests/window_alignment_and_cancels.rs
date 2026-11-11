use super::*;

#[tokio::test]
async fn recent_fill_reserves_one_level_without_pinning_a_moving_window() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine.state.position.write().await.size = dec!(10);
    engine.state.ticker.write().await.last_price = dec!(119.25);
    let mut buy = order("first_buy", OrderSide::Buy, dec!(119.2), dec!(0.1));
    assert!(engine.on_order_filled(&mut buy).await);
    let config = engine.state.config.read().await.clone();
    let rules = engine.state.rules.read().await.clone();
    assert_eq!(
        engine
            .window_prices(&config, &rules, dec!(119.25))
            .await
            .unwrap(),
        (
            vec![dec!(119.1), dec!(119), dec!(118.9)],
            vec![dec!(119.3), dec!(119.4), dec!(119.5)]
        )
    );
    assert!(
        !engine
            .place_grid_order(
                OrderSide::Buy,
                dec!(119.2),
                "gb_buy_repeat".into(),
                -1,
                None
            )
            .await
    );
    // A rising market may place higher buys while the last filled level stays reserved.
    engine.state.ticker.write().await.last_price = dec!(119.45);
    assert!(
        engine
            .place_grid_order(
                OrderSide::Buy,
                dec!(119.3),
                "gb_buy_higher".into(),
                -1,
                None
            )
            .await
    );
    engine.state.ticker.write().await.last_price = dec!(119.25);
    assert!(
        engine
            .place_grid_order(OrderSide::Buy, dec!(119.1), "gb_buy_lower".into(), -1, None)
            .await
    );
    let mut sell = order("opposite_sell", OrderSide::Sell, dec!(119.3), dec!(0.1));
    assert!(engine.on_order_filled(&mut sell).await);
    assert_eq!(
        engine
            .window_prices(&config, &rules, dec!(119.25))
            .await
            .unwrap(),
        (
            vec![dec!(119.2), dec!(119.1), dec!(119)],
            vec![dec!(119.4), dec!(119.5), dec!(119.6)]
        )
    );
    assert!(
        engine
            .place_grid_order(
                OrderSide::Buy,
                dec!(119.2),
                "gb_buy_reopened".into(),
                -1,
                None
            )
            .await
    );
    assert!(
        !engine
            .place_grid_order(
                OrderSide::Sell,
                dec!(119.3),
                "gb_sell_repeat".into(),
                1,
                None
            )
            .await
    );
    engine.state.ticker.write().await.last_price = dec!(119.15);
    assert!(
        engine
            .place_grid_order(
                OrderSide::Sell,
                dec!(119.2),
                "gb_sell_lower".into(),
                1,
                None
            )
            .await
    );
    engine.state.ticker.write().await.last_price = dec!(119.25);
    assert!(
        engine
            .place_grid_order(
                OrderSide::Sell,
                dec!(119.4),
                "gb_sell_higher".into(),
                1,
                None
            )
            .await
    );
    let mut rebuy = order("rebuy_fill", OrderSide::Buy, dec!(119.2), dec!(0.1));
    assert!(engine.on_order_filled(&mut rebuy).await);
    assert!(
        engine
            .place_grid_order(
                OrderSide::Sell,
                dec!(119.3),
                "gb_sell_reopened".into(),
                1,
                None
            )
            .await
    );
}

#[tokio::test]
async fn migration_aligns_all_idle_orders_and_preserves_partial_executions() {
    let mut engine = paper_engine().await;
    engine.state.position.write().await.size = dec!(1.68);
    let old = order("b_old", OrderSide::Buy, dec!(118.71), dec!(1.68));
    let mut partial = order("b_partial", OrderSide::Buy, dec!(118.59), dec!(0.5));
    partial.status = OrderStatus::PartiallyFilled;
    let parent = order("b_parent", OrderSide::Buy, dec!(118.84), dec!(1.68));
    let exit = new_pair_intent(&parent, OrderSide::Sell, dec!(118.94), parent.quantity);
    for o in [&old, &partial, &exit] {
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(o.client_order_id.clone(), o.clone());
    }
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    assert_eq!(active.len(), 1);
    assert!(!active
        .iter()
        .any(|o| o.client_order_id == old.client_order_id));
    assert!(active
        .iter()
        .any(|o| o.client_order_id == partial.client_order_id));
    assert!(!active
        .iter()
        .any(|o| o.client_order_id == exit.client_order_id));
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    assert!(active.iter().any(|o| o.price == dec!(118.7)));
    assert!(active
        .iter()
        .filter(|o| o.client_order_id != partial.client_order_id
            && o.client_order_id != exit.client_order_id)
        .all(|o| o.price % dec!(0.1) == Decimal::ZERO));
}

#[tokio::test]
async fn duplicate_buy_and_position_limit_apply_to_window_replenishment() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.max_position_usdc = Some(dec!(400));
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    assert_eq!(active.len(), 2);
    assert!(buy_exposure_usdc(Decimal::ZERO, dec!(118.84), &active) <= dec!(400));
    let existing = &active[0];
    assert!(
        !engine
            .place_grid_order(
                OrderSide::Buy,
                existing.price,
                "gb_b_duplicate".into(),
                -1,
                Some("parent".into())
            )
            .await
    );
    assert!(
        !engine
            .place_grid_order(
                OrderSide::Buy,
                dec!(118.5),
                "gb_b_over_limit".into(),
                -1,
                Some("parent".into())
            )
            .await
    );
    assert_eq!(orders(&engine).await.len(), 2);
}

#[tokio::test]
async fn invalid_spacing_is_rejected_before_canceling_or_saving_configuration() {
    let mut engine = paper_engine().await;
    engine.maintain_grid_window().await;
    let original = orders(&engine).await;
    let mut config = engine.state.config.read().await.clone();
    config.grid.grid_interval = dec!(0.015);
    assert!(engine.apply_config(config).await.is_err());
    assert_eq!(
        engine.state.config.read().await.grid.grid_interval,
        dec!(0.1)
    );
    let active = orders(&engine).await;
    assert_eq!(active.len(), original.len());
    assert!(original.iter().all(|o| active
        .iter()
        .any(|a| a.client_order_id == o.client_order_id)));
}

#[tokio::test]
async fn moving_window_keeps_fixed_levels_through_multiple_fills() {
    let mut engine = paper_engine().await;
    engine.maintain_grid_window().await;
    for price in [
        dec!(118.74),
        dec!(118.64),
        dec!(118.94),
        dec!(119.14),
        dec!(119.04),
        dec!(118.94),
    ] {
        engine
            .handle_ticker_update(TickerInfo {
                symbol: "SOLUSDC".into(),
                last_price: price,
                update_time: Utc::now(),
                ..Default::default()
            })
            .await;
        engine.maintain_grid_window().await;
        let active = orders(&engine).await;
        for side in [OrderSide::Buy, OrderSide::Sell] {
            let mut prices: Vec<_> = active
                .iter()
                .filter(|o| o.side == side)
                .map(|o| o.price)
                .collect();
            prices.sort();
            assert!(prices.iter().all(|p| *p % dec!(0.1) == Decimal::ZERO));
            assert!(
                prices.windows(2).all(|p| p[1] - p[0] >= dec!(0.1)),
                "duplicate levels at market {price}: {prices:?}"
            );
        }
        assert!(reserved_sell_quantity(&active) <= engine.state.position.read().await.size);
    }
    assert!(engine.state.stats.read().await.total_trades > 0);
    assert_eq!(engine.state.stats.read().await.completed_cycles, 0);
    let mut trades = engine.state.db.get_recent_trades(100).unwrap();
    trades.reverse();
    assert_execution_steps(&trades, dec!(0.1));
}

#[tokio::test]
async fn counter_order_reuses_an_occupied_level_without_replacing_it() {
    let mut engine = paper_engine().await;
    engine.state.position.write().await.size = dec!(1.68);
    let ordinary = order("s_background", OrderSide::Sell, dec!(118.9), dec!(1.68));
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(ordinary.client_order_id.clone(), ordinary.clone());
    let mut buy = order("b_fill", OrderSide::Buy, dec!(118.8), dec!(1.68));
    engine.on_order_filled(&mut buy).await;
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    let at_level: Vec<_> = active
        .iter()
        .filter(|o| o.side == OrderSide::Sell && o.price == dec!(118.9))
        .collect();
    assert_eq!(at_level.len(), 1);
    assert_eq!(at_level[0].quantity, buy.quantity);
    assert_eq!(at_level[0].client_order_id, ordinary.client_order_id);
}

#[tokio::test]
async fn migration_and_window_pruning_require_terminal_cancellation_and_recover_racing_fill() {
    use axum::{routing::get, Json, Router};
    for (price, terminal) in [
        (dec!(118.81), false),
        (dec!(118.81), true),
        (dec!(118.4), false),
        (dec!(118.4), true),
    ] {
        let mut engine = paper_engine().await;
        engine.state.config.write().await.exchange.api_key = "live-key".into();
        engine.state.config.write().await.exchange.api_secret = "live-secret".into();
        let old = order("b_migration", OrderSide::Buy, price, dec!(1.68));
        let response = serde_json::json!({
            "orderId": 42, "clientOrderId": old.client_order_id,
            "symbol": "SOLUSDC", "status": if terminal { "CANCELED" } else { "PARTIALLY_FILLED" },
            "price": price.to_string(), "avgPrice": price.to_string(), "origQty": "1.68", "executedQty": "0.46",
            "side": "BUY", "type": "LIMIT", "timeInForce": "GTX"
        });
        let app = Router::new()
            .route(
                "/fapi/v1/order",
                get(move || {
                    let response = response.clone();
                    async move { Json(response) }
                })
                .delete(|| async { Json(serde_json::json!({})) }),
            )
            .route("/fapi/v2/positionRisk", get(|| async {
                Json(serde_json::json!([{"symbol":"SOLUSDC", "positionSide":"BOTH",
                    "positionAmt":"0.46", "entryPrice":"118.81", "markPrice":"118.84",
                    "unRealizedProfit":"0", "liquidationPrice":"0", "leverage":"20"}]))
            }))
            .route("/fapi/v2/account", get(|| async {
                Json(serde_json::json!({"totalWalletBalance":"10000", "totalMarginBalance":"10000",
                    "totalUnrealizedProfit":"0", "availableBalance":"10000", "assets":[
                        {"asset":"USDC", "walletBalance":"10000", "marginBalance":"10000",
                         "availableBalance":"10000", "unrealizedProfit":"0"}]}))
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        engine.client = Arc::new(BinanceFuturesClient::with_base_url(
            &engine.state.config.read().await.exchange,
            url,
        ));
        engine.last_orders_sync = Some(Instant::now());
        engine.last_account_sync = Some(Instant::now());
        engine.state.db.save_managed_order(&old).unwrap();
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(old.client_order_id.clone(), old);
        if price == dec!(118.4) {
            // Exercise ordinary window cleanup with a pending cancellation.
            assert!(
                engine
                    .prune_grid_window(&[dec!(118.8), dec!(118.7), dec!(118.6)], &[])
                    .await
            );
        } else {
            engine.maintain_grid_window().await;
        }
        let intents = engine
            .state
            .db
            .load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap();
        if terminal {
            assert!(
                orders(&engine).await.is_empty(),
                "no replenishment before paired intent is reconciled"
            );
            assert!(intents.is_empty());
            assert_eq!(engine.state.db.get_recent_trades(10).unwrap().len(), 1);
        } else {
            assert_eq!(orders(&engine).await.len(), 1);
            assert!(intents.is_empty());
            assert!(engine.state.db.get_recent_trades(10).unwrap().is_empty());
            assert!(engine.last_orders_sync.is_none());
        }
        server.abort();
    }
}

#[tokio::test]
async fn interrupted_window_cleanup_retries_remaining_orders_after_partial_success() {
    use axum::{extract::Query, routing::get, Json, Router};
    use std::sync::atomic::{AtomicBool, Ordering};

    let mut engine = paper_engine().await;
    engine.state.config.write().await.exchange.api_key = "live-key".into();
    engine.state.config.write().await.exchange.api_secret = "live-secret".into();
    let allow_second_cancel = Arc::new(AtomicBool::new(false));
    let gate = allow_second_cancel.clone();
    let app = Router::new().route(
        "/fapi/v1/order",
        get(move |Query(query): Query<HashMap<String, String>>| {
            let gate = gate.clone();
            async move {
                let id = &query["origClientOrderId"];
                Json(serde_json::json!({
                    "orderId": 42, "clientOrderId": id, "symbol": "SOLUSDC",
                    "status": if id == "gb_a" || gate.load(Ordering::SeqCst) { "CANCELED" } else { "NEW" },
                    "price": "118.4", "origQty": "1.68", "executedQty": "0",
                    "side": "BUY", "type": "LIMIT", "timeInForce": "GTX"
                }))
            }
        }).delete(|| async { Json(serde_json::json!({})) }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    engine.client = Arc::new(BinanceFuturesClient::with_base_url(
        &engine.state.config.read().await.exchange,
        url,
    ));
    for (id, price) in [("a", dec!(118.4)), ("b", dec!(118.3))] {
        let o = order(id, OrderSide::Buy, price, dec!(1.68));
        engine.state.db.save_managed_order(&o).unwrap();
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(o.client_order_id.clone(), o);
    }
    assert!(engine.prune_grid_window(&[dec!(118.8)], &[]).await);
    assert_eq!(orders(&engine).await.len(), 1);
    allow_second_cancel.store(true, Ordering::SeqCst);
    assert!(engine.prune_grid_window(&[dec!(118.8)], &[]).await);
    assert!(orders(&engine).await.is_empty());
    assert!(engine
        .state
        .db
        .load_managed_orders("SOLUSDC")
        .unwrap()
        .is_empty());
    server.abort();
}

#[tokio::test]
async fn crossed_unsubmitted_remainder_does_not_starve_sells_with_large_position() {
    let mut engine = paper_engine().await;
    engine.state.ticker.write().await.last_price = dec!(119.35);
    engine.state.position.write().await.size = dec!(107.33);
    for price in [
        dec!(119.0),
        dec!(118.9),
        dec!(118.8),
        dec!(118.7),
        dec!(118.6),
        dec!(118.5),
    ] {
        let buy = order(&format!("b_{price}"), OrderSide::Buy, price, dec!(1.68));
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(buy.client_order_id.clone(), buy);
    }
    let mut target = order(
        "s_stale_remainder",
        OrderSide::Sell,
        dec!(119.1),
        dec!(0.24),
    );
    target.purpose = OrderPurpose::Remainder;
    engine
        .state
        .db
        .save_remainder_plan(&RemainderPlan {
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Paper,
            sources: vec![],
            target,
            phase: RemainderPhase::Submitting,
        })
        .unwrap();

    engine.maintain_grid_window().await;
    assert!(
        engine.load_remainder_plan().await.unwrap().is_none(),
        "crossed unsubmitted target must not block the whole grid forever"
    );
    // Confirm stale buys before replenishing either side of the new window.
    engine.maintain_grid_window().await;
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    let mut sells: Vec<_> = active
        .iter()
        .filter(|o| o.side == OrderSide::Sell)
        .collect();
    sells.sort_by_key(|o| o.price);
    assert_eq!(
        sells.iter().map(|o| o.price).collect::<Vec<_>>(),
        vec![dec!(119.4), dec!(119.5), dec!(119.6)]
    );
    assert_eq!(reserved_sell_quantity(&active), dec!(5.01));
    assert_eq!(
        sells.iter().map(|o| o.quantity).collect::<Vec<_>>(),
        vec![dec!(1.67), dec!(1.67), dec!(1.67)]
    );
}

#[tokio::test]
async fn restart_recenters_sells_near_market_even_after_higher_sell_fills() {
    let engine = paper_engine().await;
    engine.state.config.write().await.grid.order_amount_usdc = dec!(300);
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine.state.position.write().await.size = dec!(100);
    engine.state.ticker.write().await.last_price = dec!(119.19);
    for (id, price) in [
        dec!(119.2),
        dec!(119.3),
        dec!(119.4),
        dec!(119.5),
        dec!(119.6),
        dec!(119.7),
        dec!(119.8),
        dec!(120.1),
    ]
    .into_iter()
    .enumerate()
    {
        engine
            .state
            .db
            .insert_trade(&journal_trade(id, OrderSide::Sell, price, dec!(2.51)))
            .unwrap();
    }
    for price in [dec!(119.9), dec!(120), dec!(120.2)] {
        let sell = order(&format!("s_old_{price}"), OrderSide::Sell, price, dec!(2.5));
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(sell.client_order_id.clone(), sell);
    }
    let (_, rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let mut restarted =
        GridTradingEngine::new(engine.state.clone(), engine.client.clone(), rx, ticker_rx);
    // Cancel and confirm the old window before reusing its inventory.
    restarted.maintain_grid_window().await;
    restarted.maintain_grid_window().await;
    restarted.maintain_grid_window().await;
    let mut sells: Vec<_> = orders(&restarted)
        .await
        .into_iter()
        .filter(|o| o.side == OrderSide::Sell)
        .map(|o| o.price)
        .collect();
    sells.sort();
    assert_eq!(sells, vec![dec!(119.2), dec!(119.3), dec!(119.4)]);
}

#[tokio::test]
async fn sell_window_tracks_rising_falling_and_exact_market_levels_with_limited_inventory() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.order_amount_usdc = dec!(300);
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine.state.position.write().await.size = dec!(7.6);
    for (market, expected) in [
        (dec!(119.19), vec![dec!(119.2), dec!(119.3), dec!(119.4)]),
        (dec!(119.25), vec![dec!(119.3), dec!(119.4), dec!(119.5)]),
        (dec!(118.95), vec![dec!(119), dec!(119.1), dec!(119.2)]),
        (dec!(120), vec![dec!(120.1), dec!(120.2), dec!(120.3)]),
    ] {
        engine.state.ticker.write().await.last_price = market;
        let previous = orders(&engine).await;
        for _ in 0..4 {
            engine.maintain_grid_window().await;
        }
        let active = orders(&engine).await;
        let mut sells: Vec<_> = active
            .iter()
            .filter(|o| o.side == OrderSide::Sell)
            .map(|o| o.price)
            .collect();
        sells.sort();
        assert_eq!(sells, expected);
        assert!(reserved_sell_quantity(&active) <= dec!(7.6));
        // Orders still inside the band retain their queue position.
        for old in previous
            .iter()
            .filter(|o| o.side == OrderSide::Sell && expected.contains(&o.price))
        {
            assert!(active
                .iter()
                .any(|o| o.client_order_id == old.client_order_id));
        }
    }
}

#[tokio::test]
async fn stale_orders_require_confirmed_cancel_and_record_racing_fills() {
    use axum::{routing::get, Json, Router};
    for side in [OrderSide::Buy, OrderSide::Sell] {
        for terminal in [false, true] {
            let mut engine = paper_engine().await;
            engine.state.config.write().await.exchange.api_key = "live-key".into();
            engine.state.config.write().await.exchange.api_secret = "live-secret".into();
            engine.state.position.write().await.size = dec!(100);
            let price = if side == OrderSide::Buy {
                dec!(118.2)
            } else {
                dec!(120)
            };
            let old = order("stale", side, price, dec!(1.66));
            let response = serde_json::json!({
                "orderId": 42, "clientOrderId": old.client_order_id,
                "symbol": "SOLUSDC", "status": if terminal { "CANCELED" } else { "PARTIALLY_FILLED" },
                "price": price.to_string(), "avgPrice": price.to_string(), "origQty": "1.66", "executedQty": "0.46",
                "side": side.as_str(), "type": "LIMIT", "timeInForce": "GTX"
            });
            let app = Router::new()
                .route(
                    "/fapi/v1/order",
                    get(move || {
                        let response = response.clone();
                        async move { Json(response) }
                    })
                    .delete(|| async { Json(serde_json::json!({})) }),
                )
                .route("/fapi/v2/positionRisk", get(|| async {
                    Json(serde_json::json!([{"symbol":"SOLUSDC", "positionSide":"BOTH",
                        "positionAmt":"100", "entryPrice":"118.81", "markPrice":"118.84",
                        "unRealizedProfit":"0", "liquidationPrice":"0", "leverage":"20"}]))
                }))
                .route("/fapi/v2/account", get(|| async {
                    Json(serde_json::json!({"totalWalletBalance":"10000", "totalMarginBalance":"10000",
                        "totalUnrealizedProfit":"0", "availableBalance":"10000", "assets":[
                            {"asset":"USDC", "walletBalance":"10000", "marginBalance":"10000",
                             "availableBalance":"10000", "unrealizedProfit":"0"}]}))
                }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            engine.client = Arc::new(BinanceFuturesClient::with_base_url(
                &engine.state.config.read().await.exchange,
                url,
            ));
            engine.last_orders_sync = Some(Instant::now());
            engine.last_account_sync = Some(Instant::now());
            engine.state.db.save_managed_order(&old).unwrap();
            engine
                .state
                .active_orders
                .write()
                .await
                .insert(old.client_order_id.clone(), old.clone());
            engine.maintain_grid_window().await;
            let active = orders(&engine).await;
            let fills = engine.state.db.get_recent_trades(10).unwrap();
            if terminal {
                assert!(
                    active.is_empty(),
                    "replenish only after the next reconciliation cycle"
                );
                assert_eq!(fills.len(), 1);
                assert_eq!(fills[0].side, side);
                let intents = engine
                    .state
                    .db
                    .load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap();
                assert!(intents.is_empty());
                assert_eq!(fills[0].quantity, dec!(0.46));
            } else {
                assert_eq!(active.len(), 1);
                assert_eq!(active[0].client_order_id, old.client_order_id);
                assert!(fills.is_empty());
            }
            server.abort();
        }
    }
}

#[tokio::test]
async fn unsubmitted_remainder_at_last_sell_price_is_retired() {
    let mut engine = paper_engine().await;
    engine.state.position.write().await.size = dec!(100);
    engine.state.ticker.write().await.last_price = dec!(119.45);
    let mut sell = order("s_initial", OrderSide::Sell, dec!(119.5), dec!(1.67));
    assert!(engine.on_order_filled(&mut sell).await);
    assert!(
        !engine
            .place_grid_order(OrderSide::Sell, dec!(119.5), "gb_s_again".into(), 1, None)
            .await
    );
    let mut target = order("s_remainder", OrderSide::Sell, dec!(119.5), dec!(0.1));
    target.purpose = OrderPurpose::Remainder;
    engine
        .state
        .db
        .save_remainder_plan(&RemainderPlan {
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Paper,
            sources: vec![],
            target: target.clone(),
            phase: RemainderPhase::Submitting,
        })
        .unwrap();
    engine.maintain_grid_window().await;
    assert!(engine.load_remainder_plan().await.unwrap().is_none());
    let active = orders(&engine).await;
    assert!(active
        .iter()
        .all(|o| o.client_order_id != target.client_order_id));
    assert!(active
        .iter()
        .filter(|o| o.side == OrderSide::Sell)
        .all(|o| o.price != dec!(119.5)));
}
