use super::*;

async fn resume(engine: &mut GridTradingEngine, expected_mode: TradingMode) -> Result<(), String> {
    let (reply, response) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Resume {
            expected_mode,
            expected_epoch: engine.state.pause_epoch(),
            reply,
        })
        .await;
    response.await.unwrap()
}

#[tokio::test]
async fn persisted_pause_actions_preserve_queued_resume_epoch() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    for action in [
        BotControlAction::PauseAlreadyPersisted,
        BotControlAction::CancelAllAlreadyPersisted,
    ] {
        let db = database();
        let mut engine = engine(db.clone(), &url).await;
        fake.orders.lock().unwrap().insert(
            "gb_b_test".into(),
            BinanceOrderResponse {
                order_id: 42,
                client_order_id: "gb_b_test".into(),
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
        engine.state.pause_trading().await.unwrap();
        let epoch = engine.state.pause_epoch();
        let (reply, response) = tokio::sync::oneshot::channel();
        let pending = BotControlAction::Resume {
            expected_mode: TradingMode::Live,
            expected_epoch: epoch,
            reply,
        };

        engine.handle_control_action(action).await;
        assert_eq!(engine.state.pause_epoch(), epoch);
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
        assert_eq!(fake.orders.lock().unwrap()["gb_b_test"].status, "CANCELED");
        engine.handle_control_action(pending).await;
        assert!(response.await.unwrap().is_ok());
        assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    }
    server.abort();
}

#[tokio::test]
async fn internal_pause_actions_still_persist_pause() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake).await;
    for action in [BotControlAction::Pause, BotControlAction::CancelAll] {
        let db = database();
        let mut engine = engine(db.clone(), &url).await;
        let epoch = engine.state.pause_epoch();
        engine.handle_control_action(action).await;
        assert!(engine.state.pause_epoch() > epoch);
        assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    }
    server.abort();
}

#[tokio::test]
async fn resume_rejects_changed_and_unknown_modes_before_exchange_work() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let mut engine = engine(database(), &url).await;
    engine.state.pause_trading().await.unwrap();
    let mut paper = engine.state.config.read().await.clone();
    paper.exchange.api_key.clear();
    paper.exchange.api_secret.clear();
    engine
        .state
        .save_runtime_config(paper.clone())
        .await
        .unwrap();
    let (reply, response) = tokio::sync::oneshot::channel();
    let pending = BotControlAction::Resume {
        expected_mode: TradingMode::Paper,
        expected_epoch: engine.state.pause_epoch(),
        reply,
    };
    paper.exchange.api_key = "live-key".into();
    paper.exchange.api_secret = "live-secret".into();
    engine.state.save_runtime_config(paper).await.unwrap();
    engine.handle_control_action(pending).await;
    assert!(response.await.unwrap().is_err());
    assert!(resume(&mut engine, TradingMode::Unknown).await.is_err());
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(!engine.exchange_rules_loaded);
    assert!(fake.posts.lock().unwrap().is_empty());
    *engine.state.ticker.write().await = TickerInfo::default();
    assert!(resume(&mut engine, TradingMode::Live).await.is_ok());
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    server.abort();
}

#[tokio::test]
async fn queued_resume_rejects_a_later_pause() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    engine.state.pause_trading().await.unwrap();
    let (reply, response) = tokio::sync::oneshot::channel();
    let pending = BotControlAction::Resume {
        expected_mode: TradingMode::Live,
        expected_epoch: engine.state.pause_epoch(),
        reply,
    };
    engine.state.pause_trading().await.unwrap();
    engine.handle_control_action(pending).await;
    assert!(response.await.unwrap().is_err());
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    server.abort();
}

#[tokio::test]
async fn resume_reloads_rules_after_initialization_failure_and_rejects_invalid_spacing() {
    let fake = Exchange::default();
    fake.rules_unavailable.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let mut engine = engine(database(), &url).await;
    engine.state.config.write().await.grid.grid_interval = dec!(0.00005);
    assert!(engine.initialize().await.is_err());
    engine.state.pause_trading().await.unwrap();

    assert!(resume(&mut engine, TradingMode::Live).await.is_err());
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(!engine.exchange_rules_loaded);
    assert_eq!(engine.state.rules.read().await.tick_size, dec!(0.01));
    assert!(fake.posts.lock().unwrap().is_empty());

    fake.rules_unavailable.store(false, Ordering::SeqCst);
    engine.state.config.write().await.grid.grid_interval = dec!(0.000015);
    assert!(resume(&mut engine, TradingMode::Live).await.is_err());
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert_eq!(engine.state.rules.read().await.tick_size, dec!(0.00001));
    assert!(fake.posts.lock().unwrap().is_empty());

    engine.state.config.write().await.grid.grid_interval = dec!(0.00005);
    assert!(resume(&mut engine, TradingMode::Live).await.is_ok());
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    server.abort();
}

#[tokio::test]
async fn paper_resume_retries_rules_after_initialization_failure() {
    let fake = Exchange::default();
    fake.rules_unavailable.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let mut engine = engine(database(), &url).await;
    {
        let mut config = engine.state.config.write().await;
        config.exchange.api_key.clear();
        config.exchange.api_secret.clear();
        config.grid.grid_interval = dec!(0.00005);
    }
    assert!(engine.initialize().await.is_err());
    engine.state.pause_trading().await.unwrap();
    *engine.state.ticker.write().await = TickerInfo::default();

    assert!(resume(&mut engine, TradingMode::Paper).await.is_err());
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(!engine.exchange_rules_loaded);

    fake.rules_unavailable.store(false, Ordering::SeqCst);
    assert!(resume(&mut engine, TradingMode::Paper).await.is_ok());
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    assert!(engine.exchange_rules_loaded);
    assert_eq!(engine.state.rules.read().await.tick_size, dec!(0.00001));
    server.abort();
}

#[tokio::test]
async fn paper_resume_can_use_default_rules_when_exchange_is_unavailable() {
    let fake = Exchange::default();
    fake.rules_unavailable.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake).await;
    let mut engine = engine(database(), &url).await;
    engine.state.config.write().await.exchange.api_key.clear();
    engine
        .state
        .config
        .write()
        .await
        .exchange
        .api_secret
        .clear();
    engine.state.pause_trading().await.unwrap();
    *engine.state.ticker.write().await = TickerInfo::default();

    assert!(resume(&mut engine, TradingMode::Paper).await.is_ok());
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    assert!(!engine.exchange_rules_loaded);
    assert_eq!(engine.state.rules.read().await.tick_size, dec!(0.01));
    server.abort();
}

#[tokio::test]
async fn immediate_partial_submission_refreshes_risk_and_retains_order_on_failure() {
    let fake = Exchange::default();
    fake.fill_on_submit.store(1, Ordering::SeqCst);
    fake.position_unavailable.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    assert!(!submit(&mut engine).await);
    let response = fake.orders.lock().unwrap()["gb_b_reliability"].clone();
    assert_eq!(
        engine.state.active_orders.read().await["gb_b_reliability"].quantity,
        response.orig_qty
    );
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].quantity,
        response.orig_qty
    );
    assert!(engine.last_account_sync.is_none());
    assert!(engine.reconciliation_blocked);

    fake.position_unavailable.store(false, Ordering::SeqCst);
    assert!(engine.sync_live_orders().await);
    assert_eq!(
        engine.state.active_orders.read().await["gb_b_reliability"].quantity,
        response.orig_qty - response.executed_qty
    );
    assert_eq!(engine.state.position.read().await.size, dec!(1));
    server.abort();
}

#[tokio::test]
async fn immediate_partial_submission_updates_position_before_publishing_remainder() {
    let fake = Exchange::default();
    fake.fill_on_submit.store(1, Ordering::SeqCst);
    fake.position_flat.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    engine
        .position_cache
        .get(&engine.client, "SOLUSDC")
        .await
        .unwrap();
    engine.account_cache.get(&engine.client).await.unwrap();
    engine.state.position.write().await.size = dec!(0);
    fake.position_flat.store(false, Ordering::SeqCst);
    assert!(submit(&mut engine).await);
    let response = fake.orders.lock().unwrap()["gb_b_reliability"].clone();
    let active = engine.state.active_orders.read().await;
    assert_eq!(
        active["gb_b_reliability"].status,
        OrderStatus::PartiallyFilled
    );
    assert_eq!(
        active["gb_b_reliability"].quantity,
        response.orig_qty - response.executed_qty
    );
    assert_eq!(engine.state.position.read().await.size, dec!(1));
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].quantity,
        response.orig_qty - response.executed_qty
    );
    server.abort();
}

#[tokio::test]
async fn immediate_terminal_submission_records_fill_once() {
    let fake = Exchange::default();
    fake.fill_on_submit.store(2, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    assert!(submit(&mut engine).await);
    assert!(engine.state.active_orders.read().await.is_empty());
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    assert!(engine.sync_live_orders().await);
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn immediate_partial_and_full_fills_pause_when_position_reaches_limit() {
    for response in [1, 2] {
        let fake = Exchange::default();
        fake.fill_on_submit.store(response, Ordering::SeqCst);
        fake.position_large.store(true, Ordering::SeqCst);
        let (url, server) = exchange_server(fake).await;
        let db = database();
        let mut engine = engine(db.clone(), &url).await;
        engine.state.position.write().await.size = dec!(0);
        {
            let mut config = engine.state.config.write().await;
            config.grid.max_position_usdc = Some(dec!(150));
            config.grid.pause_on_max_position = true;
        }
        assert!(!submit(&mut engine).await);
        assert_eq!(engine.state.position.read().await.size, dec!(2));
        assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
        server.abort();
    }
}

#[tokio::test]
async fn immediate_fills_suspend_later_openings_and_cancel_existing_window_orders() {
    for response in [1, 2] {
        let fake = Exchange::default();
        fake.fill_on_submit.store(response, Ordering::SeqCst);
        fake.liquidation_close_after_submission
            .store(true, Ordering::SeqCst);
        let db = database();
        for (id, order_id, side, price) in [
            ("gb_guard_opening", 123, OrderSide::Buy, dec!(99)),
            ("gb_guard_exit", 124, OrderSide::Sell, dec!(101)),
        ] {
            let (mut order, mut accepted) = managed_price_fixture();
            order.client_order_id = id.into();
            order.order_id = Some(order_id);
            order.side = side;
            order.price = price;
            order.quantity = dec!(1);
            order.amount_usdc = price;
            order.paired_client_order_id = None;
            accepted.client_order_id = id.into();
            accepted.order_id = order_id;
            accepted.side = side.as_str().into();
            accepted.price = price;
            accepted.orig_qty = order.quantity;
            db.save_managed_order(&order).unwrap();
            fake.orders.lock().unwrap().insert(id.into(), accepted);
        }
        let (url, server) = exchange_server(fake.clone()).await;
        let mut engine = engine(db.clone(), &url).await;
        {
            let mut config = engine.state.config.write().await;
            config.grid.buy_window = 3;
            config.grid.min_liquidation_distance_pct = Some(dec!(10));
        }

        engine.maintain_grid_window().await;

        assert_eq!(*engine.state.status.read().await, BotStatus::Running);
        assert_eq!(
            engine.state.position.read().await.liquidation_price,
            dec!(95)
        );
        let submitted = fake.posts.lock().unwrap().clone();
        assert_eq!(submitted.len(), 1, "response={response}");
        assert_eq!(fake.orders.lock().unwrap()[&submitted[0]].side, "BUY");
        assert_eq!(
            fake.orders.lock().unwrap()["gb_guard_opening"].status,
            "CANCELED"
        );
        assert_eq!(fake.orders.lock().unwrap()["gb_guard_exit"].status, "NEW");
        let active = engine.state.active_orders.read().await;
        assert!(active.contains_key("gb_guard_exit"));
        assert!(!active.contains_key("gb_guard_opening"));
        assert!(!active
            .values()
            .any(|order| order.side == OrderSide::Buy && order.status == OrderStatus::New));
        if response == 1 {
            assert_eq!(active[&submitted[0]].status, OrderStatus::PartiallyFilled);
            assert_eq!(active[&submitted[0]].quantity, dec!(0.5));
            assert_eq!(
                fake.orders.lock().unwrap()[&submitted[0]].status,
                "PARTIALLY_FILLED"
            );
        }
        drop(active);
        assert!(
            !engine
                .place_grid_order(OrderSide::Buy, dec!(98), "gb_guard_later".into(), -1, None)
                .await
        );
        assert_eq!(fake.posts.lock().unwrap().len(), 1);
        assert!(db
            .load_managed_orders("SOLUSDC")
            .unwrap()
            .iter()
            .any(|order| order.client_order_id == "gb_guard_exit"));
        server.abort();
    }
}

#[tokio::test]
async fn closed_submission_gates_do_not_block_protective_opening_cancellation_or_allow_posts() {
    for direction in [GridDirection::Long, GridDirection::Short] {
        for gate in ["pending_fill", "blocked", "configuration"] {
            for entry in ["maintenance", "ticker", "placement", "sync_unready"] {
                // This exchange fixture reports a long position during reconciliation.
                if direction == GridDirection::Short && entry == "sync_unready" {
                    continue;
                }
                let fake = Exchange::default();
                let db = database();
                let (mut opening, mut accepted) = managed_price_fixture();
                opening.side = direction.opening_side();
                opening.quantity = dec!(1);
                opening.amount_usdc = opening.price;
                accepted.side = opening.side.as_str().into();
                accepted.orig_qty = opening.quantity;
                db.save_managed_order(&opening).unwrap();
                fake.orders
                    .lock()
                    .unwrap()
                    .insert(opening.client_order_id.clone(), accepted.clone());
                let mut exit = opening.clone();
                exit.client_order_id = "gb_closed_gate_exit".into();
                exit.order_id = Some(124);
                exit.side = direction.closing_side();
                exit.price = if direction == GridDirection::Long {
                    dec!(120)
                } else {
                    dec!(80)
                };
                exit.amount_usdc = exit.price * exit.quantity;
                accepted.client_order_id = exit.client_order_id.clone();
                accepted.order_id = 124;
                accepted.side = exit.side.as_str().into();
                accepted.price = exit.price;
                db.save_managed_order(&exit).unwrap();
                fake.orders
                    .lock()
                    .unwrap()
                    .insert(exit.client_order_id.clone(), accepted);
                let (url, server) = exchange_server(fake.clone()).await;
                let mut engine = engine(db.clone(), &url).await;
                let second = self::engine(database(), &url).await;
                let global = test_pair_global_limit(&engine, &second).await;
                {
                    let mut config = engine.state.config.write().await;
                    config.grid.direction = direction;
                    config.grid.max_position_usdc = Some(dec!(100));
                    config.grid.cancel_opening_on_max_position = true;
                }
                engine.state.position.write().await.size = direction.sign();
                let _pending =
                    (gate == "pending_fill").then(|| second.state.publish_fill_reconciliation());
                let _configuration = if gate == "configuration" {
                    Some(
                        engine
                            .state
                            .order_submission_lock
                            .clone()
                            .write_owned()
                            .await,
                    )
                } else {
                    None
                };
                if gate == "blocked" {
                    second
                        .state
                        .global_position_initialized
                        .store(false, Ordering::SeqCst);
                    second
                        .state
                        .global_position_ready
                        .store(false, Ordering::SeqCst);
                    global.check().await;
                }
                assert!(engine.state.try_order_submission().is_none());

                match entry {
                    "maintenance" => engine.maintain_grid_window().await,
                    "ticker" => {
                        let mut update = engine.state.ticker.read().await.clone();
                        update.mark_update_time = Utc::now();
                        engine.handle_ticker_update(update).await;
                    }
                    "placement" => {
                        assert!(
                            !engine
                                .place_grid_order(
                                    direction.opening_side(),
                                    dec!(99),
                                    "gb_blocked_new".into(),
                                    -1,
                                    None
                                )
                                .await
                        );
                    }
                    "sync_unready" => {
                        fake.account_unavailable.store(true, Ordering::SeqCst);
                        engine.sync_cycle(true).await;
                        assert!(engine.last_account_sync.is_none());
                    }
                    _ => unreachable!(),
                }

                assert!(
                    fake.posts.lock().unwrap().is_empty(),
                    "direction={direction:?}, gate={gate}, entry={entry}"
                );
                assert_eq!(
                    fake.orders.lock().unwrap()[&opening.client_order_id].status,
                    "CANCELED"
                );
                assert_eq!(
                    fake.orders.lock().unwrap()[&exit.client_order_id].status,
                    "NEW"
                );
                let active = engine.state.active_orders.read().await;
                assert_eq!(
                    active.len(),
                    1,
                    "direction={direction:?}, gate={gate}, entry={entry}"
                );
                assert_eq!(active[&exit.client_order_id].price, exit.price);
                assert_eq!(active[&exit.client_order_id].quantity, exit.quantity);
                drop(active);
                let saved = db.load_managed_orders("SOLUSDC").unwrap();
                assert_eq!(saved.len(), 1);
                assert_eq!(saved[0].client_order_id, exit.client_order_id);
                assert!(db.get_recent_trades(10).unwrap().is_empty());
                server.abort();
            }
        }
    }
}

#[tokio::test]
async fn pending_or_failing_remainder_recovery_cannot_block_opening_cancellation() {
    for failing in [false, true] {
        let fake = Exchange::default();
        let db = database();
        let (mut opening, mut accepted) = managed_price_fixture();
        opening.price = dec!(100);
        opening.quantity = dec!(1);
        opening.amount_usdc = opening.price;
        accepted.price = opening.price;
        accepted.orig_qty = opening.quantity;
        db.save_managed_order(&opening).unwrap();
        fake.orders
            .lock()
            .unwrap()
            .insert(opening.client_order_id.clone(), accepted.clone());
        let mut exit = opening.clone();
        exit.client_order_id = "gb_recovery_exit".into();
        exit.order_id = Some(124);
        exit.side = OrderSide::Sell;
        exit.price = dec!(101);
        exit.amount_usdc = exit.price;
        accepted.client_order_id = exit.client_order_id.clone();
        accepted.order_id = 124;
        accepted.side = "SELL".into();
        accepted.price = exit.price;
        db.save_managed_order(&exit).unwrap();
        fake.orders
            .lock()
            .unwrap()
            .insert(exit.client_order_id.clone(), accepted);
        let mut target = exit.clone();
        target.client_order_id = "gb_pending_remainder_guard".into();
        target.order_id = None;
        target.purpose = OrderPurpose::Remainder;
        let mut missing_source = exit.clone();
        missing_source.client_order_id = "gb_missing_remainder_source".into();
        missing_source.order_id = Some(125);
        let phase = if failing {
            RemainderPhase::Canceling
        } else {
            RemainderPhase::Submitting
        };
        db.save_remainder_plan(&RemainderPlan {
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Live,
            sources: if failing {
                vec![missing_source]
            } else {
                vec![]
            },
            target,
            phase,
        })
        .unwrap();
        let (url, server) = exchange_server(fake.clone()).await;
        let mut engine = engine(db.clone(), &url).await;
        engine
            .state
            .config
            .write()
            .await
            .grid
            .min_liquidation_distance_pct = Some(dec!(10));
        engine.state.position.write().await.liquidation_price = dec!(95);
        engine.last_account_sync = None;

        engine.maintain_grid_window().await;

        assert_eq!(*engine.state.status.read().await, BotStatus::Running);
        assert!(fake.posts.lock().unwrap().is_empty());
        assert_eq!(
            fake.orders.lock().unwrap()[&opening.client_order_id].status,
            "CANCELED"
        );
        assert_eq!(
            fake.orders.lock().unwrap()[&exit.client_order_id].status,
            "NEW"
        );
        let active = engine.state.active_orders.read().await;
        assert_eq!(active.len(), 1);
        assert!(active.contains_key(&exit.client_order_id));
        drop(active);
        assert_eq!(
            db.load_remainder_plan("SOLUSDC", TradingMode::Live)
                .unwrap()
                .unwrap()
                .phase,
            phase
        );
        assert_eq!(
            db.load_managed_orders("SOLUSDC").unwrap()[0].client_order_id,
            exit.client_order_id
        );
        server.abort();
    }
}

#[tokio::test]
async fn protective_opening_cancellation_retries_unconfirmed_orders_and_preserves_exits() {
    let fake = Exchange::default();
    fake.cancel_fails.store(true, Ordering::SeqCst);
    let db = database();
    let (opening, mut accepted) = managed_price_fixture();
    db.save_managed_order(&opening).unwrap();
    fake.orders
        .lock()
        .unwrap()
        .insert(opening.client_order_id.clone(), accepted.clone());
    let mut exit = opening.clone();
    exit.client_order_id = "gb_stale_guard_exit".into();
    exit.order_id = Some(124);
    exit.side = OrderSide::Sell;
    exit.price = dec!(120);
    exit.amount_usdc = exit.price * exit.quantity;
    accepted.client_order_id = exit.client_order_id.clone();
    accepted.order_id = 124;
    accepted.side = "SELL".into();
    accepted.price = exit.price;
    db.save_managed_order(&exit).unwrap();
    fake.orders
        .lock()
        .unwrap()
        .insert(exit.client_order_id.clone(), accepted);
    let (url, server) = exchange_server(fake.clone()).await;
    let mut engine = engine(db.clone(), &url).await;
    engine
        .state
        .config
        .write()
        .await
        .grid
        .min_liquidation_distance_pct = Some(dec!(10));
    engine.state.position.write().await.liquidation_price = dec!(95);
    engine.state.ticker.write().await.update_time = Utc::now() - chrono::Duration::seconds(16);
    let _configuration = engine
        .state
        .order_submission_lock
        .clone()
        .write_owned()
        .await;

    engine.maintain_grid_window().await;

    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    assert!(engine.last_orders_sync.is_none());
    assert_eq!(engine.state.active_orders.read().await.len(), 2);
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 2);
    assert!(fake.posts.lock().unwrap().is_empty());
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    fake.cancel_fails.store(false, Ordering::SeqCst);
    fake.fill_on_cancel.store(true, Ordering::SeqCst);

    engine.maintain_grid_window().await;

    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    let active = engine.state.active_orders.read().await;
    assert_eq!(active.len(), 1);
    assert_eq!(active[&exit.client_order_id].price, exit.price);
    assert_eq!(active[&exit.client_order_id].quantity, exit.quantity);
    drop(active);
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].client_order_id,
        exit.client_order_id
    );
    assert!(fake.posts.lock().unwrap().is_empty());
    let trades = db.get_recent_trades(10).unwrap();
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].client_order_id, opening.client_order_id);
    assert_eq!(trades[0].quantity, opening.quantity);
    assert_eq!(
        fake.orders.lock().unwrap()[&opening.client_order_id].status,
        "FILLED"
    );
    server.abort();
}

#[tokio::test]
async fn partial_submission_persistence_failure_blocks_global_risk() {
    let path = std::env::temp_dir().join(format!("partial-submit-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_partial BEFORE INSERT ON managed_orders \
        WHEN json_extract(NEW.order_json, '$.status') = 'PARTIALLYFILLED' \
        BEGIN SELECT RAISE(ABORT, 'partial write failed'); END;",
        )
        .unwrap();
    let fake = Exchange::default();
    fake.fill_on_submit.store(1, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let mut engine = engine(db.clone(), &url).await;
    engine.state.position.write().await.update_time = Some(Utc::now());
    engine
        .state
        .config
        .write()
        .await
        .global_max_position_notional = Some(dec!(10000));
    let global =
        crate::server::state::GlobalPositionLimit::new(Arc::new(tokio::sync::RwLock::new(vec![
            engine.state.clone(),
        ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    engine
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    engine
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    let _ = engine
        .state
        .global_position_limit
        .set(Arc::downgrade(&global));
    assert!(!submit(&mut engine).await);
    assert!(!engine.state.global_position_ready.load(Ordering::SeqCst));
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    let response = fake.orders.lock().unwrap()["gb_b_reliability"].clone();
    assert_eq!(
        engine.state.active_orders.read().await["gb_b_reliability"].quantity,
        response.orig_qty
    );
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].quantity,
        response.orig_qty
    );
    server.abort();
    drop(connection);
    drop(engine);
    drop(db);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn zero_fill_terminal_submission_does_not_leave_a_new_order() {
    for response in [3, 4] {
        let fake = Exchange::default();
        fake.fill_on_submit.store(response, Ordering::SeqCst);
        let (url, server) = exchange_server(fake).await;
        let db = database();
        let mut engine = engine(db.clone(), &url).await;
        assert!(!submit(&mut engine).await);
        assert!(engine.state.active_orders.read().await.is_empty());
        assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
        assert!(db.get_recent_trades(10).unwrap().is_empty());
        server.abort();
    }
}

#[tokio::test]
async fn filled_with_zero_quantity_remains_blocked_until_terminal_details_are_consistent() {
    for (confirmed_fill, submission) in [(false, 0), (true, 0), (false, 1), (true, 1)] {
        let fake = Exchange::default();
        fake.fill_on_submit.store(5, Ordering::SeqCst);
        fake.submission.store(submission, Ordering::SeqCst);
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let mut engine = engine(db.clone(), &url).await;
        assert!(!submit(&mut engine).await);
        let order = engine.state.active_orders.read().await["gb_b_reliability"].clone();
        assert!(!engine.cancel_single_order(&order).await);
        assert!(!engine.sync_live_orders().await);
        assert!(engine.reconciliation_blocked);
        assert!(engine.last_account_sync.is_none());
        assert!(engine
            .state
            .active_orders
            .read()
            .await
            .contains_key("gb_b_reliability"));
        assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
        assert!(db.get_recent_trades(10).unwrap().is_empty());
        {
            let mut orders = fake.orders.lock().unwrap();
            let found = orders.get_mut("gb_b_reliability").unwrap();
            if confirmed_fill {
                found.executed_qty = found.orig_qty;
                found.avg_price = Some(found.price);
            } else {
                found.status = "CANCELED".into();
            }
        }
        assert!(engine.sync_live_orders().await);
        assert!(!engine
            .state
            .active_orders
            .read()
            .await
            .contains_key("gb_b_reliability"));
        assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
        assert_eq!(
            db.get_recent_trades(10).unwrap().len(),
            usize::from(confirmed_fill)
        );
        server.abort();
    }
}

#[tokio::test]
async fn inconsistent_submission_and_timeout_lookup_block_before_active_order_lock() {
    for submission in [0, 1] {
        let fake = Exchange::default();
        fake.fill_on_submit.store(5, Ordering::SeqCst);
        fake.submission.store(submission, Ordering::SeqCst);
        fake.block_submit.store(true, Ordering::SeqCst);
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let mut engine = engine(db, &url).await;
        engine
            .state
            .global_position_ready
            .store(true, Ordering::SeqCst);
        let state = engine.state.clone();
        let task = tokio::spawn(async move { submit(&mut engine).await });
        tokio::time::timeout(Duration::from_secs(2), fake.submit_started.notified())
            .await
            .unwrap();
        let guard = state.active_orders.write().await;
        fake.release_submit.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.global_position_ready.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(guard);
        assert!(!task.await.unwrap());
        assert!(state
            .active_orders
            .read()
            .await
            .contains_key("gb_b_reliability"));
        server.abort();
    }
}
