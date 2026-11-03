use super::*;
use crate::config::TradingPairConfig;
use crate::server::create_managed_router;
use crate::types::{BotControlAction, BotStatus, TradingMode};
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use serde_json::{json, Value};
use tower::ServiceExt;

fn fixture() -> TradingRuntime {
    fixture_with_shutdown_failure(false)
}

fn fixture_with_shutdown_failure(fail_shutdown: bool) -> TradingRuntime {
    fixture_with_failures(fail_shutdown, false)
}

fn fixture_with_failures(fail_shutdown: bool, fail_global: bool) -> TradingRuntime {
    let db = Arc::new(Database::open(":memory:").unwrap());
    fixture_with_database(db, fail_shutdown, fail_global)
}

fn fixture_with_database(
    db: Arc<Database>,
    fail_shutdown: bool,
    fail_global: bool,
) -> TradingRuntime {
    fixture_with_shutdown_observer(db, fail_shutdown, fail_global, None)
}

fn fixture_with_shutdown_observer(
    db: Arc<Database>,
    fail_shutdown: bool,
    fail_global: bool,
    observer: Option<mpsc::UnboundedSender<String>>,
) -> TradingRuntime {
    fixture_with_unresponsive_engine(db, fail_shutdown, fail_global, observer, false)
}

fn fixture_with_unresponsive_engine(
    db: Arc<Database>,
    fail_shutdown: bool,
    fail_global: bool,
    observer: Option<mpsc::UnboundedSender<String>>,
    unresponsive_btc: bool,
) -> TradingRuntime {
    db.create_session("configuration-test-token").unwrap();
    let config = AppConfig {
        pairs: ["SOLUSDC", "BTCUSDT"]
            .map(|symbol| TradingPairConfig::new(symbol.into()))
            .to_vec(),
        ..Default::default()
    };
    db.save_config(&config).unwrap();
    for strategy in config.strategy_configs().unwrap() {
        let mode = TradingMode::from_dry_run(strategy.exchange.dry_run);
        db.arm_execution_guard(&strategy.exchange.symbol, mode)
            .unwrap();
    }
    let lock = Arc::new(Mutex::new(()));
    let pairs = config
        .strategy_configs()
        .unwrap()
        .into_iter()
        .map(|config| {
            let (tx, mut rx) = mpsc::channel(8);
            let state = AppState::new_for_pair_with_admin_auth(
                config,
                db.clone(),
                tx,
                lock.clone(),
                Some(Arc::new(
                    crate::auth::AdminAuth::from_password("test-password").unwrap(),
                )),
            );
            let engine_state = state.clone();
            let observer = observer.clone();
            let engine = tokio::spawn(async move {
                while let Some(action) = rx.recv().await {
                    match action {
                        BotControlAction::UpdateConfig { config, reply } => {
                            let result = engine_state
                                .save_runtime_config(*config)
                                .await
                                .map_err(|error| error.to_string());
                            let _ = reply.send(result);
                        }
                        BotControlAction::UpdateGlobalConfig { config, reply } => {
                            assert!(
                                engine_state.try_order_submission().is_none(),
                                "submissions must remain blocked through propagation and rollback"
                            );
                            let reject = fail_global
                                && config.recv_window == 8888
                                && engine_state.pair_scope() == Some("BTCUSDT");
                            let mut updated = engine_state.config.read().await.clone();
                            config.apply_to(&mut updated);
                            let result = engine_state
                                .save_runtime_config(updated)
                                .await
                                .map_err(|error| error.to_string());
                            assert!(engine_state.try_order_submission().is_none());
                            let _ = reply.send(if reject {
                                Err("模拟保存后对账失败".into())
                            } else {
                                result
                            });
                        }
                        BotControlAction::Shutdown { reply, .. } => {
                            if let Some(observer) = &observer {
                                let _ =
                                    observer.send(engine_state.pair_scope().unwrap().to_string());
                            }
                            if unresponsive_btc && engine_state.pair_scope() == Some("BTCUSDT") {
                                std::future::pending::<()>().await;
                            }
                            let result = if fail_shutdown {
                                Err("撤单结果仍未确认".into())
                            } else {
                                Ok(())
                            };
                            let _ = reply.send(result);
                            break;
                        }
                        BotControlAction::Pause | BotControlAction::CancelAll => {
                            engine_state.pause_trading().await.unwrap();
                        }
                        BotControlAction::PauseAlreadyPersisted
                        | BotControlAction::CancelAllAlreadyPersisted => {}
                        BotControlAction::Resume {
                            expected_mode,
                            expected_epoch,
                            reply,
                        } => {
                            let result = engine_state
                                .resume_trading_in_mode(expected_mode, expected_epoch)
                                .await
                                .map_err(|error| error.to_string());
                            let _ = reply.send(result);
                        }
                        BotControlAction::Rebalance { reply, .. } => {
                            let _ = reply.send(Ok(()));
                        }
                        BotControlAction::GlobalRiskCancel => {}
                    }
                }
            });
            let (shutdown_tx, _) = watch::channel(false);
            PairTask {
                state,
                engine,
                engine_joined: false,
                recovery_failed: false,
                shutdown_tx,
                background: vec![],
            }
        })
        .collect();
    TradingRuntime::from_pairs(
        pairs,
        db,
        lock,
        config.server.clone(),
        GlobalConfig::from_app(&config),
        None,
    )
}

async fn seed_paper_account_and_position(
    state: &AppState,
) -> (crate::types::AccountInfo, crate::types::PositionInfo) {
    assert!(state.config.read().await.exchange.dry_run);
    let account = {
        let mut account = state.account.write().await;
        account.asset = "USDC".into();
        account.total_wallet_balance = rust_decimal_macros::dec!(9123.45);
        account.available_balance = rust_decimal_macros::dec!(8765.43);
        account.margin_balance = rust_decimal_macros::dec!(9135.45);
        account.unrealized_profit = rust_decimal_macros::dec!(12);
        account.clone()
    };
    let position = {
        let mut position = state.position.write().await;
        position.symbol = "SOLUSDC".into();
        position.size = rust_decimal_macros::dec!(2.5);
        position.entry_price = rust_decimal_macros::dec!(100);
        position.mark_price = rust_decimal_macros::dec!(104.8);
        position.unrealized_pnl = rust_decimal_macros::dec!(12);
        position.liquidation_price = rust_decimal_macros::dec!(70);
        position.leverage = 10;
        position.clone()
    };
    (account, position)
}

async fn assert_paper_account_and_position_preserved(
    state: &AppState,
    account: &crate::types::AccountInfo,
    position: &crate::types::PositionInfo,
) {
    assert_eq!(
        serde_json::to_value(&*state.account.read().await).unwrap(),
        serde_json::to_value(account).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&*state.position.read().await).unwrap(),
        serde_json::to_value(position).unwrap()
    );
}
#[tokio::test]
async fn paper_removal_rejects_open_positions_and_rebuilds_member() {
    for size in [
        rust_decimal_macros::dec!(2.5),
        rust_decimal_macros::dec!(-2.5),
    ] {
        let mut runtime = fixture();
        let state = runtime.states()[0].clone();
        let other = runtime.states()[1].clone();
        let (account, mut position) = seed_paper_account_and_position(&state).await;
        position.size = size;
        state.position.write().await.size = size;
        state.pause_trading().await.unwrap();
        let saved =
            serde_json::to_value(runtime.handle.db.load_config().unwrap().unwrap()).unwrap();

        let error = runtime.remove_pair("SOLUSDC").await.unwrap_err();
        assert!(error.to_string().contains("持仓"));
        assert_eq!(
            serde_json::to_value(runtime.handle.db.load_config().unwrap().unwrap()).unwrap(),
            saved
        );
        let members = runtime.handle.states.read().await.clone();
        assert_eq!(members.len(), 2);
        let replacement = runtime.states()[0].clone();
        assert!(!Arc::ptr_eq(&state, &replacement));
        assert!(Arc::ptr_eq(&members[0], &replacement));
        assert!(Arc::ptr_eq(&members[1], &other));
        assert!(state.is_shutting_down());
        assert!(!replacement.is_shutting_down());
        assert_eq!(*replacement.status.read().await, BotStatus::Paused);
        assert!(!runtime.pairs[0].engine.is_finished());
        assert_paper_account_and_position_preserved(&replacement, &account, &position).await;
    }
}

fn position_risk(symbol: &str, amount: &str) -> Value {
    json!({
        "symbol": symbol, "positionSide": "BOTH", "positionAmt": amount,
        "entryPrice": "100", "markPrice": "99", "unRealizedProfit": "0",
        "liquidationPrice": "0", "leverage": "20"
    })
}

#[tokio::test]
async fn live_removal_requires_confirmed_zero_exchange_position_after_shutdown() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    for (time_status, status, body, allowed) in [
        (
            StatusCode::OK,
            StatusCode::OK,
            json!([position_risk("SOLUSDC", "1")]),
            false,
        ),
        (
            StatusCode::OK,
            StatusCode::OK,
            json!([position_risk("SOLUSDC", "-1")]),
            false,
        ),
        (
            StatusCode::OK,
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"msg": "exchange error"}),
            false,
        ),
        (StatusCode::OK, StatusCode::OK, json!([]), false),
        (
            StatusCode::OK,
            StatusCode::OK,
            json!([position_risk("BTCUSDT", "0")]),
            false,
        ),
        (
            StatusCode::OK,
            StatusCode::OK,
            json!([{"symbol": "SOLUSDC"}]),
            false,
        ),
        (
            StatusCode::OK,
            StatusCode::OK,
            json!([position_risk("SOLUSDC", "0")]),
            true,
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::OK,
            json!([position_risk("SOLUSDC", "0")]),
            false,
        ),
    ] {
        let confirmed_open = time_status == StatusCode::OK
            && status == StatusCode::OK
            && body.as_array().is_some_and(|positions| {
                positions.first().is_some_and(|position| {
                    position["symbol"] == "SOLUSDC"
                        && (position["positionAmt"] == "1" || position["positionAmt"] == "-1")
                })
            });
        let stopped = Arc::new(AtomicBool::new(false));
        let queried_early = Arc::new(AtomicBool::new(false));
        let queries = Arc::new(AtomicUsize::new(0));
        let time_queries = Arc::new(AtomicUsize::new(0));
        let sync_completed = Arc::new(AtomicBool::new(false));
        let app = Router::new().route(
            "/fapi/v1/time",
            axum::routing::get({
                let time_queries = time_queries.clone();
                let sync_completed = sync_completed.clone();
                move || {
                    let time_queries = time_queries.clone();
                    let sync_completed = sync_completed.clone();
                    async move {
                        if time_queries.fetch_add(1, Ordering::SeqCst) > 0 {
                            std::future::pending::<()>().await;
                        }
                        sync_completed.store(true, Ordering::SeqCst);
                        (time_status, axum::Json(json!({"serverTime": chrono::Utc::now().timestamp_millis() + 60_000})))
                    }
                }
            }),
        ).route(
            "/fapi/v2/positionRisk",
            axum::routing::get({
                let stopped = stopped.clone();
                let queried_early = queried_early.clone();
                let queries = queries.clone();
                let sync_completed = sync_completed.clone();
                move |axum::extract::RawQuery(query): axum::extract::RawQuery| {
                    let body = body.clone();
                    let stopped = stopped.clone();
                    let queried_early = queried_early.clone();
                    let queries = queries.clone();
                    let sync_completed = sync_completed.clone();
                    async move {
                        assert!(sync_completed.load(Ordering::SeqCst));
                        let timestamp: i64 = query.unwrap().split('&')
                            .find_map(|part| part.strip_prefix("timestamp="))
                            .unwrap().parse().unwrap();
                        assert!(timestamp > chrono::Utc::now().timestamp_millis() + 30_000);
                        if !stopped.load(Ordering::SeqCst) {
                            queried_early.store(true, Ordering::SeqCst);
                        }
                        queries.fetch_add(1, Ordering::SeqCst);
                        (status, axum::Json(body))
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let mut runtime = fixture();
        runtime.test_exchange_url = Some(url);
        let old = runtime.pairs.remove(0);
        old.engine.abort();
        let mut config = old.state.config.read().await.clone();
        config.exchange.dry_run = false;
        let (tx, mut rx) = mpsc::channel(8);
        let state = AppState::new_for_pair(
            config,
            runtime.handle.db.clone(),
            tx,
            runtime.trade_lock.clone(),
        );
        // The cached value deliberately disagrees with the exchange response.
        state.position.write().await.size = if allowed {
            rust_decimal_macros::dec!(3)
        } else {
            rust_decimal::Decimal::ZERO
        };
        state.position.write().await.mark_price = rust_decimal_macros::dec!(99);
        state.position.write().await.update_time = Some(chrono::Utc::now());
        {
            let mut account = state.account.write().await;
            account.asset = "USDC".into();
            account.available_balance = rust_decimal::Decimal::ZERO;
            account.update_time = chrono::Utc::now();
        }
        state.pause_trading().await.unwrap();
        let stopped_on_exit = stopped.clone();
        let engine = tokio::spawn(async move {
            if let Some(BotControlAction::Shutdown { reply, .. }) = rx.recv().await {
                let _ = reply.send(Ok(()));
                stopped_on_exit.store(true, Ordering::SeqCst);
            }
        });
        let (shutdown_tx, _) = watch::channel(false);
        runtime.pairs.insert(
            0,
            PairTask {
                state: state.clone(),
                engine,
                engine_joined: false,
                recovery_failed: false,
                shutdown_tx,
                background: vec![],
            },
        );
        runtime.handle.register_engine(&runtime.pairs[0]);
        *runtime.handle.states.write().await = runtime.states();
        let other = runtime.states()[1].clone();
        {
            let mut account = other.account.write().await;
            account.asset = "USDT".into();
            account.available_balance = rust_decimal_macros::dec!(100);
            account.update_time = chrono::Utc::now();
        }
        let mut global = GlobalConfig::from_app(&*state.config.read().await);
        global.global_max_position_notional = Some(rust_decimal_macros::dec!(1000));
        global.pause_on_zero_available_margin = false;
        runtime.global_position_limit.initialize_config(global);
        runtime
            .global_position_limit
            .membership_ready
            .store(true, Ordering::SeqCst);
        for member in [&state, &other] {
            member
                .global_position_initialized
                .store(true, Ordering::SeqCst);
            member.global_position_ready.store(true, Ordering::SeqCst);
            let _ = member
                .global_position_limit
                .set(Arc::downgrade(&runtime.global_position_limit));
        }
        other.resume_trading().await.unwrap();
        let saved =
            serde_json::to_value(runtime.handle.db.load_config().unwrap().unwrap()).unwrap();

        let result = runtime.remove_pair("SOLUSDC").await;
        assert_eq!(
            result.is_ok(),
            allowed,
            "unexpected removal result: {result:?}"
        );
        assert!(time_queries.load(Ordering::SeqCst) > 0);
        assert_eq!(
            queries.load(Ordering::SeqCst) > 0,
            time_status == StatusCode::OK
        );
        if time_status != StatusCode::OK {
            assert!(format!("{:#}", result.as_ref().unwrap_err()).contains("同步交易所时间失败"));
        }
        if confirmed_open {
            assert!(state.global_position_ready.load(Ordering::SeqCst));
            assert_eq!(*other.status.read().await, BotStatus::Running);
            assert_eq!(
                state.position.read().await.size.abs(),
                rust_decimal_macros::dec!(1)
            );
        } else if !allowed {
            assert!(!state.global_position_ready.load(Ordering::SeqCst));
            assert_eq!(*other.status.read().await, BotStatus::Paused);
        }
        assert!(!queried_early.load(Ordering::SeqCst));
        let members = runtime.handle.states.read().await.clone();
        if allowed {
            assert_eq!(members.len(), 1);
            assert!(Arc::ptr_eq(&members[0], &other));
            let saved = runtime.handle.db.load_config().unwrap().unwrap();
            assert_eq!(saved.pairs.len(), 1);
            assert_eq!(saved.pairs[0].symbol, "BTCUSDT");
        } else {
            assert_eq!(
                serde_json::to_value(runtime.handle.db.load_config().unwrap().unwrap()).unwrap(),
                saved
            );
            assert_eq!(members.len(), 2);
            let replacement = runtime.states()[0].clone();
            assert!(!Arc::ptr_eq(&state, &replacement));
            assert!(Arc::ptr_eq(&members[0], &replacement));
            assert!(Arc::ptr_eq(&members[1], &other));
            assert!(state.is_shutting_down());
            assert!(!replacement.is_shutting_down());
            assert_eq!(*replacement.status.read().await, BotStatus::Paused);
            assert!(!runtime.pairs[0].engine.is_finished());
            if confirmed_open {
                // The replacement's time request is held by the mock exchange, so
                // its initialization cannot refresh the inherited snapshots yet.
                assert!(!replacement
                    .global_position_initialized
                    .load(Ordering::SeqCst));
                assert_eq!(*other.status.read().await, BotStatus::Running);
                let mut risk = GlobalConfig::from_app(&*state.config.read().await);
                risk.global_max_position_notional = Some(rust_decimal_macros::dec!(50));
                risk.pause_on_zero_available_margin = false;
                runtime
                    .global_position_limit
                    .initialize_config(risk.clone());
                assert!(runtime.global_position_limit.check().await);
                assert_eq!(*other.status.read().await, BotStatus::Paused);
                assert!(other.recent_logs.read().await.iter().any(|entry| {
                    entry.message.contains("全局持仓名义价值") && entry.message.contains("99")
                }));

                // Clear the position limit and resume the other member, then
                // exercise the inherited account snapshot through margin risk.
                risk.global_max_position_notional = None;
                runtime
                    .global_position_limit
                    .initialize_config(risk.clone());
                assert!(!runtime.global_position_limit.check().await);
                // The fixture engine ignores risk cancellations, so acknowledge
                // the cancellation before manually resuming the strategy.
                other.global_cancel_pending.store(false, Ordering::SeqCst);
                other.resume_trading().await.unwrap();
                risk.pause_on_zero_available_margin = true;
                runtime.global_position_limit.initialize_config(risk);
                assert!(runtime.global_position_limit.check().await);
                assert_eq!(*other.status.read().await, BotStatus::Paused);
                assert!(other
                    .recent_logs
                    .read()
                    .await
                    .iter()
                    .any(|entry| { entry.message.contains("可用保证金已达到零") }));
                assert!(!replacement
                    .global_position_initialized
                    .load(Ordering::SeqCst));
            }
        }
        server.abort();
    }
}

#[tokio::test]
async fn failed_removal_keeps_configuration_and_restarts_paused_engine() {
    let mut runtime = fixture_with_shutdown_failure(true);
    let state = runtime.states()[0].clone();
    let (account, position) = seed_paper_account_and_position(&state).await;
    let mut retirement = state.subscribe_retirement();
    assert!(!*retirement.borrow());
    let other_state = runtime.states()[1].clone();
    let other_retirement = other_state.subscribe_retirement();
    state.pause_trading().await.unwrap();
    assert!(runtime
        .remove_pair("SOLUSDC")
        .await
        .unwrap_err()
        .to_string()
        .contains("撤单结果仍未确认"));
    assert_eq!(
        runtime
            .handle
            .db
            .load_config()
            .unwrap()
            .unwrap()
            .pairs
            .len(),
        2
    );
    assert_eq!(runtime.handle.states.read().await.len(), 2);
    assert!(state.is_shutting_down());
    let replacement = runtime.states()[0].clone();
    assert!(!Arc::ptr_eq(&state, &replacement));
    tokio::time::timeout(Duration::from_secs(2), retirement.changed())
        .await
        .expect("旧 state 应通知订阅者退休")
        .unwrap();
    assert!(*retirement.borrow());
    assert!(!*replacement.subscribe_retirement().borrow());
    assert!(!*other_retirement.borrow());
    assert_paper_account_and_position_preserved(&replacement, &account, &position).await;
    assert!(!replacement.is_shutting_down());
    assert_eq!(*replacement.status.read().await, BotStatus::Paused);
    assert!(!runtime.pairs[0].engine.is_finished());
}

#[tokio::test]
async fn failed_removal_persistence_restarts_paused_engine() {
    let path =
        std::env::temp_dir().join(format!("remove-commit-failure-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    let mut runtime = fixture_with_database(db.clone(), false, false);
    let state = runtime.states()[0].clone();
    let (account, mut position) = seed_paper_account_and_position(&state).await;
    position.size = rust_decimal::Decimal::ZERO;
    state.position.write().await.size = position.size;
    let mut retirement = state.subscribe_retirement();
    assert!(!*retirement.borrow());
    state.pause_trading().await.unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_remove BEFORE UPDATE ON app_config BEGIN SELECT RAISE(ABORT, 'simulated removal failure'); END;").unwrap();
    drop(connection);
    assert!(runtime.remove_pair("SOLUSDC").await.is_err());
    assert_eq!(db.load_config().unwrap().unwrap().pairs.len(), 2);
    let replacement = runtime.states()[0].clone();
    assert!(!Arc::ptr_eq(&state, &replacement));
    tokio::time::timeout(Duration::from_secs(2), retirement.changed())
        .await
        .expect("旧 state 应通知订阅者退休")
        .unwrap();
    assert!(*retirement.borrow());
    assert!(!*replacement.subscribe_retirement().borrow());
    assert_paper_account_and_position_preserved(&replacement, &account, &position).await;
    assert!(!replacement.is_shutting_down());
    assert_eq!(*replacement.status.read().await, BotStatus::Paused);
    assert!(!runtime.pairs[0].engine.is_finished());
    drop(runtime);
    drop(replacement);
    drop(state);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn failed_removal_rebuilds_with_configuration_committed_during_shutdown() {
    let mut runtime = fixture();
    let old = runtime.pairs.remove(0);
    old.engine.abort();
    let config = old.state.config.read().await.clone();
    let (tx, mut rx) = mpsc::channel(8);
    let state = AppState::new_for_pair(
        config,
        runtime.handle.db.clone(),
        tx,
        runtime.trade_lock.clone(),
    );
    state.pause_trading().await.unwrap();
    let engine = tokio::spawn(async move {
        if let Some(BotControlAction::Shutdown { reply, .. }) = rx.recv().await {
            let _ = reply.send(Err("模拟停机失败".into()));
        }
    });
    let (shutdown_tx, _) = watch::channel(false);
    runtime.pairs.insert(
        0,
        PairTask {
            state: state.clone(),
            engine,
            engine_joined: false,
            recovery_failed: false,
            shutdown_tx,
            background: vec![],
        },
    );
    runtime.handle.register_engine(&runtime.pairs[0]);
    *runtime.handle.states.write().await = runtime.states();

    let mut updated = state.config.read().await.clone();
    updated.grid.grid_interval = rust_decimal_macros::dec!(12.5);
    // The writer has already acquired the status lock and committed to SQLite,
    // but cannot publish its configuration until this read guard is released.
    let config_guard = state.config.read().await;
    let writer_state = state.clone();
    let writer = tokio::spawn(async move { writer_state.save_runtime_config(updated).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let saved = runtime.handle.db.load_config().unwrap().unwrap();
            if saved.strategy_configs().unwrap()[0].grid.grid_interval
                == rust_decimal_macros::dec!(12.5)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("配置写入应在退休前提交");
    let mut retirement = state.subscribe_retirement();
    let removal = async move {
        let result = runtime.remove_pair("SOLUSDC").await;
        (runtime, result)
    };
    let removal = tokio::spawn(removal);
    tokio::time::timeout(Duration::from_secs(2), retirement.changed())
        .await
        .expect("删除应在配置发布前退休旧实例")
        .unwrap();
    drop(config_guard);
    writer.await.unwrap().unwrap();
    let (runtime, result) = removal.await.unwrap();
    assert!(result.is_err());
    let replacement = runtime.states()[0].clone();
    assert!(!Arc::ptr_eq(&state, &replacement));
    assert_eq!(
        replacement.config.read().await.grid.grid_interval,
        rust_decimal_macros::dec!(12.5)
    );
    assert_eq!(
        runtime
            .handle
            .db
            .load_config()
            .unwrap()
            .unwrap()
            .strategy_configs()
            .unwrap()[0]
            .grid
            .grid_interval,
        rust_decimal_macros::dec!(12.5)
    );
    assert_eq!(*replacement.status.read().await, BotStatus::Paused);
}

#[tokio::test]
async fn failed_removal_with_unpersisted_pause_rebuilds_paused() {
    let path =
        std::env::temp_dir().join(format!("remove-pause-failure-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    let mut runtime = fixture_with_database(db.clone(), false, false);
    let state = runtime.states()[0].clone();
    state.pause_trading().await.unwrap();
    db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Running)
        .unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_pause_insert BEFORE INSERT ON pair_runtime WHEN NEW.symbol = 'SOLUSDC' BEGIN SELECT RAISE(ABORT, 'simulated pause failure'); END; CREATE TRIGGER reject_pause_update BEFORE UPDATE ON pair_runtime WHEN NEW.symbol = 'SOLUSDC' BEGIN SELECT RAISE(ABORT, 'simulated pause failure'); END;").unwrap();
    drop(connection);

    assert!(runtime.remove_pair("SOLUSDC").await.is_err());
    let replacement = runtime.states()[0].clone();
    assert!(!Arc::ptr_eq(&state, &replacement));
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
        Some(BotStatus::Running)
    );
    assert_eq!(*replacement.status.read().await, BotStatus::Paused);
    assert!(!replacement.is_shutting_down());
    assert!(!runtime.pairs[0].engine.is_finished());
    drop(runtime);
    let config = db.load_config().unwrap().unwrap();
    let reopened = Arc::new(Database::open(&path).unwrap());
    let restarted =
        TradingRuntime::start_with_test_exchange(&config, reopened, "http://127.0.0.1:1".into())
            .await
            .unwrap();
    assert_eq!(
        *restarted.states()[0].status.read().await,
        BotStatus::Paused
    );
    drop(restarted);
    drop(replacement);
    drop(state);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn failed_removal_and_rebuild_keeps_member_in_global_risk() {
    let path = std::env::temp_dir().join(format!(
        "remove-rebuild-failure-{}.db",
        uuid::Uuid::new_v4()
    ));
    let db = Arc::new(Database::open(&path).unwrap());
    let mut runtime = fixture_with_database(db.clone(), true, false);
    let state = runtime.states()[0].clone();
    let other = runtime.states()[1].clone();
    let mut global = GlobalConfig::from_app(&*state.config.read().await);
    global.global_max_position_notional = Some(rust_decimal_macros::dec!(100));
    runtime.global_position_limit.initialize_config(global);
    runtime
        .global_position_limit
        .membership_ready
        .store(true, std::sync::atomic::Ordering::SeqCst);
    for member in [&state, &other] {
        member
            .global_position_initialized
            .store(true, std::sync::atomic::Ordering::SeqCst);
        member
            .global_position_ready
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = member
            .global_position_limit
            .set(Arc::downgrade(&runtime.global_position_limit));
    }
    state.position.write().await.size = rust_decimal_macros::dec!(1);
    state.ticker.write().await.mark_price = rust_decimal_macros::dec!(200);
    state.ticker.write().await.mark_update_time = chrono::Utc::now();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_guard_insert BEFORE INSERT ON pair_execution_guard \
             BEGIN SELECT RAISE(ABORT, 'simulated guard failure'); END; \
             CREATE TRIGGER reject_guard_update BEFORE UPDATE ON pair_execution_guard \
             BEGIN SELECT RAISE(ABORT, 'simulated guard failure'); END;",
        )
        .unwrap();
    drop(connection);
    state.pause_trading().await.unwrap();

    let handle = runtime.handle();
    let global_limit = runtime.global_position_limit.clone();
    let other_engine = runtime.pairs[1].engine.abort_handle();
    let router = create_managed_router(handle.clone()).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    let error = tokio::time::timeout(Duration::from_secs(2), handle.remove_pair("SOLUSDC".into()))
        .await
        .unwrap()
        .unwrap_err();
    assert!(format!("{error:#}").contains("删除失败后无法重启策略"));
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch("DROP TRIGGER reject_guard_insert; DROP TRIGGER reject_guard_update;")
        .unwrap();
    drop(connection);
    assert_eq!(db.load_config().unwrap().unwrap().pairs.len(), 2);
    assert!(state.is_shutting_down());
    assert!(*state.subscribe_retirement().borrow());
    let published = handle.states.read().await.clone();
    assert_eq!(published.len(), 2);
    assert!(Arc::ptr_eq(&published[0], &state));
    assert!(Arc::ptr_eq(&published[1], &other));
    assert!(!state
        .global_position_ready
        .load(std::sync::atomic::Ordering::SeqCst));
    assert!(global_limit.check().await);
    assert!(other.try_order_submission().is_none());
    assert!(other.resume_trading().await.is_err());
    assert!(!other.is_shutting_down());
    assert!(!*other.subscribe_retirement().borrow());
    let updated = tokio::time::timeout(
        Duration::from_secs(2),
        handle.update_global_config(crate::types::UpdateGlobalConfigPayload {
            recv_window: Some(9000),
            global_max_position_notional: Some(Some(rust_decimal_macros::dec!(100))),
            ..Default::default()
        }),
    )
    .await
    .expect("隔离成员不能阻止全局配置更新")
    .unwrap();
    assert_eq!(updated.recv_window, 9000);
    assert_eq!(
        db.load_config().unwrap().unwrap().exchange.recv_window,
        9000
    );
    assert_eq!(other.config.read().await.exchange.recv_window, 9000);
    assert_eq!(state.config.read().await.exchange.recv_window, 5000);
    let members = handle.states.read().await.clone();
    assert_eq!(members.len(), 2);
    assert!(Arc::ptr_eq(&members[0], &state));
    assert!(global_limit.check().await);
    assert!(other.try_order_submission().is_none());
    assert!(other.resume_trading().await.is_err());
    drop(members);
    let (status, body) = request(
        &router,
        "/api/pairs/BTCUSDT/status",
        "GET",
        Value::Null,
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    let error = tokio::time::timeout(Duration::from_secs(2), handle.remove_pair("UNKNOWN".into()))
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("交易对不存在"));
    let error = tokio::time::timeout(Duration::from_secs(2), handle.remove_pair("SOLUSDC".into()))
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("策略重建失败"));
    other.pause_trading().await.unwrap();
    assert_eq!(*other.status.read().await, BotStatus::Paused);
    other_engine.abort();
    let reason = tokio::time::timeout(Duration::from_secs(2), manager)
        .await
        .unwrap()
        .unwrap();
    assert!(reason.contains("BTCUSDT"));
    drop(published);
    drop(router);
    drop(handle);
    drop(other);
    drop(state);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn removal_preserves_at_least_one_pair_and_requires_confirmed_empty_orders() {
    let mut runtime = fixture();
    let state = runtime.states()[0].clone();
    state.pause_trading().await.unwrap();
    let order = crate::types::GridOrder {
        client_order_id: "gb_b_removal".into(),
        symbol: "SOLUSDC".into(),
        order_id: None,
        side: crate::types::OrderSide::Buy,
        price: rust_decimal_macros::dec!(100),
        quantity: rust_decimal_macros::dec!(1),
        amount_usdc: rust_decimal_macros::dec!(100),
        status: crate::types::OrderStatus::New,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        grid_level: 1,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: vec![],
    };
    state
        .active_orders
        .write()
        .await
        .insert(order.client_order_id.clone(), order);
    assert!(runtime
        .remove_pair("SOLUSDC")
        .await
        .unwrap_err()
        .to_string()
        .contains("仍有挂单"));
    assert!(!state.is_shutting_down());
    state.active_orders.write().await.clear();
    runtime.remove_pair("SOLUSDC").await.unwrap();
    assert!(runtime
        .remove_pair("BTCUSDT")
        .await
        .unwrap_err()
        .to_string()
        .contains("至少保留"));
    assert_eq!(
        runtime
            .handle
            .db
            .load_config()
            .unwrap()
            .unwrap()
            .pairs
            .len(),
        1
    );
}

async fn request(
    router: &Router,
    path: &str,
    method: &str,
    body: Value,
    authenticated: bool,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .uri(path)
        .method(method)
        .header("Content-Type", "application/json");
    if authenticated {
        request = request.header("Authorization", "Bearer configuration-test-token");
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1_000_000).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn symbols_route_shares_runtime_limiter() {
    use crate::exchange::rate_limit::RequestClass;

    let runtime = fixture();
    let handle = runtime.handle();
    let mainnet = "https://fapi.binance.com";
    let config = AppConfig::default().exchange;
    let trading_client = runtime.exchange_client(&config);
    assert!(Arc::ptr_eq(&trading_client.limiter(), &handle.rate_limiter));
    handle
        .rate_limiter
        .reserve(mainnet, [7; 32], 1800, RequestClass::Normal)
        .unwrap();
    let router = create_managed_router(handle.clone()).await;
    let (status, _) = request(
        &router,
        "/api/exchange/symbols?environment=mainnet",
        "GET",
        Value::Null,
        true,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
}

fn payload(symbol: &str) -> Value {
    json!({ "symbol": symbol, "grid_interval": "0.1", "order_amount_usdc": "100", "buy_window": 5, "sell_window": 5 })
}

fn exchange_info(status: &str, filters: Value) -> Value {
    json!({"symbols": [{
        "symbol": "ETHUSDT", "status": status, "pricePrecision": 2,
        "quantityPrecision": 3, "filters": filters
    }]})
}

fn valid_filters() -> Value {
    json!([
        {"filterType":"PRICE_FILTER", "minPrice":"0.01", "maxPrice":"1000000", "tickSize":"0.01"},
        {"filterType":"LOT_SIZE", "minQty":"0.001", "maxQty":"1000000", "stepSize":"0.001"},
        {"filterType":"MIN_NOTIONAL", "notional":"5"}
    ])
}

async fn mock_exchange(status: StatusCode, body: Value) -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new().route(
        "/fapi/v1/exchangeInfo",
        axum::routing::get(move || {
            let body = body.clone();
            async move { (status, axum::Json(body)) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, server)
}

async fn versioned_payload(router: &Router, symbol: &str) -> Value {
    let path = format!("/api/pairs/{symbol}/config");
    let (_, current) = request(router, &path, "GET", Value::Null, true).await;
    let mut result = payload(symbol);
    result["baseline_version"] = current["data"]["config_version"].clone();
    result
}

#[tokio::test]
async fn startup_registers_engines_for_early_shutdown_timeout() {
    let (entered_tx, entered_rx) = oneshot::channel();
    let entered = Arc::new(std::sync::Mutex::new(Some(entered_tx)));
    let app = Router::new().fallback(move || {
        let entered = entered.clone();
        async move {
            if let Some(tx) = entered.lock().unwrap().take() {
                let _ = tx.send(());
            }
            std::future::pending::<StatusCode>().await
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let db = Arc::new(Database::open(":memory:").unwrap());
    let config = AppConfig {
        pairs: vec![TradingPairConfig::new("SOLUSDC".into())],
        ..Default::default()
    };
    db.save_config(&config).unwrap();
    let mut runtime = TradingRuntime::start_with_test_exchange(&config, db, url)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(!runtime.pairs[0].engine.is_finished());

    let handle = runtime.handle();
    let state = runtime.pairs[0].state.clone();
    let guard = state.status.write().await;
    handle.request_shutdown();
    handle.start_early_shutdown(Duration::from_millis(50)).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while !runtime.pairs[0].engine.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("early shutdown did not abort the startup engine");
    drop(guard);
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        runtime.shutdown(Duration::from_secs(5)),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("SOLUSDC"), "{error}");
    server.abort();
}

#[tokio::test]
async fn cancelled_exit_monitor_never_repolls_consumed_engine() {
    let mut runtime = fixture();
    let state = runtime.pairs[0].state.clone();
    let config_guard = state.config.write().await;
    runtime.pairs[0].engine.abort();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !runtime.pairs[0].engine.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), runtime.wait_for_exit())
            .await
            .is_err()
    );
    drop(config_guard);

    let reason = tokio::time::timeout(Duration::from_secs(5), runtime.wait_for_exit())
        .await
        .unwrap();
    assert!(reason.contains("SOLUSDC"), "{reason}");
    assert!(runtime.pairs[0].engine_joined);
    let reason = tokio::time::timeout(Duration::from_secs(5), runtime.wait_for_exit())
        .await
        .expect("joined engine exit was not reported");
    assert!(reason.contains("SOLUSDC"), "{reason}");
    let error = runtime.remove_pair("SOLUSDC").await.unwrap_err();
    assert!(error.to_string().contains("策略引擎已退出"), "{error}");
    assert_eq!(runtime.states().len(), 2);
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        runtime.shutdown(Duration::from_secs(5)),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("SOLUSDC"), "{error}");
}

#[tokio::test]
async fn early_shutdown_reports_engine_already_joined_by_run() {
    let mut runtime = fixture();
    runtime.pairs[0].engine.abort();
    let _ = (&mut runtime.pairs[0].engine).await;
    let (exit_tx, exit_rx) = oneshot::channel::<()>();
    runtime.pairs[0].engine = tokio::spawn(async move {
        let _ = exit_rx.await;
    });
    runtime.handle.register_engine(&runtime.pairs[0]);
    let handle = runtime.handle();
    let reason = {
        let run = runtime.run();
        tokio::pin!(run);
        tokio::select! {
            biased;
            reason = &mut run => panic!("runtime exited before engine was released: {reason}"),
            _ = tokio::task::yield_now() => {}
        }
        exit_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), &mut run)
            .await
            .unwrap()
    };
    assert!(reason.contains("SOLUSDC"), "{reason}");
    assert!(runtime.pairs[0].engine_joined);
    handle.request_shutdown();
    handle.start_early_shutdown(Duration::from_secs(5)).await;
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        runtime.shutdown(Duration::from_secs(5)),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("SOLUSDC"), "{error}");
}

#[tokio::test]
async fn early_shutdown_aborts_unresponsive_engine_before_membership_commit_returns() {
    for operation in ["add", "remove"] {
        let directory = std::env::temp_dir().join(format!(
            "runtime-early-timeout-{operation}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&directory).unwrap();
        let db = Arc::new(Database::open(directory.join("bot.db")).unwrap());
        let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
        let mut runtime =
            fixture_with_unresponsive_engine(db.clone(), false, false, Some(observed_tx), true);
        let (url, server) =
            mock_exchange(StatusCode::OK, exchange_info("TRADING", valid_filters())).await;
        runtime.test_exchange_url = Some(url);
        let states = runtime.states();
        if operation == "remove" {
            states[0].pause_trading().await.unwrap();
        }
        let mut added = db
            .load_config()
            .unwrap()
            .unwrap()
            .strategy_configs()
            .unwrap()
            .remove(0);
        added.exchange.symbol = "ETHUSDT".into();
        let abort = runtime.pairs[1].engine.abort_handle();
        let handle = runtime.handle();
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        db.set_membership_commit_hook(
            if operation == "add" {
                "add_after"
            } else {
                "remove_after"
            },
            reached_tx,
            resume_rx,
        );
        let manager = tokio::spawn(async move {
            let reason = runtime.run().await;
            (reason, runtime)
        });
        let command_handle = handle.clone();
        let command = tokio::spawn(async move {
            if operation == "add" {
                command_handle.add_pair(added).await
            } else {
                command_handle.remove_pair("SOLUSDC".into()).await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), reached_rx)
            .await
            .unwrap()
            .unwrap();
        if operation == "remove" {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), observed_rx.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                "SOLUSDC"
            );
        }
        handle.request_shutdown();
        handle.start_early_shutdown(Duration::from_millis(50)).await;
        loop {
            let symbol = tokio::time::timeout(Duration::from_secs(5), observed_rx.recv())
                .await
                .unwrap()
                .unwrap();
            if symbol == "BTCUSDT" {
                break;
            }
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while !abort.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("unresponsive engine was not aborted while membership commit was held");
        assert!(!command.is_finished());
        assert!(!manager.is_finished());
        resume_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), command)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let (reason, mut runtime) = tokio::time::timeout(Duration::from_secs(10), manager)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reason, "Runtime shutdown requested");
        let error = tokio::time::timeout(
            Duration::from_secs(10),
            runtime.shutdown(Duration::from_secs(5)),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(error.to_string().contains("BTCUSDT"), "{error}");
        assert_eq!(*states[1].status.read().await, BotStatus::Paused);
        drop(runtime);
        drop(handle);
        drop(states);
        drop(db);
        server.abort();
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[tokio::test]
async fn shutdown_stops_all_engines_while_removal_waits_for_membership_lock() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut runtime = fixture_with_shutdown_observer(db.clone(), false, false, Some(observed_tx));
    let handle = runtime.handle();
    let membership = handle.membership_lock.lock().await;
    let (waiting_tx, waiting_rx) = oneshot::channel();
    let removal = tokio::spawn(async move {
        let result = {
            let removal = runtime.remove_pair("SOLUSDC");
            tokio::pin!(removal);
            tokio::select! {
                biased;
                result = &mut removal => panic!("removal unexpectedly completed: {result:?}"),
                _ = tokio::task::yield_now() => {}
            }
            waiting_tx.send(()).unwrap();
            removal.await
        };
        (result, runtime)
    });
    tokio::time::timeout(Duration::from_secs(5), waiting_rx)
        .await
        .expect("removal did not reach the membership lock")
        .unwrap();

    handle.request_shutdown();
    handle.start_early_shutdown(Duration::from_secs(30)).await;
    let mut observed = Vec::new();
    for _ in 0..2 {
        observed.push(
            tokio::time::timeout(Duration::from_secs(5), observed_rx.recv())
                .await
                .expect("engine did not begin shutdown while membership was locked")
                .unwrap(),
        );
    }
    observed.sort();
    assert_eq!(observed, vec!["BTCUSDT", "SOLUSDC"]);
    assert!(!removal.is_finished());
    assert!(observed_rx.try_recv().is_err());

    drop(membership);
    let (result, mut runtime) = tokio::time::timeout(Duration::from_secs(5), removal)
        .await
        .expect("removal did not finish after unlocking membership")
        .unwrap();
    assert_eq!(
        result.unwrap_err().to_string(),
        "程序正在退出，无法修改交易对列表"
    );
    runtime.shutdown(Duration::from_secs(5)).await.unwrap();
    assert!(runtime.pairs.iter().all(|pair| pair.engine.is_finished()));
    assert!(observed_rx.try_recv().is_err());
    let symbols: Vec<_> = db
        .load_config()
        .unwrap()
        .unwrap()
        .pairs
        .into_iter()
        .map(|pair| pair.symbol)
        .collect();
    assert_eq!(symbols, vec!["SOLUSDC", "BTCUSDT"]);
}

#[tokio::test]
async fn early_shutdown_cleans_existing_engines_during_membership_commit() {
    for operation in ["add", "remove"] {
        let directory = std::env::temp_dir().join(format!(
            "runtime-early-{operation}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("bot.db");
        let db = Arc::new(Database::open(&path).unwrap());
        let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
        let mut runtime =
            fixture_with_shutdown_observer(db.clone(), false, false, Some(observed_tx));
        let (url, server) =
            mock_exchange(StatusCode::OK, exchange_info("TRADING", valid_filters())).await;
        runtime.test_exchange_url = Some(url);
        let states = runtime.states();
        if operation == "remove" {
            states[0].pause_trading().await.unwrap();
        }
        let mut added = db
            .load_config()
            .unwrap()
            .unwrap()
            .strategy_configs()
            .unwrap()
            .remove(0);
        added.exchange.symbol = "ETHUSDT".into();
        let handle = runtime.handle();
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        db.set_membership_commit_hook(
            if operation == "add" {
                "add_after"
            } else {
                "remove_after"
            },
            reached_tx,
            resume_rx,
        );
        let manager = tokio::spawn(async move {
            let reason = runtime.run().await;
            (reason, runtime)
        });
        let command_handle = handle.clone();
        let command = tokio::spawn(async move {
            if operation == "add" {
                command_handle.add_pair(added).await
            } else {
                command_handle.remove_pair("SOLUSDC".into()).await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), reached_rx)
            .await
            .unwrap()
            .unwrap();
        if operation == "remove" {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), observed_rx.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                "SOLUSDC"
            );
        }
        handle.request_shutdown();
        handle.start_early_shutdown(Duration::from_secs(30)).await;
        let count = if operation == "add" { 2 } else { 1 };
        let mut observed = Vec::new();
        for _ in 0..count {
            observed.push(
                tokio::time::timeout(Duration::from_secs(5), observed_rx.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        observed.sort();
        assert_eq!(
            observed,
            if operation == "add" {
                vec!["BTCUSDT", "SOLUSDC"]
            } else {
                vec!["BTCUSDT"]
            }
        );
        assert!(observed_rx.try_recv().is_err());
        assert!(!command.is_finished());
        assert!(!manager.is_finished());
        if operation == "remove" {
            let early = handle.early_stops.lock().await;
            assert!(early
                .iter()
                .all(|(state, _)| state.pair_scope() != Some("SOLUSDC")));
        }
        resume_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), command)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let (reason, mut runtime) = tokio::time::timeout(Duration::from_secs(10), manager)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reason, "Runtime shutdown requested");
        let added_state = runtime
            .states()
            .into_iter()
            .find(|state| state.pair_scope() == Some("ETHUSDT"));
        if operation == "add" {
            assert!(added_state.as_ref().unwrap().is_shutting_down());
        }
        runtime.shutdown(Duration::from_secs(10)).await.unwrap();
        if operation == "add" {
            assert!(runtime
                .pairs
                .iter()
                .find(|pair| pair.state.pair_scope() == Some("ETHUSDT"))
                .unwrap()
                .engine
                .is_finished());
            let connection = rusqlite::Connection::open(&path).unwrap();
            let dirty: i64 = connection.query_row(
                "SELECT dirty FROM pair_execution_guard WHERE symbol = 'ETHUSDT' AND mode = 'PAPER'",
                [], |row| row.get(0)
            ).unwrap();
            assert_eq!(dirty, 0);
        }
        assert!(observed_rx.try_recv().is_err());
        drop(added_state);
        drop(runtime);
        drop(handle);
        drop(states);
        drop(db);
        server.abort();
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[tokio::test]
async fn shutdown_during_membership_commit_finishes_command_and_preserves_restart() {
    for operation in ["add", "remove"] {
        let directory = std::env::temp_dir().join(format!(
            "runtime-shutdown-{operation}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("bot.db");
        let db = Arc::new(Database::open(&path).unwrap());
        let mut runtime = fixture_with_database(db.clone(), false, false);
        let (url, server) =
            mock_exchange(StatusCode::OK, exchange_info("TRADING", valid_filters())).await;
        runtime.test_exchange_url = Some(url.clone());
        let states = runtime.states();
        if operation == "remove" {
            states[0].pause_trading().await.unwrap();
        }
        assert!(states[1].try_order_submission().is_some());
        let mut added_config = db
            .load_config()
            .unwrap()
            .unwrap()
            .strategy_configs()
            .unwrap()
            .remove(0);
        added_config.exchange.symbol = "ETHUSDT".into();
        let handle = runtime.handle();
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        db.set_membership_commit_hook(operation, reached_tx, resume_rx);
        let manager = tokio::spawn(async move {
            let reason = runtime.run().await;
            (reason, runtime)
        });
        let command_handle = handle.clone();
        let command = tokio::spawn(async move {
            if operation == "add" {
                command_handle.add_pair(added_config).await
            } else {
                command_handle.remove_pair("SOLUSDC".into()).await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), reached_rx)
            .await
            .expect("membership commit was not reached")
            .expect("membership commit hook dropped");
        assert!(!command.is_finished());
        assert!(!manager.is_finished());

        handle.request_shutdown();
        for state in &states {
            assert!(state.is_shutting_down());
            assert!(state.try_order_submission().is_none());
        }
        assert!(!command.is_finished());
        assert!(!manager.is_finished());
        let (queued_reply, queued_result) = oneshot::channel();
        assert!(handle
            .commands
            .try_send(RuntimeCommand::Remove {
                symbol: "BTCUSDT".into(),
                reply: queued_reply,
            })
            .is_ok());
        resume_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), command)
            .await
            .expect("membership command timed out")
            .unwrap()
            .unwrap();
        let (reason, mut runtime) = tokio::time::timeout(Duration::from_secs(10), manager)
            .await
            .expect("runtime did not stop after command")
            .unwrap();
        assert_eq!(reason, "Runtime shutdown requested");
        let expected: Vec<&str> = if operation == "add" {
            vec!["SOLUSDC", "BTCUSDT", "ETHUSDT"]
        } else {
            vec!["BTCUSDT"]
        };
        assert_eq!(
            runtime
                .states()
                .iter()
                .map(|state| state.pair_scope().unwrap().to_string())
                .collect::<Vec<_>>(),
            expected
        );
        for state in runtime.states() {
            assert!(state.is_shutting_down());
            assert!(state.try_order_submission().is_none());
        }
        runtime.shutdown(Duration::from_secs(10)).await.unwrap();
        assert!(queued_result.await.unwrap().is_err());
        drop(runtime);
        drop(handle);
        drop(states);
        drop(db);

        let reopened = Arc::new(Database::open(&path).unwrap());
        let saved = reopened.load_config().unwrap().unwrap();
        assert_eq!(
            saved
                .pairs
                .iter()
                .map(|pair| pair.symbol.as_str())
                .collect::<Vec<_>>(),
            expected
        );
        let mut restarted = TradingRuntime::start_with_test_exchange(&saved, reopened.clone(), url)
            .await
            .unwrap();
        assert_eq!(
            restarted
                .states()
                .iter()
                .map(|state| state.pair_scope().unwrap().to_string())
                .collect::<Vec<_>>(),
            expected
        );
        restarted.shutdown(Duration::from_secs(10)).await.unwrap();
        drop(restarted);
        drop(reopened);
        server.abort();
        std::fs::remove_dir_all(directory).unwrap();
    }
}
#[tokio::test]
async fn shutdown_during_failed_removal_reports_error_and_preserves_pair_on_restart() {
    let directory = std::env::temp_dir().join(format!(
        "runtime-shutdown-remove-failure-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("bot.db");
    let db = Arc::new(Database::open(&path).unwrap());
    let mut runtime = fixture_with_database(db.clone(), false, false);
    let (url, server) =
        mock_exchange(StatusCode::OK, exchange_info("TRADING", valid_filters())).await;
    runtime.test_exchange_url = Some(url.clone());
    let states = runtime.states();
    states[0].pause_trading().await.unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_shutdown_remove BEFORE UPDATE ON app_config BEGIN SELECT RAISE(ABORT, 'shutdown removal failed'); END;").unwrap();
    drop(connection);
    let handle = runtime.handle();
    let (reached_tx, reached_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    db.set_membership_commit_hook("remove", reached_tx, resume_rx);
    let manager = tokio::spawn(async move {
        let reason = runtime.run().await;
        (reason, runtime)
    });
    let command_handle = handle.clone();
    let command = tokio::spawn(async move { command_handle.remove_pair("SOLUSDC".into()).await });
    tokio::time::timeout(Duration::from_secs(5), reached_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(states[1].try_order_submission().is_some());
    handle.request_shutdown();
    assert!(states[1].try_order_submission().is_none());
    assert!(!command.is_finished());
    assert!(!manager.is_finished());
    resume_tx.send(()).unwrap();
    let error = tokio::time::timeout(Duration::from_secs(10), command)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Failed to save config to SQLite database"),
        "{error:#}"
    );
    let (reason, mut runtime) = tokio::time::timeout(Duration::from_secs(10), manager)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reason, "Runtime shutdown requested");
    assert_eq!(runtime.states().len(), 2);
    for state in runtime.states() {
        assert!(state.is_shutting_down());
        assert!(state.try_order_submission().is_none());
    }
    runtime.shutdown(Duration::from_secs(10)).await.unwrap();
    drop(runtime);
    drop(handle);
    drop(states);
    drop(db);
    let reopened = Arc::new(Database::open(&path).unwrap());
    let saved = reopened.load_config().unwrap().unwrap();
    assert_eq!(
        saved
            .pairs
            .iter()
            .map(|pair| pair.symbol.as_str())
            .collect::<Vec<_>>(),
        vec!["SOLUSDC", "BTCUSDT"]
    );
    let mut restarted = TradingRuntime::start_with_test_exchange(&saved, reopened.clone(), url)
        .await
        .unwrap();
    assert_eq!(
        restarted
            .states()
            .iter()
            .map(|state| state.pair_scope().unwrap().to_string())
            .collect::<Vec<_>>(),
        vec!["SOLUSDC", "BTCUSDT"]
    );
    restarted.shutdown(Duration::from_secs(10)).await.unwrap();
    drop(restarted);
    drop(reopened);
    server.abort();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn adding_pair_rolls_back_when_guard_write_fails_and_can_be_retried() {
    let path = std::env::temp_dir().join(format!("add-guard-failure-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    let mut runtime = fixture_with_database(db.clone(), false, false);
    let (url, server) =
        mock_exchange(StatusCode::OK, exchange_info("TRADING", valid_filters())).await;
    runtime.test_exchange_url = Some(url);
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_new_guard BEFORE INSERT ON pair_execution_guard WHEN NEW.symbol = 'ETHUSDT' BEGIN SELECT RAISE(ABORT, 'guard write failed'); END;").unwrap();
    let manager = tokio::spawn(async move { runtime.run().await });

    let (_, response) = request(&router, "/api/pairs", "POST", payload("ETHUSDT"), true).await;
    assert_eq!(response["success"], false);
    assert!(!db
        .load_config()
        .unwrap()
        .unwrap()
        .pairs
        .iter()
        .any(|pair| pair.symbol == "ETHUSDT"));
    assert_eq!(
        db.load_pair_status("ETHUSDT", TradingMode::Paper).unwrap(),
        None
    );
    assert_eq!(handle.states.read().await.len(), 2);

    connection
        .execute_batch("DROP TRIGGER reject_new_guard;")
        .unwrap();
    let (_, response) = request(&router, "/api/pairs", "POST", payload("ETHUSDT"), true).await;
    assert_eq!(response["success"], true, "{response}");
    assert!(db
        .load_config()
        .unwrap()
        .unwrap()
        .pairs
        .iter()
        .any(|pair| pair.symbol == "ETHUSDT"));
    assert_eq!(
        db.load_pair_status("ETHUSDT", TradingMode::Paper).unwrap(),
        Some(BotStatus::Paused)
    );
    assert_eq!(handle.states.read().await.len(), 3);
    manager.abort();
    server.abort();
    drop(connection);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn adding_pairs_requires_valid_exchange_rules_before_saving() {
    let valid = valid_filters();
    let mut no_price = valid.clone();
    no_price.as_array_mut().unwrap().remove(0);
    let mut no_lot = valid.clone();
    no_lot.as_array_mut().unwrap().remove(1);
    let mut zero_tick = valid.clone();
    zero_tick[0]["tickSize"] = json!("0");
    let mut negative_tick = valid.clone();
    negative_tick[0]["tickSize"] = json!("-0.01");
    let mut zero_step = valid.clone();
    zero_step[1]["stepSize"] = json!("0");
    let mut negative_step = valid.clone();
    negative_step[1]["stepSize"] = json!("-0.001");
    let mut mismatched_tick = valid.clone();
    mismatched_tick[0]["tickSize"] = json!("0.03");

    for (name, status, body) in [
        ("missing symbol", StatusCode::OK, json!({"symbols": []})),
        (
            "wrong symbol",
            StatusCode::OK,
            json!({"symbols": [{"symbol":"OTHER", "status":"TRADING", "pricePrecision":2, "quantityPrecision":3, "filters":valid.clone()}]}),
        ),
        (
            "not trading",
            StatusCode::OK,
            exchange_info("BREAK", valid.clone()),
        ),
        (
            "missing price filter",
            StatusCode::OK,
            exchange_info("TRADING", no_price),
        ),
        (
            "missing lot filter",
            StatusCode::OK,
            exchange_info("TRADING", no_lot),
        ),
        (
            "zero tick",
            StatusCode::OK,
            exchange_info("TRADING", zero_tick),
        ),
        (
            "negative tick",
            StatusCode::OK,
            exchange_info("TRADING", negative_tick),
        ),
        (
            "zero step",
            StatusCode::OK,
            exchange_info("TRADING", zero_step),
        ),
        (
            "negative step",
            StatusCode::OK,
            exchange_info("TRADING", negative_step),
        ),
        (
            "grid mismatch",
            StatusCode::OK,
            exchange_info("TRADING", mismatched_tick),
        ),
        (
            "invalid response",
            StatusCode::OK,
            json!({"symbols": "invalid"}),
        ),
        (
            "API error",
            StatusCode::BAD_REQUEST,
            json!({"code":-1121,"msg":"Invalid symbol."}),
        ),
        (
            "other API error",
            StatusCode::BAD_REQUEST,
            json!({"code":-1000,"msg":"Internal error"}),
        ),
    ] {
        let (url, server) = mock_exchange(status, body).await;
        let mut runtime = fixture();
        runtime.test_exchange_url = Some(url);
        let handle = runtime.handle();
        let router = create_managed_router(handle.clone()).await;
        let manager = tokio::spawn(async move { runtime.run().await });
        let (_, response) = request(&router, "/api/pairs", "POST", payload("ETHUSDT"), true).await;
        assert_eq!(response["success"], false, "{name}: {response}");
        if name == "API error" {
            assert!(response["message"]
                .as_str()
                .unwrap()
                .contains("交易对 ETHUSDT 不存在"));
        } else if name == "other API error" || name == "invalid response" {
            assert!(response["message"]
                .as_str()
                .unwrap()
                .contains("暂时无法验证交易对"));
        }
        let saved = handle.db.load_config().unwrap().unwrap();
        assert_eq!(saved.pairs.len(), 2, "{name}");
        assert!(
            !saved.pairs.iter().any(|pair| pair.symbol == "ETHUSDT"),
            "{name}"
        );
        assert_eq!(handle.states.read().await.len(), 2, "{name}");
        manager.abort();
        server.abort();
    }
}

#[tokio::test]
async fn adding_pair_rejects_network_failure_without_saving() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let mut runtime = fixture();
    runtime.test_exchange_url = Some(url);
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    let (_, response) = request(&router, "/api/pairs", "POST", payload("ETHUSDT"), true).await;
    assert_eq!(response["success"], false);
    assert!(response["message"]
        .as_str()
        .unwrap()
        .contains("暂时无法验证交易对"));
    let saved = handle.db.load_config().unwrap().unwrap();
    assert_eq!(saved.pairs.len(), 2);
    assert!(!saved.pairs.iter().any(|pair| pair.symbol == "ETHUSDT"));
    assert_eq!(handle.states.read().await.len(), 2);
    manager.abort();
}

#[tokio::test]
async fn management_authenticates_add_remove_server_settings_and_pair_resources() {
    let runtime = fixture();
    let router = create_managed_router(runtime.handle()).await;
    for (path, method, body) in [
        ("/api/overview", "GET", Value::Null),
        ("/api/overview/history", "GET", Value::Null),
        ("/api/pairs", "POST", payload("ETHUSDT")),
        ("/api/pairs/SOLUSDC", "DELETE", json!({"confirm":true})),
        ("/api/server/config", "GET", Value::Null),
        (
            "/api/server/config",
            "POST",
            json!({"host":"127.0.0.1","port":9090}),
        ),
        ("/api/pairs/BTCUSDT/config", "GET", Value::Null),
        ("/api/global/config", "GET", Value::Null),
        ("/api/global/config", "POST", json!({"recv_window":8000})),
    ] {
        assert_eq!(
            request(&router, path, method, body, false).await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        runtime
            .handle
            .db
            .load_config()
            .unwrap()
            .unwrap()
            .pairs
            .len(),
        2
    );
}

#[tokio::test]
async fn overview_history_returns_saved_snapshots() {
    let runtime = fixture();
    let hour = chrono::Utc::now().timestamp().div_euclid(3600) * 3600;
    let group = crate::snapshots::SnapshotGroup {
        mode: TradingMode::Paper,
        asset: "USDC".into(),
        equity: Some(rust_decimal_macros::dec!(100)),
        long: Some(rust_decimal_macros::dec!(20)),
        short: Some(rust_decimal_macros::dec!(0)),
    };
    runtime
        .handle
        .db
        .save_hourly_account_snapshot(hour, &[group])
        .unwrap();
    let router = create_managed_router(runtime.handle()).await;
    let (status, body) = request(&router, "/api/overview/history", "GET", Value::Null, true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["hour"], hour);
    assert_eq!(body["data"][0]["groups"][0]["asset"], "USDC");
}

#[tokio::test]
async fn overview_reads_current_managed_membership_without_restarting_router() {
    let mut runtime = fixture();
    let router = create_managed_router(runtime.handle()).await;
    let (status, response) = request(&router, "/api/overview", "GET", Value::Null, true).await;
    assert_eq!(status, StatusCode::OK);
    let pairs = response["data"]["pairs"].as_array().unwrap();
    assert_eq!(pairs.len(), 2);
    assert_eq!(pairs[0]["snapshot"]["symbol"], "SOLUSDC");
    assert_eq!(pairs[1]["snapshot"]["symbol"], "BTCUSDT");

    runtime.states()[0].pause_trading().await.unwrap();
    runtime.remove_pair("SOLUSDC").await.unwrap();
    let (status, response) = request(&router, "/api/overview", "GET", Value::Null, true).await;
    assert_eq!(status, StatusCode::OK);
    let pairs = response["data"]["pairs"].as_array().unwrap();
    assert_eq!(pairs.len(), 1);
    assert_eq!(pairs[0]["snapshot"]["symbol"], "BTCUSDT");
}

#[tokio::test]
async fn dashboard_membership_commands_update_routes_without_restarting_and_preserve_pause() {
    let (url, server) =
        mock_exchange(StatusCode::OK, exchange_info("TRADING", valid_filters())).await;
    let mut runtime = fixture();
    runtime.test_exchange_url = Some(url);
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    let (_, added) = request(&router, "/api/pairs", "POST", payload("ETHUSDT"), true).await;
    assert_eq!(added["success"], true);
    let (_, list) = request(&router, "/api/pairs", "GET", Value::Null, true).await;
    assert_eq!(list["data"].as_array().unwrap().len(), 3);
    let (_, added_config) = request(
        &router,
        "/api/pairs/ETHUSDT/config",
        "GET",
        Value::Null,
        true,
    )
    .await;
    assert_eq!(added_config["data"]["recv_window"], 5000);
    assert_eq!(added_config["data"]["sync_interval_secs"], 3);
    assert_eq!(added_config["data"]["has_api_key"], false);
    assert_eq!(
        handle
            .db
            .load_pair_status("ETHUSDT", TradingMode::Paper)
            .unwrap(),
        Some(BotStatus::Paused)
    );
    let (_, duplicate) = request(&router, "/api/pairs", "POST", payload("ETHUSDT"), true).await;
    assert_eq!(duplicate["success"], false);
    let (_, running_remove) = request(
        &router,
        "/api/pairs/SOLUSDC",
        "DELETE",
        json!({"confirm":true}),
        true,
    )
    .await;
    assert_eq!(running_remove["success"], false);
    request(
        &router,
        "/api/pairs/SOLUSDC/control",
        "POST",
        json!({"action":"pause"}),
        true,
    )
    .await;
    let (_, removed) = request(
        &router,
        "/api/pairs/SOLUSDC",
        "DELETE",
        json!({"confirm":true}),
        true,
    )
    .await;
    assert_eq!(removed["success"], true);
    assert_eq!(
        request(
            &router,
            "/api/pairs/SOLUSDC/status",
            "GET",
            Value::Null,
            true
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let (_, status) = request(&router, "/api/status", "GET", Value::Null, true).await;
    assert_eq!(status["data"]["symbol"], "BTCUSDT");
    assert_eq!(
        handle
            .db
            .load_config()
            .unwrap()
            .unwrap()
            .pairs
            .iter()
            .map(|pair| pair.symbol.as_str())
            .collect::<Vec<_>>(),
        ["BTCUSDT", "ETHUSDT"]
    );
    manager.abort();
    let _ = manager.await;
    server.abort();
}

#[tokio::test]
async fn settings_validation_and_server_changes_preserve_independent_pair_updates() {
    let mut runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    let (_, rejected) = request(
        &router,
        "/api/global/config",
        "POST",
        json!({"recv_window":0}),
        true,
    )
    .await;
    assert_eq!(rejected["success"], false);
    let mut update = json!({"recv_window":8000,"sync_interval_secs":7});
    update["api_key"] = json!("configuration-key");
    update["api_secret"] = json!("configuration-secret");
    update["telegram_bot_token"] = json!("configuration-bot-token");
    update["telegram_chat_id"] = json!("123456");
    let (_, applied) = request(&router, "/api/global/config", "POST", update, true).await;
    assert_eq!(applied["success"], true);
    let serialized = applied.to_string();
    for secret in [
        "configuration-key",
        "configuration-secret",
        "configuration-bot-token",
    ] {
        assert!(!serialized.contains(secret));
    }
    let mut clear = json!({});
    clear["clear_api_credentials"] = json!(true);
    clear["clear_telegram_credentials"] = json!(true);
    let (_, cleared) = request(&router, "/api/global/config", "POST", clear, true).await;
    assert_eq!(cleared["data"]["has_api_key"], false);
    assert_eq!(cleared["data"]["has_telegram_bot_token"], false);
    let (_, invalid_server) = request(
        &router,
        "/api/server/config",
        "POST",
        json!({"host":"bad-host","port":0}),
        true,
    )
    .await;
    assert_eq!(invalid_server["success"], false);
    let (_, server) = request(
        &router,
        "/api/server/config",
        "POST",
        json!({"host":"::1","port":9090}),
        true,
    )
    .await;
    assert_eq!(server["data"]["saved"]["port"], 9090);
    assert_eq!(server["data"]["active"]["port"], 8080);
    let (_, updated_btc) = request(
        &router,
        "/api/pairs/BTCUSDT/config",
        "POST",
        versioned_payload(&router, "BTCUSDT").await,
        true,
    )
    .await;
    assert_eq!(updated_btc["success"], true);
    let saved = handle.db.load_config().unwrap().unwrap();
    assert_eq!(saved.server.host, "::1");
    assert_eq!(saved.server.port, 9090);
    let configs = saved.strategy_configs().unwrap();
    assert!(configs.iter().all(
        |config| config.exchange.recv_window == 8000 && config.exchange.sync_interval_secs == 7
    ));
    assert!(configs[0].exchange.api_key.is_empty());
    manager.abort();
    let _ = manager.await;
}

#[tokio::test]
async fn managed_pair_updates_require_a_current_version() {
    let runtime = fixture();
    let router = create_managed_router(runtime.handle()).await;
    for path in ["/api/pairs/BTCUSDT/config", "/api/config"] {
        let symbol = if path == "/api/config" {
            "SOLUSDC"
        } else {
            "BTCUSDT"
        };
        for baseline in [None, Some(json!(null)), Some(json!("")), Some(json!("  "))] {
            let mut body = payload(symbol);
            if let Some(baseline) = baseline {
                body["baseline_version"] = baseline;
            }
            let (status, result) = request(&router, path, "POST", body, true).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(result["message"]
                .as_str()
                .unwrap()
                .contains("baseline_version"));
        }
        let mut stale = payload(symbol);
        stale["baseline_version"] = json!("stale");
        assert_eq!(
            request(&router, path, "POST", stale, true).await.0,
            StatusCode::CONFLICT
        );
    }
}

#[tokio::test]
async fn configuration_versions_reject_concurrent_stale_forms() {
    let mut runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    for (path, field, first, second) in [
        (
            "/api/global/config",
            "sync_interval_secs",
            json!(11),
            json!(12),
        ),
        ("/api/server/config", "port", json!(9001), json!(9002)),
    ] {
        let (_, initial) = request(&router, path, "GET", json!({}), true).await;
        let version = initial["data"]["version"].as_str().unwrap();
        let mut a = if field == "port" {
            json!({"host":"127.0.0.1"})
        } else {
            json!({})
        };
        let mut b = a.clone();
        a[field] = first;
        b[field] = second;
        a["baseline_version"] = json!(version);
        b["baseline_version"] = json!(version);
        let (a, b) = tokio::join!(
            request(&router, path, "POST", a, true),
            request(&router, path, "POST", b, true)
        );
        let (success, conflict) = if a.1["success"] == true {
            (a.1, b.1)
        } else {
            (b.1, a.1)
        };
        assert_eq!(success["success"], true);
        assert_eq!(conflict["success"], false);
        assert!(conflict["message"].as_str().unwrap().contains("刷新"));
        let (_, current) = request(&router, path, "GET", json!({}), true).await;
        assert_eq!(success["data"]["version"], current["data"]["version"]);
        assert_ne!(success["data"]["version"], version);
    }
    manager.abort();
    let _ = manager.await;
}

#[tokio::test]
async fn managed_account_stream_tracks_current_pair_membership() {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let url = format!("ws://{address}/api/accounts/ws");
    assert!(tokio_tungstenite::connect_async(&url).await.is_err());
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        "auth, token.configuration-test-token".parse().unwrap(),
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let first: Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
    assert_eq!(first["data"]["pairs"].as_array().unwrap().len(), 2);
    handle.states.write().await.pop();
    let updated = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let updated: Value = serde_json::from_str(updated.to_text().unwrap()).unwrap();
    assert_eq!(updated["data"]["pairs"].as_array().unwrap().len(), 1);
    assert_eq!(updated["data"]["pairs"][0]["snapshot"]["symbol"], "SOLUSDC");
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn managed_websockets_accept_protocol_tokens_without_bearer_headers() {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
    let runtime = fixture();
    let router = create_managed_router(runtime.handle()).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let mut request = format!("ws://{address}/api/pairs/BTCUSDT/ws")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        "auth, token.configuration-test-token".parse().unwrap(),
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Message::Text(text) = message else {
        panic!("Expected snapshot");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&text).unwrap()["data"]["symbol"],
        "BTCUSDT"
    );
    assert!(
        tokio_tungstenite::connect_async(format!("ws://{address}/api/pairs/BTCUSDT/ws"))
            .await
            .is_err()
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn global_position_limit_api_persists_validates_and_supports_legacy_updates() {
    let mut runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    let path = "/api/global/config";

    let (_, initial) = request(&router, path, "GET", Value::Null, true).await;
    assert!(initial["data"]["global_max_position_notional"].is_null());
    let (_, updated) = request(
        &router,
        path,
        "POST",
        json!({"global_max_position_notional":"1234.50"}),
        true,
    )
    .await;
    assert_eq!(updated["success"], true);
    assert_eq!(updated["data"]["global_max_position_notional"], "1234.50");
    assert_eq!(
        handle
            .db
            .load_config()
            .unwrap()
            .unwrap()
            .global_max_position_notional
            .unwrap()
            .to_string(),
        "1234.50"
    );

    for value in ["0", "-1"] {
        let (_, rejected) = request(
            &router,
            path,
            "POST",
            json!({ "global_max_position_notional": value }),
            true,
        )
        .await;
        assert_eq!(rejected["success"], false);
        assert_eq!(
            handle
                .db
                .load_config()
                .unwrap()
                .unwrap()
                .global_max_position_notional
                .unwrap()
                .to_string(),
            "1234.50"
        );
    }
    let (_, legacy) = request(&router, path, "POST", json!({"recv_window":8000}), true).await;
    assert_eq!(legacy["data"]["global_max_position_notional"], "1234.50");
    let (_, cleared) = request(
        &router,
        path,
        "POST",
        json!({ "global_max_position_notional": null }),
        true,
    )
    .await;
    assert_eq!(cleared["success"], true);
    assert!(cleared["data"]["global_max_position_notional"].is_null());
    assert!(handle
        .db
        .load_config()
        .unwrap()
        .unwrap()
        .global_max_position_notional
        .is_none());
    manager.abort();
    let _ = manager.await;
}

#[tokio::test]
async fn zero_available_margin_pause_global_api_preserves_legacy_updates() {
    let mut runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    let path = "/api/global/config";

    let (_, initial) = request(&router, path, "GET", Value::Null, true).await;
    assert_eq!(initial["data"]["pause_on_zero_available_margin"], false);
    for (payload, expected) in [
        (json!({"pause_on_zero_available_margin": true}), true),
        (json!({"recv_window": 8000}), true),
        (json!({"pause_on_zero_available_margin": false}), false),
        (json!({"recv_window": 9000}), false),
    ] {
        let (_, updated) = request(&router, path, "POST", payload, true).await;
        assert_eq!(updated["success"], true);
        assert_eq!(updated["data"]["pause_on_zero_available_margin"], expected);
        let (_, fetched) = request(&router, path, "GET", Value::Null, true).await;
        assert_eq!(fetched["data"]["pause_on_zero_available_margin"], expected);
        assert_eq!(
            handle
                .db
                .load_config()
                .unwrap()
                .unwrap()
                .pause_on_zero_available_margin,
            expected
        );
    }
    manager.abort();
    let _ = manager.await;
}

#[tokio::test]
async fn global_settings_reach_every_engine_and_new_pairs_inherit_without_overrides() {
    let (url, server) =
        mock_exchange(StatusCode::OK, exchange_info("TRADING", valid_filters())).await;
    let mut runtime = fixture();
    runtime.test_exchange_url = Some(url);
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    let mut btc = versioned_payload(&router, "BTCUSDT").await;
    btc["grid_interval"] = json!("10");
    request(&router, "/api/pairs/BTCUSDT/config", "POST", btc, true).await;
    let (_, global) = request(
        &router,
        "/api/global/config",
        "POST",
        json!({
            "api_key":"shared-key", "api_secret":"shared-secret", "recv_window":9000,
            "sync_interval_secs":9, "telegram_enabled":true,
            "telegram_bot_token":"shared-token", "telegram_chat_id":"123456"
        }),
        true,
    )
    .await;
    assert_eq!(global["success"], true);
    for secret in ["shared-key", "shared-secret", "shared-token"] {
        assert!(!global.to_string().contains(secret));
    }
    for state in handle.states.read().await.iter() {
        let config = state.config.read().await;
        assert_eq!(config.exchange.api_key, "shared-key");
        assert_eq!(config.exchange.recv_window, 9000);
        assert_eq!(config.exchange.sync_interval_secs, 9);
        assert_eq!(config.telegram.bot_token, "shared-token");
        assert!(config.telegram.enabled);
        assert_eq!(*state.status.read().await, BotStatus::Running);
    }
    let (_, blank) = request(
        &router,
        "/api/global/config",
        "POST",
        json!({
            "api_key":" ", "api_secret":"", "telegram_bot_token":"", "telegram_enabled":false
        }),
        true,
    )
    .await;
    assert_eq!(blank["success"], true);
    let (_, added) = request(&router, "/api/pairs", "POST", payload("ETHUSDT"), true).await;
    assert_eq!(added["success"], true);
    let (_, eth) = request(
        &router,
        "/api/pairs/ETHUSDT/config",
        "GET",
        Value::Null,
        true,
    )
    .await;
    assert_eq!(eth["data"]["has_api_key"], true);
    assert_eq!(eth["data"]["has_telegram_bot_token"], true);
    assert_eq!(eth["data"]["recv_window"], 9000);
    assert_eq!(eth["data"]["sync_interval_secs"], 9);
    let saved = handle.db.load_config().unwrap().unwrap();
    assert_eq!(saved.exchange.api_key, "shared-key");
    assert_eq!(saved.exchange.api_secret, "shared-secret");
    assert_eq!(saved.telegram.bot_token, "shared-token");
    assert!(saved
        .pairs
        .iter()
        .all(|pair| pair.exchange.api_key.is_none()
            && pair.exchange.api_secret.is_none()
            && pair.exchange.recv_window.is_none()
            && pair.exchange.sync_interval_secs.is_none()
            && pair.telegram.is_none()));
    let strategies = saved.strategy_configs().unwrap();
    assert_eq!(strategies[1].grid.grid_interval.to_string(), "10");
    assert_eq!(strategies[2].grid.grid_interval.to_string(), "0.1");
    assert!(strategies
        .iter()
        .all(|config| config.exchange.api_key == "shared-key"));
    let mut override_payload = versioned_payload(&router, "SOLUSDC").await;
    override_payload["api_key"] = json!("pair-key");
    let (_, rejected) = request(
        &router,
        "/api/pairs/SOLUSDC/config",
        "POST",
        override_payload.clone(),
        true,
    )
    .await;
    assert_eq!(rejected["success"], false);
    let (_, rejected_add) = request(&router, "/api/pairs", "POST", override_payload, true).await;
    assert_eq!(rejected_add["success"], false);
    manager.abort();
    let _ = manager.await;
    server.abort();
}

#[tokio::test]
async fn global_clearing_validates_all_pair_modes_and_notification_credentials() {
    let mut runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    let (_, applied) = request(
        &router,
        "/api/global/config",
        "POST",
        json!({
            "api_key":"test-key", "api_secret":"test-secret", "telegram_enabled":true,
            "telegram_bot_token":"test-token", "telegram_chat_id":"123"
        }),
        true,
    )
    .await;
    assert_eq!(applied["success"], true);
    let mut live = versioned_payload(&router, "BTCUSDT").await;
    live["dry_run"] = json!(false);
    assert_eq!(
        request(&router, "/api/pairs/BTCUSDT/config", "POST", live, true)
            .await
            .1["success"],
        true
    );
    for clear in [
        json!({"clear_api_credentials":true}),
        json!({"clear_telegram_credentials":true}),
    ] {
        let (_, rejected) = request(&router, "/api/global/config", "POST", clear, true).await;
        assert_eq!(rejected["success"], false);
    }
    assert!(handle
        .states
        .read()
        .await
        .iter()
        .all(|state| !state.is_shutting_down()));
    let mut paper = versioned_payload(&router, "BTCUSDT").await;
    paper["dry_run"] = json!(true);
    request(&router, "/api/pairs/BTCUSDT/config", "POST", paper, true).await;
    let (_, cleared) = request(&router, "/api/global/config", "POST", json!({
        "clear_api_credentials":true, "clear_telegram_credentials":true, "telegram_enabled":false
    }), true).await;
    assert_eq!(cleared["success"], true);
    for state in handle.states.read().await.iter() {
        let config = state.config.read().await;
        assert!(config.exchange.api_key.is_empty());
        assert!(config.telegram.bot_token.is_empty());
        assert!(config.telegram.chat_id.is_empty());
        assert!(!config.telegram.enabled);
    }
    let mut saved = handle.db.load_config().unwrap().unwrap();
    saved.ensure_pair_list().unwrap();
    assert!(saved.strategy_configs().unwrap().iter().all(|config| config
        .exchange
        .api_key
        .is_empty()
        && config.telegram.bot_token.is_empty()));
    manager.abort();
    let _ = manager.await;
}

#[tokio::test]
async fn global_reconciliation_failure_restores_both_changed_engines_and_saved_globals() {
    let runtime = fixture_with_failures(false, true);
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let (_, rejected) = request(
        &router,
        "/api/global/config",
        "POST",
        json!({"recv_window":8888}),
        true,
    )
    .await;
    assert_eq!(rejected["success"], false);
    assert!(rejected["message"]
        .as_str()
        .unwrap()
        .contains("已恢复原配置"));
    assert_eq!(
        handle
            .db
            .load_config()
            .unwrap()
            .unwrap()
            .exchange
            .recv_window,
        5000
    );
    for state in handle.states.read().await.iter() {
        assert_eq!(state.config.read().await.exchange.recv_window, 5000);
        assert!(state.try_order_submission().is_some());
    }
}

#[tokio::test]
async fn global_update_drains_old_submissions_before_switching_and_releases_only_after_commit() {
    let runtime = fixture();
    let handle = runtime.handle();
    let states = runtime.states();
    // An order in the second engine is still being submitted with the old credentials.
    let in_flight = states[1].try_order_submission().unwrap();
    let updating = handle.clone();
    let update = tokio::spawn(async move {
        updating
            .update_global_config(crate::types::UpdateGlobalConfigPayload {
                api_key: Some("new-account-key".into()),
                api_secret: Some("new-account-secret".into()),
                recv_window: Some(9000),
                ..Default::default()
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while states[0].try_order_submission().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!update.is_finished());
    // No pair may switch while an old-account submission remains in flight.
    for state in &states {
        assert!(state.config.read().await.exchange.api_key.is_empty());
        assert_eq!(state.config.read().await.exchange.recv_window, 5000);
    }
    // Even a manual resume cannot bypass the gate.
    states[0].resume_trading().await.unwrap();
    assert!(states[0].try_order_submission().is_none());
    assert!(handle
        .db
        .load_config()
        .unwrap()
        .unwrap()
        .exchange
        .api_key
        .is_empty());
    drop(in_flight);
    tokio::time::timeout(Duration::from_secs(2), update)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let saved = handle.db.load_config().unwrap().unwrap();
    assert_eq!(saved.exchange.api_key, "new-account-key");
    assert_eq!(saved.exchange.recv_window, 9000);
    for state in &states {
        assert_eq!(
            state.config.read().await.exchange.api_key,
            "new-account-key"
        );
        assert!(state.try_order_submission().is_some());
        assert_eq!(*state.status.read().await, BotStatus::Running);
    }
}

#[tokio::test]
async fn enabling_global_guard_blocks_submissions_before_commit_returns() {
    let runtime = fixture();
    runtime
        .global_position_limit
        .membership_ready
        .store(true, std::sync::atomic::Ordering::SeqCst);
    for state in runtime.states() {
        state
            .global_position_initialized
            .store(true, std::sync::atomic::Ordering::SeqCst);
        state.account.write().await.available_balance = rust_decimal::Decimal::ZERO;
    }
    tokio::time::timeout(
        Duration::from_secs(2),
        runtime
            .handle()
            .update_global_config(crate::types::UpdateGlobalConfigPayload {
                pause_on_zero_available_margin: Some(true),
                ..Default::default()
            }),
    )
    .await
    .unwrap()
    .unwrap();
    for state in runtime.states() {
        assert!(state.try_order_submission().is_none());
        assert_eq!(*state.status.read().await, BotStatus::Paused);
    }
}

#[tokio::test]
async fn global_persistence_failure_rolls_back_before_allowing_new_submissions() {
    let path =
        std::env::temp_dir().join(format!("global-commit-failure-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    let runtime = fixture_with_database(db.clone(), false, false);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_global_config BEFORE UPDATE ON app_config
         WHEN json_extract(NEW.config_json, '$.exchange.recv_window') = 9000
         BEGIN SELECT RAISE(ABORT, 'simulated global persistence failure'); END;",
        )
        .unwrap();
    drop(connection);
    let error = runtime
        .handle()
        .update_global_config(crate::types::UpdateGlobalConfigPayload {
            recv_window: Some(9000),
            pause_on_zero_available_margin: Some(true),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(!runtime
        .global_position_limit
        .enabled
        .load(std::sync::atomic::Ordering::SeqCst));
    assert!(error.to_string().contains("已恢复原配置"));
    assert_eq!(
        db.load_config().unwrap().unwrap().exchange.recv_window,
        5000
    );
    for state in runtime.states() {
        assert_eq!(state.config.read().await.exchange.recv_window, 5000);
        assert!(state.try_order_submission().is_some());
    }
    drop(runtime);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn concurrent_global_patches_and_pair_updates_preserve_shared_and_independent_settings() {
    let runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let mut pair = versioned_payload(&router, "BTCUSDT").await;
    pair["buy_window"] = json!(3);
    let (recv, sync, pair) = tokio::join!(
        request(
            &router,
            "/api/global/config",
            "POST",
            json!({"recv_window":9000}),
            true
        ),
        request(
            &router,
            "/api/global/config",
            "POST",
            json!({"sync_interval_secs":9}),
            true
        ),
        request(&router, "/api/pairs/BTCUSDT/config", "POST", pair, true)
    );
    assert_eq!(recv.1["success"], true);
    assert_eq!(sync.1["success"], true);
    if pair.0 == StatusCode::CONFLICT {
        assert_eq!(pair.1["success"], false);
        let mut retry = versioned_payload(&router, "BTCUSDT").await;
        retry["buy_window"] = json!(3);
        let retried = request(&router, "/api/pairs/BTCUSDT/config", "POST", retry, true).await;
        assert_eq!(retried.1["success"], true);
    } else {
        assert_eq!(pair.0, StatusCode::OK);
        assert_eq!(pair.1["success"], true);
    }
    let saved = handle.db.load_config().unwrap().unwrap();
    assert_eq!(saved.exchange.recv_window, 9000);
    assert_eq!(saved.exchange.sync_interval_secs, 9);
    for state in handle.states.read().await.iter() {
        let config = state.config.read().await;
        assert_eq!(config.exchange.recv_window, 9000);
        assert_eq!(config.exchange.sync_interval_secs, 9);
    }
    assert_eq!(saved.strategy_configs().unwrap()[0].grid.buy_window, 5);
    assert_eq!(saved.strategy_configs().unwrap()[1].grid.buy_window, 3);
}

#[tokio::test]
async fn engine_start_fails_when_execution_guard_cannot_be_written() {
    let path =
        std::env::temp_dir().join(format!("guard-write-failure-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    let config = AppConfig::default();
    db.save_config(&config).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_guard BEFORE INSERT ON pair_execution_guard BEGIN SELECT RAISE(ABORT, 'guard write failed'); END;").unwrap();
    drop(connection);
    assert!(TradingRuntime::start_with_test_exchange(
        &config,
        db.clone(),
        "http://127.0.0.1:1".into()
    )
    .await
    .is_err());
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn later_startup_failure_cleans_up_prepared_engine_and_preserves_unstarted_metadata() {
    let app = Router::new().fallback(|| async { std::future::pending::<StatusCode>().await });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let directory =
        std::env::temp_dir().join(format!("runtime-start-failure-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("bot.db");
    let config = AppConfig {
        pairs: ["SOLUSDC", "BTCUSDT"]
            .map(|symbol| TradingPairConfig::new(symbol.into()))
            .to_vec(),
        ..Default::default()
    };
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_config(&config).unwrap();
    for symbol in ["SOLUSDC", "BTCUSDT"] {
        db.save_pair_status(symbol, TradingMode::Paper, BotStatus::Running)
            .unwrap();
    }
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_second_guard BEFORE INSERT ON pair_execution_guard WHEN NEW.symbol = 'BTCUSDT' BEGIN SELECT RAISE(ABORT, 'second guard failed'); END;").unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        TradingRuntime::start_with_test_exchange(&config, db.clone(), url),
    )
    .await
    .expect("startup cleanup timed out");
    let error = match result {
        Ok(_) => panic!("startup unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(format!("{error:#}").contains("second guard failed"));
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
        Some(BotStatus::Paused)
    );
    assert_eq!(
        db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
        Some(BotStatus::Running)
    );
    let first_guard: i64 = connection
        .query_row(
            "SELECT dirty FROM pair_execution_guard WHERE symbol = 'SOLUSDC' AND mode = 'PAPER'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let second_guards: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM pair_execution_guard WHERE symbol = 'BTCUSDT'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(first_guard, 0);
    assert_eq!(second_guards, 0);
    drop(connection);
    drop(db);
    server.abort();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn startup_already_interrupted_does_not_arm_guards_or_change_status() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let config = AppConfig {
        pairs: ["SOLUSDC", "BTCUSDT"]
            .map(|symbol| TradingPairConfig::new(symbol.into()))
            .to_vec(),
        ..Default::default()
    };
    db.save_config(&config).unwrap();
    for symbol in ["SOLUSDC", "BTCUSDT"] {
        db.save_pair_status(symbol, TradingMode::Paper, BotStatus::Running)
            .unwrap();
    }
    let (_interrupt_tx, interrupt_rx) = watch::channel(true);
    let (hook_tx, mut hook_rx) = mpsc::channel(1);
    let result = TradingRuntime::start_with_test_exchange_and_hook(
        &config,
        db.clone(),
        "http://127.0.0.1:1".into(),
        interrupt_rx,
        hook_tx,
    )
    .await;
    assert!(result.is_err());
    assert!(hook_rx.try_recv().is_err());
    for symbol in ["SOLUSDC", "BTCUSDT"] {
        assert_eq!(
            db.load_pair_status(symbol, TradingMode::Paper).unwrap(),
            Some(BotStatus::Running)
        );
        assert!(!db.arm_execution_guard(symbol, TradingMode::Paper).unwrap());
    }
}

#[tokio::test]
async fn startup_reports_cleanup_failure_and_retains_dirty_recovery_guard() {
    let app = Router::new().fallback(|| async { std::future::pending::<StatusCode>().await });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let directory = std::env::temp_dir().join(format!(
        "runtime-start-cleanup-failure-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("bot.db");
    let config = AppConfig {
        pairs: ["SOLUSDC", "BTCUSDT"]
            .map(|symbol| TradingPairConfig::new(symbol.into()))
            .to_vec(),
        ..Default::default()
    };
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_config(&config).unwrap();
    db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Running)
        .unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_second_guard BEFORE INSERT ON pair_execution_guard WHEN NEW.symbol = 'BTCUSDT' BEGIN SELECT RAISE(ABORT, 'second guard failed'); END;
        CREATE TRIGGER reject_guard_clear BEFORE UPDATE ON pair_execution_guard WHEN OLD.symbol = 'SOLUSDC' AND NEW.dirty = 0 BEGIN SELECT RAISE(ABORT, 'guard clear failed'); END;").unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        TradingRuntime::start_with_test_exchange(&config, db.clone(), url),
    )
    .await
    .expect("startup cleanup timed out");
    let error = match result {
        Ok(_) => panic!("startup unexpectedly succeeded"),
        Err(error) => format!("{error:#}"),
    };
    assert!(error.contains("second guard failed"), "{error}");
    assert!(error.contains("guard clear failed"), "{error}");
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
        Some(BotStatus::Paused)
    );
    let dirty: i64 = connection
        .query_row(
            "SELECT dirty FROM pair_execution_guard WHERE symbol = 'SOLUSDC' AND mode = 'PAPER'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(dirty, 1);
    drop(connection);
    drop(db);
    server.abort();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn interrupted_startup_cleans_every_prepared_engine_and_leaves_unstarted_pair_untouched() {
    let app = Router::new().fallback(|| async { std::future::pending::<StatusCode>().await });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let directory = std::env::temp_dir().join(format!(
        "runtime-start-interrupted-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("bot.db");
    let config = AppConfig {
        pairs: ["SOLUSDC", "BTCUSDT", "ETHUSDT"]
            .map(|symbol| TradingPairConfig::new(symbol.into()))
            .to_vec(),
        ..Default::default()
    };
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_config(&config).unwrap();
    for symbol in ["SOLUSDC", "BTCUSDT", "ETHUSDT"] {
        db.save_pair_status(symbol, TradingMode::Paper, BotStatus::Running)
            .unwrap();
    }
    let (interrupt_tx, interrupt_rx) = watch::channel(false);
    let (hook_tx, mut hook_rx) = mpsc::channel(1);
    let startup_db = db.clone();
    let startup = tokio::spawn(async move {
        TradingRuntime::start_with_test_exchange_and_hook(
            &config,
            startup_db,
            url,
            interrupt_rx,
            hook_tx,
        )
        .await
    });
    for expected in ["SOLUSDC", "BTCUSDT"] {
        let (symbol, ack) = tokio::time::timeout(Duration::from_secs(5), hook_rx.recv())
            .await
            .unwrap()
            .expect("startup ended before preparing expected pair");
        assert_eq!(symbol, expected);
        if expected == "BTCUSDT" {
            interrupt_tx.send(true).unwrap();
        }
        ack.send(()).unwrap();
    }
    let result = tokio::time::timeout(Duration::from_secs(10), startup)
        .await
        .expect("interrupted startup cleanup timed out")
        .unwrap();
    assert!(result.is_err());
    let connection = rusqlite::Connection::open(&path).unwrap();
    for symbol in ["SOLUSDC", "BTCUSDT"] {
        assert_eq!(
            db.load_pair_status(symbol, TradingMode::Paper).unwrap(),
            Some(BotStatus::Paused)
        );
        let dirty: i64 = connection
            .query_row(
                "SELECT dirty FROM pair_execution_guard WHERE symbol = ?1 AND mode = 'PAPER'",
                [symbol],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(dirty, 0, "{symbol} guard was not cleared after cleanup");
    }
    assert_eq!(
        db.load_pair_status("ETHUSDT", TradingMode::Paper).unwrap(),
        Some(BotStatus::Running)
    );
    let unstarted_guards: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM pair_execution_guard WHERE symbol = 'ETHUSDT'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(unstarted_guards, 0);
    drop(connection);
    drop(db);
    server.abort();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn interrupt_during_pair_preparation_blocks_orders_and_cleans_up_guards() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let order_submissions = Arc::new(AtomicUsize::new(0));
    let observed_orders = order_submissions.clone();
    let app = Router::new().fallback(move |request: Request<Body>| {
        let observed_orders = observed_orders.clone();
        async move {
            if request.method() == axum::http::Method::POST
                && request.uri().path() == "/fapi/v1/order"
            {
                observed_orders.fetch_add(1, Ordering::SeqCst);
            }
            if request.method() == axum::http::Method::GET
                && request.uri().path() == "/fapi/v1/openOrders"
            {
                return (StatusCode::OK, "[]");
            }
            std::future::pending::<(StatusCode, &'static str)>().await
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let directory = std::env::temp_dir().join(format!(
        "runtime-prepare-interrupted-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("bot.db");
    let mut config = AppConfig {
        pairs: ["SOLUSDC", "BTCUSDT", "ETHUSDT"]
            .map(|symbol| TradingPairConfig::new(symbol.into()))
            .to_vec(),
        ..Default::default()
    };
    config.exchange.dry_run = false;
    config.exchange.api_key = "test-key".into();
    config.exchange.api_secret = "test-secret".into();
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_config(&config).unwrap();
    for symbol in ["SOLUSDC", "BTCUSDT", "ETHUSDT"] {
        db.save_pair_status(symbol, TradingMode::Live, BotStatus::Running)
            .unwrap();
    }
    let (interrupt_tx, interrupt_rx) = watch::channel(false);
    let (hook_tx, mut hook_rx) = mpsc::channel(1);
    let startup_db = db.clone();
    let startup = tokio::spawn(async move {
        TradingRuntime::start_with_test_exchange_and_prepare_hook(
            &config,
            startup_db,
            url,
            interrupt_rx,
            hook_tx,
        )
        .await
    });
    for expected in ["SOLUSDC", "BTCUSDT"] {
        let (symbol, ack) = tokio::time::timeout(Duration::from_secs(5), hook_rx.recv())
            .await
            .unwrap()
            .expect("startup ended before reaching prepare hook");
        assert_eq!(symbol, expected);
        if expected == "BTCUSDT" {
            interrupt_tx.send(true).unwrap();
        }
        ack.send(()).unwrap();
    }
    let result = tokio::time::timeout(Duration::from_secs(10), startup)
        .await
        .expect("interrupted startup cleanup timed out")
        .unwrap();
    assert!(result.is_err());
    assert!(
        hook_rx.try_recv().is_err(),
        "unstarted pair reached prepare hook"
    );
    assert_eq!(order_submissions.load(Ordering::SeqCst), 0);
    let connection = rusqlite::Connection::open(&path).unwrap();
    for symbol in ["SOLUSDC", "BTCUSDT"] {
        assert_eq!(
            db.load_pair_status(symbol, TradingMode::Live).unwrap(),
            Some(BotStatus::Paused)
        );
        let dirty: i64 = connection
            .query_row(
                "SELECT dirty FROM pair_execution_guard WHERE symbol = ?1 AND mode = 'LIVE'",
                [symbol],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(dirty, 0, "{symbol} guard was not cleared after cleanup");
    }
    assert_eq!(
        db.load_pair_status("ETHUSDT", TradingMode::Live).unwrap(),
        Some(BotStatus::Running)
    );
    let unstarted_guards: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM pair_execution_guard WHERE symbol = 'ETHUSDT'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(unstarted_guards, 0);
    drop(connection);
    drop(db);
    server.abort();
    std::fs::remove_dir_all(directory).unwrap();
}

async fn wait_for_runtime_initialization(runtime: &TradingRuntime) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if runtime.states().iter().all(|state| {
                state
                    .global_position_initialized
                    .load(std::sync::atomic::Ordering::SeqCst)
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("runtime initialization timed out");
    for state in runtime.states() {
        assert!(state
            .global_position_ready
            .load(std::sync::atomic::Ordering::SeqCst));
    }
}

#[tokio::test]
async fn restart_restores_each_pair_after_clean_shutdown() {
    // Paper mode tolerates market-data outages; the local endpoint guarantees that
    // startup never contacts Binance and no market price can trigger paper orders.
    let app = Router::new().fallback(|| async { StatusCode::SERVICE_UNAVAILABLE });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let directory = std::env::temp_dir().join(format!("runtime-restart-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("bot.db");
    let config = AppConfig {
        pairs: ["SOLUSDC", "BTCUSDT"]
            .map(|symbol| TradingPairConfig::new(symbol.into()))
            .to_vec(),
        ..Default::default()
    };
    assert!(config.exchange.dry_run);
    {
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_config(&config).unwrap();
        let mut runtime = TradingRuntime::start_with_test_exchange(&config, db, url.clone())
            .await
            .unwrap();
        wait_for_runtime_initialization(&runtime).await;
        let states = runtime.states();
        states[0].resume_trading().await.unwrap();
        states[1].pause_trading().await.unwrap();
        drop(states);
        runtime.shutdown(Duration::from_secs(10)).await.unwrap();
    }
    {
        let db = Arc::new(Database::open(&path).unwrap());
        assert_eq!(
            db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
            Some(BotStatus::Running)
        );
        assert_eq!(
            db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
            Some(BotStatus::Paused)
        );
        let mut runtime = TradingRuntime::start_with_test_exchange(&config, db, url)
            .await
            .unwrap();
        wait_for_runtime_initialization(&runtime).await;
        let states = runtime.states();
        assert_eq!(*states[0].status.read().await, BotStatus::Running);
        assert_eq!(*states[1].status.read().await, BotStatus::Paused);
        drop(states);
        runtime.shutdown(Duration::from_secs(10)).await.unwrap();
    }
    server.abort();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn failed_runtime_shutdown_keeps_running_pairs_paused_after_reopen() {
    let directory =
        std::env::temp_dir().join(format!("runtime-failed-shutdown-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("bot.db");
    {
        let db = Arc::new(Database::open(&path).unwrap());
        let mut runtime = fixture_with_database(db, true, false);
        for state in runtime.states() {
            state
                .global_position_initialized
                .store(true, std::sync::atomic::Ordering::SeqCst);
            state
                .global_position_ready
                .store(true, std::sync::atomic::Ordering::SeqCst);
            state.resume_trading().await.unwrap();
        }
        assert!(runtime.shutdown(Duration::from_secs(2)).await.is_err());
    }
    {
        let db = Database::open(&path).unwrap();
        for symbol in ["SOLUSDC", "BTCUSDT"] {
            assert_eq!(
                db.load_pair_status(symbol, TradingMode::Paper).unwrap(),
                Some(BotStatus::Paused)
            );
        }
    }
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn manual_pause_before_recovery_commit_remains_paused_after_reopen() {
    for pause_before_commit in [true, false] {
        let directory =
            std::env::temp_dir().join(format!("runtime-pause-order-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("bot.db");
        {
            let db = Arc::new(Database::open(&path).unwrap());
            let (tx, _rx) = mpsc::channel(1);
            let config = AppConfig::default();
            db.save_config(&config).unwrap();
            let state = AppState::new(config, db, tx);
            state
                .global_position_initialized
                .store(true, std::sync::atomic::Ordering::SeqCst);
            state
                .global_position_ready
                .store(true, std::sync::atomic::Ordering::SeqCst);
            state.resume_trading().await.unwrap();
            state.pause_for_shutdown().await.unwrap();
            if pause_before_commit {
                state.pause_trading().await.unwrap();
            }
            state.commit_shutdown_recovery().await.unwrap();
            if !pause_before_commit {
                assert!(state.pause_trading().await.is_err());
            }
        }
        {
            let db = Database::open(&path).unwrap();
            assert_eq!(
                db.load_bot_status().unwrap(),
                Some(if pause_before_commit {
                    BotStatus::Paused
                } else {
                    BotStatus::Running
                })
            );
        }
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[tokio::test]
async fn shutdown_during_initialization_does_not_restore_running_status() {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let entered = Arc::new(std::sync::Mutex::new(Some(entered_tx)));
    let app = Router::new().fallback(move || {
        let entered = entered.clone();
        async move {
            if let Some(sender) = entered.lock().unwrap().take() {
                let _ = sender.send(());
            }
            std::future::pending::<StatusCode>().await
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let directory =
        std::env::temp_dir().join(format!("runtime-interrupted-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("bot.db");
    let config = AppConfig::default();
    {
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_config(&config).unwrap();
        db.save_bot_status(BotStatus::Running).unwrap();
        let mut runtime = TradingRuntime::start_with_test_exchange(&config, db, url)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .unwrap()
            .unwrap();
        runtime.shutdown(Duration::from_secs(5)).await.unwrap();
    }
    {
        let db = Database::open(&path).unwrap();
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    }
    server.abort();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn concurrent_safety_pause_prevents_shutdown_recovery() {
    let directory =
        std::env::temp_dir().join(format!("runtime-concurrent-pause-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("bot.db");
    {
        let db = Arc::new(Database::open(&path).unwrap());
        let config = AppConfig::default();
        db.save_config(&config).unwrap();
        let lock = Arc::new(Mutex::new(()));
        let (tx, mut rx) = mpsc::channel(1);
        let state = AppState::new(config.clone(), db.clone(), tx);
        state
            .global_position_initialized
            .store(true, std::sync::atomic::Ordering::SeqCst);
        state
            .global_position_ready
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let (received_tx, received_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let engine = tokio::spawn(async move {
            let BotControlAction::Shutdown { reply, .. } = rx.recv().await.unwrap() else {
                panic!("expected shutdown")
            };
            received_tx.send(()).unwrap();
            release_rx.await.unwrap();
            reply.send(Ok(())).unwrap();
        });
        let (shutdown_tx, _) = watch::channel(false);
        let pair = PairTask {
            state: state.clone(),
            engine,
            engine_joined: false,
            recovery_failed: false,
            shutdown_tx,
            background: vec![],
        };
        let mut runtime = TradingRuntime::from_pairs(
            vec![pair],
            db,
            lock,
            config.server.clone(),
            GlobalConfig::from_app(&config),
            None,
        );
        state.resume_trading().await.unwrap();
        let shutdown = tokio::spawn(async move { runtime.shutdown(Duration::from_secs(5)).await });
        received_rx.await.unwrap();
        state
            .pause_trading_with_reason("模拟并发风控暂停")
            .await
            .unwrap();
        release_tx.send(()).unwrap();
        shutdown.await.unwrap().unwrap();
    }
    {
        let db = Database::open(&path).unwrap();
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    }
    std::fs::remove_dir_all(directory).unwrap();
}
