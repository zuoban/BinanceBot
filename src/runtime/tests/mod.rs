use super::*;
use crate::config::{GlobalConfig, TradingPairConfig};
use crate::db::Database;
use crate::types::BotControlAction;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use serde_json::{json, Value};
use tower::ServiceExt;

mod add_pair_validation_and_rollback;
mod early_and_concurrent_shutdown;
mod failed_removal_rebuild;
mod global_config_and_probes;
mod history_cleanup_cancellation;
mod managed_api_and_history;
mod removal_positions_and_guards;
mod startup_and_restart;

pub(super) fn fixture() -> TradingRuntime {
    fixture_with_shutdown_failure(false)
}

pub(super) fn fixture_with_shutdown_failure(fail_shutdown: bool) -> TradingRuntime {
    fixture_with_failures(fail_shutdown, false)
}

pub(super) fn fixture_with_failures(fail_shutdown: bool, fail_global: bool) -> TradingRuntime {
    let db = Arc::new(Database::open(":memory:").unwrap());
    fixture_with_database(db, fail_shutdown, fail_global)
}

pub(super) fn fixture_with_database(
    db: Arc<Database>,
    fail_shutdown: bool,
    fail_global: bool,
) -> TradingRuntime {
    fixture_with_shutdown_observer(db, fail_shutdown, fail_global, None)
}

pub(super) fn fixture_with_shutdown_observer(
    db: Arc<Database>,
    fail_shutdown: bool,
    fail_global: bool,
    observer: Option<mpsc::UnboundedSender<String>>,
) -> TradingRuntime {
    fixture_with_unresponsive_engine(db, fail_shutdown, fail_global, observer, false)
}

pub(super) fn fixture_with_unresponsive_engine(
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
        let mode = strategy.exchange.trading_mode();
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
                        BotControlAction::Reverse { reply, .. } => {
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

pub(super) async fn request(
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

pub(super) fn payload(symbol: &str) -> Value {
    json!({ "symbol": symbol, "grid_interval": "0.1", "order_amount_usdc": "100", "buy_window": 5, "sell_window": 5 })
}

pub(super) fn exchange_info(status: &str, filters: Value) -> Value {
    json!({"symbols": [{
        "symbol": "ETHUSDT", "status": status, "pricePrecision": 2,
        "quantityPrecision": 3, "filters": filters
    }]})
}

pub(super) fn valid_filters() -> Value {
    json!([
        {"filterType":"PRICE_FILTER", "minPrice":"0.01", "maxPrice":"1000000", "tickSize":"0.01"},
        {"filterType":"LOT_SIZE", "minQty":"0.001", "maxQty":"1000000", "stepSize":"0.001"},
        {"filterType":"MIN_NOTIONAL", "notional":"5"}
    ])
}

pub(super) async fn mock_exchange(
    status: StatusCode,
    body: Value,
) -> (String, tokio::task::JoinHandle<()>) {
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

pub(super) async fn versioned_payload(router: &Router, symbol: &str) -> Value {
    let path = format!("/api/pairs/{symbol}/config");
    let (_, current) = request(router, &path, "GET", Value::Null, true).await;
    let mut result = payload(symbol);
    result["baseline_version"] = current["data"]["config_version"].clone();
    result
}

pub(super) async fn seed_paper_account_and_position(
    state: &AppState,
) -> (crate::types::AccountInfo, crate::types::PositionInfo) {
    assert!(state.config.read().await.exchange.is_paper());
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

pub(super) async fn assert_paper_account_and_position_preserved(
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
