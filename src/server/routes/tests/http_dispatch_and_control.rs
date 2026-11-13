use super::super::*;
use super::{auth_pair_state, auth_state};
use crate::config::AppConfig;
use crate::db::Database;
use crate::types::{BotControlAction, BotStatus, OverviewSnapshot, TradingMode};

#[tokio::test]
async fn dispatch_pair_preserves_query_strings_for_unscoped_and_scoped_routes() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let mut config = AppConfig::default();
    config.pairs = vec![crate::config::TradingPairConfig::new("SOLUSDC".into())];
    db.save_config(&config).unwrap();
    db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Paused)
        .unwrap();
    let mut runtime = crate::runtime::TradingRuntime::start_with_test_exchange(
        &config,
        db,
        "http://127.0.0.1:1".into(),
    )
    .await
    .unwrap();
    let handle = runtime.handle();
    assert!(
        handle.states.read().await[0]
            .managed_router
            .set(Router::new().route(
                "/trades",
                get(|uri: axum::http::Uri| async move { uri.to_string() }),
            ))
            .is_ok(),
        "managed_router should be unset before first dispatch"
    );
    let router = create_managed_router(handle);

    for (request_uri, expected_dispatched_uri) in [
        (
            "/api/trades?limit=25&symbol=SOLUSDC",
            "/trades?limit=25&symbol=SOLUSDC",
        ),
        (
            "/api/pairs/SOLUSDC/trades?limit=10&mode=PAPER",
            "/trades?limit=10&mode=PAPER",
        ),
        ("/api/trades", "/trades"),
        ("/api/pairs/SOLUSDC/trades", "/trades"),
    ] {
        let response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(request_uri)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{request_uri}");
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        assert_eq!(
            std::str::from_utf8(&body).unwrap(),
            expected_dispatched_uri,
            "{request_uri}"
        );
    }

    let _ = runtime.shutdown(std::time::Duration::from_secs(2)).await;
}

#[tokio::test]
async fn dedicated_metrics_token_is_read_only_for_metrics_and_preserves_admin_session_access() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("admin-session-token").unwrap();
    let mut config = AppConfig::default();
    config.pairs = vec![crate::config::TradingPairConfig::new("SOLUSDC".into())];
    db.save_config(&config).unwrap();
    db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Paused)
        .unwrap();
    let admin_auth = Arc::new(crate::auth::AdminAuth::from_password("test-password").unwrap());
    let mut runtime = crate::runtime::TradingRuntime::start_with_test_exchange_and_admin_auth(
        &config,
        db,
        "http://127.0.0.1:1".into(),
        admin_auth,
    )
    .await
    .unwrap();
    let handle = runtime.handle();
    handle.set_metrics_auth(Arc::new(
        crate::auth::MetricsAuth::from_token("prom-scrape-secret-token-2026").unwrap(),
    ));
    let router = create_managed_router(handle);

    let scrape_metrics = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/metrics")
                .header("authorization", "Bearer prom-scrape-secret-token-2026")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(scrape_metrics.status(), StatusCode::OK);

    for forbidden_uri in ["/api/overview", "/api/pairs", "/api/database/export"] {
        let rejected = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(forbidden_uri)
                    .header("authorization", "Bearer prom-scrape-secret-token-2026")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            rejected.status(),
            StatusCode::UNAUTHORIZED,
            "{forbidden_uri}"
        );
    }

    let admin_metrics = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/metrics")
                .header("authorization", "Bearer admin-session-token")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(admin_metrics.status(), StatusCode::OK);

    let wrong_metrics = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/metrics")
                .header("authorization", "Bearer wrong-scrape-secret-token-2026")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(wrong_metrics.status(), StatusCode::UNAUTHORIZED);

    let _ = runtime.shutdown(std::time::Duration::from_secs(2)).await;

    let open_db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let open_state = AppState::new(AppConfig::default(), open_db, tx);
    open_state.auth.set_metrics_auth(Arc::new(
        crate::auth::MetricsAuth::from_token("metrics-only-secret-token").unwrap(),
    ));
    let single_router = create_router(open_state);
    let anon_metrics = single_router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/metrics")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(anon_metrics.status(), StatusCode::UNAUTHORIZED);
    let ok_metrics = single_router
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/metrics")
                .header("X-Auth-Token", "metrics-only-secret-token")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ok_metrics.status(), StatusCode::OK);
}

#[tokio::test]
async fn control_requires_confirmation_mode_and_propagates_engine_rejections() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, mut action_rx) = tokio::sync::mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db, action_tx);
    for action in ["resume", "rebalance"] {
        for expected_mode in [None, Some(TradingMode::Unknown)] {
            let response = post_control_handler(
                State(state.clone()),
                Json(ControlRequest {
                    action: action.into(),
                    expected_mode,
                    expected_direction: None,
                }),
            )
            .await;
            assert!(!response.0.success);
            assert!(action_rx.try_recv().is_err());
        }
    }
    for action in ["resume", "rebalance"] {
        let request_state = state.clone();
        let request = tokio::spawn(async move {
            post_control_handler(
                State(request_state),
                Json(ControlRequest {
                    action: action.into(),
                    expected_mode: Some(TradingMode::Paper),
                    expected_direction: None,
                }),
            )
            .await
        });
        let reply = match action_rx.recv().await.unwrap() {
            BotControlAction::Resume {
                expected_mode,
                reply,
                ..
            } => {
                assert_eq!(expected_mode, TradingMode::Paper);
                reply
            }
            BotControlAction::Rebalance {
                expected_mode,
                reply,
            } => {
                assert_eq!(expected_mode, TradingMode::Paper);
                reply
            }
            other => panic!("unexpected action: {:?}", other),
        };
        reply.send(Err("操作被拒绝".into())).unwrap();
        let response = request.await.unwrap().0;
        assert!(!response.success);
        assert_eq!(response.message.as_deref(), Some("操作被拒绝"));
    }

    for (expected_mode, expected_direction) in [
        (None, Some(crate::types::GridDirection::Long)),
        (
            Some(TradingMode::Unknown),
            Some(crate::types::GridDirection::Long),
        ),
        (Some(TradingMode::Live), None),
    ] {
        let response = post_control_handler(
            State(state.clone()),
            Json(ControlRequest {
                action: "reverse".into(),
                expected_mode,
                expected_direction,
            }),
        )
        .await;
        assert!(!response.0.success);
        assert!(action_rx.try_recv().is_err());
    }

    let request_state = state.clone();
    let request = tokio::spawn(async move {
        post_control_handler(
            State(request_state),
            Json(ControlRequest {
                action: "reverse".into(),
                expected_mode: Some(TradingMode::Live),
                expected_direction: Some(crate::types::GridDirection::Long),
            }),
        )
        .await
    });
    let reply = match action_rx.recv().await.unwrap() {
        BotControlAction::Reverse {
            expected_mode,
            expected_direction,
            reply,
        } => {
            assert_eq!(expected_mode, TradingMode::Live);
            assert_eq!(expected_direction, crate::types::GridDirection::Long);
            reply
        }
        other => panic!("unexpected action: {:?}", other),
    };
    reply.send(Ok(())).unwrap();
    let response = request.await.unwrap().0;
    assert!(response.success);
}

async fn overview_response(
    router: &Router,
    authenticated: bool,
) -> (StatusCode, serde_json::Value) {
    let mut request = axum::http::Request::builder().uri("/api/overview");
    if authenticated {
        request = request.header("Authorization", "Bearer overview-test-token");
    }
    let response = router
        .clone()
        .oneshot(request.body(axum::body::Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1_000_000)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn overview_authenticates_single_and_multi_pair_routes() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("overview-test-token").unwrap();
    let (tx, _) = tokio::sync::mpsc::channel(1);
    let mut config = AppConfig::default();
    config.exchange.api_key = "overview-private-api-key".into();
    config.exchange.api_secret = "overview-private-api-secret".into();
    config.telegram.bot_token = "overview-private-telegram-token".into();
    config.telegram.chat_id = "overview-private-chat-id".into();
    let state = auth_state(config, db, tx);
    let router = create_router(state);
    assert_eq!(
        overview_response(&router, false).await.0,
        StatusCode::UNAUTHORIZED
    );
    let (status, response) = overview_response(&router, true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["success"], true);
    assert_eq!(response["data"]["pairs"].as_array().unwrap().len(), 1);
    assert!(response["data"]["today_pnl"]["window_start"].is_string());
    assert!(response["data"]["today_pnl"]["window_end"].is_string());
    assert!(response["data"]["today_pnl"]["groups"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(response["data"]["pairs"][0]["scoped"], false);
    assert_eq!(
        response["data"]["pairs"][0]["snapshot"]["symbol"],
        "SOLUSDC"
    );
    let summary = &response["data"]["pairs"][0]["snapshot"];
    for field in [
        "active_orders",
        "recent_trades",
        "recent_logs",
        "hourly_trade_stats",
    ] {
        assert!(
            summary.get(field).is_none(),
            "overview retained heavy field {field}"
        );
    }
    assert!(summary["today_trade_counts"]["buy"].is_u64());
    assert!(response["data"]["recent_trades"]
        .as_array()
        .unwrap()
        .is_empty());
    let serialized = response.to_string();
    for private_value in [
        "overview-private-api-key",
        "overview-private-api-secret",
        "overview-private-telegram-token",
        "overview-private-chat-id",
    ] {
        assert!(!serialized.contains(private_value));
    }
    let summary = &response["data"]["pairs"][0]["snapshot"]["grid_config"];
    for private_field in ["api_key", "api_secret", "telegram", "bot_token", "chat_id"] {
        assert!(summary.get(private_field).is_none());
    }

    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("overview-test-token").unwrap();
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    let mut states = Vec::new();
    for (symbol, paper) in [("SOLUSDC", true), ("BTCUSDT", false)] {
        let mut config = AppConfig::default();
        config.exchange.symbol = symbol.into();
        if !paper {
            config.exchange.api_key = "live-key".into();
            config.exchange.api_secret = "live-secret".into();
        }
        let (tx, _) = tokio::sync::mpsc::channel(1);
        states.push(auth_pair_state(config, db.clone(), tx, lock.clone()));
    }
    let account_time = chrono::Utc::now() - chrono::Duration::minutes(2);
    states[0].ticker.write().await.mark_price = rust_decimal_macros::dec!(123);
    states[1].ticker.write().await.mark_price = rust_decimal_macros::dec!(67890);
    states[1].account.write().await.update_time = account_time;
    states[0].add_log("INFO", "SOL scoped log").await;
    states[1].add_log("WARN", "BTC scoped log").await;
    let router = create_multi_router(states.clone());
    assert_eq!(
        overview_response(&router, false).await.0,
        StatusCode::UNAUTHORIZED
    );
    let (status, response) = overview_response(&router, true).await;
    assert_eq!(status, StatusCode::OK);
    let overview: OverviewSnapshot = serde_json::from_value(response["data"].clone()).unwrap();
    assert_eq!(overview.pairs.len(), 2);
    assert!(overview.pairs.iter().all(|pair| pair.scoped));
    assert_eq!(overview.pairs[0].mode, TradingMode::Paper);
    assert_eq!(overview.pairs[1].mode, TradingMode::Live);
    assert_eq!(overview.pairs[0].snapshot.symbol, "SOLUSDC");
    assert_eq!(overview.pairs[1].snapshot.symbol, "BTCUSDT");
    assert_eq!(
        overview.pairs[0].snapshot.ticker.mark_price,
        rust_decimal_macros::dec!(123)
    );
    assert_eq!(
        overview.pairs[1].snapshot.ticker.mark_price,
        rust_decimal_macros::dec!(67890)
    );
    assert_eq!(overview.pairs[1].snapshot.account.update_time, account_time);
    for pair in response["data"]["pairs"].as_array().unwrap() {
        assert!(pair["snapshot"].get("recent_logs").is_none());
        assert!(pair["snapshot"].get("active_orders").is_none());
    }

    // No WebSocket subscribers are required for live fields to refresh.
    // Only chart queries are cached between overview polls.
    assert_eq!(db.hourly_stats_query_count(), 2);
    states[1].ticker.write().await.mark_price = rust_decimal_macros::dec!(67900);
    states[1].account.write().await.update_time = chrono::Utc::now();
    let (_, response) = overview_response(&router, true).await;
    let refreshed: OverviewSnapshot = serde_json::from_value(response["data"].clone()).unwrap();
    assert_eq!(
        refreshed.pairs[1].snapshot.ticker.mark_price,
        rust_decimal_macros::dec!(67900)
    );
    assert!(refreshed.pairs[1].snapshot.account.update_time > account_time);
    assert_eq!(db.hourly_stats_query_count(), 2);
}

#[tokio::test]
async fn retired_control_cannot_change_recreated_pair() {
    for action in ["pause", "cancel_all"] {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let config = AppConfig::default();
        let trade_lock = Arc::new(tokio::sync::Mutex::new(()));
        let (old_tx, mut old_rx) = tokio::sync::mpsc::channel(1);
        let old = AppState::new_for_pair(config.clone(), db.clone(), old_tx, trade_lock.clone());
        let retirement = old.socket_control_lock.write().await;
        let request_state = old.clone();
        let request = tokio::spawn(async move {
            post_control_handler(
                State(request_state),
                Json(ControlRequest {
                    action: action.into(),
                    expected_mode: None,
                    expected_direction: None,
                }),
            )
            .await
        });
        tokio::task::yield_now().await;
        old.retire();
        drop(retirement);

        let (new_tx, _new_rx) = tokio::sync::mpsc::channel(1);
        let new = AppState::new_for_pair(config.clone(), db.clone(), new_tx, trade_lock);
        new.resume_trading().await.unwrap();
        assert!(!request.await.unwrap().0.success);
        assert!(old_rx.try_recv().is_err());
        assert_eq!(*new.status.read().await, BotStatus::Running);
        assert_eq!(
            db.load_pair_status(&config.exchange.symbol, TradingMode::Paper)
                .unwrap(),
            Some(BotStatus::Running)
        );

        let response = post_control_handler(
            State(old),
            Json(ControlRequest {
                action: action.into(),
                expected_mode: None,
                expected_direction: None,
            }),
        )
        .await;
        assert!(!response.0.success);
        assert!(old_rx.try_recv().is_err());
        assert_eq!(
            db.load_pair_status(&config.exchange.symbol, TradingMode::Paper)
                .unwrap(),
            Some(BotStatus::Running)
        );
    }
}

#[tokio::test]
async fn cancelled_control_finishes_before_retirement_and_recreation() {
    for action in ["pause", "cancel_all"] {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let config = AppConfig::default();
        let trade_lock = Arc::new(tokio::sync::Mutex::new(()));
        let (old_tx, mut old_rx) = tokio::sync::mpsc::channel(1);
        let old = AppState::new_for_pair(config.clone(), db.clone(), old_tx, trade_lock.clone());
        let status = old.status.write().await;
        let request_state = old.clone();
        let request = tokio::spawn(async move {
            post_control_handler(
                State(request_state),
                Json(ControlRequest {
                    action: action.into(),
                    expected_mode: None,
                    expected_direction: None,
                }),
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while old.socket_control_lock.try_write().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        request.abort();
        assert!(matches!(request.await, Err(error) if error.is_cancelled()));
        let retiring = old.clone();
        let retirement = tokio::spawn(async move {
            let _guard = retiring.socket_control_lock.write().await;
            retiring.retire();
        });
        tokio::task::yield_now().await;
        assert!(!retirement.is_finished());
        drop(status);
        retirement.await.unwrap();
        assert!(matches!(
            old_rx.try_recv(),
            Ok(BotControlAction::PauseAlreadyPersisted
                | BotControlAction::CancelAllAlreadyPersisted)
                | Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));

        let (new_tx, _new_rx) = tokio::sync::mpsc::channel(1);
        let new = AppState::new_for_pair(config.clone(), db.clone(), new_tx, trade_lock);
        new.resume_trading().await.unwrap();
        assert_eq!(*new.status.read().await, BotStatus::Running);
        assert_eq!(
            db.load_pair_status(&config.exchange.symbol, TradingMode::Paper)
                .unwrap(),
            Some(BotStatus::Running)
        );
    }
}

#[tokio::test]
async fn pause_updates_status_before_engine_accepts_action() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = tokio::sync::mpsc::channel(1);
    action_tx
        .send(BotControlAction::GlobalRiskCancel)
        .await
        .unwrap();
    let state = AppState::new(AppConfig::default(), db.clone(), action_tx);
    let request_state = state.clone();
    let request = tokio::spawn(async move {
        post_control_handler(
            State(request_state),
            Json(ControlRequest {
                action: "pause".into(),
                expected_mode: None,
                expected_direction: None,
            }),
        )
        .await
    });

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while *state.status.read().await != BotStatus::Paused {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while db.load_bot_status().unwrap() != Some(BotStatus::Paused) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!request.is_finished());
    drop(action_rx);
    assert!(!request.await.unwrap().0.success);
}

#[tokio::test]
async fn get_orders_handler_returns_orders_sorted_by_price_desc() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, _action_rx) = tokio::sync::mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db, action_tx);
    {
        let mut orders = state.active_orders.write().await;
        for (id, price) in [
            ("b", rust_decimal_macros::dec!(100)),
            ("a", rust_decimal_macros::dec!(200)),
            ("c", rust_decimal_macros::dec!(100)),
        ] {
            orders.insert(
                id.into(),
                crate::types::GridOrder {
                    client_order_id: id.into(),
                    order_id: None,
                    symbol: "SOLUSDC".into(),
                    side: crate::types::OrderSide::Buy,
                    price,
                    quantity: rust_decimal_macros::dec!(1),
                    amount_usdc: price,
                    status: crate::types::OrderStatus::New,
                    created_at: chrono::Utc::now(),
                    updated_at: chrono::Utc::now(),
                    grid_level: 0,
                    paired_client_order_id: None,
                    is_take_profit: false,
                    purpose: crate::types::OrderPurpose::Grid,
                    merge_sources: Vec::new(),
                },
            );
        }
    }
    let response = get_orders_handler(State(state)).await.0;
    let ids: Vec<_> = response
        .data
        .unwrap()
        .into_iter()
        .map(|o| o.client_order_id)
        .collect();
    assert_eq!(ids, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn get_logs_handler_and_logs_route_return_newest_first_matching_snapshot() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("logs-test-token").unwrap();
    let (action_tx, _action_rx) = tokio::sync::mpsc::channel(1);
    let state = auth_pair_state(
        AppConfig::default(),
        db,
        action_tx,
        Arc::new(tokio::sync::Mutex::new(())),
    );
    state.add_log("INFO", "oldest").await;
    state.add_log("WARN", "middle").await;
    state.add_log("ERROR", "newest").await;

    let snapshot_messages: Vec<_> = state
        .snapshot()
        .await
        .recent_logs
        .into_iter()
        .map(|entry| entry.message)
        .collect();
    assert_eq!(snapshot_messages, vec!["newest", "middle", "oldest"]);

    let direct_messages: Vec<_> = get_logs_handler(State(state.clone()))
        .await
        .0
        .data
        .unwrap()
        .into_iter()
        .map(|entry| entry.message)
        .collect();
    assert_eq!(direct_messages, snapshot_messages);

    let router = create_multi_router(vec![state]);
    for uri in ["/api/logs", "/api/pairs/SOLUSDC/logs"] {
        let response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
                    .header("Authorization", "Bearer logs-test-token")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        let bytes = axum::body::to_bytes(response.into_body(), 1_000_000)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let messages: Vec<_> = body["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["message"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(messages, snapshot_messages, "{uri}");
    }
}

#[tokio::test]
async fn metrics_endpoint_requires_authentication_and_exposes_prometheus_text() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("metrics-token").unwrap();
    let mut config = AppConfig::default();
    config.pairs = vec![crate::config::TradingPairConfig::new("SOLUSDC".into())];
    db.save_config(&config).unwrap();
    db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Paused)
        .unwrap();
    let admin_auth = Arc::new(crate::auth::AdminAuth::from_password("test-password").unwrap());
    let mut runtime = crate::runtime::TradingRuntime::start_with_test_exchange_and_admin_auth(
        &config,
        db,
        "http://127.0.0.1:1".into(),
        admin_auth,
    )
    .await
    .unwrap();
    let router = create_managed_router(runtime.handle());

    let anonymous = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/metrics")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);

    let authorized = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/metrics")
                .header("authorization", "Bearer metrics-token")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(authorized.status(), StatusCode::OK);
    assert!(authorized.headers()["content-type"]
        .to_str()
        .unwrap()
        .starts_with("text/plain; version=0.0.4"));
    let body = axum::body::to_bytes(authorized.into_body(), 1 << 20)
        .await
        .unwrap();
    let text = std::str::from_utf8(&body).unwrap();
    for name in [
        "binance_bot_sqlite_connection_lock_wait_seconds_count",
        "binance_bot_sqlite_blocking_queue_wait_seconds_count",
        "binance_bot_websocket_reconnects_total{stream=\"market\"}",
        "binance_bot_websocket_reconnects_total{stream=\"user_data\"}",
        "binance_bot_sync_failures_total{kind=\"open_orders\"}",
        "binance_bot_http_requests_total{status=\"4xx\"}",
        "binance_bot_broadcast_lagged_total{channel=\"ticker\"}",
        "binance_bot_broadcast_lagged_total{channel=\"websocket_client\"}",
    ] {
        assert!(text.contains(name), "missing metric {name}");
    }

    let _ = runtime.shutdown(std::time::Duration::from_secs(2)).await;
}
