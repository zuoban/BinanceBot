use super::config_probes::{apply_config_payload, mask_api_key_preview, pair_config_version};
use super::control::{
    control_action, dispatch_prepared_control, prepare_control, PendingControlPause,
};
use super::exchange_preview::{
    fetch_exchange_symbol_preview, fetch_exchange_symbols, ExchangePreviewService,
};
use super::probes::{resolve_exchange_probe, PROBE_COOLDOWN};
use super::ws::{send_until_retired, ws_origin_allowed};
use super::*;
use crate::config::AppConfig;
use crate::db::Database;
use crate::exchange::BinanceFuturesClient;
use crate::types::{
    BotControlAction, BotStatus, OverviewSnapshot, TradingMode, UpdateConfigPayload,
};
use axum::extract::ws::Message;
use futures_util::{SinkExt, StreamExt};

#[test]
fn probe_gate_allows_one_call_per_cooldown_and_reports_the_wait() {
    let gate = ProbeGate::new();
    let start = std::time::Instant::now();
    assert_eq!(gate.enter(start), Ok(()));
    assert_eq!(
        gate.enter(start + std::time::Duration::from_millis(400)),
        Err(3)
    );
    assert_eq!(
        gate.enter(start + PROBE_COOLDOWN - std::time::Duration::from_millis(10)),
        Err(1)
    );
    assert_eq!(gate.enter(start + PROBE_COOLDOWN), Ok(()));
}

#[test]
fn exchange_probe_rejects_malformed_api_keys() {
    let saved = AppConfig::default().exchange;
    for bad_key in ["key with space", "key\ncontrol", "密钥非ASCII"] {
        let payload = serde_json::from_value(serde_json::json!({
            "api_key": bad_key,
            "api_secret": "valid-secret",
        }))
        .unwrap();
        let error = resolve_exchange_probe(&saved, payload).unwrap_err();
        assert!(error.contains("API Key 格式无效"), "{bad_key}: {error}");
    }
    let valid_payload = serde_json::from_value(serde_json::json!({
        "api_key": "validKey123",
        "api_secret": "validSecret456",
    }))
    .unwrap();
    let resolved = resolve_exchange_probe(&saved, valid_payload).unwrap();
    assert_eq!(resolved.api_key, "validKey123");
}

fn test_admin_auth() -> Option<Arc<crate::auth::AdminAuth>> {
    Some(Arc::new(
        crate::auth::AdminAuth::from_password("test-password").unwrap(),
    ))
}

fn auth_state(
    config: AppConfig,
    db: Arc<Database>,
    action_tx: tokio::sync::mpsc::Sender<BotControlAction>,
) -> Arc<AppState> {
    AppState::new_with_admin_auth(config, db, action_tx, test_admin_auth())
}

fn auth_pair_state(
    config: AppConfig,
    db: Arc<Database>,
    action_tx: tokio::sync::mpsc::Sender<BotControlAction>,
    trade_lock: Arc<tokio::sync::Mutex<()>>,
) -> Arc<AppState> {
    AppState::new_for_pair_with_admin_auth(config, db, action_tx, trade_lock, test_admin_auth())
}

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
    let router = create_managed_router(handle).await;

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
async fn body_limits_reject_oversized_login_and_protected_payloads() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("body-limit-token").unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let state = auth_state(AppConfig::default(), db, tx);
    let connect_info =
        axum::extract::ConnectInfo(std::net::SocketAddr::from(([127, 0, 0, 1], 12345)));
    let router = create_router(state).layer(Extension(connect_info));

    let oversized_login = serde_json::json!({
        "password": "x".repeat(5 * 1024),
    })
    .to_string();
    let login_res = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(oversized_login))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login_res.status(), StatusCode::PAYLOAD_TOO_LARGE);

    // Authenticated API routes allow >4 KiB up to 64 KiB, then reject >64 KiB.
    let medium_control = serde_json::json!({
        "action": format!("unknown_{}", "a".repeat(8 * 1024)),
    })
    .to_string();
    let medium_res = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/api/control")
                .header("authorization", "Bearer body-limit-token")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(medium_control))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(medium_res.status(), StatusCode::OK);

    let oversized_control = serde_json::json!({
        "action": format!("unknown_{}", "a".repeat(70 * 1024)),
    })
    .to_string();
    let oversized_res = router
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/api/control")
                .header("authorization", "Bearer body-limit-token")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(oversized_control))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(oversized_res.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn config_view_omits_tick_size_until_symbol_rules_match_pair_symbol() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _) = tokio::sync::mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db, tx);
    let initial = get_config_handler(State(state.clone())).await;
    assert_eq!(initial.0.data.unwrap().tick_size, None);
    {
        let mut rules = state.rules.write().await;
        rules.symbol = "SOLUSDC".into();
        rules.tick_size = rust_decimal_macros::dec!(0.05);
    }
    let loaded = get_config_handler(State(state)).await.0.data.unwrap();
    assert_eq!(loaded.tick_size, Some(rust_decimal_macros::dec!(0.05)));
    assert_eq!(loaded.step_size, Some(rust_decimal_macros::dec!(0.01)));
    assert_eq!(loaded.min_qty, Some(rust_decimal_macros::dec!(0.01)));
    assert_eq!(loaded.min_notional, Some(rust_decimal_macros::dec!(5.0)));
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
}

#[tokio::test]
async fn symbols_consumer_filters_sorts_and_reports_upstream_errors() {
    use axum::{routing::get, Json, Router};
    use serde_json::json;

    let app = Router::new().route(
        "/fapi/v1/exchangeInfo",
        get(|| async {
            Json(json!({"symbols": [
                {"symbol":"ZUSDT", "status":"TRADING", "pricePrecision":2, "quantityPrecision":2, "filters":[]},
                {"symbol":"BUSDT", "status":"BREAK", "pricePrecision":2, "quantityPrecision":2, "filters":[]},
                {"symbol":"AUSDT", "status":"TRADING", "pricePrecision":2, "quantityPrecision":2, "filters":[]}
            ]}))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = BinanceFuturesClient::with_base_url(&AppConfig::default().exchange, url);
    let (status, Json(response)) = fetch_exchange_symbols(&client).await;
    assert_eq!(status, StatusCode::OK);
    assert!(response.success);
    assert_eq!(response.data.unwrap(), vec!["AUSDT", "ZUSDT"]);
    assert!(response.message.is_none());
    server.abort();

    let app = Router::new().route(
        "/fapi/v1/exchangeInfo",
        get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = BinanceFuturesClient::with_base_url(&AppConfig::default().exchange, url);
    let (status, Json(response)) = fetch_exchange_symbols(&client).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(!response.success);
    assert!(response.data.is_none());
    assert!(response.message.is_some());
    server.abort();
}

#[tokio::test]
async fn symbol_preview_returns_mark_price_and_rules_and_validates_inputs() {
    let app = Router::new()
        .route(
            "/fapi/v1/exchangeInfo",
            get(|| async {
                Json(serde_json::json!({
                    "symbols": [{
                        "symbol": "SOLUSDC",
                        "status": "TRADING",
                        "baseAsset": "SOL",
                        "quoteAsset": "USDC",
                        "pricePrecision": 2,
                        "quantityPrecision": 2,
                        "filters": [
                            {"filterType": "PRICE_FILTER", "minPrice": "0.10", "maxPrice": "100000.00", "tickSize": "0.01"},
                            {"filterType": "LOT_SIZE", "minQty": "0.01", "maxQty": "100000.00", "stepSize": "0.01"},
                            {"filterType": "MIN_NOTIONAL", "notional": "5.0"}
                        ]
                    }]
                }))
            }),
        )
        .route(
            "/fapi/v1/ticker/price",
            get(|| async {
                Json(serde_json::json!({
                    "symbol": "SOLUSDC",
                    "price": "142.40",
                    "time": 1700000000000i64
                }))
            }),
        )
        .route(
            "/fapi/v1/premiumIndex",
            get(|| async {
                Json(serde_json::json!({
                    "symbol": "SOLUSDC",
                    "markPrice": "142.35",
                    "indexPrice": "142.30",
                    "lastFundingRate": "0.0001",
                    "nextFundingTime": 1700000000000i64,
                    "time": 1700000000000i64
                }))
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = BinanceFuturesClient::with_base_url(&AppConfig::default().exchange, url);
    let (status, Json(response)) = fetch_exchange_symbol_preview(&client, "SOLUSDC").await;
    assert_eq!(status, StatusCode::OK);
    let preview = response.data.expect("preview data");
    assert_eq!(preview.symbol, "SOLUSDC");
    assert_eq!(preview.last_price, Some(rust_decimal_macros::dec!(142.40)));
    assert_eq!(preview.mark_price, Some(rust_decimal_macros::dec!(142.35)));
    assert_eq!(preview.tick_size, rust_decimal_macros::dec!(0.01));
    assert_eq!(preview.step_size, rust_decimal_macros::dec!(0.01));
    assert_eq!(preview.min_qty, rust_decimal_macros::dec!(0.01));
    assert_eq!(preview.min_notional, rust_decimal_macros::dec!(5.0));
    server.abort();

    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("symbols-test-token").unwrap();
    let (tx, _) = tokio::sync::mpsc::channel(1);
    let router = create_router(auth_state(AppConfig::default(), db, tx));
    let unauth = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/exchange/symbol-preview?environment=mainnet&symbol=SOLUSDC")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

    for uri in [
        "/api/exchange/symbol-preview?environment=testnet&symbol=SOLUSDC",
        "/api/exchange/symbol-preview?environment=mainnet&symbol=",
        "/api/exchange/symbol-preview?environment=mainnet&symbol=SOL-USDC",
        "/api/exchange/symbol-preview?environment=mainnet&symbol=AB",
        "/api/exchange/symbol-preview?environment=mainnet&symbol=ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567",
    ] {
        let bad = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
                    .header("Authorization", "Bearer symbols-test-token")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bad.status(), StatusCode::BAD_REQUEST, "{uri}");
    }
}

#[tokio::test]
async fn symbols_route_requires_auth_and_rejects_invalid_environment() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("symbols-test-token").unwrap();
    let (tx, _) = tokio::sync::mpsc::channel(1);
    let router = create_router(auth_state(AppConfig::default(), db, tx));
    let response = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/exchange/symbols?environment=mainnet")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    for uri in [
        "/api/exchange/symbols",
        "/api/exchange/symbols?environment=testnet",
        "/api/exchange/symbols?environment=invalid",
        "/api/exchange/symbols?environment=mainnet&environment=testnet",
    ] {
        let response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
                    .header("Authorization", "Bearer symbols-test-token")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = axum::body::to_bytes(response.into_body(), 1_000_000)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["success"], false);
        assert!(body["data"].is_null());
        assert!(body["message"].is_string());
    }
}

struct BlockedSocketSink(Option<tokio::sync::oneshot::Sender<()>>);

impl futures_util::Sink<Message> for BlockedSocketSink {
    type Error = ();

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        if let Some(started) = self.0.take() {
            let _ = started.send(());
        }
        std::task::Poll::Pending
    }

    fn start_send(self: std::pin::Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
        unreachable!("blocked sink cannot accept a message")
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Pending
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Pending
    }
}

#[tokio::test]
async fn retirement_interrupts_blocked_snapshot_and_broadcast_sends() {
    for message in ["snapshot", "broadcast"] {
        let (retire, mut retired) = tokio::sync::watch::channel(false);
        let (started, waiting) = tokio::sync::oneshot::channel();
        let mut sender = BlockedSocketSink(Some(started));
        let send = tokio::spawn(async move {
            send_until_retired(&mut sender, Message::Text(message.into()), &mut retired)
                .await
                .is_none()
        });
        waiting.await.expect("send must reach the blocked sink");
        retire.send_replace(true);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(3), send)
                .await
                .expect("retirement must interrupt a blocked send")
                .unwrap()
        );
    }
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

#[test]
fn account_routes_can_be_constructed_without_a_tokio_runtime() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _) = tokio::sync::mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db, tx);
    let _single = create_router(state.clone());
    let _fixed = create_multi_router(vec![state]);
}

#[tokio::test]
async fn account_sampler_shares_frames_cancels_idle_collection_and_restarts() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _) = tokio::sync::mpsc::channel(1);
    let trade_lock = Arc::new(tokio::sync::Mutex::new(()));
    let state = AppState::new_for_pair(AppConfig::default(), db, tx, trade_lock.clone());
    let hub = AccountHub::new(AccountSource::Fixed(Arc::new(vec![state.clone()])));
    assert!(hub.sampler.lock().await.is_none());
    let mut first = hub.subscribe().await;
    let second = hub.subscribe().await;
    tokio::time::timeout(std::time::Duration::from_secs(2), first.changed())
        .await
        .unwrap()
        .unwrap();
    let frame = first.borrow_and_update().clone().unwrap();
    assert!(Arc::ptr_eq(&frame, second.borrow().as_ref().unwrap()));
    let decoded: serde_json::Value = serde_json::from_str(&frame).unwrap();
    assert!(decoded["data"]["generated_at"].is_string());
    drop(first);
    drop(second);

    // A 0 -> 1 reconnect before the sampler observes closed() must reuse the
    // existing channel rather than spawning a duplicate sampler task.
    let existing = hub.sampler.lock().await.clone().unwrap();
    assert_eq!(existing.receiver_count(), 0);
    let reused = hub.subscribe().await;
    assert!(existing.same_channel(&hub.sampler.lock().await.clone().unwrap()));
    assert_eq!(existing.receiver_count(), 1);
    drop(existing);
    drop(reused);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while hub.sampler.lock().await.is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    // Force collection to wait after taking the commit lock, then remove
    // its only subscriber. Cancellation must release that lock promptly.
    let config = state.config.write().await;
    let pending = hub.subscribe().await;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if trade_lock.try_lock().is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(pending);
    let commit = tokio::time::timeout(std::time::Duration::from_secs(2), trade_lock.lock())
        .await
        .unwrap();
    drop(commit);
    drop(config);

    state.position.write().await.unrealized_pnl = rust_decimal_macros::dec!(42);
    let mut restarted = hub.subscribe().await;
    tokio::time::timeout(std::time::Duration::from_secs(2), restarted.changed())
        .await
        .unwrap()
        .unwrap();
    let decoded: serde_json::Value =
        serde_json::from_str(restarted.borrow().as_ref().unwrap()).unwrap();
    assert_eq!(
        decoded["data"]["pairs"][0]["snapshot"]["position"]["unrealized_pnl"],
        "42"
    );
}

#[tokio::test]
async fn account_stream_authenticates_pushes_live_balances_and_rejects_revoked_sessions() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _) = tokio::sync::mpsc::channel(1);
    let mut config = AppConfig::default();
    config.exchange.api_key = "account-stream-private-key".into();
    config.exchange.api_secret = "account-stream-private-secret".into();
    let state = auth_state(config.clone(), db.clone(), tx.clone());
    let scoped = auth_pair_state(
        config,
        db.clone(),
        tx,
        Arc::new(tokio::sync::Mutex::new(())),
    );
    for (router, state) in [
        (create_router(state.clone()), state),
        (create_multi_router(vec![scoped.clone()]), scoped),
    ] {
        db.create_session("account-stream-token").unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let url = format!("ws://{address}/api/accounts/ws");
        assert!(tokio_tungstenite::connect_async(&url).await.is_err());
        let mut request = url.into_client_request().unwrap();
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            "auth, token.account-stream-token".parse().unwrap(),
        );
        let (mut socket, response) = tokio_tungstenite::connect_async(request).await.unwrap();
        assert_eq!(response.headers()["Sec-WebSocket-Protocol"], "auth");
        let first = tokio::time::timeout(std::time::Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!first
            .to_text()
            .unwrap()
            .contains("account-stream-private-key"));
        assert!(!first
            .to_text()
            .unwrap()
            .contains("account-stream-private-secret"));
        assert!(!first.to_text().unwrap().contains("account-stream-token"));
        let first: serde_json::Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
        assert_eq!(first["type"], "accounts");
        assert_eq!(first["data"]["pairs"].as_array().unwrap().len(), 1);
        let snapshot = &first["data"]["pairs"][0]["snapshot"];
        assert_eq!(snapshot.as_object().unwrap().len(), 7);
        assert!(snapshot["ticker"]["mark_update_time"].is_string());
        assert!(snapshot["ticker"]["mark_price"].is_string());
        assert!(snapshot["position"]["size"].is_string());
        state.account.write().await.available_balance = rust_decimal_macros::dec!(1234);
        let updated = tokio::time::timeout(std::time::Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let updated: serde_json::Value = serde_json::from_str(updated.to_text().unwrap()).unwrap();
        assert_eq!(
            updated["data"]["pairs"][0]["snapshot"]["account"]["available_balance"],
            "1234"
        );
        db.delete_session("account-stream-token").unwrap();
        let closed = tokio::time::timeout(std::time::Duration::from_secs(3), socket.next())
            .await
            .unwrap();
        assert!(!matches!(
            closed,
            Some(Ok(tokio_tungstenite::tungstenite::Message::Text(_)))
        ));
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn revoked_socket_commands_waiting_for_capacity_are_not_submitted() {
    for name in ["resume", "rebalance"] {
        let db = Arc::new(Database::open(":memory:").unwrap());
        db.create_session("revoked-control-token").unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        tx.send(BotControlAction::GlobalRiskCancel).await.unwrap();
        let state = auth_state(AppConfig::default(), db.clone(), tx);
        let action = control_action(
            &ControlRequest {
                action: name.into(),
                expected_mode: Some(TradingMode::Paper),
            },
            state.pause_epoch(),
        )
        .unwrap();
        let request = tokio::spawn(dispatch_prepared_control(
            state,
            action,
            None,
            None,
            None,
            Some("revoked-control-token".into()),
        ));
        tokio::task::yield_now().await;
        db.delete_session("revoked-control-token").unwrap();
        assert!(matches!(
            rx.recv().await,
            Some(BotControlAction::GlobalRiskCancel)
        ));
        let response = tokio::time::timeout(std::time::Duration::from_secs(3), request)
            .await
            .unwrap()
            .unwrap();
        assert!(!response.0.success);
        assert!(rx.try_recv().is_err());
    }
}

#[tokio::test]
async fn pending_persisted_pause_blocks_other_clients_until_queued() {
    for name in ["pause", "cancel_all"] {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db, tx);
        let pause = control_action(
            &ControlRequest {
                action: name.into(),
                expected_mode: None,
            },
            state.pause_epoch(),
        )
        .unwrap();
        let pending = PendingControlPause::new(&state, &pause.0);
        prepare_control(&state, &pause.0).await.unwrap();
        let resume = control_action(
            &ControlRequest {
                action: "resume".into(),
                expected_mode: Some(TradingMode::Paper),
            },
            state.pause_epoch(),
        )
        .unwrap();
        let response =
            dispatch_prepared_control(state.clone(), resume, None, None, None, None).await;
        assert!(!response.0.success);
        assert!(rx.try_recv().is_err());
        let response =
            dispatch_prepared_control(state.clone(), pause, pending, None, None, None).await;
        assert!(response.0.success);
        assert!(matches!(
            rx.recv().await,
            Some(
                BotControlAction::PauseAlreadyPersisted
                    | BotControlAction::CancelAllAlreadyPersisted
            )
        ));
        assert_eq!(*state.status.read().await, BotStatus::Paused);
    }
}

#[tokio::test]
async fn socket_pause_is_processed_while_engine_reply_is_pending() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
    for action in ["resume", "rebalance"] {
        let db = Arc::new(Database::open(":memory:").unwrap());
        db.create_session("pause-test-token").unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let state = AppState::new(AppConfig::default(), db, tx);
        state.resume_trading().await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = create_router(state.clone());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let mut request = format!("ws://{address}/ws").into_client_request().unwrap();
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            "auth, token.pause-test-token".parse().unwrap(),
        );
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        socket.next().await.unwrap().unwrap();
        socket
            .send(Message::Text(format!(
                r#"{{"action":"{action}","expected_mode":"PAPER"}}"#
            )))
            .await
            .unwrap();
        let first = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let reply = match first {
            BotControlAction::Resume { reply, .. } | BotControlAction::Rebalance { reply, .. } => {
                reply
            }
            other => panic!("unexpected action: {:?}", other),
        };
        assert!(state.socket_control_lock.try_write().is_ok());
        socket
            .send(Message::Text(r#"{"action":"pause"}"#.into()))
            .await
            .unwrap();
        let paused = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(paused, BotControlAction::PauseAlreadyPersisted));
        assert_eq!(*state.status.read().await, BotStatus::Paused);
        drop(reply);
        server.abort();
    }
}

#[tokio::test]
async fn socket_disconnect_and_revocation_keep_accepted_pause_and_cancel_queued() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
    for action in ["pause", "cancel_all"] {
        let db = Arc::new(Database::open(":memory:").unwrap());
        db.create_session("disconnect-test-token").unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        tx.send(BotControlAction::GlobalRiskCancel).await.unwrap();
        let state = auth_state(AppConfig::default(), db.clone(), tx);
        state.resume_trading().await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = create_router(state.clone());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let mut request = format!("ws://{address}/ws").into_client_request().unwrap();
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            "auth, token.disconnect-test-token".parse().unwrap(),
        );
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        socket.next().await.unwrap().unwrap();
        socket
            .send(Message::Text(format!(r#"{{"action":"{action}"}}"#)))
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while *state.status.read().await != BotStatus::Paused {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        socket.close(None).await.unwrap();
        db.delete_session("disconnect-test-token").unwrap();
        drop(socket);
        tokio::task::yield_now().await;
        assert!(matches!(
            rx.recv().await,
            Some(BotControlAction::GlobalRiskCancel)
        ));
        let pending = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            (action, pending),
            ("pause", BotControlAction::PauseAlreadyPersisted)
                | ("cancel_all", BotControlAction::CancelAllAlreadyPersisted)
        ));
        server.abort();
    }
}

#[tokio::test]
async fn retirement_releases_control_waiting_for_queue_capacity() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    tx.send(BotControlAction::GlobalRiskCancel).await.unwrap();
    let state = AppState::new(AppConfig::default(), db, tx);
    let request_state = state.clone();
    let request = tokio::spawn(async move {
        post_control_handler(
            State(request_state),
            Json(ControlRequest {
                action: "rebalance".into(),
                expected_mode: Some(TradingMode::Paper),
            }),
        )
        .await
    });
    tokio::task::yield_now().await;
    state.retire();
    let response = tokio::time::timeout(std::time::Duration::from_secs(3), request)
        .await
        .unwrap()
        .unwrap();
    assert!(!response.0.success);
}

#[tokio::test]
async fn socket_orders_pause_before_resume_and_reports_failures() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("ordering-test-token").unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    let state = AppState::new(AppConfig::default(), db, tx);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = create_router(state.clone());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut request = format!("ws://{address}/ws").into_client_request().unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        "auth, token.ordering-test-token".parse().unwrap(),
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    socket.next().await.unwrap().unwrap();
    let original_epoch = state.pause_epoch();
    socket
        .send(Message::Text(r#"{"action":"pause"}"#.into()))
        .await
        .unwrap();
    socket
        .send(Message::Text(
            r#"{"action":"resume","expected_mode":"PAPER"}"#.into(),
        ))
        .await
        .unwrap();
    socket.send(Message::Text("not json".into())).await.unwrap();
    let first = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(first, BotControlAction::PauseAlreadyPersisted));
    let second = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let reply = match second {
        BotControlAction::Resume {
            expected_epoch,
            reply,
            ..
        } => {
            assert!(expected_epoch > original_epoch);
            assert_eq!(expected_epoch, state.pause_epoch());
            reply
        }
        other => panic!("unexpected action: {:?}", other),
    };
    reply.send(Err("恢复被拒绝".into())).unwrap();
    let mut parse_error = false;
    let mut resume_error = false;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !parse_error || !resume_error {
            let frame = socket.next().await.unwrap().unwrap();
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(frame.to_text().unwrap()) {
                if value["type"] == "control" && value["success"] == false {
                    if value["action"].is_null() {
                        parse_error = true;
                    }
                    if value["action"] == "resume" && value["message"] == "恢复被拒绝" {
                        resume_error = true;
                    }
                }
            }
        }
    })
    .await
    .unwrap();
    server.abort();
}

#[tokio::test]
async fn retired_state_closes_connected_pair_socket() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("retirement-test-token").unwrap();
    let (tx, _) = tokio::sync::mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db, tx);
    let router = create_router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut request = format!("ws://{address}/ws").into_client_request().unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        "auth, token.retirement-test-token".parse().unwrap(),
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    let initial = tokio::time::timeout(std::time::Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let snapshot: serde_json::Value = serde_json::from_str(initial.to_text().unwrap()).unwrap();
    assert_eq!(snapshot["type"], "snapshot");
    let (mut accounts, _) =
        tokio_tungstenite::connect_async(format!("ws://{address}/api/accounts/ws"))
            .await
            .unwrap();
    let account = tokio::time::timeout(std::time::Duration::from_secs(3), accounts.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let account: serde_json::Value = serde_json::from_str(account.to_text().unwrap()).unwrap();
    assert_eq!(account["type"], "accounts");

    state.retire();
    let closed = tokio::time::timeout(std::time::Duration::from_secs(3), socket.next())
        .await
        .expect("retired state must close its socket");
    assert!(matches!(
        closed,
        None | Some(Ok(Message::Close(_))) | Some(Err(_))
    ));
    let closed = tokio::time::timeout(std::time::Duration::from_secs(3), accounts.next())
        .await
        .expect("retirement must also close the account stream");
    assert!(matches!(
        closed,
        None | Some(Ok(Message::Close(_))) | Some(Err(_))
    ));
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn already_retired_state_does_not_send_socket_snapshot() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("retirement-test-token").unwrap();
    let (tx, _) = tokio::sync::mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db, tx);
    state.retire();
    let router = create_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut request = format!("ws://{address}/ws").into_client_request().unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        "auth, token.retirement-test-token".parse().unwrap(),
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    let closed = tokio::time::timeout(std::time::Duration::from_secs(3), socket.next())
        .await
        .expect("retired state must close its socket");
    assert!(matches!(
        closed,
        None | Some(Ok(Message::Close(_))) | Some(Err(_))
    ));
    server.abort();
    let _ = server.await;
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

#[test]
fn pair_update_preserves_optional_settings_and_handles_explicit_clears() {
    use rust_decimal_macros::dec;

    let mut config = AppConfig::default();
    config.exchange.api_key = "key".into();
    config.exchange.api_secret = "secret".into();
    config.telegram.bot_token = "token".into();
    config.telegram.chat_id = "chat".into();
    config.grid.min_price = Some(dec!(1));
    config.grid.max_price = Some(dec!(100));
    let mut payload = serde_json::json!({
        "symbol": "SOLUSDC", "grid_interval": "0.1", "order_amount_usdc": "100",
        "buy_window": 5, "sell_window": 5,
        "api_key": " ", "api_secret": " ", "telegram_bot_token": " "
    });
    let updated = apply_config_payload(
        config.clone(),
        serde_json::from_value(payload.clone()).unwrap(),
    )
    .unwrap();
    assert_eq!(updated.exchange.api_key, "key");
    assert_eq!(updated.exchange.api_secret, "secret");
    assert_eq!(updated.telegram.bot_token, "token");
    assert_eq!(updated.telegram.chat_id, "chat");
    assert_eq!(updated.grid.min_price, None);
    assert_eq!(updated.grid.max_price, None);
    assert_eq!(updated.grid.max_position_usdc, None);

    payload["min_price"] = serde_json::Value::Null;
    payload["max_price"] = serde_json::Value::Null;
    payload["max_position_usdc"] = serde_json::Value::Null;
    payload["clear_api_credentials"] = serde_json::json!(true);
    payload["clear_telegram_credentials"] = serde_json::json!(true);
    payload["api_key"] = serde_json::json!("replacement");
    payload["telegram_bot_token"] = serde_json::json!("replacement");
    let cleared = apply_config_payload(config, serde_json::from_value(payload).unwrap()).unwrap();
    assert!(cleared.exchange.api_key.is_empty() && cleared.exchange.api_secret.is_empty());
    assert!(cleared.telegram.bot_token.is_empty() && cleared.telegram.chat_id.is_empty());
    assert_eq!(cleared.grid.min_price, None);
    assert_eq!(cleared.grid.max_price, None);
    assert_eq!(cleared.grid.max_position_usdc, None);
}

#[test]
fn retired_testnet_request_is_rejected_instead_of_trading_live() {
    let payload: UpdateConfigPayload = serde_json::from_value(serde_json::json!({
        "symbol": "SOLUSDC", "grid_interval": "0.1", "order_amount_usdc": "100",
        "buy_window": 5, "sell_window": 5, "is_testnet": true
    }))
    .unwrap();
    let error = apply_config_payload(AppConfig::default(), payload).unwrap_err();
    assert!(error.to_string().contains("测试网模式已移除"));
}

#[test]
fn retired_dry_run_request_is_rejected() {
    let payload: UpdateConfigPayload = serde_json::from_value(serde_json::json!({
        "symbol": "SOLUSDC", "grid_interval": "0.1", "order_amount_usdc": "100",
        "buy_window": 5, "sell_window": 5, "dry_run": true
    }))
    .unwrap();
    let error = apply_config_payload(AppConfig::default(), payload).unwrap_err();
    assert!(error.to_string().contains("API Key"));
}

#[tokio::test]
async fn pair_update_rejects_missing_and_stale_versions_before_dispatch() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, mut action_rx) = tokio::sync::mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db.clone(), action_tx);
    let mut payload = serde_json::json!({
        "symbol": "SOLUSDC", "grid_interval": "0.1", "order_amount_usdc": "100",
        "buy_window": 5, "sell_window": 5
    });
    for version in [None, Some(""), Some("   "), Some("stale")] {
        if let Some(version) = version {
            payload["baseline_version"] = serde_json::json!(version);
        }
        let (status, response) = post_config_handler(
            State(state.clone()),
            Json(serde_json::from_value(payload.clone()).unwrap()),
        )
        .await;
        assert_eq!(
            status,
            if version == Some("stale") {
                StatusCode::CONFLICT
            } else {
                StatusCode::BAD_REQUEST
            }
        );
        assert!(!response.0.success);
        assert!(action_rx.try_recv().is_err());
        assert!(db.load_config().unwrap().is_none());
    }
}

#[tokio::test]
async fn short_direction_is_saved_returned_and_preserved_for_legacy_clients() {
    use crate::types::GridDirection;
    let mut config = AppConfig::default();
    let payload = serde_json::json!({
        "symbol": "SOLUSDC", "grid_interval": "0.1", "order_amount_usdc": "100",
        "buy_window": 5, "sell_window": 5, "direction": "SHORT"
    });
    config =
        apply_config_payload(config, serde_json::from_value(payload.clone()).unwrap()).unwrap();
    assert_eq!(config.grid.direction, GridDirection::Short);
    let mut legacy = payload.clone();
    legacy.as_object_mut().unwrap().remove("direction");
    config = apply_config_payload(config, serde_json::from_value(legacy).unwrap()).unwrap();
    assert_eq!(config.grid.direction, GridDirection::Short);
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&config).unwrap();
    let saved = db.load_config().unwrap().unwrap();
    assert_eq!(saved.grid.direction, GridDirection::Short);
    let (tx, _) = tokio::sync::mpsc::channel(1);
    let state = AppState::new(saved, db, tx);
    assert_eq!(
        get_config_handler(State(state))
            .await
            .0
            .data
            .unwrap()
            .direction,
        GridDirection::Short
    );
    let mut invalid = payload;
    invalid["direction"] = serde_json::json!("BOTH");
    assert!(serde_json::from_value::<UpdateConfigPayload>(invalid).is_err());
}

#[tokio::test]
async fn max_position_pause_is_saved_returned_and_preserved_for_legacy_clients() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, mut action_rx) = tokio::sync::mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db.clone(), action_tx);
    let engine_state = state.clone();
    let engine = tokio::spawn(async move {
        while let Some(BotControlAction::UpdateConfig { config, reply }) = action_rx.recv().await {
            engine_state.db.save_config(&config).unwrap();
            *engine_state.config.write().await = *config;
            reply.send(Ok(())).unwrap();
        }
    });
    let mut payload = serde_json::json!({
        "symbol": "SOLUSDC",
        "grid_interval": "0.1",
        "order_amount_usdc": "100",
        "buy_window": 5,
        "sell_window": 5,
        "pause_on_max_position": true
    });
    payload["baseline_version"] = serde_json::json!(pair_config_version(
        &*state.config.read().await,
        state.config_instance
    ));
    let response = post_config_handler(
        State(state.clone()),
        Json(serde_json::from_value(payload.clone()).unwrap()),
    )
    .await
    .1;
    assert!(!response.0.success);
    assert!(db.load_config().unwrap().is_none());

    payload["max_position_usdc"] = serde_json::json!("1000");
    for (field, expected) in [(Some(true), true), (None, true), (Some(false), false)] {
        if let Some(enabled) = field {
            payload["pause_on_max_position"] = serde_json::json!(enabled);
        } else {
            payload
                .as_object_mut()
                .unwrap()
                .remove("pause_on_max_position");
        }
        payload["baseline_version"] = serde_json::json!(pair_config_version(
            &*state.config.read().await,
            state.config_instance
        ));
        let response = post_config_handler(
            State(state.clone()),
            Json(serde_json::from_value(payload.clone()).unwrap()),
        )
        .await
        .1;
        assert!(response.0.success);
        assert_eq!(response.0.data.unwrap().pause_on_max_position, expected);
        let response = get_config_handler(State(state.clone())).await;
        assert_eq!(response.0.data.unwrap().pause_on_max_position, expected);
        assert_eq!(
            db.load_config()
                .unwrap()
                .unwrap()
                .grid
                .pause_on_max_position,
            expected
        );
    }
    engine.abort();
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

#[test]
fn mask_api_key_preview_counts_characters_instead_of_utf8_bytes() {
    assert_eq!(mask_api_key_preview(""), "未配置");
    assert_eq!(mask_api_key_preview("   "), "未配置");
    assert_eq!(mask_api_key_preview("abcd1234"), "****");
    // 4 CJK characters = 12 UTF-8 bytes (> 8 bytes, <= 8 chars): must stay masked.
    assert_eq!(mask_api_key_preview("密钥测试"), "****");
    assert_eq!(mask_api_key_preview("一二三四五六七八"), "****");
    assert_eq!(
        mask_api_key_preview("一二三四五六七八九"),
        "一二三四...六七八九"
    );
    assert_eq!(mask_api_key_preview("abcdefghijkl"), "abcd...ijkl");
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
async fn websocket_rejects_cross_origin_and_accepts_same_origin_or_cli() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _) = tokio::sync::mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db, tx);
    let router = create_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    for path in ["/ws", "/api/accounts/ws"] {
        for bad_origin in ["https://evil.example", "null", "http://127.0.0.1:1"] {
            let mut req = format!("ws://{address}{path}")
                .into_client_request()
                .unwrap();
            req.headers_mut()
                .insert("Sec-WebSocket-Protocol", "auth".parse().unwrap());
            req.headers_mut()
                .insert("Origin", bad_origin.parse().unwrap());
            let err = tokio_tungstenite::connect_async(req).await.unwrap_err();
            match err {
                tokio_tungstenite::tungstenite::Error::Http(response) => {
                    assert_eq!(response.status(), StatusCode::FORBIDDEN);
                }
                other => panic!("expected HTTP 403 rejection, got {other:?}"),
            }
        }

        let mut same_origin = format!("ws://{address}{path}")
            .into_client_request()
            .unwrap();
        same_origin
            .headers_mut()
            .insert("Sec-WebSocket-Protocol", "auth".parse().unwrap());
        same_origin
            .headers_mut()
            .insert("Origin", format!("http://{address}").parse().unwrap());
        let (mut socket, _) = tokio_tungstenite::connect_async(same_origin).await.unwrap();
        let _ = socket.close(None).await;
    }
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn managed_accounts_ws_survives_removing_initial_pair() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let mut config = AppConfig::default();
    config.pairs = ["SOLUSDC", "BTCUSDT"]
        .map(|symbol| crate::config::TradingPairConfig::new(symbol.into()))
        .to_vec();
    db.save_config(&config).unwrap();
    db.save_pair_status(
        "SOLUSDC",
        crate::types::TradingMode::Paper,
        crate::types::BotStatus::Paused,
    )
    .unwrap();
    db.save_pair_status(
        "BTCUSDT",
        crate::types::TradingMode::Paper,
        crate::types::BotStatus::Paused,
    )
    .unwrap();
    let mut runtime = crate::runtime::TradingRuntime::start_with_test_exchange(
        &config,
        db,
        "http://127.0.0.1:1".into(),
    )
    .await
    .unwrap();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone()).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let (mut accounts_ws, _) =
        tokio_tungstenite::connect_async(format!("ws://{address}/api/accounts/ws"))
            .await
            .unwrap();
    let first_msg = tokio::time::timeout(std::time::Duration::from_secs(3), accounts_ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let first_json: serde_json::Value = serde_json::from_str(first_msg.to_text().unwrap()).unwrap();
    assert_eq!(first_json["data"]["pairs"].as_array().unwrap().len(), 2);

    let removed = handle.states.write().await.remove(0);
    assert_eq!(removed.pair_scope(), Some("SOLUSDC"));
    removed.retire();

    let updated = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let msg = accounts_ws
                .next()
                .await
                .expect("accounts stream must remain open after retiring first pair")
                .expect("accounts frame must succeed");
            let json: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
            let pairs = json["data"]["pairs"].as_array().unwrap();
            if pairs.len() == 1 {
                break pairs[0]["snapshot"]["symbol"].as_str().unwrap().to_owned();
            }
        }
    })
    .await
    .expect("accounts stream must publish single remaining pair");
    assert_eq!(updated, "BTCUSDT");

    handle.request_shutdown();
    let closed = tokio::time::timeout(std::time::Duration::from_secs(3), accounts_ws.next())
        .await
        .expect("runtime shutdown must close managed accounts stream");
    assert!(matches!(
        closed,
        None | Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) | Some(Err(_))
    ));

    server.abort();
    let _ = server.await;
    let _ = runtime.shutdown(std::time::Duration::from_secs(2)).await;
}

#[tokio::test]
async fn cold_token_lookups_are_bounded_and_malformed_tokens_skip_sqlite() {
    use axum::body::{to_bytes, Body};
    use axum::http::Request;

    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("hot-valid-token").unwrap();
    db.create_session("cold-valid-token").unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let state = auth_state(AppConfig::default(), db.clone(), tx);
    let app = create_router(state.clone());

    // Warm "hot-valid-token" into the in-memory session cache.
    assert_eq!(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri("/api/status")
                    .header("Authorization", "Bearer hot-valid-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let permits: Vec<_> = (0..16)
        .map(|_| state.auth.try_acquire_session_lookup_permit().unwrap())
        .collect();
    assert!(state.auth.try_acquire_session_lookup_permit().is_none());

    // Hot cached tokens still succeed without acquiring a cold lookup permit.
    assert_eq!(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri("/api/status")
                    .header("Authorization", "Bearer hot-valid-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // Cold tokens return 503 when all 16 cold lookup permits are busy.
    assert_eq!(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri("/api/status")
                    .header("Authorization", "Bearer cold-valid-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri("/api/auth/status")
                    .header("Authorization", "Bearer cold-valid-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );

    // Oversized (>128) or non-ASCII-graphic tokens are rejected before touching AuthContext.
    let oversized = "a".repeat(129);
    for bad in [oversized.as_str(), "bad token with space"] {
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/api/status")
                        .header("Authorization", format!("Bearer {bad}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let status_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/auth/status")
                    .header("Authorization", format!("Bearer {bad}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status_res.status(), StatusCode::OK);
        let bytes = to_bytes(status_res.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["data"]["authenticated"], false);
    }

    drop(permits);
    assert_eq!(
        app.oneshot(
            Request::builder()
                .uri("/api/status")
                .header("Authorization", "Bearer cold-valid-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn routers_attach_security_headers_without_touching_websocket_upgrades() {
    use axum::body::Body;
    use axum::http::Request;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let db = Arc::new(Database::open(":memory:").unwrap());
    let mut config = AppConfig::default();
    config.ensure_pair_list().unwrap();
    db.save_config(&config).unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let state = AppState::new(config.clone(), db.clone(), tx);
    let mut runtime = crate::runtime::TradingRuntime::start_with_test_exchange(
        &config,
        db,
        "http://127.0.0.1:1".into(),
    )
    .await
    .unwrap();
    let managed = create_managed_router(runtime.handle()).await;
    for router in [
        create_router(state.clone()),
        create_multi_router(vec![state.clone()]),
        create_multi_router(Vec::new()),
        managed,
    ] {
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/auth/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let headers = response.headers();
        assert_eq!(headers["Cache-Control"], "no-store");
        assert_eq!(headers["X-Content-Type-Options"], "nosniff");
        assert_eq!(headers["X-Frame-Options"], "DENY");
        assert_eq!(headers["Referrer-Policy"], "same-origin");
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, create_router(state)).await.unwrap();
    });
    let mut ws_req = format!("ws://{address}/ws").into_client_request().unwrap();
    ws_req
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "auth".parse().unwrap());
    let (_socket, upgrade_res) = tokio_tungstenite::connect_async(ws_req).await.unwrap();
    assert_eq!(upgrade_res.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert!(!upgrade_res.headers().contains_key("X-Frame-Options"));
    server.abort();
    let _ = runtime.shutdown(std::time::Duration::from_secs(2)).await;
}

#[tokio::test]
async fn shared_proxy_ip_successful_login_preserves_prior_failed_attempts_and_disabled_auth_is_isolated(
) {
    use axum::{body::Body, http::Request};
    let unconfigured_db = Arc::new(Database::open(":memory:").unwrap());
    let (unconfigured_tx, _unconfigured_rx) = tokio::sync::mpsc::channel(1);
    let unconfigured_state = AppState::new(AppConfig::default(), unconfigured_db, unconfigured_tx);
    let unconfigured_app = create_router(unconfigured_state.clone()).layer(axum::Extension(
        axum::extract::ConnectInfo(
            "198.51.100.10:4000"
                .parse::<std::net::SocketAddr>()
                .unwrap(),
        ),
    ));
    let disabled_probe = Request::builder()
        .method("POST")
        .uri("/api/auth/login")
        .header("Content-Type", "application/json")
        .body(Body::from(r#"{"password":"probe"}"#))
        .unwrap();
    assert_eq!(
        unconfigured_app
            .oneshot(disabled_probe)
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    {
        let failures = unconfigured_state.auth.login_failures.lock().await;
        assert!(failures.by_source.is_empty());
        assert!(failures.global.is_empty());
    }

    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let state = AppState::new_with_admin_auth(
        AppConfig::default(),
        db,
        tx,
        Some(Arc::new(
            crate::auth::AdminAuth::from_password("admin-secret-pw").unwrap(),
        )),
    );
    let proxy_addr: std::net::SocketAddr = "198.51.100.20:5000".parse().unwrap();
    let proxy_ip = proxy_addr.ip();
    let app =
        create_router(state.clone()).layer(axum::Extension(axum::extract::ConnectInfo(proxy_addr)));
    let login_req = |pw: &str| {
        Request::builder()
            .method("POST")
            .uri("/api/auth/login")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::json!({"password": pw}).to_string()))
            .unwrap()
    };

    // 5 failed attempts behind shared proxy IP
    for _ in 0..5 {
        let res = app.clone().oneshot(login_req("wrong-pw")).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }
    assert_eq!(
        state.auth.login_failures.lock().await.by_source[&proxy_ip].len(),
        5
    );

    // 1 valid login behind the same shared proxy IP rolls back ONLY its own attempt
    let ok_res = app
        .clone()
        .oneshot(login_req("admin-secret-pw"))
        .await
        .unwrap();
    assert_eq!(ok_res.status(), StatusCode::OK);
    {
        let failures = state.auth.login_failures.lock().await;
        assert_eq!(failures.by_source[&proxy_ip].len(), 5);
        assert_eq!(failures.global.len(), 5);
    }

    // 5 more failed attempts hit the 10-attempt source cap (proving prior 5 were not wiped)
    for _ in 0..5 {
        let res = app.clone().oneshot(login_req("wrong-pw")).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }
    let blocked_res = app.oneshot(login_req("wrong-pw")).await.unwrap();
    assert_eq!(blocked_res.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[test]
fn ws_origin_allowed_enforces_forwarded_proto_and_host_syntax() {
    use axum::http::{HeaderMap, HeaderValue};

    let build_headers = |host: &str, origin: Option<&str>, proto: Option<&str>| {
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_str(host).unwrap());
        if let Some(origin) = origin {
            headers.insert("origin", HeaderValue::from_str(origin).unwrap());
        }
        if let Some(proto) = proto {
            headers.insert("x-forwarded-proto", HeaderValue::from_str(proto).unwrap());
        }
        headers
    };

    assert!(ws_origin_allowed(&build_headers(
        "bot.example.com",
        Some("https://bot.example.com"),
        Some("https"),
    )));
    assert!(ws_origin_allowed(&build_headers(
        "bot.example.com",
        Some("https://bot.example.com"),
        Some("HTTPS, http"),
    )));
    assert!(!ws_origin_allowed(&build_headers(
        "bot.example.com",
        Some("http://bot.example.com"),
        Some("https"),
    )));
    assert!(!ws_origin_allowed(&build_headers(
        "bot.example.com",
        Some("https://bot.example.com"),
        Some("http"),
    )));
    assert!(!ws_origin_allowed(&build_headers(
        "bot.example.com",
        Some("https://bot.example.com"),
        Some("ws"),
    )));
    assert!(!ws_origin_allowed(&build_headers(
        "bot.example.com:80",
        Some("https://bot.example.com"),
        None,
    )));

    for bad_host in [
        "",
        "evil@localhost:8080",
        "localhost:8080/path",
        "localhost:8080?x=1",
        "localhost:8080#frag",
        "localhost:8080\\evil",
        "local host:8080",
    ] {
        assert!(
            !ws_origin_allowed(&build_headers(
                bad_host,
                Some("http://localhost:8080"),
                None
            )),
            "expected host {bad_host:?} to be rejected"
        );
    }
}

#[tokio::test]
async fn websocket_lagged_receiver_resyncs_snapshot_without_disconnecting() {
    use rust_decimal_macros::dec;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db, tx);
    let router = create_router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let mut req = format!("ws://{address}/ws").into_client_request().unwrap();
    req.headers_mut()
        .insert("Sec-WebSocket-Protocol", "auth".parse().unwrap());
    let (mut socket, _) = tokio_tungstenite::connect_async(req).await.unwrap();

    let initial = tokio::time::timeout(std::time::Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let initial_json: serde_json::Value = serde_json::from_str(initial.to_text().unwrap()).unwrap();
    assert_eq!(initial_json["type"], "snapshot");

    state.ticker.write().await.last_price = dec!(777.77);
    for index in 0..150 {
        let payload = serde_json::json!({
            "type": "log",
            "data": {"message": format!("burst-{index}")}
        });
        state
            .ws_broadcast_tx
            .send(Arc::<str>::from(payload.to_string()))
            .unwrap();
    }
    let sentinel = serde_json::json!({
        "type": "log",
        "data": {"message": "after-lag-resync"}
    });
    state
        .ws_broadcast_tx
        .send(Arc::<str>::from(sentinel.to_string()))
        .unwrap();

    let mut saw_resync_snapshot = false;
    let mut saw_sentinel = false;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !(saw_resync_snapshot && saw_sentinel) {
            let msg = socket
                .next()
                .await
                .expect("socket must remain connected across Lagged resync")
                .expect("frame must succeed");
            if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                if value["type"] == "snapshot"
                    && value["data"]["ticker"]["last_price"] == serde_json::json!(dec!(777.77))
                {
                    saw_resync_snapshot = true;
                }
                if value["type"] == "log" && value["data"]["message"] == "after-lag-resync" {
                    saw_sentinel = true;
                }
            }
        }
    })
    .await
    .unwrap();

    let _ = socket.close(None).await;
    server.abort();
}

#[tokio::test]
async fn exchange_catalog_filters_perpetuals_coalesces_concurrent_loads_and_cools_down_failures() {
    use axum::{routing::get, Json, Router};
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let calls = Arc::new(AtomicUsize::new(0));
    let fail_mode = Arc::new(AtomicBool::new(false));
    let calls_handler = calls.clone();
    let fail_handler = fail_mode.clone();
    let app = Router::new().route(
        "/fapi/v1/exchangeInfo",
        get(move || {
            let calls = calls_handler.clone();
            let fail = fail_handler.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                if fail.load(Ordering::SeqCst) {
                    return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({})));
                }
                (
                    StatusCode::OK,
                    Json(json!({"symbols": [
                        {"symbol": "ETHUSDT", "status": "TRADING", "contractType": "PERPETUAL", "filters": []},
                        {"symbol": "SOLUSDC", "status": "TRADING", "filters": []},
                        {"symbol": "BTCUSDT_261225", "status": "TRADING", "contractType": "CURRENT_QUARTER", "filters": []},
                        {"symbol": "BTCUSDT", "status": "TRADING", "contractType": "CURRENT_QUARTER", "filters": []},
                        {"symbol": "币安人生USDT", "status": "TRADING", "contractType": "PERPETUAL", "filters": []},
                        {"symbol": "BTCETH", "status": "TRADING", "contractType": "PERPETUAL", "filters": []},
                        {"symbol": "XRPUSDT", "status": "BREAK", "contractType": "PERPETUAL", "filters": []}
                    ]})),
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let service = ExchangePreviewService::with_client(BinanceFuturesClient::with_base_url(
        &AppConfig::default().exchange,
        url.clone(),
    ));
    let tasks: Vec<_> = (0..6)
        .map(|_| {
            let svc = service.clone();
            tokio::spawn(async move { svc.load_or_fetch_catalog().await.unwrap().1 })
        })
        .collect();
    for task in tasks {
        assert_eq!(task.await.unwrap(), vec!["ETHUSDT", "SOLUSDC"]);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    fail_mode.store(true, Ordering::SeqCst);
    let failing_service = ExchangePreviewService::with_client(BinanceFuturesClient::with_base_url(
        &AppConfig::default().exchange,
        url,
    ));
    assert!(failing_service.load_or_fetch_catalog().await.is_err());
    assert!(failing_service.load_or_fetch_catalog().await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    server.abort();
}

#[tokio::test]
async fn websocket_upgrade_returns_503_when_lookup_permits_busy_and_logout_permits_are_isolated() {
    use axum::body::Body;
    use axum::http::Request;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let state = AppState::new_with_admin_auth(
        AppConfig::default(),
        db,
        tx,
        Some(Arc::new(
            crate::auth::AdminAuth::from_password("test-password").unwrap(),
        )),
    );
    state.create_session("cold-ws-token-1").await.unwrap();
    state.create_session("cold-ws-token-2").await.unwrap();

    let app = create_router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_app = app.clone();
    let server = tokio::spawn(async move { axum::serve(listener, server_app).await.unwrap() });

    let logout_permits: Vec<_> = (0..4)
        .map(|_| state.auth.try_acquire_logout_permit().unwrap())
        .collect();
    assert!(state.auth.try_acquire_logout_permit().is_none());

    assert_eq!(
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/logout")
                    .header("Authorization", "Bearer cold-ws-token-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );

    let mut ws_ok = format!("ws://{address}/ws").into_client_request().unwrap();
    ws_ok.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        "auth, token.cold-ws-token-1".parse().unwrap(),
    );
    let (mut connected, _) = tokio_tungstenite::connect_async(ws_ok).await.unwrap();
    let _ = connected.close(None).await;
    drop(logout_permits);

    let lookup_permits: Vec<_> = (0..16)
        .map(|_| state.auth.try_acquire_session_lookup_permit().unwrap())
        .collect();
    assert!(state.auth.try_acquire_session_lookup_permit().is_none());

    for path in ["/ws", "/api/accounts/ws"] {
        let mut req = format!("ws://{address}{path}")
            .into_client_request()
            .unwrap();
        req.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            "auth, token.cold-ws-token-2".parse().unwrap(),
        );
        let err = tokio_tungstenite::connect_async(req).await.unwrap_err();
        match err {
            tokio_tungstenite::tungstenite::Error::Http(resp) => {
                assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
            }
            other => panic!("expected HTTP 503 response for {path}, got {other:?}"),
        }
    }

    assert_eq!(
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/logout")
                .header("Authorization", "Bearer cold-ws-token-2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .status(),
        StatusCode::OK
    );

    drop(lookup_permits);
    server.abort();
}

#[test]
fn probe_payload_debug_redacts_secrets_and_error_sanitizer_handles_exchange_errors() {
    let exchange_payload: probes::TestExchangePayload = serde_json::from_value(serde_json::json!({
        "api_key": "probe-secret-key",
        "api_secret": "probe-secret-val",
        "recv_window": 5000
    }))
    .unwrap();
    let telegram_payload: probes::TestTelegramPayload = serde_json::from_value(serde_json::json!({
        "bot_token": "123:probe-bot-secret",
        "chat_id": "456"
    }))
    .unwrap();
    let debug_str = format!("{exchange_payload:?} {telegram_payload:?}");
    assert!(!debug_str.contains("probe-secret-key"));
    assert!(!debug_str.contains("probe-secret-val"));
    assert!(!debug_str.contains("probe-bot-secret"));
    assert!(debug_str.contains("[REDACTED]"));

    let api_err: anyhow::Error = crate::exchange::ExchangeError::ApiError {
        code: -2015,
        msg: "Invalid API-key, IP, or permissions for action.".into(),
    }
    .into();
    assert_eq!(
        probes::sanitized_probe_error("账户验证失败", &api_err),
        "账户验证失败：币安返回错误 (-2015): Invalid API-key, IP, or permissions for action."
    );

    let html_err: anyhow::Error = crate::exchange::ExchangeError::Other(
        "<!DOCTYPE html><html><body>502 Bad Gateway</body></html>".into(),
    )
    .into();
    let sanitized_html = probes::sanitized_probe_error("无法连接币安", &html_err);
    assert!(!sanitized_html.contains("<html"), "{sanitized_html}");
    assert!(
        sanitized_html.contains("远端服务返回异常响应"),
        "{sanitized_html}"
    );
}
