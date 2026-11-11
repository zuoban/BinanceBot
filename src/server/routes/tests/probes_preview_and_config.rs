use super::super::config_probes::{
    apply_config_payload, mask_api_key_preview, pair_config_version,
};
use super::super::exchange_preview::{
    fetch_exchange_symbol_preview, fetch_exchange_symbols, ExchangePreviewService,
};
use super::super::probes::{resolve_exchange_probe, resolve_telegram_probe, PROBE_COOLDOWN};
use super::super::*;
use super::auth_state;
use crate::config::AppConfig;
use crate::db::Database;
use crate::exchange::BinanceFuturesClient;
use crate::types::{BotControlAction, UpdateConfigPayload};

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
fn exchange_probe_rejects_malformed_api_keys_and_secrets() {
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
    for bad_secret in ["secret with space", "secret\ncontrol", "密钥非ASCII"] {
        let payload = serde_json::from_value(serde_json::json!({
            "api_key": "validKey123",
            "api_secret": bad_secret,
        }))
        .unwrap();
        let error = resolve_exchange_probe(&saved, payload).unwrap_err();
        assert!(
            error.contains("API Secret 格式无效"),
            "{bad_secret}: {error}"
        );
    }
    let valid_payload = serde_json::from_value(serde_json::json!({
        "api_key": "validKey123",
        "api_secret": "validSecret456",
    }))
    .unwrap();
    let resolved = resolve_exchange_probe(&saved, valid_payload).unwrap();
    assert_eq!(resolved.api_key, "validKey123");
}

#[test]
fn exchange_and_telegram_probes_enforce_completeness_and_token_format_with_saved_credentials() {
    let mut saved_exchange = AppConfig::default().exchange;
    saved_exchange.api_key = "validKey123".into();
    saved_exchange.api_secret = "validSecret456".into();
    let saved_blank: probes::TestExchangePayload =
        serde_json::from_value(serde_json::json!({"api_key": "  ", "api_secret": ""})).unwrap();
    let resolved = resolve_exchange_probe(&saved_exchange, saved_blank).unwrap();
    assert_eq!(resolved.api_key, "validKey123");
    assert_eq!(resolved.api_secret, "validSecret456");

    for partial in [
        serde_json::json!({"api_key": "probe-secret-key"}),
        serde_json::json!({"api_secret": "probe-secret-val"}),
        serde_json::json!({"api_key": "probe-secret-key", "api_secret": "   "}),
    ] {
        let payload: probes::TestExchangePayload = serde_json::from_value(partial.clone()).unwrap();
        let error = resolve_exchange_probe(&saved_exchange, payload).unwrap_err();
        assert!(
            error.contains("请同时填写 API Key 和 Secret"),
            "{partial}: {error}"
        );
    }

    let saved_telegram = crate::config::TelegramConfig {
        enabled: true,
        bot_token: "123456:valid-token".into(),
        chat_id: "10001".into(),
    };
    let prefilled_chat: probes::TestTelegramPayload =
        serde_json::from_value(serde_json::json!({"bot_token": "", "chat_id": "10002"})).unwrap();
    let resolved_tg = resolve_telegram_probe(&saved_telegram, prefilled_chat).unwrap();
    assert_eq!(resolved_tg.bot_token, "123456:valid-token");
    assert_eq!(resolved_tg.chat_id, "10002");

    for bad_token in [
        "123:abc/../../getMe",
        "123:abc?x=1",
        "123:abc#f",
        "bad token",
    ] {
        let payload: probes::TestTelegramPayload = serde_json::from_value(serde_json::json!({
            "bot_token": bad_token,
            "chat_id": "10001"
        }))
        .unwrap();
        let err = resolve_telegram_probe(&saved_telegram, payload).unwrap_err();
        assert!(
            err.contains("Telegram Bot Token 格式无效"),
            "{bad_token}: {err}"
        );
    }
    for bad_chat_id in ["10001\n10002", "10001\r", &"9".repeat(129)] {
        let payload: probes::TestTelegramPayload = serde_json::from_value(serde_json::json!({
            "bot_token": "123456:valid-token",
            "chat_id": bad_chat_id
        }))
        .unwrap();
        let err = resolve_telegram_probe(&saved_telegram, payload).unwrap_err();
        assert!(
            err.contains("Telegram Chat ID 格式无效"),
            "{bad_chat_id}: {err}"
        );
    }
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

    let mut contradictory_api = payload.clone();
    contradictory_api["clear_api_credentials"] = serde_json::json!(true);
    contradictory_api["api_key"] = serde_json::json!("replacement");
    let err = apply_config_payload(
        config.clone(),
        serde_json::from_value(contradictory_api).unwrap(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("清除 API 凭证"));

    let mut contradictory_tg = payload.clone();
    contradictory_tg["clear_telegram_credentials"] = serde_json::json!(true);
    contradictory_tg["telegram_bot_token"] = serde_json::json!("replacement");
    let err = apply_config_payload(
        config.clone(),
        serde_json::from_value(contradictory_tg).unwrap(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("清除 Telegram 凭证"));

    payload["clear_api_credentials"] = serde_json::json!(true);
    payload["clear_telegram_credentials"] = serde_json::json!(true);
    payload["api_key"] = serde_json::json!("");
    payload["api_secret"] = serde_json::Value::Null;
    payload["telegram_bot_token"] = serde_json::json!("");
    payload["telegram_chat_id"] = serde_json::Value::Null;
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
async fn single_pair_config_rejects_partial_credential_rotation_with_400_without_mutation() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let mut initial = AppConfig::default();
    initial.exchange.api_key = "validKey123".into();
    initial.exchange.api_secret = "validSecret456".into();
    db.save_config(&initial).unwrap();
    let (action_tx, mut action_rx) = tokio::sync::mpsc::channel(1);
    let state = AppState::new(initial.clone(), db.clone(), action_tx);
    let version = pair_config_version(&*state.config.read().await, state.config_instance);
    for partial in [
        serde_json::json!({"api_key": "probe-secret-key"}),
        serde_json::json!({"api_secret": "probe-secret-val"}),
    ] {
        let mut body = serde_json::json!({
            "baseline_version": version,
            "symbol": "SOLUSDC",
            "grid_interval": "0.1",
            "order_amount_usdc": "100",
            "buy_window": 5,
            "sell_window": 5,
        });
        for (k, v) in partial.as_object().unwrap() {
            body[k] = v.clone();
        }
        let (status, response) = post_config_handler(
            State(state.clone()),
            Json(serde_json::from_value(body).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(!response.0.success);
        assert!(action_rx.try_recv().is_err());
    }
    let saved = db.load_config().unwrap().unwrap();
    assert_eq!(saved.exchange.api_key, "validKey123");
    assert_eq!(saved.exchange.api_secret, "validSecret456");
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
