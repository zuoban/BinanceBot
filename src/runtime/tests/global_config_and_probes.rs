use super::super::*;
use super::{
    exchange_info, fixture, fixture_with_database, fixture_with_failures, mock_exchange, payload,
    request, valid_filters, versioned_payload,
};
use crate::db::Database;
use crate::server::create_managed_router;
use crate::types::BotStatus;
use axum::http::StatusCode;
use serde_json::{json, Value};

#[tokio::test]
async fn global_position_limit_api_persists_validates_and_supports_legacy_updates() {
    let mut runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone());
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
    let router = create_managed_router(handle.clone());
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
    let router = create_managed_router(handle.clone());
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
        // Switching to live mode requires a fresh manual resume.
        assert_eq!(*state.status.read().await, BotStatus::Paused);
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
    let _ = server.await;
}

#[tokio::test]
async fn clearing_credentials_switches_every_pair_to_paper() {
    let mut runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone());
    let manager = tokio::spawn(async move { runtime.run().await });
    let (_, applied) = request(
        &router,
        "/api/global/config",
        "POST",
        json!({
            "api_key":"test-key", "api_secret":"test-secret", "telegram_enabled":true,
            "telegram_bot_token":"test-token", "telegram_chat_id":"123",
            "global_max_position_notional":"10000"
        }),
        true,
    )
    .await;
    assert_eq!(applied["success"], true);
    for state in handle.states.read().await.iter() {
        assert!(!state.config.read().await.exchange.is_paper());
    }
    let (_, rejected) = request(
        &router,
        "/api/global/config",
        "POST",
        json!({"clear_telegram_credentials":true}),
        true,
    )
    .await;
    assert_eq!(rejected["success"], false);
    let (_, cleared) = request(&router, "/api/global/config", "POST", json!({
        "clear_api_credentials":true, "clear_telegram_credentials":true, "telegram_enabled":false
    }), true).await;
    assert_eq!(cleared["success"], true);
    for state in handle.states.read().await.iter() {
        assert!(!state
            .global_cancel_pending
            .load(std::sync::atomic::Ordering::SeqCst));
        assert!(state
            .recent_logs
            .read()
            .await
            .iter()
            .all(|entry| !entry.message.contains("全局风控")));
        let config = state.config.read().await;
        assert!(config.exchange.api_key.is_empty());
        assert!(config.exchange.is_paper());
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
async fn global_config_rejects_partial_credential_rotation_and_contradictory_clears_with_400() {
    let mut runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone());
    let manager = tokio::spawn(async move { runtime.run().await });
    let (status, seeded) = request(
        &router,
        "/api/global/config",
        "POST",
        json!({
            "api_key": "test-key",
            "api_secret": "test-secret",
            "telegram_bot_token": "test-token",
            "telegram_chat_id": "12345"
        }),
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(seeded["success"], true);

    for bad_body in [
        json!({"api_key": "new-key-only"}),
        json!({"api_secret": "new-secret-only"}),
        json!({"clear_api_credentials": true, "api_key": "contradictory-key"}),
        json!({"clear_api_credentials": true, "api_secret": "contradictory-secret"}),
        json!({"clear_telegram_credentials": true, "telegram_bot_token": "contradictory-token"}),
        json!({"clear_telegram_credentials": true, "telegram_chat_id": "99999"}),
        json!({"clear_ntfy_credentials": true, "ntfy_topic": "grid_topic"}),
        json!({"clear_ntfy_credentials": true, "ntfy_token": "tk_contradictory"}),
    ] {
        let (bad_status, body) = request(
            &router,
            "/api/global/config",
            "POST",
            bad_body.clone(),
            true,
        )
        .await;
        assert_eq!(bad_status, StatusCode::BAD_REQUEST, "{bad_body}");
        assert_eq!(body["success"], false, "{bad_body}");
    }

    assert!(handle
        .update_global_config(crate::types::UpdateGlobalConfigPayload {
            api_key: Some("direct-partial-key".into()),
            ..Default::default()
        })
        .await
        .is_err());
    assert!(handle
        .update_global_config(crate::types::UpdateGlobalConfigPayload {
            clear_api_credentials: true,
            api_secret: Some("direct-contradictory-secret".into()),
            ..Default::default()
        })
        .await
        .is_err());

    let saved = handle.db.load_config().unwrap().unwrap();
    assert_eq!(saved.exchange.api_key, "test-key");
    assert_eq!(saved.exchange.api_secret, "test-secret");
    assert_eq!(saved.telegram.bot_token, "test-token");
    assert_eq!(saved.telegram.chat_id, "12345");
    for state in handle.states.read().await.iter() {
        let cfg = state.config.read().await;
        assert_eq!(cfg.exchange.api_key, "test-key");
        assert_eq!(cfg.exchange.api_secret, "test-secret");
    }
    manager.abort();
    let _ = manager.await;
}

#[tokio::test]
async fn global_reconciliation_failure_restores_both_changed_engines_and_saved_globals() {
    let runtime = fixture_with_failures(false, true);
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone());
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
        // Credential changes switch the run mode, which requires a manual resume.
        assert_eq!(*state.status.read().await, BotStatus::Paused);
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
    let dir = crate::test_support::TempTestDir::new("global-commit-failure");
    let path = dir.join("bot.db");
    let db = Arc::new(Database::open(&path).unwrap());
    let mut runtime = fixture_with_database(db.clone(), false, false);
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
    runtime.shutdown(Duration::from_secs(2)).await.unwrap();
    drop(runtime);
    drop(db);
}

#[tokio::test]
async fn concurrent_global_patches_and_pair_updates_preserve_shared_and_independent_settings() {
    let runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone());
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
async fn credential_probes_reject_incomplete_input_without_network_or_persistence() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let before = {
        let runtime = fixture_with_database(db.clone(), false, false);
        serde_json::to_value(runtime.handle().db.load_config().unwrap().unwrap()).unwrap()
    };
    for (path, body, expected) in [
        (
            "/api/global/test-exchange",
            json!({"api_key": "only-a-key"}),
            "请同时填写 API Key 和 Secret",
        ),
        (
            "/api/global/test-exchange",
            json!({"api_key": "k", "api_secret": "s", "recv_window": 0}),
            "请求有效窗口必须在 1-60000 毫秒之间",
        ),
        (
            "/api/global/test-telegram",
            json!({"bot_token": "123:abc"}),
            "请填写 Bot Token 和 Chat ID",
        ),
        (
            "/api/global/test-ntfy",
            json!({"server_url": "https://ntfy.sh"}),
            "请填写 Topic",
        ),
    ] {
        // Each case gets a fresh runtime, so the per-kind cooldown never interferes.
        let runtime = fixture_with_database(db.clone(), false, false);
        let router = create_managed_router(runtime.handle());
        let (status, response) = request(&router, path, "POST", body, true).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response["success"], false, "{path}");
        assert!(
            response["message"].as_str().unwrap().contains(expected),
            "{path}: {response}"
        );
    }
    let after = serde_json::to_value(db.load_config().unwrap().unwrap()).unwrap();
    assert_eq!(after, before);
}

#[tokio::test]
async fn credential_probes_are_throttled_per_kind_and_independently() {
    let runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone());
    let incomplete = json!({"api_key": "only-a-key"});
    let (_, first) = request(
        &router,
        "/api/global/test-exchange",
        "POST",
        incomplete.clone(),
        true,
    )
    .await;
    assert!(first["message"].as_str().unwrap().contains("请同时填写"));
    let (_, second) = request(
        &router,
        "/api/global/test-exchange",
        "POST",
        incomplete,
        true,
    )
    .await;
    assert!(second["message"].as_str().unwrap().contains("测试过于频繁"));
    // A different probe is not blocked by the exchange cooldown.
    let (_, telegram) = request(
        &router,
        "/api/global/test-telegram",
        "POST",
        json!({"bot_token": "123:abc"}),
        true,
    )
    .await;
    assert!(telegram["message"]
        .as_str()
        .unwrap()
        .contains("请填写 Bot Token"));
    let (_, ntfy) = request(&router, "/api/global/test-ntfy", "POST", json!({}), true).await;
    assert!(ntfy["message"].as_str().unwrap().contains("请填写 Topic"));
}
