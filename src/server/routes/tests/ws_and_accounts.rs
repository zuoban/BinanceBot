use super::super::control::{
    control_action, dispatch_prepared_control, prepare_control, PendingControlPause,
};
use super::super::ws::{send_until_retired, ws_origin_allowed};
use super::super::*;
use super::{auth_pair_state, auth_state};
use crate::config::AppConfig;
use crate::db::Database;
use crate::types::{BotControlAction, BotStatus, TradingMode};
use axum::extract::ws::Message;
use futures_util::{SinkExt, StreamExt};

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
