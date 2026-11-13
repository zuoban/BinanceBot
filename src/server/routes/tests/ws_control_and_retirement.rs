use super::super::control::{
    control_action, dispatch_prepared_control, prepare_control, PendingControlPause,
};
use super::super::ws::send_until_retired;
use super::super::*;
use super::auth_state;
use crate::config::AppConfig;
use crate::db::Database;
use crate::types::{BotControlAction, BotStatus, GridDirection, TradingMode};
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
                expected_direction: None,
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
                expected_direction: None,
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
                expected_direction: None,
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
        let _ = server.await;
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
        let _ = server.await;
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
                expected_direction: None,
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
    socket
        .send(Message::Text(
            r#"{"action":"unexpected_secret_payload"}"#.into(),
        ))
        .await
        .unwrap();
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
    let mut unknown_action_error = false;
    let mut resume_error = false;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !parse_error || !unknown_action_error || !resume_error {
            let frame = socket.next().await.unwrap().unwrap();
            let raw = frame.to_text().unwrap();
            assert!(!raw.contains("unexpected_secret_payload"), "{raw}");
            assert!(!raw.contains("not json"), "{raw}");
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) {
                if value["type"] == "control" && value["success"] == false {
                    if value["action"].is_null() && value["message"] == "无效的控制请求" {
                        parse_error = true;
                    }
                    if value["action"].is_null() && value["message"] == "未知操作指令" {
                        unknown_action_error = true;
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
    let _ = server.await;
}

#[tokio::test]
async fn socket_reverse_dispatches_and_reports_validation_and_completion() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

    let db = Arc::new(Database::open(":memory:").unwrap());
    db.create_session("reverse-ws-token").unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    let state = AppState::new(AppConfig::default(), db, tx);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = create_router(state);
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut request = format!("ws://{address}/ws").into_client_request().unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        "auth, token.reverse-ws-token".parse().unwrap(),
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    socket.next().await.unwrap().unwrap();

    socket
        .send(Message::Text(
            r#"{"action":"reverse","expected_mode":"PAPER"}"#.into(),
        ))
        .await
        .unwrap();
    let invalid_frame = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let frame = socket.next().await.unwrap().unwrap();
            let value: serde_json::Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
            if value["type"] == "control" {
                break value;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(invalid_frame["action"], "reverse");
    assert_eq!(invalid_frame["success"], false);
    assert_eq!(invalid_frame["message"], "反手交易需要提供确认时的交易方向");

    socket
        .send(Message::Text(
            r#"{"action":"reverse","expected_mode":"PAPER","expected_direction":"LONG"}"#.into(),
        ))
        .await
        .unwrap();
    let dispatched = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    match dispatched {
        BotControlAction::Reverse {
            expected_mode,
            expected_direction,
            reply,
        } => {
            assert_eq!(expected_mode, TradingMode::Paper);
            assert_eq!(expected_direction, GridDirection::Long);
            reply.send(Ok(())).unwrap();
        }
        other => panic!("unexpected action: {:?}", other),
    }

    let completed_frame = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let frame = socket.next().await.unwrap().unwrap();
            let value: serde_json::Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
            if value["type"] == "control" {
                break value;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(completed_frame["action"], "reverse");
    assert_eq!(completed_frame["success"], true);
    assert_eq!(
        completed_frame["data"],
        "Action reverse triggered successfully"
    );

    server.abort();
    let _ = server.await;
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
