use super::*;
use rust_decimal_macros::dec;

#[test]
fn combined_streams_are_routed_and_decoded_independently() {
    assert_eq!(
        stream_url("BTCUSDT"),
        "wss://fstream.binance.com/market/stream?streams=btcusdt@ticker/btcusdt@markPrice@1s"
    );
    let now = Utc::now();
    let mut times = EventTimes::default();
    let mark = format!(
        r#"{{"stream":"btcusdt@markPrice@1s","data":{{"e":"markPriceUpdate","E":{},"s":"BTCUSDT","p":"100.5"}}}}"#,
        now.timestamp_millis()
    );
    let update = parse_market_update(&mark, "BTCUSDT", now, &mut times).unwrap();
    assert_eq!(update.mark_price, dec!(100.5));
    assert_eq!(update.last_price, Decimal::ZERO);
    let ticker = format!(
        r#"{{"stream":"btcusdt@ticker","data":{{"e":"24hrTicker","E":{},"s":"BTCUSDT","c":"101","o":"100","h":"102","l":"99","v":"3","q":"300"}}}}"#,
        now.timestamp_millis()
    );
    let update = parse_market_update(&ticker, "BTCUSDT", now, &mut times).unwrap();
    assert_eq!(update.last_price, dec!(101));
    assert_eq!(update.mark_price, Decimal::ZERO);
}

#[test]
fn rejects_wrong_symbol_stream_invalid_price_and_old_or_future_events() {
    let now = Utc::now();
    let mut times = EventTimes::default();
    let event = |stream: &str, symbol: &str, price: &str, ms: i64| {
        format!(
            r#"{{"stream":"{stream}","data":{{"e":"markPriceUpdate","E":{ms},"s":"{symbol}","p":"{price}"}}}}"#
        )
    };
    let ms = now.timestamp_millis();
    assert!(parse_market_update(
        &event("ethusdt@markPrice@1s", "BTCUSDT", "1", ms),
        "BTCUSDT",
        now,
        &mut times
    )
    .is_none());
    assert!(parse_market_update(
        &event("btcusdt@markPrice@1s", "ETHUSDT", "1", ms),
        "BTCUSDT",
        now,
        &mut times
    )
    .is_none());
    assert!(parse_market_update(
        &event("btcusdt@markPrice@1s", "BTCUSDT", "0", ms),
        "BTCUSDT",
        now,
        &mut times
    )
    .is_none());
    assert!(parse_market_update(
        &event("btcusdt@markPrice@1s", "BTCUSDT", "1", ms - 16000),
        "BTCUSDT",
        now,
        &mut times
    )
    .is_none());
    assert!(parse_market_update(
        &event("btcusdt@markPrice@1s", "BTCUSDT", "1", ms + 3000),
        "BTCUSDT",
        now,
        &mut times
    )
    .is_none());
    assert!(parse_market_update(
        &event("btcusdt@markPrice@1s", "BTCUSDT", "1", ms),
        "BTCUSDT",
        now,
        &mut times
    )
    .is_some());
    assert!(parse_market_update(
        &event("btcusdt@markPrice@1s", "BTCUSDT", "2", ms - 1),
        "BTCUSDT",
        now,
        &mut times
    )
    .is_none());
}

#[tokio::test]
async fn reconnects_and_switches_symbol_without_delivering_old_market_updates() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for (index, symbol) in ["BTCUSDT", "BTCUSDT", "ETHUSDT"].iter().enumerate() {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
            if index == 2 {
                let wrong = serde_json::json!({"stream": "btcusdt@markPrice@1s", "data": {"e": "markPriceUpdate", "E": Utc::now().timestamp_millis(), "s": "BTCUSDT", "p": "1"}});
                socket.send(Message::Text(wrong.to_string())).await.unwrap();
            }
            let event = serde_json::json!({"stream": format!("{}@markPrice@1s", symbol.to_lowercase()), "data": {"e": "markPriceUpdate", "E": Utc::now().timestamp_millis(), "s": symbol, "p": "100"}});
            socket.send(Message::Text(event.to_string())).await.unwrap();
            if index == 1 {
                while socket.next().await.is_some() {}
            }
        }
    });
    let (config_tx, config_rx) = watch::channel("BTCUSDT".to_string());
    let (ticker_tx, mut ticker_rx) = broadcast::channel(8);
    let client = tokio::spawn(
        BinanceWsStream::new(config_rx, ticker_tx)
            .run_with_url(move |symbol| format!("ws://{address}/market/stream?symbol={symbol}")),
    );
    async fn receive(receiver: &mut broadcast::Receiver<TickerInfo>) -> TickerInfo {
        tokio::time::timeout(Duration::from_secs(4), receiver.recv())
            .await
            .unwrap()
            .unwrap()
    }
    assert_eq!(receive(&mut ticker_rx).await.symbol, "BTCUSDT");
    assert_eq!(receive(&mut ticker_rx).await.symbol, "BTCUSDT");
    config_tx.send("ETHUSDT".into()).unwrap();
    assert_eq!(receive(&mut ticker_rx).await.symbol, "ETHUSDT");
    assert!(ticker_rx.try_recv().is_err());
    client.abort();
    server.abort();
}

#[tokio::test]
async fn replies_pong_to_ping_and_reconnects_after_idle_read_timeout() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (pong_tx, pong_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        // First connection: send Ping, expect Pong, then stay silent to trigger idle timeout.
        let (socket, _) = listener.accept().await.unwrap();
        let mut first = tokio_tungstenite::accept_async(socket).await.unwrap();
        first
            .send(Message::Ping(b"heartbeat".to_vec()))
            .await
            .unwrap();
        while let Some(Ok(frame)) = first.next().await {
            if let Message::Pong(payload) = frame {
                let _ = pong_tx.send(payload);
                break;
            }
        }
        // Second connection after idle watchdog reconnects: deliver fresh mark price.
        let (socket, _) = listener.accept().await.unwrap();
        let mut second = tokio_tungstenite::accept_async(socket).await.unwrap();
        let event = serde_json::json!({
            "stream": "btcusdt@markPrice@1s",
            "data": {"e": "markPriceUpdate", "E": Utc::now().timestamp_millis(), "s": "BTCUSDT", "p": "105"}
        });
        second.send(Message::Text(event.to_string())).await.unwrap();
        while second.next().await.is_some() {}
    });
    let (_config_tx, config_rx) = watch::channel("BTCUSDT".to_string());
    let (ticker_tx, mut ticker_rx) = broadcast::channel(8);
    let client = tokio::spawn(
        BinanceWsStream::new(config_rx, ticker_tx).run_with_url_and_timeout(
            move |symbol| format!("ws://{address}/market/stream?symbol={symbol}"),
            Duration::from_millis(80),
        ),
    );
    let pong = tokio::time::timeout(Duration::from_secs(2), pong_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pong, b"heartbeat");
    let update = tokio::time::timeout(Duration::from_secs(4), ticker_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(update.mark_price, dec!(105));
    client.abort();
    server.abort();
}

#[test]
fn ticker_extreme_decimal_values_do_not_overflow() {
    let now = Utc::now();
    let mut times = EventTimes::default();
    let frame = format!(
        r#"{{"stream":"btcusdt@ticker","data":{{"e":"24hrTicker","E":{},"s":"BTCUSDT","c":"{}","o":"0.0000000000000000000000000001","h":"{}","l":"1","v":"1","q":"1"}}}}"#,
        now.timestamp_millis(),
        Decimal::MAX,
        Decimal::MAX,
    );
    let update = parse_market_update(&frame, "BTCUSDT", now, &mut times).unwrap();
    assert_eq!(update.last_price, Decimal::MAX);
    assert_eq!(update.change_percent_24h, Decimal::ZERO);
}

#[test]
fn local_observed_time_preserves_event_age_across_clock_skew() {
    let local_now = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
    let exchange_now = local_now + chrono::Duration::seconds(5);
    let event_ms = (exchange_now - chrono::Duration::seconds(12)).timestamp_millis();
    let mut times = EventTimes::default();
    let frame = format!(
        r#"{{"stream":"btcusdt@markPrice@1s","data":{{"e":"markPriceUpdate","E":{event_ms},"s":"BTCUSDT","p":"100"}}}}"#
    );
    let update = parse_market_update(&frame, "BTCUSDT", exchange_now, &mut times).unwrap();
    let translated = local_observed_time(local_now, exchange_now, update.mark_update_time);
    assert_eq!(translated, local_now - chrono::Duration::seconds(12));
}

#[tokio::test]
async fn rejects_oversized_upstream_ws_frames_and_reconnects_cleanly() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut first = tokio_tungstenite::accept_async(socket).await.unwrap();
        let _ = first
            .send(Message::Text("x".repeat(WS_MAX_FRAME_BYTES + 1024)))
            .await;
        let poisoned = serde_json::json!({
            "stream": "btcusdt@markPrice@1s",
            "data": {"e": "markPriceUpdate", "E": Utc::now().timestamp_millis(), "s": "BTCUSDT", "p": "999"}
        });
        let _ = first.send(Message::Text(poisoned.to_string())).await;
        while first.next().await.is_some() {}

        let (socket, _) = listener.accept().await.unwrap();
        let mut second = tokio_tungstenite::accept_async(socket).await.unwrap();
        let valid = serde_json::json!({
            "stream": "btcusdt@markPrice@1s",
            "data": {"e": "markPriceUpdate", "E": Utc::now().timestamp_millis(), "s": "BTCUSDT", "p": "108"}
        });
        second.send(Message::Text(valid.to_string())).await.unwrap();
        while second.next().await.is_some() {}
    });

    let (_config_tx, config_rx) = watch::channel("BTCUSDT".to_string());
    let (ticker_tx, mut ticker_rx) = broadcast::channel(8);
    let client = tokio::spawn(
        BinanceWsStream::new(config_rx, ticker_tx)
            .run_with_url(move |symbol| format!("ws://{address}/market/stream?symbol={symbol}")),
    );
    let update = tokio::time::timeout(Duration::from_secs(4), ticker_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(update.mark_price, dec!(108));
    client.abort();
    server.abort();
}

#[tokio::test]
async fn market_stream_hub_multiplexes_pairs_and_sends_live_subscribe_unsubscribe() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (sub_tx, mut sub_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
        let now_ms = Utc::now().timestamp_millis();
        let btc_mark = serde_json::json!({
            "stream": "btcusdc@markPrice@1s",
            "data": {"e": "markPriceUpdate", "E": now_ms, "s": "BTCUSDC", "p": "50000"}
        });
        ws.send(Message::Text(btc_mark.to_string())).await.unwrap();

        while let Some(Ok(msg)) = ws.next().await {
            if let Message::Text(text) = msg {
                let _ = sub_tx.send(text.clone());
                if text.contains("\"SUBSCRIBE\"") && text.contains("ethusdc@markPrice@1s") {
                    let eth_mark = serde_json::json!({
                        "stream": "ethusdc@markPrice@1s",
                        "data": {"e": "markPriceUpdate", "E": Utc::now().timestamp_millis(), "s": "ETHUSDC", "p": "3000"}
                    });
                    ws.send(Message::Text(eth_mark.to_string())).await.unwrap();
                }
            }
        }
    });

    let hub = MarketStreamHub::with_base_url_and_timeout(
        format!("ws://{address}"),
        Duration::from_secs(5),
    );
    let (_btc_cfg_tx, btc_cfg_rx) = watch::channel("BTCUSDC".to_string());
    let (btc_tx, mut btc_rx) = broadcast::channel(8);
    let btc_handle = hub.register(btc_cfg_rx, btc_tx);

    let btc_update = tokio::time::timeout(Duration::from_secs(3), btc_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(btc_update.symbol, "BTCUSDC");
    assert_eq!(btc_update.mark_price, dec!(50000));

    // Register a second symbol on the same hub; it should send a live SUBSCRIBE without reconnecting.
    let (_eth_cfg_tx, eth_cfg_rx) = watch::channel("ETHUSDC".to_string());
    let (eth_tx, mut eth_rx) = broadcast::channel(8);
    let eth_handle = hub.register(eth_cfg_rx, eth_tx);

    let subscribe_cmd = tokio::time::timeout(Duration::from_secs(3), sub_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(subscribe_cmd.contains("\"SUBSCRIBE\""));
    assert!(subscribe_cmd.contains("ethusdc@ticker"));
    assert!(subscribe_cmd.contains("ethusdc@markPrice@1s"));

    let eth_update = tokio::time::timeout(Duration::from_secs(3), eth_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(eth_update.symbol, "ETHUSDC");
    assert_eq!(eth_update.mark_price, dec!(3000));

    // Unregister ETH; hub should send live UNSUBSCRIBE.
    eth_handle.abort();
    let unsubscribe_cmd = tokio::time::timeout(Duration::from_secs(3), sub_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(unsubscribe_cmd.contains("\"UNSUBSCRIBE\""));
    assert!(unsubscribe_cmd.contains("ethusdc@ticker"));

    btc_handle.abort();
    server.abort();
}

#[tokio::test]
async fn user_data_hub_rotates_expired_listen_key_and_wakes_symbol_on_order_update() {
    use axum::{routing::post, Json, Router};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let key_seq = Arc::new(AtomicUsize::new(0));
    let key_seq_route = Arc::clone(&key_seq);
    let app = Router::new().route(
        "/fapi/v1/listenKey",
        post(move || {
            let seq = key_seq_route.fetch_add(1, Ordering::SeqCst) + 1;
            async move { Json(serde_json::json!({ "listenKey": format!("lk_{seq}") })) }
        })
        .put(|| async { Json(serde_json::json!({})) })
        .delete(|| async { Json(serde_json::json!({})) }),
    );
    let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_url = format!("http://{}", http_listener.local_addr().unwrap());
    let http_server = tokio::spawn(async move { axum::serve(http_listener, app).await.unwrap() });

    let ws_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = ws_listener.local_addr().unwrap();
    let ws_server = tokio::spawn(async move {
        // First connection receives listenKeyExpired to trigger immediate rotation.
        let (sock1, _) = ws_listener.accept().await.unwrap();
        let mut ws1 = tokio_tungstenite::accept_async(sock1).await.unwrap();
        ws1.send(Message::Text(
            r#"{"e":"listenKeyExpired","E":1700000000000}"#.to_string(),
        ))
        .await
        .unwrap();

        // Second connection sends an ORDER_TRADE_UPDATE for BTCUSDC.
        let (sock2, _) = ws_listener.accept().await.unwrap();
        let mut ws2 = tokio_tungstenite::accept_async(sock2).await.unwrap();
        ws2.send(Message::Text(
            r#"{"e":"ORDER_TRADE_UPDATE","E":1700000001000,"o":{"s":"BTCUSDC","x":"TRADE","X":"FILLED"}}"#
                .to_string(),
        ))
        .await
        .unwrap();
        while ws2.next().await.is_some() {}
    });

    let mut cfg = crate::config::AppConfig::default().exchange;
    cfg.api_key = "live-user-key".into();
    cfg.api_secret = "live-user-secret".into();
    let client = Arc::new(crate::exchange::BinanceFuturesClient::with_base_url(
        &cfg, http_url,
    ));

    let account_cache = Arc::new(crate::exchange::account_cache::AccountCache::default());
    let position_cache = Arc::new(crate::exchange::position_cache::PositionCache::default());
    let hub = UserDataHub::with_options(
        account_cache,
        position_cache,
        format!("ws://{ws_addr}"),
        Duration::from_secs(60),
        Duration::from_secs(30),
        Duration::from_secs(10),
    );
    let (_scope_tx, scope_rx) = watch::channel(UserDataScope {
        client,
        symbol: "BTCUSDC".to_string(),
        is_paper: false,
    });
    let (mut wakeup_rx, guard_task) = hub.register(scope_rx);

    // Wait for wakeup counter to advance at least twice (initial connect + second connect order update).
    tokio::time::timeout(Duration::from_secs(4), async {
        while *wakeup_rx.borrow_and_update() < 3 {
            wakeup_rx.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(key_seq.load(Ordering::SeqCst) >= 2);

    guard_task.abort();
    ws_server.abort();
    http_server.abort();
}
