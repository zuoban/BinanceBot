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
