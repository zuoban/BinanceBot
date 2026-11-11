use crate::types::TickerInfo;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::borrow::Cow;
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const WS_READ_IDLE_TIMEOUT: Duration = Duration::from_secs(15);
const WS_PONG_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) struct BinanceWsStream {
    config_rx: watch::Receiver<String>,
    ticker_tx: broadcast::Sender<TickerInfo>,
}

fn stream_url(symbol: &str) -> String {
    let host = "wss://fstream.binance.com";
    let symbol = symbol.to_lowercase();
    format!("{host}/market/stream?streams={symbol}@ticker/{symbol}@markPrice@1s")
}

#[derive(Default)]
struct EventTimes {
    ticker: Option<i64>,
    mark: Option<i64>,
}

fn stream_matches_symbol(stream: Option<&str>, suffix: &str, symbol: &str) -> bool {
    stream.is_none_or(|stream| {
        stream
            .strip_suffix(suffix)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(symbol))
    })
}

#[derive(Deserialize)]
struct WsMarketFields<'a> {
    #[serde(borrow, rename = "e")]
    event_type: Cow<'a, str>,
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(borrow, rename = "s")]
    symbol: Cow<'a, str>,
    #[serde(default, rename = "c")]
    close_price: Option<Decimal>,
    #[serde(default, rename = "o")]
    open_price: Option<Decimal>,
    #[serde(default, rename = "h")]
    high_price: Option<Decimal>,
    #[serde(default, rename = "l")]
    low_price: Option<Decimal>,
    #[serde(default, rename = "v")]
    volume: Option<Decimal>,
    #[serde(default, rename = "q")]
    quote_volume: Option<Decimal>,
    #[serde(default, rename = "p")]
    price_field: Option<Decimal>,
}

#[derive(Deserialize)]
struct WsRawFrame<'a> {
    #[serde(borrow, default)]
    stream: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    data: Option<WsMarketFields<'a>>,
    #[serde(borrow, default, rename = "e")]
    event_type: Option<Cow<'a, str>>,
    #[serde(default, rename = "E")]
    event_time: Option<i64>,
    #[serde(borrow, default, rename = "s")]
    symbol: Option<Cow<'a, str>>,
    #[serde(default, rename = "c")]
    close_price: Option<Decimal>,
    #[serde(default, rename = "o")]
    open_price: Option<Decimal>,
    #[serde(default, rename = "h")]
    high_price: Option<Decimal>,
    #[serde(default, rename = "l")]
    low_price: Option<Decimal>,
    #[serde(default, rename = "v")]
    volume: Option<Decimal>,
    #[serde(default, rename = "q")]
    quote_volume: Option<Decimal>,
    #[serde(default, rename = "p")]
    price_field: Option<Decimal>,
}

fn parse_market_update(
    text: &str,
    symbol: &str,
    now: DateTime<Utc>,
    times: &mut EventTimes,
) -> Option<TickerInfo> {
    let raw: WsRawFrame<'_> = serde_json::from_str(text).ok()?;
    let stream = raw.stream;
    let fields = if stream.is_some() {
        raw.data?
    } else {
        WsMarketFields {
            event_type: raw.event_type?,
            event_time: raw.event_time?,
            symbol: raw.symbol?,
            close_price: raw.close_price,
            open_price: raw.open_price,
            high_price: raw.high_price,
            low_price: raw.low_price,
            volume: raw.volume,
            quote_volume: raw.quote_volume,
            price_field: raw.price_field,
        }
    };
    if !fields.symbol.eq_ignore_ascii_case(symbol) {
        return None;
    }
    let event_at = DateTime::from_timestamp_millis(fields.event_time)?;
    let age = now.signed_duration_since(event_at);
    if age < chrono::Duration::seconds(-2) || age >= chrono::Duration::seconds(15) {
        return None;
    }
    let observed_at = event_at.min(now);
    match fields.event_type.as_ref() {
        "24hrTicker" if stream_matches_symbol(stream.as_deref(), "@ticker", symbol) => {
            let close_price = fields.close_price?;
            let open_price = fields.open_price?;
            let high_price = fields.high_price?;
            let low_price = fields.low_price?;
            let volume = fields.volume?;
            let _ = fields.quote_volume?;
            if close_price <= Decimal::ZERO
                || times.ticker.is_some_and(|last| fields.event_time <= last)
            {
                return None;
            }
            times.ticker = Some(fields.event_time);
            let change = close_price.checked_sub(open_price).unwrap_or(Decimal::ZERO);
            let change_pct = if open_price.is_zero() {
                Decimal::ZERO
            } else {
                change
                    .checked_div(open_price)
                    .and_then(|ratio| ratio.checked_mul(rust_decimal_macros::dec!(100)))
                    .unwrap_or(Decimal::ZERO)
            };
            Some(TickerInfo {
                symbol: symbol.to_string(),
                last_price: close_price,
                high_24h: high_price,
                low_24h: low_price,
                change_24h: change,
                change_percent_24h: change_pct,
                volume_24h: volume,
                update_time: observed_at,
                ..Default::default()
            })
        }
        "markPriceUpdate" if stream_matches_symbol(stream.as_deref(), "@markPrice@1s", symbol) => {
            let mark_price = fields.price_field?;
            if mark_price <= Decimal::ZERO
                || times.mark.is_some_and(|last| fields.event_time <= last)
            {
                return None;
            }
            times.mark = Some(fields.event_time);
            Some(TickerInfo {
                symbol: symbol.to_string(),
                mark_price,
                mark_update_time: observed_at,
                ..Default::default()
            })
        }
        _ => None,
    }
}

fn local_observed_time(
    local_now: DateTime<Utc>,
    exchange_now: DateTime<Utc>,
    exchange_time: DateTime<Utc>,
) -> DateTime<Utc> {
    let age = exchange_now.signed_duration_since(exchange_time);
    local_now.checked_sub_signed(age).unwrap_or(local_now)
}

impl BinanceWsStream {
    pub(crate) fn new(
        config_rx: watch::Receiver<String>,
        ticker_tx: broadcast::Sender<TickerInfo>,
    ) -> Self {
        Self {
            config_rx,
            ticker_tx,
        }
    }

    pub(crate) async fn run(self) {
        self.run_with_url_and_timeout(stream_url, WS_READ_IDLE_TIMEOUT)
            .await;
    }

    #[cfg(test)]
    async fn run_with_url<F>(self, build_url: F)
    where
        F: Fn(&str) -> String,
    {
        self.run_with_url_and_timeout(build_url, WS_READ_IDLE_TIMEOUT)
            .await;
    }

    async fn run_with_url_and_timeout<F>(mut self, build_url: F, read_timeout: Duration)
    where
        F: Fn(&str) -> String,
    {
        let mut backoff_secs = 1u64;
        let mut previous_config = None;
        let mut event_times = EventTimes::default();
        'connect: loop {
            let symbol = self.config_rx.borrow_and_update().clone();
            if previous_config.as_ref() != Some(&symbol) {
                event_times = EventTimes::default();
                previous_config = Some(symbol.clone());
            }
            let ws_url = build_url(&symbol);
            info!("Starting Binance WebSocket stream for {}", ws_url);

            let connection = tokio::select! {
                result = tokio::time::timeout(WS_CONNECT_TIMEOUT, connect_async(&ws_url)) => result,
                changed = self.config_rx.changed() => {
                    if changed.is_err() { return; }
                    continue 'connect;
                }
            };
            match connection {
                Ok(Ok((ws_stream, _))) => {
                    info!("Connected to Binance WebSocket: {}", ws_url);
                    backoff_secs = 1;
                    let (mut write, mut read) = ws_stream.split();
                    loop {
                        let next_msg = tokio::select! {
                            msg = tokio::time::timeout(read_timeout, read.next()) => msg,
                            changed = self.config_rx.changed() => {
                                if changed.is_err() { return; }
                                continue 'connect;
                            }
                        };
                        let msg_res = match next_msg {
                            Ok(Some(msg_res)) => msg_res,
                            Ok(None) => break,
                            Err(_) => {
                                warn!(
                                    "WebSocket read timed out after {:?} for {}; reconnecting",
                                    read_timeout, ws_url
                                );
                                break;
                            }
                        };
                        match msg_res {
                            Ok(Message::Ping(payload)) => {
                                match tokio::time::timeout(
                                    WS_PONG_WRITE_TIMEOUT,
                                    write.send(Message::Pong(payload)),
                                )
                                .await
                                {
                                    Ok(Ok(())) => {}
                                    Ok(Err(e)) => {
                                        warn!("WebSocket pong send error: {}", e);
                                        break;
                                    }
                                    Err(_) => {
                                        warn!("WebSocket pong send timed out for {}", ws_url);
                                        break;
                                    }
                                }
                            }
                            Ok(Message::Close(_)) => break,
                            Ok(msg) if msg.is_text() => {
                                if *self.config_rx.borrow() != symbol {
                                    continue 'connect;
                                }
                                if let Ok(text) = msg.to_text() {
                                    let local_now = Utc::now();
                                    let exchange_now = local_now
                                        .checked_add_signed(chrono::Duration::milliseconds(
                                            super::client::mainnet_time_offset_ms(),
                                        ))
                                        .unwrap_or(local_now);
                                    if let Some(mut update) = parse_market_update(
                                        text,
                                        &symbol,
                                        exchange_now,
                                        &mut event_times,
                                    ) {
                                        if update.last_price > Decimal::ZERO {
                                            update.update_time = local_observed_time(
                                                local_now,
                                                exchange_now,
                                                update.update_time,
                                            );
                                        }
                                        if update.mark_price > Decimal::ZERO {
                                            update.mark_update_time = local_observed_time(
                                                local_now,
                                                exchange_now,
                                                update.mark_update_time,
                                            );
                                        }
                                        let _ = self.ticker_tx.send(update);
                                    }
                                }
                            }
                            Ok(_) => {}
                            Err(e) => {
                                warn!("WebSocket read error: {}", e);
                                break;
                            }
                        }
                    }
                }
                Ok(Err(e)) => error!("Failed to connect to Binance WebSocket: {}", e),
                Err(_) => error!("Timed out connecting to Binance WebSocket: {}", ws_url),
            }
            warn!(
                "WebSocket disconnected. Reconnecting in {} seconds...",
                backoff_secs
            );
            tokio::select! {
                _ = sleep(Duration::from_millis(rand::random::<u64>() % (backoff_secs * 500 + 1) + backoff_secs * 500)) => {},
                changed = self.config_rx.changed() => {
                    if changed.is_err() { return; }
                    continue 'connect;
                }
            }
            backoff_secs = (backoff_secs * 2).min(30);
        }
    }
}

#[cfg(test)]
mod tests {
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
            BinanceWsStream::new(config_rx, ticker_tx).run_with_url(move |symbol| {
                format!("ws://{address}/market/stream?symbol={symbol}")
            }),
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
}
