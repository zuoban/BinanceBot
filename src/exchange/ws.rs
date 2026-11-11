use crate::types::TickerInfo;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::borrow::Cow;
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};

pub struct BinanceWsStream {
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
            let change = close_price - open_price;
            let change_pct = if open_price.is_zero() {
                Decimal::ZERO
            } else {
                change / open_price * rust_decimal_macros::dec!(100)
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

impl BinanceWsStream {
    pub fn new(
        config_rx: watch::Receiver<String>,
        ticker_tx: broadcast::Sender<TickerInfo>,
    ) -> Self {
        Self {
            config_rx,
            ticker_tx,
        }
    }

    pub async fn run(self) {
        self.run_with_url(stream_url).await;
    }

    async fn run_with_url<F>(mut self, build_url: F)
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
                result = connect_async(&ws_url) => result,
                changed = self.config_rx.changed() => {
                    if changed.is_err() { return; }
                    continue 'connect;
                }
            };
            match connection {
                Ok((ws_stream, _)) => {
                    info!("Connected to Binance WebSocket: {}", ws_url);
                    backoff_secs = 1;
                    let (_, mut read) = ws_stream.split();
                    loop {
                        let msg_res = tokio::select! {
                            msg = read.next() => msg,
                            changed = self.config_rx.changed() => {
                                if changed.is_err() { return; }
                                continue 'connect;
                            }
                        };
                        let Some(msg_res) = msg_res else { break };
                        match msg_res {
                            Ok(msg) if msg.is_text() => {
                                if *self.config_rx.borrow() != symbol {
                                    continue 'connect;
                                }
                                if let Ok(text) = msg.to_text() {
                                    if let Some(update) = parse_market_update(
                                        text,
                                        &symbol,
                                        Utc::now(),
                                        &mut event_times,
                                    ) {
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
                Err(e) => error!("Failed to connect to Binance WebSocket: {}", e),
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
    use futures_util::SinkExt;
    use rust_decimal_macros::dec;
    use tokio_tungstenite::tungstenite::Message;

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
}
