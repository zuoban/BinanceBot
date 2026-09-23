use crate::exchange::model::BinanceWs24hrTicker;
use crate::types::TickerInfo;
use chrono::Utc;
use futures_util::StreamExt;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};

pub struct BinanceWsStream {
    symbol: String,
    is_testnet: bool,
    ticker_tx: broadcast::Sender<TickerInfo>,
}

impl BinanceWsStream {
    pub fn new(
        symbol: String,
        is_testnet: bool,
        ticker_tx: broadcast::Sender<TickerInfo>,
    ) -> Self {
        Self {
            symbol,
            is_testnet,
            ticker_tx,
        }
    }

    pub async fn run(self) {
        let stream_name = format!("{}@ticker", self.symbol.to_lowercase());
        let base_ws_url = if self.is_testnet {
            "wss://fstream.binancefuture.com/ws"
        } else {
            "wss://fstream.binance.com/ws"
        };
        let ws_url = format!("{}/{}", base_ws_url, stream_name);

        info!("Starting Binance WebSocket stream for {}", ws_url);

        let mut backoff_secs = 1u64;

        loop {
            match connect_async(&ws_url).await {
                Ok((ws_stream, _)) => {
                    info!("Connected to Binance WebSocket: {}", ws_url);
                    backoff_secs = 1;

                    let (_, mut read) = ws_stream.split();

                    while let Some(msg_res) = read.next().await {
                        match msg_res {
                            Ok(msg) => {
                                if msg.is_text() {
                                    if let Ok(text) = msg.to_text() {
                                        if let Ok(ticker_msg) =
                                            serde_json::from_str::<BinanceWs24hrTicker>(text)
                                        {
                                            let change = ticker_msg.close_price - ticker_msg.open_price;
                                            let change_pct = if !ticker_msg.open_price.is_zero() {
                                                (change / ticker_msg.open_price) * rust_decimal_macros::dec!(100.0)
                                            } else {
                                                rust_decimal::Decimal::ZERO
                                            };

                                            let info = TickerInfo {
                                                symbol: ticker_msg.symbol,
                                                last_price: ticker_msg.close_price,
                                                // The 24h ticker stream does not include a mark price.
                                                mark_price: rust_decimal::Decimal::ZERO,
                                                mark_update_time: chrono::DateTime::<Utc>::default(),
                                                high_24h: ticker_msg.high_price,
                                                low_24h: ticker_msg.low_price,
                                                change_24h: change,
                                                change_percent_24h: change_pct,
                                                volume_24h: ticker_msg.volume,
                                                update_time: Utc::now(),
                                            };

                                            let _ = self.ticker_tx.send(info);
                                        }
                                    }
                                } else if msg.is_ping() {
                                    // Tungstenite handles pong automatically
                                }
                            }
                            Err(e) => {
                                warn!("WebSocket read error: {}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to connect to Binance WebSocket: {}", e);
                }
            }

            warn!(
                "WebSocket disconnected. Reconnecting in {} seconds...",
                backoff_secs
            );
            sleep(Duration::from_secs(backoff_secs)).await;
            backoff_secs = (backoff_secs * 2).min(30);
        }
    }
}
