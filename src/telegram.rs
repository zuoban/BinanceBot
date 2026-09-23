use crate::config::TelegramConfig;
use crate::types::{OrderSide, TradeRecord};
use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct SendMessage<'a> {
    chat_id: &'a str,
    text: String,
    parse_mode: &'static str,
}

#[derive(Deserialize)]
struct TelegramResponse {
    ok: bool,
    description: Option<String>,
}

pub fn format_trade_message(trade: &TradeRecord, is_dry_run: bool, is_testnet: bool) -> String {
    let (icon, direction) = match trade.side {
        OrderSide::Buy => ("🟢", "买入"),
        OrderSide::Sell => ("🔴", "卖出"),
    };
    let mode = if is_dry_run {
        "模拟盘"
    } else if is_testnet {
        "测试网"
    } else {
        "实盘"
    };
    let quote = ["USDC", "USDT", "FDUSD", "BUSD"]
        .into_iter()
        .find(|asset| trade.symbol.ends_with(asset))
        .unwrap_or("计价资产");
    format!(
        "<b>{icon} {direction}成交｜{price}</b>\n交易对：{symbol}\n数量：{quantity}\n成交金额：{amount} {quote}\n模式：{mode}",
        price = trade.price,
        symbol = escape_html(&trade.symbol),
        quantity = trade.quantity,
        amount = trade.amount_usdc,
    )
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

pub async fn send_trade_notification(
    client: &Client,
    config: &TelegramConfig,
    trade: &TradeRecord,
    is_dry_run: bool,
    is_testnet: bool,
) -> Result<()> {
    if config.bot_token.trim().is_empty() || config.chat_id.trim().is_empty() {
        return Err(anyhow!("Bot Token or Chat ID is empty"));
    }
    let endpoint = format!(
        "https://api.telegram.org/bot{}/sendMessage",
        config.bot_token
    );
    let response = client
        .post(endpoint)
        .json(&SendMessage {
            chat_id: &config.chat_id,
            text: format_trade_message(trade, is_dry_run, is_testnet),
            parse_mode: "HTML",
        })
        .send()
        .await
        .map_err(|err| {
            anyhow!(
                "Telegram request failed ({})",
                if err.is_timeout() {
                    "timeout"
                } else {
                    "network error"
                }
            )
        })?;

    let status = response.status();
    let body: TelegramResponse = response
        .json()
        .await
        .map_err(|_| anyhow!("Telegram returned an invalid response (HTTP {})", status))?;
    if !status.is_success() || !body.ok {
        return Err(anyhow!(
            "Telegram rejected notification (HTTP {}): {}",
            status,
            body.description
                .unwrap_or_else(|| "unknown error".to_string())
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::format_trade_message;
    use crate::types::{OrderSide, TradeRecord};
    use chrono::Utc;
    use rust_decimal_macros::dec;

    #[test]
    fn title_shows_direction_and_fill_price() {
        let mut trade = TradeRecord {
            trade_id: "1".into(),
            client_order_id: "order-1".into(),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price: dec!(123.45),
            quantity: dec!(2),
            amount_usdc: dec!(246.90),
            realized_pnl: dec!(0),
            commission: dec!(0),
            is_maker: true,
            timestamp: Utc::now(),
            note: String::new(),
        };
        assert!(format_trade_message(&trade, true, false).starts_with("<b>🟢 买入成交｜123.45</b>"));
        trade.side = OrderSide::Sell;
        assert!(
            format_trade_message(&trade, false, false).starts_with("<b>🔴 卖出成交｜123.45</b>")
        );
    }
}
