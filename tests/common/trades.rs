use binance_grid_bot::types::{OrderSide, TradeRecord, TradingMode};
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

pub fn sample_trade(
    trade_id: &str,
    client_order_id: &str,
    symbol: &str,
    mode: TradingMode,
    side: OrderSide,
    price: Decimal,
    quantity: Decimal,
) -> TradeRecord {
    TradeRecord {
        trade_id: trade_id.to_string(),
        client_order_id: client_order_id.to_string(),
        symbol: symbol.to_string(),
        mode,
        side,
        price,
        quantity,
        amount_usdc: price * quantity,
        realized_pnl: dec!(0),
        commission: dec!(0),
        pnl_verified: true,
        is_maker: true,
        timestamp: Utc::now(),
        note: String::new(),
    }
}
