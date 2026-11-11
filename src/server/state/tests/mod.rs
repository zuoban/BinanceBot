use super::*;
use crate::types::{OrderSide, TradingMode};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

mod auth_and_lifecycle;
mod global_risk_and_notifications;
mod trades_and_stats_cache;

pub(super) fn cache_test_trade(id: &str, seconds: i64, mode: TradingMode) -> TradeRecord {
    TradeRecord {
        trade_id: id.into(),
        client_order_id: format!("gb_b_{id}"),
        symbol: "SOLUSDC".into(),
        mode,
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        realized_pnl: Decimal::ZERO,
        commission: Decimal::ZERO,
        pnl_verified: false,
        is_maker: true,
        timestamp: chrono::DateTime::from_timestamp(1_700_000_000 + seconds, 0).unwrap(),
        note: String::new(),
    }
}
