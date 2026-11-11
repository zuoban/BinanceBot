use super::*;
use crate::types::{OrderSide, TradeRecord, TradingMode};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

mod legacy_migrations;
mod rollups_and_revisions;
mod security_cleanup_and_export;
mod trades_orders_and_config;

pub(super) fn revision_test_trade() -> TradeRecord {
    TradeRecord {
        trade_id: "revision-trade".into(),
        client_order_id: "revision-order".into(),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Paper,
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        realized_pnl: Decimal::ZERO,
        commission: Decimal::ZERO,
        pnl_verified: false,
        is_maker: true,
        timestamp: Utc::now(),
        note: String::new(),
    }
}
