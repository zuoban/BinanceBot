use super::*;

pub(super) const GRID_ORDER_PREFIX: &str = "gb_";

/// Binance Futures allows at most 36 characters for newClientOrderId.
pub(super) fn new_grid_client_order_id(side: OrderSide) -> String {
    let mut uuid_buf = [0u8; 32];
    let uuid = Uuid::new_v4().simple().encode_lower(&mut uuid_buf);
    let side_code = match side {
        OrderSide::Buy => "b",
        OrderSide::Sell => "s",
    };
    let mut id = String::with_capacity(35);
    id.push_str(GRID_ORDER_PREFIX);
    id.push_str(side_code);
    id.push('_');
    id.push_str(&uuid[..30]);
    id
}

#[must_use]
#[inline]
pub(super) fn is_grid_order(client_order_id: &str) -> bool {
    client_order_id.starts_with(GRID_ORDER_PREFIX)
}

#[must_use]
#[inline]
pub(super) fn unresolved_filled_quantity(order: &BinanceOrderResponse) -> bool {
    order.status == "FILLED" && order.executed_qty <= Decimal::ZERO
}

/// All idle orders follow the window. Partial executions finish on their existing order.
#[must_use]
#[inline]
pub(super) fn is_window_order(order: &GridOrder) -> bool {
    is_grid_order(&order.client_order_id) && order.status == OrderStatus::New
}

// Paper accounts have no exchange-provided leverage until one is explicitly set.
// Treat the default zero as 1x so a missing leverage never understates margin.
pub(super) fn update_paper_account(
    position: &mut PositionInfo,
    account: &mut AccountInfo,
    ticker: &TickerInfo,
) {
    let price = ticker.grid_reference_price();
    if price > Decimal::ZERO {
        position.mark_price = price;
    }
    position.unrealized_pnl = if position.size.is_zero() || position.mark_price <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        let diff = position.mark_price.saturating_sub(position.entry_price);
        diff.checked_mul(position.size).unwrap_or_else(|| {
            if diff.is_sign_negative() == position.size.is_sign_negative() {
                Decimal::MAX
            } else {
                Decimal::MIN
            }
        })
    };
    account.unrealized_profit = position.unrealized_pnl;
    account.margin_balance = account
        .total_wallet_balance
        .saturating_add(position.unrealized_pnl);
    account.available_balance = account
        .margin_balance
        .saturating_sub(position.position_margin());
    account.update_time = if ticker.mark_price > Decimal::ZERO {
        ticker.mark_update_time
    } else {
        ticker.update_time
    };
}

#[must_use]
#[inline]
pub(super) fn within_grid_spacing(
    a: Decimal,
    b: Decimal,
    interval: Decimal,
    tick_size: Decimal,
) -> bool {
    let spacing = interval.max(tick_size);
    a.checked_sub(b).is_some_and(|diff| diff.abs() < spacing)
}

#[must_use]
#[inline]
pub(super) fn has_nearby_grid_order(
    prices: &[Decimal],
    target: Decimal,
    grid_interval: Decimal,
    tick_size: Decimal,
) -> bool {
    prices
        .iter()
        .any(|&price| within_grid_spacing(price, target, grid_interval, tick_size))
}

/// Same-side active orders reserve their current price level.
pub(super) fn grid_level_occupied<'a>(
    orders: impl IntoIterator<Item = &'a GridOrder>,
    side: OrderSide,
    price: Decimal,
    interval: Decimal,
    tick_size: Decimal,
) -> bool {
    orders.into_iter().any(|order| {
        order.side == side && within_grid_spacing(order.price, price, interval, tick_size)
    })
}

pub(super) fn reserved_closing_quantity<'a>(
    direction: GridDirection,
    orders: impl IntoIterator<Item = &'a GridOrder>,
) -> Decimal {
    orders
        .into_iter()
        .filter(|o| o.side == direction.closing_side())
        .fold(Decimal::ZERO, |acc, o| {
            acc.checked_add(o.quantity).unwrap_or(Decimal::MAX)
        })
}

pub(super) fn closing_quantity_available<'a>(
    direction: GridDirection,
    position_size: Decimal,
    orders: impl IntoIterator<Item = &'a GridOrder>,
) -> Decimal {
    direction
        .position_quantity(position_size)
        .checked_sub(reserved_closing_quantity(direction, orders))
        .unwrap_or(Decimal::ZERO)
        .max(Decimal::ZERO)
}

#[cfg(test)]
pub(super) fn reserved_sell_quantity(orders: &[GridOrder]) -> Decimal {
    reserved_closing_quantity(GridDirection::Long, orders)
}

#[cfg(test)]
pub(super) fn sell_quantity_available(position_size: Decimal, orders: &[GridOrder]) -> Decimal {
    closing_quantity_available(GridDirection::Long, position_size, orders)
}
