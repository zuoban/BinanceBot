use super::sim::{BtOrder, Sim};
use crate::types::{GridDirection, OrderSide};
use rust_decimal::Decimal;

impl Sim<'_> {
    /// Mirrors `GridTradingEngine::maintain_grid_window` for a single symbol.
    pub(super) fn maintain(&mut self) {
        if self.paused.is_some() || self.pause_at_position_limit() {
            return;
        }
        let opening = self.p.grid.direction.opening_side();
        let (buys, sells) = self.compute_window_prices();
        self.prune_and_trim_orders(&buys, &sells);
        let (opening_prices, closing_prices) = if opening == OrderSide::Buy {
            (buys.as_slice(), sells.as_slice())
        } else {
            (sells.as_slice(), buys.as_slice())
        };
        if !self.replenish_opening_orders(opening_prices) {
            return;
        }
        let Some(reserved_qty) = self.replenish_closing_orders(closing_prices) else {
            return;
        };
        self.replenish_remainder_order(closing_prices, reserved_qty);
    }

    fn compute_window_prices(&self) -> (Vec<Decimal>, Vec<Decimal>) {
        let grid = &self.p.grid;
        let direction = grid.direction;
        let opening = direction.opening_side();
        let market = self.market;
        let position_value = self.size.abs().checked_mul(market).unwrap_or(Decimal::MAX);
        let interval = grid.grid_interval;
        let last_fill = self.last_fill;
        let (mut buys, mut sells) = self.p.rules.grid_window_prices(
            market,
            interval,
            (grid.buy_window, grid.sell_window),
            (grid.min_price, grid.max_price),
            |_, price| {
                last_fill.is_none_or(|fill| {
                    price
                        .checked_sub(fill)
                        .is_none_or(|diff| diff.abs() >= interval)
                })
            },
        );
        if let Some(spacings) = self.p.exit_floor_spacings.filter(|_| !self.size.is_zero()) {
            if let Some(offset) = spacings.checked_mul(interval) {
                if direction == GridDirection::Long {
                    let floor = self.entry.checked_add(offset).unwrap_or(Decimal::MAX);
                    sells.retain(|price| *price >= floor);
                } else {
                    let ceiling = self.entry.checked_sub(offset).unwrap_or(Decimal::MIN);
                    buys.retain(|price| *price <= ceiling);
                }
            } else if direction == GridDirection::Long {
                sells.clear();
            } else {
                buys.clear();
            }
        }
        let suspended = grid.cancel_opening_on_max_position
            && !grid.pause_on_max_position
            && self.limit().is_some_and(|limit| position_value >= limit);
        if suspended {
            if opening == OrderSide::Buy {
                buys.clear();
            } else {
                sells.clear();
            }
        }
        (buys, sells)
    }

    fn prune_and_trim_orders(&mut self, buys: &[Decimal], sells: &[Decimal]) {
        let direction = self.p.grid.direction;
        let closing = direction.closing_side();
        let mut position_qty = direction.position_quantity(self.size);
        self.orders.retain(|order| {
            if order.side == closing && position_qty.is_zero() {
                return false;
            }
            if order.side == OrderSide::Buy {
                buys.contains(&order.price)
            } else {
                sells.contains(&order.price)
            }
        });
        if position_qty.is_zero() {
            return;
        }
        // Keep closing orders within the position, nearest first.
        let mut closing_orders: Vec<usize> = (0..self.orders.len())
            .filter(|&i| self.orders[i].side == closing)
            .collect();
        closing_orders.sort_by(|&a, &b| {
            direction.closing_price_cmp(self.orders[a].price, self.orders[b].price)
        });
        let mut drop = Vec::new();
        for index in closing_orders {
            if self.orders[index].qty <= position_qty {
                position_qty = position_qty.saturating_sub(self.orders[index].qty);
            } else {
                drop.push(index);
            }
        }
        drop.sort_unstable_by(|a, b| b.cmp(a));
        for index in drop {
            self.orders.remove(index);
        }
    }

    fn current_opening_notional(&self, opening: OrderSide) -> Decimal {
        self.orders
            .iter()
            .filter(|o| o.side == opening)
            .fold(Decimal::ZERO, |acc, o| {
                o.price
                    .checked_mul(o.qty)
                    .and_then(|n| acc.checked_add(n))
                    .unwrap_or(Decimal::MAX)
            })
    }

    fn replenish_opening_orders(&mut self, opening_prices: &[Decimal]) -> bool {
        let direction = self.p.grid.direction;
        let opening = direction.opening_side();
        let market = self.market;
        let limit = self.limit();
        let position_value = self.size.abs().checked_mul(market).unwrap_or(Decimal::MAX);
        let mut opening_notional =
            limit.map_or(Decimal::ZERO, |_| self.current_opening_notional(opening));
        for &price in opening_prices {
            if self
                .orders
                .iter()
                .any(|o| o.side == opening && o.price == price)
            {
                continue;
            }
            let Some(qty) = self
                .qty_at(price)
                .filter(|_| opening.is_maker_price(price, market))
            else {
                continue;
            };
            let Some(order_notional) = price.checked_mul(qty) else {
                continue;
            };
            if let Some(limit) = limit {
                if self.p.grid.pause_on_max_position {
                    let projected = direction
                        .sign()
                        .checked_mul(qty)
                        .and_then(|delta| self.size.checked_add(delta))
                        .and_then(|s| s.abs().checked_mul(market))
                        .unwrap_or(Decimal::MAX);
                    if projected > limit
                        && (projected > position_value || projected == Decimal::MAX)
                    {
                        self.pause("下一笔开仓单成交后预计持仓名义价值超过上限");
                        return false;
                    }
                }
                let valuation = market.max(price);
                let exposure = direction
                    .position_quantity(self.size)
                    .checked_mul(valuation)
                    .and_then(|pos| pos.checked_add(opening_notional))
                    .and_then(|sum| sum.checked_add(order_notional))
                    .unwrap_or(Decimal::MAX);
                if exposure > limit || exposure == Decimal::MAX {
                    continue;
                }
            }
            opening_notional = opening_notional
                .checked_add(order_notional)
                .unwrap_or(Decimal::MAX);
            self.orders.push(BtOrder {
                side: opening,
                price,
                qty,
                remainder: false,
            });
        }
        true
    }

    fn replenish_closing_orders(&mut self, closing_prices: &[Decimal]) -> Option<Decimal> {
        let direction = self.p.grid.direction;
        let closing = direction.closing_side();
        let market = self.market;
        let position_qty = direction.position_quantity(self.size);
        if position_qty.is_zero() {
            return None;
        }
        let mut reserved_qty: Decimal = self
            .orders
            .iter()
            .filter(|o| o.side == closing)
            .fold(Decimal::ZERO, |acc, o| {
                acc.checked_add(o.qty).unwrap_or(Decimal::MAX)
            });
        for &price in closing_prices {
            if self
                .orders
                .iter()
                .any(|o| o.side == closing && o.price == price)
            {
                continue;
            }
            let Some(qty) = self
                .qty_at(price)
                .filter(|_| closing.is_maker_price(price, market))
            else {
                continue;
            };
            let available = position_qty
                .checked_sub(reserved_qty)
                .unwrap_or(Decimal::ZERO)
                .max(Decimal::ZERO);
            if available >= qty {
                reserved_qty = reserved_qty.checked_add(qty).unwrap_or(Decimal::MAX);
                self.orders.push(BtOrder {
                    side: closing,
                    price,
                    qty,
                    remainder: false,
                });
            }
        }
        Some(reserved_qty)
    }

    fn replenish_remainder_order(&mut self, closing_prices: &[Decimal], reserved_qty: Decimal) {
        let direction = self.p.grid.direction;
        let closing = direction.closing_side();
        let market = self.market;
        let position_qty = direction.position_quantity(self.size);
        // Ordinary allocation must honor existing remainder reservations. Only replace
        // their queue after a compliant target can reuse their reserved inventory.
        let remainder_qty: Decimal = self
            .orders
            .iter()
            .filter(|o| o.side == closing && o.remainder)
            .fold(Decimal::ZERO, |acc, o| {
                acc.checked_add(o.qty).unwrap_or(Decimal::MAX)
            });
        let available = position_qty
            .checked_sub(reserved_qty)
            .and_then(|v| v.checked_add(remainder_qty))
            .unwrap_or(Decimal::ZERO)
            .max(Decimal::ZERO);
        let Some(&price) = closing_prices.iter().find(|&&price| {
            !self
                .orders
                .iter()
                .any(|o| o.side == closing && o.price == price && !o.remainder)
                && closing.is_maker_price(price, market)
        }) else {
            return;
        };
        let Some((qty, purpose)) =
            self.p
                .rules
                .plan_remainder_quantity(price, self.p.grid.order_amount_usdc, available)
        else {
            return;
        };
        let remainder = purpose == crate::types::OrderPurpose::Remainder;
        let mut sources = self
            .orders
            .iter()
            .filter(|o| o.side == closing && o.remainder);
        if sources
            .next()
            .is_some_and(|o| o.price == price && o.qty == qty && o.remainder == remainder)
            && sources.next().is_none()
        {
            return;
        }
        self.orders.retain(|o| o.side != closing || !o.remainder);
        self.orders.push(BtOrder {
            side: closing,
            price,
            qty,
            remainder,
        });
    }
}
