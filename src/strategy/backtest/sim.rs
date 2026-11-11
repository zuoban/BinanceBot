use super::BacktestParams;
use crate::types::{GridDirection, OrderSide};
use rust_decimal::Decimal;

pub(super) struct BtOrder {
    pub(super) side: OrderSide,
    pub(super) price: Decimal,
    pub(super) qty: Decimal,
    pub(super) remainder: bool,
}

pub(super) struct Sim<'a> {
    pub(super) p: &'a BacktestParams,
    pub(super) orders: Vec<BtOrder>,
    pub(super) size: Decimal,
    pub(super) entry: Decimal,
    pub(super) last_fill: Option<Decimal>,
    pub(super) market: Decimal,
    pub(super) now_ms: i64,
    pub(super) realized: Decimal,
    /// Sum of signed fill cash flows, kept to cross-check the average-cost accounting.
    pub(super) cash_flow: Decimal,
    pub(super) fees: Decimal,
    pub(super) funding: Decimal,
    pub(super) fills: u64,
    pub(super) buys: u64,
    pub(super) sells: u64,
    pub(super) peak_equity: Decimal,
    pub(super) max_drawdown: Decimal,
    pub(super) peak_notional: Decimal,
    pub(super) paused: Option<(i64, String)>,
}

impl<'a> Sim<'a> {
    pub(super) fn new(p: &'a BacktestParams, start_price: Decimal, now_ms: i64) -> Self {
        Self {
            p,
            orders: Vec::new(),
            size: Decimal::ZERO,
            entry: Decimal::ZERO,
            last_fill: None,
            market: start_price,
            now_ms,
            realized: Decimal::ZERO,
            cash_flow: Decimal::ZERO,
            fees: Decimal::ZERO,
            funding: Decimal::ZERO,
            fills: 0,
            buys: 0,
            sells: 0,
            peak_equity: Decimal::ZERO,
            max_drawdown: Decimal::ZERO,
            peak_notional: Decimal::ZERO,
            paused: None,
        }
    }

    fn limit(&self) -> Option<Decimal> {
        self.p
            .grid
            .max_position_usdc
            .filter(|limit| *limit > Decimal::ZERO)
    }

    pub(super) fn unrealized(&self) -> Decimal {
        if self.size.is_zero() {
            Decimal::ZERO
        } else {
            self.market
                .checked_sub(self.entry)
                .and_then(|diff| diff.checked_mul(self.size))
                .unwrap_or(Decimal::ZERO)
        }
    }

    fn equity(&self) -> Decimal {
        self.realized
            .checked_sub(self.fees)
            .and_then(|v| v.checked_sub(self.funding))
            .and_then(|v| v.checked_add(self.unrealized()))
            .unwrap_or(Decimal::ZERO)
    }

    pub(super) fn record(&mut self) {
        let equity = self.equity();
        self.peak_equity = self.peak_equity.max(equity);
        self.max_drawdown = self
            .max_drawdown
            .max(self.peak_equity.checked_sub(equity).unwrap_or(Decimal::MAX));
        self.peak_notional = self.peak_notional.max(
            self.size
                .abs()
                .checked_mul(self.market)
                .unwrap_or(Decimal::MAX),
        );
    }

    fn pause(&mut self, reason: &str) {
        if self.paused.is_none() {
            self.paused = Some((self.now_ms, reason.to_string()));
        }
        self.orders.clear();
    }

    fn qty_at(&self, price: Decimal) -> Option<Decimal> {
        if price <= Decimal::ZERO {
            return None;
        }
        self.p
            .rules
            .calculate_quantity(price, self.p.grid.order_amount_usdc)
            .filter(|qty| *qty > Decimal::ZERO)
    }

    fn pause_at_position_limit(&mut self) -> bool {
        if self.p.grid.pause_on_max_position
            && self.limit().is_some_and(|limit| {
                self.size
                    .abs()
                    .checked_mul(self.market)
                    .is_none_or(|notional| notional >= limit)
            })
        {
            self.pause("持仓名义价值达到最大持仓名义价值");
            return true;
        }
        false
    }

    pub(super) fn move_to(&mut self, target: Decimal) {
        loop {
            let current = self.market;
            if target == current {
                break;
            }
            if self.paused.is_some() {
                self.market = target;
                self.record();
                break;
            }
            let down = target < current;
            let next = if down {
                self.orders
                    .iter()
                    .filter(|o| o.side == OrderSide::Buy && o.price < current && o.price >= target)
                    .map(|o| o.price)
                    .max()
            } else {
                self.orders
                    .iter()
                    .filter(|o| o.side == OrderSide::Sell && o.price > current && o.price <= target)
                    .map(|o| o.price)
                    .min()
            };
            match next {
                Some(price) => {
                    self.market = price;
                    // The engine marks the position and checks its cap before paper matching.
                    // Do not maintain the window here: it would prune the touched order.
                    self.record();
                    if self.pause_at_position_limit() {
                        continue;
                    }
                    let side = if down {
                        OrderSide::Buy
                    } else {
                        OrderSide::Sell
                    };
                    self.fill_at(side, price);
                    self.record();
                    self.maintain();
                    self.record();
                }
                None => {
                    let interval = self.p.grid.grid_interval;
                    let same_cell = !self.orders.is_empty()
                        && self.limit().is_none()
                        && interval > Decimal::ZERO
                        && current
                            .checked_div(interval)
                            .zip(target.checked_div(interval))
                            .is_some_and(|(a, b)| a.floor() == b.floor() && a.ceil() == b.ceil());
                    self.market = target;
                    self.record();
                    if !same_cell {
                        self.maintain();
                        self.record();
                    }
                    break;
                }
            }
        }
    }

    pub(super) fn fill_at(&mut self, side: OrderSide, price: Decimal) {
        let Some(index) = self
            .orders
            .iter()
            .position(|o| o.side == side && o.price == price)
        else {
            return;
        };
        let order = self.orders.remove(index);
        let direction = self.p.grid.direction;
        let mut qty = order.qty;
        if side == direction.closing_side() {
            qty = qty.min(direction.position_quantity(self.size));
        }
        if qty <= Decimal::ZERO {
            return;
        }
        let (new_size, new_entry, realized_delta) =
            crate::types::apply_fill_to_position(self.size, self.entry, side, price, qty);
        self.size = new_size;
        self.entry = new_entry;
        self.realized = self.realized.saturating_add(realized_delta);
        let fill_notional = price.checked_mul(qty).unwrap_or(Decimal::MAX);
        let signed_flow = if side == OrderSide::Buy {
            fill_notional
        } else {
            -fill_notional
        };
        self.cash_flow = self.cash_flow.saturating_sub(signed_flow);
        let fee = fill_notional
            .checked_mul(self.p.maker_fee_rate)
            .unwrap_or(Decimal::MAX);
        self.fees = self.fees.saturating_add(fee);
        self.last_fill = Some(price);
        self.fills += 1;
        if side == OrderSide::Buy {
            self.buys += 1;
        } else {
            self.sells += 1;
        }
    }

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

    fn replenish_opening_orders(&mut self, opening_prices: &[Decimal]) -> bool {
        let direction = self.p.grid.direction;
        let opening = direction.opening_side();
        let market = self.market;
        let limit = self.limit();
        let position_value = self.size.abs().checked_mul(market).unwrap_or(Decimal::MAX);
        let mut opening_notional: Decimal = limit.map_or(Decimal::ZERO, |_| {
            self.orders
                .iter()
                .filter(|o| o.side == opening)
                .fold(Decimal::ZERO, |acc, o| {
                    o.price
                        .checked_mul(o.qty)
                        .and_then(|n| acc.checked_add(n))
                        .unwrap_or(Decimal::MAX)
                })
        });
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
