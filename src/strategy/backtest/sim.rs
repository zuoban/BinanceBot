use super::BacktestParams;
use crate::types::OrderSide;
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

    pub(super) fn limit(&self) -> Option<Decimal> {
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
            .saturating_sub(self.fees)
            .saturating_sub(self.funding)
            .saturating_add(self.unrealized())
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

    pub(super) fn pause(&mut self, reason: &str) {
        if self.paused.is_none() {
            self.paused = Some((self.now_ms, reason.to_string()));
        }
        self.orders.clear();
    }

    pub(super) fn qty_at(&self, price: Decimal) -> Option<Decimal> {
        if price <= Decimal::ZERO {
            return None;
        }
        self.p
            .rules
            .calculate_quantity(price, self.p.grid.order_amount_usdc)
            .filter(|qty| *qty > Decimal::ZERO)
    }

    pub(super) fn pause_at_position_limit(&mut self) -> bool {
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
}
