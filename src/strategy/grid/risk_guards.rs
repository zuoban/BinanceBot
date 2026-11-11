use super::*;

#[derive(Clone, Copy)]
pub(super) struct RiskConfigSnapshot {
    pub(super) is_paper: bool,
    pub(super) sync_interval_secs: u64,
    pub(super) grid: crate::config::GridConfig,
}

impl RiskConfigSnapshot {
    pub(super) fn from_config(config: &crate::config::AppConfig) -> Self {
        Self {
            is_paper: config.exchange.is_paper(),
            sync_interval_secs: config.exchange.sync_interval_secs,
            grid: config.grid,
        }
    }
}
pub(super) fn opening_exposure_usdc<'a>(
    direction: GridDirection,
    position_size: Decimal,
    mark_price: Decimal,
    orders: impl IntoIterator<Item = &'a GridOrder>,
) -> Decimal {
    let Some(mut total) = direction
        .position_quantity(position_size)
        .checked_mul(mark_price)
    else {
        return Decimal::MAX;
    };
    for order in orders
        .into_iter()
        .filter(|order| order.side == direction.opening_side())
    {
        let Some(notional) = order.price.checked_mul(order.quantity) else {
            return Decimal::MAX;
        };
        let Some(next) = total.checked_add(notional) else {
            return Decimal::MAX;
        };
        total = next;
    }
    total
}

#[cfg(test)]
pub(super) fn buy_exposure_usdc(
    position_size: Decimal,
    mark_price: Decimal,
    orders: &[GridOrder],
) -> Decimal {
    opening_exposure_usdc(GridDirection::Long, position_size, mark_price, orders)
}

/// Distance from `mark` to `liquidation` as a percentage of `mark`, only on the losing side of
/// the position (below mark for longs, above for shorts). Binance reports 0 when no liquidation
/// price applies, which never counts as close.
pub(super) fn liquidation_too_close(
    size: Decimal,
    mark: Decimal,
    liquidation: Decimal,
    threshold_pct: Option<Decimal>,
) -> bool {
    let Some(threshold) = threshold_pct else {
        return false;
    };
    if size.is_zero() || mark <= Decimal::ZERO || liquidation <= Decimal::ZERO {
        return false;
    }
    let Some(gap) = (if size > Decimal::ZERO {
        mark.checked_sub(liquidation)
    } else {
        liquidation.checked_sub(mark)
    }) else {
        return true;
    };
    if gap <= Decimal::ZERO {
        return true;
    }
    let (Some(lhs), Some(rhs)) = (
        gap.checked_mul(Decimal::ONE_HUNDRED),
        threshold.checked_mul(mark),
    ) else {
        return true;
    };
    lhs <= rhs
}

impl GridTradingEngine {
    pub(super) fn mark_price_is_fresh(ticker: &TickerInfo) -> bool {
        ticker.mark_price_is_fresh(Utc::now())
    }

    pub(super) fn position_mark_is_fresh(
        &self,
        sync_interval_secs: u64,
        position: &PositionInfo,
    ) -> bool {
        let max_age = max_reconciliation_age_secs(sync_interval_secs);
        position.mark_price > Decimal::ZERO
            && position
                .update_time
                .is_some_and(|time| is_timestamp_fresh(time, Utc::now(), max_age))
    }

    pub(super) fn position_valuation_price(
        &self,
        sync_interval_secs: u64,
        position: &PositionInfo,
        ticker: &TickerInfo,
    ) -> Decimal {
        position.valuation_price_with_freshness(
            ticker,
            Self::mark_price_is_fresh(ticker),
            self.position_mark_is_fresh(sync_interval_secs, position),
        )
    }

    /// Check filled position value independently of opening-order reservations.
    pub(super) async fn pause_if_position_limit_reached(&mut self) -> bool {
        if let Some(global) = self.state.global_limit() {
            if self
                .state
                .config
                .read()
                .await
                .global_max_position_notional
                .is_some()
                && !global
                    .membership_ready
                    .load(std::sync::atomic::Ordering::SeqCst)
            {
                return true;
            }
            if global.check().await {
                return true;
            }
        }
        if *self.state.status.read().await != BotStatus::Running {
            return false;
        }
        let config = self.state.config.read().await;
        if !config.grid.pause_on_max_position {
            return false;
        }
        let Some(limit) = config
            .grid
            .max_position_usdc
            .filter(|limit| *limit > Decimal::ZERO)
        else {
            return false;
        };
        let sync_interval_secs = config.exchange.sync_interval_secs;
        drop(config);
        let position = self.state.position.read().await;
        let ticker = self.state.ticker.read().await;
        let valuation_price = self.position_valuation_price(sync_interval_secs, &position, &ticker);
        let position_size = position.size;
        drop(ticker);
        drop(position);
        if valuation_price <= Decimal::ZERO {
            return false;
        }
        let position_value = position_size
            .abs()
            .checked_mul(valuation_price)
            .unwrap_or(Decimal::MAX);
        if position_value < limit {
            return false;
        }

        self.pause_for_position_limit(format!(
            "持仓名义价值 {} 已达到最大持仓名义价值 {}",
            position_value, limit
        ))
        .await;
        true
    }

    /// True while opening must be suspended without pausing the strategy: the filled position
    /// value is at the cap in cancel-opening mode, or the mark price is within the configured
    /// distance of the liquidation price.
    pub(super) async fn opening_suspended_at_limit(&self, risk: RiskConfigSnapshot) -> bool {
        let cap = risk.grid.max_position_usdc.filter(|limit| {
            *limit > Decimal::ZERO
                && risk.grid.cancel_opening_on_max_position
                && !risk.grid.pause_on_max_position
        });
        if cap.is_none() && risk.grid.min_liquidation_distance_pct.is_none() {
            return false;
        }
        let position = self.state.position.read().await;
        let size = position.size;
        let liquidation_price = position.liquidation_price;
        let liquidation_guard =
            risk.grid.min_liquidation_distance_pct.is_some() && liquidation_price > Decimal::ZERO;
        if size.is_zero() || (cap.is_none() && !liquidation_guard) {
            return false;
        }
        let ticker = self.state.ticker.read().await;
        // Market marks follow the existing 15-second market-data freshness limit.
        // Position marks follow the account reconciliation freshness allowance.
        let price = if Self::mark_price_is_fresh(&ticker) {
            ticker.mark_price
        } else if self.position_mark_is_fresh(risk.sync_interval_secs, &position) {
            position.mark_price
        } else {
            // Last trades cannot establish liquidation distance. Keep openings
            // suspended until a fresh mark can establish that protection is safe.
            return true;
        };
        drop(ticker);
        drop(position);
        if cap.is_some_and(|limit| {
            size.abs()
                .checked_mul(price)
                .is_none_or(|notional| notional >= limit)
        }) {
            return true;
        }
        liquidation_too_close(
            size,
            price,
            liquidation_price,
            risk.grid.min_liquidation_distance_pct,
        )
    }

    /// Cancel only idle opening orders; partial executions retain their existing protection.
    /// Keep this outside fill recording so cancellation cannot recurse through another fill.
    pub(super) async fn cancel_suspended_opening_orders(
        &mut self,
        risk: RiskConfigSnapshot,
    ) -> bool {
        if self.state.is_shutting_down() || *self.state.status.read().await != BotStatus::Running {
            return true;
        }
        if !self.opening_suspended_at_limit(risk).await {
            return true;
        }
        let mut openings: Vec<_> = self
            .state
            .active_orders
            .read()
            .await
            .values()
            .filter(|order| {
                order.side == risk.grid.direction.opening_side() && is_window_order(order)
            })
            .cloned()
            .collect();
        openings.sort_by(|a, b| a.client_order_id.cmp(&b.client_order_id));
        let mut canceled = true;
        for order in openings {
            if *self.state.status.read().await != BotStatus::Running {
                return false;
            }
            canceled &= self.cancel_single_order(&order).await;
        }
        canceled
    }

    pub(super) async fn protect_openings_and_check_limits(
        &mut self,
        risk: RiskConfigSnapshot,
    ) -> bool {
        let protected = self.cancel_suspended_opening_orders(risk).await;
        let paused = self.pause_if_position_limit_reached().await;
        protected && !paused
    }

    pub(super) async fn pause_for_position_limit(&mut self, reason: String) {
        // Pause before canceling so cancellation cannot restore Running or replenish orders.
        self.pause_trading().await;
        self.state
            .add_log("WARN", format!("{}，策略已暂停，需手动恢复", reason))
            .await;
        if !self.cancel_all_orders().await {
            self.state
                .add_log(
                    "ERROR",
                    "策略已暂停，但部分策略挂单可能尚未撤销，请检查交易所挂单",
                )
                .await;
        }
    }

    pub(super) async fn position_direction_ready(&mut self) -> bool {
        let direction = self.state.config.read().await.grid.direction;
        if self
            .state
            .position
            .read()
            .await
            .size
            .checked_mul(direction.sign())
            .is_some_and(|signed| signed >= Decimal::ZERO)
        {
            return true;
        }
        self.pause_trading().await;
        self.state
            .add_log("ERROR", "持仓方向与策略方向不一致，策略已暂停")
            .await;
        self.cancel_all_orders().await;
        false
    }
}
