//! Restore per-level sell/buy alternation from the durable fill journal.
//! No extra write can be lost between recording a fill and reserving its level.
use super::*;
use std::collections::HashSet;

pub(super) struct GridLevelLedger {
    symbol: String,
    mode: TradingMode,
    interval: Decimal,
    amount: Decimal,
    rules: SymbolRules,
    cursor: i64,
    waiting: HashSet<Decimal>,
    bought: HashMap<Decimal, Decimal>,
}

impl GridTradingEngine {
    pub(super) async fn waiting_grid_levels(&mut self) -> Result<(Vec<Decimal>, HashSet<Decimal>)> {
        self.refresh_level_ledger().await?;
        let ledger = self.grid_levels.as_ref().unwrap();
        Ok((
            ledger.bought.keys().copied().collect(),
            ledger.waiting.clone(),
        ))
    }

    pub(super) async fn level_is_waiting(
        &mut self,
        side: OrderSide,
        price: Decimal,
    ) -> Result<bool> {
        self.refresh_level_ledger().await?;
        let ledger = self.grid_levels.as_ref().unwrap();
        Ok(match side {
            OrderSide::Buy => has_nearby_grid_order(
                &ledger.bought.keys().copied().collect::<Vec<_>>(),
                price,
                ledger.interval,
                ledger.rules.tick_size,
            ),
            OrderSide::Sell => ledger.waiting.contains(&price),
        })
    }

    async fn refresh_level_ledger(&mut self) -> Result<()> {
        let config = self.state.config.read().await.clone();
        let mode = TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet);
        let rules = self.state.rules.read().await.clone();
        let reset = self.grid_levels.as_ref().is_none_or(|ledger| {
            ledger.symbol != config.exchange.symbol
                || ledger.mode != mode
                || ledger.interval != config.grid.grid_interval
                || ledger.amount != config.grid.order_amount_usdc
                || ledger.rules.step_size != rules.step_size
                || ledger.rules.tick_size != rules.tick_size
        });
        if reset {
            self.grid_levels = Some(GridLevelLedger {
                symbol: config.exchange.symbol.clone(),
                mode,
                interval: config.grid.grid_interval,
                amount: config.grid.order_amount_usdc,
                rules: rules.clone(),
                cursor: 0,
                waiting: HashSet::new(),
                bought: HashMap::new(),
            });
        }
        let ledger = self.grid_levels.as_mut().unwrap();
        loop {
            let symbol = ledger.symbol.clone();
            let cursor = ledger.cursor;
            let fills = self
                .state
                .db
                .run_blocking(move |db| db.grid_fills_after(&symbol, mode, cursor))
                .await?;
            let count = fills.len();
            for fill in fills {
                if fill.quantity > Decimal::ZERO {
                    match fill.side {
                        OrderSide::Sell => {
                            ledger.waiting.insert(fill.price.normalize());
                            // Keep bought levels reserved independently of open exits.
                            // A skipped/canceled exit cannot erase the acquired inventory,
                            // and a partial sell cannot release an entire bought level.
                            let mut remaining = fill.quantity;
                            let mut levels: Vec<_> = ledger
                                .bought
                                .keys()
                                .copied()
                                .filter(|buy| {
                                    ((*buy + ledger.interval) / rules.tick_size).ceil()
                                        * rules.tick_size
                                        == fill.price
                                })
                                .collect();
                            levels.sort();
                            for buy in levels {
                                let quantity = ledger.bought.get_mut(&buy).unwrap();
                                let closed = remaining.min(*quantity);
                                *quantity -= closed;
                                remaining -= closed;
                            }
                            ledger
                                .bought
                                .retain(|_, quantity| *quantity > Decimal::ZERO);
                        }
                        OrderSide::Buy => {
                            *ledger.bought.entry(fill.price.normalize()).or_default() +=
                                fill.quantity;
                            // Tiny/canceled partial buys must not reopen a full-size
                            // inventory sell. Their own equal-quantity exits remain allowed.
                            if rules
                                .calculate_quantity(fill.price, ledger.amount)
                                .is_some_and(|full| fill.quantity >= full)
                            {
                                let exit = ((fill.price + ledger.interval) / rules.tick_size)
                                    .ceil()
                                    * rules.tick_size;
                                ledger.waiting.remove(&exit);
                            }
                        }
                    }
                }
                ledger.cursor = fill.cursor;
            }
            if count < 1000 {
                break;
            }
        }
        Ok(())
    }
}
