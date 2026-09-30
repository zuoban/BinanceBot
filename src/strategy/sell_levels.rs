//! Restore bought-level reservations from the durable fill journal.
//! Historical sells do not restrict the current inventory sell window.
use super::*;

pub(super) struct SellLevelLedger {
    symbol: String,
    mode: TradingMode,
    interval: Decimal,
    rules: SymbolRules,
    cursor: i64,
    bought: HashMap<Decimal, Decimal>,
}

impl GridTradingEngine {
    pub(super) async fn waiting_buy_levels(&mut self) -> Result<Vec<Decimal>> {
        self.refresh_level_ledger().await?;
        Ok(self
            .sell_levels
            .as_ref()
            .unwrap()
            .bought
            .keys()
            .copied()
            .collect())
    }

    async fn refresh_level_ledger(&mut self) -> Result<()> {
        let config = self.state.config.read().await.clone();
        let mode = TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet);
        let rules = self.state.rules.read().await.clone();
        let reset = self.sell_levels.as_ref().is_none_or(|ledger| {
            ledger.symbol != config.exchange.symbol
                || ledger.mode != mode
                || ledger.interval != config.grid.grid_interval
                || ledger.rules.step_size != rules.step_size
                || ledger.rules.tick_size != rules.tick_size
        });
        if reset {
            self.sell_levels = Some(SellLevelLedger {
                symbol: config.exchange.symbol.clone(),
                mode,
                interval: config.grid.grid_interval,
                rules: rules.clone(),
                cursor: 0,
                bought: HashMap::new(),
            });
        }
        let ledger = self.sell_levels.as_mut().unwrap();
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
