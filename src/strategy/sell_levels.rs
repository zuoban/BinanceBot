//! Restore per-level sell/buy alternation from the durable fill journal.
//! No extra write can be lost between recording a fill and reserving its level.
use super::*;
use std::collections::HashSet;

pub(super) struct SellLevelLedger {
    symbol: String,
    mode: TradingMode,
    interval: Decimal,
    amount: Decimal,
    rules: SymbolRules,
    cursor: i64,
    waiting: HashSet<Decimal>,
}

impl GridTradingEngine {
    pub(super) async fn waiting_sell_levels(&mut self) -> Result<HashSet<Decimal>> {
        let config = self.state.config.read().await.clone();
        let mode = TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet);
        let rules = self.state.rules.read().await.clone();
        let reset = self.sell_levels.as_ref().is_none_or(|ledger| {
            ledger.symbol != config.exchange.symbol
                || ledger.mode != mode
                || ledger.interval != config.grid.grid_interval
                || ledger.amount != config.grid.order_amount_usdc
                || ledger.rules.step_size != rules.step_size
                || ledger.rules.tick_size != rules.tick_size
        });
        if reset {
            self.sell_levels = Some(SellLevelLedger {
                symbol: config.exchange.symbol.clone(),
                mode,
                interval: config.grid.grid_interval,
                amount: config.grid.order_amount_usdc,
                rules: rules.clone(),
                cursor: 0,
                waiting: HashSet::new(),
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
                            ledger.waiting.insert(fill.price.normalize());
                        }
                        OrderSide::Buy => {
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
        Ok(ledger.waiting.clone())
    }
}
