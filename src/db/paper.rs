use super::Database;
use crate::types::{AccountInfo, PositionInfo, PAPER_INITIAL_BALANCE};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::Connection;
use rust_decimal::Decimal;
use std::collections::{BTreeMap, HashMap, HashSet};

struct Contribution {
    asset: String,
    wallet_delta: Decimal,
    unrealized_pnl: Decimal,
    position_margin: Decimal,
    size: Decimal,
    entry_price: Decimal,
    open_valuation_time: Option<DateTime<Utc>>,
    update_time: DateTime<Utc>,
    committed: bool,
}

impl Contribution {
    fn new(position: &PositionInfo, account: &AccountInfo, committed: bool) -> Self {
        let mut contribution = Self {
            asset: account.asset.clone(),
            wallet_delta: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            position_margin: Decimal::ZERO,
            size: Decimal::ZERO,
            entry_price: Decimal::ZERO,
            open_valuation_time: None,
            update_time: account.update_time,
            committed,
        };
        contribution.refresh(position, account, committed);
        contribution
    }

    fn refresh(&mut self, position: &PositionInfo, account: &AccountInfo, committed: bool) {
        let valuation_price = if position.mark_price > Decimal::ZERO {
            position.mark_price
        } else {
            position.entry_price
        };
        self.wallet_delta = account.total_wallet_balance - PAPER_INITIAL_BALANCE;
        self.unrealized_pnl = position.unrealized_pnl;
        self.position_margin =
            position.size.abs() * valuation_price / Decimal::from(position.leverage.max(1));
        self.size = position.size;
        self.entry_price = position.entry_price;
        // Paper valuation timestamps live on the local account in older states.
        self.open_valuation_time = (!position.size.is_zero()).then(|| {
            position
                .update_time
                .unwrap_or(account.update_time)
                .min(account.update_time)
        });
        self.update_time = account.update_time;
        self.committed |= committed;
    }

    fn accepts_valuation(&self, position: &PositionInfo, account: &AccountInfo) -> bool {
        // A snapshot captured before a newer transaction cannot roll its wallet
        // or holdings back after that transaction has published its contribution.
        (!self.committed
            || (self.wallet_delta == account.total_wallet_balance - PAPER_INITIAL_BALANCE
                && self.size == position.size
                && self.entry_price == position.entry_price))
            && account.update_time >= self.update_time
            && self.open_valuation_time.is_none_or(|previous| {
                position.update_time.unwrap_or(account.update_time) >= previous
            })
    }
}

#[derive(Default)]
struct AssetLedger {
    wallet_delta: Decimal,
    unrealized_pnl: Decimal,
    position_margin: Decimal,
    open_valuation_times: BTreeMap<DateTime<Utc>, usize>,
    latest_update: DateTime<Utc>,
}

impl AssetLedger {
    fn remove(&mut self, contribution: &Contribution) {
        self.wallet_delta -= contribution.wallet_delta;
        self.unrealized_pnl -= contribution.unrealized_pnl;
        self.position_margin -= contribution.position_margin;
        if let Some(time) = contribution.open_valuation_time {
            let count = self.open_valuation_times.get_mut(&time).unwrap();
            *count -= 1;
            if *count == 0 {
                self.open_valuation_times.remove(&time);
            }
        }
    }

    fn add(&mut self, contribution: &Contribution) {
        self.wallet_delta += contribution.wallet_delta;
        self.unrealized_pnl += contribution.unrealized_pnl;
        self.position_margin += contribution.position_margin;
        if let Some(time) = contribution.open_valuation_time {
            *self.open_valuation_times.entry(time).or_default() += 1;
        }
        self.latest_update = self.latest_update.max(contribution.update_time);
    }

    fn account(&self, asset: &str) -> AccountInfo {
        let total_wallet_balance = PAPER_INITIAL_BALANCE + self.wallet_delta;
        let margin_balance = total_wallet_balance + self.unrealized_pnl;
        AccountInfo {
            asset: asset.to_owned(),
            total_wallet_balance,
            available_balance: margin_balance - self.position_margin,
            margin_balance,
            unrealized_profit: self.unrealized_pnl,
            update_time: self
                .open_valuation_times
                .first_key_value()
                .map(|(time, _)| *time)
                .unwrap_or(self.latest_update),
        }
    }
}

/// Compact process-local contributions; trade history is never the balance source.
#[derive(Default)]
pub(super) struct PaperLedger {
    symbols: HashMap<String, Contribution>,
    assets: HashMap<String, AssetLedger>,
    unresolved_legacy: HashMap<String, HashSet<String>>,
}

impl PaperLedger {
    pub(super) fn load(conn: &Connection) -> Result<Self> {
        let mut ledger = Self::default();
        let mut accounts =
            conn.prepare("SELECT symbol, position_json, account_json FROM paper_accounts")?;
        let mut rows = accounts.query([])?;
        while let Some(row) = rows.next()? {
            let symbol: String = row.get(0)?;
            let position: PositionInfo = serde_json::from_str(&row.get::<_, String>(1)?)
                .with_context(|| format!("Invalid paper position for {symbol}"))?;
            let account: AccountInfo = serde_json::from_str(&row.get::<_, String>(2)?)
                .with_context(|| format!("Invalid paper account for {symbol}"))?;
            anyhow::ensure!(
                !account.asset.is_empty(),
                "Missing paper asset for {symbol}"
            );
            ledger.publish(&symbol, &position, &account, true);
        }
        let mut legacy = conn.prepare(
            "SELECT symbol FROM trades WHERE mode IN ('PAPER', 'UNKNOWN') \
             UNION SELECT symbol FROM execution_frontiers WHERE mode IN ('PAPER', 'UNKNOWN') \
             UNION SELECT symbol FROM cleared_pair_openings WHERE mode IN ('PAPER', 'UNKNOWN')",
        )?;
        let mut rows = legacy.query([])?;
        while let Some(row) = rows.next()? {
            let symbol: String = row.get(0)?;
            ledger.record_legacy(&symbol);
        }
        Ok(ledger)
    }

    pub(super) fn record_legacy(&mut self, symbol: &str) {
        // An already registered in-process baseline remains known even when a
        // caller records analysis-only trade history without a state update.
        // A fresh process still rejects that history unless a durable row exists.
        if self.symbols.contains_key(symbol) {
            return;
        }
        let asset = crate::types::quote_asset(symbol).unwrap_or(symbol);
        self.unresolved_legacy
            .entry(asset.to_owned())
            .or_default()
            .insert(symbol.to_owned());
    }

    pub(super) fn commit(&mut self, symbol: &str, position: &PositionInfo, account: &AccountInfo) {
        let asset = crate::types::quote_asset(symbol).unwrap_or(symbol);
        if let Some(unresolved) = self.unresolved_legacy.get_mut(asset) {
            unresolved.remove(symbol);
            if unresolved.is_empty() {
                self.unresolved_legacy.remove(asset);
            }
        }
        self.publish(symbol, position, account, true);
    }

    fn publish(
        &mut self,
        symbol: &str,
        position: &PositionInfo,
        account: &AccountInfo,
        committed: bool,
    ) {
        if let Some(previous) = self.symbols.get_mut(symbol) {
            if !committed && !previous.accepts_valuation(position, account) {
                return;
            }
            let totals = self.assets.get_mut(&previous.asset).unwrap();
            totals.remove(previous);
            if previous.asset == account.asset {
                previous.refresh(position, account, committed);
                totals.add(previous);
                return;
            }
            previous.asset.clone_from(&account.asset);
            previous.refresh(position, account, committed);
            self.assets
                .entry(account.asset.clone())
                .or_default()
                .add(previous);
            return;
        }
        let contribution = Contribution::new(position, account, committed);
        self.assets
            .entry(account.asset.clone())
            .or_default()
            .add(&contribution);
        self.symbols.insert(symbol.to_owned(), contribution);
    }

    fn shared(
        &mut self,
        symbol: &str,
        position: &PositionInfo,
        account: &AccountInfo,
    ) -> Result<AccountInfo> {
        anyhow::ensure!(
            !account.asset.is_empty(),
            "Missing paper asset for {symbol}"
        );
        if let Some(unresolved) = self.unresolved_legacy.get(&account.asset) {
            anyhow::bail!(
                "旧模拟盘状态无法从成交历史可靠恢复：{}，请先人工核对并迁移仓位和余额",
                unresolved.iter().next().unwrap()
            );
        }
        self.publish(symbol, position, account, false);
        Ok(self
            .assets
            .get(&account.asset)
            .unwrap()
            .account(&account.asset))
    }
}

impl Database {
    /// Publish one committed local subledger's valuation and return its shared wallet.
    /// The same Database owns the cache; steady-state publication never queries SQLite.
    pub(crate) fn shared_paper_account(
        &self,
        symbol: &str,
        position: &PositionInfo,
        account: &AccountInfo,
    ) -> Result<AccountInfo> {
        {
            let mut ledger = self.paper_ledger.lock().unwrap();
            if let Some(ledger) = ledger.as_mut() {
                return ledger.shared(symbol, position, account);
            }
        }
        // Writers use this same connection -> ledger lock order. Loading all
        // committed rows and installing the cache is atomic with their commits.
        let conn = self.lock_connection("shared_paper_account");
        let mut ledger = self.paper_ledger.lock().unwrap();
        if ledger.is_none() {
            *ledger = Some(PaperLedger::load(&conn)?);
        }
        ledger.as_mut().unwrap().shared(symbol, position, account)
    }
}
