use super::{
    config_runtime::save_pair_status_on, immediate_tx, migrations::has_legacy_paper_history_on,
    Database,
};
use crate::types::{AccountInfo, BotStatus, PositionInfo, TradingMode, PAPER_INITIAL_BALANCE};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
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
        self.wallet_delta = account
            .total_wallet_balance
            .saturating_sub(PAPER_INITIAL_BALANCE);
        self.unrealized_pnl = position.unrealized_pnl;
        self.position_margin = position.position_margin();
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
            || (self.wallet_delta
                == account
                    .total_wallet_balance
                    .saturating_sub(PAPER_INITIAL_BALANCE)
                && self.size == position.size
                && self.entry_price == position.entry_price))
            && account.update_time >= self.update_time
            && self.open_valuation_time.is_none_or(|previous| {
                position.update_time.unwrap_or(account.update_time) >= previous
            })
    }

    fn is_unchanged(&self, position: &PositionInfo, account: &AccountInfo) -> bool {
        let open_time = (!position.size.is_zero()).then(|| {
            position
                .update_time
                .unwrap_or(account.update_time)
                .min(account.update_time)
        });
        self.asset == account.asset
            && self.wallet_delta
                == account
                    .total_wallet_balance
                    .saturating_sub(PAPER_INITIAL_BALANCE)
            && self.unrealized_pnl == position.unrealized_pnl
            && self.position_margin == position.position_margin()
            && self.size == position.size
            && self.entry_price == position.entry_price
            && self.open_valuation_time == open_time
            && self.update_time == account.update_time
    }
}

#[derive(Default)]
struct AssetLedger {
    wallet_delta: Decimal,
    unrealized_pnl: Decimal,
    position_margin: Decimal,
    saturated: bool,
    open_valuation_times: BTreeMap<DateTime<Utc>, usize>,
    latest_update: DateTime<Utc>,
}

impl AssetLedger {
    fn remove(&mut self, contribution: &Contribution) {
        match (
            self.wallet_delta.checked_sub(contribution.wallet_delta),
            self.unrealized_pnl.checked_sub(contribution.unrealized_pnl),
            self.position_margin
                .checked_sub(contribution.position_margin),
        ) {
            (Some(wallet), Some(pnl), Some(margin)) if !self.saturated => {
                self.wallet_delta = wallet;
                self.unrealized_pnl = pnl;
                self.position_margin = margin;
            }
            _ => {
                self.saturated = true;
            }
        }
        if let Some(time) = contribution.open_valuation_time {
            if let Some(count) = self.open_valuation_times.get_mut(&time) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.open_valuation_times.remove(&time);
                }
            }
        }
    }

    fn add(&mut self, contribution: &Contribution) {
        match (
            self.wallet_delta.checked_add(contribution.wallet_delta),
            self.unrealized_pnl.checked_add(contribution.unrealized_pnl),
            self.position_margin
                .checked_add(contribution.position_margin),
        ) {
            (Some(wallet), Some(pnl), Some(margin)) if !self.saturated => {
                self.wallet_delta = wallet;
                self.unrealized_pnl = pnl;
                self.position_margin = margin;
            }
            _ => {
                self.saturated = true;
            }
        }
        if let Some(time) = contribution.open_valuation_time {
            *self.open_valuation_times.entry(time).or_default() += 1;
        }
        self.latest_update = self.latest_update.max(contribution.update_time);
    }

    fn recompute<'a>(&mut self, contributions: impl Iterator<Item = &'a Contribution>) {
        let mut wallet = Decimal::ZERO;
        let mut pnl = Decimal::ZERO;
        let mut margin = Decimal::ZERO;
        let mut saturated = false;
        for item in contributions {
            match wallet.checked_add(item.wallet_delta) {
                Some(next) => wallet = next,
                None => {
                    wallet = wallet.saturating_add(item.wallet_delta);
                    saturated = true;
                }
            }
            match pnl.checked_add(item.unrealized_pnl) {
                Some(next) => pnl = next,
                None => {
                    pnl = pnl.saturating_add(item.unrealized_pnl);
                    saturated = true;
                }
            }
            match margin.checked_add(item.position_margin) {
                Some(next) => margin = next,
                None => {
                    margin = margin.saturating_add(item.position_margin);
                    saturated = true;
                }
            }
        }
        self.wallet_delta = wallet;
        self.unrealized_pnl = pnl;
        self.position_margin = margin;
        self.saturated = saturated;
    }

    fn account(&self, asset: &str) -> AccountInfo {
        let total_wallet_balance = PAPER_INITIAL_BALANCE.saturating_add(self.wallet_delta);
        let margin_balance = total_wallet_balance.saturating_add(self.unrealized_pnl);
        AccountInfo {
            asset: asset.to_owned(),
            total_wallet_balance,
            available_balance: margin_balance.saturating_sub(self.position_margin),
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
            if !committed
                && (!previous.accepts_valuation(position, account)
                    || previous.is_unchanged(position, account))
            {
                return;
            }
            if let Some(totals) = self.assets.get_mut(&previous.asset) {
                totals.remove(previous);
                if previous.asset == account.asset {
                    previous.refresh(position, account, committed);
                    totals.add(previous);
                    return;
                }
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
        if let Some(first_unresolved) = self
            .unresolved_legacy
            .get(&account.asset)
            .and_then(|unresolved| unresolved.iter().next())
        {
            anyhow::bail!(
                "旧模拟盘状态无法从成交历史可靠恢复：{first_unresolved}，请先人工核对并迁移仓位和余额"
            );
        }
        self.publish(symbol, position, account, false);
        let totals = self.assets.entry(account.asset.clone()).or_default();
        if totals.saturated {
            totals.recompute(
                self.symbols
                    .values()
                    .filter(|item| item.asset == account.asset),
            );
        }
        Ok(totals.account(&account.asset))
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
            let mut ledger = self.paper_ledger.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(ledger) = ledger.as_mut() {
                return ledger.shared(symbol, position, account);
            }
        }
        // Writers hold `conn` until after updating `paper_ledger`, so loading all
        // committed rows under `conn` -> `paper_ledger` is atomic with their commits.
        let conn = self.lock_connection("shared_paper_account");
        let mut ledger = self.paper_ledger.lock().unwrap_or_else(|e| e.into_inner());
        let ledger = match ledger.as_mut() {
            Some(existing) => existing,
            None => ledger.insert(PaperLedger::load(&conn)?),
        };
        ledger.shared(symbol, position, account)
    }

    /// Pre-load the shared paper ledger on a blocking thread so later runtime
    /// transitions from Live to Paper never touch SQLite on Tokio worker threads.
    pub(crate) fn warm_paper_ledger(&self) {
        if self
            .paper_ledger
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
        {
            return;
        }
        let conn = self.lock_connection("warm_paper_ledger");
        let mut ledger = self.paper_ledger.lock().unwrap_or_else(|e| e.into_inner());
        if ledger.is_none() {
            if let Ok(loaded) = PaperLedger::load(&conn) {
                *ledger = Some(loaded);
            }
        }
    }

    pub(crate) fn load_paper_state(
        &self,
        symbol: &str,
    ) -> Result<Option<(PositionInfo, AccountInfo)>> {
        let conn = self.lock_connection("load_paper_state");
        let saved: Option<(String, String)> = conn
            .query_row(
                "SELECT position_json, account_json FROM paper_accounts WHERE symbol = ?1",
                params![symbol],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((position, account)) = saved {
            return Ok(Some((
                serde_json::from_str(&position)?,
                serde_json::from_str(&account)?,
            )));
        }
        // Older versions never stored the account. Cleared history and earlier
        // restarts make replay ambiguous, so do not invent a balance or position.
        let legacy = has_legacy_paper_history_on(&conn, symbol)?;
        anyhow::ensure!(
            !legacy,
            "旧模拟盘状态无法从成交历史可靠恢复：{}，请先人工核对并迁移仓位和余额",
            symbol
        );
        Ok(None)
    }

    /// Explicitly establish a new paper baseline; this cannot recover old holdings.
    pub fn initialize_legacy_paper_account(&self, symbol: &str) -> Result<()> {
        let mut conn = self.lock_connection("initialize_legacy_paper_account");
        let tx = immediate_tx(&mut conn)?;
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM paper_accounts WHERE symbol = ?1)",
            params![symbol],
            |row| row.get(0),
        )?;
        anyhow::ensure!(!exists, "该交易对已有模拟盘状态，拒绝覆盖");
        anyhow::ensure!(
            has_legacy_paper_history_on(&tx, symbol)?,
            "该交易对没有需要迁移的旧模拟盘记录"
        );
        let position = PositionInfo {
            symbol: symbol.to_string(),
            ..PositionInfo::default()
        };
        let account = AccountInfo::initial_paper(symbol, Some(Utc::now()));
        tx.execute(
            "INSERT INTO paper_accounts (symbol, position_json, account_json) VALUES (?1, ?2, ?3)",
            params![
                symbol,
                serde_json::to_string(&position)?,
                serde_json::to_string(&account)?
            ],
        )?;
        save_pair_status_on(&tx, symbol, TradingMode::Paper, BotStatus::Paused)?;
        tx.commit()?;
        if let Some(ledger) = self
            .paper_ledger
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_mut()
        {
            ledger.commit(symbol, &position, &account);
        }
        Ok(())
    }
}
