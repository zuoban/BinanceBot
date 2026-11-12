use super::{evidence::load_terminal_order_disagreements_on, Database};
use crate::types::{GridOrder, RemainderPlan, TradingMode};
use anyhow::Result;
use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};

pub(crate) type LiveSyncPreflightDbResult = std::result::Result<
    (Vec<GridOrder>, Option<RemainderPlan>, Option<Result<bool>>),
    (bool, anyhow::Error),
>;

fn load_pair_intents_on(
    conn: &Connection,
    symbol: &str,
    mode: TradingMode,
) -> Result<Vec<GridOrder>> {
    super::query_json_vec(
        conn,
        "SELECT order_json FROM pair_intents WHERE symbol = ?1 AND mode = ?2 ORDER BY created_at;",
        params![symbol, mode.as_str()],
        "pair_intents",
    )
}

fn load_remainder_plan_on(
    conn: &Connection,
    symbol: &str,
    mode: TradingMode,
) -> Result<Option<RemainderPlan>> {
    let json: Option<String> = conn
        .prepare_cached(
            "SELECT plan_json FROM remainder_plans WHERE symbol = ?1 AND mode = ?2 AND active = 1;",
        )?
        .query_row(params![symbol, mode.as_str()], |row| row.get(0))
        .optional()?;
    json.map(|json| serde_json::from_str(&json).map_err(Into::into))
        .transpose()
}

impl Database {
    pub fn load_pair_intents(&self, symbol: &str, mode: TradingMode) -> Result<Vec<GridOrder>> {
        let conn = self.lock_connection("load_pair_intents");
        load_pair_intents_on(&conn, symbol, mode)
    }

    pub(crate) fn load_live_sync_preflight(
        &self,
        account: [u8; 32],
        symbol: &str,
        mode: TradingMode,
        check_disagreements: bool,
    ) -> LiveSyncPreflightDbResult {
        let conn = self.lock_connection("load_live_sync_preflight");
        let intents = match load_pair_intents_on(&conn, symbol, mode) {
            Ok(orders) => orders,
            Err(err) => return Err((true, err)),
        };
        let plan = match load_remainder_plan_on(&conn, symbol, mode) {
            Ok(plan) => plan,
            Err(err) => return Err((false, err)),
        };
        let disagreements_blocked = check_disagreements.then(|| {
            load_terminal_order_disagreements_on(&conn, account, symbol)
                .map(|records| !records.is_empty())
        });
        Ok((intents, plan, disagreements_blocked))
    }

    pub(crate) fn delete_pair_intent(&self, parent_client_order_id: &str) -> Result<()> {
        let conn = self.lock_connection("delete_pair_intent");
        conn.prepare_cached("DELETE FROM pair_intents WHERE parent_client_order_id = ?1;")?
            .execute(params![parent_client_order_id])?;
        Ok(())
    }

    pub(crate) fn clear_pair_intents(&self, symbol: &str, mode: TradingMode) -> Result<()> {
        let conn = self.lock_connection("clear_pair_intents");
        conn.prepare_cached("DELETE FROM pair_intents WHERE symbol = ?1 AND mode = ?2;")?
            .execute(params![symbol, mode.as_str()])?;
        Ok(())
    }

    pub(crate) fn save_remainder_plan(&self, plan: &RemainderPlan) -> Result<()> {
        let active = matches!(
            plan.phase,
            crate::types::RemainderPhase::Canceling | crate::types::RemainderPhase::Submitting
        );
        let plan_json = serde_json::to_string(plan)?;
        let updated_at = Utc::now().to_rfc3339();
        let mut conn = self.lock_connection("save_remainder_plan");
        let tx = super::immediate_tx(&mut conn)?;
        tx.prepare_cached(
            "INSERT INTO remainder_plans (target_client_order_id, symbol, mode, active, plan_json, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT(target_client_order_id) DO UPDATE SET active = excluded.active, plan_json = excluded.plan_json, updated_at = excluded.updated_at;",
        )?
        .execute(params![
            plan.target.client_order_id,
            plan.symbol,
            plan.mode.as_str(),
            active,
            plan_json,
            updated_at
        ])?;
        if !active {
            tx.prepare_cached(
                "DELETE FROM remainder_plans WHERE symbol = ?1 AND mode = ?2 AND active = 0 AND rowid NOT IN (SELECT rowid FROM remainder_plans WHERE symbol = ?1 AND mode = ?2 AND active = 0 ORDER BY rowid DESC LIMIT 64);",
            )?
            .execute(params![plan.symbol, plan.mode.as_str()])?;
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn load_remainder_plan(
        &self,
        symbol: &str,
        mode: TradingMode,
    ) -> Result<Option<RemainderPlan>> {
        let conn = self.lock_connection("load_remainder_plan");
        load_remainder_plan_on(&conn, symbol, mode)
    }

    pub fn save_managed_order(&self, order: &GridOrder) -> Result<()> {
        let json = serde_json::to_string(order)?;
        let conn = self.lock_connection("save_managed_order");
        conn.prepare_cached(
            "INSERT INTO managed_orders (client_order_id, symbol, order_json, updated_at) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(client_order_id) DO UPDATE SET symbol = excluded.symbol, order_json = excluded.order_json, updated_at = excluded.updated_at;",
        )?
        .execute(params![order.client_order_id, order.symbol, json, Utc::now().to_rfc3339()])?;
        Ok(())
    }

    pub(crate) fn save_managed_orders(&self, orders: &[GridOrder]) -> Result<()> {
        let mut conn = self.lock_connection("save_managed_orders");
        let tx = super::immediate_tx(&mut conn)?;
        {
            let mut statement = tx.prepare_cached(
                "INSERT INTO managed_orders (client_order_id, symbol, order_json, updated_at) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(client_order_id) DO UPDATE SET symbol = excluded.symbol, order_json = excluded.order_json, updated_at = excluded.updated_at;",
            )?;
            for order in orders {
                statement.execute(params![
                    order.client_order_id,
                    order.symbol,
                    serde_json::to_string(order)?,
                    Utc::now().to_rfc3339(),
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn save_managed_orders_if_absent(&self, orders: &[GridOrder]) -> Result<()> {
        let mut conn = self.lock_connection("save_managed_orders_if_absent");
        let tx = super::immediate_tx(&mut conn)?;
        {
            let mut statement = tx.prepare_cached(
                "INSERT INTO managed_orders (client_order_id, symbol, order_json, updated_at) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(client_order_id) DO NOTHING;",
            )?;
            for order in orders {
                let inserted = statement.execute(params![
                    order.client_order_id,
                    order.symbol,
                    serde_json::to_string(order)?,
                    Utc::now().to_rfc3339(),
                ])?;
                if inserted == 0 {
                    let (stored_symbol, stored_json): (String, String) = tx
                        .prepare_cached(
                            "SELECT symbol, order_json FROM managed_orders WHERE client_order_id = ?1;",
                        )?
                        .query_row(params![order.client_order_id], |row| {
                            Ok((row.get(0)?, row.get(1)?))
                        })?;
                    let stored: GridOrder = serde_json::from_str(&stored_json)?;
                    if stored.client_order_id != order.client_order_id
                        || stored_symbol != order.symbol
                        || stored.symbol != order.symbol
                        || stored.side != order.side
                        || stored.order_id != order.order_id
                    {
                        anyhow::bail!(
                            "managed order identity conflict for client order ID {}",
                            order.client_order_id
                        );
                    }
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn load_managed_orders(&self, symbol: &str) -> Result<Vec<GridOrder>> {
        let conn = self.lock_connection("load_managed_orders");
        super::query_json_vec(
            &conn,
            "SELECT order_json FROM managed_orders WHERE symbol = ?1;",
            params![symbol],
            "managed_orders",
        )
    }

    pub(crate) fn delete_managed_order(&self, client_order_id: &str) -> Result<()> {
        let conn = self.lock_connection("delete_managed_order");
        conn.prepare_cached("DELETE FROM managed_orders WHERE client_order_id = ?1;")?
            .execute(params![client_order_id])?;
        Ok(())
    }

    pub(crate) fn delete_managed_orders(&self, client_order_ids: &[String]) -> Result<()> {
        if client_order_ids.is_empty() {
            return Ok(());
        }
        if client_order_ids.len() == 1 {
            return self.delete_managed_order(&client_order_ids[0]);
        }
        let mut conn = self.lock_connection("delete_managed_orders");
        let tx = super::immediate_tx(&mut conn)?;
        {
            let mut stmt =
                tx.prepare_cached("DELETE FROM managed_orders WHERE client_order_id = ?1;")?;
            for id in client_order_ids {
                stmt.execute(params![id])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn clear_managed_orders(&self, symbol: &str) -> Result<()> {
        let conn = self.lock_connection("clear_managed_orders");
        conn.prepare_cached("DELETE FROM managed_orders WHERE symbol = ?1;")?
            .execute(params![symbol])?;
        Ok(())
    }
}
