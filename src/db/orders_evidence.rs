use super::Database;
use crate::types::{GridOrder, RemainderPlan, TradingMode};
use anyhow::Result;
use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};

impl Database {
    pub fn load_pair_intents(&self, symbol: &str, mode: TradingMode) -> Result<Vec<GridOrder>> {
        let conn = self.lock_connection("load_pair_intents");
        super::query_json_vec(
            &conn,
            "SELECT order_json FROM pair_intents WHERE symbol = ?1 AND mode = ?2 ORDER BY created_at;",
            params![symbol, mode.as_str()],
            "pair_intents",
        )
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
        let json: Option<String> = conn
            .prepare_cached(
                "SELECT plan_json FROM remainder_plans WHERE symbol = ?1 AND mode = ?2 AND active = 1;",
            )?
            .query_row(params![symbol, mode.as_str()], |row| row.get(0))
            .optional()?;
        json.map(|json| serde_json::from_str(&json).map_err(Into::into))
            .transpose()
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

    #[cfg(test)]
    pub(crate) fn save_initial_order_responses(
        &self,
        account: [u8; 32],
        symbol: &str,
        client_order_id: &str,
        responses: &[crate::exchange::BinanceOrderResponse],
    ) -> Result<()> {
        let observations: Vec<_> = responses
            .iter()
            .map(|response| {
                let id = uuid::Uuid::new_v4().to_string();
                (id.clone(), id, response.clone())
            })
            .collect();
        self.save_identified_initial_order_responses(
            account,
            symbol,
            client_order_id,
            &observations,
        )
    }

    // Reuse each observation ID on retry; allocate a fresh ID for every new observation.
    #[cfg(test)]
    pub(crate) fn save_identified_initial_order_responses(
        &self,
        account: [u8; 32],
        symbol: &str,
        client_order_id: &str,
        observations: &[(String, String, crate::exchange::BinanceOrderResponse)],
    ) -> Result<()> {
        self.save_initial_order_response_rows(
            account,
            symbol,
            &[],
            false,
            observations.iter().map(|(id, batch, response)| {
                (client_order_id, id.as_str(), batch.as_str(), response)
            }),
        )
    }

    pub(crate) fn save_initial_order_response_snapshot(
        &self,
        account: [u8; 32],
        symbol: &str,
        observations: &[(
            String,
            String,
            String,
            crate::exchange::BinanceOrderResponse,
        )],
        identities: &[(GridOrder, crate::types::ObservedOrderIdentity)],
    ) -> Result<()> {
        self.save_initial_order_response_rows(
            account,
            symbol,
            identities,
            true,
            observations.iter().map(|(client_id, id, batch, response)| {
                (client_id.as_str(), id.as_str(), batch.as_str(), response)
            }),
        )
    }

    fn save_initial_order_response_rows<'a>(
        &self,
        account: [u8; 32],
        symbol: &str,
        identities: &[(GridOrder, crate::types::ObservedOrderIdentity)],
        deduplicate_snapshot: bool,
        observations: impl IntoIterator<
            Item = (
                &'a str,
                &'a str,
                &'a str,
                &'a crate::exchange::BinanceOrderResponse,
            ),
        >,
    ) -> Result<()> {
        let mut conn = self.lock_connection("save_initial_order_responses");
        let tx = super::immediate_tx(&mut conn)?;
        Self::save_identity_observation_rows(&tx, account, identities)?;
        let observations: Vec<_> = observations
            .into_iter()
            .map(|(client_id, obs_id, batch_id, resp)| {
                Ok((
                    client_id,
                    obs_id,
                    batch_id,
                    resp,
                    serde_json::to_string(resp)?,
                ))
            })
            .collect::<Result<_>>()?;
        let duplicate_snapshot = deduplicate_snapshot
            && is_duplicate_response_snapshot(&tx, account, symbol, &observations)?;
        let duplicate_query = deduplicate_snapshot
            && is_duplicate_single_response_query(&tx, account, symbol, &observations)?;
        if !duplicate_snapshot && !duplicate_query {
            let mut statement = tx.prepare_cached(
                "INSERT INTO initial_order_responses (account_key, expected_symbol, client_order_id, observed_client_order_id, observed_symbol, observed_side, observed_order_id, response_json, observation_id, batch_id) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(observation_id) DO NOTHING",
            )?;
            for (client_order_id, observation_id, batch_id, response, json) in &observations {
                let inserted = statement.execute(params![
                    &account[..],
                    symbol,
                    client_order_id,
                    response.client_order_id,
                    response.symbol,
                    response.side,
                    response.order_id,
                    json,
                    observation_id,
                    batch_id
                ])?;
                if inserted == 0 {
                    let matches: bool = tx
                        .prepare_cached(
                            "SELECT account_key=?1 AND expected_symbol=?2 AND client_order_id=?3 AND response_json=?4 AND batch_id=?5 FROM initial_order_responses WHERE observation_id=?6",
                        )?
                        .query_row(
                            params![&account[..], symbol, client_order_id, json, batch_id, observation_id],
                            |row| row.get(0),
                        )?;
                    anyhow::ensure!(
                        matches,
                        "initial order response observation ID reused with different data"
                    );
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn load_initial_order_responses(
        &self,
        account: [u8; 32],
        symbol: &str,
        client_order_id: &str,
    ) -> Result<Vec<crate::exchange::BinanceOrderResponse>> {
        Ok(self
            .load_initial_order_response_timeline(account, symbol, client_order_id)?
            .into_iter()
            .map(|(_, response)| response)
            .collect())
    }

    pub(crate) fn load_initial_order_response_timeline(
        &self,
        account: [u8; 32],
        symbol: &str,
        client_order_id: &str,
    ) -> Result<Vec<(String, crate::exchange::BinanceOrderResponse)>> {
        let conn = self.lock_connection("load_initial_order_responses");
        let mut statement = conn.prepare_cached("SELECT batch_id, response_json FROM initial_order_responses WHERE account_key=?1 AND expected_symbol=?2 AND client_order_id=?3 ORDER BY rowid")?;
        let rows = statement.query_map(params![&account[..], symbol, client_order_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut responses = Vec::new();
        for row in rows {
            let (batch_id, json) = row?;
            responses.push((batch_id, serde_json::from_str(&json)?));
        }
        Ok(responses)
    }

    pub(crate) fn delete_order_evidence_groups(
        &self,
        account: [u8; 32],
        symbol: &str,
        client_ids: &[String],
    ) -> Result<()> {
        if client_ids.is_empty() {
            return Ok(());
        }
        let mut conn = self.lock_connection("delete_order_evidence_groups");
        let tx = super::immediate_tx(&mut conn)?;
        {
            let mut initial = tx.prepare_cached("DELETE FROM initial_order_responses WHERE account_key=?1 AND expected_symbol=?2 AND client_order_id=?3")?;
            let mut observations = tx.prepare_cached("DELETE FROM order_identity_observations WHERE account_key=?1 AND expected_symbol=?2 AND client_order_id=?3")?;
            for id in client_ids {
                initial.execute(params![&account[..], symbol, id])?;
                observations.execute(params![&account[..], symbol, id])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn save_terminal_order_disagreement(
        &self,
        account: [u8; 32],
        symbol: &str,
        client_order_id: &str,
        first: &crate::exchange::BinanceOrderResponse,
        conflicting: &crate::exchange::BinanceOrderResponse,
    ) -> Result<()> {
        let conn = self.lock_connection("save_terminal_order_disagreement");
        conn.prepare_cached(
            "INSERT INTO terminal_order_disagreements (account_key, expected_symbol, client_order_id, first_json, conflicting_json) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(account_key, expected_symbol, client_order_id, first_json, conflicting_json) DO NOTHING",
        )?
        .execute(params![
            &account[..],
            symbol,
            client_order_id,
            serde_json::to_string(first)?,
            serde_json::to_string(conflicting)?
        ])?;
        Ok(())
    }

    pub(crate) fn load_terminal_order_disagreements(
        &self,
        account: [u8; 32],
        symbol: &str,
    ) -> Result<
        Vec<(
            String,
            crate::exchange::BinanceOrderResponse,
            crate::exchange::BinanceOrderResponse,
        )>,
    > {
        let conn = self.lock_connection("load_terminal_order_disagreements");
        let mut stmt = conn.prepare_cached("SELECT client_order_id, first_json, conflicting_json FROM terminal_order_disagreements WHERE account_key=?1 AND expected_symbol=?2")?;
        let rows = stmt.query_map(params![&account[..], symbol], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut records = Vec::new();
        for row in rows {
            let (client_order_id, first, conflicting) = row?;
            records.push((
                client_order_id,
                serde_json::from_str(&first)?,
                serde_json::from_str(&conflicting)?,
            ));
        }
        Ok(records)
    }

    pub(crate) fn save_identity_observations(
        &self,
        account: [u8; 32],
        records: &[(GridOrder, crate::types::ObservedOrderIdentity)],
    ) -> Result<()> {
        let mut conn = self.lock_connection("save_identity_observations");
        let tx = super::immediate_tx(&mut conn)?;
        Self::save_identity_observation_rows(&tx, account, records)?;
        tx.commit()?;
        Ok(())
    }

    fn save_identity_observation_rows(
        tx: &rusqlite::Transaction<'_>,
        account: [u8; 32],
        records: &[(GridOrder, crate::types::ObservedOrderIdentity)],
    ) -> Result<()> {
        {
            let mut statement = tx.prepare_cached(
                "INSERT INTO order_identity_observations (account_key, client_order_id, expected_symbol, observed_client_order_id, observed_symbol, observed_side, observed_order_id, expected_json, observed_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT(account_key,client_order_id,expected_symbol,observed_client_order_id,observed_symbol,observed_side,observed_order_id) DO UPDATE SET expected_json=excluded.expected_json, observed_json=excluded.observed_json",
            )?;
            for (expected, observed) in records {
                statement.execute(params![
                    &account[..],
                    expected.client_order_id,
                    expected.symbol,
                    observed.client_order_id,
                    observed.symbol,
                    observed.side,
                    observed.order_id,
                    serde_json::to_string(expected)?,
                    serde_json::to_string(observed)?
                ])?;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn save_identity_observation(
        &self,
        account: [u8; 32],
        expected: &GridOrder,
        observed: &crate::types::ObservedOrderIdentity,
    ) -> Result<()> {
        self.save_identity_observations(account, &[(expected.clone(), observed.clone())])
    }

    pub(crate) fn load_identity_observations(
        &self,
        account: [u8; 32],
        symbol: &str,
    ) -> Result<Vec<(GridOrder, crate::types::ObservedOrderIdentity)>> {
        let conn = self.lock_connection("load_identity_observations");
        let mut stmt = conn.prepare_cached("SELECT expected_json, observed_json FROM order_identity_observations WHERE account_key=?1 AND expected_symbol=?2")?;
        let rows = stmt.query_map(params![&account[..], symbol], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut observations = Vec::new();
        for row in rows {
            let (expected, observed) = row?;
            observations.push((
                serde_json::from_str(&expected)?,
                serde_json::from_str(&observed)?,
            ));
        }
        Ok(observations)
    }

    pub(crate) fn delete_managed_order(&self, client_order_id: &str) -> Result<()> {
        let conn = self.lock_connection("delete_managed_order");
        conn.prepare_cached("DELETE FROM managed_orders WHERE client_order_id = ?1;")?
            .execute(params![client_order_id])?;
        Ok(())
    }

    pub(crate) fn clear_managed_orders(&self, symbol: &str) -> Result<()> {
        let conn = self.lock_connection("clear_managed_orders");
        conn.prepare_cached("DELETE FROM managed_orders WHERE symbol = ?1;")?
            .execute(params![symbol])?;
        Ok(())
    }
}

type SerializedObservation<'a> = (
    &'a str,
    &'a str,
    &'a str,
    &'a crate::exchange::BinanceOrderResponse,
    String,
);

/// An earlier identical snapshot already precedes every later individual
/// query, so it supplies the same comparison evidence.
fn is_duplicate_response_snapshot(
    tx: &Connection,
    account: [u8; 32],
    symbol: &str,
    observations: &[SerializedObservation<'_>],
) -> Result<bool> {
    if observations.is_empty()
        || !observations[0].2.starts_with("snapshot:")
        || !observations.iter().all(|row| row.2 == observations[0].2)
    {
        return Ok(false);
    }
    let last_batch: Option<String> = tx
        .prepare_cached(
            "SELECT batch_id FROM initial_order_responses WHERE account_key=?1 AND expected_symbol=?2 AND substr(batch_id,1,9)='snapshot:' ORDER BY rowid DESC LIMIT 1",
        )?
        .query_row(params![&account[..], symbol], |row| row.get(0))
        .optional()?;
    let Some(last_batch) = last_batch
        .filter(|batch| batch.starts_with("snapshot:") && batch.as_str() != observations[0].2)
    else {
        return Ok(false);
    };
    let mut statement = tx.prepare_cached(
        "SELECT client_order_id, response_json FROM initial_order_responses WHERE account_key=?1 AND expected_symbol=?2 AND batch_id=?3 ORDER BY rowid",
    )?;
    let previous: Vec<(String, String)> = statement
        .query_map(params![&account[..], symbol, last_batch], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<rusqlite::Result<_>>()?;
    if previous.len() != observations.len() {
        return Ok(false);
    }
    for ((client, json), (new_client, _, _, _, new_json)) in previous.iter().zip(observations) {
        if client.as_str() != *new_client || json.as_str() != new_json.as_str() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Compare the latest evidence for this exact order identity. Queries
/// for other identities cannot affect its transition history.
fn is_duplicate_single_response_query(
    tx: &Connection,
    account: [u8; 32],
    symbol: &str,
    observations: &[SerializedObservation<'_>],
) -> Result<bool> {
    if observations.len() != 1 {
        return Ok(false);
    }
    let (client, observation_id, batch_id, response, new_json) = &observations[0];
    if uuid::Uuid::parse_str(observation_id).is_err() || uuid::Uuid::parse_str(batch_id).is_err() {
        return Ok(false);
    }
    let previous: Option<(String, String)> = tx
        .prepare_cached(
            "SELECT batch_id, response_json FROM initial_order_responses WHERE account_key=?1 AND expected_symbol=?2 AND client_order_id=?3 AND observed_client_order_id=?4 AND observed_symbol=?5 AND observed_side=?6 AND observed_order_id=?7 ORDER BY rowid DESC LIMIT 1",
        )?
        .query_row(
            params![
                &account[..],
                symbol,
                client,
                response.client_order_id,
                response.symbol,
                response.side,
                response.order_id
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((previous_batch, previous_json)) = previous else {
        return Ok(false);
    };
    if uuid::Uuid::parse_str(&previous_batch).is_err()
        || previous_batch == *batch_id
        || previous_json != *new_json
    {
        return Ok(false);
    }
    let safe: bool = tx
        .prepare_cached(
            "SELECT (SELECT count(*) FROM initial_order_responses WHERE account_key=?1 AND expected_symbol=?2 AND batch_id=?3)=1 AND NOT EXISTS(SELECT 1 FROM initial_order_responses WHERE observation_id=?4 OR (account_key=?1 AND expected_symbol=?2 AND batch_id=?5))",
        )?
        .query_row(
            params![&account[..], symbol, previous_batch, observation_id, batch_id],
            |row| row.get(0),
        )?;
    Ok(safe)
}
