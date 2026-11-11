use super::*;

struct PreparedConfigTransition {
    new_client: Arc<BinanceFuturesClient>,
    new_rules: SymbolRules,
    new_price: Option<Decimal>,
    paper_state: Option<(PositionInfo, AccountInfo)>,
    direction_changed: bool,
    only_window_changed: bool,
    market_changed: bool,
    mode_changed: bool,
    credentials_changed: bool,
    client_changed: bool,
    fetch_rules: bool,
}

struct ConfigScopeReset<'a> {
    new_config: &'a crate::config::AppConfig,
    new_client: Arc<BinanceFuturesClient>,
    new_rules: SymbolRules,
    new_price: Option<Decimal>,
    paper_state: Option<(PositionInfo, AccountInfo)>,
    market_changed: bool,
    mode_changed: bool,
    credentials_changed: bool,
    client_changed: bool,
    fetch_rules: bool,
}

impl GridTradingEngine {
    pub(super) async fn load_exchange_rules(&mut self, symbol: &str) -> Result<()> {
        let info = self.client.get_exchange_info(Some(symbol)).await?;
        let rules = SymbolRules::from_trading_exchange_info(&info, symbol)
            .map_err(|error| error.into_chinese_error(symbol))?;
        info!(
            "Loaded Binance exchange rules for {}: tick_size={}, step_size={}, min_notional={}",
            symbol, rules.tick_size, rules.step_size, rules.min_notional
        );
        self.state
            .add_log(
                "INFO",
                format!(
                    "Loaded Binance exchange rules for {}: tick_size={}, step_size={}",
                    symbol, rules.tick_size, rules.step_size
                ),
            )
            .await;
        *self.state.rules.write().await = rules;
        self.exchange_rules_loaded = true;
        Ok(())
    }

    async fn ensure_flat_for_direction_change(&mut self, direction: GridDirection) -> Result<()> {
        if !self.state.config.read().await.exchange.is_paper() {
            anyhow::ensure!(
                self.sync_account_and_position().await,
                "无法确认当前持仓，交易方向未切换"
            );
        }
        anyhow::ensure!(
            self.state
                .position
                .read()
                .await
                .size
                .checked_mul(direction.sign())
                .is_some_and(|signed| signed >= Decimal::ZERO),
            "切换做多 / 做空方向前，请先平掉当前持仓"
        );
        Ok(())
    }

    pub(super) async fn apply_and_reply_config(
        &mut self,
        config: crate::config::AppConfig,
        log_prefix: &str,
        reply: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    ) {
        let result = self.apply_config(config).await;
        if let Err(error) = &result {
            self.state
                .add_log("ERROR", format!("{}: {}", log_prefix, error))
                .await;
        }
        let _ = reply.send(result.map_err(|error| error.to_string()));
    }

    async fn load_target_paper_state(
        &self,
        new_config: &crate::config::AppConfig,
        direction_changed: bool,
    ) -> Result<Option<(PositionInfo, AccountInfo)>> {
        let symbol = new_config.exchange.symbol.clone();
        let state = self
            .state
            .db
            .run_blocking(move |db| db.load_paper_state(&symbol))
            .await?;
        anyhow::ensure!(
            !direction_changed
                || state.as_ref().is_none_or(|(position, _)| {
                    position
                        .size
                        .checked_mul(new_config.grid.direction.sign())
                        .is_some_and(|signed| signed >= Decimal::ZERO)
                }),
            "切换做多 / 做空方向前，请先平掉目标账户的持仓"
        );
        Ok(state)
    }

    async fn load_transition_rules_and_price(
        &self,
        new_client: &BinanceFuturesClient,
        new_config: &crate::config::AppConfig,
        market_changed: bool,
        mode_changed: bool,
    ) -> Result<(SymbolRules, Option<Decimal>, bool)> {
        let fetch_rules = market_changed
            || mode_changed
            || (!new_config.exchange.is_paper() && !self.exchange_rules_loaded);
        let new_rules = if fetch_rules {
            let info = new_client
                .get_exchange_info(Some(&new_config.exchange.symbol))
                .await?;
            SymbolRules::parse_and_validate_interval(
                &info,
                &new_config.exchange.symbol,
                new_config.grid.grid_interval,
            )?
        } else {
            let rules = self.state.rules.read().await.clone();
            rules.validate_grid_interval(new_config.grid.grid_interval)?;
            rules
        };
        let new_price = if market_changed || mode_changed {
            let price = new_client
                .get_ticker_price(&new_config.exchange.symbol)
                .await?;
            if price <= Decimal::ZERO {
                return Err(anyhow!(
                    "Invalid market price for {}",
                    new_config.exchange.symbol
                ));
            }
            Some(price)
        } else {
            None
        };
        Ok((new_rules, new_price, fetch_rules))
    }

    async fn prepare_config_transition(
        &mut self,
        new_config: &crate::config::AppConfig,
    ) -> Result<Option<PreparedConfigTransition>> {
        new_config.validate()?;
        self.retire_market_requests();
        if let Some(symbol) = self.state.pair_scope() {
            anyhow::ensure!(
                symbol == new_config.exchange.symbol,
                "交易对不能直接重命名，请在页面中添加新交易对"
            );
        }
        let old_config = self.state.config.read().await.clone();
        let direction_changed = old_config.grid.direction != new_config.grid.direction;
        if direction_changed {
            self.ensure_flat_for_direction_change(new_config.grid.direction)
                .await?;
        }
        let strategy_changed =
            old_config.exchange != new_config.exchange || old_config.grid != new_config.grid;
        if !strategy_changed {
            return Ok(None);
        }
        let mut resized_grid = old_config.grid;
        resized_grid.buy_window = new_config.grid.buy_window;
        resized_grid.sell_window = new_config.grid.sell_window;
        let only_window_changed =
            old_config.exchange == new_config.exchange && resized_grid == new_config.grid;
        let market_changed = old_config.exchange.symbol != new_config.exchange.symbol;
        let mode_changed = old_config.exchange.trading_mode() != new_config.exchange.trading_mode();
        let credentials_changed = old_config.exchange.api_key != new_config.exchange.api_key
            || old_config.exchange.api_secret != new_config.exchange.api_secret;
        let client_changed = credentials_changed
            || old_config.exchange.recv_window != new_config.exchange.recv_window;

        let paper_state = if new_config.exchange.is_paper() && (market_changed || mode_changed) {
            self.load_target_paper_state(new_config, direction_changed)
                .await?
        } else {
            None
        };
        let new_client = if client_changed {
            Arc::new(self.client.reconfig(&new_config.exchange))
        } else {
            Arc::clone(&self.client)
        };
        if !new_config.exchange.is_paper() {
            Self::validate_live_config_target(&new_client, new_config, direction_changed).await?;
        }
        let (new_rules, new_price, fetch_rules) = self
            .load_transition_rules_and_price(&new_client, new_config, market_changed, mode_changed)
            .await?;
        Ok(Some(PreparedConfigTransition {
            new_client,
            new_rules,
            new_price,
            paper_state,
            direction_changed,
            only_window_changed,
            market_changed,
            mode_changed,
            credentials_changed,
            client_changed,
            fetch_rules,
        }))
    }

    async fn commit_config_transition(
        &mut self,
        new_config: &crate::config::AppConfig,
        mut prepared: PreparedConfigTransition,
    ) -> Result<(bool, bool, bool)> {
        // Resizing uses ordinary window pruning so existing exits keep their
        // prices and queue priority. Other changes cancel in the old scope first.
        let cancel_before_switch = !prepared.only_window_changed
            || prepared.market_changed
            || prepared.mode_changed
            || prepared.credentials_changed;
        if cancel_before_switch && !self.cancel_all_orders().await {
            return Err(anyhow!(
                "Could not cancel old managed orders; configuration unchanged"
            ));
        }
        // Cancellation may reveal a racing fill. Recheck the old account before
        // committing the new direction so an exit cannot become an opening order.
        if prepared.direction_changed {
            if let Err(error) = self
                .ensure_flat_for_direction_change(new_config.grid.direction)
                .await
            {
                self.pause_trading().await;
                return Err(error);
            }
        }
        if new_config.exchange.is_paper() && (prepared.market_changed || prepared.mode_changed) {
            prepared.paper_state = self
                .load_target_paper_state(new_config, prepared.direction_changed)
                .await?;
        }
        // Snapshot readers must see the new config together with its matching runtime data.
        let transition_state = Arc::clone(&self.state);
        let scope_transition = transition_state.snapshot_scope_lock.write().await;
        if let Err(error) = self.state.save_runtime_config(new_config.clone()).await {
            self.pause_trading().await;
            return Err(error);
        }
        let (market_changed, mode_changed, credentials_changed) = (
            prepared.market_changed,
            prepared.mode_changed,
            prepared.credentials_changed,
        );
        self.reset_runtime_scope_after_config_change(ConfigScopeReset {
            new_config,
            new_client: prepared.new_client,
            new_rules: prepared.new_rules,
            new_price: prepared.new_price,
            paper_state: prepared.paper_state,
            market_changed,
            mode_changed,
            credentials_changed,
            client_changed: prepared.client_changed,
            fetch_rules: prepared.fetch_rules,
        })
        .await;
        drop(scope_transition);
        Ok((market_changed, mode_changed, credentials_changed))
    }

    async fn reconcile_committed_config_transition(
        &mut self,
        new_config: &crate::config::AppConfig,
        market_changed: bool,
        mode_changed: bool,
        credentials_changed: bool,
    ) -> Result<()> {
        if self.state.is_stats_dirty() {
            self.state.refresh_current_scope().await;
        }
        let scope_ready = if new_config.exchange.is_paper() {
            self.update_paper_valuation().await;
            !self
                .state
                .paper_restore_failed
                .load(std::sync::atomic::Ordering::SeqCst)
        } else {
            self.sync_live_orders().await && self.sync_account_and_position().await
        };
        let ready = scope_ready && !self.state.is_stats_dirty();
        // A dirty target scope is installed paused, but reconciliation can have
        // recovered its live orders. Apply the same cancellation as startup.
        let cancellation_complete = if (market_changed || mode_changed || credentials_changed)
            && !new_config.exchange.is_paper()
            && *self.state.status.read().await == BotStatus::Paused
        {
            self.cancel_all_orders().await
        } else {
            true
        };
        if !cancellation_complete {
            self.set_global_reconciliation_ready(false).await;
            return Err(anyhow!(
                "Configuration saved, but paused target order cancellation failed; bot paused"
            ));
        }
        self.set_global_reconciliation_ready(ready).await;
        if !ready {
            self.pause_trading().await;
            return Err(anyhow!(
                "Configuration saved, but exchange reconciliation failed; bot paused"
            ));
        }
        if *self.state.status.read().await == BotStatus::Running {
            self.maintain_grid_window().await;
        }
        self.state
            .add_log(
                "SUCCESS",
                format!(
                    "Strategy configuration updated for {}",
                    new_config.exchange.symbol
                ),
            )
            .await;
        Ok(())
    }

    pub(super) async fn apply_config(
        &mut self,
        new_config: crate::config::AppConfig,
    ) -> Result<()> {
        let Some(prepared) = self.prepare_config_transition(&new_config).await? else {
            let sync_secs = new_config.exchange.sync_interval_secs.max(1);
            self.state.save_runtime_config(new_config).await?;
            self.sync_interval_secs = sync_secs;
            self.state
                .add_log("SUCCESS", "Notification configuration updated")
                .await;
            return Ok(());
        };
        let (market_changed, mode_changed, credentials_changed) =
            self.commit_config_transition(&new_config, prepared).await?;
        self.reconcile_committed_config_transition(
            &new_config,
            market_changed,
            mode_changed,
            credentials_changed,
        )
        .await
    }

    async fn validate_live_config_target(
        new_client: &BinanceFuturesClient,
        new_config: &crate::config::AppConfig,
        direction_changed: bool,
    ) -> Result<()> {
        new_client.sync_server_time().await?;
        let new_position = new_client
            .get_position(&new_config.exchange.symbol)
            .await?
            .ok_or_else(|| anyhow!("New symbol has no position information"))?;
        anyhow::ensure!(
            !direction_changed
                || new_position
                    .position_amt
                    .checked_mul(new_config.grid.direction.sign())
                    .is_some_and(|signed| signed >= Decimal::ZERO),
            "切换做多 / 做空方向前，请先平掉目标账户的持仓"
        );
        let account = new_client.get_account().await?;
        if account
            .margin_asset_for_symbol(&new_config.exchange.symbol)
            .is_none()
        {
            return Err(anyhow!("New symbol has no matching margin asset"));
        }
        Ok(())
    }

    async fn reset_market_or_mode_scope(
        &mut self,
        new_config: &crate::config::AppConfig,
        new_price: Option<Decimal>,
        paper_state: Option<(PositionInfo, AccountInfo)>,
    ) {
        self.mark_retry.reset();
        self.ticker_retry.reset();
        let mut ticker = TickerInfo {
            symbol: new_config.exchange.symbol.clone(),
            ..Default::default()
        };
        if let Some(price) = new_price {
            ticker.last_price = price;
            ticker.update_time = Utc::now();
        }
        *self.state.ticker.write().await = ticker;
        *self.state.position.write().await = PositionInfo::default();
        if new_config.exchange.is_paper() {
            if let Some((position, account)) = paper_state {
                *self.state.position.write().await = position;
                *self.state.account.write().await = account;
            } else {
                *self.state.account.write().await =
                    AccountInfo::initial_paper(&new_config.exchange.symbol, Some(Utc::now()));
            }
        } else {
            *self.state.account.write().await = AccountInfo::default();
        }
        self.state
            .paper_restore_failed
            .store(false, std::sync::atomic::Ordering::SeqCst);
        if let Some(tx) = &self.market_stream_tx {
            let _ = tx.send(new_config.exchange.symbol.clone());
        }
        while let Ok(_) | Err(broadcast::error::TryRecvError::Lagged(_)) = self.ticker_rx.try_recv()
        {
        }
        self.state.refresh_current_scope().await;
    }

    async fn reset_runtime_scope_after_config_change(&mut self, reset: ConfigScopeReset<'_>) {
        let ConfigScopeReset {
            new_config,
            new_client,
            new_rules,
            new_price,
            paper_state,
            market_changed,
            mode_changed,
            credentials_changed,
            client_changed,
            fetch_rules,
        } = reset;
        if client_changed {
            self.client = new_client;
        }
        self.sync_interval_secs = new_config.exchange.sync_interval_secs.max(1);
        if !new_config.exchange.is_paper() {
            self.next_time_sync_at = Instant::now() + super::PERIODIC_TIME_SYNC_INTERVAL;
        }
        if market_changed || mode_changed || credentials_changed {
            if let Some(task) = self.pnl_reconcile_task.take() {
                task.abort();
            }
            self.pnl_order_ids.clear();
            self.pnl_clear_revision = 0;
            self.pnl_reconcile_offset = 0;
            self.pnl_retry_at = None;
            self.reconciliation_blocked = false;
            self.market_data_blocked = false;
            self.paper_window_cache = None;
            self.last_account_sync = None;
            self.last_orders_sync = None;
            self.evidence = OrderEvidenceBook::default();
        }
        if credentials_changed {
            self.state.position.write().await.update_time = None;
            self.state.account.write().await.update_time = chrono::DateTime::<Utc>::default();
        }
        if fetch_rules {
            *self.state.rules.write().await = new_rules;
            self.exchange_rules_loaded = true;
        }
        if market_changed || mode_changed {
            self.reset_market_or_mode_scope(new_config, new_price, paper_state)
                .await;
        } else if credentials_changed && !new_config.exchange.is_paper() {
            *self.state.position.write().await = PositionInfo {
                symbol: new_config.exchange.symbol.clone(),
                ..Default::default()
            };
            *self.state.account.write().await = AccountInfo::default();
        }
    }
}
