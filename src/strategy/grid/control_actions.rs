use super::*;

impl GridTradingEngine {
    fn reject_action_during_shutdown(action: BotControlAction) {
        if let BotControlAction::UpdateConfig { reply, .. }
        | BotControlAction::UpdateGlobalConfig { reply, .. }
        | BotControlAction::Resume { reply, .. }
        | BotControlAction::Rebalance { reply, .. } = action
        {
            let _ = reply.send(Err("程序正在退出，操作未执行".into()));
        }
    }

    async fn handle_global_risk_cancel(&mut self) {
        // The engine serializes submissions and cancellations. Any order already
        // in flight when the global gate closed has finished before this runs.
        if self.cancel_all_orders().await {
            self.state
                .global_cancel_pending
                .store(false, std::sync::atomic::Ordering::SeqCst);
        } else {
            self.state
                .add_log("ERROR", "全局风控撤单未完成，将继续重试")
                .await;
        }
        self.state
            .global_cancel_queued
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) async fn handle_control_action(&mut self, action: BotControlAction) {
        if self.state.is_shutting_down() && !matches!(action, BotControlAction::Shutdown { .. }) {
            Self::reject_action_during_shutdown(action);
            return;
        }
        match action {
            BotControlAction::Shutdown { removing, reply } => {
                let result = Box::pin(self.shutdown_inner(removing)).await;
                let _ = reply.send(result.map_err(|error| error.to_string()));
            }
            BotControlAction::Pause
            | BotControlAction::PauseAlreadyPersisted
            | BotControlAction::CancelAll
            | BotControlAction::CancelAllAlreadyPersisted => {
                Box::pin(self.handle_pause_or_cancel_action(action)).await;
            }
            BotControlAction::GlobalRiskCancel => self.handle_global_risk_cancel().await,
            BotControlAction::Resume {
                expected_mode,
                expected_epoch,
                reply,
            } => {
                let fut = self.handle_resume_action(expected_mode, expected_epoch, reply);
                Box::pin(fut).await;
            }
            BotControlAction::Rebalance {
                expected_mode,
                reply,
            } => {
                Box::pin(self.handle_rebalance_action(expected_mode, reply)).await;
            }
            BotControlAction::UpdateConfig { config, reply } => {
                self.apply_and_reply_config(*config, "Configuration update rejected", reply)
                    .await;
            }
            BotControlAction::UpdateGlobalConfig { config, reply } => {
                let mut updated = self.state.config.read().await.clone();
                config.apply_to(&mut updated);
                self.apply_and_reply_config(updated, "Global configuration update rejected", reply)
                    .await;
            }
        }
    }

    async fn handle_rebalance_action(
        &mut self,
        expected_mode: TradingMode,
        reply: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    ) {
        let current_mode = self.state.config.read().await.exchange.trading_mode();
        let result = if expected_mode == TradingMode::Unknown || expected_mode != current_mode {
            Err("策略运行模式已改变，无法重建网格".into())
        } else if *self.state.status.read().await != BotStatus::Running {
            Err("策略未运行，无法重建网格".into())
        } else {
            info!("Grid rebalance requested");
            self.state
                .add_log("INFO", "Manual grid rebalance triggered")
                .await;
            Box::pin(self.rebalance_grid()).await
        };
        let _ = reply.send(result);
    }

    async fn handle_pause_or_cancel_action(&mut self, action: BotControlAction) {
        let is_cancel_all = matches!(
            action,
            BotControlAction::CancelAll | BotControlAction::CancelAllAlreadyPersisted
        );
        let already_persisted = matches!(
            action,
            BotControlAction::PauseAlreadyPersisted | BotControlAction::CancelAllAlreadyPersisted
        );
        let (info_msg, ok_log, err_log) = if is_cancel_all {
            (
                "Cancel all orders requested",
                "Managed grid orders canceled; bot paused",
                "Cancellation incomplete; bot paused",
            )
        } else {
            (
                "Bot paused by user command",
                "Trading bot paused and managed orders canceled",
                "Trading bot paused, but some managed orders may still be open on Binance",
            )
        };
        info!("{}", info_msg);
        if !already_persisted {
            self.pause_trading().await;
        }
        if self.cancel_all_orders_inner(already_persisted).await {
            self.state.add_log("WARN", ok_log).await;
        } else {
            self.state.add_log("ERROR", err_log).await;
        }
    }

    async fn ensure_resume_rules_ready(
        &mut self,
        config: &crate::config::AppConfig,
    ) -> std::result::Result<(), String> {
        if !self.exchange_rules_loaded {
            if let Err(error) = self.load_exchange_rules(&config.exchange.symbol).await {
                if !config.exchange.is_paper() {
                    let message = format!(
                        "Resume failed: could not load Binance exchange rules for {}: {}",
                        config.exchange.symbol, error
                    );
                    self.state.add_log("ERROR", &message).await;
                    return Err(message);
                }
                self.state
                    .add_log(
                        "WARN",
                        format!(
                            "Could not load Binance exchange rules for {}; paper trading will use default rules: {}",
                            config.exchange.symbol, error
                        ),
                    )
                    .await;
            }
        }
        if let Err(error) = self
            .state
            .rules
            .read()
            .await
            .validate_grid_interval(config.grid.grid_interval)
        {
            let message = format!("Resume failed: {}", error);
            self.state.add_log("ERROR", &message).await;
            return Err(message);
        }
        Ok(())
    }

    async fn handle_resume_action(
        &mut self,
        expected_mode: TradingMode,
        expected_epoch: u64,
        reply: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    ) {
        let config = self.state.config.read().await.clone();
        let current_mode = config.exchange.trading_mode();
        if expected_mode == TradingMode::Unknown || current_mode != expected_mode {
            let _ = reply.send(Err("策略运行模式已改变，无法恢复策略".into()));
            return;
        }
        if let Err(message) = Box::pin(self.ensure_resume_rules_ready(&config)).await {
            let _ = reply.send(Err(message));
            return;
        }
        if let Err(error) = self
            .state
            .resume_trading_in_mode(expected_mode, expected_epoch)
            .await
        {
            error!("Could not resume bot: {}", error);
            let message = format!("Resume failed: {}", error);
            self.state.add_log("ERROR", &message).await;
            let _ = reply.send(Err(message));
            return;
        }
        info!("Bot resumed by user command");
        self.state
            .add_log("INFO", "Trading bot resumed by user")
            .await;
        let _ = reply.send(Ok(()));
        Box::pin(self.sync_cycle(false)).await;
    }

    #[cfg(test)]
    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        self.shutdown_inner(false).await
    }

    async fn verify_removed_invalid_symbol(&mut self, symbol: &str) -> Result<()> {
        anyhow::ensure!(
            self.state.active_orders.read().await.is_empty(),
            "无效交易对仍有内存挂单，请核实恢复数据"
        );
        let symbol = symbol.to_owned();
        let clear = self
            .state
            .db
            .run_blocking(move |db| {
                if !db.load_managed_orders(&symbol)?.is_empty() {
                    return Ok(false);
                }
                for mode in [TradingMode::Unknown, TradingMode::Paper, TradingMode::Live] {
                    if !db.load_pair_intents(&symbol, mode)?.is_empty()
                        || db.load_remainder_plan(&symbol, mode)?.is_some()
                    {
                        return Ok(false);
                    }
                }
                Ok(true)
            })
            .await?;
        anyhow::ensure!(clear, "无效交易对仍有待恢复订单或意图，请核实恢复数据");
        if let Some(task) = self.pnl_reconcile_task.take() {
            task.abort();
            let _ = task.await;
        }
        Ok(())
    }

    pub(super) async fn shutdown_inner(&mut self, removing: bool) -> Result<()> {
        self.retire_market_requests();
        self.state.request_shutdown();
        let paused = self.state.pause_for_shutdown().await;
        // Shutdown can interrupt initialization before durable orders have been
        // installed in memory. Restore them before canceling so racing fills
        // are recorded rather than discarded with the recovery metadata.
        let (is_paper, symbol) = {
            let config = self.state.config.read().await;
            (config.exchange.is_paper(), config.exchange.symbol.clone())
        };
        if removing && !is_paper {
            if let Err(error) = self.client.get_open_orders(&symbol).await {
                if matches!(
                    error.downcast_ref::<ExchangeError>(),
                    Some(ExchangeError::ApiError { code: -1121, .. })
                ) {
                    paused?;
                    return self.verify_removed_invalid_symbol(&symbol).await;
                }
                return Err(error.context("无法确认删除交易对的交易所挂单"));
            }
        }
        if !is_paper {
            let restored = self.load_managed_orders(&symbol).await?;
            self.insert_restored_managed_orders(restored).await;
            // An interrupted initialization may have committed a fill before
            // its runtime statistics were updated. Reload before recording
            // additional cancellation-time executions.
            self.state.refresh_current_scope().await;
        }
        let canceled = self.cancel_all_orders().await;
        if let Some(task) = self.pnl_reconcile_task.take() {
            task.abort();
            let _ = task.await;
        }
        paused?;
        anyhow::ensure!(
            canceled,
            "停机撤单未完成，恢复元数据已保留；请核实交易所挂单"
        );
        Ok(())
    }
}
