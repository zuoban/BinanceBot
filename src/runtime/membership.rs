use super::{PreparePairOptions, TradingRuntime};
use crate::config::{AppConfig, GlobalConfig};
use crate::lifecycle::remove_engine;
use crate::server::AppState;
use crate::strategy::SymbolRules;
use crate::sync_ext::MutexUnpoisonedExt;
use std::sync::Arc;
use std::time::Duration;

struct ActiveRemoval(Arc<std::sync::Mutex<Option<String>>>);

impl Drop for ActiveRemoval {
    fn drop(&mut self) {
        *self.0.lock_unpoisoned() = None;
    }
}

impl TradingRuntime {
    pub(super) async fn add_pair(&mut self, mut config: AppConfig) -> anyhow::Result<()> {
        let _membership = self.handle.membership_lock.lock().await;
        let saved = self.handle.db.load_saved_config().await?;
        GlobalConfig::from_app(&saved).apply_to(&mut config);
        config.validate()?;
        let rules = self.validate_new_pair_rules(&config).await?;
        let added_symbol = config.exchange.symbol.clone();
        let added_mode = config.exchange.trading_mode();
        let saved = config.clone();
        let previous_unclean = self
            .handle
            .db
            .run_blocking(move |db| db.add_pair_config(&saved))
            .await?;
        self.global_position_limit
            .membership_ready
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let pair = match self
            .prepare_pair_with_state(
                config,
                PreparePairOptions {
                    multi: true,
                    prearmed: Some(previous_unclean),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(pair) => pair,
            Err(err) => {
                self.rollback_failed_add_pair(added_symbol, added_mode, previous_unclean)
                    .await;
                return Err(err);
            }
        };
        *pair.state.rules.write().await = rules;
        self.handle.register_engine(&pair);
        self.pairs.push(pair);
        *self.handle.states.write().await = self.states();
        self.global_position_limit
            .membership_ready
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.global_position_limit.check().await;
        Ok(())
    }

    async fn validate_new_pair_rules(&self, config: &AppConfig) -> anyhow::Result<SymbolRules> {
        let symbol = &config.exchange.symbol;
        anyhow::ensure!(
            !self
                .pairs
                .iter()
                .any(|pair| pair.state.pair_scope() == Some(symbol.as_str())),
            "交易对已存在"
        );
        let info = self
            .exchange_client(&config.exchange)
            .get_exchange_info(Some(symbol))
            .await
            .map_err(|error| {
                if matches!(
                    error.downcast_ref::<crate::exchange::ExchangeError>(),
                    Some(crate::exchange::ExchangeError::ApiError { code: -1121, .. })
                ) {
                    anyhow::anyhow!("币安交易对 {} 不存在", symbol)
                } else {
                    anyhow::Error::msg(crate::server::routes::sanitized_probe_error(
                        &format!("暂时无法验证交易对 {symbol}"),
                        &error,
                    ))
                }
            })?;
        SymbolRules::parse_and_validate_interval(&info, symbol, config.grid.grid_interval)
    }

    async fn rollback_failed_add_pair(
        &self,
        symbol: String,
        mode: crate::types::TradingMode,
        previous_unclean: bool,
    ) {
        let rollback = self
            .handle
            .db
            .run_blocking(move |db| {
                db.remove_pair_config(&symbol)?;
                if !previous_unclean {
                    db.clear_execution_guard(&symbol, mode)?;
                }
                Ok(())
            })
            .await;
        match rollback {
            Ok(()) => {
                self.global_position_limit
                    .membership_ready
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
            Err(rollback_err) => {
                tracing::error!(
                    %rollback_err,
                    "Failed to roll back persisted pair config after add_pair failure"
                );
                self.global_position_limit.check().await;
            }
        }
    }

    pub(super) async fn remove_pair(&mut self, symbol: &str) -> anyhow::Result<()> {
        let membership_lock = self.handle.membership_lock.clone();
        let _membership = membership_lock.lock().await;
        let (index, _removal) = self.admit_pair_removal(symbol).await?;
        let pair = &mut self.pairs[index];
        pair.signal_shutdown();
        let mut result =
            remove_engine(&pair.state, &mut pair.engine, Duration::from_secs(30)).await;
        let state = pair.state.clone();
        pair.stop_background().await;
        // Retired states reject new verification admission. Finish old commits
        // before either removing the member or restoring a replacement.
        // Use the old state's lock, and release it before replacement startup.
        state.drain_trade_commits().await;
        if result.is_ok() {
            result = self.verify_pair_flat_before_removal(&state, symbol).await;
        }
        if result.is_ok() {
            let symbol = symbol.to_string();
            result = self
                .handle
                .db
                .run_blocking(move |db| db.remove_pair_config(&symbol))
                .await;
        }
        if let Err(error) = result {
            return self.recover_pair_after_failed_removal(index, error).await;
        }
        self.pairs.remove(index);
        *self.handle.states.write().await = self.states();
        Ok(())
    }

    async fn admit_pair_removal(&mut self, symbol: &str) -> anyhow::Result<(usize, ActiveRemoval)> {
        anyhow::ensure!(self.pairs.len() > 1, "至少保留一个交易对");
        let index = self
            .pairs
            .iter()
            .position(|pair| pair.state.pair_scope() == Some(symbol))
            .ok_or_else(|| anyhow::anyhow!("交易对不存在"))?;
        anyhow::ensure!(
            !self.pairs[index].recovery_failed,
            "策略重建失败，请重启程序并核实交易所状态"
        );
        anyhow::ensure!(
            !self.pairs[index].engine_joined,
            "策略引擎已退出，请重启程序并核实交易所状态"
        );
        let pair = &mut self.pairs[index];
        let control_lock = pair.state.socket_control_lock.clone();
        let control = control_lock.write().await;
        anyhow::ensure!(
            *pair.state.status.read().await == crate::types::BotStatus::Paused,
            "请先暂停该策略并等待挂单撤销完成"
        );
        anyhow::ensure!(
            pair.state.active_orders.read().await.is_empty(),
            "仍有挂单，请先撤单并等待交易所确认"
        );
        // Claim the engine only once all potentially blocking admission checks
        // are complete. Shutdown can stop it while removal waits for those locks.
        let removal = {
            let mut active = self.handle.active_removal.lock_unpoisoned();
            anyhow::ensure!(
                !*self.handle.stop.borrow(),
                "程序正在退出，无法修改交易对列表"
            );
            *active = Some(symbol.to_string());
            ActiveRemoval(self.handle.active_removal.clone())
        };
        pair.state.retire();
        drop(control);
        Ok((index, removal))
    }

    async fn verify_pair_flat_before_removal(
        &self,
        state: &Arc<AppState>,
        symbol: &str,
    ) -> anyhow::Result<()> {
        let config = state.config.read().await.clone();
        if config.exchange.is_paper() {
            self.verify_paper_pair_flat(state, symbol).await
        } else {
            self.verify_live_pair_flat(state, &config.exchange, symbol)
                .await
        }
    }

    async fn verify_paper_pair_flat(
        &self,
        state: &Arc<AppState>,
        symbol: &str,
    ) -> anyhow::Result<()> {
        let paper_symbol = symbol.to_string();
        let saved = state
            .db
            .run_blocking(move |db| db.load_paper_state(&paper_symbol))
            .await;
        match saved {
            Err(error) => Err(error.context("无法确认模拟盘持仓，无法移除交易对")),
            Ok(Some((saved_pos, saved_acc))) if !saved_pos.size.is_zero() => {
                *state.position.write().await = saved_pos;
                *state.account.write().await = saved_acc;
                self.global_position_limit.check().await;
                Err(anyhow::anyhow!("仍有持仓，请先平仓再移除交易对"))
            }
            Ok(_) if state.position.read().await.size.is_zero() => Ok(()),
            Ok(_) => Err(anyhow::anyhow!("仍有持仓，请先平仓再移除交易对")),
        }
    }

    async fn verify_live_pair_flat(
        &self,
        state: &Arc<AppState>,
        exchange: &crate::config::ExchangeConfig,
        symbol: &str,
    ) -> anyhow::Result<()> {
        let client = self.exchange_client(exchange);
        let position = match client.sync_server_time().await {
            Ok(_) => client.get_position(symbol).await,
            Err(error) => Err(error.context("同步交易所时间失败")),
        };
        match position {
            Ok(Some(position)) => {
                let has_position = !position.position_amt.is_zero();
                *state.position.write().await =
                    position.into_position_info(Some(chrono::Utc::now()));
                if has_position {
                    self.global_position_limit.check().await;
                    Err(anyhow::anyhow!("仍有持仓，请先平仓再移除交易对"))
                } else {
                    Ok(())
                }
            }
            Ok(None) => {
                state
                    .global_position_ready
                    .store(false, std::sync::atomic::Ordering::SeqCst);
                self.global_position_limit.check().await;
                Err(anyhow::anyhow!("无法确认交易所持仓，请稍后重试"))
            }
            Err(error) => {
                state
                    .global_position_ready
                    .store(false, std::sync::atomic::Ordering::SeqCst);
                self.global_position_limit.check().await;
                Err(error.context("查询交易所持仓失败，无法移除交易对"))
            }
        }
    }

    async fn recover_pair_after_failed_removal(
        &mut self,
        index: usize,
        error: anyhow::Error,
    ) -> anyhow::Result<()> {
        // The engine may have committed a configuration update while stopping.
        let config = self.pairs[index].state.config.read().await.clone();
        // Keep the last known exposure visible while a replacement reconciles.
        let previous_state = Some((
            self.pairs[index].state.position.read().await.clone(),
            self.pairs[index].state.account.read().await.clone(),
        ));
        match self
            .prepare_pair_with_state(
                config,
                PreparePairOptions {
                    multi: true,
                    previous_state,
                    force_paused: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(replacement) => {
                self.handle.register_engine(&replacement);
                self.pairs[index] = replacement;
                *self.handle.states.write().await = self.states();
                self.global_position_limit.check().await;
                Err(error)
            }
            Err(restart_error) => {
                tracing::error!(
                    error = %format_args!("{restart_error:#}"),
                    "Failed to restart strategy after failed pair removal"
                );
                self.pairs[index].recovery_failed = true;
                // Keep the retired member in the risk set. Its position can
                // still exist, but its stopped engine can no longer refresh it.
                self.pairs[index]
                    .state
                    .global_position_initialized
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                self.pairs[index]
                    .state
                    .global_position_ready
                    .store(false, std::sync::atomic::Ordering::SeqCst);
                self.global_position_limit.check().await;
                Err(error.context("删除失败后无法重启策略，请查看服务器日志"))
            }
        }
    }
}
