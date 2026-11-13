use super::snapshot_identities::invalid_snapshot_price;
use super::*;
use crate::exchange::client::NewMarketOrderRequest;
use crate::telegram::ReverseNotification;
use std::sync::atomic::Ordering;

struct ReverseLegSummary {
    executed_qty: Decimal,
    avg_price: Option<Decimal>,
}

struct ReverseNotificationSpec<'a> {
    symbol: &'a str,
    mode: TradingMode,
    from_direction: GridDirection,
    to_direction: GridDirection,
}

impl ReverseLegSummary {
    const fn empty() -> Self {
        Self {
            executed_qty: Decimal::ZERO,
            avg_price: None,
        }
    }
}

struct WeightedExecutionTracker {
    total_qty: Decimal,
    total_notional: Decimal,
}

impl WeightedExecutionTracker {
    const fn new() -> Self {
        Self {
            total_qty: Decimal::ZERO,
            total_notional: Decimal::ZERO,
        }
    }

    fn record(&mut self, qty: Decimal, price: Decimal) {
        self.total_qty = self.total_qty.saturating_add(qty);
        if let Some(notional) = qty.checked_mul(price) {
            self.total_notional = self.total_notional.saturating_add(notional);
        }
    }

    fn finish(&self) -> ReverseLegSummary {
        let avg_price = (self.total_qty > Decimal::ZERO && self.total_notional > Decimal::ZERO)
            .then(|| self.total_notional.checked_div(self.total_qty))
            .flatten();
        ReverseLegSummary {
            executed_qty: self.total_qty,
            avg_price,
        }
    }
}

impl GridTradingEngine {
    async fn resolve_reverse_reference_price(
        &self,
        config: &crate::config::AppConfig,
    ) -> std::result::Result<Decimal, String> {
        let now = Utc::now();
        let ticker = self.state.ticker.read().await.clone();
        if config.exchange.is_paper() {
            if !ticker.prices_are_fresh(now) {
                return Err("当前行情已过期或缺失，无法执行反手交易".into());
            }
            return Ok(ticker.last_price);
        }
        let position = self.state.position.read().await.clone();
        let valuation = position.fresh_market_valuation_price(&ticker, |ts| {
            is_timestamp_fresh(ts, now, MARKET_PRICE_MAX_AGE_SECS)
        });
        if valuation > Decimal::ZERO {
            return Ok(valuation);
        }
        let fetched = self
            .client
            .get_ticker_price(&config.exchange.symbol)
            .await
            .map_err(|error| format!("无法获取当前市场价格：{}", error))?;
        if fetched <= Decimal::ZERO {
            return Err("交易所返回无效市场价格，无法执行反手交易".into());
        }
        Ok(fetched)
    }

    async fn validate_reverse_plan(
        &self,
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        target_qty: Decimal,
        ref_price: Decimal,
    ) -> std::result::Result<(Vec<Decimal>, Vec<Decimal>), String> {
        let close_chunks = rules
            .plan_market_chunks(target_qty, true, ref_price)
            .map_err(|error| error.to_string())?;
        let open_chunks = rules
            .plan_market_chunks(target_qty, false, ref_price)
            .map_err(|error| error.to_string())?;
        let open_total = open_chunks
            .iter()
            .copied()
            .fold(Decimal::ZERO, Decimal::saturating_add);
        if let Some(limit) = config.grid.max_position_usdc {
            let projected = open_total.checked_mul(ref_price).unwrap_or(Decimal::MAX);
            if projected > limit {
                return Err(format!(
                    "预计反向持仓名义价值 {} 超过单交易对最大持仓限制 {}，无法执行等量反手",
                    projected.normalize(),
                    limit.normalize()
                ));
            }
        }
        if config.exchange.is_paper() {
            let pos = self.state.position.read().await.clone();
            let acc = self.state.account.read().await.clone();
            let (_, _, close_pnl) = crate::types::apply_fill_to_position(
                pos.size,
                pos.entry_price,
                config.grid.direction.closing_side(),
                ref_price,
                target_qty,
            );
            let wallet_after_close = acc
                .total_wallet_balance
                .checked_add(close_pnl)
                .unwrap_or(acc.total_wallet_balance);
            let leverage = Decimal::from(pos.leverage.max(1));
            let open_margin = open_total
                .checked_mul(ref_price)
                .and_then(|notional| notional.checked_div(leverage))
                .unwrap_or(Decimal::MAX);
            if wallet_after_close < open_margin {
                return Err(format!(
                    "模拟盘平仓后可用保证金 {} 不足反向开仓所需保证金 {}",
                    crate::types::format_decimal_dp(wallet_after_close, 2, false),
                    crate::types::format_decimal_dp(open_margin, 2, false),
                ));
            }
        }
        Ok((close_chunks, open_chunks))
    }

    async fn preflight_reverse(
        &mut self,
        expected_mode: TradingMode,
        expected_direction: GridDirection,
    ) -> std::result::Result<crate::config::AppConfig, String> {
        if self.state.is_shutting_down() {
            return Err("程序正在退出，操作未执行".into());
        }
        let config = self.state.config.read().await.clone();
        let current_mode = config.exchange.trading_mode();
        if expected_mode == TradingMode::Unknown || current_mode != expected_mode {
            return Err("策略运行模式已改变，无法执行反手交易".into());
        }
        if config.grid.direction != expected_direction {
            return Err("策略交易方向已改变，请刷新状态后再确认".into());
        }
        self.ensure_action_rules_ready(&config, "Reverse").await?;
        if config.exchange.is_paper() {
            if self.state.paper_restore_failed.load(Ordering::SeqCst) {
                return Err("模拟盘仓位和余额未能恢复，请核对数据并重启".into());
            }
        } else {
            self.invalidate_account_and_position_caches();
            if !self.sync_live_orders().await || !Box::pin(self.sync_account_and_position()).await {
                return Err("无法确认当前交易所挂单或持仓，反手交易未执行".into());
            }
        }
        let pre_size = self.state.position.read().await.size;
        if pre_size
            .checked_mul(expected_direction.sign())
            .is_none_or(|signed| signed < Decimal::ZERO)
        {
            self.pause_trading().await;
            return Err("当前持仓方向与策略方向不一致，策略已暂停，请先核实仓位".into());
        }
        if self.state.order_submission_lock.try_read().is_err() {
            return Err("全局配置正在更新，请稍后重试".into());
        }
        let pre_qty = expected_direction.position_quantity(pre_size);
        if pre_qty > Decimal::ZERO {
            if self.state.try_order_submission().is_none() {
                return Err("全局风控限制已触发或风险数据未就绪，无法执行反手开仓".into());
            }
            let ref_price = self.resolve_reverse_reference_price(&config).await?;
            let rules = self.state.rules.read().await.clone();
            self.validate_reverse_plan(&config, &rules, pre_qty, ref_price)
                .await?;
        }
        Ok(config)
    }

    async fn execute_paper_market_order(
        &mut self,
        symbol: &str,
        side: OrderSide,
        quantity: Decimal,
        fill_price: Decimal,
    ) -> std::result::Result<ReverseLegSummary, String> {
        let now = Utc::now();
        let mut order = GridOrder {
            client_order_id: new_grid_client_order_id(side),
            order_id: Some(rand::random::<i64>().saturating_abs().max(1)),
            symbol: symbol.to_owned(),
            side,
            price: fill_price,
            quantity,
            amount_usdc: fill_price.checked_mul(quantity).unwrap_or(Decimal::MAX),
            status: OrderStatus::Filled,
            created_at: now,
            updated_at: now,
            grid_level: 0,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: OrderPurpose::Reverse,
            merge_sources: Vec::new(),
        };
        if !self.commit_order_fill(&mut order).await {
            self.pause_trading().await;
            return Err("模拟盘市价成交写入失败，策略已暂停".into());
        }
        Ok(ReverseLegSummary {
            executed_qty: quantity,
            avg_price: Some(fill_price),
        })
    }

    async fn handle_invalid_market_order_response(
        &mut self,
        symbol: &str,
        order: &mut GridOrder,
        resp: &BinanceOrderResponse,
        err_msg: &str,
    ) -> String {
        self.block_reconciliation_and_invalidate_caches().await;
        if !order.matches_exchange_identity(resp) || invalid_snapshot_price(resp) {
            if !order.matches_exchange_identity(resp) {
                self.track_identity_conflict(order, resp);
            }
            self.state
                .active_orders
                .write()
                .await
                .insert(order.client_order_id.clone(), order.clone());
            let _ = self
                .persist_identity_observations(self.client.account_identity_key(), symbol)
                .await;
            self.pause_trading().await;
            return err_msg.into();
        }
        order.order_id = Some(resp.order_id);
        let _ = self.save_managed_order(order).await;
        self.state
            .active_orders
            .write()
            .await
            .insert(order.client_order_id.clone(), order.clone());
        self.pause_trading().await;
        err_msg.into()
    }

    async fn poll_live_market_order_terminal(
        &mut self,
        symbol: &str,
        order: &mut GridOrder,
    ) -> std::result::Result<BinanceOrderResponse, String> {
        for delay_ms in [100_u64, 200, 400, 600, 800] {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            if let Ok(Some(found)) = self
                .client
                .lookup_order_by_client_id(symbol, &order.client_order_id)
                .await
            {
                if !order.matches_exchange_identity(&found)
                    || invalid_snapshot_price(&found)
                    || unresolved_filled_quantity(&found)
                {
                    return Err(self
                        .handle_invalid_market_order_response(
                            symbol,
                            order,
                            &found,
                            "交易所市价单回执身份或价格异常，策略已暂停",
                        )
                        .await);
                }
                order.order_id = Some(found.order_id);
                if found.is_terminal() {
                    return Ok(found);
                }
                order.status = found.open_order_status();
                let _ = self.save_managed_order(order).await;
            }
        }
        self.last_orders_sync = None;
        let _ = self.save_managed_order(order).await;
        self.state
            .active_orders
            .write()
            .await
            .insert(order.client_order_id.clone(), order.clone());
        self.pause_trading().await;
        Err("市价订单执行状态尚未确认，策略已暂停等待自动对账，请勿重复提交".into())
    }

    async fn execute_live_market_chunk(
        &mut self,
        symbol: &str,
        side: OrderSide,
        chunk_qty: Decimal,
        reduce_only: bool,
        ref_price: Decimal,
        rules: &SymbolRules,
    ) -> std::result::Result<(Decimal, Decimal, bool), String> {
        let cid = new_grid_client_order_id(side);
        let placeholder_price = ref_price.max(rules.tick_size);
        let now = Utc::now();
        let mut order = GridOrder {
            client_order_id: cid.clone(),
            order_id: None,
            symbol: symbol.to_owned(),
            side,
            price: placeholder_price,
            quantity: chunk_qty,
            amount_usdc: placeholder_price
                .checked_mul(chunk_qty)
                .unwrap_or(Decimal::MAX),
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
            grid_level: 0,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: OrderPurpose::Reverse,
            merge_sources: Vec::new(),
        };
        if let Err(error) = self.save_managed_order(&order).await {
            self.pause_trading().await;
            return Err(format!("无法持久化市价单恢复记录：{}", error));
        }
        if self.state.is_shutting_down() {
            self.delete_unsubmitted_order_intent(&cid, "reverse-shutdown")
                .await;
            return Err("程序正在退出，市价单未提交".into());
        }
        let formatted_qty = if reduce_only {
            rules.format_quantity(chunk_qty)
        } else {
            rules.format_market_quantity(chunk_qty)
        };
        let should_guard_fill = |res: &std::result::Result<BinanceOrderResponse, ExchangeError>| {
            res.is_ok()
                || res
                    .as_ref()
                    .is_err_and(ExchangeError::execution_status_unknown)
        };
        let (result, mut fill_guard) = if reduce_only {
            let Ok(_sub_lock) = self.state.order_submission_lock.clone().try_read_owned() else {
                self.delete_unsubmitted_order_intent(&cid, "reverse-locked")
                    .await;
                return Err("全局配置正在更新，市价平仓未提交".into());
            };
            let res = self
                .client
                .place_market_order(NewMarketOrderRequest {
                    symbol,
                    side: side.as_str(),
                    quantity: &formatted_qty,
                    client_order_id: &cid,
                    reduce_only: true,
                })
                .await;
            let mut guard =
                should_guard_fill(&res).then(|| self.state.publish_fill_reconciliation());
            if let Some(pending) = &mut guard {
                pending.acquire().await;
            }
            (res, guard)
        } else {
            let Some(_sub_lock) = self.state.try_order_submission() else {
                self.delete_unsubmitted_order_intent(&cid, "reverse-open-locked")
                    .await;
                return Err("全局风控或配置变更阻止反向市价开仓".into());
            };
            let Some(exchange_guard) = self.state.try_exchange_submission() else {
                self.delete_unsubmitted_order_intent(&cid, "reverse-open-gated")
                    .await;
                return Err("全局风控限制已触发，反向市价开仓未提交".into());
            };
            let res = self
                .client
                .place_market_order(NewMarketOrderRequest {
                    symbol,
                    side: side.as_str(),
                    quantity: &formatted_qty,
                    client_order_id: &cid,
                    reduce_only: false,
                })
                .await;
            let mut guard =
                should_guard_fill(&res).then(|| self.state.publish_fill_reconciliation());
            exchange_guard.complete();
            if let Some(pending) = &mut guard {
                pending.acquire().await;
            }
            (res, guard)
        };

        let terminal = match result {
            Ok(resp) => {
                if !order.matches_exchange_identity(&resp)
                    || invalid_snapshot_price(&resp)
                    || unresolved_filled_quantity(&resp)
                {
                    return Err(self
                        .handle_invalid_market_order_response(
                            symbol,
                            &mut order,
                            &resp,
                            "交易所市价单回执异常，策略已暂停",
                        )
                        .await);
                }
                order.order_id = Some(resp.order_id);
                if resp.is_terminal() {
                    resp
                } else {
                    order.status = resp.open_order_status();
                    let _ = self.save_managed_order(&order).await;
                    self.poll_live_market_order_terminal(symbol, &mut order)
                        .await?
                }
            }
            Err(error) if error.execution_status_unknown() => {
                warn!("Market order {} status uncertain: {}", cid, error);
                self.poll_live_market_order_terminal(symbol, &mut order)
                    .await?
            }
            Err(error) => {
                if let Some(guard) = &mut fill_guard {
                    guard.resolve();
                }
                self.delete_unsubmitted_order_intent(&cid, "reverse-rejected")
                    .await;
                return Err(format!("市价单被拒绝：{}", error));
            }
        };

        if terminal.executed_qty <= Decimal::ZERO {
            let _ = self.remove_managed_and_active_order(&cid).await;
            if let Some(guard) = &mut fill_guard {
                guard.resolve();
            }
            return Err(format!("市价单未成交（状态：{}）", terminal.status));
        }

        order.apply_terminal_fill(&terminal, exchange_fill_time(&terminal));
        self.invalidate_account_and_position_caches();
        if !Box::pin(self.sync_account_and_position()).await {
            let _ = self.save_managed_order(&order).await;
            self.state.active_orders.write().await.insert(cid, order);
            self.pause_trading().await;
            return Err("市价单成交后无法同步账户及持仓，策略已暂停".into());
        }
        if !self.commit_order_fill(&mut order).await {
            self.pause_trading().await;
            return Err("市价单成交记录持久化失败，策略已暂停".into());
        }
        if let Some(guard) = &mut fill_guard {
            guard.resolve();
        }
        let full_chunk = terminal.executed_qty >= chunk_qty;
        Ok((order.quantity, order.price, full_chunk))
    }

    async fn execute_live_market_leg(
        &mut self,
        symbol: &str,
        side: OrderSide,
        chunks: &[Decimal],
        reduce_only: bool,
        ref_price: Decimal,
        rules: &SymbolRules,
    ) -> std::result::Result<ReverseLegSummary, (ReverseLegSummary, String)> {
        let mut tracker = WeightedExecutionTracker::new();
        for &chunk in chunks {
            match self
                .execute_live_market_chunk(symbol, side, chunk, reduce_only, ref_price, rules)
                .await
            {
                Ok((qty, price, true)) => tracker.record(qty, price),
                Ok((qty, price, false)) => {
                    tracker.record(qty, price);
                    return Err((
                        tracker.finish(),
                        format!(
                            "市价单仅部分成交（目标 {}，实际 {}）",
                            chunk.normalize(),
                            qty.normalize()
                        ),
                    ));
                }
                Err(reason) => return Err((tracker.finish(), reason)),
            }
        }
        Ok(tracker.finish())
    }

    async fn emit_reverse_failure(
        &self,
        spec: &ReverseNotificationSpec<'_>,
        effective_direction: GridDirection,
        closed: &ReverseLegSummary,
        opened: &ReverseLegSummary,
        reason: String,
    ) -> String {
        self.pause_trading().await;
        self.state
            .add_log("ERROR", format!("反手交易异常中止：{}", reason))
            .await;
        self.state
            .notify_reverse_result(ReverseNotification {
                symbol: spec.symbol.to_owned(),
                mode: spec.mode,
                from_direction: spec.from_direction,
                to_direction: spec.to_direction,
                effective_direction,
                closed_qty: closed.executed_qty,
                close_avg_price: closed.avg_price,
                opened_qty: opened.executed_qty,
                open_avg_price: opened.avg_price,
                status: BotStatus::Paused,
                failure_reason: Some(reason.clone()),
                timestamp: Utc::now(),
            })
            .await;
        reason
    }

    pub(super) async fn handle_reverse_action(
        &mut self,
        expected_mode: TradingMode,
        expected_direction: GridDirection,
        reply: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    ) {
        let result = self
            .execute_reverse_workflow(expected_mode, expected_direction)
            .await;
        let _ = reply.send(result);
    }

    async fn execute_reverse_workflow(
        &mut self,
        expected_mode: TradingMode,
        expected_direction: GridDirection,
    ) -> std::result::Result<(), String> {
        let pause_epoch = self.state.pause_epoch();
        let was_running = !self.state.has_pending_pause()
            && *self.state.status.read().await == BotStatus::Running;
        let initial_config = self
            .preflight_reverse(expected_mode, expected_direction)
            .await?;
        let symbol = initial_config.exchange.symbol.clone();
        let is_paper = initial_config.exchange.is_paper();
        let new_direction = expected_direction.opposite();

        info!(
            "Reverse trade requested for {}: {} -> {} (mode={})",
            symbol,
            expected_direction.label_zh(),
            new_direction.label_zh(),
            expected_mode.as_str()
        );
        self.state
            .add_log(
                "INFO",
                format!(
                    "开始执行反手交易：{} → {}",
                    expected_direction.label_zh(),
                    new_direction.label_zh()
                ),
            )
            .await;

        if let Err(error) = self.state.pause_trading_silently().await {
            self.pause_trading().await;
            return Err(format!("无法进入反手保护暂停状态：{}", error));
        }
        let expected_resume_epoch = pause_epoch.saturating_add(1);

        if !self.cancel_all_orders_inner(true).await {
            self.pause_trading().await;
            let msg = "反手交易中止：未能确认全部策略挂单已撤销，策略保持暂停";
            self.state.add_log("ERROR", msg).await;
            return Err(msg.into());
        }

        if !is_paper {
            self.invalidate_account_and_position_caches();
            if !Box::pin(self.sync_account_and_position()).await {
                self.pause_trading().await;
                let msg = "撤单后无法同步最新持仓，反手交易中止，策略已暂停";
                self.state.add_log("ERROR", msg).await;
                return Err(msg.into());
            }
        }

        let post_cancel_size = self.state.position.read().await.size;
        if post_cancel_size
            .checked_mul(expected_direction.sign())
            .is_none_or(|signed| signed < Decimal::ZERO)
        {
            self.pause_trading().await;
            let msg = "撤单后检测到持仓方向异常，反手交易中止，策略已暂停";
            self.state.add_log("ERROR", msg).await;
            return Err(msg.into());
        }

        let target_qty = expected_direction.position_quantity(post_cancel_size);
        let rules = self.state.rules.read().await.clone();
        let spec = ReverseNotificationSpec {
            symbol: &symbol,
            mode: expected_mode,
            from_direction: expected_direction,
            to_direction: new_direction,
        };
        let mut closed = ReverseLegSummary::empty();
        let opened;

        if target_qty > Decimal::ZERO {
            let ref_price = match self.resolve_reverse_reference_price(&initial_config).await {
                Ok(price) => price,
                Err(reason) => {
                    self.pause_trading().await;
                    return Err(reason);
                }
            };
            let (close_chunks, open_chunks) = match self
                .validate_reverse_plan(&initial_config, &rules, target_qty, ref_price)
                .await
            {
                Ok(plan) => plan,
                Err(reason) => {
                    self.pause_trading().await;
                    return Err(reason);
                }
            };

            if is_paper {
                match self
                    .execute_paper_market_order(
                        &symbol,
                        expected_direction.closing_side(),
                        target_qty,
                        ref_price,
                    )
                    .await
                {
                    Ok(summary) => closed = summary,
                    Err(reason) => {
                        return Err(self
                            .emit_reverse_failure(
                                &spec,
                                expected_direction,
                                &closed,
                                &ReverseLegSummary::empty(),
                                reason,
                            )
                            .await);
                    }
                }
            } else {
                match self
                    .execute_live_market_leg(
                        &symbol,
                        expected_direction.closing_side(),
                        &close_chunks,
                        true,
                        ref_price,
                        &rules,
                    )
                    .await
                {
                    Ok(summary) => closed = summary,
                    Err((partial, reason)) => {
                        return Err(self
                            .emit_reverse_failure(
                                &spec,
                                expected_direction,
                                &partial,
                                &ReverseLegSummary::empty(),
                                format!("市价平仓未完成：{}", reason),
                            )
                            .await);
                    }
                }
                self.invalidate_account_and_position_caches();
                if !Box::pin(self.sync_account_and_position()).await
                    || !self.state.position.read().await.size.is_zero()
                {
                    return Err(self
                        .emit_reverse_failure(
                            &spec,
                            expected_direction,
                            &closed,
                            &ReverseLegSummary::empty(),
                            "平仓后账户仍有残余仓位或无法确认空仓，已停止后续反向开仓".into(),
                        )
                        .await);
                }
            }

            if let Err(reason) = self.persist_flipped_direction(new_direction).await {
                return Err(self
                    .emit_reverse_failure(
                        &spec,
                        expected_direction,
                        &closed,
                        &ReverseLegSummary::empty(),
                        reason,
                    )
                    .await);
            }

            let open_total = open_chunks
                .iter()
                .copied()
                .fold(Decimal::ZERO, Decimal::saturating_add);
            if is_paper {
                let open_price = match self
                    .resolve_reverse_reference_price(&self.state.config.read().await.clone())
                    .await
                {
                    Ok(price) => price,
                    Err(reason) => {
                        return Err(self
                            .emit_reverse_failure(
                                &spec,
                                new_direction,
                                &closed,
                                &ReverseLegSummary::empty(),
                                format!("已平仓并切换方向，但反向开仓价格校验失败：{}", reason),
                            )
                            .await);
                    }
                };
                match self
                    .execute_paper_market_order(
                        &symbol,
                        new_direction.opening_side(),
                        open_total,
                        open_price,
                    )
                    .await
                {
                    Ok(summary) => opened = summary,
                    Err(reason) => {
                        return Err(self
                            .emit_reverse_failure(
                                &spec,
                                new_direction,
                                &closed,
                                &ReverseLegSummary::empty(),
                                format!("已平仓并切换方向，但反向开仓失败：{}", reason),
                            )
                            .await);
                    }
                }
            } else {
                let open_ref = closed.avg_price.unwrap_or(ref_price);
                match self
                    .execute_live_market_leg(
                        &symbol,
                        new_direction.opening_side(),
                        &open_chunks,
                        false,
                        open_ref,
                        &rules,
                    )
                    .await
                {
                    Ok(summary) => opened = summary,
                    Err((partial, reason)) => {
                        return Err(self
                            .emit_reverse_failure(
                                &spec,
                                new_direction,
                                &closed,
                                &partial,
                                format!(
                                    "已平仓并切换为{}，但反向市价开仓未完成：{}",
                                    new_direction.label_zh(),
                                    reason
                                ),
                            )
                            .await);
                    }
                }
                self.state.check_global_position_limit().await;
            }
        } else {
            opened = ReverseLegSummary::empty();
            if let Err(reason) = self.persist_flipped_direction(new_direction).await {
                return Err(self
                    .emit_reverse_failure(&spec, expected_direction, &closed, &opened, reason)
                    .await);
            }
        }

        if !is_paper {
            self.reconcile_unverified_trades().await;
        }

        if was_running {
            if let Err(error) = self
                .state
                .resume_trading_in_mode(expected_mode, expected_resume_epoch)
                .await
            {
                let reason = format!("反手仓位与方向已完成，但策略保持暂停：{}", error);
                self.state.add_log("WARN", &reason).await;
                self.state
                    .notify_reverse_result(ReverseNotification {
                        symbol,
                        mode: expected_mode,
                        from_direction: expected_direction,
                        to_direction: new_direction,
                        effective_direction: new_direction,
                        closed_qty: closed.executed_qty,
                        close_avg_price: closed.avg_price,
                        opened_qty: opened.executed_qty,
                        open_avg_price: opened.avg_price,
                        status: BotStatus::Paused,
                        failure_reason: Some(reason.clone()),
                        timestamp: Utc::now(),
                    })
                    .await;
                return Err(reason);
            }
            Box::pin(self.maintain_grid_window()).await;
        }

        let final_status = *self.state.status.read().await;
        let summary_log = if target_qty.is_zero() {
            format!(
                "反手交易完成：{} → {}（空仓切换方向）",
                expected_direction.label_zh(),
                new_direction.label_zh()
            )
        } else {
            format!(
                "反手交易完成：{} → {}（平仓 {}，反向开仓 {}）",
                expected_direction.label_zh(),
                new_direction.label_zh(),
                closed.executed_qty.normalize(),
                opened.executed_qty.normalize()
            )
        };
        self.state.add_log("SUCCESS", &summary_log).await;
        self.state
            .notify_reverse_result(ReverseNotification {
                symbol,
                mode: expected_mode,
                from_direction: expected_direction,
                to_direction: new_direction,
                effective_direction: new_direction,
                closed_qty: closed.executed_qty,
                close_avg_price: closed.avg_price,
                opened_qty: opened.executed_qty,
                open_avg_price: opened.avg_price,
                status: final_status,
                failure_reason: None,
                timestamp: Utc::now(),
            })
            .await;
        Ok(())
    }

    async fn persist_flipped_direction(
        &mut self,
        new_direction: GridDirection,
    ) -> std::result::Result<(), String> {
        let transition_state = Arc::clone(&self.state);
        let _scope_guard = transition_state.snapshot_scope_lock.write().await;
        let mut updated = self.state.config.read().await.clone();
        updated.grid.direction = new_direction;
        self.state
            .save_runtime_config(updated)
            .await
            .map_err(|error| format!("保存新交易方向失败：{}", error))?;
        self.paper_window_cache = None;
        Ok(())
    }
}
