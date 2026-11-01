use super::*;

impl GridTradingEngine {
    /// Reconcile open orders from Binance in Live mode
    pub(super) async fn sync_live_orders(&mut self) -> bool {
        self.sync_live_orders_with_priority(ReconciliationPriority::Critical)
            .await
    }

    pub(super) async fn sync_live_orders_with_priority(
        &mut self,
        priority: ReconciliationPriority,
    ) -> bool {
        let pause_epoch = self.state.pause_epoch();
        let config = self.state.config.read().await.clone();
        let symbol = config.exchange.symbol.clone();
        let mode = TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet);
        let intent_symbol = symbol.clone();
        let pair_intents = match self
            .state
            .db
            .run_blocking(move |db| db.load_pair_intents(&intent_symbol, mode))
            .await
        {
            Ok(intents) => intents
                .into_iter()
                .map(|intent| (intent.client_order_id.clone(), intent))
                .collect::<HashMap<_, _>>(),
            Err(e) => {
                error!("Could not load pending pair intents: {}", e);
                self.report_reconciliation_state(true).await;
                return false;
            }
        };
        let mut remainder_plan = match self.load_remainder_plan().await {
            Ok(plan) => plan,
            Err(e) => {
                error!("Could not load remainder recovery metadata: {}", e);
                self.last_orders_sync = None;
                return false;
            }
        };
        let open_orders = match priority {
            ReconciliationPriority::Periodic => {
                self.client.get_open_orders_for_periodic_sync(&symbol).await
            }
            ReconciliationPriority::Critical => self.client.get_open_orders(&symbol).await,
        };
        match open_orders {
            Ok(live_orders) => {
                self.last_orders_sync = None;
                if self.state.pause_epoch() != pause_epoch {
                    return false;
                }
                let live_ids: HashMap<String, BinanceOrderResponse> = live_orders
                    .into_iter()
                    .filter(|order| is_grid_order(&order.client_order_id))
                    .map(|o| (o.client_order_id.clone(), o))
                    .collect();

                let mut filled_or_closed = Vec::new();
                let mut persistence_ok = true;
                let mut orders_to_persist = Vec::new();

                {
                    let mut active = self.state.active_orders.write().await;
                    for (client_id, live_order) in &live_ids {
                        if let Some(plan) = &remainder_plan {
                            if let Some(metadata) = plan
                                .sources
                                .iter()
                                .chain(std::iter::once(&plan.target))
                                .find(|order| &order.client_order_id == client_id)
                            {
                                active.insert(client_id.clone(), metadata.clone());
                            }
                        }
                        if let Some(intent) = pair_intents.get(client_id) {
                            active.insert(client_id.clone(), intent.clone());
                        }
                        if let Some(order) = active.get_mut(client_id) {
                            order.order_id = Some(live_order.order_id);
                            order.quantity =
                                (live_order.orig_qty - live_order.executed_qty).max(Decimal::ZERO);
                            order.amount_usdc = order.price * order.quantity;
                            order.status = if live_order.executed_qty > Decimal::ZERO {
                                OrderStatus::PartiallyFilled
                            } else {
                                OrderStatus::New
                            };
                            orders_to_persist.push(order.clone());
                        } else {
                            let order = GridOrder {
                                client_order_id: client_id.clone(),
                                order_id: Some(live_order.order_id),
                                symbol: live_order.symbol.clone(),
                                side: if live_order.side == "BUY" {
                                    OrderSide::Buy
                                } else {
                                    OrderSide::Sell
                                },
                                price: live_order.price,
                                quantity: (live_order.orig_qty - live_order.executed_qty)
                                    .max(Decimal::ZERO),
                                amount_usdc: live_order.price
                                    * (live_order.orig_qty - live_order.executed_qty)
                                        .max(Decimal::ZERO),
                                status: if live_order.executed_qty > Decimal::ZERO {
                                    OrderStatus::PartiallyFilled
                                } else {
                                    OrderStatus::New
                                },
                                created_at: Utc::now(),
                                updated_at: Utc::now(),
                                grid_level: 0,
                                paired_client_order_id: None,
                                is_take_profit: false,
                                purpose: crate::types::OrderPurpose::Legacy,
                                merge_sources: Vec::new(),
                            };
                            orders_to_persist.push(order.clone());
                            active.insert(client_id.clone(), order);
                        }
                    }
                }

                if !orders_to_persist.is_empty() {
                    if let Err(e) = self
                        .state
                        .db
                        .run_blocking(move |db| db.save_managed_orders(&orders_to_persist))
                        .await
                    {
                        error!("Could not persist reconciled orders: {}", e);
                        persistence_ok = false;
                    }
                }
                if self.state.pause_epoch() != pause_epoch {
                    return false;
                }

                {
                    let active = self.state.active_orders.read().await;
                    for (client_id, order) in active.iter() {
                        if !live_ids.contains_key(client_id) {
                            filled_or_closed.push(order.clone());
                        }
                    }
                }

                let mut terminal_orders = Vec::new();
                let mut unresolved_orders = false;
                for mut order in filled_or_closed {
                    if self.state.pause_epoch() != pause_epoch {
                        return false;
                    }
                    let exchange_result = match order.order_id {
                        Some(order_id) => self
                            .client
                            .get_order(&order.symbol, order_id)
                            .await
                            .map(Some),
                        None => {
                            self.client
                                .lookup_order_by_client_id(&order.symbol, &order.client_order_id)
                                .await
                        }
                    };
                    if self.state.pause_epoch() != pause_epoch {
                        return false;
                    }
                    match exchange_result {
                        Ok(None)
                            if (Utc::now() - order.created_at).num_seconds()
                                >= UNKNOWN_SUBMISSION_GRACE_SECS =>
                        {
                            if let Err(error) =
                                self.delete_managed_order(&order.client_order_id).await
                            {
                                error!(
                                    "Could not retire absent order intent {}: {}",
                                    order.client_order_id, error
                                );
                                persistence_ok = false;
                            } else {
                                self.state
                                    .active_orders
                                    .write()
                                    .await
                                    .remove(&order.client_order_id);
                                self.state.add_log("WARN", format!("Unaccepted order intent {} retired after confirmed absence", order.client_order_id)).await;
                            }
                        }
                        Ok(None) => unresolved_orders = true,
                        Ok(Some(exchange_order)) => {
                            // A query may confirm acceptance before the open-
                            // orders snapshot catches up. Persist it before a
                            // later lookup or restart can lose that distinction.
                            if order.order_id.is_none() {
                                order.order_id = Some(exchange_order.order_id);
                                if let Err(error) = self.save_managed_order(&order).await {
                                    error!(
                                        "Could not persist confirmed order {}: {}",
                                        order.client_order_id, error
                                    );
                                    persistence_ok = false;
                                    continue;
                                }
                                self.state
                                    .active_orders
                                    .write()
                                    .await
                                    .insert(order.client_order_id.clone(), order.clone());
                            }
                            let terminal = matches!(
                                exchange_order.status.as_str(),
                                "FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
                            );
                            if !terminal {
                                unresolved_orders = true;
                                continue;
                            }
                            terminal_orders.push((order, exchange_order));
                        }
                        Err(e) => {
                            warn!(
                                "Could not verify order {} status: {}",
                                order.client_order_id, e
                            );
                            unresolved_orders = true;
                        }
                    }
                }

                if !persistence_ok || unresolved_orders {
                    self.report_reconciliation_state(true).await;
                    return false;
                }
                self.last_orders_sync = Some(Instant::now());
                terminal_orders
                    .sort_by_key(|(_, response)| (response.update_time, response.order_id));
                for (mut order, exchange_order) in terminal_orders {
                    if exchange_order.executed_qty > Decimal::ZERO {
                        order.updated_at = exchange_fill_time(&exchange_order);
                        order.quantity = exchange_order.executed_qty;
                        if let Some(avg_price) = exchange_order
                            .avg_price
                            .filter(|price| *price > Decimal::ZERO)
                        {
                            order.price = avg_price;
                        }
                        order.amount_usdc = order.price * order.quantity;
                        if !self.on_order_filled(&mut order).await {
                            persistence_ok = false;
                        }
                    } else {
                        if let Err(e) = self.delete_managed_order(&order.client_order_id).await {
                            error!(
                                "Could not delete closed order {}: {}",
                                order.client_order_id, e
                            );
                            persistence_ok = false;
                        } else {
                            self.state
                                .active_orders
                                .write()
                                .await
                                .remove(&order.client_order_id);
                        }
                        debug!(
                            "Order {} closed without a fill ({})",
                            order.client_order_id, exchange_order.status
                        );
                    }
                }
                if persistence_ok {
                    if let Some(plan) = &mut remainder_plan {
                        if plan.phase == RemainderPhase::Submitting {
                            if let Err(e) = self.adopt_remainder_target(plan).await {
                                warn!("Remainder submission remains unresolved: {}", e);
                                persistence_ok = false;
                            }
                        }
                    }
                }
                if !persistence_ok {
                    self.last_orders_sync = None;
                } else if !self.drain_pair_intents(&symbol, mode).await {
                    persistence_ok = false;
                    self.last_orders_sync = None;
                }
                self.report_reconciliation_state(!persistence_ok).await;
                persistence_ok
            }
            Err(e) => {
                warn!("Error during live orders sync: {}", e);
                self.last_orders_sync = None;
                self.report_reconciliation_state(true).await;
                false
            }
        }
    }
}
