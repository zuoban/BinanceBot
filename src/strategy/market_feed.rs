use super::*;

const MARKET_FALLBACK_AFTER_SECS: i64 = 8;

#[derive(Default)]
pub(super) struct MarketRetry {
    failures: u32,
    pub(super) retry_at: Option<Instant>,
}

impl MarketRetry {
    pub(super) fn ready(&self) -> bool {
        self.retry_at.is_none_or(|at| Instant::now() >= at)
    }

    pub(super) fn failed(&mut self) {
        let seconds = (2_u64 << self.failures.min(4)).min(30);
        let jitter_ms = rand::random::<u64>() % 501;
        self.retry_at = Some(Instant::now() + Duration::from_millis(seconds * 1000 + jitter_ms));
        self.failures = self.failures.saturating_add(1);
    }

    pub(super) fn reset(&mut self) {
        *self = Self::default();
    }
}

#[derive(Clone)]
pub(super) struct MarketRequestScope {
    pub(super) generation: u64,
    pub(super) account: [u8; 32],
    pub(super) symbol: String,
    pub(super) mode: TradingMode,
}

pub(super) struct MarkPriceCompletion {
    pub(super) scope: MarketRequestScope,
    pub(super) previous: TickerInfo,
    pub(super) result: Result<(Decimal, chrono::DateTime<Utc>)>,
}

pub(super) struct TickerCompletion {
    pub(super) scope: MarketRequestScope,
    pub(super) result: Result<TickerInfo>,
}

pub(super) async fn market_completion<T>(
    task: &mut Option<JoinHandle<T>>,
) -> std::result::Result<T, tokio::task::JoinError> {
    match task {
        Some(task) => task.await,
        None => std::future::pending().await,
    }
}

impl GridTradingEngine {
    pub(super) async fn market_data_ready(&mut self) -> bool {
        let ready = self.state.ticker.read().await.prices_are_fresh(Utc::now());
        let blocked = !ready;
        if self.market_data_blocked != blocked {
            self.market_data_blocked = blocked;
            let (level, message) = if blocked {
                ("WARN", "行情缺失或超过 15 秒未更新，暂停新增订单和窗口调整")
            } else {
                ("INFO", "行情已恢复，账户及订单对账有效后继续维护窗口")
            };
            self.state.add_log(level, message).await;
        }
        ready
    }

    pub(super) async fn update_paper_valuation(&self) {
        let config = self.state.config.read().await;
        let ticker = self.state.ticker.read().await;
        let mut position = self.state.position.write().await;
        let mut account = self.state.account.write().await;
        update_paper_account(&mut position, &mut account, &ticker);
        if let Err(error) =
            self.state
                .db
                .shared_paper_account(&config.exchange.symbol, &position, &account)
        {
            error!(%error, "Could not value shared paper account");
            self.state
                .paper_restore_failed
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    pub(super) async fn handle_ticker_update(&mut self, ticker_update: TickerInfo) {
        if ticker_update.symbol != self.state.config.read().await.exchange.symbol {
            return;
        }

        let (last_updated, mark_updated) = {
            let mut ticker = self.state.ticker.write().await;
            let now = Utc::now();
            let fresh = |time| is_timestamp_fresh(time, now, MARKET_PRICE_MAX_AGE_SECS);
            let last_updated = ticker_update.last_price > Decimal::ZERO
                && fresh(ticker_update.update_time)
                && ticker_update.update_time >= ticker.update_time;
            if last_updated {
                ticker.last_price = ticker_update.last_price;
                ticker.high_24h = ticker_update.high_24h;
                ticker.low_24h = ticker_update.low_24h;
                ticker.change_24h = ticker_update.change_24h;
                ticker.change_percent_24h = ticker_update.change_percent_24h;
                ticker.volume_24h = ticker_update.volume_24h;
                ticker.update_time = ticker_update.update_time;
            }
            let mark_updated = ticker_update.mark_price > Decimal::ZERO
                && fresh(ticker_update.mark_update_time)
                && ticker_update.mark_update_time >= ticker.mark_update_time;
            if mark_updated {
                ticker.mark_price = ticker_update.mark_price;
                ticker.mark_update_time = ticker_update.mark_update_time;
            }
            if ticker.symbol != ticker_update.symbol {
                ticker.symbol = ticker_update.symbol.clone();
            }
            (last_updated, mark_updated)
        };
        // Rejected market events cannot change risk inputs or trigger paper fills.
        if !last_updated && !mark_updated {
            return;
        }
        if last_updated {
            self.ticker_retry.reset();
        }
        if mark_updated {
            self.mark_retry.reset();
        }

        // Update unrealized PnL for current position
        let config = self.state.config.read().await.clone();
        let paper = config.exchange.is_paper();
        if paper {
            self.update_paper_valuation().await;
        }

        if !self.protect_openings_and_check_limits(&config).await {
            return;
        }
        let is_running = *self.state.status.read().await == BotStatus::Running;

        // In paper mode, match simulated orders against live market price
        if is_running
            && paper
            && last_updated
            && self.state.ticker.read().await.prices_are_fresh(Utc::now())
        {
            self.pending_paper_ticker = None;
            self.simulate_order_fills(ticker_update.last_price).await;
        }
        if mark_updated {
            self.match_pending_paper_ticker().await;
        }
    }

    /// Check simulated fills in paper mode
    pub(super) async fn simulate_order_fills(&mut self, current_price: Decimal) {
        if !self.position_direction_ready().await {
            return;
        }
        let mut filled_orders = Vec::new();

        {
            let orders = self.state.active_orders.read().await;
            for order in orders.values() {
                match order.side {
                    OrderSide::Buy => {
                        // Buy order fills when market price drops to or below order price
                        if current_price <= order.price {
                            filled_orders.push(order.clone());
                        }
                    }
                    OrderSide::Sell => {
                        // Sell order fills when market price rises to or above order price
                        if current_price >= order.price {
                            filled_orders.push(order.clone());
                        }
                    }
                }
            }
        }

        filled_orders.sort_by(|a, b| {
            a.side
                .as_str()
                .cmp(b.side.as_str())
                .then_with(|| {
                    if a.side == OrderSide::Buy {
                        b.price.cmp(&a.price)
                    } else {
                        a.price.cmp(&b.price)
                    }
                })
                .then_with(|| a.client_order_id.cmp(&b.client_order_id))
        });
        let had_fills = !filled_orders.is_empty();
        for mut order in filled_orders {
            if *self.state.status.read().await != BotStatus::Running {
                break;
            }
            let direction = self.state.config.read().await.grid.direction;
            if order.side == direction.closing_side() {
                order.quantity = order
                    .quantity
                    .min(direction.position_quantity(self.state.position.read().await.size));
                if order.quantity <= Decimal::ZERO {
                    self.state
                        .active_orders
                        .write()
                        .await
                        .remove(&order.client_order_id);
                    continue;
                }
                order.amount_usdc = order.price * order.quantity;
            }
            self.on_order_filled(&mut order).await;
            if self.pause_if_position_limit_reached().await {
                break;
            }
        }

        if !self.state.active_orders.read().await.is_empty() {
            self.maintain_grid_window_inner(!had_fills).await;
        }
    }

    pub(super) fn retire_market_requests(&mut self) {
        self.market_request_generation = self.market_request_generation.wrapping_add(1);
        self.pending_paper_ticker = None;
        if let Some(task) = self.mark_price_task.take() {
            task.abort();
        }
        if let Some(task) = self.ticker_task.take() {
            task.abort();
        }
    }

    pub(super) async fn market_request_scope(&self, symbol: &str) -> MarketRequestScope {
        MarketRequestScope {
            generation: self.market_request_generation,
            account: self.client.account_identity_key(),
            symbol: symbol.to_owned(),
            mode: self.state.config.read().await.exchange.trading_mode(),
        }
    }

    async fn market_scope_is_current(&self, scope: &MarketRequestScope) -> bool {
        let config = self.state.config.read().await;
        !self.state.is_shutting_down()
            && scope.generation == self.market_request_generation
            && scope.account == self.client.account_identity_key()
            && scope.symbol == config.exchange.symbol
            && scope.mode == config.exchange.trading_mode()
    }

    pub(super) async fn start_mark_price_request(&mut self, symbol: &str) {
        if self.mark_price_task.is_some() || self.state.is_shutting_down() {
            return;
        }
        let previous = self.state.ticker.read().await.clone();
        if previous.symbol == symbol
            && previous.mark_price > Decimal::ZERO
            && is_timestamp_fresh(
                previous.mark_update_time,
                Utc::now(),
                MARKET_FALLBACK_AFTER_SECS,
            )
        {
            self.mark_retry.reset();
            return;
        }
        if !self.mark_retry.ready() {
            return;
        }
        let scope = self.market_request_scope(symbol).await;
        let client = Arc::clone(&self.client);
        let cache = Arc::clone(&self.market_cache);
        self.mark_price_task = Some(tokio::spawn(async move {
            let result = cache.mark_price(&client, &scope.symbol).await;
            MarkPriceCompletion {
                scope,
                previous,
                result,
            }
        }));
    }

    pub(super) async fn refresh_mark_price(&mut self, symbol: &str) {
        self.start_mark_price_request(symbol).await;
        if let Some(task) = self.mark_price_task.take() {
            self.finish_mark_price_task(task.await).await;
        }
    }

    pub(super) async fn finish_mark_price_task(
        &mut self,
        completion: std::result::Result<MarkPriceCompletion, tokio::task::JoinError>,
    ) {
        self.mark_price_task = None;
        match completion {
            Ok(completion) => self.apply_mark_price_completion(completion).await,
            Err(error) => {
                self.mark_retry.failed();
                warn!(%error, "Mark-price fallback task failed");
            }
        }
    }

    pub(super) async fn apply_mark_price_completion(&mut self, completion: MarkPriceCompletion) {
        if !self.market_scope_is_current(&completion.scope).await {
            return;
        }
        let symbol = &completion.scope.symbol;
        match completion.result {
            Ok((price, requested_at)) if price > Decimal::ZERO => {
                self.mark_retry.reset();
                let previous = completion.previous;
                let now = Utc::now();
                let mut ticker = self.state.ticker.write().await;
                if !is_timestamp_fresh(requested_at, now, MARKET_PRICE_MAX_AGE_SECS)
                    || ticker.symbol != previous.symbol
                    || ticker.mark_update_time != previous.mark_update_time
                    || ticker.mark_price != previous.mark_price
                    || (ticker.mark_price > Decimal::ZERO && requested_at < ticker.mark_update_time)
                {
                    return;
                }
                ticker.symbol = symbol.clone();
                ticker.mark_price = price;
                ticker.mark_update_time = requested_at;
                drop(ticker);
                let pause_epoch = self.state.pause_epoch();
                if completion.scope.mode == TradingMode::Paper {
                    self.update_paper_valuation().await;
                }
                let config = self.state.config.read().await.clone();
                if self.protect_openings_and_check_limits(&config).await
                    && self.state.pause_epoch() == pause_epoch
                {
                    self.match_pending_paper_ticker().await;
                    if self.state.pause_epoch() == pause_epoch {
                        self.maintain_grid_window().await;
                    }
                }
            }
            Ok(_) => {
                self.mark_retry.failed();
                warn!("Binance returned a zero mark price for {}", symbol);
            }
            Err(e) => {
                self.mark_retry.failed();
                debug!("Could not refresh mark price for {}: {}", symbol, e);
            }
        }
    }

    pub(super) async fn start_ticker_request(&mut self, symbol: &str) {
        if self.ticker_task.is_some() || self.state.is_shutting_down() {
            return;
        }
        let ticker = self.state.ticker.read().await.clone();
        if ticker.symbol == symbol
            && is_timestamp_fresh(ticker.update_time, Utc::now(), MARKET_FALLBACK_AFTER_SECS)
        {
            self.ticker_retry.reset();
            return;
        }
        if !self.ticker_retry.ready() {
            return;
        }
        let scope = self.market_request_scope(symbol).await;
        let client = Arc::clone(&self.client);
        self.ticker_task = Some(tokio::spawn(async move {
            let mut update = ticker;
            update.symbol = scope.symbol.clone();
            update.mark_price = Decimal::ZERO;
            update.update_time = Utc::now();
            let result = async {
                match client.get_24hr_ticker_for_fallback(&scope.symbol).await {
                    Ok(stats) => {
                        update.last_price = stats.last_price;
                        update.high_24h = stats.high_price;
                        update.low_24h = stats.low_price;
                        update.change_24h = stats.price_change;
                        update.change_percent_24h = stats.price_change_percent;
                        update.volume_24h = stats.volume;
                    }
                    Err(e) => {
                        debug!("Could not refresh 24h ticker for {}: {}", scope.symbol, e);
                        update.update_time = Utc::now();
                        update.last_price =
                            client.get_ticker_price_for_fallback(&scope.symbol).await?;
                    }
                }
                anyhow::ensure!(
                    update.last_price > Decimal::ZERO,
                    "Zero fallback ticker price"
                );
                Ok(update)
            }
            .await;
            TickerCompletion { scope, result }
        }));
    }

    pub(super) async fn refresh_stale_ticker(&mut self, symbol: &str) {
        self.start_ticker_request(symbol).await;
        if let Some(task) = self.ticker_task.take() {
            self.finish_ticker_task(task.await).await;
        }
    }

    pub(super) async fn finish_ticker_task(
        &mut self,
        completion: std::result::Result<TickerCompletion, tokio::task::JoinError>,
    ) {
        self.ticker_task = None;
        match completion {
            Ok(completion) => self.apply_ticker_completion(completion).await,
            Err(error) => {
                self.ticker_retry.failed();
                warn!(%error, "Ticker fallback task failed");
            }
        }
    }

    pub(super) async fn apply_ticker_completion(&mut self, completion: TickerCompletion) {
        if !self.market_scope_is_current(&completion.scope).await {
            return;
        }
        match completion.result {
            Ok(update) => {
                let price = update.last_price;
                let timestamp = update.update_time;
                let pause_epoch = self.state.pause_epoch();
                self.handle_ticker_update(update).await;
                let running = *self.state.status.read().await == BotStatus::Running;
                let ticker = self.state.ticker.read().await;
                if completion.scope.mode == TradingMode::Paper
                    && running
                    && self.state.pause_epoch() == pause_epoch
                    && ticker.last_price == price
                    && ticker.update_time == timestamp
                    && !ticker.prices_are_fresh(Utc::now())
                {
                    // A ticker arriving before its mark must still be matched
                    // once valuation is fresh, using this exact price event.
                    self.pending_paper_ticker = Some((price, timestamp, pause_epoch));
                }
                drop(ticker);
                if self.state.pause_epoch() == pause_epoch {
                    let ticker = self.state.ticker.read().await;
                    let accepted = ticker.last_price == price && ticker.update_time == timestamp;
                    drop(ticker);
                    if accepted {
                        self.maintain_grid_window().await;
                    }
                }
            }
            Err(e) => {
                self.ticker_retry.failed();
                warn!(
                    "Could not refresh stale market price for {}: {}",
                    completion.scope.symbol, e
                );
            }
        }
    }

    async fn match_pending_paper_ticker(&mut self) {
        let Some((price, timestamp, pause_epoch)) = self.pending_paper_ticker else {
            return;
        };
        let ticker = self.state.ticker.read().await;
        if self.state.pause_epoch() != pause_epoch
            || ticker.last_price != price
            || ticker.update_time != timestamp
        {
            self.pending_paper_ticker = None;
            return;
        }
        if !ticker.prices_are_fresh(Utc::now()) {
            return;
        }
        drop(ticker);
        self.pending_paper_ticker = None;
        let config = self.state.config.read().await.clone();
        if config.exchange.is_paper()
            && *self.state.status.read().await == BotStatus::Running
            && self.protect_openings_and_check_limits(&config).await
        {
            self.simulate_order_fills(price).await;
        }
    }
}
