use super::AppState;
use crate::telegram::TradeNotificationContext;
use crate::types::{GridDirection, HourlyTradeStats, TradeRecord};
use chrono::Utc;
use std::sync::Arc;
use tracing::warn;

impl AppState {
    pub(super) async fn trade_notification_stats(
        &self,
        trade: &TradeRecord,
    ) -> Option<HourlyTradeStats> {
        let symbol = trade.symbol.clone();
        let mode = trade.mode;
        let timestamp = trade.timestamp;
        match self
            .db
            .run_blocking(move |db| db.get_hourly_trade_stats(&symbol, mode, timestamp))
            .await
        {
            Ok(stats) => Some(stats),
            Err(error) => {
                warn!("Could not load Telegram trade counts: {}", error);
                None
            }
        }
    }

    pub(super) async fn spawn_trade_telegram_notification(
        self: &Arc<Self>,
        telegram: crate::config::TelegramConfig,
        trade: TradeRecord,
        notification_stats: Option<HourlyTradeStats>,
        direction: GridDirection,
        sync_interval_secs: u64,
    ) {
        let account = self.current_account().await;
        let position = self.position.read().await.clone();
        let ticker = self.ticker.read().await.clone();
        let cny_rate = self.cny_rates.get(&account.asset).await;
        let context = TradeNotificationContext::for_trade(
            &trade,
            notification_stats.as_ref(),
            &account,
            cny_rate.as_ref(),
            Utc::now(),
            sync_interval_secs,
        )
        .with_position(&trade, &position, &ticker)
        .with_direction(direction);
        let notifier = self.telegram_notifier.clone();
        let state = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(err) = notifier
                .send_trade_notification(&telegram, &trade, &context)
                .await
            {
                warn!("Failed to send Telegram trade notification: {}", err);
                state
                    .add_log("WARN", format!("Telegram 成交通知发送失败: {}", err))
                    .await;
            }
        });
    }

    pub(super) async fn closing_trade_notification_target(
        &self,
        trade: &TradeRecord,
    ) -> Option<(crate::config::TelegramConfig, GridDirection, u64)> {
        let config = self.config.read().await;
        (config.telegram.enabled
            && trade.symbol == config.exchange.symbol
            && trade.mode == config.exchange.trading_mode()
            && trade.side == config.grid.direction.closing_side())
        .then(|| {
            (
                config.telegram.clone(),
                config.grid.direction,
                config.exchange.sync_interval_secs,
            )
        })
    }

    pub(super) async fn notify_verified_closing_trade(
        self: &Arc<Self>,
        mut trade: TradeRecord,
        target: (crate::config::TelegramConfig, GridDirection, u64),
    ) {
        let (telegram, direction, sync_interval_secs) = target;
        let notification_stats = self.trade_notification_stats(&trade).await;
        trade.pnl_verified = true;
        self.spawn_trade_telegram_notification(
            telegram,
            trade,
            notification_stats,
            direction,
            sync_interval_secs,
        )
        .await;
    }
}
