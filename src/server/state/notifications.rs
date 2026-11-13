use super::AppState;
use crate::telegram::TradeNotificationContext;
use crate::types::{GridDirection, HourlyTradeStats, TradeRecord};
use chrono::Utc;
use std::sync::Arc;
use tracing::warn;

#[derive(Clone)]
pub(crate) struct NotificationChannels {
    pub(crate) telegram: crate::config::TelegramConfig,
    pub(crate) ntfy: crate::config::NtfyConfig,
}

impl NotificationChannels {
    pub(crate) fn from_app(config: &crate::config::AppConfig) -> Self {
        Self {
            telegram: config.telegram.clone(),
            ntfy: config.ntfy.clone(),
        }
    }

    pub(crate) fn from_global(config: &crate::config::GlobalConfig) -> Self {
        Self {
            telegram: config.telegram.clone(),
            ntfy: config.ntfy.clone(),
        }
    }

    pub(crate) fn any_enabled(&self) -> bool {
        self.telegram.enabled || self.ntfy.enabled
    }
}

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
                warn!("Could not load trade notification counts: {}", error);
                None
            }
        }
    }

    pub(super) async fn spawn_trade_notifications(
        self: &Arc<Self>,
        channels: NotificationChannels,
        trade: TradeRecord,
        notification_stats: Option<HourlyTradeStats>,
        direction: GridDirection,
        sync_interval_secs: u64,
    ) {
        let account = self.current_account().await;
        let position = self.position.read().await.clone();
        let ticker = self.ticker.read().await.clone();
        let cny_rate = self.cny_rates.get(&account.asset).await;
        let context = Arc::new(
            TradeNotificationContext::for_trade(
                &trade,
                notification_stats.as_ref(),
                &account,
                cny_rate.as_ref(),
                Utc::now(),
                sync_interval_secs,
            )
            .with_position(&trade, &position, &ticker)
            .with_direction(direction),
        );
        let trade = Arc::new(trade);
        if channels.telegram.enabled {
            let notifier = self.telegram_notifier.clone();
            let telegram = channels.telegram;
            let trade = Arc::clone(&trade);
            let context = Arc::clone(&context);
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
        if channels.ntfy.enabled {
            let notifier = self.ntfy_notifier.clone();
            let ntfy = channels.ntfy;
            let trade = Arc::clone(&trade);
            let context = Arc::clone(&context);
            let state = Arc::clone(self);
            tokio::spawn(async move {
                if let Err(err) = notifier
                    .send_trade_notification(&ntfy, &trade, &context)
                    .await
                {
                    warn!("Failed to send ntfy trade notification: {}", err);
                    state
                        .add_log("WARN", format!("ntfy 成交通知发送失败: {}", err))
                        .await;
                }
            });
        }
    }

    pub(super) async fn closing_trade_notification_target(
        &self,
        trade: &TradeRecord,
    ) -> Option<(NotificationChannels, GridDirection, u64)> {
        let config = self.config.read().await;
        let channels = NotificationChannels::from_app(&config);
        (channels.any_enabled()
            && trade.symbol == config.exchange.symbol
            && trade.mode == config.exchange.trading_mode()
            && trade.side == config.grid.direction.closing_side())
        .then(|| {
            (
                channels,
                config.grid.direction,
                config.exchange.sync_interval_secs,
            )
        })
    }

    pub(super) async fn notify_verified_closing_trade(
        self: &Arc<Self>,
        mut trade: TradeRecord,
        target: (NotificationChannels, GridDirection, u64),
    ) {
        let (channels, direction, sync_interval_secs) = target;
        let notification_stats = self.trade_notification_stats(&trade).await;
        trade.pnl_verified = true;
        self.spawn_trade_notifications(
            channels,
            trade,
            notification_stats,
            direction,
            sync_interval_secs,
        )
        .await;
    }
}
