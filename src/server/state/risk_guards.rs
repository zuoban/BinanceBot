use super::{AppState, GlobalPositionLimit};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard};

#[must_use = "dropping this guard immediately releases the lock or barrier"]
pub(crate) struct ExchangeSubmissionGuard {
    state: Arc<AppState>,
    global: Option<Arc<GlobalPositionLimit>>,
    _barrier: Option<OwnedRwLockReadGuard<()>>,
    completed: bool,
}

impl ExchangeSubmissionGuard {
    pub(crate) fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for ExchangeSubmissionGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.state
                .global_position_ready
                .store(false, Ordering::SeqCst);
            if let Some(global) = &self.global {
                global
                    .fill_failure_generation
                    .fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

#[must_use = "dropping this guard immediately releases the lock or barrier"]
pub(crate) struct PendingFillReconciliation {
    state: Arc<AppState>,
    global: Option<Arc<GlobalPositionLimit>>,
    barrier: Option<OwnedRwLockWriteGuard<()>>,
    resolved: bool,
}

impl PendingFillReconciliation {
    pub(crate) async fn acquire(&mut self) {
        if let Some(global) = &self.global {
            if global.enabled.load(Ordering::SeqCst) {
                self.barrier = Some(global.submission_barrier.clone().write_owned().await);
            }
        }
    }

    pub(crate) fn resolve(&mut self) {
        self.resolved = true;
    }
}

impl Drop for PendingFillReconciliation {
    fn drop(&mut self) {
        if !self.resolved {
            self.state
                .global_position_ready
                .store(false, Ordering::SeqCst);
        }
        if let Some(global) = &self.global {
            if !self.resolved {
                global
                    .fill_failure_generation
                    .fetch_add(1, Ordering::SeqCst);
            }
            global
                .pending_fill_reconciliations
                .fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl AppState {
    pub(crate) fn publish_fill_reconciliation(self: &Arc<Self>) -> PendingFillReconciliation {
        let global = self.global_limit();
        if let Some(global) = &global {
            global
                .pending_fill_reconciliations
                .fetch_add(1, Ordering::SeqCst);
        }
        PendingFillReconciliation {
            state: self.clone(),
            global,
            barrier: None,
            resolved: false,
        }
    }

    pub(crate) async fn begin_fill_reconciliation(self: &Arc<Self>) -> PendingFillReconciliation {
        let mut guard = self.publish_fill_reconciliation();
        guard.acquire().await;
        guard
    }

    pub(crate) fn try_exchange_submission(self: &Arc<Self>) -> Option<ExchangeSubmissionGuard> {
        let global = self.global_limit();
        let barrier = if let Some(global) = &global {
            if global.enabled.load(Ordering::SeqCst) {
                Some(global.submission_barrier.clone().try_read_owned().ok()?)
            } else {
                None
            }
        } else {
            None
        };
        if self.is_shutting_down()
            || self.global_cancel_pending.load(Ordering::SeqCst)
            || global
                .as_ref()
                .is_some_and(|global| global.blocks_submission())
        {
            return None;
        }
        Some(ExchangeSubmissionGuard {
            state: self.clone(),
            global,
            _barrier: barrier,
            completed: false,
        })
    }

    pub(crate) fn try_order_submission(&self) -> Option<OwnedRwLockReadGuard<()>> {
        // Skip replenishment instead of waiting: the engine must stay available for
        // configuration actions and cancellation while the coordinator holds the writer.
        if self.is_shutting_down() || self.global_cancel_pending.load(Ordering::SeqCst) {
            return None;
        }
        if self
            .global_limit()
            .is_some_and(|global| global.blocks_submission())
        {
            return None;
        }
        self.order_submission_lock.clone().try_read_owned().ok()
    }
}
