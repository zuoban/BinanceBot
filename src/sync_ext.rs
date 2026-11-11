use std::sync::{Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub(crate) trait MutexUnpoisonedExt<T> {
    fn lock_unpoisoned(&self) -> MutexGuard<'_, T>;
}

impl<T> MutexUnpoisonedExt<T> for Mutex<T> {
    #[inline]
    fn lock_unpoisoned(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

pub(crate) trait RwLockUnpoisonedExt<T> {
    fn read_unpoisoned(&self) -> RwLockReadGuard<'_, T>;
    fn write_unpoisoned(&self) -> RwLockWriteGuard<'_, T>;
}

impl<T> RwLockUnpoisonedExt<T> for RwLock<T> {
    #[inline]
    fn read_unpoisoned(&self) -> RwLockReadGuard<'_, T> {
        self.read().unwrap_or_else(PoisonError::into_inner)
    }

    #[inline]
    fn write_unpoisoned(&self) -> RwLockWriteGuard<'_, T> {
        self.write().unwrap_or_else(PoisonError::into_inner)
    }
}
