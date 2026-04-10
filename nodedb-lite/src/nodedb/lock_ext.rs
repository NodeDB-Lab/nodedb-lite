//! Graceful mutex lock recovery.

use std::sync::Mutex;

/// Extension trait for graceful mutex lock recovery.
///
/// Recovers from poisoned mutexes (a thread panicked while holding the lock)
/// by extracting the inner guard. Logs at error level for observability.
pub(crate) trait LockExt<T> {
    fn lock_or_recover(&self) -> std::sync::MutexGuard<'_, T>;
}

impl<T> LockExt<T> for Mutex<T> {
    fn lock_or_recover(&self) -> std::sync::MutexGuard<'_, T> {
        self.lock().unwrap_or_else(|p| {
            tracing::error!("mutex poisoned, recovering guard");
            p.into_inner()
        })
    }
}
