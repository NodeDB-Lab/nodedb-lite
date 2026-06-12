//! Platform-specific async runtime abstractions.
//!
//! NodeDB-Lite compiles for native (Tokio) and WASM (`wasm-bindgen-futures`).
//! This module provides a thin abstraction over the differences so engine
//! code doesn't need `#[cfg]` everywhere.
//!
//! **Native (iOS/Android/Desktop):** Tokio — `spawn`, `spawn_blocking`, `sleep`, `interval`.
//! **WASM (Browser):** `wasm-bindgen-futures` + `gloo-timers` — `spawn_local`, no blocking
//! threads, timer-backed sleep and interval.

use std::future::Future;
use std::time::Duration;

/// Spawn a future on the runtime.
///
/// - Native: `tokio::spawn` (runs on Tokio thread pool, requires `Send`).
/// - WASM: `wasm_bindgen_futures::spawn_local` (runs on the microtask queue,
///   no `Send` requirement).
#[cfg(not(target_arch = "wasm32"))]
pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future);
}

#[cfg(target_arch = "wasm32")]
pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

/// Run a blocking closure off the async runtime.
///
/// - Native: `tokio::task::spawn_blocking` — moves closure to the blocking pool.
/// - WASM: Runs synchronously (WASM has no blocking pool; callers must
///   ensure the closure is fast or use the async StorageEngine path).
#[cfg(not(target_arch = "wasm32"))]
pub async fn spawn_blocking<F, T>(f: F) -> Result<T, crate::error::LiteError>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| crate::error::LiteError::JoinError {
            detail: e.to_string(),
        })
}

#[cfg(target_arch = "wasm32")]
pub async fn spawn_blocking<F, T>(f: F) -> Result<T, crate::error::LiteError>
where
    F: FnOnce() -> T,
{
    // No blocking pool on WASM — run synchronously.
    // This is acceptable because:
    // 1. SQLite WASM operations are fast (in-memory or OPFS sync access)
    // 2. HNSW/CSR operations are CPU-bound but sub-millisecond for edge datasets
    Ok(f())
}

/// Sleep for a duration.
///
/// - Native: `tokio::time::sleep`.
/// - WASM: `gloo_timers::future::sleep` (backed by JS `setTimeout`).
#[cfg(not(target_arch = "wasm32"))]
pub async fn sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

#[cfg(target_arch = "wasm32")]
pub async fn sleep(duration: Duration) {
    gloo_timers::future::sleep(duration).await;
}

/// A recurring interval timer.
///
/// Obtain one via [`interval`]. Call `.tick().await` to wait for each period.
///
/// On native the first `tick()` returns immediately (matches Tokio semantics).
/// On WASM the first `tick()` waits one full period. The primary consumer
/// (sync keepalive) tolerates either behaviour.
pub struct Interval {
    #[cfg(not(target_arch = "wasm32"))]
    inner: tokio::time::Interval,
    #[cfg(target_arch = "wasm32")]
    period: Duration,
}

impl Interval {
    /// Wait until the next tick.
    pub async fn tick(&mut self) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.inner.tick().await;
        }
        #[cfg(target_arch = "wasm32")]
        {
            gloo_timers::future::sleep(self.period).await;
        }
    }
}

/// Create a recurring interval timer that ticks every `period`.
///
/// - Native: wraps `tokio::time::interval`; first tick is immediate.
/// - WASM: backed by `gloo_timers`; first tick waits one period.
pub fn interval(period: Duration) -> Interval {
    #[cfg(not(target_arch = "wasm32"))]
    {
        Interval {
            inner: tokio::time::interval(period),
        }
    }
    #[cfg(target_arch = "wasm32")]
    {
        Interval { period }
    }
}

/// Get the current timestamp in milliseconds since Unix epoch.
///
/// Platform-independent — works on native and WASM.
pub fn now_millis() -> u64 {
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
    #[cfg(target_arch = "wasm32")]
    {
        // js_sys::Date::now() returns milliseconds since epoch as f64.
        js_sys::Date::now() as u64
    }
}

/// Get the current timestamp in milliseconds since Unix epoch, as `i64`.
///
/// Same clock as [`now_millis`] but signed, for the system/valid-time fields
/// used by the bitemporal engines. Platform-independent — works on native and
/// WASM.
pub fn now_millis_i64() -> i64 {
    now_millis() as i64
}

/// Get the current timestamp in seconds since Unix epoch.
pub fn now_secs() -> u64 {
    now_millis() / 1000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn spawn_blocking_works() {
        let result = spawn_blocking(|| 42).await.unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn spawn_blocking_string() {
        let result = spawn_blocking(|| "hello".to_string()).await.unwrap();
        assert_eq!(result, "hello");
    }

    #[tokio::test]
    async fn sleep_returns() {
        // Just verify it doesn't hang.
        sleep(Duration::from_millis(1)).await;
    }

    #[test]
    fn now_millis_nonzero() {
        let ts = now_millis();
        assert!(ts > 0, "timestamp should be nonzero on native");
    }

    #[test]
    fn now_secs_reasonable() {
        let ts = now_secs();
        // Should be after 2024-01-01 (1704067200).
        assert!(ts > 1_704_067_200, "timestamp {ts} seems too old");
    }

    #[tokio::test]
    async fn interval_ticks_twice() {
        let mut iv = interval(Duration::from_millis(1));
        // First tick is immediate on native (Tokio semantics).
        iv.tick().await;
        // Second tick waits one period — should still resolve promptly.
        iv.tick().await;
    }

    #[tokio::test]
    async fn spawn_fires() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        spawn(async move {
            let _ = tx.send(42);
        });
        let val = rx.await.unwrap();
        assert_eq!(val, 42);
    }
}
