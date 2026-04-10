//! Platform-specific async runtime abstractions.
//!
//! NodeDB-Lite compiles for native (Tokio) and WASM (`wasm-bindgen-futures`).
//! This module provides a thin abstraction over the differences so engine
//! code doesn't need `#[cfg]` everywhere.
//!
//! **Native (iOS/Android/Desktop):** Tokio — `spawn`, `spawn_blocking`, `sleep`.
//! **WASM (Browser):** `wasm-bindgen-futures` — `spawn_local`, no blocking threads.

use std::future::Future;
use std::time::Duration;

/// Spawn a future on the runtime.
///
/// - Native: `tokio::spawn` (runs on Tokio thread pool, requires `Send`).
/// - WASM: `wasm_bindgen_futures::spawn_local` (runs on the microtask queue).
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
    // wasm_bindgen_futures::spawn_local(future);
    // For now, this is a compile-gate placeholder. The actual
    // wasm-bindgen-futures dependency is added when WASM support
    // is fully wired.
    let _ = future;
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
/// - WASM: placeholder (will use `gloo_timers` or JS `setTimeout` via wasm-bindgen).
#[cfg(not(target_arch = "wasm32"))]
pub async fn sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

#[cfg(target_arch = "wasm32")]
pub async fn sleep(duration: Duration) {
    // Placeholder for WASM sleep. In production, this would use:
    // gloo_timers::future::sleep(duration).await
    let _ = duration;
}

/// Create a recurring interval timer.
///
/// Returns a stream-like async function that yields at each tick.
/// Used by the sync client for periodic keepalive and vector clock exchange.
///
/// - Native: `tokio::time::interval`.
/// - WASM: placeholder (will use `gloo_timers::future::IntervalStream`).
#[cfg(not(target_arch = "wasm32"))]
pub fn interval(period: Duration) -> tokio::time::Interval {
    tokio::time::interval(period)
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
        // js_sys::Date::now() returns milliseconds as f64.
        // For now, return 0 — wired when wasm-bindgen is added.
        0
    }
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
    async fn interval_creation() {
        let _iv = interval(Duration::from_secs(1));
        // Just verify it compiles and doesn't panic.
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
