// SPDX-License-Identifier: Apache-2.0

//! [`LiteConfig::from_env`] — environment-variable overrides.
//!
//! Every variable is optional and independently applied: an absent or
//! malformed value leaves the corresponding default in place and logs a
//! warning rather than failing the open.

use super::types::LiteConfig;

impl LiteConfig {
    /// Load configuration from environment variables, falling back to defaults
    /// for any variable that is absent or malformed.
    ///
    /// Handled variables:
    /// - `NODEDB_LITE_MEMORY_MB` — total memory budget in mebibytes (parsed as `usize`)
    /// - `NODEDB_LITE_AUTO_FLUSH_MS` — auto-flush interval in milliseconds (parsed as `u64`;
    ///   0 = disabled)
    /// - `NODEDB_LITE_AUTO_COMPACT_MS` — auto-compact interval in milliseconds (parsed as `u64`;
    ///   0 = disabled, the default)
    /// - `NODEDB_LITE_OUTBOUND_QUEUE_CAP` — max pending entries per durable outbound queue
    ///   (parsed as `usize`; must be > 0)
    pub fn from_env() -> Self {
        let mut cfg = Self::default();

        if let Ok(val) = std::env::var("NODEDB_LITE_MEMORY_MB") {
            match val.trim().parse::<usize>() {
                Ok(mb) => {
                    let bytes = mb.saturating_mul(1024 * 1024);
                    tracing::info!(
                        env_var = "NODEDB_LITE_MEMORY_MB",
                        value = mb,
                        bytes,
                        "environment variable override applied"
                    );
                    cfg.memory_budget = bytes;
                }
                Err(_) => {
                    tracing::warn!(
                        env_var = "NODEDB_LITE_MEMORY_MB",
                        value = %val,
                        "ignoring malformed environment variable (expected unsigned integer), \
                         using default 100 MiB"
                    );
                }
            }
        }

        if let Ok(val) = std::env::var("NODEDB_LITE_OUTBOUND_QUEUE_CAP") {
            match val.trim().parse::<usize>() {
                Ok(cap) if cap > 0 => {
                    tracing::info!(
                        env_var = "NODEDB_LITE_OUTBOUND_QUEUE_CAP",
                        value = cap,
                        "environment variable override applied"
                    );
                    cfg.outbound_queue_cap = cap;
                }
                Ok(_) => {
                    tracing::warn!(
                        env_var = "NODEDB_LITE_OUTBOUND_QUEUE_CAP",
                        "value must be > 0; using default 100_000"
                    );
                }
                Err(_) => {
                    tracing::warn!(
                        env_var = "NODEDB_LITE_OUTBOUND_QUEUE_CAP",
                        value = %val,
                        "ignoring malformed environment variable (expected unsigned integer), \
                         using default 100_000"
                    );
                }
            }
        }

        if let Ok(val) = std::env::var("NODEDB_LITE_AUTO_FLUSH_MS") {
            match val.trim().parse::<u64>() {
                Ok(ms) => {
                    tracing::info!(
                        env_var = "NODEDB_LITE_AUTO_FLUSH_MS",
                        value = ms,
                        "environment variable override applied"
                    );
                    cfg.auto_flush_ms = ms;
                }
                Err(_) => {
                    tracing::warn!(
                        env_var = "NODEDB_LITE_AUTO_FLUSH_MS",
                        value = %val,
                        "ignoring malformed environment variable (expected unsigned integer), \
                         using default 1000 ms"
                    );
                }
            }
        }

        if let Ok(val) = std::env::var("NODEDB_LITE_AUTO_COMPACT_MS") {
            match val.trim().parse::<u64>() {
                Ok(ms) => {
                    tracing::info!(
                        env_var = "NODEDB_LITE_AUTO_COMPACT_MS",
                        value = ms,
                        "environment variable override applied"
                    );
                    cfg.auto_compact_ms = ms;
                }
                Err(_) => {
                    tracing::warn!(
                        env_var = "NODEDB_LITE_AUTO_COMPACT_MS",
                        value = %val,
                        "ignoring malformed environment variable (expected unsigned integer), \
                         using default 0 (disabled)"
                    );
                }
            }
        }

        cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// All `from_env` cases run sequentially in one test to avoid parallel
    /// env-var mutation across threads (no `serial_test` dependency needed).
    #[test]
    fn from_env_all_cases() {
        // Use a mutex so if other test files ever share this process they
        // cannot race on the env var.
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        // SAFETY: we hold ENV_LOCK and are the only thread touching this var.

        // Case 1: var absent → default.
        unsafe { std::env::remove_var("NODEDB_LITE_MEMORY_MB") };
        let cfg = LiteConfig::from_env();
        assert_eq!(
            cfg.memory_budget,
            100 * 1024 * 1024,
            "absent var should give default 100 MiB"
        );

        // Case 2: valid integer → applied.
        unsafe { std::env::set_var("NODEDB_LITE_MEMORY_MB", "256") };
        let cfg = LiteConfig::from_env();
        assert_eq!(
            cfg.memory_budget,
            256 * 1024 * 1024,
            "256 MiB should be applied"
        );

        // Case 3: malformed → fallback to default.
        unsafe { std::env::set_var("NODEDB_LITE_MEMORY_MB", "not_a_number") };
        let cfg = LiteConfig::from_env();
        assert_eq!(
            cfg.memory_budget,
            100 * 1024 * 1024,
            "malformed var should fall back to default"
        );

        // Case 4: whitespace-padded integer → trimmed and applied.
        unsafe { std::env::set_var("NODEDB_LITE_MEMORY_MB", "  512  ") };
        let cfg = LiteConfig::from_env();
        assert_eq!(
            cfg.memory_budget,
            512 * 1024 * 1024,
            "padded value should be trimmed and applied"
        );

        // Cleanup.
        unsafe { std::env::remove_var("NODEDB_LITE_MEMORY_MB") };

        // NODEDB_LITE_AUTO_FLUSH_MS cases.

        // Case A: var absent → default 1000.
        unsafe { std::env::remove_var("NODEDB_LITE_AUTO_FLUSH_MS") };
        let cfg = LiteConfig::from_env();
        assert_eq!(
            cfg.auto_flush_ms, 1_000,
            "absent var should give default 1000 ms"
        );

        // Case B: valid integer → applied.
        unsafe { std::env::set_var("NODEDB_LITE_AUTO_FLUSH_MS", "500") };
        let cfg = LiteConfig::from_env();
        assert_eq!(cfg.auto_flush_ms, 500, "500 ms should be applied");

        // Case C: 0 = disabled.
        unsafe { std::env::set_var("NODEDB_LITE_AUTO_FLUSH_MS", "0") };
        let cfg = LiteConfig::from_env();
        assert_eq!(cfg.auto_flush_ms, 0, "0 should disable auto-flush");

        // Case D: malformed → fallback to default.
        unsafe { std::env::set_var("NODEDB_LITE_AUTO_FLUSH_MS", "not_a_number") };
        let cfg = LiteConfig::from_env();
        assert_eq!(
            cfg.auto_flush_ms, 1_000,
            "malformed var should fall back to default 1000 ms"
        );

        // Cleanup.
        unsafe { std::env::remove_var("NODEDB_LITE_AUTO_FLUSH_MS") };

        // NODEDB_LITE_AUTO_COMPACT_MS cases.

        // Case A: var absent → default 0 (disabled).
        unsafe { std::env::remove_var("NODEDB_LITE_AUTO_COMPACT_MS") };
        let cfg = LiteConfig::from_env();
        assert_eq!(
            cfg.auto_compact_ms, 0,
            "absent var should give default 0 (disabled)"
        );

        // Case B: valid integer → applied.
        unsafe { std::env::set_var("NODEDB_LITE_AUTO_COMPACT_MS", "300000") };
        let cfg = LiteConfig::from_env();
        assert_eq!(cfg.auto_compact_ms, 300_000, "300000 ms should be applied");

        // Case C: malformed → fallback to default 0.
        unsafe { std::env::set_var("NODEDB_LITE_AUTO_COMPACT_MS", "not_a_number") };
        let cfg = LiteConfig::from_env();
        assert_eq!(
            cfg.auto_compact_ms, 0,
            "malformed var should fall back to default 0"
        );

        // Cleanup.
        unsafe { std::env::remove_var("NODEDB_LITE_AUTO_COMPACT_MS") };
    }
}
