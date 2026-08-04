// SPDX-License-Identifier: Apache-2.0

//! The [`LiteConfig`] struct and its defaults.

use serde::{Deserialize, Serialize};

use super::defaults::{
    default_argon2_m_cost, default_argon2_p_cost, default_argon2_t_cost, default_auto_compact_ms,
    default_auto_flush_ms, default_kv_cache_capacity, default_outbound_queue_cap,
    default_sync_enabled,
};
use crate::storage::corruption::CorruptionPolicy;

/// Runtime configuration for a NodeDB-Lite instance.
///
/// All percentage fields express a fraction of `memory_budget` allocated to
/// the corresponding engine. The remaining percentage is headroom (untracked).
///
/// # Example
/// ```
/// use nodedb_lite::config::LiteConfig;
///
/// let cfg = LiteConfig {
///     memory_budget: 256 * 1024 * 1024, // 256 MiB
///     ..LiteConfig::default()
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiteConfig {
    /// Total memory budget in bytes. Default: 100 MiB.
    pub memory_budget: usize,

    /// Percentage of `memory_budget` reserved for HNSW vector index. Default: 40.
    pub hnsw_percent: usize,

    /// Percentage of `memory_budget` reserved for CSR graph index. Default: 15.
    pub csr_percent: usize,

    /// Percentage of `memory_budget` reserved for Loro CRDT engine. Default: 15.
    pub loro_percent: usize,

    /// Percentage of `memory_budget` reserved for query scratch space. Default: 15.
    pub query_percent: usize,

    /// Enable CRDT sync for KV operations. Default: `true`.
    ///
    /// When `false`, KV operations go directly to the B+ tree, bypassing
    /// Loro entirely. This gives SQLite-class performance for local-only use.
    /// Other engines (vector, graph, document) still use Loro for their storage.
    ///
    /// When `true`, KV writes also generate sync log entries (append-only)
    /// for replication to Origin via LWW merge.
    #[serde(default = "default_sync_enabled")]
    pub sync_enabled: bool,

    /// Argon2id memory cost in KiB. Default: 19 MiB (19_456 KiB).
    /// Corresponds to the OWASP recommended minimum for interactive login.
    #[serde(default = "default_argon2_m_cost")]
    pub argon2_m_cost: u32,

    /// Argon2id iteration count. Default: 2.
    #[serde(default = "default_argon2_t_cost")]
    pub argon2_t_cost: u32,

    /// Argon2id parallelism lanes. Default: 1.
    #[serde(default = "default_argon2_p_cost")]
    pub argon2_p_cost: u32,

    /// Maximum entries in the in-memory KV read cache. Default: 10_000.
    ///
    /// Each entry holds the raw encoded value (typically ~80 bytes for a
    /// 64-byte payload + 8-byte TTL framing), so 10 000 entries ≈ 800 KB.
    ///
    /// A value of 0 is rejected at open time; use 1 as the effective minimum.
    #[serde(default = "default_kv_cache_capacity")]
    pub kv_cache_capacity: usize,

    /// Maximum number of pending entries in each durable outbound queue
    /// (columnar and timeseries). Default: 100_000.
    ///
    /// When a queue reaches this cap, write operations return
    /// [`LiteError::Backpressure`] until the sync transport drains entries.
    /// This bounds RAM usage to the key/pointer overhead regardless of how
    /// long the device stays offline; the payloads themselves are on disk.
    ///
    /// Can also be set via the `NODEDB_LITE_OUTBOUND_QUEUE_CAP` environment
    /// variable.
    #[serde(default = "default_outbound_queue_cap")]
    pub outbound_queue_cap: usize,

    /// Interval between automatic background flushes, in milliseconds.
    /// Default: 1000 (1 second).
    ///
    /// Opening a database spawns a background task that calls the global
    /// `flush()` every `auto_flush_ms` milliseconds, bounding the data-loss
    /// window uniformly across all engines (KV buffer, vector id-map, CRDT
    /// deltas, CSR graph, spatial, FTS). The task lives as long as the
    /// returned `Arc<NodeDbLite>` and stops when the last handle is dropped.
    ///
    /// **Durability contract**: `await`-ing a write operation (e.g. `kv_put`,
    /// `vector_insert`) returning `Ok` does NOT guarantee on-disk durability.
    /// Durability is bounded by `auto_flush_ms`. Set to 0 to disable the
    /// background task; call `flush()` explicitly to guarantee durability.
    #[serde(default = "default_auto_flush_ms")]
    pub auto_flush_ms: u64,

    /// Interval between automatic background compactions, in milliseconds.
    /// Default: 0 (disabled).
    ///
    /// When non-zero, opening a database spawns a background task that calls
    /// the global `compact()` every `auto_compact_ms` milliseconds, reclaiming
    /// dead pages and truncating the backing file to bound on-disk growth.
    /// Unlike auto-flush this is **opt-in**: compaction is a heavier operation
    /// (it repacks B+ trees and truncates the file, and no-ops while a reader
    /// pins the reclaimable range), so it is not imposed on every embedder by
    /// default.
    ///
    /// Enable it when writing one commit per entry (where the deferred-free
    /// list would otherwise grow unbounded). A much larger interval than
    /// `auto_flush_ms` is appropriate — e.g. minutes, not seconds. Set to 0 to
    /// leave compaction fully manual via `compact()`.
    #[serde(default = "default_auto_compact_ms")]
    pub auto_compact_ms: u64,

    /// What to do when the store being opened cannot be read.
    /// Default: [`CorruptionPolicy::FailClosed`].
    ///
    /// The default refuses to open and leaves the damaged store untouched, so
    /// an embedder that has no Origin to re-sync from does not silently start
    /// against an empty database. Selecting
    /// [`CorruptionPolicy::DiscardStoreAndRecreate`] is a statement that the
    /// data can be rebuilt from somewhere else.
    #[serde(default)]
    pub corruption_policy: CorruptionPolicy,
}

impl Default for LiteConfig {
    fn default() -> Self {
        Self {
            memory_budget: 100 * 1024 * 1024, // 100 MiB
            hnsw_percent: 40,
            csr_percent: 15,
            loro_percent: 15,
            query_percent: 15,
            sync_enabled: true,
            outbound_queue_cap: default_outbound_queue_cap(),
            argon2_m_cost: default_argon2_m_cost(),
            argon2_t_cost: default_argon2_t_cost(),
            argon2_p_cost: default_argon2_p_cost(),
            kv_cache_capacity: default_kv_cache_capacity(),
            auto_flush_ms: default_auto_flush_ms(),
            auto_compact_ms: default_auto_compact_ms(),
            corruption_policy: CorruptionPolicy::FailClosed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let cfg = LiteConfig::default();
        assert_eq!(cfg.memory_budget, 100 * 1024 * 1024);
        assert_eq!(cfg.hnsw_percent, 40);
        assert_eq!(cfg.csr_percent, 15);
        assert_eq!(cfg.loro_percent, 15);
        assert_eq!(cfg.query_percent, 15);
        assert_eq!(cfg.argon2_m_cost, 19_456);
        assert_eq!(cfg.argon2_t_cost, 2);
        assert_eq!(cfg.argon2_p_cost, 1);
        assert_eq!(cfg.auto_flush_ms, 1_000);
        // Auto-compaction is opt-in: disabled by default.
        assert_eq!(cfg.auto_compact_ms, 0);
    }

    #[test]
    fn serde_roundtrip() {
        let cfg = LiteConfig::default();
        let json = sonic_rs::to_string(&cfg).unwrap();
        let parsed: LiteConfig = sonic_rs::from_str(&json).unwrap();
        assert_eq!(parsed, cfg);
    }
}
