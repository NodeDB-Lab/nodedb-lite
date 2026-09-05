// SPDX-License-Identifier: Apache-2.0

//! Default values for [`LiteConfig`](super::LiteConfig) fields.
//!
//! Each `default_*` function backs both the `Default` impl and the matching
//! `#[serde(default = "...")]` attribute, so a partially-specified config
//! deserializes to the same values a programmatic caller would get.

/// Per-engine budget percentages must leave at least some headroom.
///
/// The thirteen engine percentages must not exceed 99 to preserve at least 1% headroom.
pub(crate) const MAX_TOTAL_ENGINE_PERCENT: usize = 99;

pub(crate) fn default_outbound_queue_cap() -> usize {
    100_000
}

pub(crate) fn default_kv_cache_capacity() -> usize {
    10_000
}

pub(crate) fn default_auto_flush_ms() -> u64 {
    1_000
}

pub(crate) fn default_auto_compact_ms() -> u64 {
    0
}

pub(crate) fn default_sync_enabled() -> bool {
    true
}

pub(crate) fn default_argon2_m_cost() -> u32 {
    19_456
}

pub(crate) fn default_argon2_t_cost() -> u32 {
    2
}

pub(crate) fn default_argon2_p_cost() -> u32 {
    1
}

/// Percentage of `memory_budget` reserved for the key-value engine.
pub(crate) fn default_kv_percent() -> usize {
    2
}

/// Percentage of `memory_budget` reserved for the schemaless document engine.
pub(crate) fn default_document_percent() -> usize {
    2
}

/// Percentage of `memory_budget` reserved for the strict document engine.
pub(crate) fn default_strict_percent() -> usize {
    2
}

/// Percentage of `memory_budget` reserved for the columnar engine.
pub(crate) fn default_columnar_percent() -> usize {
    2
}

/// Percentage of `memory_budget` reserved for the timeseries engine.
pub(crate) fn default_timeseries_percent() -> usize {
    1
}

/// Percentage of `memory_budget` reserved for the spatial engine.
pub(crate) fn default_spatial_percent() -> usize {
    1
}

/// Percentage of `memory_budget` reserved for the full-text search engine.
pub(crate) fn default_fts_percent() -> usize {
    1
}

/// Percentage of `memory_budget` reserved for the array engine.
pub(crate) fn default_array_percent() -> usize {
    1
}

/// Percentage of `memory_budget` reserved for the sparse-vector metadata engine.
pub(crate) fn default_sparse_percent() -> usize {
    1
}
