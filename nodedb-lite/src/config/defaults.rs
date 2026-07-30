// SPDX-License-Identifier: Apache-2.0

//! Default values for [`LiteConfig`](super::LiteConfig) fields.
//!
//! Each `default_*` function backs both the `Default` impl and the matching
//! `#[serde(default = "...")]` attribute, so a partially-specified config
//! deserializes to the same values a programmatic caller would get.

/// Per-engine budget percentages must leave at least some headroom.
///
/// The four engine percentages must not exceed 99 to preserve at least 1% headroom.
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
