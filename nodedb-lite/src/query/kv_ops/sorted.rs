// SPDX-License-Identifier: Apache-2.0
//! Sorted indexes (leaderboards) over KV collections.
//!
//! - `def.rs`: the definition a `RegisterSortedIndex` plan builds, and the
//!   entry a stored row contributes to it.
//! - `keys.rs`: entry keys and values.
//! - `maintain.rs`: the entries a KV write batch implies.
//! - `query.rs`: rank, top-k, range, count, score.
//! - `register.rs`: register and drop.
//! - `window.rs`: time windows.

mod def;
mod keys;
mod maintain;
mod query;
mod register;
mod window;

#[cfg(test)]
mod tests;

pub use def::RegisterSortedIndex;
pub(crate) use keys::{entries_prefix, f64_to_sort_bytes};
pub(crate) use maintain::plan_sorted_ops;
pub use query::{
    kv_sorted_index_count, kv_sorted_index_range, kv_sorted_index_rank, kv_sorted_index_score,
    kv_sorted_index_top_k,
};
pub use register::{kv_drop_sorted_index, kv_register_sorted_index};
