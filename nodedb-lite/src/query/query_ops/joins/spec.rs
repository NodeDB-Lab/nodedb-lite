// SPDX-License-Identifier: Apache-2.0
//! Parameter structs shared by the hash, broadcast, and inline-hash join executors.

use nodedb_physical::physical_plan::query::JoinProjection;

/// A join input read from a named collection.
pub struct CollectionSide<'a> {
    pub collection: &'a str,
    pub alias: Option<&'a str>,
}

/// Join behavior shared by every join strategy.
pub struct JoinSpec<'a> {
    /// `(left_key, right_key)` equality pairs.
    pub on: &'a [(String, String)],
    pub join_type: &'a str,
    /// Row cap on the join output. `0` means unlimited.
    pub limit: usize,
    pub projection: &'a [JoinProjection],
    /// Msgpack-encoded post-join filters. Empty means no filter.
    pub post_filters: &'a [u8],
}

impl JoinSpec<'_> {
    /// Left and right key columns split out of `on`.
    pub(super) fn split_keys(&self) -> (Vec<String>, Vec<String>) {
        let left = self.on.iter().map(|(l, _)| l.clone()).collect();
        let right = self.on.iter().map(|(_, r)| r.clone()).collect();
        (left, right)
    }

    /// `limit` with `0` mapped to `usize::MAX`.
    pub(super) fn effective_limit(&self) -> usize {
        if self.limit == 0 {
            usize::MAX
        } else {
            self.limit
        }
    }
}

/// Aggregation applied to the joined rows.
pub struct PostAggregate<'a> {
    pub group_by: &'a [String],
    /// `(function, field)` pairs.
    pub aggregates: &'a [(String, String)],
}
