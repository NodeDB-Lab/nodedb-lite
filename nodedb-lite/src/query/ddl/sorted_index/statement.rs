// SPDX-License-Identifier: Apache-2.0

//! Parsed sorted-index statements.

/// Default window timestamp column when a `WINDOW` clause names none.
pub(super) const DEFAULT_WINDOW_COLUMN: &str = "updated_at";

/// The `WINDOW` clause of `CREATE SORTED INDEX`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::query) struct WindowClause {
    /// `none`, `daily`, `weekly`, `monthly` or `custom`.
    pub kind: &'static str,
    /// Empty when `kind` is `none`.
    pub timestamp_column: String,
    /// Start of a custom window, ms since epoch.
    pub start_ms: u64,
    /// End (exclusive) of a custom window, ms since epoch.
    pub end_ms: u64,
}

impl WindowClause {
    pub(super) fn none() -> Self {
        Self {
            kind: "none",
            timestamp_column: String::new(),
            start_ms: 0,
            end_ms: 0,
        }
    }
}

/// `CREATE SORTED INDEX name ON collection (col [ASC|DESC], ...) KEY col [WINDOW ...]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::query) struct CreateSortedIndex {
    pub name: String,
    pub collection: String,
    /// `(column, "ASC" | "DESC")` pairs. The first is the score column.
    pub sort_columns: Vec<(String, String)>,
    pub key_column: String,
    pub window: WindowClause,
}

/// One sorted-index statement.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::query) enum SortedIndexStatement {
    Create(CreateSortedIndex),
    /// `DROP SORTED INDEX name`.
    Drop {
        name: String,
    },
    /// `SELECT RANK(index, key)`.
    Rank {
        index: String,
        key: Vec<u8>,
    },
    /// `SELECT TOPK(index, k)` or `SELECT * FROM TOPK(index, k)`.
    TopK {
        index: String,
        k: u32,
    },
    /// `SELECT * FROM RANGE(index, min, max)`. `None` is an open bound.
    Range {
        index: String,
        min: Option<f64>,
        max: Option<f64>,
    },
    /// `SELECT SORTED_COUNT(index)`.
    Count {
        index: String,
    },
}
