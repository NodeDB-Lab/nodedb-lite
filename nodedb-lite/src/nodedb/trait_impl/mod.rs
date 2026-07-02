// SPDX-License-Identifier: Apache-2.0

//! `NodeDb` trait implementation for `NodeDbLite`, split by concern.
//!
//! - `dispatch`: the single `impl NodeDb for NodeDbLite<S>` block.
//! - `vector` / `graph` / `document` / `sql_lifecycle`: inherent helpers
//!   the dispatch block delegates to, one file per domain.

mod dispatch;
mod document;
mod document_batch;
mod graph;
mod sql_lifecycle;
mod vector;

pub use document_batch::BatchItem;
