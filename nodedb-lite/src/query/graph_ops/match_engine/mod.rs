// SPDX-License-Identifier: Apache-2.0

//! Graph MATCH pattern engine for Lite.
//!
//! `ast`      — Mirror AST types (wire-compatible with Origin's MatchQuery).
//! `executor` — Pattern executor against the in-memory CSR index.
//! `dispatch` — Entry point invoked from the physical visitor.

pub(super) mod ast;
pub(super) mod dispatch;
pub(super) mod executor;
pub(super) mod predicates;

pub use dispatch::graph_match;
