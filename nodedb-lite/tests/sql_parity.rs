//! §10 — SQL parity test suite.
//!
//! Verifies that the supported Lite 0.1.0 SQL subset returns results that
//! match (or document known divergences from) Origin for every CRUD lifecycle,
//! and that unsupported features return a typed Unsupported error rather than
//! silently succeeding or panicking.
//!
//! Run with:
//!   cargo nextest run -p nodedb-lite --test sql_parity

mod common;

#[path = "sql_parity/document.rs"]
mod document;

#[path = "sql_parity/strict.rs"]
mod strict;

#[path = "sql_parity/columnar.rs"]
mod columnar;

#[path = "sql_parity/timeseries.rs"]
mod timeseries;

#[path = "sql_parity/negative.rs"]
mod negative;
