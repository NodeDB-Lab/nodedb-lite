// SPDX-License-Identifier: Apache-2.0

//! `PlanVisitor` impl for Lite — split by concern across submodules.
//!
//! - `visitor`        — `LiteVisitor` struct and the `PlanVisitor` trait impl
//!   (all method bodies delegate; adding a new SqlPlan variant is a hard
//!   compile error here).
//! - `basic`          — direct engine CRUD lowerings (scan, point_get, insert,
//!   upsert, update, delete, truncate, constant_result, create_index,
//!   drop_index).
//! - `vector_search`  — `vector_search` lowering + array prefilter resolution.
//! - `text_search`    — `text_search` lowering (FtsQuery → TextOp dispatch).

mod basic;
mod text_search;
mod vector_search;
mod visitor;

pub(crate) use visitor::{LiteFut, LiteVisitor};
