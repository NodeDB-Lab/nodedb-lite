// SPDX-License-Identifier: Apache-2.0

//! `LiteVisitor` / `PlanVisitor` split by statement family.
//!
//! - `trait_impl`    — `LiteVisitor` struct, `LiteFut` alias, and the
//!   `PlanVisitor` trait impl (one-line delegations only).
//! - `dml`           — insert/upsert/update/delete/insert_select/merge/
//!   update_from/timeseries_ingest.
//! - `reads_scan`    — constant_result/scan/point_get/document_index_lookup/
//!   range_scan/spatial_scan/timeseries_scan/recursive_scan/recursive_value/
//!   lateral_top_k/lateral_loop.
//! - `reads_search`  — vector_search/text_search/multi_vector_search/
//!   sparse_search/hybrid_search/hybrid_search_triple.
//! - `reads_combine` — join/aggregate/union/intersect/except/cte/subquery.
//! - `ddl`           — create_array/drop_array/alter_array.
//! - `admin`         — truncate/create_index/drop_index.
//! - `array`         — insert_array/delete_array/array_slice/array_project/
//!   array_agg/array_elementwise/array_flush/array_compact.
//! - `vector`        — vector_primary_insert/vector_primary_delete/
//!   vector_primary_update/vector_primary_truncate.
//! - `kv`            — kv_insert.

mod admin;
mod array;
mod ddl;
mod dml;
mod kv;
mod reads_combine;
mod reads_scan;
mod reads_search;
mod trait_impl;
mod vector;

pub(crate) use trait_impl::{LiteFut, LiteVisitor};
