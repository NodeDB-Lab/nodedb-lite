// SPDX-License-Identifier: Apache-2.0

//! SQL-visitor lowering for Array `SqlPlan` variants.
//!
//! - `schema`     — type mappers (AST → engine types), schema builder, catalog
//!   lookups (`load_schema`, `load_audit_retain`), `LITE_TENANT` constant.
//! - `coerce`     — coord / attr literal coercion + reducer / binary-op mappers.
//! - `ddl`        — `CreateArray`, `DropArray`, `AlterArray`.
//! - `dml`        — `InsertArray`, `DeleteArray`.
//! - `query`      — `ArraySlice`, `ArrayProject`, `ArrayAgg`, `ArrayElementwise`.
//! - `maintenance`— `ArrayFlush`, `ArrayCompact`.

mod coerce;
mod ddl;
mod dml;
mod maintenance;
mod query;
mod schema;

#[cfg(test)]
mod testing;

pub(super) use ddl::{lower_alter_array, lower_create_array, lower_drop_array};
pub(super) use dml::{lower_delete_array, lower_insert_array};
pub(super) use maintenance::{lower_array_compact, lower_array_flush};
pub(super) use query::{
    lower_array_agg, lower_array_elementwise, lower_array_project, lower_array_slice,
};
