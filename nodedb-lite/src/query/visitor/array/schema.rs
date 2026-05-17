// SPDX-License-Identifier: Apache-2.0

//! Type mappers (SQL AST → array-engine types), schema builder, and array-state
//! catalog lookups. The `LITE_TENANT` constant lives here because every array
//! op in Lite runs against the single-tenant ID space.

use nodedb_array::schema::{
    ArraySchema, ArraySchemaBuilder, AttrSpec, AttrType as EngineAttrType, CellOrder, DimSpec,
    DimType as EngineDimType, TileOrder,
};
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_sql::temporal::TemporalScope;
use nodedb_sql::types_array::{
    ArrayAttrAst, ArrayAttrType, ArrayCellOrderAst, ArrayDimAst, ArrayDimType, ArrayDomainBound,
    ArrayTileOrderAst,
};
use nodedb_types::TenantId;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

/// Lite is single-tenant; all `ArrayId` allocations use tenant 0.
pub(super) const LITE_TENANT: TenantId = TenantId::new(0);

pub(super) fn map_dim_type(t: ArrayDimType) -> EngineDimType {
    match t {
        ArrayDimType::Int64 => EngineDimType::Int64,
        ArrayDimType::Float64 => EngineDimType::Float64,
        ArrayDimType::TimestampMs => EngineDimType::TimestampMs,
        ArrayDimType::String => EngineDimType::String,
    }
}

pub(super) fn map_attr_type(t: ArrayAttrType) -> EngineAttrType {
    match t {
        ArrayAttrType::Int64 => EngineAttrType::Int64,
        ArrayAttrType::Float64 => EngineAttrType::Float64,
        ArrayAttrType::String => EngineAttrType::String,
        ArrayAttrType::Bytes => EngineAttrType::Bytes,
    }
}

fn map_cell_order(o: ArrayCellOrderAst) -> CellOrder {
    match o {
        ArrayCellOrderAst::RowMajor => CellOrder::RowMajor,
        ArrayCellOrderAst::ColMajor => CellOrder::ColMajor,
        ArrayCellOrderAst::Hilbert => CellOrder::Hilbert,
        ArrayCellOrderAst::ZOrder => CellOrder::ZOrder,
    }
}

fn map_tile_order(o: ArrayTileOrderAst) -> TileOrder {
    match o {
        ArrayTileOrderAst::RowMajor => TileOrder::RowMajor,
        ArrayTileOrderAst::ColMajor => TileOrder::ColMajor,
        ArrayTileOrderAst::Hilbert => TileOrder::Hilbert,
        ArrayTileOrderAst::ZOrder => TileOrder::ZOrder,
    }
}

fn bound_to_engine(b: &ArrayDomainBound) -> DomainBound {
    match b {
        ArrayDomainBound::Int64(v) => DomainBound::Int64(*v),
        ArrayDomainBound::Float64(v) => DomainBound::Float64(*v),
        ArrayDomainBound::TimestampMs(v) => DomainBound::TimestampMs(*v),
        ArrayDomainBound::String(v) => DomainBound::String(v.clone()),
    }
}

pub(super) fn build_schema(
    name: &str,
    dims: &[ArrayDimAst],
    attrs: &[ArrayAttrAst],
    tile_extents: &[i64],
    cell_order: ArrayCellOrderAst,
    tile_order: ArrayTileOrderAst,
) -> Result<ArraySchema, LiteError> {
    let mut builder = ArraySchemaBuilder::new(name);
    for d in dims {
        let dtype = map_dim_type(d.dtype);
        let lo = bound_to_engine(&d.lo);
        let hi = bound_to_engine(&d.hi);
        builder = builder.dim(DimSpec::new(d.name.clone(), dtype, Domain::new(lo, hi)));
    }
    for a in attrs {
        let dtype = map_attr_type(a.dtype);
        builder = builder.attr(AttrSpec::new(a.name.clone(), dtype, a.nullable));
    }
    let extents: Vec<u64> = tile_extents.iter().map(|n| *n as u64).collect();
    builder = builder
        .tile_extents(extents)
        .cell_order(map_cell_order(cell_order))
        .tile_order(map_tile_order(tile_order));
    builder.build().map_err(|e| LiteError::BadRequest {
        detail: format!("CREATE ARRAY {name}: {e}"),
    })
}

pub(super) fn extract_temporal(scope: &TemporalScope) -> (Option<i64>, Option<i64>) {
    use nodedb_sql::temporal::ValidTime;
    let sys = scope.system_as_of_ms;
    let valid = match &scope.valid_time {
        ValidTime::At(ms) => Some(*ms),
        _ => None,
    };
    (sys, valid)
}

/// Read the schema for `name` from the locked array state.
pub(super) fn load_schema<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    name: &str,
) -> Result<ArraySchema, LiteError> {
    let state = engine
        .array_state
        .lock()
        .map_err(|_| LiteError::LockPoisoned)?;
    state
        .arrays
        .get(name)
        .map(|s| s.schema.clone())
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!("array '{name}' not found"),
        })
}

/// Read `audit_retain_ms` for `name` from the locked array state.
pub(super) fn load_audit_retain<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    name: &str,
) -> Result<Option<i64>, LiteError> {
    let state = engine
        .array_state
        .lock()
        .map_err(|_| LiteError::LockPoisoned)?;
    state
        .arrays
        .get(name)
        .map(|s| s.audit_retain_ms)
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!("array '{name}' not found"),
        })
}
