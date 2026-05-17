// SPDX-License-Identifier: Apache-2.0

//! Coord / attr literal coercion against an array schema, plus reducer and
//! binary-op AST → engine mappers. Used by `dml` (insert/delete) and `query`
//! (slice/agg/elementwise).

use nodedb_array::schema::{ArraySchema, AttrType as EngineAttrType, DimType as EngineDimType};
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::domain::DomainBound;
use nodedb_physical::physical_plan::{ArrayBinaryOp, ArrayReducer};
use nodedb_sql::types_array::{
    ArrayAttrLiteral, ArrayBinaryOpAst, ArrayCoordLiteral, ArrayReducerAst,
};

use crate::error::LiteError;

pub(super) fn coerce_coord(
    lit: &ArrayCoordLiteral,
    dtype: EngineDimType,
    dim_name: &str,
) -> Result<CoordValue, LiteError> {
    match (lit, dtype) {
        (ArrayCoordLiteral::Int64(n), EngineDimType::Int64) => Ok(CoordValue::Int64(*n)),
        (ArrayCoordLiteral::Int64(n), EngineDimType::TimestampMs) => {
            Ok(CoordValue::TimestampMs(*n))
        }
        (ArrayCoordLiteral::Int64(n), EngineDimType::Float64) => Ok(CoordValue::Float64(*n as f64)),
        (ArrayCoordLiteral::Float64(f), EngineDimType::Float64) => Ok(CoordValue::Float64(*f)),
        (ArrayCoordLiteral::String(s), EngineDimType::String) => Ok(CoordValue::String(s.clone())),
        (got, want) => Err(LiteError::BadRequest {
            detail: format!(
                "coord literal for dim '{dim_name}': got {got:?}, expected dim type {want:?}"
            ),
        }),
    }
}

pub(super) fn coerce_coords(
    lits: &[ArrayCoordLiteral],
    schema: &ArraySchema,
) -> Result<Vec<CoordValue>, LiteError> {
    if lits.len() != schema.dims.len() {
        return Err(LiteError::BadRequest {
            detail: format!(
                "coord arity {} does not match dim count {}",
                lits.len(),
                schema.dims.len()
            ),
        });
    }
    lits.iter()
        .zip(schema.dims.iter())
        .map(|(lit, dim)| coerce_coord(lit, dim.dtype, &dim.name))
        .collect()
}

fn coerce_attr(
    lit: &ArrayAttrLiteral,
    spec: &nodedb_array::schema::attr_spec::AttrSpec,
) -> Result<CellValue, LiteError> {
    match (lit, spec.dtype) {
        (ArrayAttrLiteral::Null, _) if spec.nullable => Ok(CellValue::Null),
        (ArrayAttrLiteral::Null, _) => Err(LiteError::BadRequest {
            detail: format!("attr '{}' is NOT NULL", spec.name),
        }),
        (ArrayAttrLiteral::Int64(n), EngineAttrType::Int64) => Ok(CellValue::Int64(*n)),
        (ArrayAttrLiteral::Int64(n), EngineAttrType::Float64) => Ok(CellValue::Float64(*n as f64)),
        (ArrayAttrLiteral::Float64(f), EngineAttrType::Float64) => Ok(CellValue::Float64(*f)),
        (ArrayAttrLiteral::String(s), EngineAttrType::String) => Ok(CellValue::String(s.clone())),
        (ArrayAttrLiteral::Bytes(b), EngineAttrType::Bytes) => Ok(CellValue::Bytes(b.clone())),
        (got, want) => Err(LiteError::BadRequest {
            detail: format!(
                "attr literal for '{}': got {got:?}, expected attr type {want:?}",
                spec.name
            ),
        }),
    }
}

pub(super) fn coerce_attrs(
    lits: &[ArrayAttrLiteral],
    schema: &ArraySchema,
) -> Result<Vec<CellValue>, LiteError> {
    if lits.len() != schema.attrs.len() {
        return Err(LiteError::BadRequest {
            detail: format!(
                "attr arity {} does not match attr count {}",
                lits.len(),
                schema.attrs.len()
            ),
        });
    }
    lits.iter()
        .zip(schema.attrs.iter())
        .map(|(lit, spec)| coerce_attr(lit, spec))
        .collect()
}

pub(super) fn coord_to_domain_bound(cv: CoordValue) -> DomainBound {
    match cv {
        CoordValue::Int64(v) => DomainBound::Int64(v),
        CoordValue::TimestampMs(v) => DomainBound::TimestampMs(v),
        CoordValue::Float64(v) => DomainBound::Float64(v),
        CoordValue::String(v) => DomainBound::String(v),
    }
}

pub(super) fn map_reducer(r: ArrayReducerAst) -> ArrayReducer {
    match r {
        ArrayReducerAst::Sum => ArrayReducer::Sum,
        ArrayReducerAst::Count => ArrayReducer::Count,
        ArrayReducerAst::Min => ArrayReducer::Min,
        ArrayReducerAst::Max => ArrayReducer::Max,
        ArrayReducerAst::Mean => ArrayReducer::Mean,
    }
}

pub(super) fn map_binary_op(o: ArrayBinaryOpAst) -> ArrayBinaryOp {
    match o {
        ArrayBinaryOpAst::Add => ArrayBinaryOp::Add,
        ArrayBinaryOpAst::Sub => ArrayBinaryOp::Sub,
        ArrayBinaryOpAst::Mul => ArrayBinaryOp::Mul,
        ArrayBinaryOpAst::Div => ArrayBinaryOp::Div,
    }
}
