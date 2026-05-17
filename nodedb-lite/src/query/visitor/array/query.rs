// SPDX-License-Identifier: Apache-2.0

//! Query lowerings: `ArraySlice`, `ArrayProject`, `ArrayAgg`, `ArrayElementwise`.

use nodedb_array::query::slice::{DimRange, Slice};
use nodedb_array::types::ArrayId;
use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::ArrayOp;
use nodedb_sql::temporal::TemporalScope;
use nodedb_sql::types_array::{ArrayBinaryOpAst, ArrayReducerAst, ArraySliceAst};

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::query::visitor::adapter::LiteFut;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

use super::coerce::{coerce_coord, coord_to_domain_bound, map_binary_op, map_reducer};
use super::schema::{LITE_TENANT, extract_temporal, load_schema};

/// Lower `SqlPlan::ArraySlice` → `ArrayOp::Slice`.
pub(crate) fn lower_array_slice<'a, S: StorageEngine + StorageEngineSync + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
    slice_ast: &ArraySliceAst,
    attr_projection: &[String],
    limit: u32,
    temporal: &TemporalScope,
) -> Result<LiteFut<'a>, LiteError> {
    let schema = load_schema(engine, name)?;

    let mut dim_ranges: Vec<Option<DimRange>> = vec![None; schema.dims.len()];
    for r in &slice_ast.dim_ranges {
        let idx = schema
            .dims
            .iter()
            .position(|d| d.name == r.dim)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("ARRAY_SLICE: array '{name}' has no dim '{}'", r.dim),
            })?;
        let dtype = schema.dims[idx].dtype;
        let lo = coerce_coord(&r.lo, dtype, &r.dim)?;
        let hi = coerce_coord(&r.hi, dtype, &r.dim)?;
        dim_ranges[idx] = Some(DimRange::new(
            coord_to_domain_bound(lo),
            coord_to_domain_bound(hi),
        ));
    }

    let attr_indices = if attr_projection.is_empty() {
        (0..schema.attrs.len() as u32).collect::<Vec<_>>()
    } else {
        attr_projection
            .iter()
            .map(|a| {
                schema
                    .attrs
                    .iter()
                    .position(|spec| spec.name == *a)
                    .ok_or_else(|| LiteError::BadRequest {
                        detail: format!("ARRAY_SLICE: array '{name}' has no attr '{a}'"),
                    })
                    .map(|i| i as u32)
            })
            .collect::<Result<Vec<_>, _>>()?
    };

    let slice = Slice::new(dim_ranges);
    let slice_msgpack = zerompk::to_msgpack_vec(&slice).map_err(|e| LiteError::Serialization {
        detail: format!("encode slice predicate: {e}"),
    })?;
    let (system_as_of, valid_at_ms) = extract_temporal(temporal);
    let aid = ArrayId::new(LITE_TENANT, name);
    let op = ArrayOp::Slice {
        array_id: aid,
        slice_msgpack,
        attr_projection: attr_indices,
        limit,
        cell_filter: None,
        hilbert_range: None,
        system_as_of,
        valid_at_ms,
    };
    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.array(&op)?;
    Ok(Box::pin(fut))
}

/// Lower `SqlPlan::ArrayProject` → `ArrayOp::Project`.
pub(crate) fn lower_array_project<'a, S: StorageEngine + StorageEngineSync + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
    attr_projection: &[String],
) -> Result<LiteFut<'a>, LiteError> {
    let schema = load_schema(engine, name)?;
    let attr_indices: Vec<u32> = if attr_projection.is_empty() {
        (0..schema.attrs.len() as u32).collect()
    } else {
        attr_projection
            .iter()
            .map(|a| {
                schema
                    .attrs
                    .iter()
                    .position(|spec| spec.name == *a)
                    .ok_or_else(|| LiteError::BadRequest {
                        detail: format!("ARRAY_PROJECT: array '{name}' has no attr '{a}'"),
                    })
                    .map(|i| i as u32)
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    if attr_indices.is_empty() {
        return Err(LiteError::BadRequest {
            detail: format!("ARRAY_PROJECT: array '{name}': attr list must not be empty"),
        });
    }
    let aid = ArrayId::new(LITE_TENANT, name);
    let op = ArrayOp::Project {
        array_id: aid,
        attr_indices,
    };
    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.array(&op)?;
    Ok(Box::pin(fut))
}

/// Lower `SqlPlan::ArrayAgg` → `ArrayOp::Aggregate`.
pub(crate) fn lower_array_agg<'a, S: StorageEngine + StorageEngineSync + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
    attr: &str,
    reducer: &ArrayReducerAst,
    group_by_dim: Option<&str>,
    temporal: &TemporalScope,
) -> Result<LiteFut<'a>, LiteError> {
    let schema = load_schema(engine, name)?;
    let attr_idx = schema
        .attrs
        .iter()
        .position(|a| a.name == attr)
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!("ARRAY_AGG: array '{name}' has no attr '{attr}'"),
        })? as u32;

    let group_by_dim_idx: i32 = match group_by_dim {
        None => -1,
        Some(dim) => schema
            .dims
            .iter()
            .position(|d| d.name == dim)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("ARRAY_AGG: array '{name}' has no dim '{dim}'"),
            })? as i32,
    };

    let (system_as_of, valid_at_ms) = extract_temporal(temporal);
    let aid = ArrayId::new(LITE_TENANT, name);
    let op = ArrayOp::Aggregate {
        array_id: aid,
        attr_idx,
        reducer: map_reducer(*reducer),
        group_by_dim: group_by_dim_idx,
        cell_filter: None,
        return_partial: false,
        hilbert_range: None,
        system_as_of,
        valid_at_ms,
    };
    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.array(&op)?;
    Ok(Box::pin(fut))
}

/// Lower `SqlPlan::ArrayElementwise` → `ArrayOp::Elementwise`.
pub(crate) fn lower_array_elementwise<'a, S: StorageEngine + StorageEngineSync + 'a>(
    engine: &'a LiteQueryEngine<S>,
    left: &str,
    right: &str,
    op_ast: ArrayBinaryOpAst,
    attr: &str,
) -> Result<LiteFut<'a>, LiteError> {
    let lschema = load_schema(engine, left)?;
    let rschema = load_schema(engine, right)?;

    if lschema.dims.len() != rschema.dims.len() || lschema.attrs.len() != rschema.attrs.len() {
        return Err(LiteError::BadRequest {
            detail: format!(
                "ARRAY_ELEMENTWISE: arrays '{left}' and '{right}' have different shapes"
            ),
        });
    }
    let attr_idx = lschema
        .attrs
        .iter()
        .position(|a| a.name == attr)
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!("ARRAY_ELEMENTWISE: array '{left}' has no attr '{attr}'"),
        })? as u32;
    if !rschema.attrs.iter().any(|a| a.name == attr) {
        return Err(LiteError::BadRequest {
            detail: format!("ARRAY_ELEMENTWISE: array '{right}' has no attr '{attr}'"),
        });
    }
    let left_id = ArrayId::new(LITE_TENANT, left);
    let right_id = ArrayId::new(LITE_TENANT, right);
    let op = ArrayOp::Elementwise {
        left: left_id,
        right: right_id,
        op: map_binary_op(op_ast),
        attr_idx,
        cell_filter: None,
    };
    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.array(&op)?;
    Ok(Box::pin(fut))
}

#[cfg(test)]
mod tests {
    use nodedb_sql::temporal::TemporalScope;
    use nodedb_sql::types_array::{
        ArrayAttrLiteral, ArrayBinaryOpAst, ArrayCellOrderAst as Cell, ArrayCoordLiteral,
        ArrayInsertRow, ArrayReducerAst, ArraySliceAst, ArrayTileOrderAst as Tile, NamedDimRange,
    };

    use super::super::ddl::lower_create_array;
    use super::super::dml::lower_insert_array;
    use super::super::maintenance::lower_array_flush;
    use super::super::testing::{attr1_ast, dim1_ast, make_engine};
    use super::*;

    #[tokio::test]
    async fn test_insert_and_slice_array() {
        let engine = make_engine();
        lower_create_array(
            &engine,
            "arr_ins",
            &dim1_ast(),
            &attr1_ast(),
            &[4],
            Cell::Hilbert,
            Tile::Hilbert,
            0,
            None,
            None,
        )
        .unwrap()
        .await
        .unwrap();

        let rows = vec![ArrayInsertRow {
            coords: vec![ArrayCoordLiteral::Int64(3)],
            attrs: vec![ArrayAttrLiteral::Int64(99)],
        }];
        let fut = lower_insert_array(&engine, "arr_ins", &rows).expect("lower");
        let r = fut.await.expect("execute");
        assert_eq!(r.rows_affected, 1);

        lower_array_flush(&engine, "arr_ins")
            .unwrap()
            .await
            .unwrap();

        let slice_ast = ArraySliceAst {
            dim_ranges: vec![NamedDimRange {
                dim: "x".to_string(),
                lo: ArrayCoordLiteral::Int64(0),
                hi: ArrayCoordLiteral::Int64(15),
            }],
        };
        let fut = lower_array_slice(
            &engine,
            "arr_ins",
            &slice_ast,
            &[],
            1000,
            &TemporalScope::default(),
        )
        .expect("lower");
        let r = fut.await.expect("execute");
        assert_eq!(r.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_array_agg() {
        let engine = make_engine();
        lower_create_array(
            &engine,
            "arr_agg",
            &dim1_ast(),
            &attr1_ast(),
            &[4],
            Cell::Hilbert,
            Tile::Hilbert,
            0,
            None,
            None,
        )
        .unwrap()
        .await
        .unwrap();

        for i in [1i64, 2, 3] {
            let rows = vec![ArrayInsertRow {
                coords: vec![ArrayCoordLiteral::Int64(i)],
                attrs: vec![ArrayAttrLiteral::Int64(i * 10)],
            }];
            lower_insert_array(&engine, "arr_agg", &rows)
                .unwrap()
                .await
                .unwrap();
        }

        let fut = lower_array_agg(
            &engine,
            "arr_agg",
            "v",
            &ArrayReducerAst::Sum,
            None,
            &TemporalScope::default(),
        )
        .expect("lower");
        let r = fut.await.expect("execute");
        assert!(!r.rows.is_empty());
    }

    #[tokio::test]
    async fn test_array_project() {
        let engine = make_engine();
        lower_create_array(
            &engine,
            "arr_proj",
            &dim1_ast(),
            &attr1_ast(),
            &[4],
            Cell::Hilbert,
            Tile::Hilbert,
            0,
            None,
            None,
        )
        .unwrap()
        .await
        .unwrap();

        let rows = vec![ArrayInsertRow {
            coords: vec![ArrayCoordLiteral::Int64(1)],
            attrs: vec![ArrayAttrLiteral::Int64(42)],
        }];
        lower_insert_array(&engine, "arr_proj", &rows)
            .unwrap()
            .await
            .unwrap();
        lower_array_flush(&engine, "arr_proj")
            .unwrap()
            .await
            .unwrap();

        let fut = lower_array_project(&engine, "arr_proj", &["v".to_string()]).expect("lower");
        let r = fut.await.expect("execute");
        assert!(!r.columns.is_empty());
    }

    #[tokio::test]
    async fn test_array_elementwise() {
        let dims = dim1_ast();
        let attrs = attr1_ast();
        let engine = make_engine();

        for arr in ["el_a", "el_b"] {
            lower_create_array(
                &engine,
                arr,
                &dims,
                &attrs,
                &[4],
                Cell::Hilbert,
                Tile::Hilbert,
                0,
                None,
                None,
            )
            .unwrap()
            .await
            .unwrap();
            let rows = vec![ArrayInsertRow {
                coords: vec![ArrayCoordLiteral::Int64(0)],
                attrs: vec![ArrayAttrLiteral::Int64(5)],
            }];
            lower_insert_array(&engine, arr, &rows)
                .unwrap()
                .await
                .unwrap();
            lower_array_flush(&engine, arr).unwrap().await.unwrap();
        }

        let fut = lower_array_elementwise(&engine, "el_a", "el_b", ArrayBinaryOpAst::Add, "v")
            .expect("lower");
        let r = fut.await.expect("execute");
        assert!(!r.columns.is_empty());
    }
}
