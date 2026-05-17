// SPDX-License-Identifier: Apache-2.0

//! DDL lowerings: `CreateArray`, `DropArray`, `AlterArray`.

use nodedb_array::types::ArrayId;
use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::ArrayOp;
use nodedb_sql::types_array::{ArrayAttrAst, ArrayCellOrderAst, ArrayDimAst, ArrayTileOrderAst};
use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::meta_ops;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::query::visitor::adapter::LiteFut;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

use super::schema::{LITE_TENANT, build_schema};

/// Lower `SqlPlan::CreateArray` → `ArrayOp::OpenArray`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn lower_create_array<'a, S: StorageEngine + StorageEngineSync + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
    dims: &[ArrayDimAst],
    attrs: &[ArrayAttrAst],
    tile_extents: &[i64],
    cell_order: ArrayCellOrderAst,
    tile_order: ArrayTileOrderAst,
    prefix_bits: u8,
    _audit_retain_ms: Option<u64>,
    _minimum_audit_retain_ms: Option<u64>,
) -> Result<LiteFut<'a>, LiteError> {
    let schema = build_schema(name, dims, attrs, tile_extents, cell_order, tile_order)?;
    let schema_bytes = zerompk::to_msgpack_vec(&schema).map_err(|e| LiteError::Serialization {
        detail: format!("encode array schema: {e}"),
    })?;
    let schema_hash = crate::engine::array::catalog::hash_schema(&schema)?;
    let aid = ArrayId::new(LITE_TENANT, name);
    let op = ArrayOp::OpenArray {
        array_id: aid,
        schema_msgpack: schema_bytes,
        schema_hash,
        prefix_bits,
    };
    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.array(&op)?;
    Ok(Box::pin(fut))
}

/// Lower `SqlPlan::DropArray` → `ArrayOp::DropArray`.
pub(crate) fn lower_drop_array<'a, S: StorageEngine + StorageEngineSync + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
    if_exists: bool,
) -> Result<LiteFut<'a>, LiteError> {
    {
        let state = engine
            .array_state
            .lock()
            .map_err(|_| LiteError::LockPoisoned)?;
        if !state.arrays.contains_key(name) {
            if if_exists {
                return Ok(Box::pin(async move {
                    Ok(QueryResult {
                        columns: vec![],
                        rows: vec![],
                        rows_affected: 0,
                    })
                }));
            }
            return Err(LiteError::BadRequest {
                detail: format!("DROP ARRAY: array '{name}' not found"),
            });
        }
    }
    let aid = ArrayId::new(LITE_TENANT, name);
    let op = ArrayOp::DropArray { array_id: aid };
    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.array(&op)?;
    Ok(Box::pin(fut))
}

/// Lower `SqlPlan::AlterArray` → `meta_ops::handle_alter_array`.
pub(crate) fn lower_alter_array<'a, S: StorageEngine + StorageEngineSync + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
    audit_retain_ms: Option<Option<i64>>,
    minimum_audit_retain_ms: Option<u64>,
) -> Result<LiteFut<'a>, LiteError> {
    let name = name.to_string();
    let min_wrap: Option<Option<u64>> = minimum_audit_retain_ms.map(Some);
    Ok(Box::pin(async move {
        meta_ops::handle_alter_array(engine, &name, audit_retain_ms, min_wrap).await
    }))
}

#[cfg(test)]
mod tests {
    use nodedb_sql::types_array::ArrayCellOrderAst as Cell;
    use nodedb_sql::types_array::ArrayTileOrderAst as Tile;

    use super::super::testing::{attr1_ast, dim1_ast, make_engine};
    use super::*;

    #[tokio::test]
    async fn test_create_array() {
        let engine = make_engine();
        let fut = lower_create_array(
            &engine,
            "arr1",
            &dim1_ast(),
            &attr1_ast(),
            &[4],
            Cell::Hilbert,
            Tile::Hilbert,
            0,
            None,
            None,
        )
        .expect("lower");
        let r = fut.await.expect("execute");
        assert_eq!(r.rows_affected, 1);
    }

    #[tokio::test]
    async fn test_drop_array() {
        let engine = make_engine();
        lower_create_array(
            &engine,
            "arr_drop",
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

        let fut = lower_drop_array(&engine, "arr_drop", false).expect("lower");
        let r = fut.await.expect("execute");
        assert_eq!(r.rows_affected, 1);
    }

    #[tokio::test]
    async fn test_drop_array_if_exists_missing() {
        let engine = make_engine();
        let fut = lower_drop_array(&engine, "nonexistent", true).expect("lower");
        let r = fut.await.expect("execute");
        assert_eq!(r.rows_affected, 0);
    }

    #[tokio::test]
    async fn test_alter_array() {
        let engine = make_engine();
        lower_create_array(
            &engine,
            "arr_alt",
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

        let fut = lower_alter_array(&engine, "arr_alt", Some(Some(86400000)), None).expect("lower");
        let r = fut.await.expect("execute");
        assert_eq!(r.rows_affected, 1);
    }
}
