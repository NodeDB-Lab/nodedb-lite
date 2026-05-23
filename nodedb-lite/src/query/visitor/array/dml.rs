// SPDX-License-Identifier: Apache-2.0

//! DML lowerings: `InsertArray`, `DeleteArray`.

use nodedb_array::types::ArrayId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::ArrayOp;
use nodedb_sql::types_array::{ArrayCoordLiteral, ArrayInsertRow};

use crate::engine::array::ops::util::time::now_ms;
use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::query::visitor::adapter::LiteFut;
use crate::storage::engine::StorageEngine;

use super::coerce::{coerce_attrs, coerce_coords};
use super::schema::{LITE_TENANT, load_schema};

type PutCellTuple = (
    Vec<CoordValue>,
    Vec<CellValue>,
    nodedb_types::Surrogate,
    i64,
    i64,
    i64,
);

/// Lower `SqlPlan::InsertArray` → `ArrayOp::Put`.
///
/// Coerces coord/attr literals against the stored schema and encodes them as
/// `Vec<PutCellWire>` (same msgpack layout the physical visitor decodes).
pub(crate) fn lower_insert_array<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
    rows: &[ArrayInsertRow],
) -> Result<LiteFut<'a>, LiteError> {
    let schema = load_schema(engine, name)?;
    let now = now_ms();

    // `PutCellWire` in adapter/array.rs decodes a positional tuple of
    // (coord, attrs, surrogate, system_from_ms, valid_from_ms, valid_until_ms).
    let mut cells: Vec<PutCellTuple> = Vec::with_capacity(rows.len());

    for row in rows {
        let coord = coerce_coords(&row.coords, &schema)?;
        let attrs = coerce_attrs(&row.attrs, &schema)?;
        cells.push((
            coord,
            attrs,
            nodedb_types::Surrogate::ZERO,
            now,
            now,
            i64::MAX,
        ));
    }

    let cells_msgpack = zerompk::to_msgpack_vec(&cells).map_err(|e| LiteError::Serialization {
        detail: format!("encode InsertArray cells: {e}"),
    })?;
    let aid = ArrayId::new(LITE_TENANT, name);
    let op = ArrayOp::Put {
        array_id: aid,
        cells_msgpack,
        wal_lsn: 0,
    };
    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.array(&op)?;
    Ok(Box::pin(fut))
}

/// Lower `SqlPlan::DeleteArray` → `ArrayOp::Delete`.
///
/// The Lite physical visitor for `ArrayOp::Delete` decodes `Vec<Vec<CoordValue>>`.
pub(crate) fn lower_delete_array<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
    coords: &[Vec<ArrayCoordLiteral>],
) -> Result<LiteFut<'a>, LiteError> {
    let schema = load_schema(engine, name)?;
    let mut typed_coords: Vec<Vec<CoordValue>> = Vec::with_capacity(coords.len());
    for row in coords {
        typed_coords.push(coerce_coords(row, &schema)?);
    }
    let coords_msgpack =
        zerompk::to_msgpack_vec(&typed_coords).map_err(|e| LiteError::Serialization {
            detail: format!("encode DeleteArray coords: {e}"),
        })?;
    let aid = ArrayId::new(LITE_TENANT, name);
    let op = ArrayOp::Delete {
        array_id: aid,
        coords_msgpack,
        wal_lsn: 0,
    };
    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.array(&op)?;
    Ok(Box::pin(fut))
}

#[cfg(test)]
mod tests {
    use nodedb_sql::types_array::{
        ArrayAttrLiteral, ArrayCellOrderAst as Cell, ArrayCoordLiteral, ArrayInsertRow,
        ArrayTileOrderAst as Tile,
    };

    use super::super::ddl::lower_create_array;
    use super::super::testing::{attr1_ast, dim1_ast, make_engine};
    use super::*;

    #[tokio::test]
    async fn test_delete_array() {
        let engine = make_engine().await;
        lower_create_array(
            &engine,
            "arr_del",
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
            coords: vec![ArrayCoordLiteral::Int64(5)],
            attrs: vec![ArrayAttrLiteral::Int64(7)],
        }];
        lower_insert_array(&engine, "arr_del", &rows)
            .unwrap()
            .await
            .unwrap();

        let coords = vec![vec![ArrayCoordLiteral::Int64(5)]];
        let fut = lower_delete_array(&engine, "arr_del", &coords).expect("lower");
        let r = fut.await.expect("execute");
        assert_eq!(r.rows_affected, 1);
    }
}
