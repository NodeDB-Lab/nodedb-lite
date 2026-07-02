// SPDX-License-Identifier: Apache-2.0

//! Maintenance lowerings: `ArrayFlush`, `ArrayCompact`.

use nodedb_array::types::ArrayId;
use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::ArrayOp;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::query::visitor::adapter::LiteFut;
use crate::storage::engine::StorageEngine;

use super::schema::{LITE_TENANT, load_audit_retain};

/// Lower `SqlPlan::ArrayFlush` → `ArrayOp::Flush`.
pub(crate) fn lower_array_flush<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
) -> Result<LiteFut<'a>, LiteError> {
    let name_owned = name.to_string();
    Ok(Box::pin(async move {
        if !engine
            .array_state
            .lock()
            .await
            .arrays
            .contains_key(&name_owned)
        {
            return Err(LiteError::BadRequest {
                detail: format!("ARRAY_FLUSH: array '{name_owned}' not found"),
            });
        }
        let aid = ArrayId::new(LITE_TENANT, &name_owned);
        let op = ArrayOp::Flush {
            array_id: aid,
            wal_lsn: 0,
        };
        let mut phys = LiteDataPlaneVisitor { engine };
        phys.array(&op)?.await
    }))
}

/// Lower `SqlPlan::ArrayCompact` → `ArrayOp::Compact`.
pub(crate) fn lower_array_compact<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
) -> Result<LiteFut<'a>, LiteError> {
    let audit_retain_ms = load_audit_retain(engine, name)?;
    let aid = ArrayId::new(LITE_TENANT, name);
    let op = ArrayOp::Compact {
        array_id: aid,
        audit_retain_ms,
    };
    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.array(&op)?;
    Ok(Box::pin(fut))
}

#[cfg(test)]
mod tests {
    use nodedb_sql::types_array::{ArrayCellOrderAst as Cell, ArrayTileOrderAst as Tile};

    use super::super::ddl::lower_create_array;
    use super::super::testing::{attr1_ast, dim1_ast, make_engine};
    use super::*;

    #[tokio::test]
    async fn test_array_flush() {
        let engine = make_engine().await;
        lower_create_array(
            &engine,
            "arr_fl",
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

        let fut = lower_array_flush(&engine, "arr_fl").expect("lower");
        let r = fut.await.expect("execute");
        assert_eq!(r.rows_affected, 0);
    }

    #[tokio::test]
    async fn test_array_compact() {
        let engine = make_engine().await;
        lower_create_array(
            &engine,
            "arr_cmp",
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

        let fut = lower_array_compact(&engine, "arr_cmp").expect("lower");
        let _r = fut.await.expect("execute");
    }
}
