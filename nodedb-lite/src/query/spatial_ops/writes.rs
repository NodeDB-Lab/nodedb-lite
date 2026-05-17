// SPDX-License-Identifier: Apache-2.0
//! Spatial write operations: Insert and Delete.

use nodedb_types::Surrogate;
use nodedb_types::geometry::Geometry;
use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

/// `SpatialOp::Insert` — index a geometry for a document surrogate.
///
/// The surrogate is used as the stable doc ID string, matching the
/// hex-encoded key that Origin uses so that cross-engine prefilter
/// bitmap intersects work without translation.
pub fn spatial_insert<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    field: &str,
    surrogate: Surrogate,
    geometry: &Geometry,
) -> Result<QueryResult, LiteError> {
    let doc_id = surrogate.0.to_string();
    engine
        .spatial
        .lock()
        .map_err(|_| LiteError::LockPoisoned)?
        .index_document(collection, field, &doc_id, geometry);
    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: 1,
    })
}

/// `SpatialOp::Delete` — remove a document's geometry from the R-tree index.
pub fn spatial_delete<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    field: &str,
    surrogate: Surrogate,
) -> Result<QueryResult, LiteError> {
    let doc_id = surrogate.0.to_string();
    engine
        .spatial
        .lock()
        .map_err(|_| LiteError::LockPoisoned)?
        .remove_document(collection, field, &doc_id);
    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: 1,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use nodedb_types::BoundingBox;
    use nodedb_types::Surrogate;
    use nodedb_types::geometry::Geometry;

    use crate::engine::spatial::SpatialIndexManager;

    fn make_mgr() -> Arc<Mutex<SpatialIndexManager>> {
        Arc::new(Mutex::new(SpatialIndexManager::new()))
    }

    #[test]
    fn insert_indexes_geometry() {
        let mgr = make_mgr();
        let doc_id = Surrogate(42).0.to_string();
        mgr.lock()
            .unwrap()
            .index_document("col", "geom", &doc_id, &Geometry::point(10.0, 20.0));
        let guard = mgr.lock().unwrap();
        let hits = guard.search("col", "geom", &BoundingBox::new(9.0, 19.0, 11.0, 21.0));
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn delete_removes_geometry() {
        let mgr = make_mgr();
        let doc_id = Surrogate(7).0.to_string();
        mgr.lock()
            .unwrap()
            .index_document("col", "geom", &doc_id, &Geometry::point(10.0, 20.0));
        mgr.lock().unwrap().remove_document("col", "geom", &doc_id);
        let guard = mgr.lock().unwrap();
        let hits = guard.search("col", "geom", &BoundingBox::new(0.0, 0.0, 180.0, 90.0));
        assert!(hits.is_empty());
    }
}
