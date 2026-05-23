// SPDX-License-Identifier: Apache-2.0
//! Spatial read operations: Scan with OGC predicate refinement.

use nodedb_physical::physical_plan::SpatialPredicate;
use nodedb_query::scan_filter::ScanFilter;
use nodedb_spatial::predicates::{st_contains, st_dwithin, st_intersects, st_within};
use nodedb_types::geometry::Geometry;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;
use nodedb_types::{BoundingBox, Surrogate, SurrogateBitmap, geometry_bbox};

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

/// Parameters for `SpatialOp::Scan`.
pub struct ScanParams {
    pub collection: String,
    pub field: String,
    pub predicate: SpatialPredicate,
    pub query_geometry: Geometry,
    pub distance_meters: f64,
    pub attribute_filters: Vec<u8>,
    pub limit: usize,
    pub projection: Vec<String>,
    pub rls_filters: Vec<u8>,
    pub prefilter: Option<SurrogateBitmap>,
}

/// Execute a spatial scan: R-tree candidate generation → prefilter → OGC
/// refinement → attribute filters → RLS → projection + limit.
pub fn spatial_scan<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    params: ScanParams,
) -> Result<QueryResult, LiteError> {
    let ScanParams {
        collection,
        field,
        predicate,
        query_geometry,
        distance_meters,
        attribute_filters,
        limit,
        projection,
        rls_filters,
        prefilter,
    } = params;

    // Expand bbox for DWithin; use exact bbox for all other predicates.
    let bbox: BoundingBox = match predicate {
        SpatialPredicate::DWithin => geometry_bbox(&query_geometry).expand_meters(distance_meters),
        SpatialPredicate::Contains | SpatialPredicate::Intersects | SpatialPredicate::Within => {
            geometry_bbox(&query_geometry)
        }
    };

    // Decode attribute filters (empty bytes → no filters).
    let attr_filters: Vec<ScanFilter> = if attribute_filters.is_empty() {
        Vec::new()
    } else {
        zerompk::from_msgpack(&attribute_filters).map_err(|e| LiteError::Serialization {
            detail: format!("decode attribute_filters: {e}"),
        })?
    };

    // Decode RLS filters.
    let rls: Vec<ScanFilter> = if rls_filters.is_empty() {
        Vec::new()
    } else {
        zerompk::from_msgpack(&rls_filters).map_err(|e| LiteError::Serialization {
            detail: format!("decode rls_filters: {e}"),
        })?
    };

    // Candidate entry IDs from R-tree range search.
    let spatial_guard = engine.spatial.lock().map_err(|_| LiteError::LockPoisoned)?;
    let candidate_entries: Vec<u64> = spatial_guard
        .search(&collection, &field, &bbox)
        .into_iter()
        .map(|e| e.id)
        .collect();

    // Resolve each entry_id to doc_id while holding the lock.
    let candidates: Vec<(String, u64)> = candidate_entries
        .into_iter()
        .filter_map(|eid| {
            spatial_guard
                .doc_id_for_entry(eid)
                .map(|doc_id| (doc_id.to_string(), eid))
        })
        .collect();
    drop(spatial_guard);

    let crdt_guard = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;

    let mut rows: Vec<Vec<Value>> = Vec::new();

    for (doc_id, _eid) in candidates {
        // Parse surrogate from doc_id string.
        // Non-numeric doc_id — surrogate was not assigned via SpatialOp::Insert.
        // Fall back to treating the doc_id as surrogate 0 for prefilter purposes.
        let surrogate_val: u32 = doc_id.parse::<u32>().unwrap_or_default();
        let surrogate = Surrogate(surrogate_val);

        // Apply cross-engine prefilter bitmap.
        if let Some(ref pf) = prefilter
            && !pf.contains(surrogate)
        {
            continue;
        }

        // Fetch the document from the CRDT store for geometry refinement and
        // attribute filtering.
        let loro_val = match crdt_guard.read(&collection, &doc_id) {
            Some(v) => v,
            None => continue,
        };
        let doc = crate::nodedb::convert::loro_value_to_document(&doc_id, &loro_val);

        // OGC predicate refinement: extract candidate geometry from the document.
        let candidate_geom: Geometry = match doc.fields.get(&field) {
            Some(Value::String(s)) => match sonic_rs::from_str::<Geometry>(s) {
                Ok(g) => g,
                Err(_) => continue,
            },
            Some(Value::Geometry(g)) => g.clone(),
            _ => continue,
        };

        let passes = match predicate {
            SpatialPredicate::DWithin => {
                st_dwithin(&candidate_geom, &query_geometry, distance_meters)
            }
            SpatialPredicate::Contains => st_contains(&query_geometry, &candidate_geom),
            SpatialPredicate::Intersects => st_intersects(&candidate_geom, &query_geometry),
            SpatialPredicate::Within => st_within(&candidate_geom, &query_geometry),
        };
        if !passes {
            continue;
        }

        // Build a Value::Object from the document fields for filter evaluation.
        let doc_value = Value::Object(doc.fields.clone());

        // Apply attribute filters.
        if !attr_filters.iter().all(|f| f.matches_value(&doc_value)) {
            continue;
        }

        // Apply RLS filters.
        if !rls.iter().all(|f| f.matches_value(&doc_value)) {
            continue;
        }

        // Build result row applying projection.
        let row = if projection.is_empty() {
            // Return id + full document.
            let msgpack =
                zerompk::to_msgpack_vec(&doc_value).map_err(|e| LiteError::Serialization {
                    detail: format!("serialize spatial doc: {e}"),
                })?;
            vec![Value::String(doc_id), Value::Bytes(msgpack)]
        } else {
            projection
                .iter()
                .map(|col| {
                    if col == "id" || col == "_id" {
                        Value::String(doc_id.clone())
                    } else {
                        doc.fields.get(col).cloned().unwrap_or(Value::Null)
                    }
                })
                .collect()
        };
        rows.push(row);

        if rows.len() >= limit && limit > 0 {
            break;
        }
    }
    drop(crdt_guard);

    let columns = if projection.is_empty() {
        vec!["id".to_string(), "data".to_string()]
    } else {
        projection
    };

    Ok(QueryResult {
        columns,
        rows,
        rows_affected: 0,
    })
}

#[cfg(test)]
mod tests {
    use nodedb_types::BoundingBox;
    use nodedb_types::geometry::Geometry;

    use crate::engine::spatial::SpatialIndexManager;

    #[test]
    fn dwithin_bbox_expansion() {
        // Verify that DWithin expands the bbox correctly via expand_meters.
        let mut mgr = SpatialIndexManager::new();
        // Point at (10.0, 20.0).
        mgr.index_document("col", "geom", "1", &Geometry::point(10.0, 20.0));
        // Point far away at (50.0, 50.0).
        mgr.index_document("col", "geom", "2", &Geometry::point(50.0, 50.0));

        // Query geometry at (10.0, 20.0), large distance should include nearby point.
        let query_geom = Geometry::point(10.0, 20.0);
        let bbox = nodedb_types::geometry_bbox(&query_geom).expand_meters(100_000.0);
        let hits = mgr.search("col", "geom", &bbox);
        // Only the nearby point should be within 100 km.
        assert!(!hits.is_empty());
    }

    #[test]
    fn intersects_bbox() {
        let mut mgr = SpatialIndexManager::new();
        mgr.index_document("col", "geom", "3", &Geometry::point(5.0, 5.0));
        mgr.index_document("col", "geom", "4", &Geometry::point(90.0, 80.0));

        // Query bbox covers only the first point.
        let bbox = BoundingBox::new(0.0, 0.0, 10.0, 10.0);
        let hits = mgr.search("col", "geom", &bbox);
        assert_eq!(hits.len(), 1);

        // Resolve doc_id via entry_to_doc inverse map.
        let entry_id = hits[0].id;
        let doc_id = mgr.doc_id_for_entry(entry_id).unwrap();
        assert_eq!(doc_id, "3");
    }
}
