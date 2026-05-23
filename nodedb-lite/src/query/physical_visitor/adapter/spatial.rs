// SPDX-License-Identifier: Apache-2.0
//! SpatialOp dispatch for the Lite physical visitor.

use nodedb_physical::physical_plan::SpatialOp;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::spatial_ops;
use crate::storage::engine::StorageEngine;

use super::LitePhysicalFut;

pub(super) fn dispatch<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    op: &SpatialOp,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    match op {
        SpatialOp::Insert {
            collection,
            field,
            surrogate,
            geometry,
        } => {
            let col = collection.clone();
            let fld = field.clone();
            let sur = *surrogate;
            let geom = geometry.clone();
            Ok(Box::pin(async move {
                spatial_ops::writes::spatial_insert(engine, &col, &fld, sur, &geom)
            }))
        }

        SpatialOp::Delete {
            collection,
            field,
            surrogate,
        } => {
            let col = collection.clone();
            let fld = field.clone();
            let sur = *surrogate;
            Ok(Box::pin(async move {
                spatial_ops::writes::spatial_delete(engine, &col, &fld, sur)
            }))
        }

        SpatialOp::Scan {
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
        } => {
            let params = spatial_ops::reads::ScanParams {
                collection: collection.clone(),
                field: field.clone(),
                predicate: *predicate,
                query_geometry: query_geometry.clone(),
                distance_meters: *distance_meters,
                attribute_filters: attribute_filters.clone(),
                limit: *limit,
                projection: projection.clone(),
                rls_filters: rls_filters.clone(),
                prefilter: prefilter.clone(),
            };
            Ok(Box::pin(async move {
                spatial_ops::reads::spatial_scan(engine, params)
            }))
        }
    }
}
