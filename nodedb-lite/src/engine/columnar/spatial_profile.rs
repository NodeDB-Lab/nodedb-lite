//! Spatial profile helpers for columnar collections.
//!
//! When a columnar collection has `ColumnarProfile::Spatial`, this module
//! provides automatic R-tree indexing and geohash computation for geometry
//! columns on insert.

use nodedb_types::columnar::{ColumnType, ColumnarProfile, ColumnarSchema};
use nodedb_types::geometry::Geometry;
use nodedb_types::value::Value;

/// Extract geometry value from a row for spatial indexing.
///
/// Returns (geometry_column_index, Geometry) if found.
pub fn extract_geometry(
    schema: &ColumnarSchema,
    profile: &ColumnarProfile,
    values: &[Value],
) -> Option<(usize, Geometry)> {
    let geom_col_name = match profile {
        ColumnarProfile::Spatial {
            geometry_column, ..
        } => geometry_column.as_str(),
        _ => return None,
    };

    for (i, col) in schema.columns.iter().enumerate() {
        if (col.name == geom_col_name || matches!(col.column_type, ColumnType::Geometry))
            && let Some(geom) = value_to_geometry(values.get(i)?)
        {
            return Some((i, geom));
        }
    }
    None
}

/// Compute a geohash string for a Point geometry.
///
/// Returns `None` if the geometry is not a Point or if geohash computation fails.
/// Uses precision 8 (approx 38m × 19m cells).
pub fn compute_geohash(geom: &Geometry) -> Option<String> {
    match geom {
        Geometry::Point { coordinates } => {
            let lng = coordinates[0];
            let lat = coordinates[1];
            if lng.is_finite() && lat.is_finite() {
                Some(nodedb_spatial::geohash::geohash_encode(lng, lat, 8))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Convert a Value to a Geometry if possible.
fn value_to_geometry(value: &Value) -> Option<Geometry> {
    match value {
        Value::Geometry(g) => Some(g.clone()),
        Value::String(s) => sonic_rs::from_str(s).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnarProfile, ColumnarSchema};

    use super::*;

    fn spatial_schema() -> (ColumnarSchema, ColumnarProfile) {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("geom", ColumnType::Geometry),
            ColumnDef::nullable("name", ColumnType::String),
        ])
        .expect("valid");
        let profile = ColumnarProfile::Spatial {
            geometry_column: "geom".into(),
            auto_rtree: true,
            auto_geohash: true,
        };
        (schema, profile)
    }

    #[test]
    fn extract_point_geometry() {
        let (schema, profile) = spatial_schema();
        let values = vec![
            Value::Integer(1),
            Value::Geometry(Geometry::Point {
                coordinates: [10.0, 20.0],
            }),
            Value::String("test".into()),
        ];

        let result = extract_geometry(&schema, &profile, &values);
        assert!(result.is_some());
        let (idx, geom) = result.unwrap();
        assert_eq!(idx, 1);
        assert!(matches!(geom, Geometry::Point { .. }));
    }

    #[test]
    fn compute_geohash_for_point() {
        let geom = Geometry::Point {
            coordinates: [-73.9857, 40.7484],
        };
        let hash = compute_geohash(&geom);
        assert!(hash.is_some());
        assert_eq!(hash.unwrap().len(), 8);
    }

    #[test]
    fn compute_geohash_for_polygon_returns_none() {
        let geom = Geometry::Polygon {
            coordinates: vec![vec![[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]],
        };
        assert!(compute_geohash(&geom).is_none());
    }

    #[test]
    fn non_spatial_profile_returns_none() {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("geom", ColumnType::Geometry),
        ])
        .expect("valid");
        let values = vec![
            Value::Integer(1),
            Value::Geometry(Geometry::Point {
                coordinates: [0.0, 0.0],
            }),
        ];

        let result = extract_geometry(&schema, &ColumnarProfile::Plain, &values);
        assert!(result.is_none());
    }
}
