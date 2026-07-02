//! Canonical test schemas used across the array-sync test suite.

use nodedb_array::schema::array_schema::ArraySchema;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::cell_order::{CellOrder, TileOrder};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::types::domain::{Domain, DomainBound};

/// One-dimensional Int64 schema over [0, 99], attribute "v" (Float64, nullable).
pub fn simple_schema(name: &str) -> ArraySchema {
    ArraySchema {
        name: name.into(),
        dims: vec![DimSpec::new(
            "x",
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(99)),
        )],
        attrs: vec![AttrSpec::new("v", AttrType::Float64, true)],
        tile_extents: vec![10],
        cell_order: CellOrder::RowMajor,
        tile_order: TileOrder::RowMajor,
    }
}
