//! `ArrayOp` builders used across the array-sync test suite.

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::{ArrayOp, ArrayOpHeader, ArrayOpKind};
use nodedb_array::sync::replica_id::ReplicaId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;

use super::clock::hlc;

pub fn put_op(
    array: &str,
    coord_x: i64,
    val: f64,
    ms: u64,
    schema_hlc: Hlc,
    rep: ReplicaId,
) -> ArrayOp {
    ArrayOp {
        header: ArrayOpHeader {
            array: array.into(),
            hlc: hlc(ms, rep),
            schema_hlc,
            valid_from_ms: 0,
            valid_until_ms: -1,
            system_from_ms: ms as i64,
        },
        kind: ArrayOpKind::Put,
        coord: vec![CoordValue::Int64(coord_x)],
        attrs: Some(vec![CellValue::Float64(val)]),
    }
}

pub fn delete_op(array: &str, coord_x: i64, ms: u64, schema_hlc: Hlc, rep: ReplicaId) -> ArrayOp {
    ArrayOp {
        header: ArrayOpHeader {
            array: array.into(),
            hlc: hlc(ms, rep),
            schema_hlc,
            valid_from_ms: 0,
            valid_until_ms: -1,
            system_from_ms: ms as i64,
        },
        kind: ArrayOpKind::Delete,
        coord: vec![CoordValue::Int64(coord_x)],
        attrs: None,
    }
}

pub fn erase_op(array: &str, coord_x: i64, ms: u64, schema_hlc: Hlc, rep: ReplicaId) -> ArrayOp {
    ArrayOp {
        header: ArrayOpHeader {
            array: array.into(),
            hlc: hlc(ms, rep),
            schema_hlc,
            valid_from_ms: 0,
            valid_until_ms: -1,
            system_from_ms: ms as i64,
        },
        kind: ArrayOpKind::Erase,
        coord: vec![CoordValue::Int64(coord_x)],
        attrs: None,
    }
}
