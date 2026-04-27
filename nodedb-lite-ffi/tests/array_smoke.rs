//! Smoke tests for the array engine C FFI.
//!
//! Each test exercises the six public functions through their `extern "C"`
//! entry points. Types are exchanged as zerompk-encoded byte slices.

use std::ffi::CString;

use nodedb_array::query::slice::DimRange;
use nodedb_array::schema::ArraySchemaBuilder;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_lite_ffi::{
    NODEDB_OK, ndb_array_create, ndb_array_delete_cell, ndb_array_gdpr_erase_cell,
    ndb_array_put_cell, ndb_array_read_coord, ndb_array_slice, nodedb_close, nodedb_free_buf,
    nodedb_open,
};
use nodedb_types::OPEN_UPPER;

fn make_schema() -> nodedb_array::schema::ArraySchema {
    ArraySchemaBuilder::new("smoke")
        .dim(DimSpec::new(
            "x",
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(63)),
        ))
        .attr(AttrSpec::new("v", AttrType::Int64, true))
        .tile_extents(vec![8])
        .build()
        .unwrap()
}

fn encode<T: zerompk::ToMessagePack>(value: &T) -> Vec<u8> {
    zerompk::to_msgpack_vec(value).expect("encode")
}

#[test]
fn array_create_put_slice_roundtrip() {
    let path = CString::new(":memory:").unwrap();
    unsafe {
        let handle = nodedb_open(path.as_ptr(), 42);
        assert!(!handle.is_null());

        let name = CString::new("grid").unwrap();
        let schema = make_schema();
        let schema_bytes = encode(&schema);

        // Create.
        let rc = ndb_array_create(
            handle,
            name.as_ptr(),
            schema_bytes.as_ptr(),
            schema_bytes.len(),
        );
        assert_eq!(rc, NODEDB_OK, "create_array");

        // Put two cells.
        let coord1 = encode(&vec![CoordValue::Int64(1)]);
        let attrs1 = encode(&vec![CellValue::Int64(100)]);
        let rc = ndb_array_put_cell(
            handle,
            name.as_ptr(),
            coord1.as_ptr(),
            coord1.len(),
            attrs1.as_ptr(),
            attrs1.len(),
            0,
            OPEN_UPPER,
        );
        assert_eq!(rc, NODEDB_OK, "put_cell 1");

        let coord5 = encode(&vec![CoordValue::Int64(5)]);
        let attrs5 = encode(&vec![CellValue::Int64(200)]);
        let rc = ndb_array_put_cell(
            handle,
            name.as_ptr(),
            coord5.as_ptr(),
            coord5.len(),
            attrs5.as_ptr(),
            attrs5.len(),
            0,
            OPEN_UPPER,
        );
        assert_eq!(rc, NODEDB_OK, "put_cell 5");

        // Slice — unconstrained (None per dim).
        let ranges = encode(&vec![Option::<DimRange>::None]);
        let mut out_buf: *mut u8 = std::ptr::null_mut();
        let mut out_len: usize = 0;
        let rc = ndb_array_slice(
            handle,
            name.as_ptr(),
            ranges.as_ptr(),
            ranges.len(),
            0,
            0, // has_as_of = false
            &mut out_buf,
            &mut out_len,
        );
        assert_eq!(rc, NODEDB_OK, "slice");
        assert!(!out_buf.is_null());
        assert!(out_len > 0);

        // Decode result.
        let slice_bytes = std::slice::from_raw_parts(out_buf, out_len);
        let cells: Vec<nodedb_array::tile::cell_payload::CellPayload> =
            zerompk::from_msgpack(slice_bytes).expect("decode slice result");
        assert_eq!(cells.len(), 2, "expected 2 live cells");

        nodedb_free_buf(out_buf, out_len);
        nodedb_close(handle);
    }
}

#[test]
fn array_read_coord_returns_cell() {
    let path = CString::new(":memory:").unwrap();
    unsafe {
        let handle = nodedb_open(path.as_ptr(), 43);
        assert!(!handle.is_null());

        let name = CString::new("rc").unwrap();
        let schema_bytes = encode(&make_schema());
        ndb_array_create(
            handle,
            name.as_ptr(),
            schema_bytes.as_ptr(),
            schema_bytes.len(),
        );

        let coord = encode(&vec![CoordValue::Int64(7)]);
        let attrs = encode(&vec![CellValue::Int64(77)]);
        ndb_array_put_cell(
            handle,
            name.as_ptr(),
            coord.as_ptr(),
            coord.len(),
            attrs.as_ptr(),
            attrs.len(),
            0,
            OPEN_UPPER,
        );

        let mut out_buf: *mut u8 = std::ptr::null_mut();
        let mut out_len: usize = 0;
        let rc = ndb_array_read_coord(
            handle,
            name.as_ptr(),
            coord.as_ptr(),
            coord.len(),
            0,
            0, // current live snapshot
            &mut out_buf,
            &mut out_len,
        );
        assert_eq!(rc, NODEDB_OK, "read_coord");
        assert!(!out_buf.is_null(), "cell must exist");
        assert!(out_len > 0);

        let cell_bytes = std::slice::from_raw_parts(out_buf, out_len);
        let cell: nodedb_array::tile::cell_payload::CellPayload =
            zerompk::from_msgpack(cell_bytes).expect("decode cell");
        assert_eq!(cell.attrs[0], CellValue::Int64(77));

        nodedb_free_buf(out_buf, out_len);
        nodedb_close(handle);
    }
}

#[test]
fn array_delete_cell_tombstones_coord() {
    let path = CString::new(":memory:").unwrap();
    unsafe {
        let handle = nodedb_open(path.as_ptr(), 44);
        assert!(!handle.is_null());

        let name = CString::new("del").unwrap();
        let schema_bytes = encode(&make_schema());
        ndb_array_create(
            handle,
            name.as_ptr(),
            schema_bytes.as_ptr(),
            schema_bytes.len(),
        );

        let coord = encode(&vec![CoordValue::Int64(3)]);
        let attrs = encode(&vec![CellValue::Int64(33)]);
        ndb_array_put_cell(
            handle,
            name.as_ptr(),
            coord.as_ptr(),
            coord.len(),
            attrs.as_ptr(),
            attrs.len(),
            0,
            OPEN_UPPER,
        );

        let rc = ndb_array_delete_cell(handle, name.as_ptr(), coord.as_ptr(), coord.len());
        assert_eq!(rc, NODEDB_OK, "delete_cell");

        // After deletion, read_coord should return OK with null/zero out (not found).
        let mut out_buf: *mut u8 = std::ptr::null_mut();
        let mut out_len: usize = 0;
        let rc = ndb_array_read_coord(
            handle,
            name.as_ptr(),
            coord.as_ptr(),
            coord.len(),
            0,
            0,
            &mut out_buf,
            &mut out_len,
        );
        assert_eq!(rc, NODEDB_OK, "read_coord after delete");
        assert!(
            out_buf.is_null() || out_len == 0,
            "tombstoned cell must be absent"
        );

        nodedb_close(handle);
    }
}

#[test]
fn array_gdpr_erase_cell_removes_content() {
    let path = CString::new(":memory:").unwrap();
    unsafe {
        let handle = nodedb_open(path.as_ptr(), 45);
        assert!(!handle.is_null());

        let name = CString::new("gdpr").unwrap();
        let schema_bytes = encode(&make_schema());
        ndb_array_create(
            handle,
            name.as_ptr(),
            schema_bytes.as_ptr(),
            schema_bytes.len(),
        );

        let coord = encode(&vec![CoordValue::Int64(9)]);
        let attrs = encode(&vec![CellValue::Int64(99)]);
        ndb_array_put_cell(
            handle,
            name.as_ptr(),
            coord.as_ptr(),
            coord.len(),
            attrs.as_ptr(),
            attrs.len(),
            0,
            OPEN_UPPER,
        );

        let rc = ndb_array_gdpr_erase_cell(handle, name.as_ptr(), coord.as_ptr(), coord.len());
        assert_eq!(rc, NODEDB_OK, "gdpr_erase_cell");

        // After erasure, coord must not be found.
        let mut out_buf: *mut u8 = std::ptr::null_mut();
        let mut out_len: usize = 0;
        let rc = ndb_array_read_coord(
            handle,
            name.as_ptr(),
            coord.as_ptr(),
            coord.len(),
            0,
            0,
            &mut out_buf,
            &mut out_len,
        );
        assert_eq!(rc, NODEDB_OK, "read_coord after gdpr erase");
        assert!(
            out_buf.is_null() || out_len == 0,
            "GDPR-erased cell must be absent"
        );

        nodedb_close(handle);
    }
}
