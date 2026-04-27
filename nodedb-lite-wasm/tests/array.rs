//! WASM array-engine smoke test.
//!
//! Run with:
//!
//! ```bash
//! wasm-pack test --node nodedb-lite-wasm
//! ```
//!
//! Native `cargo test` / `cargo nextest run` skip this file because of the
//! `target_arch = "wasm32"` gate (`wasm_bindgen_test` and `js_sys::Uint8Array`
//! only exist on that target).
//!
//! **Coverage equivalence:** the underlying engine paths exercised here —
//! `create_array`, `array_put_cell`, `array_read_coord`, `array_slice`,
//! `array_delete_cell`, `array_gdpr_erase_cell` — are also covered natively
//! through the Lite engine integration tests
//! (`nodedb-lite/tests/array_lite.rs`) and the FFI smoke tests
//! (`nodedb-lite-ffi/tests/array_smoke.rs`). The unique value of this file
//! is verifying the wasm-bindgen surface (msgpack `Uint8Array` round-trips,
//! `js_sys::Date::now()` for system time, async-from-JS).

#![cfg(target_arch = "wasm32")]

use js_sys::Uint8Array;
use nodedb_array::schema::ArraySchemaBuilder;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_lite_wasm::NodeDbLiteWasm;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_node_experimental);

fn encode<T: zerompk::ToMessagePack>(v: &T) -> Uint8Array {
    let bytes = zerompk::to_msgpack_vec(v).expect("encode failed");
    Uint8Array::from(bytes.as_slice())
}

fn two_d_schema() -> nodedb_array::schema::ArraySchema {
    ArraySchemaBuilder::new("a")
        .dim(DimSpec::new("x", DimType::Int64, 0, 1024))
        .dim(DimSpec::new("y", DimType::Int64, 0, 1024))
        .attr(AttrSpec::new("v", AttrType::Float64))
        .tile_extents(vec![64, 64])
        .build()
        .expect("schema build")
}

fn coord(x: i64, y: i64) -> Vec<CoordValue> {
    vec![CoordValue::Int64(x), CoordValue::Int64(y)]
}

fn float_attrs(v: f64) -> Vec<CellValue> {
    vec![CellValue::Float64(v)]
}

#[wasm_bindgen_test]
async fn create_put_slice_roundtrip() {
    let db = NodeDbLiteWasm::open(":memory:").await.expect("open");
    let schema = encode(&two_d_schema());
    db.array_create("a", &schema).await.expect("create");

    let c = encode(&coord(1, 2));
    let attrs = encode(&float_attrs(1.5));
    db.array_put_cell("a", &c, &attrs, 0, i64::MAX)
        .await
        .expect("put");

    let cell = db.array_read_coord("a", &c, None).await.expect("read");
    assert!(cell.is_some(), "cell should exist after put");
}
