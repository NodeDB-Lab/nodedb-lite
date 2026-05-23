//! wasm-bindgen array-engine methods on [`NodeDbLiteWasm`].
//!
//! All six methods mirror the FFI surface.  Structured arguments (schema,
//! coordinates, ranges, payloads) are passed as MessagePack-encoded
//! `Uint8Array` blobs — callers encode with any msgpack library and receive
//! the same format back.
//!
//! # Wire format
//!
//! | Parameter          | Rust type decoded from bytes                     |
//! |--------------------|--------------------------------------------------|
//! | `schema`           | `nodedb_array::schema::ArraySchema`              |
//! | `dims`             | `Vec<nodedb_array::query::slice::DimRange>` wrapped as `Vec<Option<DimRange>>` |
//! | `coord`            | `Vec<nodedb_array::types::coord::value::CoordValue>` |
//! | `payload` (return) | `nodedb_array::tile::cell_payload::CellPayload`  |
//!
//! Decode on the JS side with any compliant msgpack library (e.g. `msgpackr`).

use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;

use nodedb_array::query::slice::DimRange;
use nodedb_array::schema::ArraySchema;
use nodedb_array::tile::cell_payload::CellPayload;
use nodedb_array::types::coord::value::CoordValue;

use crate::NodeDbLiteWasm;

/// Decode msgpack bytes from a JS `Uint8Array` into `T`.
fn decode_msgpack<T: for<'a> zerompk::FromMessagePack<'a>>(
    bytes: &Uint8Array,
    label: &str,
) -> Result<T, JsError> {
    let vec = bytes.to_vec();
    zerompk::from_msgpack::<T>(&vec)
        .map_err(|e| JsError::new(&format!("msgpack decode error for {label}: {e}")))
}

/// Encode `T` to msgpack and wrap in a JS `Uint8Array`.
fn encode_msgpack<T: zerompk::ToMessagePack>(
    value: &T,
    label: &str,
) -> Result<Uint8Array, JsError> {
    let bytes = zerompk::to_msgpack_vec(value)
        .map_err(|e| JsError::new(&format!("msgpack encode error for {label}: {e}")))?;
    Ok(Uint8Array::from(bytes.as_slice()))
}

#[wasm_bindgen]
impl NodeDbLiteWasm {
    /// Create a new ND sparse array.
    ///
    /// `schema` — msgpack-encoded [`ArraySchema`]. Dims and attrs are part of
    /// the schema; no separate dims argument is needed.
    ///
    /// Returns an error if an array named `name` already exists.
    #[wasm_bindgen(js_name = "arrayCreate")]
    pub async fn array_create(&self, name: &str, schema: &Uint8Array) -> Result<(), JsError> {
        let array_schema: ArraySchema = decode_msgpack(schema, "schema")?;
        self.db
            .create_array(name, array_schema)
            .await
            .map_err(|e| JsError::new(&e.to_string()))
    }

    /// Write a cell into array `name` at `coord`.
    ///
    /// `coord`   — msgpack-encoded `Vec<CoordValue>`.
    /// `payload` — msgpack-encoded `Vec<CellValue>` (attribute values).
    /// `valid_from_ms` / `valid_until_ms` — bitemporal valid-time bounds in
    ///   milliseconds since Unix epoch.  Pass `i64::MAX` for an open upper
    ///   bound (no expiry).
    #[wasm_bindgen(js_name = "arrayPutCell")]
    pub async fn array_put_cell(
        &self,
        name: &str,
        coord: &Uint8Array,
        payload: &Uint8Array,
        valid_from_ms: i64,
        valid_until_ms: i64,
    ) -> Result<(), JsError> {
        use nodedb_array::types::cell_value::value::CellValue;

        let coord_vec: Vec<CoordValue> = decode_msgpack(coord, "coord")?;
        let attrs: Vec<CellValue> = decode_msgpack(payload, "payload")?;

        let system_from_ms = js_sys::Date::now() as i64;

        self.db
            .array_put_cell(
                name,
                coord_vec,
                attrs,
                system_from_ms,
                valid_from_ms,
                valid_until_ms,
            )
            .await
            .map_err(|e| JsError::new(&e.to_string()))
    }

    /// Slice query: return all live cells whose coordinates fall within `ranges`.
    ///
    /// `ranges`           — msgpack-encoded `Vec<Option<DimRange>>`.  Pass
    ///   `None` for a dimension to select the full extent.
    /// `as_of_system_ms`  — AS-OF system time in ms; `undefined` / `null`
    ///   selects the current live snapshot.
    ///
    /// Returns msgpack-encoded `Vec<CellPayload>`.
    #[wasm_bindgen(js_name = "arraySlice")]
    pub async fn array_slice(
        &self,
        name: &str,
        ranges: &Uint8Array,
        as_of_system_ms: Option<i64>,
    ) -> Result<Uint8Array, JsError> {
        let ranges_vec: Vec<Option<DimRange>> = decode_msgpack(ranges, "ranges")?;

        let cells: Vec<CellPayload> = self
            .db
            .array_slice(name, ranges_vec, as_of_system_ms)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;

        encode_msgpack(&cells, "cells")
    }

    /// Read the most recent live payload for a single coordinate.
    ///
    /// `coord`           — msgpack-encoded `Vec<CoordValue>`.
    /// `as_of_system_ms` — AS-OF system time in ms; `undefined` selects the
    ///   current live snapshot.
    ///
    /// Returns msgpack-encoded [`CellPayload`] or `undefined` if not found /
    /// tombstoned / erased.
    #[wasm_bindgen(js_name = "arrayReadCoord")]
    pub async fn array_read_coord(
        &self,
        name: &str,
        coord: &Uint8Array,
        as_of_system_ms: Option<i64>,
    ) -> Result<Option<Uint8Array>, JsError> {
        let coord_vec: Vec<CoordValue> = decode_msgpack(coord, "coord")?;

        let result: Option<CellPayload> = self
            .db
            .array_read_coord(name, &coord_vec, as_of_system_ms)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;

        match result {
            Some(cell) => encode_msgpack(&cell, "cell").map(Some),
            None => Ok(None),
        }
    }

    /// Soft-delete a cell by writing a tombstone at the current system time.
    ///
    /// `coord` — msgpack-encoded `Vec<CoordValue>`.
    ///
    /// The cell remains visible AS-OF any system time before the tombstone.
    #[wasm_bindgen(js_name = "arrayDeleteCell")]
    pub async fn array_delete_cell(&self, name: &str, coord: &Uint8Array) -> Result<(), JsError> {
        let coord_vec: Vec<CoordValue> = decode_msgpack(coord, "coord")?;
        let system_from_ms = js_sys::Date::now() as i64;

        self.db
            .array_delete_cell(name, coord_vec, system_from_ms)
            .await
            .map_err(|e| JsError::new(&e.to_string()))
    }

    /// GDPR erasure: overwrite the cell with the `0xFE` sentinel and flush.
    ///
    /// `coord` — msgpack-encoded `Vec<CoordValue>`.
    ///
    /// After this call [`array_read_coord`] returns `undefined` for the erased
    /// coordinate at any AS-OF time ≥ the erasure system time, and no raw
    /// payload bytes remain on disk.
    ///
    /// [`array_read_coord`]: NodeDbLiteWasm::array_read_coord
    #[wasm_bindgen(js_name = "arrayGdprEraseCell")]
    pub async fn array_gdpr_erase_cell(
        &self,
        name: &str,
        coord: &Uint8Array,
    ) -> Result<(), JsError> {
        let coord_vec: Vec<CoordValue> = decode_msgpack(coord, "coord")?;
        let system_from_ms = js_sys::Date::now() as i64;

        self.db
            .array_gdpr_erase_cell(name, coord_vec, system_from_ms)
            .await
            .map_err(|e| JsError::new(&e.to_string()))
    }
}
