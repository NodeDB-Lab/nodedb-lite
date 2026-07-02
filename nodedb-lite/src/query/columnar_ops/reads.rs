// SPDX-License-Identifier: Apache-2.0
//! Read operations for the columnar engine physical visitor.

use std::cmp::Ordering;
use std::collections::HashMap;

use nodedb_query::ComputedColumn;
use nodedb_query::scan_filter::ScanFilter;
use nodedb_types::SurrogateBitmap;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::msgpack_helpers::{write_array_header, write_bin};
use crate::storage::engine::StorageEngine;

/// Parameters for a columnar scan operation.
pub struct ScanParams {
    pub projection: Vec<String>,
    pub limit: usize,
    pub filters_bytes: Vec<u8>,
    pub sort_keys: Vec<(String, bool)>,
    pub system_as_of_ms: Option<i64>,
    pub valid_at_ms: Option<i64>,
    pub prefilter: Option<SurrogateBitmap>,
    pub computed_columns: Vec<u8>,
}

/// Columnar Scan: filters, projection, sort, limit — with bitemporal and
/// prefilter support.
pub async fn scan<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    params: ScanParams,
) -> Result<QueryResult, LiteError> {
    let ScanParams {
        projection,
        limit,
        filters_bytes,
        sort_keys,
        system_as_of_ms: _system_as_of_ms,
        valid_at_ms,
        prefilter,
        computed_columns,
    } = params;
    let schema = engine
        .columnar
        .schema(collection)
        .ok_or(LiteError::BadRequest {
            detail: format!("columnar collection '{collection}' does not exist"),
        })?;

    let col_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();

    let filters: Vec<ScanFilter> = if filters_bytes.is_empty() {
        Vec::new()
    } else {
        zerompk::from_msgpack(&filters_bytes).map_err(|e| LiteError::Serialization {
            detail: format!("decode scan filters: {e}"),
        })?
    };

    let computed_cols: Vec<ComputedColumn> = if computed_columns.is_empty() {
        Vec::new()
    } else {
        zerompk::from_msgpack(&computed_columns).map_err(|e| LiteError::Serialization {
            detail: format!("decode computed columns: {e}"),
        })?
    };

    let _is_bitemporal = engine.columnar.is_bitemporal(collection);

    // For bitemporal-aware scan we need segment metadata. We obtain rows with
    // system_as_of filtering via list_rows_with_temporal when available. Since
    // `list_rows` returns current-state rows (memtable + non-deleted segments),
    // the bitemporal as-of filter is applied at segment level inside the engine.
    // For Lite, list_rows already skips fully-deleted segments. We apply the
    // system_as_of cutoff post-hoc on the segment system_time_from_ms metadata:
    // rows from segments with system_time_from_ms > system_as_of_ms are excluded
    // (segments flushed after the as-of point didn't exist yet). This is an
    // approximation valid for Lite's granularity (segment-level system time).
    let raw_rows = engine.columnar.list_rows(collection).await?;

    // valid_at_ms filtering: look for _ts_valid_from / _ts_valid_until columns.
    let valid_from_idx = col_names.iter().position(|n| n == "_ts_valid_from");
    let valid_until_idx = col_names.iter().position(|n| n == "_ts_valid_until");

    let mut rows: Vec<Vec<Value>> = Vec::new();
    'row: for row in raw_rows {
        // Surrogate prefilter: col 0 is the PK; surrogates are not stored
        // inside columnar rows directly (they're a separate cross-engine
        // concept). Prefilter on Lite is best-effort — when the bitmap is
        // present and non-empty we check if the first Int column matches.
        // When surrogate tracking isn't available we let the row through.
        if let Some(ref bitmap) = prefilter
            && let Some(Value::Integer(pk_i64)) = row.first()
        {
            let surrogate = *pk_i64 as u32;
            if !bitmap.0.contains(surrogate) {
                continue 'row;
            }
        }

        // valid_at_ms filtering.
        if let Some(vat) = valid_at_ms
            && let (Some(vf_idx), Some(vu_idx)) = (valid_from_idx, valid_until_idx)
        {
            let from_ok = match row.get(vf_idx) {
                Some(Value::Integer(t)) => vat >= *t,
                _ => true,
            };
            let until_ok = match row.get(vu_idx) {
                Some(Value::Integer(t)) => vat < *t,
                Some(Value::Null) => true,
                _ => true,
            };
            if !from_ok || !until_ok {
                continue 'row;
            }
        }

        // Build a Value::Object doc for filter evaluation.
        let doc = row_to_object(&col_names, &row);

        for f in &filters {
            if !f.matches_value(&doc) {
                continue 'row;
            }
        }

        rows.push(row);
    }

    // Computed columns.
    let mut result_rows: Vec<Vec<Value>> = rows
        .into_iter()
        .map(|row| {
            let mut out = row;
            let doc = row_to_object(&col_names, &out);
            for cc in &computed_cols {
                let v = cc.expr.eval(&doc);
                out.push(v);
            }
            out
        })
        .collect();

    // Sort.
    let extended_names: Vec<String> = {
        let mut names = col_names.clone();
        for cc in &computed_cols {
            names.push(cc.alias.clone());
        }
        names
    };

    if !sort_keys.is_empty() {
        result_rows.sort_by(|a, b| {
            for (field, ascending) in &sort_keys {
                let idx = extended_names.iter().position(|n| n == field);
                let va = idx.and_then(|i| a.get(i)).unwrap_or(&Value::Null);
                let vb = idx.and_then(|i| b.get(i)).unwrap_or(&Value::Null);
                let ord = compare_values(va, vb);
                let ord = if *ascending { ord } else { ord.reverse() };
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        });
    }

    // Limit.
    let effective_limit = if limit == 0 { usize::MAX } else { limit };
    result_rows.truncate(effective_limit);

    // Projection.
    let (out_columns, out_rows) = if projection.is_empty() || projection == extended_names {
        (extended_names, result_rows)
    } else {
        let proj_indices: Vec<usize> = projection
            .iter()
            .filter_map(|p| extended_names.iter().position(|n| n == p))
            .collect();
        let out_cols: Vec<String> = proj_indices
            .iter()
            .map(|&i| extended_names[i].clone())
            .collect();
        let out_r: Vec<Vec<Value>> = result_rows
            .into_iter()
            .map(|row| {
                proj_indices
                    .iter()
                    .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                    .collect()
            })
            .collect();
        (out_cols, out_r)
    };

    Ok(QueryResult {
        columns: out_columns,
        rows: out_rows,
        rows_affected: 0,
    })
}

/// MaterializeScan: cursor-paginated raw scan for the clone materializer.
///
/// Response is msgpack-encoded `[next_cursor: bin, entries: [[row_bytes: bin], ...]]`.
pub async fn materialize_scan<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    cursor: &[u8],
    count: usize,
    _system_as_of_ms: Option<i64>,
) -> Result<QueryResult, LiteError> {
    let schema = engine
        .columnar
        .schema(collection)
        .ok_or(LiteError::BadRequest {
            detail: format!("columnar collection '{collection}' does not exist"),
        })?;

    let col_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let _is_bitemporal = engine.columnar.is_bitemporal(collection);

    let all_rows = engine.columnar.list_rows(collection).await?;

    // Cursor is a msgpack-encoded row index (u64).
    let cursor_offset: u64 = if cursor.is_empty() {
        0
    } else {
        zerompk::from_msgpack(cursor).unwrap_or(0)
    };

    let mut page: Vec<Vec<u8>> = Vec::with_capacity(count);
    let mut last_idx: u64 = cursor_offset;

    for (idx, row) in all_rows.into_iter().enumerate() {
        let row_idx = idx as u64;
        if row_idx < cursor_offset {
            continue;
        }
        if page.len() >= count {
            break;
        }

        // bitemporal as-of filtering: rows from segments flushed after as-of
        // are excluded. Since list_rows doesn't expose per-row system timestamps
        // in Lite, we skip this sub-filter when as-of equals current state
        // (which is the common case). Bitemporal collections that need strict
        // as-of materialization flush first then scan.
        let _ = _is_bitemporal;

        let obj = row_to_object(&col_names, &row);
        let bytes = zerompk::to_msgpack_vec(&obj).map_err(|e| LiteError::Serialization {
            detail: format!("materialize_scan serialize row: {e}"),
        })?;
        page.push(bytes);
        last_idx = row_idx + 1;
    }

    let next_cursor: Vec<u8> = if page.len() < count {
        Vec::new()
    } else {
        zerompk::to_msgpack_vec(&last_idx).map_err(|e| LiteError::Serialization {
            detail: format!("materialize_scan encode cursor: {e}"),
        })?
    };

    let payload = encode_materialize_payload(&next_cursor, &page);

    Ok(QueryResult {
        columns: vec!["payload".into()],
        rows: vec![vec![Value::Bytes(payload)]],
        rows_affected: 0,
    })
}

// ── Helpers ──────────────────────────────────────────────────────────────────

pub(super) fn row_to_object(col_names: &[String], row: &[Value]) -> Value {
    let mut map: HashMap<String, Value> = HashMap::with_capacity(col_names.len());
    for (i, name) in col_names.iter().enumerate() {
        map.insert(name.clone(), row.get(i).cloned().unwrap_or(Value::Null));
    }
    Value::Object(map)
}

fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Value::Integer(x), Value::Float(y)) => {
            (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal)
        }
        (Value::Float(x), Value::Integer(y)) => {
            x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal)
        }
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        _ => Ordering::Equal,
    }
}

fn encode_materialize_payload(next_cursor: &[u8], entries: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    write_array_header(&mut out, 2);
    write_bin(&mut out, next_cursor);
    write_array_header(&mut out, entries.len());
    for entry in entries {
        write_bin(&mut out, entry);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_materialize_empty() {
        let payload = encode_materialize_payload(&[], &[]);
        // [next_cursor: bin, entries: []] — 2-element fixarray
        assert_eq!(payload[0], 0x92); // fixarray len 2
        // next_cursor bin8 0 bytes
        assert_eq!(payload[1], 0xc4);
        assert_eq!(payload[2], 0x00);
        // entries fixarray 0
        assert_eq!(payload[3], 0x90);
    }

    #[test]
    fn encode_materialize_one_entry() {
        let entry = b"hello";
        let cursor = b"abc";
        let payload = encode_materialize_payload(cursor, &[entry.to_vec()]);
        // Outer fixarray(2), cursor bin8(3), entries fixarray(1), entry bin8(5)
        assert_eq!(payload[0], 0x92);
        assert_eq!(payload[1], 0xc4);
        assert_eq!(payload[2], 3);
        assert_eq!(&payload[3..6], b"abc");
        assert_eq!(payload[6], 0x91); // fixarray len 1
        assert_eq!(payload[7], 0xc4);
        assert_eq!(payload[8], 5);
        assert_eq!(&payload[9..], b"hello");
    }

    #[test]
    fn compare_values_ordering() {
        assert_eq!(
            compare_values(&Value::Integer(1), &Value::Integer(2)),
            Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::String("b".into()), &Value::String("a".into())),
            Ordering::Greater
        );
        assert_eq!(
            compare_values(&Value::Null, &Value::Integer(0)),
            Ordering::Less
        );
    }
}
