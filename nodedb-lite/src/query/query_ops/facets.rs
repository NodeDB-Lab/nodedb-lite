// SPDX-License-Identifier: Apache-2.0
//! FacetCounts — per-field value-frequency aggregation over a filtered collection.

use std::collections::HashMap;

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::query_ops::joins::common::{
    apply_filters, decode_filters, maps_to_result, scan_collection,
};
use crate::storage::engine::StorageEngine;

/// Compute per-field facet counts for a filtered collection.
///
/// For each field in `fields`, scans the (optionally filtered) collection and
/// returns `(value, count)` pairs sorted by count descending and truncated to
/// `limit_per_facet` (0 = unlimited).
///
/// Output rows have three columns: `field`, `value`, `count`.
pub async fn execute_facet_counts<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    filters: &[u8],
    fields: &[String],
    limit_per_facet: usize,
) -> Result<QueryResult, LiteError> {
    let parsed_filters = decode_filters(filters)?;
    let all_rows = scan_collection(engine, collection).await?;
    let rows = apply_filters(all_rows, &parsed_filters);

    let mut output: Vec<HashMap<String, Value>> = Vec::new();

    for field in fields {
        let mut counts: HashMap<String, (Value, u64)> = HashMap::new();

        for row in &rows {
            let val = row.get(field).cloned().unwrap_or(Value::Null);
            let key = facet_key(&val);
            counts
                .entry(key)
                .and_modify(|(_, c)| *c += 1)
                .or_insert((val, 1));
        }

        let mut pairs: Vec<(Value, u64)> = counts.into_values().collect();
        // Sort by count descending, then by string key ascending for determinism.
        pairs.sort_by(|(va, ca), (vb, cb)| {
            cb.cmp(ca).then_with(|| facet_key(va).cmp(&facet_key(vb)))
        });

        let take = if limit_per_facet == 0 {
            pairs.len()
        } else {
            limit_per_facet.min(pairs.len())
        };

        for (val, count) in pairs.into_iter().take(take) {
            let mut row: HashMap<String, Value> = HashMap::new();
            row.insert("field".to_string(), Value::String(field.clone()));
            row.insert("value".to_string(), val);
            row.insert("count".to_string(), Value::Integer(count as i64));
            output.push(row);
        }
    }

    Ok(maps_to_result(output))
}

/// Stable string key for a `Value` used as a facet bucket identifier.
fn facet_key(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => format!("bool:{b}"),
        Value::Integer(n) => format!("int:{n}"),
        Value::Float(f) => format!("float:{f}"),
        Value::String(s) => format!("str:{s}"),
        Value::Uuid(s) | Value::Ulid(s) => format!("id:{s}"),
        Value::Bytes(b) => format!("bytes:{}", b.len()),
        Value::Array(a) | Value::Set(a) => format!("array:{}", a.len()),
        Value::Object(m) => format!("object:{}", m.len()),
        _ => "other".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(fields: &[(&str, Value)]) -> HashMap<String, Value> {
        fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    /// Directly test bucket counting and ordering logic without a storage backend.
    #[test]
    fn test_facet_counts_bucket_ordering() {
        let rows = vec![
            make_row(&[
                ("category", Value::String("electronics".into())),
                ("brand", Value::String("acme".into())),
            ]),
            make_row(&[
                ("category", Value::String("electronics".into())),
                ("brand", Value::String("acme".into())),
            ]),
            make_row(&[
                ("category", Value::String("electronics".into())),
                ("brand", Value::String("bravo".into())),
            ]),
            make_row(&[
                ("category", Value::String("clothing".into())),
                ("brand", Value::String("acme".into())),
            ]),
            make_row(&[
                ("category", Value::String("clothing".into())),
                ("brand", Value::String("charlie".into())),
            ]),
        ];

        // Test per-field bucket building directly.
        let fields = ["category".to_string(), "brand".to_string()];
        let mut output: Vec<HashMap<String, Value>> = Vec::new();

        for field in &fields {
            let mut counts: HashMap<String, (Value, u64)> = HashMap::new();
            for row in &rows {
                let val = row.get(field).cloned().unwrap_or(Value::Null);
                let key = facet_key(&val);
                counts
                    .entry(key)
                    .and_modify(|(_, c)| *c += 1)
                    .or_insert((val, 1));
            }
            let mut pairs: Vec<(Value, u64)> = counts.into_values().collect();
            pairs.sort_by(|(va, ca), (vb, cb)| {
                cb.cmp(ca).then_with(|| facet_key(va).cmp(&facet_key(vb)))
            });
            for (val, count) in pairs.into_iter().take(10) {
                let mut row: HashMap<String, Value> = HashMap::new();
                row.insert("field".to_string(), Value::String(field.clone()));
                row.insert("value".to_string(), val);
                row.insert("count".to_string(), Value::Integer(count as i64));
                output.push(row);
            }
        }

        // category: electronics(3) then clothing(2) → 2 rows
        // brand: acme(3), bravo(1), charlie(1) → 3 rows; total = 5
        assert_eq!(output.len(), 5);

        // First facet bucket for "category" must be electronics with count 3.
        let cat_rows: Vec<_> = output
            .iter()
            .filter(|r| r.get("field") == Some(&Value::String("category".into())))
            .collect();
        assert_eq!(cat_rows.len(), 2);
        assert_eq!(cat_rows[0]["count"], Value::Integer(3));
        assert_eq!(cat_rows[0]["value"], Value::String("electronics".into()));

        // Top brand bucket: acme with count 3.
        let brand_rows: Vec<_> = output
            .iter()
            .filter(|r| r.get("field") == Some(&Value::String("brand".into())))
            .collect();
        assert_eq!(brand_rows[0]["count"], Value::Integer(3));
        assert_eq!(brand_rows[0]["value"], Value::String("acme".into()));
    }

    /// limit_per_facet=1 truncates to the top bucket per field.
    #[test]
    fn test_facet_counts_limit_per_facet() {
        let rows = vec![
            make_row(&[("color", Value::String("red".into()))]),
            make_row(&[("color", Value::String("red".into()))]),
            make_row(&[("color", Value::String("blue".into()))]),
        ];
        let field = "color".to_string();
        let limit_per_facet = 1usize;

        let mut counts: HashMap<String, (Value, u64)> = HashMap::new();
        for row in &rows {
            let val = row.get(&field).cloned().unwrap_or(Value::Null);
            let key = facet_key(&val);
            counts
                .entry(key)
                .and_modify(|(_, c)| *c += 1)
                .or_insert((val, 1));
        }
        let mut pairs: Vec<(Value, u64)> = counts.into_values().collect();
        pairs.sort_by(|(va, ca), (vb, cb)| {
            cb.cmp(ca).then_with(|| facet_key(va).cmp(&facet_key(vb)))
        });
        let truncated: Vec<_> = pairs.into_iter().take(limit_per_facet).collect();
        assert_eq!(truncated.len(), 1);
        assert_eq!(truncated[0].1, 2); // red appears twice
    }
}
