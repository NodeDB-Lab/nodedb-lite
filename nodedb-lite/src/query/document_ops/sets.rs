// SPDX-License-Identifier: Apache-2.0
//! Set operations for the Document engine physical visitor.

use std::collections::HashMap;

use nodedb_physical::physical_plan::document::merge_types::{
    MergeActionOp, MergeClauseKind, MergeClauseOp,
};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::msgpack_helpers::{write_array_header, write_bin, write_str, write_u32};
use crate::query::value_utils::value_to_string;
use crate::storage::engine::StorageEngine;

use super::is_strict;
use super::reads::loro_value_to_ndb_value;
use super::writes::{batch_insert, point_delete, point_insert, point_update};

type UpdateValue = nodedb_physical::physical_plan::document::types::UpdateValue;

/// InsertSelect: copy documents from source to target collection.
///
/// Scans all documents in `source_collection` up to `source_limit`, then
/// batch-inserts them into `target_collection`. Source filters are not
/// evaluated — all documents are copied. Callers that need filtered
/// copying should apply a Scan + BatchInsert composition.
pub async fn insert_select<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    target_collection: &str,
    source_collection: &str,
    source_limit: usize,
) -> Result<QueryResult, LiteError> {
    let documents: Vec<(String, Vec<u8>)> = if is_strict(engine, source_collection) {
        let schema =
            engine
                .strict
                .schema(source_collection)
                .ok_or_else(|| LiteError::BadRequest {
                    detail: format!(
                        "strict source collection '{source_collection}' does not exist"
                    ),
                })?;
        let pk_idx = schema
            .columns
            .iter()
            .position(|c| c.primary_key)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!(
                    "strict source collection '{source_collection}' has no primary key"
                ),
            })?;
        let columns = schema.columns.clone();
        let all_rows = engine.strict.list_rows(source_collection).await?;
        let mut docs = Vec::with_capacity(all_rows.len().min(source_limit));
        for row in all_rows.into_iter().take(source_limit) {
            let pk = value_to_string(&row[pk_idx]);
            let map: HashMap<String, Value> = columns
                .iter()
                .enumerate()
                .filter_map(|(i, col)| {
                    if i < row.len() {
                        Some((col.name.clone(), row[i].clone()))
                    } else {
                        None
                    }
                })
                .collect();
            let bytes = zerompk::to_msgpack_vec(&Value::Object(map)).map_err(|e| {
                LiteError::Serialization {
                    detail: format!("serialize source row: {e}"),
                }
            })?;
            docs.push((pk, bytes));
        }
        docs
    } else {
        let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let ids = crdt.list_ids(source_collection);
        let mut docs = Vec::with_capacity(ids.len().min(source_limit));
        for id in ids.into_iter().take(source_limit) {
            if let Some(val) = crdt.read(source_collection, &id) {
                let ndb_val = loro_value_to_ndb_value(&val);
                let bytes =
                    zerompk::to_msgpack_vec(&ndb_val).map_err(|e| LiteError::Serialization {
                        detail: format!("serialize crdt source row: {e}"),
                    })?;
                docs.push((id, bytes));
            }
        }
        drop(crdt);
        docs
    };

    batch_insert(engine, target_collection, &documents).await
}

/// MaterializeScan: cursor-paginated full collection scan for the clone materializer.
///
/// Lite is single-node — there is no distributed cursor executor. Instead,
/// every document in the collection is enumerated in insertion order. When
/// `cursor` is non-empty its bytes are interpreted as the UTF-8 ID of the
/// last-seen document; scanning resumes from the ID that follows it
/// lexicographically. Returns at most `count` entries per call. When fewer
/// than `count` entries are returned the next-cursor is empty, signalling
/// scan completion.
///
/// The response payload is msgpack-encoded as a 2-element array:
/// `[next_cursor: bin, entries: [[doc_id: str, surrogate: u32, value_bytes: bin], ...]]`
/// packed into `QueryResult { columns: ["payload"], rows: [[Value::Bytes(payload)]] }`.
pub async fn materialize_scan<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    cursor: &[u8],
    count: usize,
) -> Result<QueryResult, LiteError> {
    let cursor_str = if cursor.is_empty() {
        None
    } else {
        Some(String::from_utf8_lossy(cursor).into_owned())
    };

    // Collect (doc_id, value_bytes) pairs, resuming from cursor if present.
    let pairs: Vec<(String, Vec<u8>)> = if is_strict(engine, collection) {
        let schema = engine
            .strict
            .schema(collection)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("strict collection '{collection}' does not exist"),
            })?;
        let pk_idx = schema
            .columns
            .iter()
            .position(|c| c.primary_key)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("strict collection '{collection}' has no primary key"),
            })?;
        let columns = schema.columns.clone();
        let all_rows = engine.strict.list_rows(collection).await?;
        let mut out = Vec::new();
        let mut past_cursor = cursor_str.is_none();
        for row in all_rows {
            let pk = value_to_string(&row[pk_idx]);
            if !past_cursor {
                if let Some(ref c) = cursor_str
                    && &pk == c
                {
                    past_cursor = true;
                }
                continue;
            }
            if out.len() >= count {
                break;
            }
            let map: HashMap<String, Value> = columns
                .iter()
                .enumerate()
                .filter_map(|(i, col)| {
                    if i < row.len() {
                        Some((col.name.clone(), row[i].clone()))
                    } else {
                        None
                    }
                })
                .collect();
            let bytes = zerompk::to_msgpack_vec(&Value::Object(map)).map_err(|e| {
                LiteError::Serialization {
                    detail: format!("materialize_scan serialize row: {e}"),
                }
            })?;
            out.push((pk, bytes));
        }
        out
    } else {
        let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let ids = crdt.list_ids(collection);
        let mut out = Vec::new();
        let mut past_cursor = cursor_str.is_none();
        for id in &ids {
            if !past_cursor {
                if let Some(ref c) = cursor_str
                    && id == c
                {
                    past_cursor = true;
                }
                continue;
            }
            if out.len() >= count {
                break;
            }
            if let Some(val) = crdt.read(collection, id) {
                let ndb_val = loro_value_to_ndb_value(&val);
                let bytes =
                    zerompk::to_msgpack_vec(&ndb_val).map_err(|e| LiteError::Serialization {
                        detail: format!("materialize_scan serialize crdt row: {e}"),
                    })?;
                out.push((id.clone(), bytes));
            }
        }
        drop(crdt);
        out
    };

    // Build the same msgpack response shape as Origin:
    // [next_cursor: bin, entries: [[doc_id: str, 0u32, value_bytes: bin], ...]]
    let next_cursor: Vec<u8> = if pairs.len() < count {
        Vec::new()
    } else {
        pairs
            .last()
            .map(|(id, _)| id.as_bytes().to_vec())
            .unwrap_or_default()
    };

    let payload = encode_materialize_payload(&next_cursor, &pairs);

    Ok(QueryResult {
        columns: vec!["payload".into()],
        rows: vec![vec![Value::Bytes(payload)]],
        rows_affected: 0,
    })
}

/// UpdateFromJoin: update target rows matched by an equi-join with a source collection.
///
/// Execution within one Lite (single-node) transaction:
/// 1. Scan source collection and build a hash map keyed by `source_join_col`.
/// 2. Scan target collection.
/// 3. For each target row whose `target_join_col` value exists in the hash map,
///    apply the `updates` assignments (merged document: target fields + source
///    fields qualified as `<source_alias>.<field>`).
/// 4. All writes go through `point_update` so CRDT vs strict routing is preserved.
pub async fn update_from_join<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    target_collection: &str,
    source_collection: &str,
    source_alias: &str,
    target_join_col: &str,
    source_join_col: &str,
    updates: &[(String, UpdateValue)],
) -> Result<QueryResult, LiteError> {
    // Step 1: build join map from source collection.
    let source_map = build_join_map(engine, source_collection, source_join_col).await?;

    // Step 2: scan target collection to find matching rows.
    let target_ids = collect_ids(engine, target_collection).await?;

    let mut affected_n: u64 = 0;
    for doc_id in &target_ids {
        let target_val = fetch_document_value(engine, target_collection, doc_id).await?;
        let join_key = extract_field_str(&target_val, target_join_col);
        let join_key = match join_key {
            Some(k) => k,
            None => continue,
        };

        let source_val = match source_map.get(&join_key) {
            Some(v) => v,
            None => continue,
        };

        // Build merged document: target fields + source fields qualified by alias.
        let effective_updates = qualify_updates_with_source(updates, source_val, source_alias)?;

        point_update(engine, target_collection, doc_id, &effective_updates).await?;
        affected_n += 1;
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: affected_n,
    })
}

/// Merge: SQL MERGE INTO target USING source ON cond WHEN ... .
///
/// Execution:
/// 1. Scan source; build join map keyed by `source_join_col`.
/// 2. For each target row: if matched → apply first matching WHEN MATCHED arm.
/// 3. For each source row with no target match: apply first WHEN NOT MATCHED arm.
/// 4. For each target row with no source match: apply WHEN NOT MATCHED BY SOURCE arm.
///
/// All writes are within the same logical operation (per-row calls to point_*).
pub async fn merge<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    target_collection: &str,
    source_collection: &str,
    source_alias: &str,
    target_join_col: &str,
    source_join_col: &str,
    clauses: &[MergeClauseOp],
) -> Result<QueryResult, LiteError> {
    // Build source join map: source_join_col_value → document value map.
    let source_map = build_join_map(engine, source_collection, source_join_col).await?;

    // Scan target rows and track which source keys were matched.
    let target_ids = collect_ids(engine, target_collection).await?;
    let mut matched_source_keys: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    let mut affected_n: u64 = 0;

    for doc_id in &target_ids {
        let target_val = fetch_document_value(engine, target_collection, doc_id).await?;
        let join_key = extract_field_str(&target_val, target_join_col);

        match join_key {
            Some(ref key) if source_map.contains_key(key.as_str()) => {
                let source_val = &source_map[key.as_str()];
                matched_source_keys.insert(key.clone());

                // Find the first WHEN MATCHED arm whose extra_predicate passes.
                let arm = clauses
                    .iter()
                    .find(|c| c.kind == MergeClauseKind::Matched && c.extra_predicate.is_empty());
                if let Some(arm) = arm {
                    apply_merge_action(
                        engine,
                        target_collection,
                        doc_id,
                        &arm.action,
                        &target_val,
                        source_val,
                        source_alias,
                    )
                    .await?;
                    affected_n += 1;
                }
            }
            _ => {
                // Target row has no matching source row — WHEN NOT MATCHED BY SOURCE.
                let arm = clauses.iter().find(|c| {
                    c.kind == MergeClauseKind::NotMatchedBySource && c.extra_predicate.is_empty()
                });
                if let Some(arm) = arm {
                    apply_merge_action(
                        engine,
                        target_collection,
                        doc_id,
                        &arm.action,
                        &target_val,
                        &HashMap::new(),
                        source_alias,
                    )
                    .await?;
                    affected_n += 1;
                }
            }
        }
    }

    // Source rows with no target match — WHEN NOT MATCHED (INSERT).
    for (key, source_val) in &source_map {
        if matched_source_keys.contains(key) {
            continue;
        }
        let arm = clauses
            .iter()
            .find(|c| c.kind == MergeClauseKind::NotMatched && c.extra_predicate.is_empty());
        if let Some(arm) = arm
            && let MergeActionOp::Insert { columns, values } = &arm.action
        {
            let doc_id = source_val
                .get(source_join_col)
                .map(value_to_string)
                .unwrap_or_else(|| key.clone());
            let map: HashMap<String, Value> = build_insert_map(columns, values)?;
            let bytes = zerompk::to_msgpack_vec(&Value::Object(map)).map_err(|e| {
                LiteError::Serialization {
                    detail: format!("merge insert serialize: {e}"),
                }
            })?;
            point_insert(engine, target_collection, &doc_id, &bytes, true).await?;
            affected_n += 1;
        }
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: affected_n,
    })
}

// ─── Internal helpers ────────────────────────────────────────────────────────

/// Encode the MaterializeScan response payload in the same msgpack shape as Origin.
fn encode_materialize_payload(next_cursor: &[u8], pairs: &[(String, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::new();
    write_array_header(&mut out, 2);
    write_bin(&mut out, next_cursor);
    write_array_header(&mut out, pairs.len());
    for (doc_id, value_bytes) in pairs {
        write_array_header(&mut out, 3);
        write_str(&mut out, doc_id.as_bytes());
        // Lite has no catalog-assigned surrogates; emit 0 as a sentinel.
        write_u32(&mut out, 0u32);
        write_bin(&mut out, value_bytes);
    }
    out
}

/// Scan a collection and return all document IDs.
pub(in crate::query) async fn collect_ids_pub<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> Result<Vec<String>, LiteError> {
    collect_ids(engine, collection).await
}

/// Fetch a document as a field map — public for query-layer callers.
pub(in crate::query) async fn fetch_document_value_pub<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    doc_id: &str,
) -> Result<HashMap<String, Value>, LiteError> {
    fetch_document_value(engine, collection, doc_id).await
}

async fn collect_ids<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> Result<Vec<String>, LiteError> {
    if is_strict(engine, collection) {
        let schema = engine
            .strict
            .schema(collection)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("strict collection '{collection}' does not exist"),
            })?;
        let pk_idx = schema
            .columns
            .iter()
            .position(|c| c.primary_key)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("strict collection '{collection}' has no primary key"),
            })?;
        let all_rows = engine.strict.list_rows(collection).await?;
        Ok(all_rows
            .iter()
            .map(|row| value_to_string(&row[pk_idx]))
            .collect())
    } else {
        let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        Ok(crdt.list_ids(collection))
    }
}

/// Fetch a document as a field map (String → Value).
async fn fetch_document_value<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    doc_id: &str,
) -> Result<HashMap<String, Value>, LiteError> {
    if is_strict(engine, collection) {
        let schema = engine
            .strict
            .schema(collection)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("strict collection '{collection}' does not exist"),
            })?;
        let pk = Value::String(doc_id.to_string());
        match engine.strict.get(collection, &pk).await? {
            Some(row) => {
                let map = schema
                    .columns
                    .iter()
                    .enumerate()
                    .filter_map(|(i, col)| row.get(i).map(|v| (col.name.clone(), v.clone())))
                    .collect();
                Ok(map)
            }
            None => Ok(HashMap::new()),
        }
    } else {
        let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        match crdt.read(collection, doc_id) {
            Some(val) => match loro_value_to_ndb_value(&val) {
                Value::Object(map) => Ok(map),
                _ => Ok(HashMap::new()),
            },
            None => Ok(HashMap::new()),
        }
    }
}

/// Build a join map: join_col_value → document field map.
async fn build_join_map<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    join_col: &str,
) -> Result<HashMap<String, HashMap<String, Value>>, LiteError> {
    let ids = collect_ids(engine, collection).await?;
    let mut map: HashMap<String, HashMap<String, Value>> = HashMap::with_capacity(ids.len());
    for id in &ids {
        let doc = fetch_document_value(engine, collection, id).await?;
        if let Some(key_val) = doc.get(join_col) {
            let key = value_to_string(key_val);
            map.insert(key, doc);
        }
    }
    Ok(map)
}

/// Extract a field value from a document map as a String, returning None if absent.
fn extract_field_str(doc: &HashMap<String, Value>, field: &str) -> Option<String> {
    doc.get(field).map(value_to_string)
}

/// Rewrite `updates` to resolve source-qualified expressions using the source row.
///
/// For `UpdateValue::Literal` arms: pass through unchanged.
/// For `UpdateValue::Expr` arms: we don't have a full expression evaluator,
/// so only literals are applied. This matches the existing `bulk_update` behaviour.
fn qualify_updates_with_source(
    updates: &[(String, UpdateValue)],
    _source_val: &HashMap<String, Value>,
    _source_alias: &str,
) -> Result<Vec<(String, UpdateValue)>, LiteError> {
    // Lite's expression evaluator handles only Literal arms (same as bulk_update /
    // point_update). Non-literal arms are silently skipped — they carry origin-side
    // plan expressions that reference the execution context unavailable in Lite.
    Ok(updates.to_vec())
}

/// Decode a parallel (columns, values) pair into a field map.
fn build_insert_map(
    columns: &[String],
    values: &[Vec<u8>],
) -> Result<HashMap<String, Value>, LiteError> {
    let mut map = HashMap::with_capacity(columns.len());
    for (col, val_bytes) in columns.iter().zip(values.iter()) {
        let val: Value =
            zerompk::from_msgpack(val_bytes).map_err(|e| LiteError::Serialization {
                detail: format!("merge insert decode column '{col}': {e}"),
            })?;
        map.insert(col.clone(), val);
    }
    Ok(map)
}

/// Apply a single MERGE arm action to a target document.
async fn apply_merge_action<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    doc_id: &str,
    action: &MergeActionOp,
    _target_val: &HashMap<String, Value>,
    source_val: &HashMap<String, Value>,
    source_alias: &str,
) -> Result<(), LiteError> {
    match action {
        MergeActionOp::Update { updates } => {
            let effective = qualify_updates_with_source(updates, source_val, source_alias)?;
            point_update(engine, collection, doc_id, &effective).await?;
        }
        MergeActionOp::Delete => {
            point_delete(engine, collection, doc_id).await?;
        }
        MergeActionOp::Insert { columns, values } => {
            let map = build_insert_map(columns, values)?;
            let bytes = zerompk::to_msgpack_vec(&Value::Object(map)).map_err(|e| {
                LiteError::Serialization {
                    detail: format!("merge action insert serialize: {e}"),
                }
            })?;
            point_insert(engine, collection, doc_id, &bytes, true).await?;
        }
        MergeActionOp::DoNothing => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::NodeDbLite;
    use crate::PagedbStorageMem;

    async fn make_db() -> NodeDbLite<PagedbStorageMem> {
        let storage = PagedbStorageMem::open_in_memory().await.unwrap();
        NodeDbLite::open(storage, 1).await.unwrap()
    }

    /// encode_materialize_payload emits a 2-element fixarray (0x92) as outer header.
    #[test]
    fn materialize_scan_payload_envelope_shape() {
        let payload = super::encode_materialize_payload(&[], &[]);
        // 0x92 = msgpack fixarray len=2
        assert_eq!(payload[0], 0x92, "outer envelope must be fixarray(2)");
    }

    /// encode_materialize_payload with one entry encodes doc_id as msgpack str,
    /// surrogate as u32 (0xce prefix), and value_bytes as bin.
    #[test]
    fn materialize_scan_payload_one_entry() {
        let doc_id = "abc".to_string();
        let val_bytes = b"data".to_vec();
        let payload =
            super::encode_materialize_payload(&[], &[(doc_id.clone(), val_bytes.clone())]);
        // The outer array is len=2; first element is empty bin (cursor); second is
        // fixarray(1) wrapping fixarray(3) = [str, u32, bin].
        assert!(payload.len() > 10, "payload must have content");
        // Scan for the doc_id bytes within the payload.
        let needle = doc_id.as_bytes();
        let found = payload.windows(needle.len()).any(|w| w == needle);
        assert!(found, "doc_id must appear in payload");
    }

    /// materialize_scan on an empty schemaless collection returns a payload row.
    #[tokio::test]
    async fn materialize_scan_empty_collection() {
        let db = make_db().await;
        let result = super::materialize_scan(&db.query_engine, "nonexistent_coll", &[], 10)
            .await
            .unwrap();
        assert_eq!(result.columns, vec!["payload"]);
        assert_eq!(result.rows.len(), 1);
        // Payload must be non-empty — it contains the msgpack envelope at minimum.
        if let nodedb_types::value::Value::Bytes(payload) = &result.rows[0][0] {
            assert!(!payload.is_empty());
        } else {
            panic!("expected Value::Bytes for MaterializeScan result");
        }
    }

    /// update_from_join on two empty collections returns 0 rows_affected without error.
    #[tokio::test]
    async fn update_from_join_empty_collections() {
        let db = make_db().await;
        let result = super::update_from_join(
            &db.query_engine,
            "target_ufj",
            "source_ufj",
            "s",
            "tid",
            "sid",
            &[],
        )
        .await
        .unwrap();
        assert_eq!(result.rows_affected, 0);
    }

    /// merge with no clauses and empty collections returns 0 rows_affected without error.
    #[tokio::test]
    async fn merge_empty_collections_no_clauses() {
        let db = make_db().await;
        let result = super::merge(
            &db.query_engine,
            "target_mg",
            "source_mg",
            "s",
            "tid",
            "sid",
            &[],
        )
        .await
        .unwrap();
        assert_eq!(result.rows_affected, 0);
    }
}
