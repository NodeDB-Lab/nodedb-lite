// SPDX-License-Identifier: Apache-2.0

//! Free-function vector search callable from both `NodeDbLite` and
//! `LiteDataPlaneVisitor` without depending on either concrete type.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_types::error::NodeDbResult;
use nodedb_types::filter::MetadataFilter;
use nodedb_types::result::SearchResult;
use nodedb_types::value::Value;

use crate::engine::crdt::CrdtEngine;
use crate::engine::vector::VectorState;
use crate::nodedb::convert::loro_value_to_document;
use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

/// Run a vector similarity search against the named HNSW index.
///
/// `index_key` is the HNSW bucket key (e.g. `"collection"` or
/// `"collection:field_name"`). `collection` is the CRDT collection name
/// used to fetch metadata.
pub(crate) async fn run_vector_search<S>(
    vector_state: &Arc<VectorState<S>>,
    crdt: &Arc<Mutex<CrdtEngine>>,
    index_key: &str,
    collection: &str,
    query: &[f32],
    k: usize,
    filter: Option<&MetadataFilter>,
    exclude_fields: &[&str],
    prefilter_bitmap: Option<&roaring::RoaringBitmap>,
    ann_options: Option<&nodedb_types::VectorAnnOptions>,
    skip_payload_fetch: bool,
    metric: Option<nodedb_types::vector_distance::DistanceMetric>,
    // Caller-supplied dynamic-list size for the HNSW search. `None` falls
    // through to the engine default. `ann_options.ef_search_override` (if
    // set) takes precedence over both.
    ef_search_caller: Option<usize>,
) -> NodeDbResult<Vec<SearchResult>>
where
    S: StorageEngine,
{
    let ef_search = ann_options
        .and_then(|o| o.ef_search_override)
        .or(ef_search_caller)
        .unwrap_or(vector_state.search_ef);
    if let Some(o) = ann_options {
        if o.quantization.is_some()
            || o.oversample.is_some()
            || o.query_dim.is_some()
            || o.meta_token_budget.is_some()
            || o.target_recall.is_some()
        {
            unimplemented!(
                "Lite vector engine does not yet honor VectorAnnOptions \
                 {{ quantization, oversample, query_dim, meta_token_budget, target_recall }}; \
                 only ef_search_override is wired. Add codec dispatch + test-time-scaling \
                 to nodedb-lite::engine::vector::search::run_vector_search."
            );
        }
    }
    {
        let has_it = vector_state
            .hnsw_indices
            .lock_or_recover()
            .contains_key(index_key);
        if !has_it {
            let key = format!("hnsw:{index_key}");
            if let Some(checkpoint) = vector_state
                .storage
                .get(nodedb_types::Namespace::Vector, key.as_bytes())
                .await?
                && let Ok(Some(index)) =
                    crate::engine::vector::graph::HnswIndex::from_checkpoint(&checkpoint)
            {
                tracing::info!(index_key, "lazy-loaded HNSW collection from storage");
                vector_state
                    .hnsw_indices
                    .lock_or_recover()
                    .insert(index_key.to_string(), index);
            }
        }
    }

    let indices = vector_state.hnsw_indices.lock_or_recover();
    let Some(index) = indices.get(index_key) else {
        return Ok(Vec::new());
    };

    // Enforce metric match: HNSW indices are built with a specific distance
    // metric baked in. Honoring a different query-time metric requires
    // either rebuilding the index or running a separate flat-scan with the
    // requested metric — neither is wired yet.
    if let Some(requested) = metric
        && requested != index.metric()
    {
        unimplemented!(
            "Lite vector search does not support query-time metric override \
             ({:?} requested, {:?} on index); add metric-aware re-search or \
             rebuild the index with the desired metric.",
            requested,
            index.metric()
        );
    }

    let id_map = vector_state.vector_id_map.lock_or_recover();
    let crdt_guard = crdt.lock_or_recover();

    let needs_filter = filter.is_some() || prefilter_bitmap.is_some();
    let fetch_k = if needs_filter { k * 3 } else { k };
    let collection_size = id_map
        .keys()
        .filter(|key| key.starts_with(index_key))
        .count();

    let raw_results = if let Some(f) = filter
        && collection_size <= 10_000
    {
        let mut allowed = roaring::RoaringBitmap::new();
        for (composite_key, (doc_id, _)) in id_map.iter() {
            if !composite_key.starts_with(index_key) {
                continue;
            }
            if let Some(loro_val) = crdt_guard.read(collection, doc_id) {
                let doc = loro_value_to_document(doc_id, &loro_val);
                let json_doc = serde_json::to_value(&doc.fields).unwrap_or_default();
                if nodedb_query::metadata_filter::matches_metadata_filter(&json_doc, f)
                    && let Some(vid_str) = composite_key.strip_prefix(&format!("{index_key}:"))
                    && let Ok(vid) = vid_str.parse::<u32>()
                {
                    allowed.insert(vid);
                }
            }
        }
        if let Some(pre) = prefilter_bitmap {
            allowed &= pre;
        }
        if allowed.is_empty() {
            return Ok(Vec::new());
        }
        index.search_filtered(query, k, ef_search, &allowed)
    } else if let Some(pre) = prefilter_bitmap {
        if pre.is_empty() {
            return Ok(Vec::new());
        }
        index.search_filtered(query, k, ef_search, pre)
    } else {
        index.search(query, fetch_k, ef_search)
    };

    let results: Vec<SearchResult> = raw_results
        .into_iter()
        .filter(|r| !index.is_deleted(r.id))
        .filter_map(|r| {
            let composite_key = format!("{index_key}:{}", r.id);
            let doc_id = id_map
                .get(&composite_key)
                .map(|(id, _)| id.clone())
                .unwrap_or_else(|| r.id.to_string());

            // When skip_payload_fetch=true and no post-filter is required,
            // skip the CRDT read and document hydration entirely.
            let needs_payload = !skip_payload_fetch || filter.is_some();
            let metadata = if needs_payload {
                if let Some(loro_val) = crdt_guard.read(collection, &doc_id) {
                    let doc = loro_value_to_document(&doc_id, &loro_val);
                    doc.fields
                        .into_iter()
                        .filter(|(k, _)| !exclude_fields.contains(&k.as_str()))
                        .collect::<HashMap<String, Value>>()
                } else {
                    HashMap::new()
                }
            } else {
                HashMap::new()
            };

            if let Some(f) = filter {
                let json_doc = serde_json::to_value(&metadata).unwrap_or_default();
                if !nodedb_query::metadata_filter::matches_metadata_filter(&json_doc, f) {
                    return None;
                }
            }
            // Honor skip_payload_fetch even when filter forced a hydration:
            // the caller asked for no payload in the result.
            let metadata = if skip_payload_fetch {
                HashMap::new()
            } else {
                metadata
            };

            Some(SearchResult {
                id: doc_id,
                node_id: None,
                distance: r.distance,
                metadata,
            })
        })
        .take(k)
        .collect();

    Ok(results)
}
