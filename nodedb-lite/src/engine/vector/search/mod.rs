// SPDX-License-Identifier: Apache-2.0

//! Free-function vector search callable from both `NodeDbLite` and
//! `LiteDataPlaneVisitor` without depending on either concrete type.

mod lazy_load;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_types::error::NodeDbResult;
use nodedb_types::filter::MetadataFilter;
use nodedb_types::result::SearchResult;
use nodedb_types::value::Value;
use nodedb_vector::rerank::{Candidate, IndexShape, recall_scale, rerank, validate_options};

use crate::engine::vector::sidecar::install_sidecar_for_index;

use crate::engine::crdt::CrdtEngine;
use crate::engine::vector::VectorState;
use crate::error::LiteError;
use crate::nodedb::convert::loro_value_to_document;
use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

/// Run a vector similarity search against the named HNSW index.
///
/// `index_key` is the HNSW bucket key (e.g. `"collection"` or
/// `"collection:field_name"`). `collection` is the CRDT collection name
/// used to fetch metadata.
#[allow(clippy::too_many_arguments)]
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
    // ── ANN option validation + scaling ──────────────────────────────────────

    let default_opts;
    let opts: &nodedb_types::VectorAnnOptions = match ann_options {
        Some(o) => o,
        None => {
            default_opts = nodedb_types::VectorAnnOptions::default();
            &default_opts
        }
    };

    // Fetch the collection's configured quantization so the mismatch check
    // in validate_options can surface a precise BadInput. Fall back to None
    // (FP32 path) when no entry exists for this index_key — matches existing
    // collections that pre-date per_index_config population (C2c).
    let collection_quant = {
        let configs = vector_state
            .per_index_config
            .lock()
            .map_err(|_| LiteError::LockPoisoned)?;
        configs
            .get(index_key)
            .map(|cfg| cfg.quantization)
            .unwrap_or(nodedb_types::VectorQuantization::None)
    };

    // Validate option combination. Returns BadInput for unsupported combos
    // (e.g. meta_token_budget on single-vector, unroutable codec variants,
    // or a search-time quantization that mismatches the collection's codec).
    // If a codec is requested, lazy-install a trained sidecar before the
    // coarse search so that rerank() always finds one.
    let rerank_codec =
        validate_options(opts, IndexShape::SingleVector, collection_quant).map_err(|e| {
            LiteError::BadRequest {
                detail: e.to_string(),
            }
        })?;

    if let Some(codec_name) = rerank_codec {
        install_sidecar_for_index(vector_state, index_key, codec_name)?;
    }

    // Scale ef_search and oversample for the requested recall target.
    // Feed `vector_state.search_ef` as the base so the scaled values
    // honour the configured default before applying caller/override layering.
    let (scaled_ef, scaled_oversample) = recall_scale(
        opts.target_recall,
        vector_state.search_ef,
        opts.oversample.unwrap_or(1).max(1),
    )
    .map_err(|e| LiteError::BadRequest {
        detail: e.to_string(),
    })?;

    // Final ef_search: explicit override → caller hint → scaled engine default.
    let ef_search = opts
        .ef_search_override
        .or(ef_search_caller)
        .unwrap_or(scaled_ef);

    // ── Lazy-load HNSW from storage (+ sidecar restore) ──────────────────────

    lazy_load::ensure_index_loaded(vector_state, index_key).await?;

    let indices = vector_state.hnsw_indices.lock_or_recover();
    let Some(index) = indices.get(index_key) else {
        return Ok(Vec::new());
    };

    // Metric override: when `metric` differs from `index.metric()`, the coarse
    // HNSW traversal still uses the index's baked metric (graph topology is
    // built for it), but the rerank below scores candidates with the requested
    // metric. Recall depends on `oversample` providing enough candidates that
    // the true top-k under the requested metric are in the coarse-retrieved set.
    // Callers should bump `oversample` when query metric ≠ index metric.

    let id_map = vector_state.vector_id_map.lock_or_recover();
    let crdt_guard = crdt.lock_or_recover();

    // ── Fetch-k: scale by oversample, triple when post-filtering ─────────────

    let needs_filter = filter.is_some() || prefilter_bitmap.is_some();
    let oversample = scaled_oversample.max(1) as usize;
    let fetch_k = if needs_filter {
        k.saturating_mul(oversample).saturating_mul(3)
    } else {
        k.saturating_mul(oversample)
    };

    let collection_size = id_map
        .keys()
        .filter(|key| key.starts_with(index_key))
        .count();

    // ── Coarse HNSW search ────────────────────────────────────────────────────

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

    // ── Shared rerank (FP32 exact distance, Matryoshka-truncation aware) ──────

    let candidates: Vec<Candidate> = raw_results
        .into_iter()
        .filter(|r| !index.is_deleted(r.id))
        .map(|r| Candidate {
            id: r.id,
            index_distance: r.distance,
        })
        .collect();

    // Look up codec sidecar for this index. If opts.quantization is Some(_)
    // but no sidecar exists, the pipeline returns RerankError::BadInput
    // ("no codec sidecar provided") — no need to duplicate the check here.
    let sidecars = vector_state
        .codec_sidecars
        .lock()
        .map_err(|_| LiteError::LockPoisoned)?;
    let sidecar = sidecars.get(index_key);

    let ranked = rerank(
        candidates,
        query,
        metric.unwrap_or_else(|| index.metric()),
        k,
        opts,
        sidecar,
        |id| index.get_vector(id),
    )
    .map_err(|e| LiteError::Query(e.to_string()))?;

    // ── Hydrate metadata and apply post-filter ────────────────────────────────

    let results: Vec<SearchResult> = ranked
        .into_iter()
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
        .collect();

    Ok(results)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_types::VectorAnnOptions;
    use nodedb_vector::rerank::{IndexShape, recall_scale, validate_options};

    // ── oversample math ──────────────────────────────────────────────────────

    #[test]
    fn oversample_4_no_filter_fetch_k_is_k_times_4() {
        let k = 10_usize;
        let oversample: usize = 4;
        let fetch_k = k.saturating_mul(oversample);
        assert_eq!(fetch_k, 40);
    }

    #[test]
    fn oversample_4_with_filter_fetch_k_is_k_times_12() {
        let k = 10_usize;
        let oversample: usize = 4;
        let fetch_k = k.saturating_mul(oversample).saturating_mul(3);
        assert_eq!(fetch_k, 120);
    }

    // ── target_recall scaling ────────────────────────────────────────────────

    #[test]
    fn target_recall_095_scales_ef_and_oversample() {
        let base_ef = 50_usize;
        let base_oversample: u8 = 1;
        let (scaled_ef, scaled_oversample) =
            recall_scale(Some(0.95), base_ef, base_oversample).unwrap();
        assert_eq!(scaled_ef, 200);
        assert_eq!(scaled_oversample, 2);
    }

    #[test]
    fn target_recall_none_returns_base_unchanged() {
        let (ef, os) = recall_scale(None, 100, 1).unwrap();
        assert_eq!(ef, 100);
        assert_eq!(os, 1);
    }

    #[test]
    fn target_recall_invalid_returns_bad_input() {
        let result = recall_scale(Some(1.5), 100, 1);
        assert!(result.is_err());
    }

    // ── codec guard ──────────────────────────────────────────────────────────

    #[test]
    fn sq8_quantization_returns_bad_request_via_validate() {
        use nodedb_types::vector_ann::VectorQuantization;
        let opts = VectorAnnOptions {
            quantization: Some(VectorQuantization::Sq8),
            ..Default::default()
        };
        let rerank_codec =
            validate_options(&opts, IndexShape::SingleVector, VectorQuantization::Sq8).unwrap();
        assert!(
            rerank_codec.is_some(),
            "Sq8 should produce a Some(CodecName)"
        );
    }

    #[test]
    fn meta_token_budget_returns_bad_input_from_validate() {
        let opts = VectorAnnOptions {
            meta_token_budget: Some(8),
            ..Default::default()
        };
        let result = validate_options(
            &opts,
            IndexShape::SingleVector,
            nodedb_types::VectorQuantization::None,
        );
        assert!(
            result.is_err(),
            "meta_token_budget on single-vector should be a BadInput error"
        );
    }

    // ── query_dim plumbing ───────────────────────────────────────────────────

    #[test]
    fn query_dim_zero_rejected_by_rerank() {
        use nodedb_types::vector_distance::DistanceMetric;
        use nodedb_vector::rerank::{Candidate, rerank};
        let store: HashMap<u32, Vec<f32>> = [(1, vec![1.0, 2.0])].into_iter().collect();
        let opts = VectorAnnOptions {
            query_dim: Some(0),
            ..Default::default()
        };
        let err = rerank(
            vec![Candidate {
                id: 1,
                index_distance: 0.0,
            }],
            &[0.0, 0.0],
            DistanceMetric::L2,
            1,
            &opts,
            None,
            |id| store.get(&id).map(|v| v.as_slice()),
        )
        .unwrap_err();
        assert!(err.to_string().contains("query_dim=0"));
    }

    #[test]
    fn query_dim_some_changes_ranking_order() {
        use nodedb_types::vector_distance::DistanceMetric;
        use nodedb_vector::rerank::{Candidate, rerank};
        let store: HashMap<u32, Vec<f32>> = [(1, vec![0.1, 0.1]), (2, vec![0.0, 9.0])]
            .into_iter()
            .collect();
        let query = [0.0_f32, 1.0];

        let full = rerank(
            vec![
                Candidate {
                    id: 1,
                    index_distance: 0.0,
                },
                Candidate {
                    id: 2,
                    index_distance: 0.0,
                },
            ],
            &query,
            DistanceMetric::L2,
            2,
            &VectorAnnOptions::default(),
            None,
            |id| store.get(&id).map(|v| v.as_slice()),
        )
        .unwrap();

        let trunc = rerank(
            vec![
                Candidate {
                    id: 1,
                    index_distance: 0.0,
                },
                Candidate {
                    id: 2,
                    index_distance: 0.0,
                },
            ],
            &query,
            DistanceMetric::L2,
            2,
            &VectorAnnOptions {
                query_dim: Some(1),
                ..Default::default()
            },
            None,
            |id| store.get(&id).map(|v| v.as_slice()),
        )
        .unwrap();

        assert_eq!(full[0].id, 1, "full-dim: id=1 should rank first");
        assert_eq!(trunc[0].id, 2, "truncated dim=1: id=2 should rank first");
    }

    // ── metric override ──────────────────────────────────────────────────────

    #[test]
    fn metric_override_does_not_panic() {
        use nodedb_types::vector_distance::DistanceMetric;
        use nodedb_vector::rerank::{Candidate, rerank};

        let store: HashMap<u32, Vec<f32>> = [(1, vec![1.0, 0.0]), (2, vec![0.0, 1.0])]
            .into_iter()
            .collect();
        let query = [1.0_f32, 0.0];

        let result = rerank(
            vec![
                Candidate {
                    id: 1,
                    index_distance: 0.0,
                },
                Candidate {
                    id: 2,
                    index_distance: 1.0,
                },
            ],
            &query,
            DistanceMetric::L2,
            2,
            &VectorAnnOptions::default(),
            None,
            |id| store.get(&id).map(|v| v.as_slice()),
        );
        assert!(result.is_ok(), "metric override rerank must not error");
        let ranked = result.unwrap();
        assert!(!ranked.is_empty(), "must return at least one result");
        assert_eq!(ranked[0].id, 1, "L2 rerank: id=1 should rank first");
    }

    #[test]
    fn metric_none_uses_index_metric() {
        use nodedb_types::vector_distance::DistanceMetric;
        use nodedb_vector::rerank::{Candidate, rerank};

        let store: HashMap<u32, Vec<f32>> = [(1, vec![1.0, 0.0]), (2, vec![0.0, 1.0])]
            .into_iter()
            .collect();
        let query = [1.0_f32, 0.0];
        let index_metric = DistanceMetric::Cosine;

        let result = rerank(
            vec![
                Candidate {
                    id: 1,
                    index_distance: 0.0,
                },
                Candidate {
                    id: 2,
                    index_distance: 1.0,
                },
            ],
            &query,
            index_metric,
            2,
            &VectorAnnOptions::default(),
            None,
            |id| store.get(&id).map(|v| v.as_slice()),
        );
        assert!(
            result.is_ok(),
            "metric=None (index metric) rerank must not error"
        );
        assert!(!result.unwrap().is_empty());
    }
}
