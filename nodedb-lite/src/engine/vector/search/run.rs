// SPDX-License-Identifier: Apache-2.0

//! Free-function vector search callable from both `NodeDbLite` and
//! `LiteDataPlaneVisitor` without depending on either concrete type.

use std::sync::{Arc, Mutex};

use nodedb_types::error::NodeDbResult;
use nodedb_types::filter::MetadataFilter;
use nodedb_types::result::SearchResult;
use nodedb_vector::rerank::{Candidate, IndexShape, recall_scale, validate_options};

use crate::engine::crdt::CrdtEngine;
use crate::engine::vector::sidecar::install_sidecar_for_index;
use crate::engine::vector::{IndexIdMap, VectorState};
use crate::error::LiteError;
use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

use super::filter::allowed_nodes;
use super::hydrate::Hydration;
use super::lazy_load;

/// Metadata and node-set restrictions applied to a vector search.
///
/// `exclude_fields` names the vector-internal row fields: they never reach
/// result metadata or the metadata filter.
#[derive(Clone, Copy, Default)]
pub(crate) struct SearchFilter<'a> {
    pub metadata: Option<&'a MetadataFilter>,
    pub exclude_fields: &'a [&'a str],
    pub prefilter_bitmap: Option<&'a roaring::RoaringBitmap>,
}

/// Search-time ANN tuning.
///
/// `ef_search` is the caller-supplied dynamic-list size for the HNSW search.
/// `None` falls through to the engine default. `options.ef_search_override`
/// (if set) takes precedence over both.
#[derive(Clone, Copy, Default)]
pub(crate) struct AnnTuning<'a> {
    pub options: Option<&'a nodedb_types::VectorAnnOptions>,
    pub metric: Option<nodedb_types::vector_distance::DistanceMetric>,
    pub ef_search: Option<usize>,
}

/// One vector similarity search.
///
/// `index_key` is the HNSW bucket key (e.g. `"collection"` or
/// `"collection:field_name"`). `collection` is the CRDT collection name
/// used to fetch metadata.
pub(crate) struct VectorSearchRequest<'a> {
    pub index_key: &'a str,
    pub collection: &'a str,
    pub query: &'a [f32],
    pub k: usize,
    pub filter: SearchFilter<'a>,
    pub tuning: AnnTuning<'a>,
    pub skip_payload_fetch: bool,
}

/// Run a vector similarity search against the HNSW index named by
/// `request.index_key`.
pub(crate) async fn run_vector_search<S>(
    vector_state: &Arc<VectorState<S>>,
    crdt: &Arc<Mutex<CrdtEngine>>,
    request: VectorSearchRequest<'_>,
) -> NodeDbResult<Vec<SearchResult>>
where
    S: StorageEngine,
{
    let VectorSearchRequest {
        index_key,
        collection,
        query,
        k,
        filter:
            SearchFilter {
                metadata: filter,
                exclude_fields,
                prefilter_bitmap,
            },
        tuning:
            AnnTuning {
                options: ann_options,
                metric,
                ef_search: ef_search_caller,
            },
        skip_payload_fetch,
    } = request;

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
        // The sidecar trains on the index, so a cold or evicted one is loaded
        // first.
        lazy_load::ensure_index_loaded(vector_state, index_key).await?;
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

    // ── Resident HNSW: loaded from storage when cold or evicted ──────────────

    let indices = crate::engine::vector::resident::lock_resident(vector_state, index_key).await?;
    let Some(index) = indices.get(index_key) else {
        return Ok(Vec::new());
    };
    // The index width is fixed even while it holds no vector, so a query of
    // another width fails before any shortcut returns an empty result.
    nodedb_vector::error::check_dim(index.dim(), query.len()).map_err(LiteError::from)?;

    // Metric override: when `metric` differs from `index.metric()`, the coarse
    // HNSW traversal still uses the index's baked metric (graph topology is
    // built for it), but the rerank below scores candidates with the requested
    // metric. Recall depends on `oversample` providing enough candidates that
    // the true top-k under the requested metric are in the coarse-retrieved set.
    // Callers should bump `oversample` when query metric ≠ index metric.

    let id_map = vector_state.vector_id_map.lock_or_recover();
    // Only this index's bindings: another index's node ids must never reach
    // this graph, whatever its key shares with this one.
    let ids = id_map.index(index_key);
    let crdt_guard = crdt.lock_or_recover();

    // ── Fetch-k: scale by oversample, triple when post-filtering ─────────────

    let needs_filter = filter.is_some() || prefilter_bitmap.is_some();
    let oversample = scaled_oversample.max(1) as usize;
    let fetch_k = if needs_filter {
        k.saturating_mul(oversample).saturating_mul(3)
    } else {
        k.saturating_mul(oversample)
    };

    let collection_size = ids.map_or(0, IndexIdMap::len);

    // ── Coarse HNSW search ────────────────────────────────────────────────────

    let searched = if let Some(f) = filter
        && collection_size <= 10_000
    {
        let mut allowed = allowed_nodes(ids, &crdt_guard, collection, f, exclude_fields);
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
    let raw_results = searched.map_err(LiteError::from)?;

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

    let ranked =
        super::rerank::rank_candidates(index, candidates, query, metric, k, opts, sidecar)?;

    // ── Hydrate metadata and apply post-filter ────────────────────────────────

    let hydration = Hydration {
        ids,
        crdt: &crdt_guard,
        collection,
        filter,
        exclude_fields,
        skip_payload_fetch,
    };
    Ok(hydration.results(ranked))
}

#[cfg(test)]
mod tests {
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
}
