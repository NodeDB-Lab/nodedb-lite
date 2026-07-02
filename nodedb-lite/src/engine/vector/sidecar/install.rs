// SPDX-License-Identifier: Apache-2.0

//! Per-collection codec sidecar install for Lite.
//!
//! Two entry points:
//!
//! - [`ensure_sidecar`] — driven by `VectorState.per_index_config`.  Given an
//!   `index_key`, looks up the registered quantization, maps it to a
//!   [`CodecName`], then delegates to [`install_sidecar_for_index`].  Returns
//!   `Ok(false)` when no codec is configured (caller skips the encode step).
//!
//! - [`install_sidecar_for_index`] — lower-level entry point used by the
//!   search path (`search.rs`) when a codec is requested at query time.  Lazily
//!   trains a quantization codec from the live vectors already in the HNSW index
//!   and populates `codec_sidecars`.  The operation is idempotent: a second call
//!   for the same `index_key` is a no-op (first-wins).

use std::sync::Arc;

use nodedb_types::VectorQuantization;
use nodedb_vector::rerank::codec::RerankCodec;
use nodedb_vector::rerank::codecs::bbq::DEFAULT_OVERSAMPLE;
use nodedb_vector::rerank::codecs::rabitq::DEFAULT_ROTATION_SEED;
use nodedb_vector::rerank::codecs::{BbqRerank, BinaryRerank, PqRerank, RaBitQRerank, Sq8Rerank};
use nodedb_vector::rerank::{CodecName, CodecSidecar};

use crate::engine::vector::VectorState;
use crate::error::LiteError;
use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

/// Maximum number of sample vectors drawn from the HNSW index for codec
/// training.  Keeping this bounded avoids holding locks for an unbounded
/// amount of work while still giving the codec a representative sample.
const MAX_TRAINING_SAMPLES: usize = 10_000;

/// Map a [`VectorQuantization`] to its corresponding [`CodecName`].
///
/// Returns `None` when `quantization == None` (no sidecar needed) or for
/// variants that have no HNSW-integrated codec path yet (`Ternary`, `Opq`).
fn quant_to_codec_name(quantization: VectorQuantization) -> Option<CodecName> {
    match quantization {
        VectorQuantization::None => None,
        VectorQuantization::Sq8 => Some(CodecName::Sq8),
        VectorQuantization::Pq => Some(CodecName::Pq),
        VectorQuantization::Binary => Some(CodecName::Binary),
        VectorQuantization::RaBitQ => Some(CodecName::RaBitQ),
        VectorQuantization::Bbq => Some(CodecName::Bbq),
        // Ternary and Opq have no HNSW-integrated sidecar path yet.
        VectorQuantization::Ternary | VectorQuantization::Opq => None,
        // Any new variant without an explicit arm is treated as no-sidecar so
        // the compiler forces us to revisit this match when new variants land.
        _ => None,
    }
}

/// Ensure a codec sidecar exists for `index_key` based on the collection's
/// registered [`VectorPrimaryConfig`] in `per_index_config`.
///
/// # Returns
///
/// - `Ok(true)` — a sidecar is now present for `index_key` (either newly
///   created or already existed before this call).
/// - `Ok(false)` — no codec is configured for this `index_key` (quantization
///   is `None`, or the collection has no `per_index_config` entry, or the
///   variant maps to no codec).  The caller should skip the encode step.
/// - `Err(LiteError::BadRequest)` — codec install failed (e.g. unsupported
///   codec variant, training failure, PQ dim constraint).
///
/// The call is idempotent: a pre-existing sidecar is never replaced.
pub(crate) fn ensure_sidecar<S: StorageEngine>(
    vector_state: &VectorState<S>,
    index_key: &str,
) -> Result<bool, LiteError> {
    // 1. Look up the quantization from per_index_config.
    let quantization = {
        let configs = vector_state.per_index_config.lock_or_recover();
        configs.get(index_key).map(|cfg| cfg.quantization)
    };

    let quantization = match quantization {
        None => return Ok(false),
        Some(q) => q,
    };

    // 2. Map quantization → codec name.  None means no sidecar is needed.
    let codec_name = match quant_to_codec_name(quantization) {
        None => {
            if quantization == VectorQuantization::Ternary
                || quantization == VectorQuantization::Opq
            {
                return Err(LiteError::BadRequest {
                    detail: format!(
                        "sidecar install for '{index_key}': quantization {quantization:?} \
                         has no HNSW-integrated codec path on Lite"
                    ),
                });
            }
            return Ok(false);
        }
        Some(name) => name,
    };

    // 3. Check if sidecar already exists — idempotency fast path.
    {
        let sidecars = vector_state.codec_sidecars.lock_or_recover();
        if sidecars.contains_key(index_key) {
            return Ok(true);
        }
    }

    // 4. Delegate to install_sidecar_for_index which gathers live vectors,
    //    trains the codec, and populates codec_sidecars.
    install_sidecar_for_index(vector_state, index_key, codec_name).map(|()| true)
}

/// Install (and populate) a codec sidecar for the HNSW index at `index_key`.
///
/// # Behaviour
///
/// 1. Acquires `codec_sidecars` lock.  If a sidecar already exists for
///    `index_key`, returns `Ok(())` immediately — first-wins, idempotent for
///    racing callers.
/// 2. Looks up the HNSW index.  Returns `LiteError::BadRequest` when missing.
/// 3. Constructs the codec for `codec_name` using the index's dimensionality.
/// 4. Collects up to [`MAX_TRAINING_SAMPLES`] live vectors and calls
///    `codec.train()`.  Returns `LiteError::BadRequest` on training failure.
/// 5. Encodes every live vector into the sidecar via `encode_and_insert`.
///    Returns `LiteError::Query` on encode failure (includes the failing id).
/// 6. Inserts the populated sidecar into `codec_sidecars`.
pub(crate) fn install_sidecar_for_index<S: StorageEngine>(
    vector_state: &VectorState<S>,
    index_key: &str,
    codec_name: CodecName,
) -> Result<(), LiteError> {
    // ── Step 1: idempotency check ─────────────────────────────────────────────
    {
        let sidecars = vector_state.codec_sidecars.lock_or_recover();
        if sidecars.contains_key(index_key) {
            return Ok(());
        }
    }

    // ── Step 2: HNSW lookup ──────────────────────────────────────────────────
    let indices = vector_state.hnsw_indices.lock_or_recover();
    let index = indices
        .get(index_key)
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!("install sidecar: no HNSW index for key '{index_key}'"),
        })?;

    let dim = index.dim();

    // ── Step 3: construct codec ───────────────────────────────────────────────
    enum AnyCodec {
        Sq8(Sq8Rerank),
        Binary(BinaryRerank),
        Pq(PqRerank),
        RaBitQ(RaBitQRerank),
        Bbq(BbqRerank),
    }

    impl AnyCodec {
        fn train(
            &mut self,
            samples: &[&[f32]],
        ) -> Result<(), nodedb_vector::rerank::types::RerankError> {
            match self {
                AnyCodec::Sq8(c) => c.train(samples),
                AnyCodec::Binary(c) => c.train(samples),
                AnyCodec::Pq(c) => c.train(samples),
                AnyCodec::RaBitQ(c) => c.train(samples),
                AnyCodec::Bbq(c) => c.train(samples),
            }
        }

        fn into_arc(self) -> Arc<dyn RerankCodec> {
            match self {
                AnyCodec::Sq8(c) => Arc::new(c),
                AnyCodec::Binary(c) => Arc::new(c),
                AnyCodec::Pq(c) => Arc::new(c),
                AnyCodec::RaBitQ(c) => Arc::new(c),
                AnyCodec::Bbq(c) => Arc::new(c),
            }
        }
    }

    let mut codec = match codec_name {
        CodecName::Sq8 => AnyCodec::Sq8(Sq8Rerank::new(dim)),
        CodecName::Binary => AnyCodec::Binary(BinaryRerank::new(dim)),
        CodecName::Pq => {
            if dim % 8 != 0 {
                return Err(LiteError::BadRequest {
                    detail: format!(
                        "install sidecar: PQ requires dim divisible by 8, got dim={dim}"
                    ),
                });
            }
            AnyCodec::Pq(PqRerank::new(dim, 8, 256))
        }
        CodecName::RaBitQ => AnyCodec::RaBitQ(RaBitQRerank::new(dim, DEFAULT_ROTATION_SEED)),
        CodecName::Bbq => AnyCodec::Bbq(BbqRerank::new(dim, DEFAULT_OVERSAMPLE)),
    };

    // ── Steps 4 & 5: gather samples, train, encode all live vectors ───────────

    // Collect up to MAX_TRAINING_SAMPLES live vectors as owned copies so that
    // training can hold &[&[f32]] without holding the index lock across the
    // (potentially slow) codec training step.
    let total = index.len();
    let sample_cap = MAX_TRAINING_SAMPLES.min(total);

    let mut samples: Vec<Vec<f32>> = Vec::with_capacity(sample_cap);
    for id in 0..total as u32 {
        if !index.is_deleted(id)
            && let Some(v) = index.get_vector(id)
        {
            samples.push(v.to_vec());
            if samples.len() >= sample_cap {
                break;
            }
        }
    }

    let sample_slices: Vec<&[f32]> = samples.iter().map(Vec::as_slice).collect();

    codec
        .train(&sample_slices)
        .map_err(|e| LiteError::BadRequest {
            detail: format!("install sidecar: codec train failed: {e}"),
        })?;

    // Build the sidecar and encode every live vector into it.
    let mut sidecar = CodecSidecar::new(codec.into_arc());

    // `index.len()` is total slots (including deleted); filter via `is_deleted`.
    for id in 0..total as u32 {
        if !index.is_deleted(id)
            && let Some(v) = index.get_vector(id)
        {
            sidecar.encode_and_insert(id, v).map_err(|e| {
                LiteError::Query(format!(
                    "install sidecar: encode_and_insert failed for id={id}: {e}"
                ))
            })?;
        }
    }

    // ── Step 6: insert sidecar ────────────────────────────────────────────────
    // Drop the indices lock before re-acquiring sidecars to maintain lock
    // order (sidecars-first) and avoid deadlock.
    drop(indices);

    let mut sidecars = vector_state.codec_sidecars.lock_or_recover();
    // Second idempotency check: another caller might have raced us.
    sidecars.entry(index_key.to_string()).or_insert(sidecar);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use async_trait::async_trait;
    use nodedb_types::Namespace;
    use nodedb_types::VectorQuantization;
    use nodedb_vector::HnswIndex;

    use nodedb_types::collection_config::VectorPrimaryConfig;

    use crate::engine::vector::state::VectorState;
    use crate::error::LiteError;
    use crate::storage::engine::{KvPair, StorageEngine, WriteOp};

    // ── minimal in-memory StorageEngine stub ──────────────────────────────────

    struct MemStore;

    #[async_trait]
    impl StorageEngine for MemStore {
        async fn get(&self, _ns: Namespace, _key: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
            Ok(None)
        }

        async fn put(&self, _ns: Namespace, _key: &[u8], _value: &[u8]) -> Result<(), LiteError> {
            Ok(())
        }

        async fn delete(&self, _ns: Namespace, _key: &[u8]) -> Result<(), LiteError> {
            Ok(())
        }

        async fn scan_prefix(
            &self,
            _ns: Namespace,
            _prefix: &[u8],
        ) -> Result<Vec<KvPair>, LiteError> {
            Ok(Vec::new())
        }

        async fn batch_write(&self, _ops: &[WriteOp]) -> Result<(), LiteError> {
            Ok(())
        }

        async fn count(&self, _ns: Namespace) -> Result<u64, LiteError> {
            Ok(0)
        }

        async fn scan_range(
            &self,
            _ns: Namespace,
            _start: &[u8],
            _limit: usize,
        ) -> Result<Vec<KvPair>, LiteError> {
            Ok(Vec::new())
        }

        async fn scan_range_bounded(
            &self,
            _ns: Namespace,
            _start: Option<&[u8]>,
            _end: Option<&[u8]>,
            _limit: Option<usize>,
        ) -> Result<Vec<KvPair>, LiteError> {
            Ok(Vec::new())
        }
    }

    fn make_state() -> VectorState<MemStore> {
        VectorState::new(Arc::new(MemStore), 50)
    }

    fn populate_index(state: &VectorState<MemStore>, index_key: &str, dim: usize, n: usize) {
        let mut indices = state.hnsw_indices.lock_or_recover();
        let index = indices
            .entry(index_key.to_string())
            .or_insert_with(|| HnswIndex::new(dim, Default::default()));
        for i in 0..n {
            let v: Vec<f32> = (0..dim).map(|j| (i * dim + j) as f32 * 0.01).collect();
            index.insert(v).expect("insert should not fail in tests");
        }
    }

    fn register_config(
        state: &VectorState<MemStore>,
        index_key: &str,
        quantization: VectorQuantization,
    ) {
        let cfg = VectorPrimaryConfig {
            quantization,
            ..Default::default()
        };
        state
            .per_index_config
            .lock_or_recover()
            .insert(index_key.to_string(), cfg);
    }

    #[test]
    fn install_sq8_then_sidecar_populated() {
        let state = make_state();
        let key = "col_sq8";
        populate_index(&state, key, 16, 10);

        install_sidecar_for_index(&state, key, CodecName::Sq8).expect("sq8 install should succeed");

        let sidecars = state.codec_sidecars.lock_or_recover();
        let sidecar = sidecars.get(key).expect("sidecar must exist after install");
        assert_eq!(sidecar.len(), 10, "all 10 vectors must be encoded");
        assert_eq!(sidecar.codec_name(), CodecName::Sq8);
    }

    #[test]
    fn install_idempotent() {
        let state = make_state();
        let key = "col_idem";
        populate_index(&state, key, 16, 8);

        install_sidecar_for_index(&state, key, CodecName::Sq8).expect("first install ok");
        install_sidecar_for_index(&state, key, CodecName::Sq8).expect("second install ok (no-op)");

        let sidecars = state.codec_sidecars.lock_or_recover();
        assert!(sidecars.contains_key(key));
        assert_eq!(sidecars.get(key).unwrap().len(), 8);
    }

    #[test]
    fn install_missing_index_key_returns_bad_request() {
        let state = make_state();
        let err = install_sidecar_for_index(&state, "nonexistent", CodecName::Binary)
            .expect_err("should fail for missing index");
        assert!(
            matches!(err, LiteError::BadRequest { .. }),
            "expected BadRequest, got {err:?}"
        );
        assert!(err.to_string().contains("no HNSW index"));
    }

    #[test]
    fn install_pq_indivisible_dim_returns_bad_request() {
        let state = make_state();
        let key = "col_pq_bad_dim";
        populate_index(&state, key, 33, 8);
        let err = install_sidecar_for_index(&state, key, CodecName::Pq)
            .expect_err("PQ with dim=33 should fail");
        assert!(
            matches!(err, LiteError::BadRequest { .. }),
            "expected BadRequest, got {err:?}"
        );
        assert!(err.to_string().contains("divisible by 8"));
    }

    #[test]
    fn ensure_sidecar_no_config_returns_false() {
        let state = make_state();
        let result =
            ensure_sidecar(&state, "no_config_col").expect("should not err when no config");
        assert!(
            !result,
            "expected Ok(false) when per_index_config has no entry"
        );
        assert!(
            state.codec_sidecars.lock_or_recover().is_empty(),
            "no sidecar should be created"
        );
    }

    #[test]
    fn ensure_sidecar_quantization_none_returns_false() {
        let state = make_state();
        register_config(&state, "col_none_quant", VectorQuantization::None);
        let result =
            ensure_sidecar(&state, "col_none_quant").expect("should not err for None quantization");
        assert!(!result, "expected Ok(false) for None quantization");
        assert!(state.codec_sidecars.lock_or_recover().is_empty());
    }

    #[test]
    fn ensure_sidecar_sq8_creates_sidecar() {
        let state = make_state();
        let key = "col_sq8_ensure";
        populate_index(&state, key, 16, 5);
        register_config(&state, key, VectorQuantization::Sq8);

        let result = ensure_sidecar(&state, key).expect("sq8 ensure_sidecar should succeed");
        assert!(result, "expected Ok(true) after sidecar creation");
        assert!(
            state.codec_sidecars.lock_or_recover().contains_key(key),
            "sidecar must be present in codec_sidecars"
        );
    }

    #[test]
    fn ensure_sidecar_idempotent() {
        let state = make_state();
        let key = "col_ensure_idem";
        populate_index(&state, key, 16, 4);
        register_config(&state, key, VectorQuantization::Binary);

        let r1 = ensure_sidecar(&state, key).expect("first call ok");
        assert!(r1);
        let len_after_first = state
            .codec_sidecars
            .lock_or_recover()
            .get(key)
            .unwrap()
            .len();

        let r2 = ensure_sidecar(&state, key).expect("second call ok");
        assert!(r2, "second call must still return Ok(true)");
        let len_after_second = state
            .codec_sidecars
            .lock_or_recover()
            .get(key)
            .unwrap()
            .len();
        assert_eq!(
            len_after_first, len_after_second,
            "sidecar must not be replaced on second call"
        );
    }

    #[test]
    fn ensure_sidecar_ternary_returns_bad_request() {
        let state = make_state();
        let key = "col_ternary";
        populate_index(&state, key, 16, 4);
        register_config(&state, key, VectorQuantization::Ternary);

        let err = ensure_sidecar(&state, key).expect_err("Ternary should return Err");
        assert!(
            matches!(err, LiteError::BadRequest { .. }),
            "expected BadRequest, got {err:?}"
        );
        assert!(
            err.to_string().to_lowercase().contains("ternary"),
            "error message should mention 'ternary', got: {err}"
        );
    }
}
