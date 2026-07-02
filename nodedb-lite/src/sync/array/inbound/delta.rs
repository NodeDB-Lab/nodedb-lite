//! Delta and delta-batch handlers — single-op and batched op application.

use nodedb_array::sync::op_codec;
use nodedb_types::sync::wire::array::{ArrayDeltaBatchMsg, ArrayDeltaMsg};

use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

use super::dispatcher::{ArrayInbound, map_apply_outcome};
use super::outcome::InboundOutcome;

impl<S: StorageEngine> ArrayInbound<S> {
    /// Apply a single delta message from Origin.
    ///
    /// Decodes the op payload, observes the HLC, then delegates to
    /// [`nodedb_array::sync::apply::apply_op`].
    pub fn handle_delta(&self, msg: &ArrayDeltaMsg) -> Result<InboundOutcome, LiteError> {
        let op = op_codec::decode_op(&msg.op_payload).map_err(|e| LiteError::Storage {
            detail: format!("handle_delta decode: {e}"),
        })?;

        self.replica.observe(op.header.hlc)?;

        let outcome = self.apply_single_op(&op)?;
        Ok(map_apply_outcome(outcome))
    }

    /// Apply a batch of delta messages from Origin.
    ///
    /// Returns one [`InboundOutcome`] per op payload in the batch.
    pub fn handle_delta_batch(
        &self,
        msg: &ArrayDeltaBatchMsg,
    ) -> Result<Vec<InboundOutcome>, LiteError> {
        let mut outcomes = Vec::with_capacity(msg.op_payloads.len());
        for payload in &msg.op_payloads {
            let op = op_codec::decode_op(payload).map_err(|e| LiteError::Storage {
                detail: format!("handle_delta_batch decode: {e}"),
            })?;
            self.replica.observe(op.header.hlc)?;
            let outcome = self.apply_single_op(&op)?;
            outcomes.push(map_apply_outcome(outcome));
        }
        Ok(outcomes)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_array::sync::apply::ApplyRejection;
    use nodedb_array::sync::hlc::Hlc;
    use nodedb_array::sync::op::{ArrayOp, ArrayOpHeader, ArrayOpKind};
    use nodedb_array::sync::op_codec;
    use nodedb_array::sync::replica_id::ReplicaId;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_types::sync::wire::array::{ArrayDeltaBatchMsg, ArrayDeltaMsg};

    use super::super::fixtures::{hlc, make_inbound, put_op, simple_schema};
    use super::super::outcome::InboundOutcome;

    #[tokio::test(flavor = "multi_thread")]
    async fn handle_delta_applies_put() {
        let (inbound, schemas, _pending, storage) = make_inbound().await;

        // Register schema in both the schemas registry AND the engine's catalog.
        schemas
            .put_schema("arr", &simple_schema("arr"))
            .await
            .unwrap();
        {
            let mut state = inbound.engine.array_state.lock().await;
            state
                .create_array(&storage, "arr", simple_schema("arr"))
                .await
                .unwrap();
        }
        let schema_hlc = schemas.schema_hlc("arr").unwrap();

        let op = ArrayOp {
            header: ArrayOpHeader {
                array: "arr".into(),
                hlc: hlc(100),
                schema_hlc,
                valid_from_ms: 0,
                valid_until_ms: -1,
                system_from_ms: 100,
            },
            kind: ArrayOpKind::Put,
            coord: vec![CoordValue::Int64(3)],
            attrs: Some(vec![CellValue::Float64(42.0)]),
        };

        let payload = op_codec::encode_op(&op).unwrap();
        let msg = ArrayDeltaMsg {
            array: "arr".into(),
            op_payload: payload,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };
        let outcome = inbound.handle_delta(&msg).unwrap();
        assert_eq!(outcome, InboundOutcome::Applied);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn handle_delta_idempotent() {
        let (inbound, schemas, _pending, storage) = make_inbound().await;
        schemas
            .put_schema("arr", &simple_schema("arr"))
            .await
            .unwrap();
        {
            let mut state = inbound.engine.array_state.lock().await;
            state
                .create_array(&storage, "arr", simple_schema("arr"))
                .await
                .unwrap();
        }
        let schema_hlc = schemas.schema_hlc("arr").unwrap();

        let op = ArrayOp {
            header: ArrayOpHeader {
                array: "arr".into(),
                hlc: hlc(200),
                schema_hlc,
                valid_from_ms: 0,
                valid_until_ms: -1,
                system_from_ms: 200,
            },
            kind: ArrayOpKind::Put,
            coord: vec![CoordValue::Int64(1)],
            attrs: Some(vec![CellValue::Float64(1.0)]),
        };
        let payload = op_codec::encode_op(&op).unwrap();
        let msg = ArrayDeltaMsg {
            array: "arr".into(),
            op_payload: payload.clone(),
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };

        // First application — should be Applied.
        let o1 = inbound.handle_delta(&msg).unwrap();
        assert_eq!(o1, InboundOutcome::Applied);

        // Second application of identical op — must be Idempotent.
        let msg2 = ArrayDeltaMsg {
            array: "arr".into(),
            op_payload: payload,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };
        let o2 = inbound.handle_delta(&msg2).unwrap();
        assert_eq!(o2, InboundOutcome::Idempotent);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn handle_delta_unknown_array_returns_rejected() {
        let (inbound, _schemas, _pending, _storage) = make_inbound().await;
        // No array registered → ApplyRejection::ArrayUnknown.
        let op = put_op("unknown_arr", 50, 50);
        let payload = op_codec::encode_op(&op).unwrap();
        let msg = ArrayDeltaMsg {
            array: "unknown_arr".into(),
            op_payload: payload,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };
        let outcome = inbound.handle_delta(&msg).unwrap();
        assert!(
            matches!(
                outcome,
                InboundOutcome::Rejected(ApplyRejection::ArrayUnknown { .. })
            ),
            "expected ArrayUnknown rejection, got: {outcome:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn handle_delta_schema_too_new_returns_rejected() {
        let (inbound, schemas, _pending, storage) = make_inbound().await;
        schemas
            .put_schema("arr", &simple_schema("arr"))
            .await
            .unwrap();
        {
            let mut state = inbound.engine.array_state.lock().await;
            state
                .create_array(&storage, "arr", simple_schema("arr"))
                .await
                .unwrap();
        }
        // Local schema_hlc is hlc(X); op carries schema_hlc far in the future.
        let schema_hlc_future = Hlc::new(u64::MAX >> 16, 0, ReplicaId::new(99)).unwrap();
        let op = ArrayOp {
            header: ArrayOpHeader {
                array: "arr".into(),
                hlc: hlc(10),
                schema_hlc: schema_hlc_future,
                valid_from_ms: 0,
                valid_until_ms: -1,
                system_from_ms: 10,
            },
            kind: ArrayOpKind::Put,
            coord: vec![CoordValue::Int64(0)],
            attrs: Some(vec![CellValue::Float64(0.0)]),
        };
        let payload = op_codec::encode_op(&op).unwrap();
        let msg = ArrayDeltaMsg {
            array: "arr".into(),
            op_payload: payload,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };
        let outcome = inbound.handle_delta(&msg).unwrap();
        assert!(
            matches!(
                outcome,
                InboundOutcome::Rejected(ApplyRejection::SchemaTooNew { .. })
            ),
            "expected SchemaTooNew, got: {outcome:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn handle_delta_batch_processes_all() {
        let (inbound, schemas, _pending, storage) = make_inbound().await;
        schemas.put_schema("b", &simple_schema("b")).await.unwrap();
        {
            let mut state = inbound.engine.array_state.lock().await;
            state
                .create_array(&storage, "b", simple_schema("b"))
                .await
                .unwrap();
        }
        let schema_hlc = schemas.schema_hlc("b").unwrap();

        let ops: Vec<ArrayOp> = (1u64..=4)
            .map(|i| ArrayOp {
                header: ArrayOpHeader {
                    array: "b".into(),
                    hlc: hlc(i * 100),
                    schema_hlc,
                    valid_from_ms: 0,
                    valid_until_ms: -1,
                    system_from_ms: (i * 100) as i64,
                },
                kind: ArrayOpKind::Put,
                coord: vec![CoordValue::Int64(i as i64)],
                attrs: Some(vec![CellValue::Float64(i as f64)]),
            })
            .collect();

        let op_payloads: Vec<Vec<u8>> = ops
            .iter()
            .map(|op| op_codec::encode_op(op).unwrap())
            .collect();

        let msg = ArrayDeltaBatchMsg {
            array: "b".into(),
            op_payloads,
        };
        let outcomes = inbound.handle_delta_batch(&msg).unwrap();
        assert_eq!(outcomes.len(), 4);
        assert!(outcomes.iter().all(|o| *o == InboundOutcome::Applied));
    }
}
