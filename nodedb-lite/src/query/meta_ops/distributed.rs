// SPDX-License-Identifier: Apache-2.0
//! Distributed / Origin-only meta-ops — return `Unsupported` with precise
//! architectural reason so callers know the request can never succeed on Lite.

use nodedb_physical::physical_plan::MetaOp;
use nodedb_types::result::QueryResult;

use crate::error::LiteError;

/// Return an `Unsupported` for any MetaOp that is architecturally Origin-only.
pub fn handle_distributed_op(op: &MetaOp) -> Result<QueryResult, LiteError> {
    let reason = match op {
        MetaOp::WalAppend { .. } => {
            "WalAppend is Origin's WAL-durable Raft-replicated commit path; \
             Lite uses redb transactional commit"
        }
        MetaOp::Cancel { .. } => {
            "Cancel is a distributed transaction coordinator op on Origin; \
             Lite executes synchronously"
        }
        MetaOp::TransactionBatch { .. } => {
            "TransactionBatch is a distributed transaction coordinator op on Origin; \
             Lite executes synchronously"
        }
        MetaOp::CreateTenantSnapshot { .. } => {
            "CreateTenantSnapshot requires Origin's tenant-scoped namespaces; \
             Lite is single-tenant"
        }
        MetaOp::RestoreTenantSnapshot { .. } => {
            "RestoreTenantSnapshot requires Origin's tenant-scoped namespaces; \
             Lite is single-tenant"
        }
        MetaOp::PurgeTenant { .. } => {
            "PurgeTenant requires Origin's tenant-scoped namespaces; \
             Lite is single-tenant"
        }
        MetaOp::CalvinExecuteStatic { .. } => {
            "CalvinExecuteStatic requires Origin's Multi-Raft sequencer; \
             Lite is single-node"
        }
        MetaOp::CalvinExecutePassive { .. } => {
            "CalvinExecutePassive requires Origin's Multi-Raft sequencer; \
             Lite is single-node"
        }
        MetaOp::CalvinExecuteActive { .. } => {
            "CalvinExecuteActive requires Origin's Multi-Raft sequencer; \
             Lite is single-node"
        }
        MetaOp::RawResponse { .. } => {
            "RawResponse is an internal Origin protocol passthrough; \
             not applicable to Lite"
        }
        _ => {
            return Err(LiteError::BadRequest {
                detail: format!("handle_distributed_op called with non-distributed op: {op:?}"),
            });
        }
    };
    Err(LiteError::Unsupported {
        detail: reason.to_owned(),
    })
}
