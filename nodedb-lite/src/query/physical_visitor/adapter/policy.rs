// SPDX-License-Identifier: Apache-2.0
//! Plan-policy slots that Lite does not execute.
//!
//! Origin attaches row-level-security programs and RETURNING projections to
//! write ops. Lite enforces neither. Executing the write while dropping the
//! slot would apply an unpoliced write, so every dispatch arm that can carry
//! one rejects it here instead.

use nodedb_physical::physical_plan::ReturningSpec;
use nodedb_types::RlsWriteCheck;

use crate::error::LiteError;

/// Reject a plan carrying a policy slot Lite cannot honour.
///
/// `rls_filters` holds the read-side RLS programs. `write_check` carries the
/// write gate's decision. `op` names the variant, so the message points at the
/// statement that produced it.
pub(super) fn deny_policy(
    op: &str,
    returning: Option<&ReturningSpec>,
    rls_filters: &[&[u8]],
    write_check: &RlsWriteCheck,
) -> Result<(), LiteError> {
    if !rls_filters.iter().all(|program| program.is_empty()) || write_gate_applies(write_check) {
        return Err(LiteError::Unsupported {
            detail: format!(
                "{op}: row-level security is enforced by the Origin data plane \
                 and has no equivalent on the single-node Lite engine"
            ),
        });
    }
    if returning.is_some() {
        return Err(LiteError::Unsupported {
            detail: format!("{op}: RETURNING is unsupported on the Lite engine"),
        });
    }
    Ok(())
}

/// Report whether the write gate carries a decision Lite cannot honour.
///
/// The match is exhaustive on purpose. A new variant must break this build
/// and force a decision, because defaulting an unknown gate state to "admit"
/// silently disables the policy.
fn write_gate_applies(check: &RlsWriteCheck) -> bool {
    match check {
        // A real policy the Lite engine has no way to evaluate.
        RlsWriteCheck::Predicate(_) => true,
        // Injection never ran. Its own contract is to deny at the gate, so a
        // missed injection fails loudly instead of admitting the write.
        RlsWriteCheck::PendingInjection => true,
        // No policy attaches here, so there is nothing for Lite to enforce.
        RlsWriteCheck::NoPolicyApplies | RlsWriteCheck::SystemInternalCollection => false,
        // The decision already happened against this exact row image. Redeciding
        // here can only diverge from what was committed.
        RlsWriteCheck::AlreadyDecidedElsewhere | RlsWriteCheck::DecidedEarlierInRequest => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_write_predicate_is_refused() {
        let err = deny_policy(
            "KvOp::Insert",
            None,
            &[],
            &RlsWriteCheck::from_injected(vec![1, 2, 3]),
        )
        .expect_err("a policy Lite cannot evaluate must not admit the write");
        assert!(matches!(err, LiteError::Unsupported { .. }));
    }

    #[test]
    fn pending_injection_is_refused() {
        // The gate contract: a check that never ran denies, so a missed
        // injection cannot quietly disable the policy.
        let err = deny_policy(
            "KvOp::Insert",
            None,
            &[],
            &RlsWriteCheck::pending_injection(),
        )
        .expect_err("an un-run injection must deny");
        assert!(matches!(err, LiteError::Unsupported { .. }));
    }

    #[test]
    fn an_unpoliced_write_is_admitted() {
        deny_policy("KvOp::Insert", None, &[], &RlsWriteCheck::NoPolicyApplies)
            .expect("no policy applies, so nothing blocks the write");
        deny_policy(
            "KvOp::Insert",
            None,
            &[],
            &RlsWriteCheck::system_internal_collection(),
        )
        .expect("a collection no policy attaches to is admitted");
        deny_policy(
            "KvOp::Insert",
            None,
            &[],
            &RlsWriteCheck::already_decided_elsewhere(),
        )
        .expect("replay carries a decision already made");
    }

    #[test]
    fn a_read_filter_is_refused() {
        let err = deny_policy(
            "KvOp::Scan",
            None,
            &[&[1u8, 2, 3]],
            &RlsWriteCheck::NoPolicyApplies,
        )
        .expect_err("a read-side program must not be dropped");
        assert!(matches!(err, LiteError::Unsupported { .. }));
    }
}
