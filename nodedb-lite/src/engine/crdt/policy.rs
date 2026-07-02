//! Conflict resolution policy execution for rejected CRDT deltas.
//!
//! When Origin rejects a delta (e.g., UNIQUE violation), the policy
//! registry determines the appropriate local action: auto-rename,
//! defer for retry, escalate to DLQ, or overwrite.

use nodedb_crdt::validator::Violation;
use nodedb_crdt::{ConflictPolicy, PolicyResolution, ResolvedAction};
use nodedb_types::sync::compensation::CompensationHint;

use super::engine::CrdtEngine;

/// Map the wire-level rejection reason (`nodedb_types::sync::compensation::
/// CompensationHint`, sent from Origin to edge) to the CRDT validator's own
/// remediation-hint type (`nodedb_crdt::CompensationHint`, i.e.
/// `nodedb_crdt::dead_letter::CompensationHint`). The two enums model
/// different concerns — one is "why was this rejected", the other is "what
/// should the caller do about it" — so there is no lossless 1:1 variant
/// correspondence. Each arm below picks the closest remediation action,
/// mirroring the mapping `nodedb_crdt::constraint_checks` itself uses
/// (`RetryWithDifferentValue` for UNIQUE, `CreateReferencedRow` for FK,
/// `ProvideRequiredField` for NOT NULL); anything that doesn't fit the
/// target shape is folded into `reason`/`detail` text instead of dropped.
fn to_crdt_hint(hint: &CompensationHint) -> nodedb_crdt::CompensationHint {
    match hint {
        CompensationHint::UniqueViolation {
            field,
            conflicting_value,
        } => nodedb_crdt::CompensationHint::RetryWithDifferentValue {
            field: field.clone(),
            conflicting_value: conflicting_value.clone(),
            suggestion: format!("{conflicting_value}_1"),
        },
        CompensationHint::ForeignKeyMissing { referenced_id } => {
            nodedb_crdt::CompensationHint::CreateReferencedRow {
                // The wire hint carries only the missing id, not the
                // referenced collection name — left empty rather than guessed.
                ref_collection: String::new(),
                ref_key: referenced_id.clone(),
                missing_value: referenced_id.clone(),
            }
        }
        CompensationHint::PermissionDenied => nodedb_crdt::CompensationHint::ManualIntervention {
            reason: "permission denied by Origin".to_string(),
        },
        CompensationHint::RateLimited { retry_after_ms } => {
            nodedb_crdt::CompensationHint::ManualIntervention {
                reason: format!("rate limited; retry after {retry_after_ms}ms"),
            }
        }
        CompensationHint::SchemaViolation { field, reason } => {
            nodedb_crdt::CompensationHint::ManualIntervention {
                reason: format!("schema violation on field `{field}`: {reason}"),
            }
        }
        CompensationHint::Custom { constraint, detail } => {
            nodedb_crdt::CompensationHint::ManualIntervention {
                reason: format!("{constraint}: {detail}"),
            }
        }
        CompensationHint::IntegrityViolation => nodedb_crdt::CompensationHint::ManualIntervention {
            reason: "delta integrity check failed (CRC32C mismatch)".to_string(),
        },
        CompensationHint::Retry { retry_after_ms } => {
            nodedb_crdt::CompensationHint::ManualIntervention {
                reason: format!("transient rejection; retry after {retry_after_ms}ms"),
            }
        }
        // `#[non_exhaustive]` on the wire enum: fold any future variant into
        // the generic manual-intervention bucket rather than failing to compile.
        _ => nodedb_crdt::CompensationHint::ManualIntervention {
            reason: hint.to_string(),
        },
    }
}

/// Build the single-violation list carried by `PolicyResolution` variants
/// that report `violations`. Lite only ever validates one delta at a time
/// (one `CompensationHint` per rejection), so the list always has length 1.
fn violation_from_hint(hint: &CompensationHint) -> Vec<Violation> {
    vec![Violation {
        constraint_name: format!("{hint:?}"),
        reason: hint.to_string(),
        hint: to_crdt_hint(hint),
    }]
}

impl CrdtEngine {
    /// Reject a delta using the registered conflict resolution policy.
    ///
    /// Instead of blindly deleting the document, consults the `PolicyRegistry`
    /// to determine the appropriate action based on the `CompensationHint`:
    ///
    /// - **UniqueViolation + RenameSuffix policy** → auto-rename field, re-upsert
    /// - **ForeignKey + CascadeDefer policy** → return Deferred (caller should retry)
    /// - **EscalateToDlq** → return Escalate (caller routes to DLQ)
    /// - **LastWriterWins** → accept the incoming write (overwrite)
    /// - **IntegrityViolation** → always delete (data corruption, no auto-resolve)
    ///
    /// Returns the `PolicyResolution` so the caller knows what action was taken.
    pub fn reject_delta_with_policy(
        &mut self,
        mutation_id: u64,
        hint: &CompensationHint,
    ) -> Option<PolicyResolution> {
        let pos = self
            .pending_deltas
            .iter()
            .position(|d| d.mutation_id == mutation_id)?;

        let delta = &self.pending_deltas[pos];
        let collection = delta.collection.clone();
        let doc_id = delta.document_id.clone();

        let policy = self.policies.get_owned(&collection);

        let resolution = match hint {
            CompensationHint::UniqueViolation { field, .. } => match &policy.unique {
                ConflictPolicy::LastWriterWins => {
                    PolicyResolution::AutoResolved(ResolvedAction::OverwriteExisting)
                }
                ConflictPolicy::RenameSuffix => {
                    let resolved = (|| {
                        let loro_val = self.state.read_row(&collection, &doc_id)?;
                        let loro::LoroValue::Map(map) = &loro_val else {
                            return None;
                        };
                        let current_val = map.get(field.as_str())?;
                        let val_str = match current_val {
                            loro::LoroValue::String(s) => s.to_string(),
                            loro::LoroValue::I64(n) => n.to_string(),
                            loro::LoroValue::Double(n) => n.to_string(),
                            other => format!("{other:?}"),
                        };
                        let new_val = format!("{val_str}_1");
                        self.state
                            .upsert(
                                &collection,
                                &doc_id,
                                &[(
                                    field.as_str(),
                                    loro::LoroValue::String(new_val.clone().into()),
                                )],
                            )
                            .ok()?;
                        Some(PolicyResolution::AutoResolved(
                            ResolvedAction::RenamedField {
                                field: field.clone(),
                                new_value: new_val,
                            },
                        ))
                    })();
                    resolved.unwrap_or(PolicyResolution::Escalate {
                        violations: violation_from_hint(hint),
                    })
                }
                ConflictPolicy::CascadeDefer { max_retries, .. } => PolicyResolution::Deferred {
                    retry_after_ms: 1000,
                    attempt: 1.min(*max_retries),
                    violations: violation_from_hint(hint),
                },
                ConflictPolicy::EscalateToDlq => PolicyResolution::Escalate {
                    violations: violation_from_hint(hint),
                },
                ConflictPolicy::Custom { .. } => PolicyResolution::Escalate {
                    violations: violation_from_hint(hint),
                },
            },
            CompensationHint::ForeignKeyMissing { .. } => match &policy.foreign_key {
                ConflictPolicy::CascadeDefer {
                    max_retries,
                    ttl_secs,
                } => PolicyResolution::Deferred {
                    retry_after_ms: (*ttl_secs * 1000 / (*max_retries).max(1) as u64).max(1000),
                    attempt: 1,
                    violations: violation_from_hint(hint),
                },
                ConflictPolicy::LastWriterWins => {
                    PolicyResolution::AutoResolved(ResolvedAction::OverwriteExisting)
                }
                ConflictPolicy::RenameSuffix
                | ConflictPolicy::Custom { .. }
                | ConflictPolicy::EscalateToDlq => PolicyResolution::Escalate {
                    violations: violation_from_hint(hint),
                },
            },
            CompensationHint::IntegrityViolation => {
                let _ = self.state.delete(&collection, &doc_id);
                self.pending_deltas.remove(pos);
                return Some(PolicyResolution::Escalate {
                    violations: violation_from_hint(hint),
                });
            }
            _ => PolicyResolution::Escalate {
                violations: violation_from_hint(hint),
            },
        };

        match &resolution {
            PolicyResolution::Escalate { .. } => {
                let _ = self.state.delete(&collection, &doc_id);
                self.pending_deltas.remove(pos);
            }
            PolicyResolution::AutoResolved(_) => {
                self.pending_deltas.remove(pos);
            }
            PolicyResolution::Deferred { .. } | PolicyResolution::WebhookRequired { .. } => {}
        }

        Some(resolution)
    }
}
