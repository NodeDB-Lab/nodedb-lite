//! Conflict resolution policy execution for rejected CRDT deltas.
//!
//! When Origin rejects a delta (e.g., UNIQUE violation), the policy
//! registry determines the appropriate local action: auto-rename,
//! defer for retry, escalate to DLQ, or overwrite.

use nodedb_crdt::{ConflictPolicy, PolicyResolution, ResolvedAction};
use nodedb_types::sync::compensation::CompensationHint;

use super::engine::CrdtEngine;

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
                    resolved.unwrap_or(PolicyResolution::Escalate)
                }
                ConflictPolicy::CascadeDefer { max_retries, .. } => PolicyResolution::Deferred {
                    retry_after_ms: 1000,
                    attempt: 1.min(*max_retries),
                },
                ConflictPolicy::EscalateToDlq => PolicyResolution::Escalate,
                ConflictPolicy::Custom { .. } => PolicyResolution::Escalate,
            },
            CompensationHint::ForeignKeyMissing { .. } => match &policy.foreign_key {
                ConflictPolicy::CascadeDefer {
                    max_retries,
                    ttl_secs,
                } => PolicyResolution::Deferred {
                    retry_after_ms: (*ttl_secs * 1000 / (*max_retries).max(1) as u64).max(1000),
                    attempt: 1,
                },
                ConflictPolicy::LastWriterWins => {
                    PolicyResolution::AutoResolved(ResolvedAction::OverwriteExisting)
                }
                _ => PolicyResolution::Escalate,
            },
            CompensationHint::IntegrityViolation => {
                let _ = self.state.delete(&collection, &doc_id);
                self.pending_deltas.remove(pos);
                return Some(PolicyResolution::Escalate);
            }
            _ => PolicyResolution::Escalate,
        };

        match &resolution {
            PolicyResolution::Escalate => {
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
