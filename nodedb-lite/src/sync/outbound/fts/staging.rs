// SPDX-License-Identifier: Apache-2.0

//! Atomic staging admission and synchronous front ownership transitions.

use super::{FtsOutbound, PendingFtsDelete, PendingFtsIndex};
use crate::{
    error::LiteError, nodedb::lock_ext::LockExt, storage::engine::StorageEngine,
    sync::outbound::durable_queue::DurableOutboundQueue,
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

const STAGING_CAP: usize = 4096;

struct Entry<T> {
    payload: Arc<T>,
    key: Option<[u8; 8]>,
}

pub(super) type ReservedFront<T> = ([u8; 8], Arc<T>);

pub(super) struct Staging<T> {
    entries: Mutex<VecDeque<Entry<T>>>,
}

impl<T> Staging<T> {
    pub(super) fn new() -> Self {
        Self {
            entries: Mutex::new(VecDeque::new()),
        }
    }

    fn admit(&self, build: impl FnOnce() -> Result<T, LiteError>) -> Result<(), LiteError> {
        let mut entries = self.entries.lock_or_recover();
        if entries.len() >= STAGING_CAP {
            return Err(LiteError::Backpressure {
                detail: format!(
                    "FTS outbound staging reached {STAGING_CAP} entries: flush or acknowledge pending sync before retrying"
                ),
            });
        }
        entries.push_back(Entry {
            payload: Arc::new(build()?),
            key: None,
        });
        Ok(())
    }

    pub(super) fn front_reserved<S: StorageEngine>(
        &self,
        durable: &DurableOutboundQueue<S>,
    ) -> Result<Option<ReservedFront<T>>, LiteError> {
        let mut entries = self.entries.lock_or_recover();
        let Some(entry) = entries.front_mut() else {
            return Ok(None);
        };
        let key = match entry.key {
            Some(key) => key,
            None => {
                let key = durable.reserve_key()?;
                entry.key = Some(key);
                key
            }
        };
        Ok(Some((key, Arc::clone(&entry.payload))))
    }

    pub(super) fn remove_front(&self, key: [u8; 8]) -> Result<(), LiteError> {
        let mut entries = self.entries.lock_or_recover();
        if entries.front().is_none_or(|entry| entry.key != Some(key)) {
            return Err(LiteError::Serialization {
                detail: format!(
                    "FTS staged front differs from durable key {key:?}: retry after inspecting outbound ownership"
                ),
            });
        }
        entries.pop_front();
        Ok(())
    }

    pub(super) fn reserved_keys(&self) -> Vec<[u8; 8]> {
        self.entries
            .lock_or_recover()
            .iter()
            .filter_map(|entry| entry.key)
            .collect()
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.lock_or_recover().len()
    }
}

impl<S: StorageEngine> FtsOutbound<S> {
    fn next_batch_id(&self) -> Result<u64, LiteError> {
        crate::sync::outbound::queue::take_next(&self.ids).ok_or_else(|| LiteError::Backpressure {
            detail: "FTS batch counter exhausted: drain and acknowledge pending entries before reopening the queue".into(),
        })
    }

    /// Stage an index entry atomically. Local source mutation can precede this error.
    pub fn stage_index(
        &self,
        collection: &str,
        doc_id: &str,
        fields: Vec<(String, String)>,
    ) -> Result<(), LiteError> {
        self.staging_indexes
            .admit(|| {
                Ok(PendingFtsIndex {
                    batch_id: self.next_batch_id()?,
                    collection: collection.into(),
                    doc_id: doc_id.into(),
                    fields,
                    seq: 0,
                })
            })
            .map_err(|error| staging_error("stage_index", collection, doc_id, error))
    }

    /// Stage a delete entry atomically. Unflushed staging remains volatile.
    pub fn stage_delete(&self, collection: &str, doc_id: &str) -> Result<(), LiteError> {
        self.staging_deletes
            .admit(|| {
                Ok(PendingFtsDelete {
                    batch_id: self.next_batch_id()?,
                    collection: collection.into(),
                    doc_id: doc_id.into(),
                    seq: 0,
                })
            })
            .map_err(|error| staging_error("stage_delete", collection, doc_id, error))
    }
}

fn staging_error(operation: &str, collection: &str, doc_id: &str, error: LiteError) -> LiteError {
    match error {
        LiteError::Backpressure { detail } => LiteError::Backpressure {
            detail: format!(
                "FTS outbound {operation} collection '{collection}' doc_id '{doc_id}': {detail}"
            ),
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_admission_never_exceeds_staging_capacity() {
        let staging = Arc::new(Staging::<usize>::new());
        let admitted = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|worker| {
                    let staging = Arc::clone(&staging);
                    scope.spawn(move || {
                        (0..1024)
                            .filter(|value| staging.admit(|| Ok(worker * 1024 + value)).is_ok())
                            .count()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .sum::<usize>()
        });
        assert_eq!(admitted, STAGING_CAP);
        assert_eq!(staging.len(), STAGING_CAP);
        assert!(matches!(
            staging.admit(|| Ok(9999)),
            Err(LiteError::Backpressure { .. })
        ));
    }
}
