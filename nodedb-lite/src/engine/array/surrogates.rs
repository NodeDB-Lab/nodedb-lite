//! Per-coordinate surrogate bindings for the Lite array engine.
//!
//! Lite binds one global surrogate per `(array, coordinate)` on the first
//! write. Overwrites, deletes, and erasures keep the binding. A put after a
//! delete reuses it. One high-water mark covers every array, so a surrogate
//! names one cell across the whole database.
//!
//! Key layout in `Namespace::Array`:
//! - `surrogate_hwm` -> u32 LE next free surrogate (at least 1).
//! - `surrogate\0{array}\0{zerompk(coord)}` -> u32 LE bound surrogate.
//!
//! The NUL after the array name stops a scan for array `a` at array `a:b`.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_types::{Namespace, Surrogate};

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};

const HWM_KEY: &[u8] = b"surrogate_hwm";
const BINDING_TAG: &[u8] = b"surrogate\0";

/// The first surrogate Lite assigns. `Surrogate::ZERO` names no row.
pub(crate) const FIRST_SURROGATE: u32 = 1;

fn binding_prefix(array: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(BINDING_TAG.len() + array.len() + 1);
    k.extend_from_slice(BINDING_TAG);
    k.extend_from_slice(array.as_bytes());
    k.push(0);
    k
}

fn binding_key(array: &str, coord_key: &[u8]) -> Vec<u8> {
    let mut k = binding_prefix(array);
    k.extend_from_slice(coord_key);
    k
}

fn decode_u32(value: &[u8], what: &str) -> Result<u32, LiteError> {
    let bytes: [u8; 4] = value.try_into().map_err(|_| LiteError::Storage {
        detail: format!(
            "{what} holds {} bytes, expected a 4-byte u32: the array surrogate store is corrupt",
            value.len()
        ),
    })?;
    Ok(u32::from_le_bytes(bytes))
}

/// Coordinate-to-surrogate bindings for one array.
#[derive(Debug, Default)]
pub(crate) struct CellBindings {
    /// zerompk coordinate key -> bound surrogate.
    bound: HashMap<Vec<u8>, Surrogate>,
    /// Coordinate keys bound since the last persisted flush.
    dirty: Vec<Vec<u8>>,
}

impl CellBindings {
    /// Read every persisted binding of `array`.
    pub(crate) async fn load<S: StorageEngine>(
        storage: &Arc<S>,
        array: &str,
    ) -> Result<Self, LiteError> {
        let prefix = binding_prefix(array);
        let rows = storage.scan_prefix(Namespace::Array, &prefix).await?;
        let mut bound = HashMap::with_capacity(rows.len());
        for (key, value) in rows {
            let raw = decode_u32(&value, &format!("surrogate binding of array '{array}'"))?;
            if raw == Surrogate::ZERO.as_u32() {
                return Err(LiteError::Storage {
                    detail: format!(
                        "array '{array}' holds a binding to Surrogate::ZERO, which names no row: \
                         the array surrogate store is corrupt"
                    ),
                });
            }
            let coord_key = key.get(prefix.len()..).unwrap_or_default().to_vec();
            bound.insert(coord_key, Surrogate::new(raw));
        }
        Ok(Self {
            bound,
            dirty: Vec::new(),
        })
    }

    /// The largest bound surrogate, or 0 when the array binds none.
    pub(crate) fn max_bound(&self) -> u32 {
        self.bound
            .values()
            .map(|s| s.as_u32())
            .max()
            .unwrap_or_default()
    }

    /// The surrogate bound to `coord_key`, if any.
    pub(crate) fn get(&self, coord_key: &[u8]) -> Option<Surrogate> {
        self.bound.get(coord_key).copied()
    }

    /// Return the surrogate bound to `coord_key`. An unbound key takes `next`,
    /// and `next` advances by one.
    pub(crate) fn bind(
        &mut self,
        array: &str,
        coord_key: Vec<u8>,
        next: &mut u32,
    ) -> Result<Surrogate, LiteError> {
        if let Some(existing) = self.bound.get(&coord_key) {
            return Ok(*existing);
        }
        let assigned = (*next).max(FIRST_SURROGATE);
        let following = assigned.checked_add(1).ok_or_else(|| LiteError::Storage {
            detail: format!(
                "cannot bind a new cell in array '{array}': all u32 surrogates are assigned"
            ),
        })?;
        let surrogate = Surrogate::new(assigned);
        *next = following;
        self.dirty.push(coord_key.clone());
        self.bound.insert(coord_key, surrogate);
        Ok(surrogate)
    }

    /// Undo a `bind` that created a new binding for `coord_key`. The caller
    /// runs this when the write that needed the binding errors, so a failed
    /// put leaves no binding and no gap in the counter.
    pub(crate) fn rollback_new(&mut self, coord_key: &[u8], surrogate: Surrogate, next: &mut u32) {
        if self.dirty.last().is_some_and(|k| k.as_slice() == coord_key) {
            self.dirty.pop();
        }
        self.bound.remove(coord_key);
        if surrogate.as_u32().checked_add(1) == Some(*next) {
            *next = surrogate.as_u32();
        }
    }

    /// Put ops for every binding made since the last `mark_clean`.
    pub(crate) fn dirty_ops(&self, array: &str) -> Vec<WriteOp> {
        self.dirty
            .iter()
            .filter_map(|coord_key| {
                self.bound.get(coord_key).map(|s| WriteOp::Put {
                    ns: Namespace::Array,
                    key: binding_key(array, coord_key),
                    value: s.as_u32().to_le_bytes().to_vec(),
                })
            })
            .collect()
    }

    /// Record that every dirty binding is persisted.
    pub(crate) fn mark_clean(&mut self) {
        self.dirty.clear();
    }

    /// Delete ops for every binding of `array`. `load` reads every persisted
    /// binding, so this covers the persisted set.
    pub(crate) fn delete_ops(&self, array: &str) -> Vec<WriteOp> {
        self.bound
            .keys()
            .map(|coord_key| WriteOp::Delete {
                ns: Namespace::Array,
                key: binding_key(array, coord_key),
            })
            .collect()
    }
}

/// Read the persisted next-free surrogate. An absent key reads as 1.
pub(crate) async fn load_hwm<S: StorageEngine>(storage: &Arc<S>) -> Result<u32, LiteError> {
    match storage.get(Namespace::Array, HWM_KEY).await? {
        Some(value) => {
            Ok(decode_u32(&value, "array surrogate high-water mark")?.max(FIRST_SURROGATE))
        }
        None => Ok(FIRST_SURROGATE),
    }
}

/// Put op that persists `next` as the next-free surrogate.
pub(crate) fn hwm_op(next: u32) -> WriteOp {
    WriteOp::Put {
        ns: Namespace::Array,
        key: HWM_KEY.to_vec(),
        value: next.to_le_bytes().to_vec(),
    }
}

/// The next-free surrogate after a reopen: past both the persisted mark and
/// every loaded binding.
pub(crate) fn next_after_load(hwm: u32, max_bound: u32) -> Result<u32, LiteError> {
    let past_bound = max_bound.checked_add(1).ok_or(LiteError::Storage {
        detail: "array surrogate store binds u32::MAX: no surrogate is free".to_owned(),
    })?;
    Ok(hwm.max(past_bound).max(FIRST_SURROGATE))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pagedb_storage::PagedbStorageMem;

    #[test]
    fn bind_reuses_existing_and_advances_next_once() {
        let mut b = CellBindings::default();
        let mut next = FIRST_SURROGATE;
        let s1 = b.bind("a", vec![1], &mut next).unwrap();
        let again = b.bind("a", vec![1], &mut next).unwrap();
        let s2 = b.bind("a", vec![2], &mut next).unwrap();
        assert_eq!(s1, again);
        assert_ne!(s1, s2);
        assert_eq!(s1, Surrogate::new(1));
        assert_eq!(s2, Surrogate::new(2));
        assert_eq!(next, 3);
        assert_eq!(b.dirty_ops("a").len(), 2);
    }

    #[test]
    fn bind_refuses_when_u32_space_is_exhausted() {
        let mut b = CellBindings::default();
        let mut next = u32::MAX;
        assert!(b.bind("a", vec![1], &mut next).is_err());
        assert_eq!(next, u32::MAX);
        assert!(b.get(&[1]).is_none());
    }

    #[test]
    fn rollback_new_removes_binding_and_restores_next() {
        let mut b = CellBindings::default();
        let mut next = FIRST_SURROGATE;
        let s = b.bind("a", vec![1], &mut next).unwrap();
        b.rollback_new(&[1], s, &mut next);
        assert!(b.get(&[1]).is_none());
        assert!(b.dirty_ops("a").is_empty());
        assert_eq!(next, FIRST_SURROGATE);
    }

    #[test]
    fn next_after_load_passes_hwm_and_bound() {
        assert_eq!(next_after_load(1, 0).unwrap(), 1);
        assert_eq!(next_after_load(10, 3).unwrap(), 10);
        assert_eq!(next_after_load(2, 7).unwrap(), 8);
        assert!(next_after_load(1, u32::MAX).is_err());
    }

    #[tokio::test]
    async fn binding_scan_for_a_excludes_a_colon_b() {
        let s = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let mut next = FIRST_SURROGATE;
        let mut a = CellBindings::default();
        let mut ab = CellBindings::default();
        a.bind("a", vec![7], &mut next).unwrap();
        ab.bind("a:b", vec![8], &mut next).unwrap();
        ab.bind("a:b", vec![9], &mut next).unwrap();
        let mut ops = a.dirty_ops("a");
        ops.extend(ab.dirty_ops("a:b"));
        ops.push(hwm_op(next));
        s.batch_write(&ops).await.unwrap();

        let loaded_a = CellBindings::load(&s, "a").await.unwrap();
        assert_eq!(loaded_a.bound.len(), 1);
        assert_eq!(loaded_a.get(&[7]), Some(Surrogate::new(1)));
        let loaded_ab = CellBindings::load(&s, "a:b").await.unwrap();
        assert_eq!(loaded_ab.bound.len(), 2);
        assert_eq!(load_hwm(&s).await.unwrap(), 4);
    }

    #[tokio::test]
    async fn load_refuses_a_malformed_binding_value() {
        let s = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        s.put(Namespace::Array, &binding_key("a", &[1]), &[1, 2, 3])
            .await
            .unwrap();
        let err = CellBindings::load(&s, "a").await.unwrap_err();
        assert!(format!("{err:?}").contains("array 'a'"));
    }

    #[tokio::test]
    async fn absent_hwm_reads_as_first_surrogate() {
        let s = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        assert_eq!(load_hwm(&s).await.unwrap(), FIRST_SURROGATE);
    }
}
