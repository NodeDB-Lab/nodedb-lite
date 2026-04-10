//! Local sequence support for NodeDB-Lite.
//!
//! Stores sequence definitions in redb via the StorageEngine.
//! Provides nextval/currval/setval for local use. Definitions sync
//! from Origin via CRDT; counter state is ephemeral (in-memory only,
//! reset to `start_value` on restart).

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use serde::{Deserialize, Serialize};

use crate::error::LiteError;

/// Lite-side sequence definition (synced from Origin via CRDT).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiteSequenceDef {
    pub name: String,
    pub start_value: i64,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub cycle: bool,
}

/// Per-sequence atomic counter.
struct LiteSeqHandle {
    counter: AtomicI64,
    called: std::sync::atomic::AtomicBool,
    def: LiteSequenceDef,
}

/// Local sequence registry for NodeDB-Lite.
///
/// Definitions arrive via CRDT sync from Origin. State (current_value)
/// is purely local — each Lite instance has its own counter within
/// an allocated range.
pub struct LiteSequenceRegistry {
    sequences: Mutex<HashMap<String, LiteSeqHandle>>,
}

impl LiteSequenceRegistry {
    pub fn new() -> Self {
        Self {
            sequences: Mutex::new(HashMap::new()),
        }
    }

    /// Register a sequence (from CRDT sync or local creation).
    pub fn register(&self, def: LiteSequenceDef) {
        let initial = def.start_value - def.increment;
        let mut map = self.sequences.lock().unwrap_or_else(|p| p.into_inner());
        map.insert(
            def.name.clone(),
            LiteSeqHandle {
                counter: AtomicI64::new(initial),
                called: std::sync::atomic::AtomicBool::new(false),
                def,
            },
        );
    }

    /// Get next value (lock-free on the hot path).
    pub fn nextval(&self, name: &str) -> Result<i64, LiteError> {
        let map = self.sequences.lock().unwrap_or_else(|p| p.into_inner());
        let handle = map
            .get(name)
            .ok_or_else(|| LiteError::Query(format!("sequence \"{name}\" does not exist")))?;

        let inc = handle.def.increment;
        let prev = handle.counter.fetch_add(inc, Ordering::Relaxed);
        let new_val = prev + inc;
        handle.called.store(true, Ordering::Relaxed);

        if inc > 0 && new_val > handle.def.max_value {
            if handle.def.cycle {
                handle
                    .counter
                    .store(handle.def.min_value, Ordering::Relaxed);
                return Ok(handle.def.min_value);
            }
            handle.counter.store(prev, Ordering::Relaxed);
            return Err(LiteError::Query(format!("sequence \"{name}\" exhausted")));
        }
        if inc < 0 && new_val < handle.def.min_value {
            if handle.def.cycle {
                handle
                    .counter
                    .store(handle.def.max_value, Ordering::Relaxed);
                return Ok(handle.def.max_value);
            }
            handle.counter.store(prev, Ordering::Relaxed);
            return Err(LiteError::Query(format!("sequence \"{name}\" exhausted")));
        }

        Ok(new_val)
    }

    /// Get current value (last returned by nextval).
    pub fn currval(&self, name: &str) -> Result<i64, LiteError> {
        let map = self.sequences.lock().unwrap_or_else(|p| p.into_inner());
        let handle = map
            .get(name)
            .ok_or_else(|| LiteError::Query(format!("sequence \"{name}\" does not exist")))?;
        if !handle.called.load(Ordering::Relaxed) {
            return Err(LiteError::Query(format!(
                "currval of \"{name}\" not yet defined"
            )));
        }
        Ok(handle.counter.load(Ordering::Relaxed))
    }

    /// Set the counter to a specific value.
    pub fn setval(&self, name: &str, value: i64) -> Result<i64, LiteError> {
        let map = self.sequences.lock().unwrap_or_else(|p| p.into_inner());
        let handle = map
            .get(name)
            .ok_or_else(|| LiteError::Query(format!("sequence \"{name}\" does not exist")))?;
        if value < handle.def.min_value || value > handle.def.max_value {
            return Err(LiteError::Query(format!(
                "setval: {value} outside [{}, {}]",
                handle.def.min_value, handle.def.max_value
            )));
        }
        handle.counter.store(value, Ordering::Relaxed);
        handle.called.store(true, Ordering::Relaxed);
        Ok(value)
    }

    /// List all sequence names.
    pub fn names(&self) -> Vec<String> {
        let map = self.sequences.lock().unwrap_or_else(|p| p.into_inner());
        map.keys().cloned().collect()
    }

    /// Remove a sequence by name.
    pub fn remove(&self, name: &str) -> bool {
        let mut map = self.sequences.lock().unwrap_or_else(|p| p.into_inner());
        map.remove(name).is_some()
    }
}

impl Default for LiteSequenceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_def(name: &str) -> LiteSequenceDef {
        LiteSequenceDef {
            name: name.to_string(),
            start_value: 1,
            increment: 1,
            min_value: 1,
            max_value: i64::MAX,
            cycle: false,
        }
    }

    #[test]
    fn basic_nextval() {
        let reg = LiteSequenceRegistry::new();
        reg.register(make_def("s1"));
        assert_eq!(reg.nextval("s1").unwrap(), 1);
        assert_eq!(reg.nextval("s1").unwrap(), 2);
        assert_eq!(reg.currval("s1").unwrap(), 2);
    }

    #[test]
    fn setval_and_nextval() {
        let reg = LiteSequenceRegistry::new();
        reg.register(make_def("s1"));
        reg.setval("s1", 50).unwrap();
        assert_eq!(reg.nextval("s1").unwrap(), 51);
    }

    #[test]
    fn not_found() {
        let reg = LiteSequenceRegistry::new();
        assert!(reg.nextval("nope").is_err());
    }
}
