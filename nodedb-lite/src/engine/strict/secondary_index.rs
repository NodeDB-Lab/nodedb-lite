//! Secondary B-tree indexes on non-PK columns for strict collections.
//!
//! Maps column values → set of PK values. Enables point lookups and range
//! scans on non-PK columns without scanning all rows.
//!
//! Index key: `{collection}:{column}:{value_bytes}` → `Vec<String>` (PK strings).
//! Stored in the `Meta` namespace for persistence.

use std::collections::{BTreeMap, HashSet};

use nodedb_types::value::Value;

/// A secondary B-tree index on a single column.
///
/// Maps encoded column values to sets of row IDs (PK strings).
/// Supports exact match lookups and range scans.
pub struct SecondaryIndex {
    /// Column name this index covers.
    pub column_name: String,
    /// Value → set of PK strings.
    entries: BTreeMap<Vec<u8>, HashSet<String>>,
}

impl SecondaryIndex {
    /// Create a new empty secondary index for the given column.
    pub fn new(column_name: impl Into<String>) -> Self {
        Self {
            column_name: column_name.into(),
            entries: BTreeMap::new(),
        }
    }

    /// Add a value → PK mapping.
    pub fn insert(&mut self, value: &Value, pk: &str) {
        let key = encode_value(value);
        self.entries.entry(key).or_default().insert(pk.to_string());
    }

    /// Remove a value → PK mapping.
    pub fn remove(&mut self, value: &Value, pk: &str) {
        let key = encode_value(value);
        if let Some(pks) = self.entries.get_mut(&key) {
            pks.remove(pk);
            if pks.is_empty() {
                self.entries.remove(&key);
            }
        }
    }

    /// Exact match lookup: returns all PKs with this exact value.
    pub fn lookup(&self, value: &Value) -> Vec<&str> {
        let key = encode_value(value);
        self.entries
            .get(&key)
            .map(|pks| pks.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// Range scan: returns all PKs with values in `[start, end]`.
    pub fn range_scan(&self, start: &Value, end: &Value) -> Vec<&str> {
        let start_key = encode_value(start);
        let end_key = encode_value(end);
        let mut results = Vec::new();
        for (_key, pks) in self.entries.range(start_key..=end_key) {
            for pk in pks {
                results.push(pk.as_str());
            }
        }
        results
    }

    /// Number of distinct values indexed.
    pub fn cardinality(&self) -> usize {
        self.entries.len()
    }

    /// Total number of index entries (value-PK pairs).
    pub fn entry_count(&self) -> usize {
        self.entries.values().map(|pks| pks.len()).sum()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Encode a Value into sortable bytes for the B-tree key.
fn encode_value(value: &Value) -> Vec<u8> {
    match value {
        Value::Integer(v) => {
            let sortable = (*v as u64) ^ (1u64 << 63);
            sortable.to_be_bytes().to_vec()
        }
        Value::Float(v) => {
            // IEEE 754 sort: flip sign bit, flip all bits if negative.
            let bits = v.to_bits();
            let sortable = if bits & (1u64 << 63) != 0 {
                !bits
            } else {
                bits ^ (1u64 << 63)
            };
            sortable.to_be_bytes().to_vec()
        }
        Value::String(s) => s.as_bytes().to_vec(),
        Value::Bool(b) => vec![*b as u8],
        Value::Null => vec![],
        _ => format!("{value:?}").into_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_lookup() {
        let mut idx = SecondaryIndex::new("email");
        idx.insert(&Value::String("alice@co.com".into()), "1");
        idx.insert(&Value::String("bob@co.com".into()), "2");
        idx.insert(&Value::String("alice@co.com".into()), "3"); // Duplicate value, different PK.

        let results = idx.lookup(&Value::String("alice@co.com".into()));
        assert_eq!(results.len(), 2);
        assert!(results.contains(&"1"));
        assert!(results.contains(&"3"));

        let results = idx.lookup(&Value::String("bob@co.com".into()));
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn remove_entry() {
        let mut idx = SecondaryIndex::new("status");
        idx.insert(&Value::String("active".into()), "1");
        idx.insert(&Value::String("active".into()), "2");

        idx.remove(&Value::String("active".into()), "1");
        let results = idx.lookup(&Value::String("active".into()));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], "2");
    }

    #[test]
    fn range_scan_integers() {
        let mut idx = SecondaryIndex::new("age");
        for i in 0..100 {
            idx.insert(&Value::Integer(i), &i.to_string());
        }

        // Range [20, 30] should return 11 PKs.
        let results = idx.range_scan(&Value::Integer(20), &Value::Integer(30));
        assert_eq!(results.len(), 11);
    }

    #[test]
    fn empty_lookup() {
        let idx = SecondaryIndex::new("col");
        assert!(idx.lookup(&Value::String("x".into())).is_empty());
        assert!(idx.is_empty());
    }

    #[test]
    fn cardinality() {
        let mut idx = SecondaryIndex::new("status");
        idx.insert(&Value::String("active".into()), "1");
        idx.insert(&Value::String("active".into()), "2");
        idx.insert(&Value::String("inactive".into()), "3");

        assert_eq!(idx.cardinality(), 2); // 2 distinct values.
        assert_eq!(idx.entry_count(), 3); // 3 total entries.
    }
}
