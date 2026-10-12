// SPDX-License-Identifier: Apache-2.0

//! Sorted-index (leaderboard) definitions over key-value collections,
//! persisted in `Namespace::Meta` under the index root and loaded at open.
//!
//! A sorted index ranks the rows of one collection by one numeric score
//! column, optionally limited to a time window over a timestamp column.
//! Its entries are written in the same storage batch as the rows they
//! follow (see `query::kv_ops::sorted`).

use crate::error::LiteError;

/// The window that limits which entries a read sees.
///
/// Encoded as its discriminant: do not reorder the variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(c_enum)]
pub(crate) enum WindowKind {
    /// Every entry is visible.
    None = 0,
    /// Entries stamped since the current UTC midnight.
    Daily = 1,
    /// Entries stamped since the current UTC Monday 00:00.
    Weekly = 2,
    /// Entries stamped since the 1st of the current UTC month.
    Monthly = 3,
    /// Entries stamped since `start_ms`, while now is in `[start_ms, end_ms)`.
    Custom = 4,
}

/// The time window of a sorted index.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(crate) struct WindowDef {
    pub kind: WindowKind,
    /// Column holding each row's timestamp in ms since epoch. Empty when
    /// `kind` is `None`.
    pub timestamp_column: String,
    /// The timestamp sorts descending among entries of equal score.
    pub timestamp_desc: bool,
    /// Start of a custom window, ms since epoch.
    pub start_ms: u64,
    /// End (exclusive) of a custom window, ms since epoch.
    pub end_ms: u64,
}

impl WindowDef {
    /// No window: every entry is visible.
    pub fn none() -> Self {
        Self {
            kind: WindowKind::None,
            timestamp_column: String::new(),
            timestamp_desc: false,
            start_ms: 0,
            end_ms: 0,
        }
    }

    /// Whether entries carry a timestamp and reads filter by it.
    pub fn is_windowed(&self) -> bool {
        self.kind != WindowKind::None
    }
}

/// One sorted index.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(crate) struct SortedIndexDef {
    /// Index name, unique across sorted indexes.
    pub name: String,
    /// Key-value collection whose rows the index ranks.
    pub collection: String,
    /// Numeric column the rows rank by.
    pub score_column: String,
    /// Higher scores rank first.
    pub score_desc: bool,
    /// Primary-key column name the plan declared.
    pub key_column: String,
    /// Time window over the rows.
    pub window: WindowDef,
}

impl SortedIndexDef {
    /// Serialized form stored under `key::sorted_def_key`.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, LiteError> {
        zerompk::to_msgpack_vec(self).map_err(|e| LiteError::Serialization {
            detail: format!("encode sorted index definition '{}': {e}", self.name),
        })
    }

    /// Decode a stored definition.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, LiteError> {
        zerompk::from_msgpack(bytes).map_err(|e| LiteError::Corrupted {
            detail: format!("stored sorted index definition does not decode: {e}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_definition_round_trips_with_its_window() {
        let def = SortedIndexDef {
            name: "lb".into(),
            collection: "players".into(),
            score_column: "score".into(),
            score_desc: true,
            key_column: "id".into(),
            window: WindowDef {
                kind: WindowKind::Custom,
                timestamp_column: "ts".into(),
                timestamp_desc: false,
                start_ms: 10,
                end_ms: 20,
            },
        };
        let decoded = SortedIndexDef::decode(&def.encode().expect("encode")).expect("decode");
        assert_eq!(decoded, def);
    }
}
