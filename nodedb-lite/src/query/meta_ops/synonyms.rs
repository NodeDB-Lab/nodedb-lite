// SPDX-License-Identifier: Apache-2.0
//! Synonym group meta-ops: PutSynonymGroup, DeleteSynonymGroup.

use sonic_rs::JsonValueTrait as _;

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

/// Key prefix for synonym groups in `Namespace::Meta`.
const SYNONYM_PREFIX: &str = "synonym/";

/// `PutSynonymGroup` — persist a synonym group record to redb meta storage.
///
/// The `record_json` field is stored verbatim under `synonym/<tenant>/<name>`.
/// The group name is extracted from the JSON `"name"` field.
pub async fn handle_put_synonym_group<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    tenant_id: u64,
    record_json: &str,
) -> Result<QueryResult, LiteError> {
    let name = extract_synonym_name(record_json)?;
    let key = format!("{SYNONYM_PREFIX}{tenant_id}/{name}");
    engine
        .storage
        .put(Namespace::Meta, key.as_bytes(), record_json.as_bytes())
        .await?;
    Ok(QueryResult {
        columns: vec!["name".into()],
        rows: vec![vec![Value::String(name)]],
        rows_affected: 1,
    })
}

/// `DeleteSynonymGroup` — remove a synonym group by tenant + name.
pub async fn handle_delete_synonym_group<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    tenant_id: u64,
    name: &str,
) -> Result<QueryResult, LiteError> {
    let key = format!("{SYNONYM_PREFIX}{tenant_id}/{name}");
    engine
        .storage
        .delete(Namespace::Meta, key.as_bytes())
        .await?;
    Ok(QueryResult {
        columns: vec!["name".into()],
        rows: vec![vec![Value::String(name.to_owned())]],
        rows_affected: 1,
    })
}

/// Extract the `"name"` field from a JSON synonym group record using the
/// project-standard JSON parser (`sonic_rs`).
fn extract_synonym_name(json: &str) -> Result<String, LiteError> {
    let value: sonic_rs::Value = sonic_rs::from_str(json).map_err(|e| LiteError::BadRequest {
        detail: format!("PutSynonymGroup: invalid JSON record: {e}"),
    })?;
    let name = value.get("name").ok_or_else(|| LiteError::BadRequest {
        detail: "PutSynonymGroup: record_json missing \"name\" field".into(),
    })?;
    let name_str = name.as_str().ok_or_else(|| LiteError::BadRequest {
        detail: "PutSynonymGroup: \"name\" value must be a JSON string".into(),
    })?;
    Ok(name_str.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_name_basic() {
        let json = r#"{"name":"stopwords","terms":["and","or"]}"#;
        assert_eq!(extract_synonym_name(json).unwrap(), "stopwords");
    }

    #[test]
    fn extract_name_with_escaped_quote() {
        let json = r#"{"name":"foo\"bar","terms":[]}"#;
        assert_eq!(extract_synonym_name(json).unwrap(), "foo\"bar");
    }

    #[test]
    fn extract_name_missing() {
        let json = r#"{"terms":["and"]}"#;
        assert!(extract_synonym_name(json).is_err());
    }

    #[test]
    fn extract_name_not_a_string() {
        let json = r#"{"name":42}"#;
        assert!(extract_synonym_name(json).is_err());
    }
}
