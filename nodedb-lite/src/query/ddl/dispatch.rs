//! DDL statement interception and dispatch for the Lite query engine.
//!
//! Which statement this is gets decided by parsing it, never by looking for
//! words inside it. `nodedb_sql::ddl_ast::parse` already returns a typed
//! statement for the whole DDL family, so the collection forms route off the
//! parsed engine, options and flags. Substring tests cannot tell
//! `CREATE COLLECTION notes_bitemporal_true` from a request for a bitemporal
//! collection, and a statement matching no pattern fell through to a planner
//! with no `COLLECTION` keyword at all — which is why the plainest form of the
//! statement, `CREATE COLLECTION <name>`, did not work.

use nodedb_sql::ddl_ast::statement::{CollectionStmt, NodedbStatement};
use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

/// Which engine a statement means when it names none.
#[derive(Clone, Copy)]
enum DefaultEngine {
    /// `CREATE COLLECTION` — schemaless document.
    Document,
    /// `CREATE TABLE` — strict relational, the Postgres-shaped spelling.
    Strict,
}

/// The storage engine a `CREATE COLLECTION` asked for, however it was spelled.
///
/// `WITH (engine='kv')`, a trailing `ENGINE = kv` suffix and the older
/// `WITH storage = 'kv'` all name the same thing; the parser folds the first
/// two into `engine` and leaves the third among the options.
fn requested_engine(engine: Option<&str>, options: &[(String, String)]) -> Option<String> {
    engine
        .map(|e| e.to_ascii_lowercase())
        .or_else(|| {
            options
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case("storage"))
                .map(|(_, value)| value.to_ascii_lowercase())
        })
        .map(|value| value.trim_matches(['\'', '"']).to_string())
}

/// Whether the statement carries `bitemporal=true`, as a flag or an option.
fn wants_bitemporal(options: &[(String, String)], flags: &[String]) -> bool {
    if flags.iter().any(|f| f.eq_ignore_ascii_case("BITEMPORAL")) {
        return true;
    }
    options.iter().any(|(key, value)| {
        key.eq_ignore_ascii_case("bitemporal")
            && value.trim_matches(['\'', '"']).eq_ignore_ascii_case("true")
    })
}

impl<S: StorageEngine> LiteQueryEngine<S> {
    /// Intercept DDL statements before passing them to the general planner.
    ///
    /// Returns `Some(result)` if the statement was handled here, `None` if it
    /// belongs to the planner (every non-DDL query, and the DDL families Lite
    /// executes through the shared plan path).
    pub(in crate::query) async fn try_handle_ddl(
        &self,
        sql: &str,
    ) -> Option<Result<QueryResult, LiteError>> {
        let upper = sql.trim().to_uppercase();

        // ── Forms with no typed representation in the shared DDL parser ──
        //
        // These are Lite-only statements the parser does not model, so they are
        // still recognised by prefix. A prefix test on a leading keyword is a
        // different thing from sniffing for words anywhere in the statement:
        // it cannot match on a collection's name or a quoted value.

        if upper.starts_with("CREATE MATERIALIZED VIEW ") {
            return Some(self.handle_create_materialized_view(sql).await);
        }

        if upper.starts_with("DROP MATERIALIZED VIEW ") {
            return Some(self.handle_drop_materialized_view(sql).await);
        }

        if upper.starts_with("CREATE CONTINUOUS AGGREGATE ") {
            return Some(self.handle_create_continuous_aggregate(sql).await);
        }

        if upper.starts_with("DROP CONTINUOUS AGGREGATE ") {
            return Some(self.handle_drop_continuous_aggregate(sql).await);
        }

        if upper.starts_with("SHOW CONTINUOUS AGGREGATES") {
            return Some(self.handle_show_continuous_aggregates(sql).await);
        }

        if upper.starts_with("CREATE TIMESERIES ") {
            return Some(self.handle_create_timeseries(sql).await);
        }

        if upper.starts_with("CONVERT COLLECTION ") || upper.starts_with("CONVERT ") {
            if upper.contains(" TO STRICT") {
                return Some(self.handle_convert_to_strict(sql).await);
            }
            if upper.contains(" TO COLUMNAR") {
                return Some(self.handle_convert_to_columnar(sql).await);
            }
            if upper.contains(" TO DOCUMENT") || upper.contains(" TO DOC") {
                return Some(self.handle_convert_to_document(sql).await);
            }
        }

        if upper.starts_with("ALTER TABLE ")
            && (upper.contains("ADD COLUMN") || upper.contains("ADD "))
        {
            return Some(self.handle_alter_add_column(sql).await);
        }

        // ── Collection DDL, routed off the parsed statement ──

        let parsed = nodedb_sql::ddl_ast::parse(sql)?;
        let statement = match parsed {
            Ok(statement) => statement,
            // The statement is structurally DDL but malformed. Reporting that
            // is the whole point of having parsed it; handing it on would only
            // produce a worse error from a planner that does not know the
            // syntax.
            Err(e) => return Some(Err(LiteError::Query(e.to_string()))),
        };

        let NodedbStatement::Collection(collection) = statement else {
            return None;
        };

        match collection {
            // `CREATE COLLECTION` with no engine is the schemaless document
            // engine; `CREATE TABLE` with no engine is strict relational. The
            // two statements differ only in that default.
            CollectionStmt::CreateCollection {
                name,
                engine,
                options,
                flags,
                ..
            } => Some(
                self.dispatch_create_collection(
                    sql,
                    &name,
                    engine.as_deref(),
                    &options,
                    &flags,
                    DefaultEngine::Document,
                )
                .await,
            ),

            CollectionStmt::CreateTable {
                name,
                engine,
                options,
                flags,
                ..
            } => Some(
                self.dispatch_create_collection(
                    sql,
                    &name,
                    engine.as_deref(),
                    &options,
                    &flags,
                    DefaultEngine::Strict,
                )
                .await,
            ),

            // The engine a collection was created on decides how it is dropped.
            // Anything not held by the strict or columnar engines is a document
            // collection — dropping it here rather than passing it on, because
            // the general planner has no `COLLECTION` keyword and would only
            // report a syntax error for a statement that is not malformed.
            CollectionStmt::DropCollection { name, .. } => {
                let name = name.to_lowercase();
                if self.strict.schema(&name).is_some() {
                    return Some(self.handle_drop_strict(&name).await);
                }
                if self.columnar.schema(&name).is_some() {
                    return Some(self.handle_drop_columnar(&name).await);
                }
                Some(self.handle_drop_document(&name).await)
            }

            CollectionStmt::DescribeCollection { name } => {
                let name = name.to_lowercase();
                let schema = self.strict.schema(&name)?;
                Some(Ok(super::describe_strict_collection(&name, &schema)))
            }

            _ => None,
        }
    }

    /// Pick the engine handler for a parsed `CREATE COLLECTION` / `CREATE TABLE`.
    ///
    /// The handlers parse their own column lists out of `sql` — that part was
    /// never the problem. What moved here is the decision of *which* one runs.
    async fn dispatch_create_collection(
        &self,
        sql: &str,
        name: &str,
        engine: Option<&str>,
        options: &[(String, String)],
        flags: &[String],
        default_engine: DefaultEngine,
    ) -> Result<QueryResult, LiteError> {
        let name = name.to_lowercase();

        if let Some(engine) = requested_engine(engine, options) {
            return match engine.as_str() {
                "strict" | "document_strict" => self.handle_create_strict(sql).await,
                "columnar" => self.handle_create_columnar(sql).await,
                "kv" => self.handle_create_kv(sql).await,
                "timeseries" => self.handle_create_timeseries(sql).await,
                "document" | "document_schemaless" => {
                    if wants_bitemporal(options, flags) {
                        self.handle_create_bitemporal_document(&name).await
                    } else {
                        self.handle_create_document(&name).await
                    }
                }
                other => Err(LiteError::Query(format!(
                    "unsupported engine '{other}' for collection '{name}'"
                ))),
            };
        }

        match default_engine {
            DefaultEngine::Strict => self.handle_create_strict(sql).await,
            DefaultEngine::Document => {
                if wants_bitemporal(options, flags) {
                    self.handle_create_bitemporal_document(&name).await
                } else {
                    self.handle_create_document(&name).await
                }
            }
        }
    }
}
