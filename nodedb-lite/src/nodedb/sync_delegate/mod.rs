//! `SyncDelegate` implementation — bridges the sync transport to NodeDbLite's engines.

mod array;
mod columnar_handlers;
mod definition_apply;
mod delegate_impl;
mod fts_handlers;
mod import_collection_schema;
mod reject;
mod spatial_handlers;
mod timeseries_handlers;
mod vector_handlers;
