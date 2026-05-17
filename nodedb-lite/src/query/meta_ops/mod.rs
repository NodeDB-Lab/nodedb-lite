// SPDX-License-Identifier: Apache-2.0
pub mod array;
pub mod continuous_agg;
pub mod distributed;
pub mod indexes;
pub mod info;
pub mod lifecycle;
pub mod synonyms;
pub mod temporal;

pub use array::handle_alter_array;
pub use continuous_agg::{
    handle_apply_continuous_agg_retention, handle_list_continuous_aggregates,
    handle_query_aggregate_last_value, handle_query_aggregate_last_values,
    handle_query_aggregate_watermark, handle_register_continuous_aggregate,
    handle_unregister_continuous_aggregate,
};
pub use distributed::txn::{handle_calvin_active, handle_calvin_passive, handle_calvin_static};
pub use distributed::{
    CancellationRegistry, handle_cancel, handle_create_tenant_snapshot, handle_purge_tenant,
    handle_raw_response, handle_restore_tenant_snapshot, handle_txn_batch, handle_wal_append,
};
pub use indexes::handle_rebuild_index;
pub use info::handle_query_collection_size;
pub use lifecycle::{
    handle_checkpoint, handle_compact, handle_convert_collection, handle_create_snapshot,
    handle_rename_collection, handle_unregister_collection, handle_unregister_materialized_view,
};
pub use synonyms::{handle_delete_synonym_group, handle_put_synonym_group};
pub use temporal::{
    handle_enforce_timeseries_retention, handle_temporal_purge_array,
    handle_temporal_purge_columnar, handle_temporal_purge_crdt,
    handle_temporal_purge_document_strict, handle_temporal_purge_edge_store,
};
