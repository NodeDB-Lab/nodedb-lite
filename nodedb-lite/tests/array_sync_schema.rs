//! Edge-side simulation — does NOT exercise real Origin transport.
//! All tests here call Lite's inbound/outbound handlers directly, bypassing
//! the WebSocket connection to a live Origin node.
//!
//! The real-transport round-trip (Lite → Origin WebSocket → Lite) is not covered
//! by any test in this file.  See §13 of the release checklist for the decision
//! record and the placeholder real-transport test in `tests/array_sync_interop.rs`.
//!
//! Original note: Schema sync uses SchemaRegistry::import_snapshot / export_snapshot
//! (the Loro CRDT layer) without live ALTER NDARRAY DDL wiring over a real transport.

mod common;

use std::sync::Arc;

use nodedb_array::schema::array_schema::ArraySchema;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::cell_order::{CellOrder, TileOrder};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_lite::storage::redb_storage::RedbStorage;
use nodedb_lite::sync::array::inbound::outcome::InboundOutcome;
use nodedb_lite::sync::array::replica_state::ReplicaState;
use nodedb_lite::sync::array::schema_registry::SchemaRegistry;
use nodedb_types::sync::wire::array::ArraySchemaSyncMsg;

fn two_attr_schema(name: &str) -> ArraySchema {
    ArraySchema {
        name: name.into(),
        dims: vec![DimSpec::new(
            "x",
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(99)),
        )],
        attrs: vec![
            AttrSpec::new("v", AttrType::Float64, true),
            AttrSpec::new("w", AttrType::Int64, true),
        ],
        tile_extents: vec![10],
        cell_order: CellOrder::RowMajor,
        tile_order: TileOrder::RowMajor,
    }
}

/// "Origin" registers a schema, exports its Loro snapshot, and ships it to
/// "Lite B" via an `ArraySchemaSyncMsg`. After import, Lite B can receive ops
/// that carry that schema_hlc.
#[test]
fn schema_import_from_origin_enables_op_apply() {
    // "Origin" registry — the authoritative schema holder.
    let origin_storage = Arc::new(RedbStorage::open_in_memory().expect("origin storage"));
    let origin_replica =
        Arc::new(ReplicaState::load_or_init(&*origin_storage).expect("origin replica"));
    let origin_schemas =
        SchemaRegistry::new(Arc::clone(&origin_storage), Arc::clone(&origin_replica));

    let schema = two_attr_schema("cross");
    origin_schemas
        .put_schema("cross", &schema)
        .expect("origin put_schema");

    let snapshot_payload = origin_schemas
        .export_snapshot("cross")
        .expect("export_snapshot")
        .expect("Some snapshot");
    let origin_hlc = origin_schemas
        .schema_hlc("cross")
        .expect("origin schema_hlc");

    // "Lite B" — receives schema from Origin.
    let receiver = common::SyncHarness::new_in_memory();

    // Array created in the engine so the apply can write cells,
    // but schema_hlc is still ZERO until we import.
    {
        let mut state = receiver.array_state.lock().expect("lock");
        state
            .create_array(&receiver.storage, "cross", two_attr_schema("cross"))
            .expect("create_array");
    }

    // Before import: schema_hlc is None.
    assert!(
        receiver.schemas.schema_hlc("cross").is_none(),
        "schema must be absent before import"
    );

    let schema_msg = ArraySchemaSyncMsg {
        array: "cross".into(),
        replica_id: 1,
        schema_hlc_bytes: origin_hlc.to_bytes(),
        snapshot_payload,
    };

    let outcome = receiver
        .inbound
        .handle_schema(&schema_msg)
        .expect("handle_schema");
    assert_eq!(outcome, InboundOutcome::SchemaImported);

    // After import: schema_hlc matches Origin's.
    let imported_hlc = receiver
        .schemas
        .schema_hlc("cross")
        .expect("schema_hlc after import");
    // After import, the local schema HLC observes the remote and advances at
    // least to it (Loro semantics — the local clock becomes max(local, remote)+tick).
    assert!(
        imported_hlc >= origin_hlc,
        "imported schema_hlc must be >= origin's; got local={imported_hlc:?} origin={origin_hlc:?}",
    );
}

/// Ops that reference the imported schema_hlc can now apply after import.
#[test]
fn ops_with_imported_schema_hlc_apply_correctly() {
    let origin_storage = Arc::new(RedbStorage::open_in_memory().expect("storage"));
    let origin_replica = Arc::new(ReplicaState::load_or_init(&*origin_storage).expect("replica"));
    let origin_schemas =
        SchemaRegistry::new(Arc::clone(&origin_storage), Arc::clone(&origin_replica));

    origin_schemas
        .put_schema("remote", &common::simple_schema("remote"))
        .expect("put");
    let origin_hlc = origin_schemas.schema_hlc("remote").expect("hlc");
    let snapshot = origin_schemas
        .export_snapshot("remote")
        .expect("export")
        .expect("Some");

    let receiver = common::SyncHarness::new_in_memory();
    {
        let mut state = receiver.array_state.lock().expect("lock");
        state
            .create_array(&receiver.storage, "remote", common::simple_schema("remote"))
            .expect("create");
    }

    // Import schema.
    receiver
        .inbound
        .handle_schema(&ArraySchemaSyncMsg {
            array: "remote".into(),
            replica_id: 9,
            schema_hlc_bytes: origin_hlc.to_bytes(),
            snapshot_payload: snapshot,
        })
        .expect("handle_schema");

    // Now deliver an op carrying origin_hlc as schema_hlc.
    let rep = common::replica(9);
    let op = common::put_op("remote", 4, 7.5, 2000, origin_hlc, rep);
    let outcome = receiver.deliver(&op);
    assert_eq!(
        outcome,
        InboundOutcome::Applied,
        "op with imported schema_hlc must apply"
    );

    receiver.flush("remote");
    let val = receiver.read_coord("remote", 4, i64::MAX);
    assert_eq!(val, Some(CellValue::Float64(7.5)));
}

/// Calling `put_schema` again on an existing array (schema "ALTER") advances
/// the schema_hlc so the new HLC is >= the old one.
#[test]
fn put_schema_again_advances_schema_hlc() {
    let harness = common::SyncHarness::new_in_memory();
    harness.create_array("evolve");

    let hlc_v1 = harness.schema_hlc("evolve");

    // "ALTER": re-register with a two-attr schema.
    harness
        .schemas
        .put_schema("evolve", &two_attr_schema("evolve"))
        .expect("put_schema second call");

    let hlc_v2 = harness.schema_hlc("evolve");
    assert!(
        hlc_v2 >= hlc_v1,
        "schema_hlc must be >= v1 after re-put; v1={hlc_v1:?}, v2={hlc_v2:?}"
    );
}
