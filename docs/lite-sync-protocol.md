# NodeDB Lite — Sync Protocol Contract (0.1.0-beta.1)

## Scope

This document specifies the handshake and vector-clock contract between
NodeDB Lite (the embedded edge client) and NodeDB Origin (the server) for the
0.1.0-beta.1 release.  It is derived directly from the implementation — not
aspirational.  When code and doc diverge, the code is authoritative and this
doc must be updated.

---

## Wire Version

The accepted wire version for 0.1.0-beta.1 is **4** (`WIRE_FORMAT_VERSION = 4`,
`MIN_WIRE_FORMAT_VERSION = 4`).  Origin enforces `floor == ceiling`: any client
sending `wire_version != 4` receives a rejection with `success: false` and an
error message containing "wire version" or "incompatible".

Source: `nodedb/nodedb-types/src/wire_version.rs`, enforced at
`nodedb/nodedb/src/control/server/sync/session/handshake.rs:35-51`.

---

## Handshake Message Fields

All fields are required to be present in the serialised MessagePack frame.
Fields marked `#[serde(default)]` deserialise to their zero value when absent
from older clients; Origin explicitly rejects the resulting `wire_version = 0`.

| Field               | Type                                      | Required | Notes                                              |
|---------------------|-------------------------------------------|----------|----------------------------------------------------|
| `jwt_token`         | `String`                                  | Yes      | Empty string → trust mode (dev/test only)          |
| `vector_clock`      | `HashMap<String, HashMap<String, u64>>`   | Yes      | See clock contract below                           |
| `subscribed_shapes` | `Vec<String>`                             | Yes      | Shape IDs; may be empty                            |
| `client_version`    | `String`                                  | Yes      | Informational; not validated                       |
| `lite_id`           | `String` (`#[serde(default)]`)            | No       | UUID v7; empty string disables fork detection      |
| `epoch`             | `u64` (`#[serde(default)]`)              | No       | 0 disables fork detection                          |
| `wire_version`      | `u16` (`#[serde(default)]`)              | Yes*     | Missing → 0 → rejected                             |

Source: `nodedb/nodedb-types/src/sync/wire/session.rs:13-32`.

---

## Handshake Ack Fields

| Field                | Type                       | Notes                                              |
|----------------------|----------------------------|----------------------------------------------------|
| `success`            | `bool`                     | `true` = session established                       |
| `session_id`         | `String`                   | Non-empty on success; echoed from server state     |
| `server_clock`       | `HashMap<String, u64>`     | Flat map: `peer_hex → counter`; used to init Lite  |
| `error`              | `Option<String>`           | Non-null on failure                                |
| `fork_detected`      | `bool`                     | If `true`, Lite must regenerate `lite_id`          |
| `server_wire_version`| `u16`                      | Always present; Lite may check against its own     |

Source: `nodedb/nodedb-types/src/sync/wire/session.rs:34-53`.

---

## Vector-Clock Contract for 0.1.0-beta.1

### Decision: global-clock encoding is the beta contract

For 0.1.0-beta.1, Lite sends a **simplified global clock** rather than a
per-collection/per-document clock.  The wire shape is:

```
vector_clock = { "_global": { "<peer_id_hex>": <counter> } }
```

Origin's `handle_handshake` extracts `last_seen_lsn` as the **maximum value
across all inner maps of all collection keys**:

```rust
// nodedb/nodedb/src/control/server/sync/session/handshake.rs:75-79
self.last_seen_lsn = msg
    .vector_clock
    .values()
    .flat_map(|m| m.values().copied())
    .max()
    .unwrap_or(0);
```

This means Origin does **not** parse collection or document identifiers from the
clock; it only extracts the scalar high-water mark.  The `_global` key is
treated identically to any real collection name — Origin takes the max counter
from its inner map.

**Consequence**: Lite's global-clock encoding is accepted by Origin and resume
semantics are preserved for the beta release.  Origin will replay deltas whose
LSN is greater than `last_seen_lsn`.

### Future (post-beta)

A per-collection/per-document clock would allow Origin to resume at finer
granularity and skip replay of already-seen collections.  This is a known
improvement area and is not part of the 0.1.0-beta.1 contract.

### Divergence from field comment

The `HandshakeMsg.vector_clock` field has a doc comment that says
`"{ collection: { doc_id: lamport_ts } }"`.  Lite sends `{ "_global": { peer_hex: counter } }`.
The mismatch is intentional at the implementation level (Origin only takes the
max), but the comment is misleading.  The comment should be updated to reflect
that Origin only uses the scalar maximum and that the `_global` encoding is
the accepted Lite contract.  This is tracked as a documentation gap, not a bug.

---

## Fork Detection

Fork detection activates when **both** `lite_id` is non-empty **and** `epoch > 0`.

| Scenario                                          | Origin response                                     |
|---------------------------------------------------|-----------------------------------------------------|
| `lite_id` empty or `epoch == 0`                   | Fork detection skipped; session proceeds normally   |
| New `lite_id` (never seen before)                 | Epoch stored; session proceeds normally             |
| Same `lite_id`, `epoch > last_seen_epoch`         | Epoch updated; session proceeds normally            |
| Same `lite_id`, `epoch == last_seen_epoch`        | `fork_detected: true`, `success: false`             |
| Same `lite_id`, `epoch < last_seen_epoch`         | `fork_detected: true`, `success: false`             |
| Same `lite_id`, same epoch, clean reconnect       | Treated as fork if `epoch_tracker` still holds the same value — Lite must bump epoch on reconnect after write |

Source: `nodedb/nodedb/src/control/server/sync/session/handshake.rs:173-214`.

**Important nuance**: the epoch tracker is in-memory (`Mutex<HashMap<String, u64>>`).
A server restart clears the tracker, so a client reconnecting after an Origin
restart with the same `lite_id` + `epoch` will **not** trigger fork detection
on that reconnect.  This is the expected behaviour for the beta.

---

## Resume Semantics

After a successful handshake, Origin sets `last_seen_lsn` to the maximum
counter from the client's `vector_clock` and uses it as the replay start point.
Deltas with LSN `> last_seen_lsn` are sent to the client; deltas at or below
that mark are skipped.

Lite's global-clock encoding means the resume point is the **highest counter
Lite has seen across all peers**, not per-collection.  This may cause minor
redundant replay in multi-collection scenarios but will never cause a gap.

---

## Trust Mode

An empty `jwt_token` bypasses JWT validation.  Origin creates an identity with
`user_id = 0`, `tenant_id = 0`, role `ReadWrite`.  This mode is for
development and integration tests only.  Production deployments must provide
a valid JWT.

Source: `nodedb/nodedb/src/control/server/sync/session/handshake.rs:54-103`.

---

## CollectionPurged (0x14) — Out of Scope for 0.1.0-beta.1

### Origin side (wired)

Origin's event plane broadcasts a `CollectionPurgedMsg` (frame type `0x14`) to
every connected Lite session that has subscribed or pushed deltas to the
affected collection.  The broadcast path is:

1. `DROP COLLECTION` DDL → `catalog_entry/post_apply/async_dispatch/collection.rs`
2. → `crdt_sync/delivery.rs::broadcast_collection_purged()`
3. → encodes `CollectionPurgedMsg { collection, purge_lsn }` and enqueues into
   each matching session's control channel.

`SyncSession::track_collection()` records the `(tenant_id, collection)` pair
on every `DeltaPush` and `ShapeSubscribe` so the broadcast filter is correct.

### Lite side (not handled for beta)

Lite's `dispatch_frame` in `sync/transport.rs` does **not** match on
`SyncMessageType::CollectionPurged` (0x14).  The frame falls through to the
`_ =>` arm and is logged as "unexpected frame type from Origin".  No shape
eviction, no local collection purge, no application notification occurs.

### Why not tested in 0.1.0-beta.1

- Triggering the broadcast requires issuing a `DROP COLLECTION` DDL against
  Origin, which requires a pgwire or HTTP control connection.  The interop test
  harness exposes only the sync WebSocket on port 9090.
- Asserting Lite's current behavior (silent log) would couple tests to log
  output rather than observable state.

### When to promote to in-scope

When Lite's `dispatch_frame` handles `CollectionPurged` by evicting the
subscribed shape's local state and notifying the application layer, and when
the test harness exposes a pgwire/HTTP endpoint so tests can issue
`DROP COLLECTION` to trigger the broadcast end-to-end.

---

## Definition Sync (PREVIEW)

Definition sync carries function, trigger, and procedure definitions from
Origin to connected Lite clients via `DefinitionSync` (frame opcode `0x70`,
`SyncMessageType::DefinitionSync`).

### Lite side (receive path)

Lite's `dispatch_frame` in `nodedb-lite/nodedb-lite/src/sync/transport.rs`
matches on `SyncMessageType::DefinitionSync` and calls
`delegate.import_definition(&msg)`.  `import_definition` is implemented in
`nodedb-lite/nodedb-lite/src/nodedb/sync_delegate.rs` and handles both `"put"`
(create/replace) and `"delete"` (drop) actions.  The wire type
`DefinitionSyncMsg` is defined in
`nodedb/nodedb-types/src/sync/wire/timeseries.rs` with opcode registered at
`nodedb/nodedb-types/src/sync/wire/frame.rs`.

### Origin side (emission path)

Origin emits `DefinitionSync` (0x70) frames after every WAL-durable DDL
commit that affects executable definitions:

- `CREATE [OR REPLACE] FUNCTION` / `DROP FUNCTION` — handled by
  `control/server/pgwire/ddl/function/create/handler.rs` and `drop.rs`
- `CREATE [OR REPLACE] TRIGGER` / `DROP TRIGGER` — handled by
  `control/server/pgwire/ddl/trigger/create.rs` and `drop.rs`
- `CREATE [OR REPLACE] PROCEDURE` / `DROP PROCEDURE` — handled by
  `control/server/pgwire/ddl/procedure/create/handler.rs` and `drop.rs`

Broadcast is coordinated through `DefinitionSyncFanout`
(`control/server/sync/definition_fanout.rs`), a per-session bounded mpsc
registry that mirrors the `ArrayDeliveryRegistry` pattern.  The fanout is held
on `SharedState` and registered per session from `session_handler.rs` after
handshake.  The session handler uses `tokio::select!` to await either an
inbound WebSocket message or a new frame on the definition-sync channel, so
server-push delivery is not gated on client traffic.

### Tests

`nodedb-lite/nodedb-lite/tests/definition_sync_interop.rs` contains four
real-transport tests against a live `OriginServer`:

- `definition_sync_function_put` — CREATE OR REPLACE FUNCTION → `"put"` frame
- `definition_sync_function_delete` — DROP FUNCTION → `"delete"` frame
- `definition_sync_trigger_put` — CREATE OR REPLACE TRIGGER → `"put"` frame
- `definition_sync_procedure_put` — CREATE OR REPLACE PROCEDURE → `"put"` frame

All four pass (4/4) in the `heavy` nextest group.

---

## Array Sync (BETA)

Array sync uses a dedicated wire sub-protocol layered on top of the standard
sync session.  The message types involved are:

| Message type            | Direction       | Handled by                                             |
|-------------------------|-----------------|--------------------------------------------------------|
| `ArraySchema`           | Lite → Origin   | `OriginArrayInbound::handle_schema`                    |
| `ArraySnapshot`         | Lite → Origin   | `OriginArrayInbound::handle_snapshot_header`           |
| `ArraySnapshotChunk`    | Lite → Origin   | `OriginArrayInbound::handle_snapshot_chunk`            |
| `ArrayAck`              | Lite → Origin   | `OriginArrayInbound::handle_ack`                       |
| `ArrayCatchupRequest`   | Lite → Origin   | `OriginArrayInbound::handle_catchup_request`           |
| `ArrayDelta`            | Origin → Lite   | `dispatch_frame` → `SyncDelegate::handle_array_delta`  |
| `ArrayDeltaBatch`       | Origin → Lite   | `dispatch_frame` → `SyncDelegate::handle_array_delta_batch` |
| `ArrayReject`           | Origin → Lite   | `dispatch_frame` → `SyncDelegate::handle_array_reject` |

### Origin-side wiring (complete)

Origin dispatches all inbound array message types in
`nodedb/nodedb/src/control/server/sync/session_handler.rs` (lines 153–448).
The full inbound implementation lives in
`nodedb/nodedb/src/control/array_sync/inbound.rs` and
`nodedb/nodedb/src/control/array_sync/snapshot_assembly.rs`.

The outbound fan-out — Origin pushing `ArrayDeltaMsg` / `ArrayDeltaBatchMsg`
to subscribed Lite sessions — is implemented in
`nodedb/nodedb/src/control/array_sync/outbound/` (`fanout.rs`, `delivery.rs`,
`cursor.rs`, `subscriber_state.rs`, `merge.rs`, `snapshot_trigger.rs`).

Shape subscription for array shapes is handled in
`nodedb/nodedb/src/control/server/sync/async_dispatch.rs` (lines 89–130): the
`ShapeType::Array` arm validates the array name against the schema registry and
registers a subscriber cursor.

### Lite-side receive path (wired)

`sync/transport.rs::dispatch_frame` matches on `SyncMessageType::ArrayDelta`
(0x90) and `SyncMessageType::ArrayDeltaBatch` (0x91).  Each arm decodes the
MessagePack body via `SyncFrame::decode_body`, calls the `SyncDelegate` method
on `NodeDbLite`, and stores the returned `ArrayAckMsg` on `SyncClient` via
`set_pending_array_ack`.  The push loop drains it and sends it to Origin
(advancing the GC frontier).

`SyncMessageType::ArrayReject` (0x96) is also handled: the delegate removes the
rejected op from the local pending queue via `ArrayInbound::handle_reject`.

### Test coverage

In-process simulations (`tests/array_sync_*.rs`, 24 tests) exercise all
inbound handler logic without a live network transport.

`tests/array_sync_interop_real.rs` (5 tests, all passing) proves the full
dispatch path: hand-crafted `ArrayDeltaMsg` / `ArrayDeltaBatchMsg` frames are
pushed through `SyncDelegate::handle_array_delta` / `handle_array_delta_batch`
(the exact methods `dispatch_frame` calls), and the tests assert both the
returned `ArrayAckMsg` and the engine-visible cell state.

`tests/array_sync_interop.rs` retains two `#[ignore]` tests that document
the future full end-to-end `OriginServer::spawn()` path for completeness.

---

## Columnar Insert Sync (PREVIEW)

Columnar insert sync replicates rows inserted into a Lite columnar collection
to Origin using a dedicated wire frame pair.

| Message type        | Opcode | Direction      | Handled by                                              |
|---------------------|--------|----------------|---------------------------------------------------------|
| `ColumnarInsert`    | 0xA0   | Lite → Origin  | `session_handler.rs` → `SyncSession::handle_columnar_insert` |
| `ColumnarInsertAck` | 0xA1   | Origin → Lite  | `dispatch_frame` → `SyncDelegate::acknowledge_columnar_batch` |

### Wire format

`ColumnarInsertMsg` (defined in `nodedb-types/src/sync/wire/columnar.rs`):
- `lite_id`: Lite instance identifier.
- `collection`: target collection name.
- `rows`: each entry is a MessagePack-serialized `Vec<nodedb_types::value::Value>` in schema column order.
- `batch_id`: monotonic per-collection ID for ACK correlation.
- `schema_bytes`: optional MessagePack-serialized `ColumnarSchema` hint for Origin validation.

`ColumnarInsertAckMsg`:
- `collection`, `batch_id`: echo from the insert.
- `accepted` / `rejected`: row counts.
- `reject_reason`: first failure detail, if any.

### Lite outbound path

`ColumnarEngine::insert` (in `src/engine/columnar/store.rs`) enqueues the row
into `ColumnarOutbound` (`src/sync/columnar_outbound.rs`).  Rows for the same
collection are coalesced into a single in-flight batch.

`NodeDbLite` holds `Arc<ColumnarOutbound>`.  `SyncDelegate::pending_columnar_batches`
(implemented in `src/nodedb/sync_delegate.rs`) drains the queue; the Lite
`delta_push_loop` in `src/sync/transport.rs` encodes each batch as a
`ColumnarInsert` frame and sends it to Origin.

On `ColumnarInsertAck`, `dispatch_frame` calls
`delegate.acknowledge_columnar_batch(batch_id)`, which removes the batch from
the queue.  On send failure the batch is re-queued via
`delegate.reject_columnar_batch`.

### Origin inbound path

`session_handler.rs` intercepts `SyncMessageType::ColumnarInsert` before the
generic `process_frame` call.  It decodes the body, calls
`SyncSession::handle_columnar_insert` with a `ColumnarDispatcher`.

`SharedStateColumnarDispatcher` (in `sync/columnar_handler.rs`) translates the
decoded rows to a JSON array payload and dispatches
`PhysicalPlan::Columnar(ColumnarOp::Insert)` to the Data Plane via the SPSC
bridge using `EventSource::CrdtSync` (suppresses AFTER triggers on synced data).
The returned row count is reported in the ACK.

### Test coverage

`tests/sync_interop_columnar.rs` contains two live-Origin gate tests:
- `columnar_inserts_replicate_to_origin` — inserts 3 rows post-connect, waits ≤5 s, asserts 3 rows visible via `SELECT id` pgwire scan (uses columnar scan path, not aggregate).
- `columnar_pre_connection_inserts_sync_after_connect` — inserts rows before the sync task starts; verifies they replicate once the connection is established.

Unit tests for `ColumnarOutbound` live in `src/sync/columnar_outbound.rs`
(enqueue/drain/ack/requeue invariants).  `columnar_handler.rs` contains
`SyncSession` unit tests covering unauthenticated rejection, successful
dispatch, and dispatch-failure paths.

---

## Timeseries Sync (PREVIEW)

Timeseries collections in Lite are created with `CREATE TIMESERIES COLLECTION`
DDL, which maps to `ColumnarEngine` with `ColumnarProfile::Timeseries`.
Because timeseries is backed by the columnar engine, inserts flow through the
existing `ColumnarOutbound` queue and are transmitted as `ColumnarInsert`
(0xA0) frames — no dedicated timeseries wire frame is needed.

On Origin, a collection created with `WITH (engine='timeseries')` accepts
`ColumnarInsert` frames and stores rows through the timeseries-profiled
columnar engine.  Rows are immediately visible via pgwire `SELECT`.

The wire type used is `ColumnarInsertMsg` (opcode 0xA0), shared with plain
columnar sync.  See the **Columnar Insert Sync** section above for the full
message format, ACK flow, and Origin inbound path.

### Test coverage

`tests/sync_interop_timeseries.rs` contains two live-Origin gate tests:
- `timeseries_inserts_replicate_to_origin` — inserts 3 rows post-connect,
  waits ≤5 s, asserts 3 rows visible via `SELECT time` pgwire scan.
- `timeseries_pre_connection_inserts_sync_after_connect` — inserts rows before
  the sync task starts; verifies they replicate once the connection is
  established.

---

## Vector Insert/Delete Sync (PREVIEW)

Vector insert and delete sync replicates HNSW vector changes made on a Lite
collection to Origin using two dedicated wire frame pairs.

| Message type       | Opcode | Direction      | Handled by                                                  |
|--------------------|--------|----------------|-------------------------------------------------------------|
| `VectorInsert`     | 0xA2   | Lite → Origin  | `session_handler.rs` → `SyncSession::handle_vector_insert` |
| `VectorInsertAck`  | 0xA3   | Origin → Lite  | `dispatch_frame` → `SyncDelegate::acknowledge_vector_insert` |
| `VectorDelete`     | 0xA4   | Lite → Origin  | `session_handler.rs` → `SyncSession::handle_vector_delete` |
| `VectorDeleteAck`  | 0xA5   | Origin → Lite  | `dispatch_frame` → `SyncDelegate::acknowledge_vector_delete` |

### Wire format

`VectorInsertMsg` (defined in `nodedb-types/src/sync/wire/vector.rs`):
- `lite_id`: Lite instance identifier.
- `collection`: target collection name.
- `id`: document/vector ID string.
- `vector`: raw FP32 embedding coefficients.
- `dim`: stated dimensionality (must equal `vector.len()`).
- `field_name`: field name in multi-field collections (empty string for default).
- `batch_id`: monotonic per-collection ID for ACK correlation.

`VectorInsertAckMsg`:
- `collection`, `id`, `batch_id`: echo from the insert.
- `accepted`: true on success.
- `reject_reason`: failure detail, if any.

`VectorDeleteMsg`:
- `lite_id`, `collection`, `id`, `field_name`, `batch_id`.

`VectorDeleteAckMsg`:
- `collection`, `id`, `batch_id`, `accepted`, `reject_reason`.

### Lite outbound path

`vector_insert_impl` / `vector_delete_impl` (in `src/nodedb/trait_impl/vector.rs`) enqueue
entries into `VectorOutbound` (`src/sync/vector_outbound.rs`).

`NodeDbLite` holds `Option<Arc<VectorOutbound>>` (present when sync is enabled).
`SyncDelegate::pending_vector_inserts` / `pending_vector_deletes` drain the queue;
the `delta_push_loop` in `src/sync/transport.rs` encodes each entry as a
`VectorInsert` or `VectorDelete` frame and sends it to Origin.

On `VectorInsertAck` / `VectorDeleteAck`, `dispatch_frame` calls
`delegate.acknowledge_vector_insert(batch_id)` or `acknowledge_vector_delete(batch_id)`,
removing the entry from the queue.  On send failure the entry is re-queued via
`delegate.reject_vector_insert` / `reject_vector_delete`.

### Origin inbound path

`session_handler.rs` intercepts `SyncMessageType::VectorInsert` and
`SyncMessageType::VectorDelete` before the generic `process_frame` call.

For inserts, `SharedStateVectorDispatcher` (in `sync/vector_handler.rs`):
1. Validates dimension consistency.
2. Assigns a stable surrogate for `(collection, id)` via `SurrogateAssigner::assign`
   (WAL-durable, idempotent).
3. Dispatches `PhysicalPlan::Vector(VectorOp::Insert)` to the Data Plane via the
   SPSC bridge with `EventSource::CrdtSync` (suppresses AFTER triggers on synced data).

For deletes, the dispatcher dispatches `PhysicalPlan::Vector(VectorOp::DeleteBySurrogate)`,
which the Data Plane resolves to the internal HNSW node ID via the `surrogate_to_local`
map.  A delete for an unknown surrogate is a silent no-op (idempotent).

`process_frame` in `session/dispatch.rs` contains explicit `None` arms for all four
vector message types so the generic `_` branch never silently absorbs them.

### Test coverage

`tests/sync_interop_vector.rs` contains three live-Origin gate tests:
- `vector_inserts_replicate_to_origin` — inserts 5 vectors post-connect, waits ≤5 s,
  probes each by point-scan, then asserts nearest-neighbour query returns the expected id.
- `vector_delete_replicates_to_origin` — inserts a target vector, confirms it appears,
  deletes it on Lite, waits ≤5 s, asserts it is no longer visible.
- `vector_pre_connection_inserts_sync_after_connect` — inserts vectors before the sync
  task starts; verifies they replicate once the connection is established.

Unit tests for `VectorOutbound` live in `src/sync/vector_outbound.rs`
(enqueue/drain/ack/requeue invariants, batch-ID monotonicity).
`vector_handler.rs` contains `SyncSession` unit tests covering unauthenticated
rejection, dimension mismatch, successful dispatch, and dispatch-failure paths.

## FTS Index/Delete Sync (PREVIEW)

FTS index sync replicates BM25 full-text index changes made on a Lite
collection to Origin using two dedicated wire frame pairs.

| Message type    | Opcode | Direction      | Handled by                                                |
|-----------------|--------|----------------|-----------------------------------------------------------|
| `FtsIndex`      | 0xA6   | Lite → Origin  | `session_handler.rs` → `SyncSession::handle_fts_index`   |
| `FtsIndexAck`   | 0xA7   | Origin → Lite  | `dispatch_frame` → `SyncDelegate::acknowledge_fts_index` |
| `FtsDelete`     | 0xA8   | Lite → Origin  | `session_handler.rs` → `SyncSession::handle_fts_delete`  |
| `FtsDeleteAck`  | 0xA9   | Origin → Lite  | `dispatch_frame` → `SyncDelegate::acknowledge_fts_delete`|

### Wire format

`FtsIndexMsg` (defined in `nodedb-types/src/sync/wire/fts.rs`):
- `lite_id`: Lite instance identifier.
- `collection`: target collection name.
- `doc_id`: document ID string.
- `text`: pre-concatenated text content (Lite concatenates all string-valued
  fields with spaces before enqueueing; no field name is transmitted).
- `batch_id`: monotonic per-collection ID for ACK correlation.

`FtsIndexAckMsg`:
- `collection`, `doc_id`, `batch_id`: echo from the index request.
- `accepted`: true on success.
- `reject_reason`: failure detail, if any.

`FtsDeleteMsg`:
- `lite_id`, `collection`, `doc_id`, `batch_id`.

`FtsDeleteAckMsg`:
- `collection`, `doc_id`, `batch_id`, `accepted`, `reject_reason`.

### Lite outbound path

`document_put_impl` (in `src/nodedb/trait_impl/document.rs`) calls
`index_document_text` on the FTS engine, which enqueues a `PendingFtsIndex`
entry into `FtsOutbound` (`src/sync/fts_outbound.rs`).  Similarly,
`document_delete_impl` enqueues a `PendingFtsDelete`.

`NodeDbLite` holds `Option<Arc<FtsOutbound>>` (present when sync is enabled).
`SyncDelegate::pending_fts_indexes` / `pending_fts_deletes` drain the queue;
the `delta_push_loop` in `src/sync/transport.rs` encodes each entry as an
`FtsIndex` or `FtsDelete` frame and sends it to Origin.

On `FtsIndexAck` / `FtsDeleteAck`, `dispatch_frame` calls
`delegate.acknowledge_fts_index(batch_id)` or `acknowledge_fts_delete(batch_id)`,
removing the entry from the queue.  On send failure the entry is re-queued via
`delegate.reject_fts_index` / `reject_fts_delete`.

### Origin inbound path

`session_handler.rs` intercepts `SyncMessageType::FtsIndex` and
`SyncMessageType::FtsDelete` before the generic `process_frame` call.

For index requests, `SharedStateFtsDispatcher` (in `sync/fts_handler.rs`):
1. Assigns a stable surrogate for `(collection, doc_id)` via `SurrogateAssigner::assign`
   (WAL-durable, idempotent).
2. Dispatches `PhysicalPlan::Text(TextOp::FtsIndexDoc)` to the Data Plane via the
   SPSC bridge with `EventSource::CrdtSync` (suppresses AFTER triggers on synced data).
3. Returns `FtsIndexAckMsg { accepted: true }` on success.

Empty text (`text.is_empty()`) is acknowledged without dispatching to avoid
inserting zero-length postings into the inverted index.

For delete requests, the dispatcher dispatches `PhysicalPlan::Text(TextOp::FtsDeleteDoc)`.
A delete for an unknown surrogate is a silent no-op (idempotent).

`process_frame` in `session/dispatch.rs` contains explicit `None` arms for all four
FTS message types so the generic `_` branch never silently absorbs them.

### Test coverage

`tests/sync_interop_fts.rs` contains three live-Origin gate tests:
- `fts_inserts_replicate_to_origin` — inserts 3 documents post-connect, waits ≤5 s,
  asserts `text_match` on Origin returns all 3.
- `fts_delete_replicates_to_origin` — inserts 3 documents (2 background + 1 target),
  confirms target appears, deletes it on Lite, waits ≤5 s, asserts target no longer
  visible while background documents remain.
- `fts_pre_connection_inserts_sync_after_connect` — inserts documents before the sync
  task starts; verifies they replicate once the connection is established.

Unit tests for `FtsOutbound` live in `src/sync/fts_outbound.rs`
(enqueue/drain/ack/requeue invariants, batch-ID monotonicity).
`fts_handler.rs` contains `SyncSession` unit tests covering unauthenticated
rejection, authenticated dispatch, empty-text no-dispatch, dispatch-failure, and
delete-ack paths.

---

## Spatial Insert/Delete Sync (PREVIEW)

Spatial insert and delete sync replicates R-tree geometry changes made on a Lite
collection to Origin using two dedicated wire frame pairs.

| Message type          | Opcode | Direction      | Handled by                                                       |
|-----------------------|--------|----------------|------------------------------------------------------------------|
| `SpatialInsert`       | 0xAA   | Lite → Origin  | `session_handler.rs` → `SyncSession::handle_spatial_insert`     |
| `SpatialInsertAck`    | 0xAB   | Origin → Lite  | `dispatch_frame` → `SyncDelegate::acknowledge_spatial_insert`   |
| `SpatialDelete`       | 0xAC   | Lite → Origin  | `session_handler.rs` → `SyncSession::handle_spatial_delete`     |
| `SpatialDeleteAck`    | 0xAD   | Origin → Lite  | `dispatch_frame` → `SyncDelegate::acknowledge_spatial_delete`   |

### Wire format

`SpatialInsertMsg` (defined in `nodedb-types/src/sync/wire/spatial.rs`):
- `lite_id`: Lite instance identifier.
- `collection`: collection name.
- `field`: geometry field name.
- `doc_id`: document identifier string.
- `geometry_bytes`: MessagePack-serialized `nodedb_types::geometry::Geometry`.
- `batch_id`: monotonically increasing batch counter for ack correlation.

`SpatialInsertAckMsg`:
- `collection`, `field`, `doc_id`, `batch_id`: echo of the request fields.
- `accepted`: `true` on success.
- `reject_reason`: `Some(String)` on rejection (auth failure, geometry
  deserialisation error, surrogate allocation failure, Data Plane error).

`SpatialDeleteMsg`:
- `lite_id`, `collection`, `field`, `doc_id`, `batch_id`.

`SpatialDeleteAckMsg`:
- `collection`, `field`, `doc_id`, `batch_id`, `accepted`, `reject_reason`.

### Lite-side flow

`NodeDbLite::spatial_insert(collection, field, doc_id, geometry)` writes the
entry to the local R-tree engine and enqueues an outbound record in
`SpatialOutbound` (`src/sync/spatial_outbound.rs`).  Similarly,
`spatial_delete` removes from the local R-tree and enqueues a delete record.

`NodeDbLite` holds `Arc<SpatialOutbound>`.  `SyncDelegate::pending_spatial_inserts`
and `pending_spatial_deletes` drain the queue.  The sync loop sends a
`SpatialInsert` or `SpatialDelete` frame for each pending entry and waits for
the corresponding ack.

On ack, `dispatch_frame` calls `delegate.acknowledge_spatial_insert(batch_id)`
(or `acknowledge_spatial_delete`), which removes the entry from the outbound
queue.  On rejection, the entry is re-queued for retry via `requeue_insert` /
`requeue_delete`.

### Origin-side flow

`session_handler.rs` intercepts `SyncMessageType::SpatialInsert` and
`SyncMessageType::SpatialDelete` before the generic dispatch path.  It calls
`SyncSession::handle_spatial_insert` / `handle_spatial_delete` with a
`SpatialDispatcher`.

`SharedStateSpatialDispatcher` (in `sync/spatial_handler.rs`) translates the
message into `PhysicalPlan::Spatial(SpatialOp::Insert)` or `SpatialOp::Delete`
and dispatches to the Data Plane via the SPSC bridge.

`CoreLoop::execute_spatial_insert` (in
`data/executor/handlers/spatial_sync.rs`) writes a minimal geometry document in
standard msgpack map format (via `nodedb_types::value_to_msgpack`) to the sparse
store and inserts a bounding-box entry into the per-field R-tree.  This mirrors
what a direct SQL `INSERT` does, so `st_dwithin` / `st_contains` queries on
Origin see the synced geometries.

`CoreLoop::execute_spatial_delete` removes the document from the sparse store
and removes the R-tree entry.

### Test coverage

`tests/sync_interop_spatial.rs` contains three live-Origin gate tests:
- `spatial_inserts_replicate_to_origin` — inserts 3 points post-connect, waits
  ≤5 s, asserts all 3 are returned by an `st_dwithin` query on Origin.
- `spatial_delete_replicates_to_origin` — inserts 3 points, confirms they
  appear, deletes the target on Lite, waits ≤5 s, asserts the target is gone
  while background points remain.
- `spatial_pre_connection_inserts_sync_after_connect` — inserts points before the
  sync task starts; verifies they replicate once the connection is established.

Unit tests for `SpatialOutbound` live in `src/sync/spatial_outbound.rs`
(enqueue/drain/ack/requeue invariants).  `spatial_handler.rs` contains
`SyncSession` unit tests covering unauthenticated rejection, geometry
deserialisation failure, successful dispatch, and dispatch-failure paths.
