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

## Definition Sync (EXPERIMENTAL — NOT IN 0.1.0-beta.1)

Definition sync carries function, trigger, and procedure definitions from
Origin to connected Lite clients via `DefinitionSync` (frame opcode `0x70`,
`SyncMessageType::DefinitionSync`).

### Lite side (wired, receive-only)

Lite's `dispatch_frame` in `nodedb-lite/nodedb-lite/src/sync/transport.rs`
matches on `SyncMessageType::DefinitionSync` (lines 317–319) and calls
`delegate.import_definition(&msg)`.  `import_definition` is implemented in
`nodedb-lite/nodedb-lite/src/nodedb/sync_delegate.rs` (line 82) and handles
both `"put"` (create/replace) and `"delete"` (drop) actions with msgpack
payloads.  The wire type `DefinitionSyncMsg` is defined in
`nodedb/nodedb-types/src/sync/wire/timeseries.rs:57` with opcode registered at
`nodedb/nodedb-types/src/sync/wire/frame.rs:42`.

### Origin side (not wired)

No code in `nodedb/nodedb/src/` constructs or sends a `DefinitionSyncMsg`.
A grep of `nodedb/nodedb/src/control/server/sync/` and every DDL handler
returns zero hits for `DefinitionSync`, `DefinitionSyncMsg`, or the `0x70`
opcode.  The sync session handler (`session_handler.rs`), the DDL post-apply
dispatcher (`async_dispatch.rs`), and the CRDT delivery path (`dlq.rs`,
`listener.rs`) have no emission path for definition changes.

### Placeholder tests

`nodedb-lite/nodedb-lite/tests/definition_sync_interop.rs` contains four
`#[ignore]` tests covering function put, function delete, trigger put, and
procedure put.

### Promotion criteria

Definition sync can be promoted from EXPERIMENTAL to PREVIEW when:

1. Origin's DDL commit path for `CREATE FUNCTION`, `CREATE TRIGGER`, and
   `CREATE PROCEDURE` constructs a `DefinitionSyncMsg` and broadcasts it to
   all sessions subscribed to the affected namespace.
2. The corresponding `DROP` paths emit `DefinitionSyncMsg` with
   `action = "delete"`.
3. `tests/definition_sync_interop.rs::definition_sync_function_put` passes
   against `OriginServer::spawn()` without `#[ignore]`.
4. `docs/lite-support-matrix.md` is updated accordingly.

---

## Array Sync (EXPERIMENTAL — NOT IN 0.1.0-beta.1)

Array sync uses a dedicated wire sub-protocol layered on top of the standard
sync session.  The message types involved are:

| Message type            | Direction       | Handled by                          |
|-------------------------|-----------------|-------------------------------------|
| `ArraySchema`           | Lite → Origin   | `OriginArrayInbound::handle_schema` |
| `ArraySnapshot`         | Lite → Origin   | `OriginArrayInbound::handle_snapshot_header` |
| `ArraySnapshotChunk`    | Lite → Origin   | `OriginArrayInbound::handle_snapshot_chunk` |
| `ArrayAck`              | Lite → Origin   | `OriginArrayInbound::handle_ack`    |
| `ArrayCatchupRequest`   | Lite → Origin   | `OriginArrayInbound::handle_catchup_request` |
| `ArrayDelta`            | Origin → Lite   | **NOT HANDLED** (see below)         |
| `ArrayDeltaBatch`       | Origin → Lite   | **NOT HANDLED** (see below)         |
| `ArrayReject`           | Origin → Lite   | Lite inbound — wired in-process only |

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

### Missing Lite-side wiring

`nodedb-lite/nodedb-lite/src/sync/client/receive.rs` does not match on
`SyncMessageType::ArrayDelta` or `SyncMessageType::ArrayDeltaBatch`.  Those
frame types fall through to the catch-all arm and are logged as unexpected.
No cell is applied, no ack is sent, and no convergence occurs.

Until this receive path is wired, the full round-trip
(Lite → Origin → Lite) cannot be asserted over a real transport.

### What the simulated tests cover

The files `tests/array_sync_basic.rs`, `tests/array_sync_bitemporal.rs`,
`tests/array_sync_catchup.rs`, `tests/array_sync_concurrent_writers.rs`,
`tests/array_sync_gdpr_erase.rs`, `tests/array_sync_reject.rs`, and
`tests/array_sync_schema.rs` exercise Lite's inbound and outbound handlers
in-process — they never open a WebSocket to a live Origin node.  Each file
carries a module-level doc comment explicitly stating this scope.

### Real-transport placeholder

`tests/array_sync_interop.rs` contains two `#[ignore]` tests that document
what end-to-end validation looks like.  Remove `#[ignore]` once the Lite
receive path is wired.

### Promotion criteria

Array sync can be promoted from EXPERIMENTAL to PREVIEW when:

1. `nodedb-lite/src/sync/client/receive.rs` handles `ArrayDelta` and
   `ArrayDeltaBatch` and routes them to the local array engine.
2. `tests/array_sync_interop.rs::array_interop_put_roundtrip` passes against
   `OriginServer::spawn()` without `#[ignore]`.
3. `docs/lite-support-matrix.md` is updated accordingly.
