# Irokle

Irokle is a Git-like Merkle-DAG of signed operations for topics. Operations form the authoritative history; indexes, reducers, clocks, materialized views, and sync summaries are derived from accepted ops.

## Core Model

- Signed ops: every application event or control change is wrapped in a signed operation. The operation body names the topic, actor, dependencies, generation, and payload.
- Operation identity: `OpId` is derived from the canonical signed op bytes, including dependencies. The `id` field on an operation envelope is a validated field: readers must recompute the id and reject mismatches.
- Dependencies: the Merkle-DAG defines causal structure. Changing event bytes, topic, actor, signature, generation, or dependencies changes the derived `OpId`.
- Actor/vector clocks: actor sequence clocks and vector clocks are derived indexes for admission, querying, reducers, and synchronization. They are not operation identity.

## Topics And Membership

Topics are invite-only peer sets. A topic is not a global broadcast channel; peers replicate a topic after they receive the topic material or are otherwise granted membership.

Typed topic access is also membership-gated: `open_topic::<E>` returns `NotTopicMember` when the local signer is not in the topic's current member set. Raw topic access remains available for local restore, inspection, and validation of stored signed operations.

Membership changes are topic control ops. Control history lives in the same Merkle-DAG model as application events, so peers can validate membership evolution from signed operations instead of relying on out-of-band mutable state.

## Events

Typed events implement an `Event` trait with `TYPE_ID` only. There is no schema version field in the core event trait. Most applications can derive the trait:

```rust
#[derive(irokle::Event, serde::Serialize, serde::Deserialize)]
#[irokle(type_id = "example.chat.message")]
struct ChatEvent {
    author: String,
    text: String,
}
```

If the `irokle` crate is renamed, use `#[irokle(crate = "path::to::irokle")]` alongside `type_id`.

The derive uses the `Event` trait's postcard-backed default encoding and decoding. The crate still exposes the lower-level `Event` trait so applications can choose their own canonical event encoding.

Backward compatibility is the application's responsibility. Applications should keep old decoders/reducers as long as old events need to be interpreted. If a peer sees an unknown future event type, typed reducers may have to stop or skip according to application policy, but raw DAG synchronization can still continue because sync moves signed operation bytes and metadata rather than typed reducer output.

## Publishing

Write concern is chosen per publish. `publish` uses the topic or node default, while `publish_with` accepts publish options such as a per-event write concern. The core crate currently records the local operation only; it does not perform transport waits or synthesize acknowledgement latency. Network integrations should turn non-local write concerns into sync obligations and complete those obligations from real `SyncAck` messages.

## Storage

The storage layer is a trait, not a hard-coded database. `Storage` stores signed ops, metadata, heads, actor indexes, topic state, peer acknowledgements, and sync obligations. Most applications construct a node with `Irokle::new(config)` for memory-backed storage or `Irokle::with_storage(storage, config)` for a custom backend. With the `fjall` feature enabled, `Irokle::open_fjall(path, config)` opens persistent storage directly.

Admission is serialized by the node operation log, so cloned handles for the same node share one critical section for actor-tip/head reads, local op creation, validation, head/index/clock updates, topic-state materialization, and durable admission. Storage backends expose a semantic admitted-op commit that writes the op, metadata/indexes, heads, and topic state as one backend operation. Fjall v0 relies on that single `records` keyspace batch with `SyncAll` persistence for admission atomicity; it does not yet include a startup rebuild path for repairing manually corrupted or partially written indexes outside Fjall's batch guarantees.

Available backends:

- `MemoryStorage`: in-memory backend for tests, examples, and ephemeral nodes.
- `FjallStorage`: feature-gated persistent storage using reduced keyspaces and independent partition handles instead of a global storage mutex.

## Synchronization

Sync is transport-neutral messages and handlers, not a `Transport` trait. The protocol surface is message-shaped: `SyncOpen`, `SyncSummary`, `SyncData`, `SyncAck`, `SyncReport`. Normal applications use the `Irokle` facade methods such as `sync_summary`, `plan_sync_data`, `receive_sync_data`, and `apply_sync_ack`; `SyncEngine` is an advanced internal building block for tests and custom integrations.

A typical exchange is:

1. A peer sends `SyncOpen` for a topic.
2. The remote peer replies with `SyncSummary`, including heads, actor clocks, and known operation ids.
3. The sender calls `irokle.plan_sync_data(peer_id, &summary)` to plan topological `SyncData` for operations the remote does not know. Planning uses the remote have-set/known-op intersection to send only wanted operations and returns no topic operations for peers that are not current topic members.
4. The receiver calls `irokle.receive_sync_data(local_peer_id, data)` to validate dependency availability, admit accepted operations, and return `SyncAck`.
5. Peers call `irokle.apply_sync_ack(&ack)` and inspect `irokle.sync_report(peer_id, topic_id)` for acknowledgement indexes and outstanding sync obligations.

Iroh integration targets `iroh` `1.0.0-rc.0` and moves these sync messages over streams using an existing `iroh::Endpoint`. `IrohNet::new(endpoint, irokle)` holds an `Irokle` handle and uses the facade methods for inbound sync handling. Irokle should not own a second abstract transport stack; network integrations only need to encode/decode `SyncMessage` values and carry them over their stream/session machinery. Connections should be reused per peer/topic where possible rather than opened for every sync message.

## RDF As An Advanced Example

RDF is not part of the core crate API and has no exported `irokle::rdf` module. `examples/rdf.rs` defines its own `RdfEvent`, `Quad`, state, and reducer as an advanced example of building application semantics on top of Irokle metadata.

The RDF example reducer is an observed-remove projection over RDF quads. Remove events should remove only quad observations that were visible to the removing op.

The intended reducer model uses operation metadata `observed_clock` plus per-quad clocks. It does not need to keep per-quad operation provenance forever: each quad tracks enough actor/vector-clock information to know which observations a remove saw. A concurrent add that was not included in the remove operation's observed clock survives that remove.

## Status

This is the first production-oriented iteration of Irokle's public API and architecture. The core direction is signed Merkle-DAG operations, invite-only topic replication, typed events, storage-backed admission, and transport-neutral sync.

Future work, where not already implemented in a given checkout, includes compaction, snapshots, persistent backend hardening, and full Iroh router integration. The examples intentionally prefer small end-to-end API sketches over complete production wiring.

## Examples

- `examples/basic.rs` derives `irokle::Event` for a postcard-encoded `ChatEvent`, creates two in-memory nodes, invites Bob at topic creation, syncs Alice's topic data to Bob with `Irokle::plan_sync_data`, publishes from both peers, and demonstrates sync message byte framing.
- `examples/rdf.rs` is an advanced, example-only RDF projection with local event/reducer types. It demonstrates the observed-remove rule where a concurrent add survives a remove that did not observe it.
- `examples/iroh_chat.rs` is gated by the `iroh` feature. It creates two Iroh endpoints, wraps them with Irokle's `IrohNet`, creates a chat topic, exchanges sync messages, publishes from both peers, and reads typed history.
