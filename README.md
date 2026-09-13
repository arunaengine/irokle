# Irokle

Irokle is a signed Merkle-DAG operation log for invite-only topics. Application events and membership changes are stored as signed operations. The graph of operations can be used to derive current heads, a history of changes, summaries for syncing, and projections.

## Features

- Signed operations: every event or control change is signed by the peer that authored it.
- Topic membership: topics are not public broadcast channels; typed access is gated by the current signed member set.
- Paged sync: peers exchange summaries, requests with a receive credit, bounded pages of operations, and signed acknowledgements. The Iroh wire protocol is `irokle/sync/3`.
- Bounded fanout: topic replication is capped by `ReplicationPolicy::max_sync_peers` so a node does not sync with every member by default.
- Observability: sync status records expose pending obligations, failure counts, last errors, last success, the newest attempt, and per-state counts.
- Storage choices: `MemoryStorage` is available by default; `FjallStorage` is available behind the `fjall` feature.
- Iroh integration: the `iroh` feature syncs over `iroh::Endpoint` using `PeerId`/`NodeId` dialing.

## Minimal Example

```rust
use irokle::history::HistoryOrder;
use irokle::{Irokle, TopicConfig};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, irokle::Event, Deserialize, Serialize)]
#[irokle(type_id = "example.chat.message")]
struct ChatEvent {
    author: String,
    text: String,
}

fn main() -> irokle::Result<()> {
    let alice = Irokle::builder().build()?;
    let bob = Irokle::builder().build()?;

    let alice_topic = alice.create_topic::<ChatEvent>(TopicConfig {
        initial_peers: [bob.peer_id()].into(),
        ..TopicConfig::default()
    })?;

    alice_topic.publish(ChatEvent {
        author: "alice".into(),
        text: "hello".into(),
    })?;

    let bob_summary = bob.sync_summary(alice_topic.id())?;
    let data_for_bob = alice.plan_sync_data(bob.peer_id(), &bob_summary)?;
    let (bob_ack, _) = bob.receive_sync_data_from(alice.peer_id(), data_for_bob)?;
    alice.apply_sync_ack(&bob_ack)?;

    let bob_topic = bob.open_topic::<ChatEvent>(alice_topic.id())?;
    bob_topic.publish(ChatEvent {
        author: "bob".into(),
        text: "reply".into(),
    })?;

    for record in bob_topic.history(HistoryOrder::OldestFirst)? {
        println!("{}: {}", record.event.author, record.event.text);
    }

    Ok(())
}
```

This example uses the transport-neutral sync API directly. Iroh examples can use `sync_now(peer_id, topic_id)` instead.

`plan_sync_data` and `negotiate_sync` export the whole missing closure in one call; they are for small histories and tests, not bounded sync. A custom transport pages instead: `plan_sync_request` builds a request that walks no history and names the branch it plans on, and `Irokle::response_page` serves one causal page within the request's credit, reporting whether the goal holds more. `plan_sync_response_data` returns the same page without that flag. A request planned on another genesis is refused with `Error::StaleIncarnation`. Iroh uses these same planners.

Bob does not hold the topic before the first receive. Data for an unknown topic is staged first and becomes visible only when its history makes both Bob and the sender members, as it does here. See "Joining A Topic" below.

For persisted or replicated topics, set an explicit, stable `#[irokle(type_id = "...")]` identifier and preserve it across compatible releases. The derive fallback uses the Rust module path and type name, so crate renames, module moves, or type renames change the wire identifier and prevent opening or syncing existing topics as that event type.

## Topics And Membership

`TopicConfig::initial_peers` defines the initial signed member set. `Topic::add_peer` and `Topic::remove_peer` write membership control operations into the same DAG as application events.

When a node receives a topic for the first time, it can discover it through `list_topics()` and then open it with `open_topic::<E>(topic_id)` if its local peer is a current member. A node can reject membership with `Irokle::reject_topic(topic_id)` or `Topic::leave()`. Rejection is represented as a signed `RemovePeer` control operation, so other nodes can observe and sync the decision.

## Joining A Topic

Data for a topic that a node does not hold yet is not admitted right away. It is staged per sender and topic, apart from every topic query. Once the staged history contains the genesis and makes both the receiving node and the sender members, the whole history is admitted in one storage transaction. Before that, the node has no topic state, no history and sends no acknowledgement.

- `Irokle::receive_sync_outcome` returns `ReceiveOutcome::Acked { ack, evictions }` or `ReceiveOutcome::Staged(staged)`. `StagedTopic` reports the staged clock and the staged op and byte counts. It is a receipt, not an acknowledgement.
- `receive_sync_data_from` and `receive_sync_data_from_evicting` keep their signatures and return `Error::BootstrapPending { staged }` while data stays staged.
- Over Iroh, a staging node answers with a `SyncMessage::Receipt`. The inviter continues the next page from the receipt clock until the invitation arrives.
- Staging is bounded: 64 MiB in total, 32 MiB and 65536 ops per session, 64 sessions, 8 sessions per sender, and sessions idle for 10 minutes are dropped.

## Bounded Replication

`ReplicationPolicy::all()` means all current topic members are eligible sync targets, but the selected set is capped by `max_sync_peers`.

```rust
use irokle::{ReplicationPolicy, TopicConfig};

let config = TopicConfig {
    replication_policy: ReplicationPolicy::all().with_max_sync_peers(4),
    ..TopicConfig::default()
};
```

Peer selection is deterministic and combines ring neighbors with hash-ranked fill peers. The goal is bounded epidemic propagation: each node syncs with only a small overlapping subset, and state reaches the rest of the topic through repeated sync rounds.

## Iroh Sync

With the `iroh` feature, `Irokle::builder().with_net(endpoint)` configures the Irokle sync ALPN automatically. Normal use is NodeId-only:

```rust
use irokle::{Irokle, TopicConfig};
use serde::{Deserialize, Serialize};
use tokio::time::{Duration, timeout};

#[derive(Clone, Debug, irokle::Event, Deserialize, Serialize)]
#[irokle(type_id = "example.sync.event.v1")]
struct MyEvent;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .bind()
        .await?;
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .bind()
        .await?;

    timeout(Duration::from_secs(10), alice_endpoint.online()).await?;
    timeout(Duration::from_secs(10), bob_endpoint.online()).await?;

    let alice = Irokle::builder().with_net(alice_endpoint).build()?;
    let bob = Irokle::builder()
        .with_peer_whitelist([alice.peer_id()])
        .with_net(bob_endpoint)
        .build()?;

    let topic = alice.create_topic::<MyEvent>(TopicConfig {
        initial_peers: [bob.peer_id()].into(),
        ..TopicConfig::default()
    })?;

    alice.sync_now(bob.peer_id(), topic.id()).await?;

    Ok(())
}
```

By default, Iroh auto-accept only admits brand-new topics from peers in `peer_whitelist`. The whitelist starts as `Some(empty)`, so add allowed peers with `with_peer_whitelist`, `add_peer_to_whitelist`, `add_peers_to_whitelist`, or `set_peer_whitelist`. Set the whitelist to `None` only when unknown-topic admission should be unrestricted. For production deployments, keep the Irokle sync ALPN dedicated to trusted peers and whitelist topic introducers explicitly.

Automatic acceptance requires a dedicated Irokle endpoint; `build()` rejects additional protocols configured through `with_alpn` or `with_alpns` while auto-accept is enabled. For multiple protocols, call `without_auto_accept()` after `with_net(endpoint)` and route incoming connections manually. Construction replaces the endpoint ALPN list with the builder-configured protocols plus Irokle, so include every required protocol in `with_alpns`.

Nodes speak the sync protocol `irokle/sync/3` (`irokle::sync::SYNC_PROTOCOL`, also the ALPN). A peer that only offers `irokle/sync/2` cannot connect, so upgrade every node of a deployment together. In version 3 a requester sends a `SyncRequest` with its branch (`genesis`) and a receive credit. The responder reads the whole request stream, then replies with every control message first, one bounded page of data per request, and a `SyncMessage::Page` that says whether more is left. A request for another branch fails for that topic and receives no data.

Manual syncs (`sync_now`, `sync_addr_now`, `sync_endpoint_now`, `sync_topic_now`) keep paging while each page makes progress, up to 64 pages. They return `Ok(())` only when the sync goal is reached. When the page budget runs out while work is still moving, they return an `io::Error` of kind `WouldBlock`. That is progress, not a failure: call again, or let the resync loop continue.

`sync_addr_now(endpoint_addr, topic_id)` remains available for explicit one-off manual dialing in local/offline setups. The peer registry API was removed; when discovery is configured, peers are identified by `PeerId`/Iroh `EndpointId`.

Iroh runtime behavior is configurable when defaults are not appropriate for the deployment:

```rust
use irokle::net::IrohRuntimeConfig;
use std::time::Duration;

let runtime = IrohRuntimeConfig {
    connect_timeout: Duration::from_secs(10),
    sync_io_timeout: Duration::from_secs(10),
    resync_interval: Duration::from_secs(15),
    ..IrohRuntimeConfig::default()
};

let node = irokle::Irokle::builder()
    .with_iroh_runtime_config(runtime)
    .with_net(endpoint)
    .build()?;
```

Use `shutdown_iroh().await` during orderly shutdown. It closes the endpoint and returns only when every task the net started has ended. It can be called more than once. `IrohNet::shutdown_with_timeout(timeout)` waits at most `timeout` and returns `ShutdownOutcome::Complete` or `ShutdownOutcome::Incomplete { running }`; after an incomplete result the net still owns the running tasks.

## Sync Failures And Status

Iroh-backed builders default to `WriteConcern::AsyncReplication` unless `with_write_concern` or `with_config` sets a different policy. Iroh nodes start a periodic resync loop whenever networking is configured; `without_auto_accept()` disables inbound auto-accept but does not disable outbound resync. The loop retries outstanding sync obligations and also performs bounded anti-entropy sync with the topic's selected peers. Publish with `WriteConcern::AsyncReplication` creates obligations for the bounded replication target set and wakes the same sync machinery. If the wake cannot start because no Tokio runtime is active, the obligation remains visible and sync status records the failure.

Applications can inspect sync state:

```rust
let statuses = node.sync_status(topic_id)?;
let counts = node.sync_state_counts(topic_id)?;
```

Each `SyncPeerStatus` includes `state`, `pending_obligations`, `failed_attempts`, `successful_attempts`, `last_attempt_ms`, `last_success_ms`, `last_error`, and `latest_attempt`. An attempt is identified by `(epoch, sequence)`, where the epoch is a durable counter that grows each time a net starts, so an older attempt cannot overwrite the state of a newer one.

A `SyncObligation` names the work one peer still owes for one topic. Its `target` is either `ObligationTarget::Clock(clock)`, one coalesced record that clears when a certified acknowledgement reaches the clock, or `ObligationTarget::Repair(ids)`, explicit operation ids that clear one by one. Acknowledgements certify one branch: they are signed with the topic genesis, and evidence for another genesis or without one certifies nothing.

## Storage Backends

`Storage` is implemented by `MemoryStorage` and, with the `fjall` feature, by `FjallStorage`. A custom backend must implement every required method. Methods that combine several reads or writes must do them in one lock or one transaction; the method documentation says which. In particular:

- `topic_view` reads state, heads, clock, tips, fingerprint, data epoch, pending holes and one peer's acknowledgement as one view.
- `actor_range` returns indexed positions of one actor after a sequence, for page planning.
- `peer_reached_op` and `peers_reached_op` read the op, the genesis and the acknowledgements from one view.
- `put_sync_obligation` and `clear_peer_sync_state` take the expected genesis and write nothing when it no longer matches.
- `sync_obligation_count` counts obligation records without decoding them where the backend can.
- `next_attempt_epoch` durably advances the attempt epoch.
- `stage_bootstrap_ops`, `staged_bootstrap_ops`, `promote_bootstrap`, `discard_bootstrap` and `expire_bootstrap` keep staged data apart from every topic query. `promote_bootstrap` admits the whole history and drops the staging in one transaction.

`FjallStorage` stores schema version 4. Opening a database at schema 1, 2 or 3 migrates it in place, one transaction per step, and each step checks the version again, so concurrent or repeated opens are safe. Acknowledgements from before schema 2 stay stored but are uncertified. A database with a newer, unknown schema version is refused.

## Disk Recovery

With `fjall` and `iroh`, durable recovery means reopening the same Fjall path and reusing the same Iroh `SecretKey`, because the Iroh key defines the node’s `PeerId`. Production applications should persist the Iroh secret in their normal secret-management system, restrict filesystem permissions for local key files, and back up the key with the Fjall database path.

See `examples/iroh_fjall_recovery.rs` for a complete example that creates a topic, closes the endpoint, reopens the database with the same key, lists recovered topics, and reads typed history.

## Examples

- `examples/basic.rs`: in-memory typed events plus transport-neutral sync planning, including the receive outcome of a first receive.
- `examples/rdf.rs`: observed-remove RDF projection implemented as application code on top of event history.
- `examples/iroh_chat.rs`: NodeId-only Iroh chat sync using discovery.
- `examples/iroh_topic_intro.rs`: introduces a peer to a topic, opens it on the receiver, then rejects membership.
- `examples/iroh_fjall_recovery.rs`: reopens an Iroh/Fjall node from disk with the same Iroh secret key.
- `examples/iroh_runtime_config.rs`: builds an Iroh node with custom runtime timeouts and resync interval.

Run examples with features as needed:

```bash
cargo run --features iroh --example iroh_chat
cargo run --features iroh --example iroh_topic_intro
cargo run --features 'iroh fjall' --example iroh_fjall_recovery
```
