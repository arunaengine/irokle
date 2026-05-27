# Irokle

Irokle is a signed Merkle-DAG operation log for invite-only topics. Application events and membership changes are stored as signed operations. The graph of operations can be used to derive current heads, a history of changes, summaries for syncing, and projections.

## Features

- Signed operations: every event or control change is signed by the peer that authored it.
- Topic membership: topics are not public broadcast channels; typed access is gated by the current signed member set.
- Deterministic sync: peers exchange summaries, missing operation closures, requests, and signed acknowledgements.
- Bounded fanout: topic replication is capped by `ReplicationPolicy::max_sync_peers` so a node does not sync with every member by default.
- Observability: sync status records expose pending obligations, failure counts, last errors, last success, and per-state counts.
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
    let bob_ack = bob.receive_sync_data_from(alice.peer_id(), data_for_bob)?;
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

## Topics And Membership

`TopicConfig::initial_peers` defines the initial signed member set. `Topic::add_peer` and `Topic::remove_peer` write membership control operations into the same DAG as application events.

When a node receives a topic for the first time, it can discover it through `list_topics()` and then open it with `open_topic::<E>(topic_id)` if its local peer is a current member. A node can reject membership with `Irokle::reject_topic(topic_id)` or `Topic::leave()`. Rejection is represented as a signed `RemovePeer` control operation, so other nodes can observe and sync the decision.

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

`sync_addr_now(endpoint_addr, topic_id)` remains available for explicit one-off manual dialing in local/offline setups. The peer registry API was removed; when discovery is configured, peers are identified by `PeerId`/Iroh `EndpointId`.

Iroh runtime behavior is configurable when defaults are not appropriate for the deployment:

```rust
use irokle::net::IrohRuntimeConfig;
use std::time::Duration;

let runtime = IrohRuntimeConfig {
    connect_timeout: Duration::from_secs(10),
    sync_io_timeout: Duration::from_secs(10),
    resync_interval: Duration::from_secs(15),
};

let node = irokle::Irokle::builder()
    .with_iroh_runtime_config(runtime)
    .with_net(endpoint)
    .build()?;
```

Use `shutdown_iroh().await` during orderly shutdown to close the endpoint and abort tracked background accept/resync tasks.

## Sync Failures And Status

Iroh-backed builders default to `WriteConcern::AsyncReplication` unless `with_write_concern` or `with_config` sets a different policy. Iroh nodes start a periodic resync loop whenever networking is configured; `without_auto_accept()` disables inbound auto-accept but does not disable outbound resync. The loop retries outstanding sync obligations and also performs bounded anti-entropy sync with the topic's selected peers. Publish with `WriteConcern::AsyncReplication` creates obligations for the bounded replication target set and wakes the same sync machinery. If the wake cannot start because no Tokio runtime is active, the obligation remains visible and sync status records the failure.

Applications can inspect sync state:

```rust
let statuses = node.sync_status(topic_id)?;
let counts = node.sync_state_counts(topic_id)?;
```

Each `SyncPeerStatus` includes `state`, `pending_obligations`, `failed_attempts`, `successful_attempts`, `last_attempt_ms`, `last_success_ms`, and `last_error`.

## Disk Recovery

With `fjall` and `iroh`, durable recovery means reopening the same Fjall path and reusing the same Iroh `SecretKey`, because the Iroh key defines the node’s `PeerId`. Production applications should persist the Iroh secret in their normal secret-management system, restrict filesystem permissions for local key files, and back up the key with the Fjall database path.

See `examples/iroh_fjall_recovery.rs` for a complete example that creates a topic, closes the endpoint, reopens the database with the same key, lists recovered topics, and reads typed history.

## Examples

- `examples/basic.rs`: in-memory typed events plus transport-neutral sync planning.
- `examples/rdf.rs`: observed-remove RDF projection implemented as application code on top of event history.
- `examples/iroh_chat.rs`: NodeId-only Iroh chat sync using discovery.
- `examples/iroh_topic_intro.rs`: introduces a peer to a topic, opens it on the receiver, then rejects membership.
- `examples/iroh_fjall_recovery.rs`: reopens an Iroh/Fjall node from disk with the same Iroh secret key.

Run examples with features as needed:

```bash
cargo run --features iroh --example iroh_chat
cargo run --features iroh --example iroh_topic_intro
cargo run --features 'iroh fjall' --example iroh_fjall_recovery
```
