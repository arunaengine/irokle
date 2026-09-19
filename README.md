# Irokle

Irokle is a signed Merkle-DAG operation log for invite-only topics. Application events and membership changes are stored as signed operations. The graph provides current heads, ordered history, sync summaries, and application-defined projections.

## Features

- Signed operations: every event and membership change is signed by its author.
- Typed topics: event types are checked when topics are created or opened.
- Invite-only membership: topic access follows the signed member set.
- Bounded sync: peers exchange paged causal data within explicit limits.
- Bounded fanout: replication targets are capped per topic.
- Storage choices: memory is available by default, with Fjall behind the `fjall` feature.
- Iroh integration: the `iroh` feature provides peer-to-peer transport and background resync.
- Observability: applications can inspect peer state, attempts, failures, and pending work.

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
    let data = alice.plan_sync_data(bob.peer_id(), &bob_summary)?;
    let (ack, _) = bob.receive_sync_data_from(alice.peer_id(), data)?;
    alice.apply_sync_ack(&ack)?;

    let bob_topic = bob.open_topic::<ChatEvent>(alice_topic.id())?;
    for record in bob_topic.history(HistoryOrder::OldestFirst)? {
        println!("{}: {}", record.event.author, record.event.text);
    }

    Ok(())
}
```

This example uses the transport-neutral sync API. With Iroh, applications normally call `sync_now(peer_id, topic_id)` instead.

Use an explicit, stable `#[irokle(type_id = "...")]` for persisted or replicated events. The fallback derives an identifier from the Rust module path and type name, so moving or renaming the type changes its identity.

## Topics And Membership

`TopicConfig::initial_peers` defines the initial signed member set. The creator is always part of the topic. `Topic::add_peer` and `Topic::remove_peer` record membership changes in the same DAG as application events.

A member can discover received topics through `list_topics()` and open one with `open_topic::<E>(topic_id)`. Opening checks the event type identifier and current membership. A node can reject an invitation with `Irokle::reject_topic(topic_id)` or leave an open topic with `Topic::leave()`.

Membership decisions are causal. Events remain valid only when their authors were members at the operation's position in the DAG, and receiving data does not bypass those checks.

## Joining A Topic

Data for an unknown topic is staged separately from visible topic state. It becomes visible only when the staged history contains the topic genesis and proves that both the receiver and sender are members.

`Irokle::receive_sync_outcome` returns either an acknowledgement or a `StagedTopic` receipt. The receipt reports staged operation and byte counts but does not certify admission. Over Iroh, the receiver sends the equivalent receipt so the inviter can continue transferring the invitation history.

Staging is bounded per store, sender, and session. A fragment that cannot fit is refused without partially admitting it. Publication is atomic from the application's perspective: topic queries see either no topic or the admitted topic state.

## Bounded Replication

`ReplicationPolicy::all()` makes every member eligible, while `max_sync_peers` limits the selected target set.

```rust
use irokle::{ReplicationPolicy, TopicConfig};

let config = TopicConfig {
    replication_policy: ReplicationPolicy::all().with_max_sync_peers(4),
    ..TopicConfig::default()
};
```

Selection is deterministic and combines ring neighbours with hash-ranked fill peers. Nodes therefore synchronize with a small overlapping subset instead of every member, while repeated rounds propagate state across the topic.

Sync requests and responses are paged. Page credit bounds data returned to the requester, and causal dependencies are included before the operations that need them. Manual sync calls continue while pages make progress and report `WouldBlock` when more bounded work remains.

## Iroh Sync

Enable the `iroh` feature and provide an `iroh::Endpoint` to the builder:

```rust
use irokle::{Irokle, TopicConfig};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, irokle::Event, Deserialize, Serialize)]
#[irokle(type_id = "example.sync.event")]
struct SyncEvent;

async fn connect(
    alice_endpoint: iroh::Endpoint,
    bob_endpoint: iroh::Endpoint,
) -> Result<(), Box<dyn std::error::Error>> {
    let alice = Irokle::builder().with_net(alice_endpoint).build()?;
    let bob = Irokle::builder()
        .with_peer_whitelist([alice.peer_id()])
        .with_net(bob_endpoint)
        .build()?;

    let topic = alice.create_topic::<SyncEvent>(TopicConfig {
        initial_peers: [bob.peer_id()].into(),
        ..TopicConfig::default()
    })?;

    alice.sync_now(bob.peer_id(), topic.id()).await?;
    Ok(())
}
```

The Irokle ALPN is `irokle/sync/2`. The builder configures it automatically when networking is enabled.

Automatic acceptance admits unknown topics only from peers in `peer_whitelist`. The whitelist starts empty. Add trusted introducers through the builder or node whitelist methods. Set the whitelist to `None` only when unrestricted topic introduction is intended.

Automatic acceptance requires a dedicated Irokle endpoint. Applications sharing an endpoint with other protocols can call `without_auto_accept()` and route incoming connections themselves.

`sync_addr_now` supports explicit one-off dialing. Other manual sync methods use known peer identities or configured discovery. Runtime timeouts and resync intervals can be changed with `IrohRuntimeConfig`.

Call `shutdown_iroh().await` during orderly shutdown. It closes the endpoint and waits for network tasks to finish. `shutdown_with_timeout` provides a bounded wait and reports whether tasks remain.

## Sync Status

Iroh-backed nodes run periodic bounded anti-entropy sync. Publishing with asynchronous replication records obligations for selected peers and wakes the same sync machinery. Failed attempts remain visible and are retried.

```rust
let statuses = node.sync_status(topic_id)?;
let counts = node.sync_state_counts(topic_id)?;
```

Each peer status reports its state, pending obligations, attempt counters, timestamps, latest attempt, and last error. Acknowledgements are signed and bound to one topic branch, so evidence for another branch does not clear pending work.

## Storage And Recovery

`MemoryStorage` is available without feature flags. Enable `fjall` for durable storage. Custom backends implement the `Storage` contract and must preserve the atomic read and write boundaries documented by each method.

Durable Iroh nodes must reopen the same Fjall path and reuse the same Iroh secret key. The key defines the node's peer identity, so applications should store it through their normal secret-management system and back it up with the database.

## Development

The workspace targets Rust 1.97.1. See [CONTRIBUTING.md](CONTRIBUTING.md) for formatting, linting, tests, documentation, and network-integration checks. Repository-owned code follows [STYLE.md](STYLE.md).
