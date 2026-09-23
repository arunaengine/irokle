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

## Trust And Consistency Model

These are the guarantees and limits applications should design around.

- Every member is an administrator. Any member can add or remove peers and change the replication policy. `Irokle::seal_topic` blocks genesis resets on a node.
- A smaller genesis replaces the current one only if it names exactly the same initial peers, its author among them. This covers two nodes creating one topic at once. Every genesis in a chain shares that set, so every replica makes the same choice whatever order records arrive in, and a reset cannot change who the initial members are. The discarded payloads are reported as `TopicEviction`s.
- Revocation is causal. A removed peer's operation stays valid when its dependencies come before the removal. Removal does not delete data the peer already holds, and former members can still see topic summaries.
- Two members that remove each other concurrently keep different views of each other. Other members see both removals and agree. Irokle does not reconcile the two removed peers automatically.
- One secret key must drive one store. If a writer identity signs two different operations at one actor position, replicas keep whichever arrived first and refuse the other. Acknowledgements then certify only the ancestry of the heads the receiving node holds, so a fork never marks an absent operation as delivered.
- History order is deterministic for one set of operations. It is not append-stable: a late concurrent operation can sort before events a consumer already processed. Reducers must commute or rebuild their projection.

Read history incrementally with `Topic::history_page`. It reads one snapshot, only visits operations after the cursor, and returns the `HistoryCursor` that covers exactly the returned page. A cursor from a replaced genesis fails with `Error::StaleIncarnation`, and the consumer rebuilds from `Topic::history`. `Topic::history_entries` decodes each event on its own, so one undecodable payload is reported with its operation id instead of failing the whole read.

Buffered operations that wait for missing dependencies expire after `storage::PENDING_IDLE_MS`. They were never admitted or acknowledged, so a later sync can send them again. The Iroh sweep runs this expiry, and other transports call `Irokle::expire_pending`.

A topic holds at most `sync::MAX_TOPIC_ACTORS` writers, so its sync summary always fits one frame. Every build refuses an operation too large for one sync frame.

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

Durable Iroh nodes must reopen the same Fjall path and reuse the same Iroh secret key. The key defines the node's peer identity, so applications should store it through their normal secret-management system and back it up with the database. Never run two stores with one key: restoring an old backup next to a live store forks the writer's actor chain.

`FjallStorage::open` persists every commit with `SyncAll`. With `PersistMode::Buffer`, an acknowledgement can precede durability until the application calls `persist`. An uncertain commit returns `Error::ReopenRequired`, and the application must reopen the store before writing again. A schema 1 store is upgraded in bounded steps when it is opened, and an interrupted upgrade continues on the next open.

`Irokle::recheck_topics` and the integrity scan check that every stored record and dependency is present. They do not decode and rehash every signed payload, so they are not a cryptographic scrub of a backup.

The transport-neutral receive methods and `Storage::put_admitted_batch` are trusted boundaries. The Iroh adapter authenticates the sending peer, applies the introduction whitelist and enforces frame and session limits. A custom transport must bind the peer id it passes to an authenticated connection and apply the same limits. A custom storage backend must keep the atomic snapshot and commit checks each `Storage` method documents.

## Development

The workspace targets Rust 1.97.1. See [CONTRIBUTING.md](CONTRIBUTING.md) for formatting, linting, tests, documentation, and network-integration checks. Repository-owned code follows [STYLE.md](STYLE.md).
