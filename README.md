# Irokle

Irokle is a signed Merkle-DAG operation log for invite-only topics. It stores typed application events and membership changes as signed operations, then synchronizes them between authorized peers.

## Features

- Signed, typed events and membership changes.
- Invite-only topics with explicit peer admission.
- Bounded, paged synchronization and replication fanout.
- In-memory storage by default and durable Fjall storage with the `fjall` feature.
- Iroh transport with the `iroh` feature and `irokle/sync/2` ALPN.
- Sync status, retry tracking, and graceful network shutdown.

## Minimal Use

```rust
use irokle::history::HistoryOrder;
use irokle::{Irokle, TopicConfig};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, irokle::Event, Deserialize, Serialize)]
#[irokle(type_id = "example.note")]
struct Note {
    text: String,
}

fn main() -> irokle::Result<()> {
    let node = Irokle::builder().build()?;
    let topic = node.create_topic::<Note>(TopicConfig::default())?;

    topic.publish(Note {
        text: "hello".into(),
    })?;

    for record in topic.history(HistoryOrder::OldestFirst)? {
        println!("{}", record.event.text);
    }

    Ok(())
}
```

Use a stable `#[irokle(type_id = "...")]` value for persisted or replicated event types.

## Topics And Sync

`TopicConfig::initial_peers` defines the initial member set. Membership changes are signed operations in the same DAG as application events.

The transport-neutral API supports custom sync implementations. With `iroh`, configure an endpoint through `Irokle::builder().with_net(endpoint)` and call `sync_now(peer_id, topic_id)`. Unknown topics are accepted only from configured whitelist peers by default.

Replication is bounded by `ReplicationPolicy::max_sync_peers`. Applications can inspect peer progress with `sync_status` and `sync_state_counts`.

## Storage

`MemoryStorage` is available by default. Enable `fjall` for durable storage. Durable Iroh nodes must reopen the same storage path and reuse the same Iroh secret key.

See [CONTRIBUTING.md](CONTRIBUTING.md) for development and verification commands.
