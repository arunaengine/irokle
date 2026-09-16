// SPDX-License-Identifier: MIT OR Apache-2.0
//! Iroh chat example using NodeId-only discovery-backed sync.

use irokle::history::HistoryOrder;
use irokle::{Irokle, TopicConfig};
use serde::{Deserialize, Serialize};
use tokio::time::{Duration, timeout};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .alpns(vec![irokle::net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await?;
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .alpns(vec![irokle::net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await?;
    timeout(Duration::from_secs(10), alice_endpoint.online()).await?;
    timeout(Duration::from_secs(10), bob_endpoint.online()).await?;

    let alice = Irokle::builder().with_net(alice_endpoint).build()?;
    let bob = Irokle::builder()
        .with_peer_whitelist([alice.peer_id()])
        .with_net(bob_endpoint)
        .build()?;

    let alice_topic = alice.create_topic::<ChatEvent>(TopicConfig {
        initial_peers: [bob.peer_id()].into(),
        ..TopicConfig::default()
    })?;
    alice_topic.publish(ChatEvent {
        author: "alice".into(),
        text: "hello over iroh".into(),
    })?;

    timeout(
        Duration::from_secs(10),
        sync_until_done(&alice, bob.peer_id(), alice_topic.id()),
    )
    .await??;

    let bob_topic = bob.open_topic::<ChatEvent>(alice_topic.id())?;
    bob_topic.publish(ChatEvent {
        author: "bob".into(),
        text: "received and replying".into(),
    })?;

    timeout(
        Duration::from_secs(10),
        sync_until_done(&bob, alice.peer_id(), alice_topic.id()),
    )
    .await??;

    let history = alice_topic.history(HistoryOrder::OldestFirst)?;
    for record in history {
        println!("{}: {}", record.event.author, record.event.text);
    }

    alice.shutdown_iroh().await;
    bob.shutdown_iroh().await;

    Ok(())
}

/// Sync until the goal is reached. `WouldBlock` means a page budget ran out
/// while the sync was still making progress, so it is called again.
async fn sync_until_done(
    node: &Irokle,
    peer: irokle::PeerId,
    topic: irokle::TopicId,
) -> std::io::Result<()> {
    loop {
        match node.sync_now(peer, topic).await {
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
            result => return result,
        }
    }
}

#[derive(Clone, Debug, irokle::Event, Deserialize, Serialize)]
struct ChatEvent {
    author: String,
    text: String,
}
