// SPDX-License-Identifier: MIT OR Apache-2.0
//! Introduce a peer to a topic, let it inspect the topic, then reject membership.

use irokle::history::HistoryOrder;
use irokle::{Irokle, TopicConfig};
use serde::{Deserialize, Serialize};
use tokio::time::{Duration, timeout};

#[derive(Clone, Debug, irokle::Event, Deserialize, Serialize)]
#[irokle(type_id = "example.intro.message")]
struct IntroEvent {
    text: String,
}

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
    let bob = Irokle::builder().with_net(bob_endpoint).build()?;

    let topic = alice.create_topic::<IntroEvent>(TopicConfig {
        initial_peers: [bob.peer_id()].into(),
        ..TopicConfig::default()
    })?;
    topic.publish(IntroEvent {
        text: "bob was introduced by genesis membership".into(),
    })?;

    timeout(
        Duration::from_secs(10),
        alice.sync_now(bob.peer_id(), topic.id()),
    )
    .await??;
    println!("bob can now list {} topic(s)", bob.list_topics()?.len());
    let bob_topic = bob.open_topic::<IntroEvent>(topic.id())?;
    println!(
        "bob saw {} event(s)",
        bob_topic.history(HistoryOrder::OldestFirst)?.len()
    );

    bob.reject_topic(topic.id())?;
    timeout(
        Duration::from_secs(10),
        bob.sync_now(alice.peer_id(), topic.id()),
    )
    .await??;
    println!("bob rejected membership with a signed remove-peer control op");

    Ok(())
}
