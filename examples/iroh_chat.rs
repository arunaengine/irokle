use futures::StreamExt;
use iroh::Watcher;
use irokle::history::HistoryOrder;
use irokle::{Irokle, TopicConfig};
use serde::{Deserialize, Serialize};
use tokio::time::{Duration, timeout};

#[derive(Clone, Debug, irokle::Event, Deserialize, Serialize)]
struct ChatEvent {
    author: String,
    text: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .alpns(vec![irokle::net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await?;
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .alpns(vec![irokle::net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await?;
    let alice = Irokle::builder().with_net(alice_endpoint).build()?;
    let bob = Irokle::builder().with_net(bob_endpoint).build()?;
    let alice_addr = ready_addr(alice.endpoint().expect("iroh endpoint")).await?;
    let bob_addr = ready_addr(bob.endpoint().expect("iroh endpoint")).await?;
    alice.add_peer_addr(bob_addr)?;
    bob.add_peer_addr(alice_addr)?;

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
        alice.sync_now(bob.peer_id(), alice_topic.id()),
    )
    .await??;

    let bob_topic = bob.open_topic::<ChatEvent>(alice_topic.id())?;
    bob_topic.publish(ChatEvent {
        author: "bob".into(),
        text: "received and replying".into(),
    })?;

    timeout(
        Duration::from_secs(10),
        bob.sync_now(alice.peer_id(), alice_topic.id()),
    )
    .await??;

    let history = alice_topic.history(HistoryOrder::OldestFirst)?;
    for record in history {
        println!("{}: {}", record.event.author, record.event.text);
    }

    Ok(())
}

async fn ready_addr(
    endpoint: &iroh::Endpoint,
) -> Result<iroh::EndpointAddr, Box<dyn std::error::Error>> {
    let addr = endpoint.addr();
    if !addr.addrs.is_empty() {
        return Ok(addr);
    }
    let mut stream = endpoint.watch_addr().stream();
    let addr = timeout(Duration::from_secs(5), async move {
        loop {
            let addr = stream
                .next()
                .await
                .ok_or_else(|| std::io::Error::other("endpoint address stream closed"))?;
            if !addr.addrs.is_empty() {
                return Ok::<_, Box<dyn std::error::Error>>(addr);
            }
        }
    })
    .await??;
    Ok(addr)
}
