// SPDX-License-Identifier: MIT OR Apache-2.0
//! Minimal in-memory example showing typed events and manual sync planning.

use irokle::history::HistoryOrder;
use irokle::{Ed25519Signer, Irokle, TopicConfig};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, irokle::Event, Deserialize, Serialize)]
#[irokle(type_id = "example.chat.message")]
struct ChatEvent {
    author: String,
    text: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let alice = Irokle::builder()
        .with_signer(Ed25519Signer::from_bytes(&[1; 32]))
        .build()?;
    let bob = Irokle::builder()
        .with_signer(Ed25519Signer::from_bytes(&[2; 32]))
        .build()?;

    let alice_topic = alice.create_topic::<ChatEvent>(TopicConfig {
        initial_peers: [bob.peer_id()].into(),
        ..TopicConfig::default()
    })?;
    let first = alice_topic.publish(ChatEvent {
        author: "alice".into(),
        text: "hello".into(),
    })?;

    let bob_summary = bob.sync_summary(alice_topic.id())?;
    let data_for_bob = alice.plan_sync_data(bob.peer_id(), &bob_summary)?;
    let (ack, _) = bob.receive_sync_data_from(alice.peer_id(), data_for_bob)?;
    alice.apply_sync_ack(&ack)?;

    let bob_topic = bob.open_topic::<ChatEvent>(alice_topic.id())?;
    let second = bob_topic.publish(ChatEvent {
        author: "bob".into(),
        text: "reply from another member".into(),
    })?;

    let bob_summary = bob.sync_summary(alice_topic.id())?;
    let data_for_bob = alice.plan_sync_data(bob.peer_id(), &bob_summary)?;
    let request_for_alice = alice.plan_sync_request(bob.peer_id(), &bob_summary)?;
    let (bob_ack, _) = bob.receive_sync_data_from(alice.peer_id(), data_for_bob)?;
    let data_for_alice = bob.plan_sync_response_data(alice.peer_id(), &request_for_alice)?;
    let (alice_ack, _) = alice.receive_sync_data_from(bob.peer_id(), data_for_alice)?;
    alice.apply_sync_ack(&bob_ack)?;
    bob.apply_sync_ack(&alice_ack)?;

    let history = alice_topic.history(HistoryOrder::OldestFirst)?;

    println!(
        "published '{}' and '{}'; alice history={}",
        first.event.text,
        second.event.text,
        history.len()
    );

    Ok(())
}
