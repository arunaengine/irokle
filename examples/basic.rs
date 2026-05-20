use irokle::history::HistoryOrder;
use irokle::net;
use irokle::{Ed25519Signer, Irokle, PublishOptions, TopicConfig, WriteConcern};
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
    let ack = bob.receive_sync_data(bob.peer_id(), data_for_bob)?;
    alice.apply_sync_ack(&ack)?;

    let bob_topic = bob.open_topic::<ChatEvent>(alice_topic.id())?;
    let second = bob_topic.publish_with(
        ChatEvent {
            author: "bob".into(),
            text: "write concern is chosen per publish".into(),
        },
        PublishOptions {
            write_concern: WriteConcern::Local,
        },
    )?;

    let bob_summary = bob.sync_summary(alice_topic.id())?;
    let data_for_bob = alice.plan_sync_data(bob.peer_id(), &bob_summary)?;
    let request_for_alice = alice.plan_sync_request(bob.peer_id(), &bob_summary)?;
    let bob_ack = bob.receive_sync_data(bob.peer_id(), data_for_bob)?;
    let data_for_alice = bob.plan_sync_response_data(alice.peer_id(), &request_for_alice)?;
    let alice_ack = alice.receive_sync_data(alice.peer_id(), data_for_alice)?;
    alice.apply_sync_ack(&bob_ack)?;
    bob.apply_sync_ack(&alice_ack)?;

    let encoded_ack = net::encode_sync_message(&irokle::sync::SyncMessage::Ack(alice_ack))?;
    let framed = net::frame_sync_bytes(&encoded_ack);
    let decoded_frames = net::decode_frames(&framed)?;
    let decoded_messages = decoded_frames
        .iter()
        .map(|frame| net::decode_sync_message(frame))
        .collect::<std::io::Result<Vec<_>>>()?;
    let history = alice_topic.history(HistoryOrder::OldestFirst)?;

    println!(
        "published '{}' and '{}'; bob history={}; sync messages={}",
        first.event.text,
        second.event.text,
        history.len(),
        decoded_messages.len()
    );

    Ok(())
}
