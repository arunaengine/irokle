// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;
use crate::tests::support::{forked_side, node};
use crate::{MemoryStorage, Op, TopicId};

fn send_ops(
    session: &mut SyncSession,
    net: &SharedNet<MemoryStorage>,
    topic_id: TopicId,
    ops: Vec<Op>,
) {
    session
        .handle(
            net,
            SyncMessage::Data(crate::sync::SyncData { topic_id, ops }),
        )
        .unwrap();
    session.hold(&net.budget).unwrap();
}

#[tokio::test]
async fn replies_follow_branch() {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .secret_key(iroh::SecretKey::from_bytes(&[211; 32]))
        .bind()
        .await
        .unwrap();
    let receiver = crate::Irokle::builder()
        .with_iroh_secret_key(endpoint.secret_key())
        .without_peer_whitelist()
        .build()
        .unwrap();
    let net = super::super::IrohNet::new(endpoint, receiver).unwrap();
    let author = node(210);
    let topic_id = TopicId::hash(b"session-branches");
    let (_, _, left, left_event) = forked_side(
        MemoryStorage::new(),
        topic_id,
        210,
        [net.node.peer_id()],
        "left",
    );
    let (_, _, right, right_event) = forked_side(
        MemoryStorage::new(),
        topic_id,
        210,
        [net.node.peer_id(), node(212).peer_id()],
        "right",
    );
    let (old, new) = if left.id > right.id {
        ([left, left_event], [right, right_event])
    } else {
        ([right, right_event], [left, left_event])
    };
    let remote = super::super::endpoint_addr(author.peer_id()).unwrap().id;
    let mut session = SyncSession::new(remote);
    session
        .handle(&net, SyncMessage::Open(author.sync_open(topic_id)))
        .unwrap();
    for op in &old {
        send_ops(&mut session, &net, topic_id, vec![op.clone()]);
    }
    let previous = &session.replies[&topic_id];
    assert_eq!(previous.genesis, Some(old[0].id));
    assert_eq!(previous.accepted, old.iter().map(|op| op.id).collect());
    previous.verify_signature().unwrap();

    let unrelated = TopicId::hash(b"session-unrelated");
    let (_, _, genesis, event) = forked_side(
        MemoryStorage::new(),
        unrelated,
        210,
        [net.node.peer_id()],
        "unrelated",
    );
    session
        .handle(&net, SyncMessage::Open(author.sync_open(unrelated)))
        .unwrap();
    send_ops(
        &mut session,
        &net,
        unrelated,
        vec![genesis.clone(), event.clone()],
    );
    session
        .handle(&net, SyncMessage::Open(author.sync_open(topic_id)))
        .unwrap();
    send_ops(&mut session, &net, topic_id, new.to_vec());
    assert_eq!(
        net.node
            .storage()
            .topic_state(&topic_id)
            .unwrap()
            .unwrap()
            .genesis,
        new[0].id
    );
    let (responses, _) = session.finish(&net, 0).unwrap();
    let acks = responses
        .iter()
        .filter_map(|message| match message {
            SyncMessage::Ack(ack) => Some((ack.topic_id, ack)),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(acks.len(), 2);
    let ack = acks[&topic_id];
    ack.verify_signature().unwrap();
    assert_eq!(ack.genesis, Some(new[0].id));
    assert_eq!(ack.accepted, new.iter().map(|op| op.id).collect());
    assert!(old.iter().all(|op| !ack.accepted.contains(&op.id)));
    let other = acks[&unrelated];
    other.verify_signature().unwrap();
    assert_eq!(other.accepted, [genesis.id, event.id].into());
    net.shutdown().await;
}
