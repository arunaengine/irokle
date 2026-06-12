use super::support::*;
#[cfg(feature = "fjall")]
use crate::storage as crate_storage;

fn assert_clears_satisfied<S: Storage>(storage: S) {
    let ack_signer = Ed25519Signer::from_bytes(&[99; 32]);
    let peer = ack_signer.peer_id();
    let irokle = Irokle::with_storage(storage.clone(), NodeConfig::default()).unwrap();
    let topic = irokle
        .create_topic::<Note>(TopicConfig {
            initial_peers: [peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let satisfied = topic
        .publish(Note {
            text: "satisfied".into(),
        })
        .unwrap();
    let unsatisfied = topic
        .publish(Note {
            text: "unsatisfied".into(),
        })
        .unwrap();
    let other_topic = irokle
        .create_topic::<Note>(TopicConfig {
            initial_peers: [peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let other = other_topic
        .publish(Note {
            text: "other".into(),
        })
        .unwrap();

    irokle
        .put_sync_obligation(peer, topic.id(), [satisfied.meta.op_id].into())
        .unwrap();
    irokle
        .put_sync_obligation(peer, topic.id(), [unsatisfied.meta.op_id].into())
        .unwrap();
    irokle
        .put_sync_obligation(peer, other_topic.id(), [other.meta.op_id].into())
        .unwrap();

    let mut clock = ActorClock::new();
    clock.observe(satisfied.meta.actor_id, satisfied.meta.actor_seq);
    let mut ack = sync::SyncAck {
        topic_id: topic.id(),
        peer_id: peer,
        accepted: BTreeSet::new(),
        heads: [satisfied.meta.op_id].into(),
        clock,
        signature: None,
    };
    ack.sign(&ack_signer).unwrap();
    irokle.apply_sync_ack(&ack).unwrap();

    let report = irokle.sync_report(peer, topic.id()).unwrap();
    assert_eq!(report.obligations.len(), 1);
    assert_eq!(
        report.obligations[0].op_ids,
        [unsatisfied.meta.op_id].into()
    );

    let other_report = irokle.sync_report(peer, other_topic.id()).unwrap();
    assert_eq!(other_report.obligations.len(), 1);
    assert_eq!(
        other_report.obligations[0].op_ids,
        [other.meta.op_id].into()
    );
}

#[test]
fn memory_clears_satisfied() {
    assert_clears_satisfied(MemoryStorage::new());
}

fn assert_stale_ack_ignored<S: Storage>(storage: S) {
    let ack_signer = Ed25519Signer::from_bytes(&[96; 32]);
    let peer = ack_signer.peer_id();
    let alice = Irokle::with_storage(storage.clone(), NodeConfig::default()).unwrap();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let first = topic
        .publish(Note {
            text: "first".into(),
        })
        .unwrap();
    let second = topic
        .publish(Note {
            text: "second".into(),
        })
        .unwrap();

    let mut fresh_clock = ActorClock::new();
    fresh_clock.observe(second.meta.actor_id, second.meta.actor_seq);
    let mut fresh = sync::SyncAck {
        topic_id: topic.id(),
        peer_id: peer,
        accepted: BTreeSet::new(),
        heads: [second.meta.op_id].into(),
        clock: fresh_clock,
        signature: None,
    };
    fresh.sign(&ack_signer).unwrap();
    alice.apply_sync_ack(&fresh).unwrap();

    let mut stale_clock = ActorClock::new();
    stale_clock.observe(first.meta.actor_id, first.meta.actor_seq);
    let mut stale = sync::SyncAck {
        topic_id: topic.id(),
        peer_id: peer,
        accepted: BTreeSet::new(),
        heads: [first.meta.op_id].into(),
        clock: stale_clock,
        signature: None,
    };
    stale.sign(&ack_signer).unwrap();
    alice.apply_sync_ack(&stale).unwrap();

    let stored = storage.peer_ack(&peer, &topic.id()).unwrap().unwrap();
    assert_eq!(stored.heads, [second.meta.op_id].into());
    assert!(stored.clock.get(&second.meta.actor_id) >= second.meta.actor_seq);
}

#[test]
fn memory_ignores_stale_ack() {
    assert_stale_ack_ignored(MemoryStorage::new());
}

#[test]
fn unsigned_ack_keeps_obligation() {
    let alice = node(47);
    let bob = node(48);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let record = topic.publish(Note { text: "ack".into() }).unwrap();
    alice
        .put_sync_obligation(bob.peer_id(), topic.id(), [record.meta.op_id].into())
        .unwrap();

    let err = alice
        .apply_sync_ack(&sync::SyncAck {
            topic_id: topic.id(),
            peer_id: bob.peer_id(),
            accepted: BTreeSet::new(),
            heads: [record.meta.op_id].into(),
            clock: ActorClock::new(),
            signature: None,
        })
        .unwrap_err();

    assert!(matches!(err, Error::MissingSignature));
    assert_eq!(
        alice
            .sync_report(bob.peer_id(), topic.id())
            .unwrap()
            .obligations
            .len(),
        1
    );
}

#[test]
fn clock_clears_obligation() {
    let alice = node(28);
    let ack_signer = Ed25519Signer::from_bytes(&[98; 32]);
    let peer = ack_signer.peer_id();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let record = topic
        .publish(Note {
            text: "clocked".into(),
        })
        .unwrap();
    alice
        .put_sync_obligation(peer, topic.id(), [record.meta.op_id].into())
        .unwrap();

    let mut clock = ActorClock::new();
    clock.observe(record.meta.actor_id, record.meta.actor_seq);
    let mut ack = sync::SyncAck {
        topic_id: topic.id(),
        peer_id: peer,
        accepted: BTreeSet::new(),
        heads: BTreeSet::new(),
        clock,
        signature: None,
    };
    ack.sign(&ack_signer).unwrap();
    alice.apply_sync_ack(&ack).unwrap();

    assert!(
        alice
            .sync_report(peer, topic.id())
            .unwrap()
            .obligations
            .is_empty()
    );
}

#[test]
fn rejects_future_clock() {
    let alice = node(94);
    let ack_signer = Ed25519Signer::from_bytes(&[95; 32]);
    let peer = ack_signer.peer_id();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let record = topic
        .publish(Note {
            text: "future".into(),
        })
        .unwrap();
    alice
        .put_sync_obligation(peer, topic.id(), [record.meta.op_id].into())
        .unwrap();

    let mut clock = ActorClock::new();
    clock.observe(record.meta.actor_id, record.meta.actor_seq + 1);
    let mut ack = sync::SyncAck {
        topic_id: topic.id(),
        peer_id: peer,
        accepted: BTreeSet::new(),
        heads: BTreeSet::new(),
        clock,
        signature: None,
    };
    ack.sign(&ack_signer).unwrap();

    let err = alice.apply_sync_ack(&ack).unwrap_err();

    assert!(matches!(err, Error::InvalidSyncAck(_)));
    assert!(
        alice
            .storage()
            .peer_ack(&peer, &topic.id())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        alice
            .sync_report(peer, topic.id())
            .unwrap()
            .obligations
            .len(),
        1
    );
}

#[test]
fn rejects_unknown_heads() {
    let alice = node(96);
    let ack_signer = Ed25519Signer::from_bytes(&[97; 32]);
    let peer = ack_signer.peer_id();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let mut ack = sync::SyncAck {
        topic_id: topic.id(),
        peer_id: peer,
        accepted: BTreeSet::new(),
        heads: [OpId::hash(b"unknown-head")].into(),
        clock: ActorClock::new(),
        signature: None,
    };
    ack.sign(&ack_signer).unwrap();

    let err = alice.apply_sync_ack(&ack).unwrap_err();

    assert!(matches!(err, Error::InvalidSyncAck(_)));
    assert!(
        alice
            .storage()
            .peer_ack(&peer, &topic.id())
            .unwrap()
            .is_none()
    );
}

fn batch_ack_fixture<S: Storage>(
    storage: S,
) -> (Irokle<S>, Vec<sync::SyncAck>, Vec<TopicId>, PeerId) {
    let ack_signer = Ed25519Signer::from_bytes(&[88; 32]);
    let peer = ack_signer.peer_id();
    let irokle = Irokle::with_storage(
        storage,
        NodeConfig {
            signer: Ed25519Signer::from_bytes(&[87; 32]),
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let oplog = oplog::Oplog::with_storage(irokle.storage().clone());
    let mut acks = Vec::new();
    let mut topics = Vec::new();
    for index in 0..3u8 {
        let topic_id = TopicId::hash([b"batch-ack".as_slice(), &[index]].concat());
        let actor_id = actor_id_for(topic_id, irokle.peer_id());
        let genesis = TopicGenesis {
            event_type_id: Note::TYPE_ID.to_owned(),
            initial_peers: [peer].into(),
            replication_policy: ReplicationPolicy::all(),
        };
        let envelope = EventEnvelope::encode_event(&Note {
            text: format!("note {index}"),
        })
        .unwrap();
        let (_, event_op) = oplog
            .create_topic_genesis_with_event(
                topic_id,
                actor_id,
                genesis,
                envelope,
                irokle.signer(),
            )
            .unwrap();
        irokle
            .put_sync_obligation(peer, topic_id, [event_op.id].into())
            .unwrap();
        let mut clock = ActorClock::new();
        clock.observe(actor_id, event_op.signed.body.actor_seq);
        let mut ack = sync::SyncAck {
            topic_id,
            peer_id: peer,
            accepted: BTreeSet::new(),
            heads: [event_op.id].into(),
            clock,
            signature: None,
        };
        ack.sign(&ack_signer).unwrap();
        acks.push(ack);
        topics.push(topic_id);
    }
    (irokle, acks, topics, peer)
}

fn assert_batch_acks_match_loop<S: Storage>(loop_storage: S, batch_storage: S) {
    let (loop_node, acks, topics, peer) = batch_ack_fixture(loop_storage);
    let (batch_node, batch_acks, _, _) = batch_ack_fixture(batch_storage);
    assert_eq!(acks, batch_acks);

    for ack in &acks {
        loop_node.apply_sync_ack(ack).unwrap();
    }
    let results = batch_node.apply_sync_acks(&batch_acks);
    assert!(results.iter().all(|result| result.is_ok()));

    for topic_id in &topics {
        assert_eq!(
            loop_node.storage().peer_ack(&peer, topic_id).unwrap(),
            batch_node.storage().peer_ack(&peer, topic_id).unwrap()
        );
        assert!(
            batch_node
                .storage()
                .peer_ack(&peer, topic_id)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            loop_node.sync_report(peer, *topic_id).unwrap().obligations,
            batch_node.sync_report(peer, *topic_id).unwrap().obligations
        );
        assert!(
            batch_node
                .sync_report(peer, *topic_id)
                .unwrap()
                .obligations
                .is_empty()
        );
    }
}

#[test]
fn memory_batch_acks_match_loop() {
    assert_batch_acks_match_loop(MemoryStorage::new(), MemoryStorage::new());
}

#[test]
fn batch_acks_isolate_bad_ack() {
    let (irokle, mut acks, topics, peer) = batch_ack_fixture(MemoryStorage::new());
    acks[1].signature = None;

    let results = irokle.apply_sync_acks(&acks);

    assert!(results[0].is_ok());
    assert!(matches!(results[1], Err(Error::MissingSignature)));
    assert!(results[2].is_ok());
    assert!(
        irokle
            .storage()
            .peer_ack(&peer, &topics[0])
            .unwrap()
            .is_some()
    );
    assert!(
        irokle
            .storage()
            .peer_ack(&peer, &topics[1])
            .unwrap()
            .is_none()
    );
    assert!(
        irokle
            .storage()
            .peer_ack(&peer, &topics[2])
            .unwrap()
            .is_some()
    );
    assert!(
        irokle
            .sync_report(peer, topics[0])
            .unwrap()
            .obligations
            .is_empty()
    );
    assert_eq!(
        irokle.sync_report(peer, topics[1]).unwrap().obligations.len(),
        1
    );
    assert!(
        irokle
            .sync_report(peer, topics[2])
            .unwrap()
            .obligations
            .is_empty()
    );
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_batch_acks_match_loop() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    assert_batch_acks_match_loop(
        crate_storage::FjallStorage::open(dir_a.path()).unwrap(),
        crate_storage::FjallStorage::open(dir_b.path()).unwrap(),
    );
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_clears_satisfied() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
    assert_clears_satisfied(storage);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_ignores_stale_ack() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
    assert_stale_ack_ignored(storage);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_clear_persists() {
    let dir = tempfile::tempdir().unwrap();
    let ack_signer = Ed25519Signer::from_bytes(&[97; 32]);
    let peer = ack_signer.peer_id();
    let (topic_id, unsatisfied_id) = {
        let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
        let irokle = Irokle::with_storage(storage, NodeConfig::default()).unwrap();
        let topic = irokle
            .create_topic::<Note>(TopicConfig {
                initial_peers: [peer].into(),
                ..TopicConfig::default()
            })
            .unwrap();
        let satisfied = topic
            .publish(Note {
                text: "durable-satisfied".into(),
            })
            .unwrap();
        let unsatisfied = topic
            .publish(Note {
                text: "durable-unsatisfied".into(),
            })
            .unwrap();

        irokle
            .put_sync_obligation(peer, topic.id(), [satisfied.meta.op_id].into())
            .unwrap();
        irokle
            .put_sync_obligation(peer, topic.id(), [unsatisfied.meta.op_id].into())
            .unwrap();
        let mut clock = ActorClock::new();
        clock.observe(satisfied.meta.actor_id, satisfied.meta.actor_seq);
        let mut ack = sync::SyncAck {
            topic_id: topic.id(),
            peer_id: peer,
            accepted: BTreeSet::new(),
            heads: [satisfied.meta.op_id].into(),
            clock,
            signature: None,
        };
        ack.sign(&ack_signer).unwrap();
        irokle.apply_sync_ack(&ack).unwrap();

        (topic.id(), unsatisfied.meta.op_id)
    };

    let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
    let obligations = storage.sync_obligations(&peer, &topic_id).unwrap();
    assert_eq!(obligations.len(), 1);
    assert_eq!(obligations[0].op_ids, [unsatisfied_id].into());
}
