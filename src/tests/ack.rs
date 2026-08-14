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
fn accepts_unknown_heads() {
    // A head we have not learned is the peer's own history, not a bad ack.
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

    alice.apply_sync_ack(&ack).unwrap();

    assert!(
        alice
            .storage()
            .peer_ack(&peer, &topic.id())
            .unwrap()
            .is_some()
    );
}

fn mesh_ack() -> (Irokle, Irokle, TopicId, sync::SyncAck) {
    // Bob buffers Charlie's op until Alice's data completes it, so Bob's ack
    // carries a third actor's clock entry, head, and accepted op that Alice
    // has never seen.
    let alice = node(70);
    let bob = node(71);
    let charlie = node(72);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id(), charlie.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    topic
        .publish(Note {
            text: "first".into(),
        })
        .unwrap();
    let history = oplog::topological(alice.storage(), &topic.id()).unwrap();
    charlie
        .receive_sync_data_from(
            alice.peer_id(),
            sync::SyncData {
                topic_id: topic.id(),
                ops: history.clone(),
            },
        )
        .unwrap();
    bob.receive_sync_data_from(
        alice.peer_id(),
        sync::SyncData {
            topic_id: topic.id(),
            ops: vec![history[0].clone()],
        },
    )
    .unwrap();

    let remote = charlie
        .open_topic::<Note>(topic.id())
        .unwrap()
        .publish(Note {
            text: "charlie".into(),
        })
        .unwrap();
    let remote_op = charlie
        .storage()
        .get_op(&remote.meta.op_id)
        .unwrap()
        .unwrap();
    bob.receive_sync_data_from(
        charlie.peer_id(),
        sync::SyncData {
            topic_id: topic.id(),
            ops: vec![remote_op],
        },
    )
    .unwrap();
    assert!(bob.storage().get_op(&remote.meta.op_id).unwrap().is_none());

    let second = topic
        .publish(Note {
            text: "second".into(),
        })
        .unwrap();
    let second_op = alice.storage().get_op(&second.meta.op_id).unwrap().unwrap();
    let ack = bob
        .receive_sync_data_from(
            alice.peer_id(),
            sync::SyncData {
                topic_id: topic.id(),
                ops: vec![history[1].clone(), second_op],
            },
        )
        .unwrap()
        .0;
    (alice, bob, topic.id(), ack)
}

#[test]
fn accepts_mesh_ack() {
    let (alice, bob, topic_id, ack) = mesh_ack();
    let local_clock = alice.storage().actor_clock(&topic_id).unwrap();
    assert!(
        ack.clock
            .iter()
            .any(|(actor_id, seq)| *seq > local_clock.get(actor_id))
    );
    assert!(
        ack.heads
            .iter()
            .any(|op_id| alice.storage().get_meta(op_id).unwrap().is_none())
    );
    assert!(
        ack.accepted
            .iter()
            .any(|op_id| alice.storage().get_meta(op_id).unwrap().is_none())
    );

    alice.apply_sync_ack(&ack).unwrap();

    assert!(
        alice
            .storage()
            .peer_ack(&bob.peer_id(), &topic_id)
            .unwrap()
            .is_some()
    );
}

#[test]
fn rejects_local_overclaim() {
    // Other actors may outrun us, our own actor never can.
    let (alice, bob, topic_id, mut ack) = mesh_ack();
    let local_actor = actor_id_for(topic_id, alice.peer_id());
    let local_seq = alice
        .storage()
        .actor_clock(&topic_id)
        .unwrap()
        .get(&local_actor);
    ack.clock.observe(local_actor, local_seq + 1);
    ack.signature = None;
    ack.sign(bob.signer()).unwrap();

    let err = alice.apply_sync_ack(&ack).unwrap_err();

    assert!(matches!(err, Error::InvalidSyncAck(_)));
    assert!(
        alice
            .storage()
            .peer_ack(&bob.peer_id(), &topic_id)
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
            .create_topic_genesis_with_event(topic_id, actor_id, genesis, envelope, irokle.signer())
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
        irokle
            .sync_report(peer, topics[1])
            .unwrap()
            .obligations
            .len(),
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

#[test]
fn ack_needs_closure() {
    // A receiver holding a hole must not clear the source's retry obligation:
    // its ack would certify a frontier it cannot replay.
    let storage = MemoryStorage::new();
    let holder_signer = Ed25519Signer::from_bytes(&[122; 32]);
    let (source, topic_id, ops) = chain_source(121, holder_signer.peer_id());
    oplog::Oplog::with_storage(storage.clone())
        .receive_ops(ops.clone())
        .unwrap();
    damage_op(&storage, &ops[1].id, Damage::Meta);
    let holder = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: holder_signer,
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    source
        .storage()
        .put_sync_obligation(crate::storage::SyncObligation {
            peer_id: holder.peer_id(),
            topic_id,
            op_ids: [ops[2].id].into(),
            target_clock: source.storage().actor_clock(&topic_id).unwrap(),
        })
        .unwrap();

    let (damaged_ack, _) = holder
        .receive_sync_data_from(
            source.peer_id(),
            sync::SyncData {
                topic_id,
                ops: vec![ops[0].clone()],
            },
        )
        .unwrap();
    source.apply_sync_ack(&damaged_ack).unwrap();

    assert!(damaged_ack.heads.is_empty());
    assert!(damaged_ack.clock.is_empty());
    assert!(
        source
            .storage()
            .has_sync_obligations(&holder.peer_id(), &topic_id)
            .unwrap()
    );

    let plan = holder
        .negotiate_sync(source.peer_id(), &source.sync_summary(topic_id).unwrap())
        .unwrap();
    let repair = source
        .plan_sync_response_data(
            holder.peer_id(),
            &sync::SyncRequest {
                topic_id,
                known: plan.common,
                wants: plan.need,
                actor_range_hints: plan.actor_range_hints,
            },
        )
        .unwrap();
    let (whole_ack, _) = holder
        .receive_sync_data_from(source.peer_id(), repair)
        .unwrap();
    source.apply_sync_ack(&whole_ack).unwrap();

    assert_eq!(whole_ack.heads, [ops[2].id].into());
    assert!(
        !source
            .storage()
            .has_sync_obligations(&holder.peer_id(), &topic_id)
            .unwrap()
    );
}
