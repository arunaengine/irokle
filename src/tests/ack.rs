use crate::tests::support::*;

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
        genesis: genesis_of(irokle.storage(), &topic.id()),
        accepted: BTreeSet::new(),
        heads: [satisfied.meta.op_id].into(),
        clock,
        signature: None,
    };
    ack.sign(&ack_signer).unwrap();
    irokle.apply_sync_ack(&ack).unwrap();

    // Both ops coalesce into one clock target; the proof of the earlier one
    // leaves only the later position outstanding.
    let report = irokle.sync_report(peer, topic.id()).unwrap();
    assert_eq!(report.obligations.len(), 1);
    assert!(obligation_covers(
        irokle.storage(),
        &report.obligations,
        &unsatisfied.meta.op_id
    ));

    let other_report = irokle.sync_report(peer, other_topic.id()).unwrap();
    assert_eq!(other_report.obligations.len(), 1);
    assert!(obligation_covers(
        irokle.storage(),
        &other_report.obligations,
        &other.meta.op_id
    ));
}

#[test]
fn memory_clears_satisfied() {
    assert_clears_satisfied(MemoryStorage::new());
}

fn assert_stale_ack<S: Storage>(storage: S) {
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
        genesis: genesis_of(alice.storage(), &topic.id()),
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
        genesis: genesis_of(alice.storage(), &topic.id()),
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
fn memory_keeps_newest() {
    assert_stale_ack(MemoryStorage::new());
}

#[test]
fn unsigned_keeps_obligation() {
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
            genesis: genesis_of(alice.storage(), &topic.id()),
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
        genesis: genesis_of(alice.storage(), &topic.id()),
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
        genesis: genesis_of(alice.storage(), &topic.id()),
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
        genesis: genesis_of(alice.storage(), &topic.id()),
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
            genesis: genesis_of(irokle.storage(), &topic_id),
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

fn assert_batch_matches<S: Storage>(loop_storage: S, batch_storage: S) {
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
fn memory_batch_matches() {
    assert_batch_matches(MemoryStorage::new(), MemoryStorage::new());
}

#[test]
fn bad_ack_isolated() {
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
        .put_sync_obligation(
            crate::storage::SyncObligation::clock(
                holder.peer_id(),
                topic_id,
                source.storage().actor_clock(&topic_id).unwrap(),
            ),
            genesis_of(source.storage(), &topic_id),
        )
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
                genesis: None,
                credit: Default::default(),
                window: plan.window,
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

/// An acknowledgement that names no incarnation certifies nothing. Genesis
/// replacement reuses actor ids and sequence numbers, so evidence that does not
/// say which branch it covers cannot be attributed to one.
#[test]
fn legacy_ack_rejected() {
    let alice = node(120);
    let ack_signer = Ed25519Signer::from_bytes(&[121; 32]);
    let peer = ack_signer.peer_id();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let record = topic
        .publish(Note {
            text: "unidentified".into(),
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
        genesis: None,
        accepted: BTreeSet::new(),
        heads: [record.meta.op_id].into(),
        clock,
        signature: None,
    };
    ack.sign(&ack_signer).unwrap();

    assert!(matches!(
        alice.apply_sync_ack(&ack),
        Err(Error::InvalidSyncAck(_))
    ));
    assert_eq!(
        alice
            .storage()
            .sync_obligations(&peer, &topic.id())
            .unwrap()
            .len(),
        1
    );
    assert!(
        alice
            .storage()
            .peer_ack(&peer, &topic.id())
            .unwrap()
            .is_none()
    );
}

/// A batch reports one verdict per acknowledgement: a record naming a replaced
/// branch is refused on its own without discarding the valid ones beside it.
fn assert_stale_batch<S: Storage>(storage: S) {
    let (irokle, mut acks, topics, peer) = batch_ack_fixture(storage);
    let ack_signer = Ed25519Signer::from_bytes(&[88; 32]);
    acks[1].genesis = Some(OpId::hash(b"some-other-branch"));
    acks[1].sign(&ack_signer).unwrap();

    let results = irokle.apply_sync_acks(&acks);
    assert!(results[0].is_ok());
    assert!(matches!(results[1], Err(Error::StaleIncarnation)));
    assert!(results[2].is_ok());

    // Only the refused topic keeps its work and stores no evidence.
    for (index, topic_id) in topics.iter().enumerate() {
        let obligations = irokle.storage().sync_obligations(&peer, topic_id).unwrap();
        let stored = irokle.storage().peer_ack(&peer, topic_id).unwrap();
        if index == 1 {
            assert_eq!(obligations.len(), 1);
            assert!(stored.is_none());
        } else {
            assert!(obligations.is_empty());
            assert!(stored.is_some());
        }
    }
}

#[test]
fn memory_stale_batch() {
    assert_stale_batch(MemoryStorage::new());
}

/// Evidence proving an earlier frontier of the current branch stays valid while
/// the author keeps publishing. Only the newer work remains outstanding.
#[test]
fn accepts_older_evidence() {
    let alice = node(122);
    let ack_signer = Ed25519Signer::from_bytes(&[123; 32]);
    let peer = ack_signer.peer_id();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let early = topic
        .publish(Note {
            text: "early".into(),
        })
        .unwrap();
    alice
        .put_sync_obligation(peer, topic.id(), [early.meta.op_id].into())
        .unwrap();

    let mut clock = ActorClock::new();
    clock.observe(early.meta.actor_id, early.meta.actor_seq);
    let mut ack = sync::SyncAck {
        topic_id: topic.id(),
        peer_id: peer,
        genesis: genesis_of(alice.storage(), &topic.id()),
        accepted: BTreeSet::new(),
        heads: [early.meta.op_id].into(),
        clock,
        signature: None,
    };
    ack.sign(&ack_signer).unwrap();

    // Publishing continues before the acknowledgement is applied.
    let late = topic
        .publish(Note {
            text: "late".into(),
        })
        .unwrap();
    alice
        .put_sync_obligation(peer, topic.id(), [late.meta.op_id].into())
        .unwrap();

    alice.apply_sync_ack(&ack).unwrap();
    assert!(
        alice
            .storage()
            .peer_reached_op(&peer, &early.meta.op_id)
            .unwrap()
    );
    assert!(
        !alice
            .storage()
            .peer_reached_op(&peer, &late.meta.op_id)
            .unwrap()
    );
    let remaining = alice
        .storage()
        .sync_obligations(&peer, &topic.id())
        .unwrap();
    assert!(
        obligation_covers(alice.storage(), &remaining, &late.meta.op_id),
        "later work must stay outstanding"
    );
    assert_eq!(remaining.len(), 1, "both targets share one record");
}

/// Removing the peer between validation and the write must make the validated
/// evidence ineffective: the storage commit repeats the authorization check.
#[test]
fn removal_defeats_evidence() {
    let alice = node(124);
    let ack_signer = Ed25519Signer::from_bytes(&[125; 32]);
    let peer = ack_signer.peer_id();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let record = topic
        .publish(Note {
            text: "raced".into(),
        })
        .unwrap();
    let mut clock = ActorClock::new();
    clock.observe(record.meta.actor_id, record.meta.actor_seq);
    let validated = crate::storage::PeerAck {
        peer_id: peer,
        topic_id: topic.id(),
        genesis: genesis_of(alice.storage(), &topic.id()),
        heads: [record.meta.op_id].into(),
        clock,
    };
    alice
        .put_sync_obligation(peer, topic.id(), [record.meta.op_id].into())
        .unwrap();

    // Stands in for the removal that commits after validation read the topic.
    topic.remove_peer(peer).unwrap();
    assert!(matches!(
        alice.storage().apply_peer_ack(validated),
        Err(Error::NotTopicMember)
    ));
    assert!(
        !alice
            .storage()
            .peer_reached_op(&peer, &record.meta.op_id)
            .unwrap()
    );
}

#[test]
fn batch_preserves_causes() {
    let pressure = Error::MemoryPressure {
        domain: crate::storage::MemoryDomain::Workspace,
        required: 10,
        limit: 1,
    };
    for error in [pressure, Error::Storage("temporary failure".into())] {
        shared_failure(error);
    }
}

fn shared_failure(error: Error) {
    let expected = std::mem::discriminant(&error);
    let storage = StaleReadStorage::new(MemoryStorage::new());
    let (node, mut acks, topics, peer) = batch_ack_fixture(storage.clone());
    acks[1].genesis = Some(OpId::hash(b"obsolete branch"));
    acks[1].sign(&Ed25519Signer::from_bytes(&[88; 32])).unwrap();
    *storage.ack_fault.lock().unwrap() = Some(AckFault {
        committed: false,
        error,
    });
    let results = node.apply_sync_acks(&acks);
    assert_eq!(results.len(), 3);
    assert!(matches!(results[1], Err(Error::StaleIncarnation)));
    let (Err(Error::Shared(first)), Err(Error::Shared(last))) = (&results[0], &results[2]) else {
        panic!("backend failure must remain shared and typed: {results:?}");
    };
    assert!(Arc::ptr_eq(first, last));
    assert_eq!(std::mem::discriminant(first.cause()), expected);
    assert!(std::error::Error::source(results[0].as_ref().unwrap_err()).is_some());
    assert_eq!(
        storage.ack_calls.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    for topic in topics {
        assert!(storage.peer_ack(&peer, &topic).unwrap().is_none());
        assert!(!storage.sync_obligations(&peer, &topic).unwrap().is_empty());
    }
}

#[cfg(feature = "fjall")]
mod fjall {
    use crate::storage as crate_storage;
    use crate::tests::ack::*;

    #[test]
    fn batch_matches() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        assert_batch_matches(
            crate_storage::FjallStorage::open(dir_a.path()).unwrap(),
            crate_storage::FjallStorage::open(dir_b.path()).unwrap(),
        );
    }

    #[test]
    fn clears_satisfied() {
        let dir = tempfile::tempdir().unwrap();
        let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
        assert_clears_satisfied(storage);
    }

    #[test]
    fn keeps_newest() {
        let dir = tempfile::tempdir().unwrap();
        let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
        assert_stale_ack(storage);
    }

    #[test]
    fn clear_persists() {
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
                genesis: genesis_of(irokle.storage(), &topic.id()),
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
        assert!(obligation_covers(&storage, &obligations, &unsatisfied_id));
    }

    #[test]
    fn stale_batch() {
        let dir = tempfile::tempdir().unwrap();
        assert_stale_batch(crate_storage::FjallStorage::open(dir.path()).unwrap());
    }

    /// A schema 1 database stored acknowledgements without naming the branch they
    /// certified. Upgrading keeps those records and their clocks but treats them as
    /// uncertified, so none of them silently proves the current incarnation.
    #[test]
    fn migrates_legacy() {
        let dir = tempfile::tempdir().unwrap();
        let ack_signer = Ed25519Signer::from_bytes(&[127; 32]);
        let peer = ack_signer.peer_id();

        let (topic_id, op_id, actor_id, actor_seq, metas) = {
            let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
            let irokle = Irokle::with_storage(storage, NodeConfig::default()).unwrap();
            let topic = irokle
                .create_topic::<Note>(TopicConfig {
                    initial_peers: [peer].into(),
                    ..TopicConfig::default()
                })
                .unwrap();
            let record = topic
                .publish(Note {
                    text: "aged".into(),
                })
                .unwrap();
            let mut clock = ActorClock::new();
            clock.observe(record.meta.actor_id, record.meta.actor_seq);
            let mut ack = sync::SyncAck {
                topic_id: topic.id(),
                peer_id: peer,
                genesis: genesis_of(irokle.storage(), &topic.id()),
                accepted: BTreeSet::new(),
                heads: [record.meta.op_id].into(),
                clock,
                signature: None,
            };
            ack.sign(&ack_signer).unwrap();
            irokle.apply_sync_ack(&ack).unwrap();
            let storage = irokle.storage();
            let metas = storage
                .list_op_ids(&topic.id())
                .unwrap()
                .iter()
                .map(|id| storage.get_meta(id).unwrap().unwrap())
                .collect::<Vec<_>>();
            (
                topic.id(),
                record.meta.op_id,
                record.meta.actor_id,
                record.meta.actor_seq,
                metas,
            )
        };

        // Rewrite the stored record in the schema 1 layout, which had no genesis
        // field, and mark the database as schema 1 again.
        {
            let db = ::fjall::OptimisticTxDatabase::builder(dir.path())
                .open()
                .unwrap();
            let records = db
                .keyspace("records", ::fjall::KeyspaceCreateOptions::default)
                .unwrap();
            let mut clock = ActorClock::new();
            clock.observe(actor_id, actor_seq);
            let legacy =
                postcard::to_allocvec(&(peer, topic_id, BTreeSet::from([op_id]), clock)).unwrap();
            let mut tx = db.write_tx().unwrap();
            // A schema 1 file has only the legacy key, never the current one.
            tx.remove(
                &records,
                [b"ak".as_slice(), topic_id.as_ref(), peer.as_ref()].concat(),
            );
            tx.insert(
                &records,
                [b"ak".as_slice(), peer.as_ref(), topic_id.as_ref()].concat(),
                legacy,
            );
            // Metadata held every clock entry and there were no clock nodes.
            for meta in &metas {
                tx.insert(
                    &records,
                    [b"m".as_slice(), meta.id.as_ref()].concat(),
                    postcard::to_allocvec(meta).unwrap(),
                );
            }
            let nodes = [b"cn".as_slice(), topic_id.as_ref()].concat();
            let node_keys = ::fjall::Readable::prefix(&tx, &records, nodes)
                .map(|item| item.key().unwrap().to_vec())
                .collect::<Vec<_>>();
            for key in node_keys {
                tx.remove(&records, key);
            }
            tx.insert(
                &records,
                b"sv".to_vec(),
                postcard::to_allocvec(&1u32).unwrap(),
            );
            tx.commit().unwrap().unwrap();
        }

        let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
        let migrated = storage.peer_ack(&peer, &topic_id).unwrap().unwrap();
        assert_eq!(
            migrated.genesis, None,
            "legacy evidence must stay uncertified"
        );
        assert_eq!(
            migrated.clock.get(&actor_id),
            actor_seq,
            "clock is preserved"
        );
        assert!(migrated.heads.contains(&op_id), "frontier is preserved");
        assert!(
            !storage.peer_reached_op(&peer, &op_id).unwrap(),
            "uncertified evidence must not prove the current branch"
        );

        // Work owed to that peer stays outstanding until it acknowledges again.
        storage
            .put_sync_obligation(
                crate_storage::SyncObligation::repair(peer, topic_id, [op_id].into()),
                genesis_of(&storage, &topic_id),
            )
            .unwrap();
        assert_eq!(storage.sync_obligations(&peer, &topic_id).unwrap().len(), 1);

        // A fresh acknowledgement naming the current branch clears it.
        let mut clock = ActorClock::new();
        clock.observe(actor_id, actor_seq);
        assert_eq!(
            storage
                .apply_peer_ack(crate_storage::PeerAck {
                    peer_id: peer,
                    topic_id,
                    genesis: genesis_of(&storage, &topic_id),
                    heads: [op_id].into(),
                    clock,
                })
                .unwrap(),
            1
        );
        assert!(storage.peer_reached_op(&peer, &op_id).unwrap());
    }

    /// A backend failure after one acknowledgement's writes are staged must abort
    /// the whole batch. Otherwise its ack row commits while clearing its work
    /// failed, and the caller is told the ack was not applied.
    #[test]
    fn batch_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let (acks, topics, peer) = {
            let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
            let (_, acks, topics, peer) = batch_ack_fixture(storage);
            (acks, topics, peer)
        };
        // An obligation record nothing can decode makes the clearing step fail
        // after the ack row of the middle topic was staged.
        {
            let db = ::fjall::OptimisticTxDatabase::builder(dir.path())
                .open()
                .unwrap();
            let records = db
                .keyspace("records", ::fjall::KeyspaceCreateOptions::default)
                .unwrap();
            let mut tx = db.write_tx().unwrap();
            tx.insert(
                &records,
                [
                    b"ob".as_slice(),
                    topics[1].as_ref(),
                    peer.as_ref(),
                    b"r".as_slice(),
                ]
                .concat(),
                vec![0xff; 3],
            );
            tx.commit().unwrap().unwrap();
        }

        let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
        let irokle = Irokle::with_storage(
            storage,
            NodeConfig {
                signer: Ed25519Signer::from_bytes(&[87; 32]),
                ..NodeConfig::default()
            },
        )
        .unwrap();
        let results = irokle.apply_sync_acks(&acks);
        assert!(
            results.iter().all(Result::is_err),
            "a backend failure fails every ack of the batch"
        );
        for topic_id in &topics {
            assert!(
                irokle
                    .storage()
                    .peer_ack(&peer, topic_id)
                    .unwrap()
                    .is_none(),
                "no ack row of the failed batch may commit"
            );
        }
    }

    #[test]
    fn backend_causes_survive() {
        for error in [
            Error::ReopenRequired(::fjall::Error::Poisoned),
            Error::StoragePressure("disk headroom".into()),
            Error::StorageBuffer {
                required: 10,
                limit: 1,
            },
            Error::StorageProbe(std::io::Error::other("probe unavailable")),
            Error::Fjall(::fjall::Error::Poisoned),
        ] {
            shared_failure(error);
        }
    }

    #[test]
    fn uncertain_ack_reopens() {
        for batch in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let storage =
                StaleReadStorage::new(crate_storage::FjallStorage::open(directory.path()).unwrap());
            let (node, acks, topics, peer) = batch_ack_fixture(storage.clone());
            let before = storage.list_op_ids(&topics[0]).unwrap();
            *storage.ack_fault.lock().unwrap() = Some(AckFault {
                committed: true,
                error: Error::ReopenRequired(::fjall::Error::Poisoned),
            });
            let result = if batch {
                node.apply_sync_acks(&acks[..1]).pop().unwrap()
            } else {
                node.apply_sync_ack(&acks[0])
            };
            assert!(matches!(
                result.unwrap_err().cause(),
                Error::ReopenRequired(_)
            ));
            assert_eq!(
                storage.ack_calls.load(std::sync::atomic::Ordering::SeqCst),
                1
            );
            assert_eq!(storage.list_op_ids(&topics[0]).unwrap(), before);
            assert!(storage.peer_ack(&peer, &topics[0]).unwrap().is_some());
            for topic in &topics[1..] {
                assert!(!storage.sync_obligations(&peer, topic).unwrap().is_empty());
                assert!(storage.peer_ack(&peer, topic).unwrap().is_none());
            }
            drop((node, storage));
            let reopened = crate_storage::FjallStorage::open(directory.path()).unwrap();
            assert_eq!(reopened.list_op_ids(&topics[0]).unwrap(), before);
            assert!(reopened.peer_ack(&peer, &topics[0]).unwrap().is_some());
            assert!(
                reopened
                    .sync_obligations(&peer, &topics[0])
                    .unwrap()
                    .is_empty()
            );
            for topic in &topics[1..] {
                assert!(!reopened.sync_obligations(&peer, topic).unwrap().is_empty());
            }
        }
    }
}
