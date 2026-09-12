use super::support::*;
use crate::storage as crate_storage;

fn assert_single_actor_chain<S: Storage>(storage: S) {
    let config = NodeConfig {
        signer: Ed25519Signer::from_bytes(&[45; 32]),
        default_write_concern: WriteConcern::Local,
        ..NodeConfig::default()
    };
    let initial = Irokle::with_storage(storage.clone(), config.clone()).unwrap();
    let topic = initial
        .create_topic::<Note>(TopicConfig::default())
        .unwrap();
    let topic_id = topic.id();
    let publishes = 32_u64;
    let barrier = Arc::new(Barrier::new(publishes as usize));

    let handles = (0..publishes)
        .map(|i| {
            let storage = storage.clone();
            let config = config.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                let node = Irokle::with_storage(storage, config).unwrap();
                let topic = node.open_topic::<Note>(topic_id).unwrap();
                barrier.wait();
                topic
                    .publish(Note {
                        text: format!("independent {i}"),
                    })
                    .unwrap()
                    .meta
            })
        })
        .collect::<Vec<_>>();

    let mut seqs = BTreeSet::new();
    let mut op_ids = BTreeSet::new();
    for handle in handles {
        let meta = handle.join().unwrap();
        assert!(seqs.insert(meta.actor_seq));
        assert!(op_ids.insert(meta.op_id));
    }

    assert_eq!(seqs, (2..=publishes + 1).collect());
    let ops = initial.raw_topic(topic_id).unwrap().history().unwrap();
    assert_eq!(ops.len(), publishes as usize + 1);
    assert_eq!(initial.storage().heads(&topic_id).unwrap().len(), 1);
}

#[test]
fn memory_facades_share_actor() {
    assert_single_actor_chain(MemoryStorage::new());
}

fn assert_unique_topic_ids<S: Storage>(storage: S) {
    let config = NodeConfig {
        signer: Ed25519Signer::from_bytes(&[46; 32]),
        default_write_concern: WriteConcern::Local,
        ..NodeConfig::default()
    };
    let creates = 32_usize;
    let barrier = Arc::new(Barrier::new(creates));
    let handles = (0..creates)
        .map(|_| {
            let storage = storage.clone();
            let config = config.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                let node = Irokle::with_storage(storage, config).unwrap();
                barrier.wait();
                node.create_topic::<Note>(TopicConfig::default())
                    .unwrap()
                    .id()
            })
        })
        .collect::<Vec<_>>();

    let mut topic_ids = BTreeSet::new();
    for handle in handles {
        assert!(topic_ids.insert(handle.join().unwrap()));
    }
    assert_eq!(topic_ids.len(), creates);
}

#[test]
fn memory_unique_topic_ids() {
    assert_unique_topic_ids(MemoryStorage::new());
}

#[test]
fn memory_publish_history() {
    let irokle = node(4);
    let topic = irokle.create_topic::<Note>(TopicConfig::default()).unwrap();
    topic
        .publish(Note {
            text: "hello".into(),
        })
        .unwrap();
    let history = topic.history(history::HistoryOrder::OldestFirst).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].event.text, "hello");
    assert_eq!(irokle.list_topics().unwrap().len(), 1);
}

fn assert_pending_reconciles<S: Storage>(storage: S) {
    let alice = node(44);
    let bob_signer = Ed25519Signer::from_bytes(&[45; 32]);
    let bob_config = NodeConfig {
        signer: bob_signer.clone(),
        default_write_concern: WriteConcern::Local,
        ..NodeConfig::default()
    };
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob_signer.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    topic.publish(Note { text: "one".into() }).unwrap();
    topic.publish(Note { text: "two".into() }).unwrap();
    let ops = oplog::topological(alice.storage(), &topic.id()).unwrap();
    let genesis = ops[0].clone();
    let first = ops[1].clone();
    let second = ops[2].clone();

    let bob = Irokle::with_storage(storage.clone(), bob_config.clone()).unwrap();
    bob.receive_sync_data_from(
        alice.peer_id(),
        sync::SyncData {
            topic_id: topic.id(),
            ops: vec![genesis.clone()],
        },
    )
    .unwrap();
    let pending_ack = bob
        .receive_sync_data_from(
            alice.peer_id(),
            sync::SyncData {
                topic_id: topic.id(),
                ops: vec![second.clone()],
            },
        )
        .unwrap()
        .0;
    assert!(pending_ack.accepted.is_empty());
    assert!(storage.get_op(&second.id).unwrap().is_none());

    let first_meta = alice.storage().get_meta(&first.id).unwrap().unwrap();
    storage
        .put_admitted_batch(crate_storage::AdmittedBatch {
            topic_id: topic.id(),
            expected_heads: storage.heads(&topic.id()).unwrap(),
            expected_topic_state: storage.topic_state(&topic.id()).unwrap(),
            entries: vec![(first.clone(), first_meta)],
            heads: [first.id].into(),
            topic_state: None,
            effects: crate_storage::AdmissionEffects::default(),
        })
        .unwrap();
    assert!(storage.get_op(&first.id).unwrap().is_some());
    assert!(storage.get_op(&second.id).unwrap().is_none());

    Irokle::with_storage(storage.clone(), bob_config).unwrap();
    assert!(storage.get_op(&second.id).unwrap().is_some());
    assert_eq!(storage.heads(&topic.id()).unwrap(), [second.id].into());
}

#[test]
fn memory_reconciles_pending() {
    assert_pending_reconciles(MemoryStorage::new());
}

fn assert_reset_topic_clears_everything<S: Storage>(storage: S) {
    let signer = Ed25519Signer::from_bytes(&[71; 32]);
    let peer = signer.peer_id();
    let other_peer = PeerId::hash(b"reset-other-peer");
    let config = NodeConfig {
        signer,
        default_write_concern: WriteConcern::Local,
        ..NodeConfig::default()
    };
    let node = Irokle::with_storage(storage.clone(), config).unwrap();

    let topic = node
        .create_topic::<Note>(TopicConfig {
            initial_peers: [other_peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let topic_id = topic.id();
    topic.publish(Note { text: "one".into() }).unwrap();
    topic.publish(Note { text: "two".into() }).unwrap();
    let genesis_id = oplog::topological(&storage, &topic_id).unwrap()[0].id;
    let actor_id = actor_id_for(topic_id, peer);

    // A second topic that reset must leave untouched.
    let survivor = node.create_topic::<Note>(TopicConfig::default()).unwrap();
    let survivor_id = survivor.id();
    survivor
        .publish(Note {
            text: "keep".into(),
        })
        .unwrap();
    let survivor_ops = storage.list_op_ids(&survivor_id).unwrap();

    // Buffered pending op targeting the topic whose dependency never arrives.
    let author_signer = Ed25519Signer::from_bytes(&[72; 32]);
    let author = author_signer.peer_id();
    let missing_dep = OpId::hash(b"reset-missing-dep");
    let author_actor = actor_id_for(topic_id, author);
    let pending_op = Op::sign(
        OpBody {
            topic_id,
            author,
            actor_id: author_actor,
            actor_seq: 5,
            actor_prev: Some(missing_dep),
            deps: [missing_dep].into(),
            generation: 9,
            payload: TopicPayload::Event(
                EventEnvelope::encode_event(&Note {
                    text: "pending".into(),
                })
                .unwrap(),
            ),
        },
        &author_signer,
    )
    .unwrap();
    let pending_meta = crate_storage::OpMeta {
        id: pending_op.id,
        topic_id,
        author,
        actor_id: author_actor,
        actor_seq: 5,
        actor_prev: Some(missing_dep),
        deps: [missing_dep].into(),
        generation: 9,
        observed_clock: ActorClock::new(),
        ready: false,
        missing_deps: [missing_dep].into(),
    };
    storage
        .put_pending_op(author, pending_op, pending_meta)
        .unwrap();

    // Stored peer ack, sync obligation, and sync status for the topic.
    let mut ack_clock = ActorClock::new();
    ack_clock.observe(actor_id, 3);
    storage
        .apply_peer_ack(crate_storage::PeerAck {
            peer_id: other_peer,
            topic_id,
            genesis: Some(genesis_id),
            heads: storage.heads(&topic_id).unwrap(),
            clock: ack_clock,
        })
        .unwrap();
    storage
        .put_sync_obligation(crate_storage::SyncObligation::repair(
            other_peer,
            topic_id,
            [OpId::hash(b"reset-obligation-op")].into(),
        ))
        .unwrap();
    storage
        .put_sync_status(crate_storage::SyncPeerStatus {
            peer_id: other_peer,
            topic_id,
            ..crate_storage::SyncPeerStatus::default()
        })
        .unwrap();

    let topic_op_ids = storage.list_op_ids(&topic_id).unwrap();
    assert_eq!(topic_op_ids.len(), 3);
    assert!(!storage.pending_waiters(&missing_dep).unwrap().is_empty());

    let removed = storage.reset_topic(&topic_id).unwrap();
    assert_eq!(removed, 3);

    // Every per-topic keyspace is empty.
    assert!(storage.topic_state(&topic_id).unwrap().is_none());
    assert!(storage.list_op_ids(&topic_id).unwrap().is_empty());
    assert!(storage.list_ops(&topic_id).unwrap().is_empty());
    assert!(storage.heads(&topic_id).unwrap().is_empty());
    assert!(storage.actor_clock(&topic_id).unwrap().is_empty());
    assert_eq!(storage.max_generation(&topic_id).unwrap(), 0);
    assert!(storage.actor_tip(&topic_id, &actor_id).unwrap().is_none());
    assert!(
        storage
            .actor_index(&topic_id, &actor_id, 1)
            .unwrap()
            .is_none()
    );
    assert!(storage.children(&genesis_id).unwrap().is_empty());
    assert_eq!(
        storage.topic_fingerprint(&topic_id).unwrap(),
        storage
            .topic_fingerprint(&TopicId::hash(b"reset-empty"))
            .unwrap()
    );
    for op_id in &topic_op_ids {
        assert!(storage.get_op(op_id).unwrap().is_none());
        assert!(storage.get_meta(op_id).unwrap().is_none());
    }
    assert!(storage.peer_acks(&topic_id).unwrap().is_empty());
    assert!(storage.peer_ack(&other_peer, &topic_id).unwrap().is_none());
    assert!(
        storage
            .sync_obligations(&other_peer, &topic_id)
            .unwrap()
            .is_empty()
    );
    assert!(
        storage
            .all_sync_obligations()
            .unwrap()
            .iter()
            .all(|o| o.topic_id != topic_id)
    );
    assert!(storage.sync_statuses(&topic_id).unwrap().is_empty());
    assert!(storage.pending_waiters(&missing_dep).unwrap().is_empty());

    // The survivor topic is intact.
    assert!(storage.topic_state(&survivor_id).unwrap().is_some());
    assert_eq!(storage.list_op_ids(&survivor_id).unwrap(), survivor_ops);
    assert!(!storage.heads(&survivor_id).unwrap().is_empty());
    assert_eq!(storage.list_topics().unwrap().len(), 1);
}

#[test]
fn memory_reset_topic_clears_everything() {
    assert_reset_topic_clears_everything(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_reset_topic_clears_everything() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
    assert_reset_topic_clears_everything(storage);
}

fn seed_chain<S: Storage>(storage: &S, topic_id: TopicId, seed: u8, events: &[&str]) -> ActorId {
    let signer = Ed25519Signer::from_bytes(&[seed; 32]);
    let actor = actor_id_for(topic_id, signer.peer_id());
    let log = oplog::Oplog::with_storage(storage.clone());
    log.create_topic_genesis(
        topic_id,
        actor,
        TopicGenesis {
            event_type_id: Note::TYPE_ID.into(),
            initial_peers: BTreeSet::new(),
            replication_policy: ReplicationPolicy::default(),
        },
        &signer,
    )
    .unwrap();
    for text in events {
        log.create_event_op(
            topic_id,
            actor,
            EventEnvelope::encode_event(&Note {
                text: (*text).into(),
            })
            .unwrap(),
            &signer,
        )
        .unwrap();
    }
    actor
}

fn assert_reset_topic_and_admit_is_atomic<S: Storage>(storage: S) {
    let topic_id = TopicId::hash(b"reset-admit-topic");
    let local_actor = seed_chain(&storage, topic_id, 61, &["one", "two"]);
    let old_ids = storage.list_op_ids(&topic_id).unwrap();
    assert_eq!(old_ids.len(), 3);

    // A survivor topic that the combined op must leave untouched.
    let survivor_id = TopicId::hash(b"reset-admit-survivor");
    seed_chain(&storage, survivor_id, 62, &["keep"]);
    let survivor_ids = storage.list_op_ids(&survivor_id).unwrap();

    // A winning foreign genesis for the same topic, built against a fresh
    // topic (empty expected heads/state), just as the adoption path does.
    let winner_signer = Ed25519Signer::from_bytes(&[63; 32]);
    let winner_src = oplog::Oplog::with_storage(MemoryStorage::new());
    let winner_actor = actor_id_for(topic_id, winner_signer.peer_id());
    let winner_genesis = winner_src
        .create_topic_genesis(
            topic_id,
            winner_actor,
            TopicGenesis {
                event_type_id: Note::TYPE_ID.into(),
                initial_peers: BTreeSet::new(),
                replication_policy: ReplicationPolicy::default(),
            },
            &winner_signer,
        )
        .unwrap();
    let winner_meta = winner_src
        .storage()
        .get_meta(&winner_genesis.id)
        .unwrap()
        .unwrap();
    let winner_state = winner_src
        .storage()
        .topic_state(&topic_id)
        .unwrap()
        .unwrap();

    let expected_state = storage.topic_state(&topic_id).unwrap().unwrap();
    storage.seal_topic(&topic_id).unwrap();
    let winner_batch = crate_storage::AdmittedBatch {
        topic_id,
        expected_heads: BTreeSet::new(),
        expected_topic_state: None,
        entries: vec![(winner_genesis.clone(), winner_meta)],
        heads: [winner_genesis.id].into(),
        topic_state: Some(winner_state),
        effects: crate_storage::AdmissionEffects::default(),
    };
    let sealed = storage
        .reset_topic_and_admit(&topic_id, &expected_state, winner_batch.clone(), None)
        .unwrap_err();
    assert!(matches!(sealed, Error::TopicSealed));
    assert_eq!(storage.list_op_ids(&topic_id).unwrap(), old_ids);
    storage.unseal_topic(&topic_id).unwrap();
    let removed = storage
        .reset_topic_and_admit(&topic_id, &expected_state, winner_batch, None)
        .unwrap();

    // Both effects landed together: the old chain is gone and the winner is in.
    assert_eq!(removed, 3);
    for id in &old_ids {
        assert!(storage.get_op(id).unwrap().is_none());
        assert!(storage.get_meta(id).unwrap().is_none());
    }
    assert_eq!(
        storage.list_op_ids(&topic_id).unwrap(),
        [winner_genesis.id].into()
    );
    assert!(storage.get_op(&winner_genesis.id).unwrap().is_some());
    assert_eq!(
        storage.topic_state(&topic_id).unwrap().unwrap().genesis,
        winner_genesis.id
    );
    assert_eq!(
        storage.heads(&topic_id).unwrap(),
        [winner_genesis.id].into()
    );
    assert_eq!(
        storage.actor_tip(&topic_id, &winner_actor).unwrap(),
        Some((1, winner_genesis.id))
    );
    assert!(
        storage
            .actor_tip(&topic_id, &local_actor)
            .unwrap()
            .is_none()
    );

    // Survivor topic non-interference.
    assert_eq!(storage.list_op_ids(&survivor_id).unwrap(), survivor_ids);
    assert!(storage.topic_state(&survivor_id).unwrap().is_some());
}

fn assert_reset_topic_and_admit_rejects_stale_state<S: Storage>(storage: S) {
    let topic_id = TopicId::hash(b"stale-reset-admit-topic");
    let actor = seed_chain(&storage, topic_id, 64, &["one"]);
    let stale_state = storage.topic_state(&topic_id).unwrap().unwrap();

    let signer = Ed25519Signer::from_bytes(&[64; 32]);
    oplog::Oplog::with_storage(storage.clone())
        .create_event_op(
            topic_id,
            actor,
            EventEnvelope::encode_event(&Note { text: "two".into() }).unwrap(),
            &signer,
        )
        .unwrap();

    let winner_signer = Ed25519Signer::from_bytes(&[65; 32]);
    let winner_src = oplog::Oplog::with_storage(MemoryStorage::new());
    let winner_actor = actor_id_for(topic_id, winner_signer.peer_id());
    let winner_genesis = winner_src
        .create_topic_genesis(
            topic_id,
            winner_actor,
            TopicGenesis {
                event_type_id: Note::TYPE_ID.into(),
                initial_peers: BTreeSet::new(),
                replication_policy: ReplicationPolicy::default(),
            },
            &winner_signer,
        )
        .unwrap();
    let winner_meta = winner_src
        .storage()
        .get_meta(&winner_genesis.id)
        .unwrap()
        .unwrap();
    let winner_state = winner_src
        .storage()
        .topic_state(&topic_id)
        .unwrap()
        .unwrap();

    let err = storage
        .reset_topic_and_admit(
            &topic_id,
            &stale_state,
            crate_storage::AdmittedBatch {
                topic_id,
                expected_heads: BTreeSet::new(),
                expected_topic_state: None,
                entries: vec![(winner_genesis.clone(), winner_meta)],
                heads: [winner_genesis.id].into(),
                topic_state: Some(winner_state),
                effects: crate_storage::AdmissionEffects::default(),
            },
            None,
        )
        .unwrap_err();
    assert!(matches!(err, Error::AdmissionConflict));
    assert!(storage.get_op(&winner_genesis.id).unwrap().is_none());
    assert_eq!(storage.list_op_ids(&topic_id).unwrap().len(), 3);
}

#[test]
fn memory_reset_topic_and_admit_is_atomic() {
    assert_reset_topic_and_admit_is_atomic(MemoryStorage::new());
}

#[test]
fn memory_reset_topic_and_admit_rejects_stale_state() {
    assert_reset_topic_and_admit_rejects_stale_state(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_reset_topic_and_admit_is_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
    assert_reset_topic_and_admit_is_atomic(storage);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_reset_topic_and_admit_rejects_stale_state() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
    assert_reset_topic_and_admit_rejects_stale_state(storage);
}

/// What a lost eviction must still be recoverable from: the topic, the chain it
/// replaced, and the one op whose payload the reset discarded.
struct LostEviction {
    topic_id: TopicId,
    losing_genesis: OpId,
    winning_genesis: OpId,
    evicted: Op,
}

/// Resolve a real genesis tie-break against `storage` and drop the returned
/// eviction without delivering it anywhere: the crash window between the reset
/// commit and the consumer's receipt.
fn evict_undelivered<S: Storage>(storage: &S) -> LostEviction {
    let topic_id = TopicId::hash(b"eviction-journal-topic");
    let local_peer = Ed25519Signer::from_bytes(&[91; 32]).peer_id();
    let foreign_peer = Ed25519Signer::from_bytes(&[92; 32]).peer_id();
    let (local, _, local_genesis, local_event) = forked_side(
        storage.clone(),
        topic_id,
        91,
        [foreign_peer],
        "acknowledged",
    );
    let (_, _, foreign_genesis, foreign_event) =
        forked_side(MemoryStorage::new(), topic_id, 92, [local_peer], "winner");
    assert!(
        foreign_genesis.id < local_genesis.id,
        "these seeds must make the local side lose the tie-break"
    );

    let admitted = local
        .receive_ops_from_peer_evicting(
            Some(foreign_peer),
            vec![foreign_genesis.clone(), foreign_event],
        )
        .unwrap();
    assert_eq!(admitted.evictions.len(), 1);
    // The crash: the only in-memory copy of the payload goes nowhere.
    drop(admitted);
    drop(local);

    LostEviction {
        topic_id,
        losing_genesis: local_genesis.id,
        winning_genesis: foreign_genesis.id,
        evicted: local_event,
    }
}

/// The journal a restart finds must describe exactly the payloads the reset
/// removed, and release them only when the consumer acknowledges.
fn assert_journal_recovers<S: Storage>(storage: &S, lost: LostEviction) {
    let pending = storage.pending_evictions().unwrap();
    assert_eq!(pending.len(), 1);
    let record = &pending[0];
    assert_eq!(record.topic_id, lost.topic_id);
    assert_eq!(record.losing_genesis, lost.losing_genesis);
    assert_eq!(record.winning_genesis, lost.winning_genesis);
    assert_eq!(record.evicted.len(), 1);
    assert_eq!(record.evicted[0].op_id, lost.evicted.id);
    assert_eq!(record.evicted[0].payload, lost.evicted.signed.body.payload);

    // The reset really committed: the journal is the only copy left.
    assert!(storage.get_op(&lost.evicted.id).unwrap().is_none());
    assert_eq!(
        storage
            .topic_state(&lost.topic_id)
            .unwrap()
            .unwrap()
            .genesis,
        lost.winning_genesis
    );

    storage.clear_eviction(&record.key()).unwrap();
    assert!(storage.pending_evictions().unwrap().is_empty());
    // Acknowledging a record that is already released is not an error.
    storage.clear_eviction(&record.key()).unwrap();
}

#[test]
fn memory_journals_eviction() {
    let storage = MemoryStorage::new();
    let lost = evict_undelivered(&storage);
    // A facade built after the fact: the record lives in the store, not in the
    // node that produced it.
    assert_journal_recovers(&storage.clone(), lost);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_journals_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
    let lost = evict_undelivered(&storage);
    drop(storage);
    // Reopened from disk: only what the reset transaction committed is left.
    let reopened = crate_storage::FjallStorage::open(dir.path()).unwrap();
    assert_journal_recovers(&reopened, lost);
}

/// One reset's worth of journal input, with a key that differs per `nonce`.
fn filler_eviction(topic_id: TopicId, winning_genesis: OpId, nonce: usize) -> crate::TopicEviction {
    crate::TopicEviction {
        topic_id,
        losing_genesis: OpId::hash(nonce.to_le_bytes()),
        winning_genesis,
        evicted: vec![crate::EvictedOp {
            op_id: OpId::hash(nonce.to_le_bytes()),
            actor_id: ActorId::default(),
            author: PeerId::default(),
            actor_seq: 1,
            payload: TopicPayload::Event(
                EventEnvelope::encode_event(&Note {
                    text: String::new(),
                })
                .unwrap(),
            ),
        }],
    }
}

/// A winner genesis for `topic_id` that no earlier `nonce` produced, with the
/// metadata and state a reset needs to install it.
fn filler_winner(
    topic_id: TopicId,
    nonce: usize,
) -> (Op, crate_storage::OpMeta, crate_storage::TopicState) {
    let signer = Ed25519Signer::from_bytes(&[96; 32]);
    let source = oplog::Oplog::with_storage(MemoryStorage::new());
    let genesis = source
        .create_topic_genesis(
            topic_id,
            actor_id_for(topic_id, signer.peer_id()),
            TopicGenesis {
                event_type_id: Note::TYPE_ID.into(),
                initial_peers: [PeerId::hash(nonce.to_le_bytes())].into(),
                replication_policy: ReplicationPolicy::default(),
            },
            &signer,
        )
        .unwrap();
    let meta = source.storage().get_meta(&genesis.id).unwrap().unwrap();
    let state = source.storage().topic_state(&topic_id).unwrap().unwrap();
    (genesis, meta, state)
}

/// The journal is bounded: once it holds `MAX_PENDING_EVICTIONS`
/// unacknowledged records the next reset is refused, rather than growing
/// without limit or dropping a payload nothing else holds.
fn assert_journal_bound<S: Storage>(storage: S) {
    let topic_id = TopicId::hash(b"eviction-bound-topic");
    seed_chain(&storage, topic_id, 95, &["one"]);

    for nonce in 0..crate_storage::MAX_PENDING_EVICTIONS {
        let expected_state = storage.topic_state(&topic_id).unwrap().unwrap();
        let (genesis, meta, state) = filler_winner(topic_id, nonce);
        storage
            .reset_topic_and_admit(
                &topic_id,
                &expected_state,
                crate_storage::AdmittedBatch {
                    topic_id,
                    expected_heads: BTreeSet::new(),
                    expected_topic_state: None,
                    entries: vec![(genesis.clone(), meta)],
                    heads: [genesis.id].into(),
                    topic_state: Some(state),
                    effects: crate_storage::AdmissionEffects::default(),
                },
                Some(&filler_eviction(topic_id, genesis.id, nonce)),
            )
            .unwrap();
    }
    assert_eq!(
        storage.pending_evictions().unwrap().len(),
        crate_storage::MAX_PENDING_EVICTIONS
    );

    let before = topic_snapshot(&storage, &topic_id);
    let expected_state = storage.topic_state(&topic_id).unwrap().unwrap();
    let (genesis, meta, state) = filler_winner(topic_id, crate_storage::MAX_PENDING_EVICTIONS);
    let refused = storage.reset_topic_and_admit(
        &topic_id,
        &expected_state,
        crate_storage::AdmittedBatch {
            topic_id,
            expected_heads: BTreeSet::new(),
            expected_topic_state: None,
            entries: vec![(genesis.clone(), meta)],
            heads: [genesis.id].into(),
            topic_state: Some(state),
            effects: crate_storage::AdmissionEffects::default(),
        },
        Some(&filler_eviction(
            topic_id,
            genesis.id,
            crate_storage::MAX_PENDING_EVICTIONS,
        )),
    );

    // Refused whole: the chain the reset would have discarded is still here.
    assert!(matches!(refused, Err(Error::EvictionJournalFull)));
    assert_eq!(topic_snapshot(&storage, &topic_id), before);
    assert_eq!(
        storage.pending_evictions().unwrap().len(),
        crate_storage::MAX_PENDING_EVICTIONS
    );
}

#[test]
fn memory_bounds_journal() {
    assert_journal_bound(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_bounds_journal() {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        crate_storage::FjallStorage::open_with_persist_mode(dir.path(), fjall::PersistMode::Buffer)
            .unwrap();
    assert_journal_bound(storage);
}

#[test]
fn rejects_too_many_pending_deps() {
    let signer = Ed25519Signer::from_bytes(&[49; 32]);
    let topic_id = TopicId::hash(b"pending-limit-topic");
    let deps = (0..=crate_storage::MAX_PENDING_MISSING_DEPS)
        .map(|i| OpId::hash(i.to_le_bytes()))
        .collect::<BTreeSet<_>>();
    let op = Op::sign(
        OpBody {
            topic_id,
            author: signer.peer_id(),
            actor_id: actor_id_for(topic_id, signer.peer_id()),
            actor_seq: 1,
            actor_prev: None,
            deps,
            generation: 1,
            payload: TopicPayload::Event(
                EventEnvelope::encode_event(&Note {
                    text: "flood".into(),
                })
                .unwrap(),
            ),
        },
        &signer,
    )
    .unwrap();
    let oplog = oplog::Oplog::with_storage(MemoryStorage::new());

    assert!(matches!(oplog.receive_op(op), Err(Error::Storage(_))));
}

#[cfg(feature = "fjall")]
#[test]
fn builder_selects_fjall() {
    let dir = tempfile::tempdir().unwrap();
    let irokle = Irokle::builder()
        .with_fjall_path(dir.path())
        .unwrap()
        .build()
        .unwrap();
    assert!(irokle.list_topics().unwrap().is_empty());
}

#[cfg(feature = "fjall")]
#[test]
fn builder_accepts_fjall_db() {
    let dir = tempfile::tempdir().unwrap();
    let db = fjall::OptimisticTxDatabase::builder(dir.path())
        .open()
        .unwrap();
    let irokle = Irokle::builder()
        .with_fjall_database(db)
        .unwrap()
        .build()
        .unwrap();

    assert!(irokle.list_topics().unwrap().is_empty());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_facades_share_actor() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
    assert_single_actor_chain(storage);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_unique_topic_ids() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
    assert_unique_topic_ids(storage);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_persists_topic_state() {
    let dir = tempfile::tempdir().unwrap();
    let signer = Ed25519Signer::from_bytes(&[7; 32]);
    let config = NodeConfig {
        signer,
        default_write_concern: WriteConcern::Local,
        ..NodeConfig::default()
    };
    let (topic_id, genesis_id, op_id, actor_id, actor_seq) = {
        let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
        let irokle = Irokle::with_storage(storage, config.clone()).unwrap();
        let topic = irokle.create_topic::<Note>(TopicConfig::default()).unwrap();
        let genesis = oplog::topological(irokle.storage(), &topic.id()).unwrap()[0].clone();
        let rec = topic
            .publish(Note {
                text: "durable".into(),
            })
            .unwrap();
        (
            topic.id(),
            genesis.id,
            rec.meta.op_id,
            rec.meta.actor_id,
            rec.meta.actor_seq,
        )
    };
    let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
    assert!(storage.get_op(&op_id).unwrap().is_some());
    assert!(storage.get_meta(&op_id).unwrap().is_some());
    assert_eq!(storage.list_ops(&topic_id).unwrap().len(), 2);
    assert_eq!(storage.list_op_ids(&topic_id).unwrap().len(), 2);
    assert!(storage.children(&genesis_id).unwrap().contains(&op_id));
    assert_eq!(
        storage
            .actor_index(&topic_id, &actor_id, actor_seq)
            .unwrap(),
        Some(op_id)
    );
    assert_eq!(
        storage.actor_tip(&topic_id, &actor_id).unwrap(),
        Some((actor_seq, op_id))
    );
    assert!(storage.actor_clock(&topic_id).unwrap().get(&actor_id) >= actor_seq);
    let heads = storage.heads(&topic_id).unwrap();
    assert!(heads.contains(&op_id));
    let topic_state = storage.topic_state(&topic_id).unwrap().unwrap();
    assert_eq!(topic_state.heads, heads);
    assert!(topic_state.heads.contains(&op_id));
    assert_eq!(storage.list_topics().unwrap().len(), 1);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_reconciles_pending() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
    assert_pending_reconciles(storage);
}

/// A genesis from one peer plus a second peer's first event on top of it, both
/// with meta. The event opens its own actor chain, so admitting it alone leaves
/// its dependency as the only thing standing in the way.
fn seed_pair(topic_id: TopicId, seed: u8) -> [(Op, crate_storage::OpMeta); 2] {
    let founder = Ed25519Signer::from_bytes(&[seed; 32]);
    let joiner = Ed25519Signer::from_bytes(&[seed.wrapping_add(1); 32]);
    let source = oplog::Oplog::with_storage(MemoryStorage::new());
    let genesis = source
        .create_topic_genesis(
            topic_id,
            actor_id_for(topic_id, founder.peer_id()),
            TopicGenesis {
                event_type_id: Note::TYPE_ID.into(),
                initial_peers: [joiner.peer_id()].into(),
                replication_policy: ReplicationPolicy::default(),
            },
            &founder,
        )
        .unwrap();
    let event = source
        .create_event_op(
            topic_id,
            actor_id_for(topic_id, joiner.peer_id()),
            EventEnvelope::encode_event(&Note {
                text: "chained".into(),
            })
            .unwrap(),
            &joiner,
        )
        .unwrap();
    [genesis, event].map(|op| {
        let meta = source.storage().get_meta(&op.id).unwrap().unwrap();
        (op, meta)
    })
}

fn assert_rejects_dangling_entry<S: Storage>(storage: S) {
    // The durability boundary must refuse an op whose dependency has no meta,
    // however the caller's pre-transaction reads decided the dep was there.
    let topic_id = TopicId::hash(b"dangling-entry-topic");
    let [(genesis, _), (event, event_meta)] = seed_pair(topic_id, 71);

    let result = storage.put_admitted_batch(crate_storage::AdmittedBatch {
        topic_id,
        expected_heads: BTreeSet::new(),
        expected_topic_state: None,
        entries: vec![(event.clone(), event_meta)],
        heads: [event.id].into(),
        topic_state: None,
        effects: crate_storage::AdmissionEffects::default(),
    });

    assert!(matches!(result, Err(Error::MissingDependency(id)) if id == genesis.id));
    assert!(storage.get_op(&event.id).unwrap().is_none());
    assert!(storage.get_meta(&event.id).unwrap().is_none());
    assert!(storage.list_op_ids(&topic_id).unwrap().is_empty());
}

#[test]
fn memory_rejects_dangling_entry() {
    assert_rejects_dangling_entry(MemoryStorage::new());
}

fn assert_rejects_partial_dep<S: Corrupt>(storage: S, drop_op: bool) {
    // Half a dependency is a hole, not a resolved edge: neither an op record
    // without metadata nor metadata without its op may let a descendant commit.
    let topic_id = TopicId::hash(b"partial-dep-topic");
    let [(genesis, genesis_meta), (event, event_meta)] = seed_pair(topic_id, 75);
    storage
        .put_admitted_batch(crate_storage::AdmittedBatch {
            topic_id,
            expected_heads: BTreeSet::new(),
            expected_topic_state: None,
            entries: vec![(genesis.clone(), genesis_meta)],
            heads: [genesis.id].into(),
            topic_state: None,
            effects: crate_storage::AdmissionEffects::default(),
        })
        .unwrap();
    if drop_op {
        storage.drop_op_record(&genesis.id);
    } else {
        storage.drop_meta_record(&genesis.id);
    }
    assert!(!storage.dep_resolvable(&genesis.id).unwrap());

    let result = storage.put_admitted_batch(crate_storage::AdmittedBatch {
        topic_id,
        expected_heads: [genesis.id].into(),
        expected_topic_state: None,
        entries: vec![(event.clone(), event_meta)],
        heads: [event.id].into(),
        topic_state: None,
        effects: crate_storage::AdmissionEffects::default(),
    });

    assert!(matches!(result, Err(Error::MissingDependency(id)) if id == genesis.id));
    assert!(storage.get_op(&event.id).unwrap().is_none());
    assert!(storage.get_meta(&event.id).unwrap().is_none());
}

#[test]
fn memory_rejects_partial_dep() {
    assert_rejects_partial_dep(MemoryStorage::new(), true);
    assert_rejects_partial_dep(MemoryStorage::new(), false);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_rejects_partial_dep() {
    let dir = tempfile::tempdir().unwrap();
    assert_rejects_partial_dep(crate_storage::FjallStorage::open(dir.path()).unwrap(), true);
    let dir = tempfile::tempdir().unwrap();
    assert_rejects_partial_dep(
        crate_storage::FjallStorage::open(dir.path()).unwrap(),
        false,
    );
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_rejects_dangling_entry() {
    let dir = tempfile::tempdir().unwrap();
    assert_rejects_dangling_entry(crate_storage::FjallStorage::open(dir.path()).unwrap());
}

fn assert_purges_waiter_closure<S: Storage>(storage: S) {
    // Dropping a dependency that can never arrive must take its whole waiter
    // chain with it in one durable step.
    let topic_id = TopicId::hash(b"waiter-closure-topic");
    let source = node(73);
    let chain = source
        .create_topic::<Note>(TopicConfig {
            initial_peers: BTreeSet::new(),
            ..TopicConfig::default()
        })
        .unwrap();
    chain.publish(Note { text: "one".into() }).unwrap();
    chain.publish(Note { text: "two".into() }).unwrap();
    let ops = oplog::topological(source.storage(), &chain.id()).unwrap();
    let log = oplog::Oplog::with_storage(storage.clone());

    // Withhold the genesis so both events buffer, the second behind the first.
    log.receive_ops_from_peer(Some(source.peer_id()), vec![ops[2].clone()])
        .unwrap();
    log.receive_ops_from_peer(Some(source.peer_id()), vec![ops[1].clone()])
        .unwrap();
    assert_eq!(storage.pending_waiters(&ops[0].id).unwrap().len(), 1);

    let purged = storage.purge_pending_waiters(&ops[0].id).unwrap();

    assert_eq!(purged, 2);
    assert!(storage.pending_waiters(&ops[0].id).unwrap().is_empty());
    assert!(storage.pending_waiters(&ops[1].id).unwrap().is_empty());
    assert!(storage.ready_pending_ops().unwrap().is_empty());
    assert!(storage.list_op_ids(&topic_id).unwrap().is_empty());
}

#[test]
fn memory_purges_waiter_closure() {
    assert_purges_waiter_closure(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_purges_waiter_closure() {
    let dir = tempfile::tempdir().unwrap();
    assert_purges_waiter_closure(crate_storage::FjallStorage::open(dir.path()).unwrap());
}

/// Everything a caller can observe about one topic, so a failed reset can be
/// proved unchanged rather than merely still present.
#[derive(Debug, PartialEq)]
struct TopicSnapshot {
    op_ids: BTreeSet<OpId>,
    records: Vec<(Option<Op>, Option<crate_storage::OpMeta>)>,
    heads: BTreeSet<OpId>,
    clock: ActorClock,
    state: Option<crate_storage::TopicState>,
    fingerprint: [u8; 32],
    max_generation: u64,
}

fn topic_snapshot<S: Storage>(storage: &S, topic_id: &TopicId) -> TopicSnapshot {
    let op_ids = storage.list_op_ids(topic_id).unwrap();
    TopicSnapshot {
        records: op_ids
            .iter()
            .map(|id| (storage.get_op(id).unwrap(), storage.get_meta(id).unwrap()))
            .collect(),
        op_ids,
        heads: storage.heads(topic_id).unwrap(),
        clock: storage.actor_clock(topic_id).unwrap(),
        state: storage.topic_state(topic_id).unwrap(),
        fingerprint: storage.topic_fingerprint(topic_id).unwrap(),
        max_generation: storage.max_generation(topic_id).unwrap(),
    }
}

fn assert_reset_rollback<S: Storage>(storage: S) {
    // A winner batch the durability boundary rejects must leave the local chain
    // in place: an empty topic is worse than the chain the reset was meant to
    // replace, and the caller has no way back to it.
    let topic_id = TopicId::hash(b"reset-rollback-topic");
    seed_chain(&storage, topic_id, 81, &["one", "two"]);
    let survivor_id = TopicId::hash(b"reset-rollback-survivor");
    seed_chain(&storage, survivor_id, 83, &["keep"]);
    let before = topic_snapshot(&storage, &topic_id);
    let survivor_before = topic_snapshot(&storage, &survivor_id);
    let expected_state = storage.topic_state(&topic_id).unwrap().unwrap();

    // A winner whose own genesis is withheld: it passes every precondition and
    // is only rejected once the reset has already been staged.
    let [(genesis, _), (event, event_meta)] = seed_pair(topic_id, 85);
    let result = storage.reset_topic_and_admit(
        &topic_id,
        &expected_state,
        crate_storage::AdmittedBatch {
            topic_id,
            expected_heads: BTreeSet::new(),
            expected_topic_state: None,
            entries: vec![(event.clone(), event_meta)],
            heads: [event.id].into(),
            topic_state: None,
            effects: crate_storage::AdmissionEffects::default(),
        },
        Some(&filler_eviction(topic_id, genesis.id, 0)),
    );

    assert!(matches!(result, Err(Error::MissingDependency(id)) if id == genesis.id));
    assert_eq!(topic_snapshot(&storage, &topic_id), before);
    assert_eq!(topic_snapshot(&storage, &survivor_id), survivor_before);
    assert!(storage.get_op(&event.id).unwrap().is_none());
    // The record belongs to the reset: no reset, no record to acknowledge.
    assert!(storage.pending_evictions().unwrap().is_empty());
}

#[test]
fn memory_reset_rollback() {
    assert_reset_rollback(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_reset_rollback() {
    let dir = tempfile::tempdir().unwrap();
    assert_reset_rollback(crate_storage::FjallStorage::open(dir.path()).unwrap());
}

fn assert_vacuous_obligation<S: Storage>(storage: S) {
    let peer = PeerId::hash(b"vacuous-peer");
    let topic_id = TopicId::hash(b"vacuous-topic");
    let actor = actor_id_for(topic_id, peer);
    let (_log, _signer, seeded, _event) =
        forked_side(storage.clone(), topic_id, 81, [peer], "vacuous");
    let mut target_clock = ActorClock::new();
    target_clock.observe(actor, 4);
    storage
        .put_sync_obligation(crate_storage::SyncObligation::clock(
            peer,
            topic_id,
            target_clock,
        ))
        .unwrap();

    // An ack that proves nothing must not stand in for the clock target.
    let cleared = storage
        .apply_peer_ack(crate_storage::PeerAck {
            peer_id: peer,
            topic_id,
            genesis: Some(seeded.id),
            heads: BTreeSet::new(),
            clock: ActorClock::new(),
        })
        .unwrap();
    assert_eq!(cleared, 0);
    assert_eq!(storage.sync_obligations(&peer, &topic_id).unwrap().len(), 1);

    let mut proof = ActorClock::new();
    proof.observe(actor, 4);
    let cleared = storage
        .apply_peer_ack(crate_storage::PeerAck {
            peer_id: peer,
            topic_id,
            genesis: Some(seeded.id),
            heads: BTreeSet::new(),
            clock: proof,
        })
        .unwrap();
    assert_eq!(cleared, 1);
    assert!(
        storage
            .sync_obligations(&peer, &topic_id)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn memory_vacuous_obligation() {
    assert_vacuous_obligation(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_vacuous_obligation() {
    let dir = tempfile::tempdir().unwrap();
    assert_vacuous_obligation(crate_storage::FjallStorage::open(dir.path()).unwrap());
}

fn assert_merged_acks<S: Storage>(storage: S) {
    let peer = PeerId::hash(b"merge-peer");
    let topic_id = TopicId::hash(b"merge-topic");
    let (_log, _signer, seeded, _event) =
        forked_side(storage.clone(), topic_id, 82, [peer], "merge");
    let first_actor = actor_id_for(topic_id, PeerId::hash(b"merge-actor-one"));
    let second_actor = actor_id_for(topic_id, PeerId::hash(b"merge-actor-two"));
    let mut target_clock = ActorClock::new();
    target_clock.observe(first_actor, 5);
    target_clock.observe(second_actor, 3);
    storage
        .put_sync_obligation(crate_storage::SyncObligation::clock(
            peer,
            topic_id,
            target_clock,
        ))
        .unwrap();

    let mut first_clock = ActorClock::new();
    first_clock.observe(first_actor, 5);
    let first_ack = crate_storage::PeerAck {
        peer_id: peer,
        topic_id,
        genesis: Some(seeded.id),
        heads: BTreeSet::new(),
        clock: first_clock,
    };
    assert_eq!(storage.apply_peer_ack(first_ack).unwrap(), 0);

    let mut second_clock = ActorClock::new();
    second_clock.observe(second_actor, 3);
    let second_ack = crate_storage::PeerAck {
        peer_id: peer,
        topic_id,
        genesis: Some(seeded.id),
        heads: BTreeSet::new(),
        clock: second_clock,
    };
    assert_eq!(storage.apply_peer_ack(second_ack.clone()).unwrap(), 1);

    // Incomparable evidence adds a component instead of replacing the proven one.
    let stored = storage.peer_ack(&peer, &topic_id).unwrap().unwrap();
    assert_eq!(stored.clock.get(&first_actor), 5);
    assert_eq!(stored.clock.get(&second_actor), 3);

    assert_eq!(storage.apply_peer_ack(second_ack).unwrap(), 0);
    let replayed = storage.peer_ack(&peer, &topic_id).unwrap().unwrap();
    assert_eq!(replayed.clock, stored.clock);
}

#[test]
fn memory_merged_acks() {
    assert_merged_acks(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_merged_acks() {
    let dir = tempfile::tempdir().unwrap();
    assert_merged_acks(crate_storage::FjallStorage::open(dir.path()).unwrap());
}

fn assert_status_counters<S: Storage>(storage: S) {
    let peer = PeerId::hash(b"counter-peer");
    let topic_id = TopicId::hash(b"counter-topic");
    let success = crate_storage::SyncStatusUpdate {
        successful_attempts: 1,
        last_attempt_ms: Some(20),
        last_success_ms: Some(20),
        last_error: Some(None),
        state: crate_storage::SyncStateUpdate::Set(crate_storage::SyncPeerState::Healthy),
        ..crate_storage::SyncStatusUpdate::default()
    };
    let failure = crate_storage::SyncStatusUpdate {
        failed_attempts: 1,
        last_attempt_ms: Some(10),
        last_error: Some(Some("dial failed".into())),
        state: crate_storage::SyncStateUpdate::Set(crate_storage::SyncPeerState::Failed),
        ..crate_storage::SyncStatusUpdate::default()
    };

    let first = storage
        .update_sync_status(&peer, &topic_id, &success)
        .unwrap();
    assert_eq!(first.peer_id, peer);
    assert_eq!(first.successful_attempts, 1);
    let second = storage
        .update_sync_status(&peer, &topic_id, &failure)
        .unwrap();
    assert_eq!(second.successful_attempts, 1);
    // The attempt still counts even though it describes an older moment.
    assert_eq!(second.failed_attempts, 1);
    // A failure that finished before the recorded success must not replace it.
    assert_eq!(second.state, crate_storage::SyncPeerState::Healthy);
    assert_eq!(second.last_error, None);
    // A late attempt timestamp cannot rewind the record.
    assert_eq!(second.last_attempt_ms, Some(20));
    assert_eq!(second.last_success_ms, Some(20));

    // A failure that really is newer does take the record.
    let newer_failure = crate_storage::SyncStatusUpdate {
        failed_attempts: 1,
        last_attempt_ms: Some(30),
        last_error: Some(Some("dial failed".into())),
        state: crate_storage::SyncStateUpdate::Set(crate_storage::SyncPeerState::Failed),
        ..crate_storage::SyncStatusUpdate::default()
    };
    let third = storage
        .update_sync_status(&peer, &topic_id, &newer_failure)
        .unwrap();
    assert_eq!(third.failed_attempts, 2);
    assert_eq!(third.state, crate_storage::SyncPeerState::Failed);

    // The guard drops an update whose attempt context is already superseded.
    let stale = crate_storage::SyncStatusUpdate {
        expected_attempts: Some(0),
        state: crate_storage::SyncStateUpdate::Set(crate_storage::SyncPeerState::Idle),
        ..crate_storage::SyncStatusUpdate::default()
    };
    let guarded = storage
        .update_sync_status(&peer, &topic_id, &stale)
        .unwrap();
    assert_eq!(guarded.state, crate_storage::SyncPeerState::Failed);
    assert_eq!(guarded.failed_attempts, 2, "the guard carries no attempt");
    assert_eq!(
        storage.sync_statuses(&topic_id).unwrap()[0].state,
        crate_storage::SyncPeerState::Failed
    );

    // A guarded update on an unknown peer records nothing.
    let other = PeerId::hash(b"counter-peer-other");
    let unmatched = crate_storage::SyncStatusUpdate {
        expected_attempts: Some(1),
        ..stale.clone()
    };
    storage
        .update_sync_status(&other, &topic_id, &unmatched)
        .unwrap();
    assert_eq!(storage.sync_statuses(&topic_id).unwrap().len(), 1);

    let rounds = 16_u64;
    let threads = 2;
    let barrier = Arc::new(Barrier::new(threads));
    let handles = (0..threads)
        .map(|index| {
            let storage = storage.clone();
            let barrier = Arc::clone(&barrier);
            let update = if index == 0 {
                success.clone()
            } else {
                failure.clone()
            };
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..rounds {
                    storage
                        .update_sync_status(&peer, &topic_id, &update)
                        .unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }

    let status = storage
        .sync_statuses(&topic_id)
        .unwrap()
        .into_iter()
        .find(|status| status.peer_id == peer)
        .unwrap();
    assert_eq!(status.successful_attempts, rounds + 1);
    // Two failures were recorded before the concurrent rounds: the stale one,
    // whose attempt still counts, and the newer one that took the record.
    assert_eq!(status.failed_attempts, rounds + 2);
}

#[test]
fn memory_status_counters() {
    assert_status_counters(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_status_counters() {
    let dir = tempfile::tempdir().unwrap();
    assert_status_counters(crate_storage::FjallStorage::open(dir.path()).unwrap());
}

fn clock_at(actor: ActorId, seq: u64) -> ActorClock {
    let mut clock = ActorClock::new();
    clock.observe(actor, seq);
    clock
}

fn put_target<S: Storage>(storage: &S, peer_id: PeerId, topic_id: TopicId, clock: ActorClock) {
    storage
        .put_sync_obligation(crate_storage::SyncObligation::clock(
            peer_id, topic_id, clock,
        ))
        .unwrap();
}

fn assert_coalesced_targets<S: Storage>(storage: S) {
    let peer = PeerId::hash(b"coalesce-peer");
    let other = PeerId::hash(b"coalesce-other");
    let topic_id = TopicId::hash(b"coalesce-topic");
    let (_log, _signer, seeded, _event) =
        forked_side(storage.clone(), topic_id, 83, [peer, other], "coalesce");
    let actor = actor_id_for(topic_id, peer);

    for rounds in [8_u64, 16] {
        for seq in 1..=rounds {
            let mut clock = ActorClock::new();
            clock.observe(actor, seq);
            put_target(&storage, peer, topic_id, clock);
        }
        // One target record per peer and topic, whatever the backlog length.
        let obligations = storage.sync_obligations(&peer, &topic_id).unwrap();
        assert_eq!(obligations.len(), 1);
        assert_eq!(
            obligations[0].target,
            crate_storage::ObligationTarget::Clock(clock_at(actor, rounds))
        );
    }
    assert_eq!(
        storage
            .topic_obligation_counts(&topic_id)
            .unwrap()
            .get(&peer),
        Some(&1)
    );

    // Explicit repair wants coalesce into one record of their own.
    for _ in 0..3 {
        storage
            .put_sync_obligation(crate_storage::SyncObligation::repair(
                peer,
                topic_id,
                [OpId::hash(b"coalesce-want-one")].into(),
            ))
            .unwrap();
    }
    storage
        .put_sync_obligation(crate_storage::SyncObligation::repair(
            peer,
            topic_id,
            [OpId::hash(b"coalesce-want-two")].into(),
        ))
        .unwrap();
    assert_eq!(storage.sync_obligations(&peer, &topic_id).unwrap().len(), 2);

    let mut lagging = ActorClock::new();
    lagging.observe(actor, 4);
    put_target(&storage, other, topic_id, lagging);
    let mut proof = ActorClock::new();
    proof.observe(actor, 16);
    let cleared = storage
        .apply_peer_ack(crate_storage::PeerAck {
            peer_id: peer,
            topic_id,
            genesis: Some(seeded.id),
            heads: BTreeSet::new(),
            clock: proof,
        })
        .unwrap();

    // Clearing one peer's reached target leaves the explicit wants and every
    // other peer's backlog in place.
    assert_eq!(cleared, 1);
    assert_eq!(
        storage.sync_obligations(&peer, &topic_id).unwrap(),
        vec![crate_storage::SyncObligation::repair(
            peer,
            topic_id,
            [
                OpId::hash(b"coalesce-want-one"),
                OpId::hash(b"coalesce-want-two")
            ]
            .into(),
        )]
    );
    assert_eq!(
        storage.sync_obligations(&other, &topic_id).unwrap().len(),
        1
    );
}

#[test]
fn memory_coalesced_targets() {
    assert_coalesced_targets(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_coalesced_targets() {
    let dir = tempfile::tempdir().unwrap();
    assert_coalesced_targets(crate_storage::FjallStorage::open(dir.path()).unwrap());
}

/// A dependency hole a later repair can close must leave the buffered op in
/// place: that hole is the condition the pending buffer exists for.
fn assert_retains_pending<S: Corrupt>(storage: S) {
    let source = node(95);
    let outsider = Ed25519Signer::from_bytes(&[96; 32]);
    let topic = source
        .create_topic::<Note>(TopicConfig {
            initial_peers: [outsider.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    for text in ["one", "two", "three"] {
        topic.publish(Note { text: text.into() }).unwrap();
    }
    let ops = oplog::topological(source.storage(), &topic.id()).unwrap();
    let (genesis, first, second, third) = (
        ops[0].clone(),
        ops[1].clone(),
        ops[2].clone(),
        ops[3].clone(),
    );
    // Depending on `second` alone rather than on the heads makes admission
    // project membership through an ancestry walk that reads the genesis.
    let waiter = Op::sign(
        OpBody {
            topic_id: topic.id(),
            author: outsider.peer_id(),
            actor_id: actor_id_for(topic.id(), outsider.peer_id()),
            actor_seq: 1,
            actor_prev: None,
            deps: [second.id].into(),
            generation: second.signed.body.generation + 1,
            payload: TopicPayload::Event(
                EventEnvelope::encode_event(&Note {
                    text: "waiter".into(),
                })
                .unwrap(),
            ),
        },
        &outsider,
    )
    .unwrap();

    let log = oplog::Oplog::with_storage(storage.clone());
    log.receive_ops_from_peer(Some(source.peer_id()), vec![waiter.clone()])
        .unwrap();
    log.receive_ops_from_peer(Some(source.peer_id()), vec![genesis.clone(), first.clone()])
        .unwrap();
    assert_eq!(storage.pending_waiters(&second.id).unwrap().len(), 1);

    // The genesis metadata the walk needs disappears after the waiter was
    // buffered, so admitting the waiter fails on a dependency that can return.
    damage_op(&storage, &genesis.id, Damage::Meta);
    let fresh = oplog::Oplog::with_storage(storage.clone());
    let admitted = fresh
        .receive_ops_from_peer(Some(source.peer_id()), vec![second.clone(), third.clone()])
        .unwrap();

    assert_eq!(admitted, [second.id, third.id].into());
    assert_eq!(storage.pending_waiters(&second.id).unwrap().len(), 1);
    assert!(storage.get_op(&waiter.id).unwrap().is_none());

    // Repairing the hole lets the retained record through.
    let repaired = fresh
        .receive_ops_from_peer(Some(source.peer_id()), vec![genesis.clone()])
        .unwrap();
    assert_eq!(repaired, [genesis.id, waiter.id].into());
    assert!(storage.get_op(&waiter.id).unwrap().is_some());
    assert!(storage.pending_waiters(&second.id).unwrap().is_empty());
}

#[test]
fn memory_retains_pending() {
    assert_retains_pending(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_retains_pending() {
    let dir = tempfile::tempdir().unwrap();
    assert_retains_pending(crate_storage::FjallStorage::open(dir.path()).unwrap());
}

/// Rejecting a permanently invalid pending root must take its whole waiting
/// subtree, and only that subtree, in one durable step.
fn assert_rejects_subtree<S: Storage>(storage: S) {
    let source = node(97);
    let outsider = Ed25519Signer::from_bytes(&[98; 32]);
    let topic = source
        .create_topic::<Note>(TopicConfig {
            initial_peers: [outsider.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    for text in ["one", "two", "three"] {
        topic.publish(Note { text: text.into() }).unwrap();
    }
    let ops = oplog::topological(source.storage(), &topic.id()).unwrap();
    let (genesis, root, child, grandchild) = (
        ops[0].clone(),
        ops[1].clone(),
        ops[2].clone(),
        ops[3].clone(),
    );
    // Waits on the same withheld genesis as `root` without being behind it.
    let sibling = Op::sign(
        OpBody {
            topic_id: topic.id(),
            author: outsider.peer_id(),
            actor_id: actor_id_for(topic.id(), outsider.peer_id()),
            actor_seq: 1,
            actor_prev: None,
            deps: [genesis.id].into(),
            generation: genesis.signed.body.generation + 1,
            payload: TopicPayload::Event(
                EventEnvelope::encode_event(&Note {
                    text: "sibling".into(),
                })
                .unwrap(),
            ),
        },
        &outsider,
    )
    .unwrap();

    let log = oplog::Oplog::with_storage(storage.clone());
    for op in [&grandchild, &child, &root, &sibling] {
        log.receive_ops_from_peer(Some(source.peer_id()), vec![op.clone()])
            .unwrap();
    }
    assert_eq!(storage.pending_waiters(&genesis.id).unwrap().len(), 2);
    assert_eq!(
        storage.pending_missing_deps(&topic.id()).unwrap(),
        [genesis.id, root.id, child.id].into()
    );

    assert_eq!(storage.reject_pending_subtree(&root.id).unwrap(), 3);

    let waiting = storage.pending_waiters(&genesis.id).unwrap();
    assert_eq!(waiting.len(), 1);
    assert_eq!(waiting[0].1.id, sibling.id);
    assert!(storage.pending_waiters(&root.id).unwrap().is_empty());
    assert!(storage.pending_waiters(&child.id).unwrap().is_empty());
    assert_eq!(
        storage.pending_missing_deps(&topic.id()).unwrap(),
        [genesis.id].into()
    );
    // A second rejection of the same root changes nothing.
    assert_eq!(storage.reject_pending_subtree(&root.id).unwrap(), 0);
    assert_eq!(storage.pending_waiters(&genesis.id).unwrap().len(), 1);

    assert_eq!(
        log.receive_ops_from_peer(Some(source.peer_id()), vec![genesis.clone()])
            .unwrap(),
        [genesis.id, sibling.id].into()
    );
    for rejected in [&root, &child, &grandchild] {
        assert!(storage.get_op(&rejected.id).unwrap().is_none());
    }
    assert!(storage.ready_pending_ops().unwrap().is_empty());
    assert!(
        storage
            .pending_missing_deps(&topic.id())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn memory_rejects_subtree() {
    assert_rejects_subtree(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_rejects_subtree() {
    let dir = tempfile::tempdir().unwrap();
    assert_rejects_subtree(crate_storage::FjallStorage::open(dir.path()).unwrap());
}

/// A pending op whose dependency turns out to belong to another topic can never
/// be admitted here, so admission must clear it together with what waits behind
/// it, including the repair wants that would otherwise re-request it forever.
fn assert_rejects_mismatch<S: Storage>(storage: S) {
    let source = node(99);
    let outsider = Ed25519Signer::from_bytes(&[100; 32]);
    let config = TopicConfig {
        initial_peers: [outsider.peer_id()].into(),
        ..TopicConfig::default()
    };
    let home = source.create_topic::<Note>(config.clone()).unwrap();
    let other = source.create_topic::<Note>(config).unwrap();
    other
        .publish(Note {
            text: "other".into(),
        })
        .unwrap();
    let home_ops = oplog::topological(source.storage(), &home.id()).unwrap();
    let other_ops = oplog::topological(source.storage(), &other.id()).unwrap();
    let (home_genesis, other_genesis, foreign) = (
        home_ops[0].clone(),
        other_ops[0].clone(),
        other_ops[1].clone(),
    );
    let sign_waiter = |seq: u64, prev: Option<OpId>, dep: OpId, generation: u64, text: &str| {
        Op::sign(
            OpBody {
                topic_id: home.id(),
                author: outsider.peer_id(),
                actor_id: actor_id_for(home.id(), outsider.peer_id()),
                actor_seq: seq,
                actor_prev: prev,
                deps: [dep].into(),
                generation,
                payload: TopicPayload::Event(
                    EventEnvelope::encode_event(&Note { text: text.into() }).unwrap(),
                ),
            },
            &outsider,
        )
        .unwrap()
    };
    let root = sign_waiter(
        1,
        None,
        foreign.id,
        foreign.signed.body.generation + 1,
        "root",
    );
    let child = sign_waiter(
        2,
        Some(root.id),
        root.id,
        root.signed.body.generation + 1,
        "child",
    );

    let log = oplog::Oplog::with_storage(storage.clone());
    for op in [&home_genesis, &child, &root] {
        log.receive_ops_from_peer(Some(source.peer_id()), vec![op.clone()])
            .unwrap();
    }
    assert_eq!(
        storage.pending_missing_deps(&home.id()).unwrap(),
        [root.id, foreign.id].into()
    );

    // Admitting the dependency under its real topic makes the root ready, and
    // the mismatch it fails on is a property of the signed records.
    let admitted = log
        .receive_ops_from_peer(
            Some(source.peer_id()),
            vec![other_genesis.clone(), foreign.clone()],
        )
        .unwrap();

    assert_eq!(admitted, [other_genesis.id, foreign.id].into());
    assert!(storage.get_op(&root.id).unwrap().is_none());
    assert!(storage.get_op(&child.id).unwrap().is_none());
    assert!(storage.pending_waiters(&root.id).unwrap().is_empty());
    assert!(storage.pending_waiters(&foreign.id).unwrap().is_empty());
    assert!(storage.pending_missing_deps(&home.id()).unwrap().is_empty());
    assert!(storage.ready_pending_ops().unwrap().is_empty());
}

#[test]
fn memory_rejects_mismatch() {
    assert_rejects_mismatch(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_rejects_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    assert_rejects_mismatch(crate_storage::FjallStorage::open(dir.path()).unwrap());
}

/// A terminal result that finished earlier must not install its state over a
/// newer one, in either arrival order, while both attempts still count.
fn assert_status_ordering<S: Storage>(storage: S) {
    let peer = PeerId::hash(b"ordering-peer");
    let topic_id = TopicId::hash(b"ordering-topic");
    let failure = |at: u64| crate_storage::SyncStatusUpdate {
        failed_attempts: 1,
        last_attempt_ms: Some(at),
        last_error: Some(Some("dial failed".into())),
        state: crate_storage::SyncStateUpdate::Set(crate_storage::SyncPeerState::Failed),
        ..crate_storage::SyncStatusUpdate::default()
    };
    let success = |at: u64| crate_storage::SyncStatusUpdate {
        successful_attempts: 1,
        last_attempt_ms: Some(at),
        last_success_ms: Some(at),
        last_error: Some(None),
        state: crate_storage::SyncStateUpdate::Set(crate_storage::SyncPeerState::Healthy),
        ..crate_storage::SyncStatusUpdate::default()
    };

    // A newer failure, then a late older success.
    storage
        .update_sync_status(&peer, &topic_id, &failure(30))
        .unwrap();
    let late = storage
        .update_sync_status(&peer, &topic_id, &success(20))
        .unwrap();
    assert_eq!(
        late.state,
        crate_storage::SyncPeerState::Failed,
        "an older success must not replace the newer failure"
    );
    assert_eq!(late.successful_attempts, 1, "its attempt still counts");
    assert_eq!(late.failed_attempts, 1);
    assert_eq!(late.last_error, Some("dial failed".into()));

    // On an equal timestamp the success is kept over a concurrent failure.
    let other = PeerId::hash(b"ordering-peer-tie");
    storage
        .update_sync_status(&other, &topic_id, &success(40))
        .unwrap();
    let tied = storage
        .update_sync_status(&other, &topic_id, &failure(40))
        .unwrap();
    assert_eq!(tied.state, crate_storage::SyncPeerState::Healthy);
    assert_eq!(tied.failed_attempts, 1);
    assert_eq!(tied.successful_attempts, 1);
}

#[test]
fn memory_status_ordering() {
    assert_status_ordering(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_status_ordering() {
    let dir = tempfile::tempdir().unwrap();
    assert_status_ordering(crate_storage::FjallStorage::open(dir.path()).unwrap());
}

/// Buffered pending payloads are bounded by bytes, not only by record count: a
/// count budget multiplied by the frame limit is far more memory than a node
/// should hold. The charge is released again when the record goes.
fn assert_pending_byte_budget<S: Storage>(storage: S) {
    let signer = Ed25519Signer::from_bytes(&[170; 32]);
    let source = signer.peer_id();
    let topic_id = TopicId::hash(b"pending-bytes-topic");
    let actor_id = actor_id_for(topic_id, source);
    let missing = OpId::hash(b"pending-bytes-missing");
    // Five of these exceed the per-source byte quota; four do not.
    let chunk = crate_storage::MAX_PENDING_BYTES_PER_SOURCE / 4;

    let buffer = |index: usize| {
        let op = Op::sign(
            OpBody {
                topic_id,
                author: source,
                actor_id,
                actor_seq: index as u64 + 1,
                actor_prev: Some(missing),
                deps: [missing].into(),
                generation: 9,
                payload: TopicPayload::Event(
                    EventEnvelope::encode_event(&Note {
                        text: format!("{index}{}", "x".repeat(chunk)),
                    })
                    .unwrap(),
                ),
            },
            &signer,
        )
        .unwrap();
        let meta = crate_storage::OpMeta {
            id: op.id,
            topic_id,
            author: source,
            actor_id,
            actor_seq: index as u64 + 1,
            actor_prev: Some(missing),
            deps: [missing].into(),
            generation: 9,
            observed_clock: ActorClock::new(),
            ready: false,
            missing_deps: [missing].into(),
        };
        (op.id, op, meta)
    };

    let mut buffered = Vec::new();
    let mut refused = None;
    for index in 0..8 {
        let (op_id, op, meta) = buffer(index);
        match storage.put_pending_op(source, op, meta) {
            Ok(()) => buffered.push(op_id),
            Err(error) => {
                refused = Some(error);
                break;
            }
        }
    }
    let refused = refused.expect("the byte quota must refuse a pending pool this large");
    assert!(
        refused.to_string().contains("byte"),
        "the refusal must name the byte budget, got {refused}"
    );
    assert!(
        !buffered.is_empty() && buffered.len() < 8,
        "some records fit and the rest were refused, buffered {}",
        buffered.len()
    );

    // Removing one record releases its charge, so one more fits again.
    storage.remove_pending_op(&buffered[0]).unwrap();
    let (retry_id, retry_op, retry_meta) = buffer(100);
    storage
        .put_pending_op(source, retry_op, retry_meta)
        .expect("a released charge must be reusable");

    // A different source has its own quota and is unaffected.
    let other_signer = Ed25519Signer::from_bytes(&[171; 32]);
    let other = other_signer.peer_id();
    let (_, other_op, mut other_meta) = buffer(200);
    other_meta.id = other_op.id;
    storage
        .put_pending_op(other, other_op, other_meta)
        .expect("a second source is charged separately");

    // Dropping everything for the first source frees its whole charge.
    storage.remove_pending_op(&retry_id).unwrap();
    for op_id in buffered.iter().skip(1) {
        storage.remove_pending_op(op_id).unwrap();
    }
    let (_, again, again_meta) = buffer(300);
    storage
        .put_pending_op(source, again, again_meta)
        .expect("the source quota is fully released");
}

#[test]
fn memory_pending_byte_budget() {
    assert_pending_byte_budget(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_pending_byte_budget() {
    let dir = tempfile::tempdir().unwrap();
    assert_pending_byte_budget(crate_storage::FjallStorage::open(dir.path()).unwrap());
}
