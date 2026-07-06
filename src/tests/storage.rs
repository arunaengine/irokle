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
        .unwrap();
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
            heads: storage.heads(&topic_id).unwrap(),
            clock: ack_clock,
        })
        .unwrap();
    storage
        .put_sync_obligation(crate_storage::SyncObligation {
            peer_id: other_peer,
            topic_id,
            op_ids: [OpId::hash(b"reset-obligation-op")].into(),
            target_clock: ActorClock::new(),
        })
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
