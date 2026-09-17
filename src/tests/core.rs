use super::support::*;

#[test]
fn rejects_tampered_id() {
    let signer = Ed25519Signer::from_bytes(&[1; 32]);
    let topic_id = TopicId::hash(b"topic");
    let body = OpBody {
        topic_id,
        author: signer.peer_id(),
        actor_id: ActorId::hash(b"actor"),
        actor_seq: 1,
        actor_prev: None,
        deps: BTreeSet::new(),
        generation: 0,
        payload: TopicPayload::Genesis(TopicGenesis::new("test.note", [signer.peer_id()])),
    };
    let mut op = Op::sign(body, &signer).unwrap();
    assert!(op.validate().is_ok());
    if let TopicPayload::Genesis(genesis) = &mut op.signed.body.payload {
        genesis.event_type_id = "tampered".into();
    }
    assert!(matches!(op.validate(), Err(Error::InvalidOpId)));
}

#[test]
fn actor_chain_links() {
    let irokle = node(2);
    let topic = irokle.create_topic::<Note>(TopicConfig::default()).unwrap();
    topic.publish(Note { text: "one".into() }).unwrap();
    topic.publish(Note { text: "two".into() }).unwrap();
    let ops = irokle.raw_topic(topic.id()).unwrap().history().unwrap();
    assert_eq!(ops[0].signed.body.actor_seq, 1);
    assert_eq!(ops[1].signed.body.actor_seq, 2);
    assert_eq!(ops[1].signed.body.actor_prev, Some(ops[0].id));
    assert_eq!(ops[2].signed.body.actor_seq, 3);
    assert_eq!(ops[2].signed.body.actor_prev, Some(ops[1].id));
}

#[test]
fn clones_serialize_actor() {
    let irokle = node(44);
    let topic = irokle.create_topic::<Note>(TopicConfig::default()).unwrap();
    let topic_id = topic.id();
    let publishes = 32_u64;

    let handles = (0..publishes)
        .map(|i| {
            let node = irokle.clone();
            thread::spawn(move || {
                let topic = node.open_topic::<Note>(topic_id).unwrap();
                topic
                    .publish(Note {
                        text: format!("note {i}"),
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
    let ops = irokle.raw_topic(topic_id).unwrap().history().unwrap();
    assert_eq!(ops.len(), publishes as usize + 1);
    assert_eq!(
        ops.iter().map(|op| op.id).collect::<BTreeSet<_>>().len(),
        ops.len()
    );
    assert_eq!(topic.heads().unwrap().len(), 1);
}

#[test]
fn builder_keeps_inputs() {
    let signer = Ed25519Signer::from_bytes(&[77; 32]);
    let irokle = Irokle::builder()
        .with_storage(MemoryStorage::new())
        .with_signer(signer.clone())
        .build()
        .unwrap();

    assert_eq!(irokle.peer_id(), signer.peer_id());
    assert!(irokle.list_topics().unwrap().is_empty());
}

#[test]
fn topic_open_mismatch() {
    let irokle = node(3);
    let topic = irokle.create_topic::<Note>(TopicConfig::default()).unwrap();
    let err = match irokle.open_topic::<Other>(topic.id()) {
        Ok(_) => panic!("opening with wrong event type unexpectedly succeeded"),
        Err(err) => err,
    };
    assert!(matches!(err, Error::EventTypeMismatch { .. }));
}

#[test]
fn dag_respects_limit() {
    let alice = node(46);
    let topic = alice.create_topic::<Note>(TopicConfig::default()).unwrap();
    let first = topic.publish(Note { text: "one".into() }).unwrap();
    let second = topic.publish(Note { text: "two".into() }).unwrap();
    let third = topic
        .publish(Note {
            text: "three".into(),
        })
        .unwrap();

    let tail = topic
        .dag(history::DagQuery::default().newest_first().limit(2))
        .unwrap();
    assert_eq!(
        tail.iter().map(|op| op.id).collect::<Vec<_>>(),
        vec![third.meta.op_id, second.meta.op_id]
    );

    let from_middle = topic
        .dag(
            history::DagQuery::from_heads([second.meta.op_id])
                .newest_first()
                .include_heads(false)
                .limit(1),
        )
        .unwrap();
    assert_eq!(
        from_middle.iter().map(|op| op.id).collect::<Vec<_>>(),
        vec![first.meta.op_id]
    );
}

#[test]
fn envelope_checks_type() {
    let envelope = EventEnvelope {
        type_id: "x".into(),
        payload: Bytes::new(),
    };
    assert!(matches!(
        envelope.decode_event::<Note>(),
        Err(Error::EventTypeMismatch { .. })
    ));
}

#[test]
fn topic_event_equivalent() {
    let signer = Ed25519Signer::from_bytes(&[71; 32]);
    let topic_id = TopicId::hash(b"combined-create");
    let actor_id = actor_id_for(topic_id, signer.peer_id());
    let genesis = TopicGenesis {
        event_type_id: Note::TYPE_ID.to_owned(),
        initial_peers: BTreeSet::new(),
        replication_policy: ReplicationPolicy::all(),
    };
    let envelope = EventEnvelope::encode_event(&Note {
        text: "combined".into(),
    })
    .unwrap();

    let combined = oplog::Oplog::new();
    let (genesis_op, event_op) = combined
        .create_topic_genesis_with_event(
            topic_id,
            actor_id,
            genesis.clone(),
            envelope.clone(),
            &signer,
        )
        .unwrap();

    let split = oplog::Oplog::new();
    let split_genesis = split
        .create_topic_genesis(topic_id, actor_id, genesis, &signer)
        .unwrap();
    let split_event = split
        .create_event_op(topic_id, actor_id, envelope, &signer)
        .unwrap();

    // Ed25519 signing is deterministic, so the same signer and bodies yield
    // identical ops in both stores.
    assert_eq!(genesis_op, split_genesis);
    assert_eq!(event_op, split_event);
    assert_eq!(event_op.signed.body.actor_seq, 2);
    assert_eq!(event_op.signed.body.actor_prev, Some(genesis_op.id));
    assert_eq!(event_op.signed.body.deps, [genesis_op.id].into());
    assert_eq!(
        combined.storage().topic_state(&topic_id).unwrap(),
        split.storage().topic_state(&topic_id).unwrap()
    );
    assert_eq!(
        combined.storage().heads(&topic_id).unwrap(),
        split.storage().heads(&topic_id).unwrap()
    );
    assert_eq!(
        combined.storage().actor_tip(&topic_id, &actor_id).unwrap(),
        split.storage().actor_tip(&topic_id, &actor_id).unwrap()
    );
    for id in [genesis_op.id, event_op.id] {
        assert_eq!(
            combined.storage().get_meta(&id).unwrap(),
            split.storage().get_meta(&id).unwrap()
        );
    }
}

#[test]
fn topic_event_race() {
    let signer = Ed25519Signer::from_bytes(&[72; 32]);
    let topic_id = TopicId::hash(b"combined-race");
    let actor_id = actor_id_for(topic_id, signer.peer_id());
    let genesis = TopicGenesis {
        event_type_id: Note::TYPE_ID.to_owned(),
        initial_peers: BTreeSet::new(),
        replication_policy: ReplicationPolicy::all(),
    };
    let envelope = EventEnvelope::encode_event(&Note {
        text: "raced".into(),
    })
    .unwrap();

    let oplog = oplog::Oplog::new();
    oplog
        .create_topic_genesis(topic_id, actor_id, genesis.clone(), &signer)
        .unwrap();

    let err = oplog
        .create_topic_genesis_with_event(topic_id, actor_id, genesis, envelope.clone(), &signer)
        .unwrap_err();
    assert!(matches!(err, Error::InvalidGenesis));

    // Same fallback as the two-call path: the topic exists, so the caller
    // re-reads state and publishes the event directly.
    let op = oplog
        .create_event_op(topic_id, actor_id, envelope, &signer)
        .unwrap();
    assert_eq!(op.signed.body.actor_seq, 2);
}

#[test]
fn event_type_mismatch() {
    let alice = node(90);
    let bob_signer = Ed25519Signer::from_bytes(&[91; 32]);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob_signer.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let genesis = oplog::topological(alice.storage(), &topic.id()).unwrap()[0].clone();
    let op = Op::sign(
        OpBody {
            topic_id: topic.id(),
            author: bob_signer.peer_id(),
            actor_id: actor_id_for(topic.id(), bob_signer.peer_id()),
            actor_seq: 1,
            actor_prev: None,
            deps: [genesis.id].into(),
            generation: 1,
            payload: TopicPayload::Event(EventEnvelope::encode_event(&Other).unwrap()),
        },
        &bob_signer,
    )
    .unwrap();
    let oplog = oplog::Oplog::with_storage(alice.storage().clone());

    assert!(matches!(
        oplog.receive_op(op),
        Err(Error::EventTypeMismatch { .. })
    ));
}

/// A signed generation cannot match a dependency with a different generation.
/// Once known, reject the op and descendants instead of revalidating it repeatedly.
/// Keep an independent waiter on that dependency.
fn assert_rejects_impossible<S: Storage>(storage: S) {
    let alice = Irokle::with_storage(
        storage,
        NodeConfig {
            signer: Ed25519Signer::from_bytes(&[140; 32]),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let bob = Ed25519Signer::from_bytes(&[141; 32]);
    let carol = Ed25519Signer::from_bytes(&[142; 32]);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id(), carol.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let topic_id = topic.id();
    let genesis = oplog::topological(alice.storage(), &topic_id).unwrap()[0].clone();
    let base = genesis.signed.body.generation;
    let bob_actor = actor_id_for(topic_id, bob.peer_id());
    let carol_actor = actor_id_for(topic_id, carol.peer_id());
    let note = |text: &str| {
        TopicPayload::Event(
            EventEnvelope::encode_event(&Note {
                text: text.to_owned(),
            })
            .unwrap(),
        )
    };

    // Withheld dependency; everything below waits on it.
    let d = Op::sign(
        OpBody {
            topic_id,
            author: bob.peer_id(),
            actor_id: bob_actor,
            actor_seq: 1,
            actor_prev: None,
            deps: [genesis.id].into(),
            generation: base + 1,
            payload: note("d"),
        },
        &bob,
    )
    .unwrap();
    // P claims a generation its only dependency can never justify.
    let p = Op::sign(
        OpBody {
            topic_id,
            author: bob.peer_id(),
            actor_id: bob_actor,
            actor_seq: 2,
            actor_prev: Some(d.id),
            deps: [d.id].into(),
            generation: base + 10,
            payload: note("p"),
        },
        &bob,
    )
    .unwrap();
    let c = Op::sign(
        OpBody {
            topic_id,
            author: bob.peer_id(),
            actor_id: bob_actor,
            actor_seq: 3,
            actor_prev: Some(p.id),
            deps: [p.id].into(),
            generation: base + 11,
            payload: note("c"),
        },
        &bob,
    )
    .unwrap();
    let gc = Op::sign(
        OpBody {
            topic_id,
            author: bob.peer_id(),
            actor_id: bob_actor,
            actor_seq: 4,
            actor_prev: Some(c.id),
            deps: [c.id].into(),
            generation: base + 12,
            payload: note("gc"),
        },
        &bob,
    )
    .unwrap();
    // Independent sibling waiting on the same dependency.
    let s = Op::sign(
        OpBody {
            topic_id,
            author: carol.peer_id(),
            actor_id: carol_actor,
            actor_seq: 1,
            actor_prev: None,
            deps: [d.id].into(),
            generation: base + 2,
            payload: note("s"),
        },
        &carol,
    )
    .unwrap();

    let log = oplog::Oplog::with_storage(alice.storage().clone());
    log.receive_ops_from_peer(Some(bob.peer_id()), vec![p.clone(), c.clone(), gc.clone()])
        .unwrap();
    log.receive_ops_from_peer(Some(carol.peer_id()), vec![s.clone()])
        .unwrap();
    assert!(
        alice
            .storage()
            .pending_missing_deps(&topic_id)
            .unwrap()
            .contains(&d.id),
        "all four must be buffered behind the withheld dependency"
    );

    log.receive_ops_from_peer(Some(bob.peer_id()), vec![d.clone()])
        .unwrap();

    assert!(
        alice.storage().ready_pending_ops().unwrap().is_empty(),
        "an impossible root must not stay eligible for revalidation"
    );
    assert!(alice.storage().pending_waiters(&p.id).unwrap().is_empty());
    assert!(alice.storage().pending_waiters(&c.id).unwrap().is_empty());
    assert!(
        alice
            .storage()
            .pending_missing_deps(&topic_id)
            .unwrap()
            .is_empty()
    );

    let admitted = alice.storage().list_op_ids(&topic_id).unwrap();
    assert!(admitted.contains(&d.id), "the dependency admits");
    assert!(admitted.contains(&s.id), "the independent sibling admits");
    for rejected in [&p, &c, &gc] {
        assert!(
            !admitted.contains(&rejected.id),
            "an impossible generation must not be admitted"
        );
    }

    // Resending the rejected root reports the permanent failure instead of
    // buffering it again, so it cannot return through the pending pool.
    assert!(matches!(
        log.receive_ops_from_peer(Some(bob.peer_id()), vec![p.clone()]),
        Err(Error::GenerationMismatch { .. })
    ));
    assert!(alice.storage().ready_pending_ops().unwrap().is_empty());
    assert!(
        !alice
            .storage()
            .list_op_ids(&topic_id)
            .unwrap()
            .contains(&p.id)
    );

    // A genuinely repairable record keeps its buffered copy: its dependency is
    // simply not here yet, which a later arrival can still resolve.
    let repairable = Op::sign(
        OpBody {
            topic_id,
            author: carol.peer_id(),
            actor_id: carol_actor,
            actor_seq: 2,
            actor_prev: Some(s.id),
            deps: [s.id, OpId::hash(b"not-yet-here")].into(),
            generation: base + 3,
            payload: note("repairable"),
        },
        &carol,
    )
    .unwrap();
    log.receive_ops_from_peer(Some(carol.peer_id()), vec![repairable.clone()])
        .unwrap();
    assert!(
        alice
            .storage()
            .pending_missing_deps(&topic_id)
            .unwrap()
            .contains(&OpId::hash(b"not-yet-here")),
        "a missing dependency must keep the buffered op"
    );
}

#[test]
fn memory_rejects_impossible() {
    assert_rejects_impossible(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_rejects_impossible() {
    let dir = tempfile::tempdir().unwrap();
    assert_rejects_impossible(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// Signer that counts the signatures it makes.
struct CountingSigner {
    inner: Ed25519Signer,
    signs: std::sync::atomic::AtomicUsize,
}

impl Signer for CountingSigner {
    fn peer_id(&self) -> PeerId {
        self.inner.peer_id()
    }

    fn sign(&self, message: &[u8]) -> Result<ed25519_dalek::Signature, Error> {
        self.signs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.sign(message)
    }
}

fn verifications() -> usize {
    crate::op::VERIFICATIONS.with(std::cell::Cell::get)
}

/// A retried admission checks each received signature once, and a retried
/// local write signs and checks its unchanged op once.
#[test]
fn retry_reuses_signatures() {
    let signer = CountingSigner {
        inner: Ed25519Signer::from_bytes(&[62; 32]),
        signs: Default::default(),
    };
    let (_source, topic_id, ops) = chain_source(61, signer.peer_id());
    let storage = StaleReadStorage::new(MemoryStorage::new());
    let log = oplog::Oplog::with_storage(storage.clone());

    storage.conflict_writes(1);
    let before = verifications();
    let accepted = log.receive_ops(ops.clone()).unwrap();
    assert_eq!(accepted.len(), ops.len());
    assert_eq!(
        storage.conflicts.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(verifications() - before, ops.len());

    storage.conflict_writes(1);
    let before = verifications();
    let op = log
        .create_event_op(
            topic_id,
            actor_id_for(topic_id, signer.peer_id()),
            EventEnvelope::encode_event(&Note {
                text: "retried".into(),
            })
            .unwrap(),
            &signer,
        )
        .unwrap();
    assert!(storage.get_op(&op.id).unwrap().is_some());
    assert_eq!(
        storage.conflicts.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(signer.signs.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(verifications() - before, 1);
}

/// A genesis created with its first event and retried after a lost commit
/// signs and checks each of the two ops once.
#[test]
fn genesis_retry_once() {
    let signer = CountingSigner {
        inner: Ed25519Signer::from_bytes(&[63; 32]),
        signs: Default::default(),
    };
    let storage = StaleReadStorage::new(MemoryStorage::new());
    let log = oplog::Oplog::with_storage(storage.clone());
    let topic_id = TopicId::hash(b"genesis-retry");
    storage.conflict_writes(2);
    let before = verifications();
    let (genesis, event) = log
        .create_topic_genesis_with_event(
            topic_id,
            actor_id_for(topic_id, signer.peer_id()),
            TopicGenesis::new(Note::TYPE_ID, [signer.peer_id()]),
            EventEnvelope::encode_event(&Note {
                text: "first".into(),
            })
            .unwrap(),
            &signer,
        )
        .unwrap();
    assert_eq!(
        storage.conflicts.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        storage.list_op_ids(&topic_id).unwrap(),
        [genesis.id, event.id].into()
    );
    assert_eq!(signer.signs.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(verifications() - before, 2);
}

/// A commit that lands while a received batch is checked makes it skip ops the
/// store now holds; the batch retries instead of reporting a false actor gap.
#[test]
fn concurrent_commit_retries() {
    let source = node(64);
    let topic = source.create_topic::<Note>(TopicConfig::default()).unwrap();
    for text in ["one", "two", "three", "four"] {
        topic.publish(Note { text: text.into() }).unwrap();
    }
    let ops = oplog::topological(source.storage(), &topic.id()).unwrap();
    let storage = StaleReadStorage::new(MemoryStorage::new());
    let receiver = oplog::Oplog::with_storage(storage.clone());
    receiver.receive_ops(ops[..2].to_vec()).unwrap();

    // The batch pauses at its first read of the fourth op, after the third entered
    // its overlay, while another receive commits the third and fourth.
    let writer = oplog::Oplog::with_storage(storage.clone());
    let (batch, committed) = (ops[2..].to_vec(), ops[2..4].to_vec());
    let accepted = interleave(
        &storage,
        (GatePoint::Meta(ops[3].id), 0),
        Isolation::Commits,
        move || receiver.receive_ops(batch),
        move || {
            writer.receive_ops(committed).unwrap();
        },
    );
    assert_eq!(accepted.unwrap(), [ops[4].id].into());
    assert_eq!(storage.heads(&topic.id()).unwrap(), [ops[4].id].into());
}
