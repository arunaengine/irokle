use super::support::*;

#[test]
fn op_id_rejects_tamper() {
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
fn actor_chain_links_ops() {
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
fn cloned_node_serializes_actor() {
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
fn builder_uses_signer_storage() {
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
fn topic_type_mismatch_on_open() {
    let irokle = node(3);
    let topic = irokle.create_topic::<Note>(TopicConfig::default()).unwrap();
    let err = match irokle.open_topic::<Other>(topic.id()) {
        Ok(_) => panic!("opening with wrong event type unexpectedly succeeded"),
        Err(err) => err,
    };
    assert!(matches!(err, Error::EventTypeMismatch { .. }));
}

#[test]
fn dag_query_honors_limit() {
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
fn event_envelope_checks_type() {
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
fn event_rejects_type_mismatch() {
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
