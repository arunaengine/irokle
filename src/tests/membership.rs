use super::support::*;

#[test]
fn introduced_peer_can_reject() {
    let alice = node(89);
    let bob = node(90);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    topic
        .publish(Note {
            text: "invite".into(),
        })
        .unwrap();

    let bob_summary = bob.sync_summary(topic.id()).unwrap();
    let data = alice.plan_sync_data(bob.peer_id(), &bob_summary).unwrap();
    bob.receive_sync_data_from(alice.peer_id(), data).unwrap();
    assert_eq!(bob.list_topics().unwrap().len(), 1);

    bob.reject_topic(topic.id()).unwrap();
    assert!(matches!(
        bob.open_topic::<Note>(topic.id()),
        Err(Error::NotTopicMember)
    ));

    let alice_summary = alice.sync_summary(topic.id()).unwrap();
    let rejection = bob.plan_sync_data(alice.peer_id(), &alice_summary).unwrap();
    alice
        .receive_sync_data_from(bob.peer_id(), rejection)
        .unwrap();
    assert!(
        !alice
            .storage()
            .topic_state(&topic.id())
            .unwrap()
            .unwrap()
            .members
            .contains(&bob.peer_id())
    );
}

#[test]
fn late_peer_accepts_batch() {
    let alice = node(107);
    let bob = node(108);
    let charlie = node(109);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    topic
        .publish(Note {
            text: "before join".into(),
        })
        .unwrap();
    topic.add_peer(charlie.peer_id()).unwrap();
    topic
        .publish(Note {
            text: "after join".into(),
        })
        .unwrap();

    let charlie_summary = charlie.sync_summary(topic.id()).unwrap();
    let data = alice
        .plan_sync_data(charlie.peer_id(), &charlie_summary)
        .unwrap();
    let ack = charlie
        .receive_sync_data_from(alice.peer_id(), data)
        .unwrap();
    alice.apply_sync_ack(&ack).unwrap();

    let charlie_topic = charlie.open_topic::<Note>(topic.id()).unwrap();
    assert_eq!(
        charlie_topic
            .history(history::HistoryOrder::OldestFirst)
            .unwrap()
            .len(),
        2
    );
    assert!(
        charlie
            .storage()
            .topic_state(&topic.id())
            .unwrap()
            .unwrap()
            .members
            .contains(&charlie.peer_id())
    );
}

#[test]
fn controls_converge_any_order() {
    let alice = node(21);
    let bob = node(22);
    let target = PeerId::hash(b"target-peer");
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let genesis = oplog::topological(alice.storage(), &topic.id()).unwrap()[0].clone();

    bob.receive_sync_data_from(
        alice.peer_id(),
        sync::SyncData {
            topic_id: topic.id(),
            ops: vec![genesis.clone()],
        },
    )
    .unwrap();
    let bob_topic = bob.open_topic::<Note>(topic.id()).unwrap();

    topic.add_peer(target).unwrap();
    bob_topic.remove_peer(target).unwrap();

    let add = oplog::topological(alice.storage(), &topic.id())
        .unwrap()
        .into_iter()
        .find(|op| matches!(&op.signed.body.payload, TopicPayload::Control(TopicControl::AddPeer { peer }) if *peer == target))
        .unwrap();
    let remove = oplog::topological(bob.storage(), &topic.id())
        .unwrap()
        .into_iter()
        .find(|op| matches!(&op.signed.body.payload, TopicPayload::Control(TopicControl::RemovePeer { peer }) if *peer == target))
        .unwrap();

    let replica_a = node(23);
    let replica_b = node(24);
    for replica in [&replica_a, &replica_b] {
        replica
            .receive_sync_data_as_local(sync::SyncData {
                topic_id: topic.id(),
                ops: vec![genesis.clone()],
            })
            .unwrap();
    }
    replica_a
        .receive_sync_data_as_local(sync::SyncData {
            topic_id: topic.id(),
            ops: vec![add.clone(), remove.clone()],
        })
        .unwrap();
    replica_b
        .receive_sync_data_as_local(sync::SyncData {
            topic_id: topic.id(),
            ops: vec![remove, add],
        })
        .unwrap();

    let members_a = replica_a
        .storage()
        .topic_state(&topic.id())
        .unwrap()
        .unwrap()
        .members;
    let members_b = replica_b
        .storage()
        .topic_state(&topic.id())
        .unwrap()
        .unwrap()
        .members;
    assert_eq!(members_a, members_b);
}

#[test]
fn rejects_unknown_nonmember() {
    let a = node(10);
    let topic = a.create_topic::<Note>(TopicConfig::default()).unwrap();
    let outsider = node(11);
    let raw_data = sync::SyncData {
        topic_id: topic.id(),
        ops: oplog::topological(a.storage(), &topic.id()).unwrap(),
    };

    let err = outsider
        .receive_sync_data_from(a.peer_id(), raw_data)
        .unwrap_err();
    assert!(matches!(err, Error::NotTopicMember));
    assert!(
        outsider
            .storage()
            .topic_state(&topic.id())
            .unwrap()
            .is_none()
    );
}

#[test]
fn cannot_backdate_join() {
    let alice = node(13);
    let bob_signer = Ed25519Signer::from_bytes(&[14; 32]);
    let topic = alice.create_topic::<Note>(TopicConfig::default()).unwrap();
    let genesis = oplog::topological(alice.storage(), &topic.id()).unwrap()[0].clone();
    topic.add_peer(bob_signer.peer_id()).unwrap();

    let op = Op::sign(
        OpBody {
            topic_id: topic.id(),
            author: bob_signer.peer_id(),
            actor_id: actor_id_for(topic.id(), bob_signer.peer_id()),
            actor_seq: 1,
            actor_prev: None,
            deps: [genesis.id].into(),
            generation: genesis.signed.body.generation + 1,
            payload: TopicPayload::Event(
                EventEnvelope::encode_event(&Note {
                    text: "backdated".into(),
                })
                .unwrap(),
            ),
        },
        &bob_signer,
    )
    .unwrap();
    let oplog = oplog::Oplog::with_storage(alice.storage().clone());

    assert!(matches!(oplog.receive_op(op), Err(Error::NotTopicMember)));
}

#[test]
fn accepts_event_before_remove() {
    let alice = node(15);
    let bob = node(16);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let genesis = oplog::topological(alice.storage(), &topic.id()).unwrap()[0].clone();
    bob.receive_sync_data_from(
        alice.peer_id(),
        sync::SyncData {
            topic_id: topic.id(),
            ops: vec![genesis.clone()],
        },
    )
    .unwrap();
    let bob_topic = bob.open_topic::<Note>(topic.id()).unwrap();
    bob_topic
        .publish(Note {
            text: "before remove".into(),
        })
        .unwrap();
    let bob_event = oplog::topological(bob.storage(), &topic.id()).unwrap()[1].clone();

    topic.remove_peer(bob.peer_id()).unwrap();
    let alice_oplog = oplog::Oplog::with_storage(alice.storage().clone());

    assert!(alice_oplog.receive_op(bob_event).is_ok());
}

#[test]
fn rejects_wrong_actor_id() {
    let alice = node(17);
    let bob_signer = Ed25519Signer::from_bytes(&[18; 32]);
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
            actor_id: actor_id_for(topic.id(), alice.peer_id()),
            actor_seq: 2,
            actor_prev: Some(genesis.id),
            deps: [genesis.id].into(),
            generation: genesis.signed.body.generation + 1,
            payload: TopicPayload::Event(
                EventEnvelope::encode_event(&Note {
                    text: "impersonate".into(),
                })
                .unwrap(),
            ),
        },
        &bob_signer,
    )
    .unwrap();
    let oplog = oplog::Oplog::with_storage(alice.storage().clone());

    assert!(matches!(
        oplog.receive_op(op),
        Err(Error::ActorAuthorMismatch)
    ));
}

#[test]
fn rejects_pending_nonmember() {
    let alice = node(85);
    let outsider = Ed25519Signer::from_bytes(&[86; 32]);
    // Alice creates a topic where the outsider is *not* a member.
    let topic = alice.create_topic::<Note>(TopicConfig::default()).unwrap();
    // Outsider crafts a structurally-valid signed op whose dep doesn't
    // exist on Alice. Without the membership check this op would consume
    // a per-source pending-quota slot until eviction; with the check it
    // is rejected immediately.
    let fake_dep = OpId::hash(b"missing-dep");
    let op = Op::sign(
        OpBody {
            topic_id: topic.id(),
            author: outsider.peer_id(),
            actor_id: actor_id_for(topic.id(), outsider.peer_id()),
            actor_seq: 1,
            actor_prev: None,
            deps: [fake_dep].into(),
            generation: 1,
            payload: TopicPayload::Event(
                EventEnvelope::encode_event(&Note {
                    text: "outsider".into(),
                })
                .unwrap(),
            ),
        },
        &outsider,
    )
    .unwrap();
    let oplog = oplog::Oplog::with_storage(alice.storage().clone());
    assert!(matches!(oplog.receive_op(op), Err(Error::NotTopicMember)));
}
