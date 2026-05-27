use super::support::*;
use crate::storage as crate_storage;
use crate::sync as crate_sync;

#[test]
fn transfers_missing_ops() {
    let a = node(5);
    let b = node(6);
    let topic = a
        .create_topic::<Note>(TopicConfig {
            initial_peers: [b.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    topic
        .publish(Note {
            text: "sync me".into(),
        })
        .unwrap();

    let summary_b = b.sync_summary(topic.id()).unwrap();
    let data = a.plan_sync_data(b.peer_id(), &summary_b).unwrap();
    assert_eq!(data.ops.len(), 2);
    let ack = b.receive_sync_data_from(a.peer_id(), data).unwrap();
    assert!(
        b.storage()
            .peer_ack(&b.peer_id(), &topic.id())
            .unwrap()
            .is_none()
    );
    a.apply_sync_ack(&ack).unwrap();
    assert!(
        a.storage()
            .peer_ack(&b.peer_id(), &topic.id())
            .unwrap()
            .is_some()
    );
    let opened = b.open_topic::<Note>(topic.id()).unwrap();
    assert_eq!(
        opened
            .history(history::HistoryOrder::OldestFirst)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn summary_has_fingerprint() {
    let alice = node(35);
    let topic = alice.create_topic::<Note>(TopicConfig::default()).unwrap();
    let summary0 = alice.sync_summary(topic.id()).unwrap();
    assert_eq!(
        summary0.fingerprint,
        alice.storage().topic_fingerprint(&topic.id()).unwrap()
    );

    topic.publish(Note { text: "one".into() }).unwrap();
    let summary1 = alice.sync_summary(topic.id()).unwrap();
    assert_ne!(summary0.fingerprint, summary1.fingerprint);
    assert_eq!(
        summary1.fingerprint,
        alice.storage().topic_fingerprint(&topic.id()).unwrap()
    );
}

#[test]
fn skips_matching_fingerprint() {
    let alice = node(36);
    let bob = node(37);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    topic.publish(Note { text: "one".into() }).unwrap();

    let bob_summary = bob.sync_summary(topic.id()).unwrap();
    let data = alice.plan_sync_data(bob.peer_id(), &bob_summary).unwrap();
    bob.receive_sync_data_from(alice.peer_id(), data).unwrap();

    let plan = alice
        .negotiate_sync(bob.peer_id(), &bob.sync_summary(topic.id()).unwrap())
        .unwrap();
    assert!(plan.send.is_empty());
    assert!(plan.need.is_empty());
    assert!(plan.actor_range_hints.is_empty());
    assert_eq!(plan.have, alice.storage().heads(&topic.id()).unwrap());
}

#[test]
fn finds_common_ancestor() {
    let alice = node(29);
    let bob = node(30);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let genesis = oplog::topological(alice.storage(), &topic.id()).unwrap()[0].clone();
    bob.receive_sync_data_from(
        alice.peer_id(),
        crate_sync::SyncData {
            topic_id: topic.id(),
            ops: vec![genesis.clone()],
        },
    )
    .unwrap();

    topic
        .publish(Note {
            text: "alice branch".into(),
        })
        .unwrap();
    bob.open_topic::<Note>(topic.id())
        .unwrap()
        .publish(Note {
            text: "bob branch".into(),
        })
        .unwrap();

    let plan = alice
        .negotiate_sync(bob.peer_id(), &bob.sync_summary(topic.id()).unwrap())
        .unwrap();

    assert_eq!(plan.common, [genesis.id].into());
    assert_eq!(plan.send.len(), 1);
    assert_eq!(plan.send[0].signed.body.deps, [genesis.id].into());
    assert_eq!(plan.need.len(), 1);
    assert_eq!(plan.actor_range_hints.len(), 1);
}

#[test]
fn request_converges() {
    let alice = node(33);
    let bob = node(34);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let genesis = oplog::topological(alice.storage(), &topic.id()).unwrap()[0].clone();
    bob.receive_sync_data_from(
        alice.peer_id(),
        crate_sync::SyncData {
            topic_id: topic.id(),
            ops: vec![genesis.clone()],
        },
    )
    .unwrap();

    topic
        .publish(Note {
            text: "alice branch".into(),
        })
        .unwrap();
    bob.open_topic::<Note>(topic.id())
        .unwrap()
        .publish(Note {
            text: "bob branch".into(),
        })
        .unwrap();

    let bob_summary = bob.sync_summary(topic.id()).unwrap();
    let data_for_bob = alice.plan_sync_data(bob.peer_id(), &bob_summary).unwrap();
    let request_for_alice = alice
        .plan_sync_request(bob.peer_id(), &bob_summary)
        .unwrap();

    assert_eq!(data_for_bob.ops.len(), 1);
    assert_eq!(request_for_alice.wants.len(), 1);
    assert_eq!(request_for_alice.actor_range_hints.len(), 1);

    let bob_ack = bob
        .receive_sync_data_from(alice.peer_id(), data_for_bob)
        .unwrap();
    let data_for_alice = bob
        .plan_sync_response_data(alice.peer_id(), &request_for_alice)
        .unwrap();
    assert_eq!(data_for_alice.ops.len(), 1);
    assert!(data_for_alice.ops[0].signed.body.deps.contains(&genesis.id));

    let alice_ack = alice
        .receive_sync_data_from(bob.peer_id(), data_for_alice)
        .unwrap();
    alice.apply_sync_ack(&bob_ack).unwrap();
    bob.apply_sync_ack(&alice_ack).unwrap();

    let alice_ops: BTreeSet<_> = oplog::topological(alice.storage(), &topic.id())
        .unwrap()
        .into_iter()
        .map(|op| op.id)
        .collect();
    let bob_ops: BTreeSet<_> = oplog::topological(bob.storage(), &topic.id())
        .unwrap()
        .into_iter()
        .map(|op| op.id)
        .collect();
    assert_eq!(alice_ops, bob_ops);
    assert_eq!(alice_ops.len(), 3);
}

#[test]
fn closure_is_ordered() {
    let alice = node(31);
    let bob = node(32);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let genesis = oplog::topological(alice.storage(), &topic.id()).unwrap()[0].clone();
    bob.receive_sync_data_from(
        alice.peer_id(),
        crate_sync::SyncData {
            topic_id: topic.id(),
            ops: vec![genesis.clone()],
        },
    )
    .unwrap();

    topic.publish(Note { text: "one".into() }).unwrap();
    topic.publish(Note { text: "two".into() }).unwrap();

    let missing = alice
        .negotiate_sync(bob.peer_id(), &bob.sync_summary(topic.id()).unwrap())
        .unwrap()
        .send;

    assert_eq!(missing.len(), 2);
    assert!(missing[1].signed.body.deps.contains(&missing[0].id));
    assert!(!missing.iter().any(|op| op.id == genesis.id));
}

#[test]
fn batches_preserve_order() {
    let alice = node(104);
    let bob = node(105);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    for index in 0..=net::MAX_SYNC_DATA_OPS_PER_MESSAGE {
        topic
            .publish(Note {
                text: format!("event-{index}"),
            })
            .unwrap();
    }
    let data = alice
        .plan_sync_data(bob.peer_id(), &bob.sync_summary(topic.id()).unwrap())
        .unwrap();
    assert!(data.ops.len() > net::MAX_SYNC_DATA_OPS_PER_MESSAGE);

    let expected_ids = data.ops.iter().map(|op| op.id).collect::<Vec<_>>();
    let batches = net::sync_data_messages(data.topic_id, data.ops);
    assert!(batches.len() > 1);
    let mut actual_ids = Vec::new();
    for batch in batches {
        let crate_sync::SyncMessage::Data(data) = batch else {
            panic!("expected data batch");
        };
        assert!(data.ops.len() <= net::MAX_SYNC_DATA_OPS_PER_MESSAGE);
        actual_ids.extend(data.ops.into_iter().map(|op| op.id));
    }
    assert_eq!(actual_ids, expected_ids);
}

#[test]
fn response_includes_closure() {
    let alice = node(38);
    let bob = node(39);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    topic.publish(Note { text: "one".into() }).unwrap();
    topic.publish(Note { text: "two".into() }).unwrap();
    let ops = oplog::topological(alice.storage(), &topic.id()).unwrap();
    let genesis = ops[0].clone();
    let first = ops[1].clone();
    let second = ops[2].clone();
    let actor_id = first.signed.body.actor_id;

    let response = alice
        .plan_sync_response_data(
            bob.peer_id(),
            &crate_sync::SyncRequest {
                topic_id: topic.id(),
                known: [genesis.id].into(),
                wants: BTreeSet::new(),
                actor_range_hints: vec![crate_sync::ActorRangeHint {
                    actor_id,
                    from_exclusive: 1,
                    to_inclusive: 3,
                }],
            },
        )
        .unwrap();
    let ids = response.ops.iter().map(|op| op.id).collect::<Vec<_>>();
    assert_eq!(ids, vec![first.id, second.id]);
    assert!(response.ops[1].signed.body.deps.contains(&first.id));
}

#[test]
fn accepts_out_of_order_batch() {
    let alice = node(40);
    let bob = node(41);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    topic.publish(Note { text: "one".into() }).unwrap();
    topic.publish(Note { text: "two".into() }).unwrap();
    let ops = oplog::topological(alice.storage(), &topic.id()).unwrap();
    let genesis = ops[0].clone();
    let first = ops[1].clone();
    let second = ops[2].clone();

    let ack = bob
        .receive_sync_data_from(
            alice.peer_id(),
            crate_sync::SyncData {
                topic_id: topic.id(),
                ops: vec![second.clone(), genesis.clone(), first.clone()],
            },
        )
        .unwrap();

    assert_eq!(ack.accepted, [genesis.id, first.id, second.id].into());
    assert_eq!(
        bob.storage().heads(&topic.id()).unwrap(),
        [second.id].into()
    );
    assert_eq!(
        bob.open_topic::<Note>(topic.id())
            .unwrap()
            .history(history::HistoryOrder::OldestFirst)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn defers_until_dependency() {
    let alice = node(42);
    let bob = node(43);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    topic.publish(Note { text: "one".into() }).unwrap();
    topic.publish(Note { text: "two".into() }).unwrap();
    let ops = oplog::topological(alice.storage(), &topic.id()).unwrap();
    let genesis = ops[0].clone();
    let first = ops[1].clone();
    let second = ops[2].clone();

    bob.receive_sync_data_from(
        alice.peer_id(),
        crate_sync::SyncData {
            topic_id: topic.id(),
            ops: vec![genesis.clone()],
        },
    )
    .unwrap();

    let first_ack = bob
        .receive_sync_data_from(
            alice.peer_id(),
            crate_sync::SyncData {
                topic_id: topic.id(),
                ops: vec![second.clone()],
            },
        )
        .unwrap();
    assert!(first_ack.accepted.is_empty());
    assert!(bob.storage().get_op(&second.id).unwrap().is_none());

    let second_ack = bob
        .receive_sync_data_from(
            alice.peer_id(),
            crate_sync::SyncData {
                topic_id: topic.id(),
                ops: vec![genesis.clone(), first.clone()],
            },
        )
        .unwrap();
    assert_eq!(second_ack.accepted, [first.id, second.id].into());
    assert!(bob.storage().get_op(&second.id).unwrap().is_some());
}

#[test]
fn caps_fanout() {
    let local = PeerId::hash(b"local");
    let topic_id = TopicId::hash(b"fanout-topic");
    let mut members = [local].into_iter().collect::<BTreeSet<_>>();
    for idx in 0..24_u8 {
        members.insert(PeerId::hash([idx]));
    }
    let state = crate_storage::TopicState {
        topic_id,
        event_type_id: Note::TYPE_ID.into(),
        genesis: OpId::hash(b"genesis"),
        heads: BTreeSet::new(),
        members,
        replication_policy: ReplicationPolicy::all().with_max_sync_peers(3),
        membership_controls: std::collections::BTreeMap::new(),
        replication_policy_control: None,
    };

    let peers = node::select_sync_peers(topic_id, local, &state);

    assert_eq!(peers.len(), 3);
    assert!(!peers.contains(&local));
}

#[test]
fn deterministic_overlap() {
    let topic_id = TopicId::hash(b"overlap-topic");
    let mut peers = Vec::new();
    let mut members = BTreeSet::new();
    for idx in 0..64_u8 {
        let peer = PeerId::hash([idx]);
        peers.push(peer);
        members.insert(peer);
    }
    let state = crate_storage::TopicState {
        topic_id,
        event_type_id: Note::TYPE_ID.into(),
        genesis: OpId::hash(b"genesis"),
        heads: BTreeSet::new(),
        members,
        replication_policy: ReplicationPolicy::all().with_max_sync_peers(6),
        membership_controls: std::collections::BTreeMap::new(),
        replication_policy_control: None,
    };

    let local = node::select_sync_peers(topic_id, peers[1], &state);
    let distant = node::select_sync_peers(topic_id, peers[61], &state);
    let local_set = local.iter().copied().collect::<BTreeSet<_>>();
    let distant_set = distant.iter().copied().collect::<BTreeSet<_>>();

    assert_eq!(local, node::select_sync_peers(topic_id, peers[1], &state));
    assert_ne!(local_set, distant_set);
    assert!(local_set.intersection(&distant_set).count() >= 2);
}

#[test]
fn receive_forwarding_obligates_other_selected_peers() {
    let alice = node(90);
    let bob = node(91);
    let charlie = node(92);
    let dana = node(93);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id(), charlie.peer_id(), dana.peer_id()].into(),
            replication_policy: ReplicationPolicy::selected([charlie.peer_id(), dana.peer_id()])
                .with_max_sync_peers(1),
        })
        .unwrap();
    let genesis = oplog::topological(alice.storage(), &topic.id()).unwrap()[0].clone();
    bob.receive_sync_data_from(
        alice.peer_id(),
        crate_sync::SyncData {
            topic_id: topic.id(),
            ops: vec![genesis],
        },
    )
    .unwrap();

    let record = topic
        .publish(Note {
            text: "forward me".into(),
        })
        .unwrap();
    let op = alice.storage().get_op(&record.meta.op_id).unwrap().unwrap();
    let ack = bob
        .receive_sync_data_from(
            alice.peer_id(),
            crate_sync::SyncData {
                topic_id: topic.id(),
                ops: vec![op],
            },
        )
        .unwrap();
    assert_eq!(ack.accepted, [record.meta.op_id].into());

    let state = bob.storage().topic_state(&topic.id()).unwrap().unwrap();
    let expected_targets = node::select_sync_peers(topic.id(), bob.peer_id(), &state)
        .into_iter()
        .filter(|peer| *peer != alice.peer_id())
        .collect::<BTreeSet<_>>();
    assert!(!expected_targets.is_empty());

    let actual_targets = bob
        .storage()
        .all_sync_obligations()
        .unwrap()
        .into_iter()
        .filter(|obligation| {
            obligation.topic_id == topic.id() && obligation.op_ids.contains(&record.meta.op_id)
        })
        .map(|obligation| obligation.peer_id)
        .collect::<BTreeSet<_>>();

    assert_eq!(actual_targets, expected_targets);
    assert!(!actual_targets.contains(&alice.peer_id()));
}

#[test]
fn reports_status() {
    let alice = node(87);
    let bob = node(88);
    let topic = alice.create_topic::<Note>(TopicConfig::default()).unwrap();
    let record = topic
        .publish(Note {
            text: "status".into(),
        })
        .unwrap();
    alice
        .put_sync_obligation(bob.peer_id(), topic.id(), [record.meta.op_id].into())
        .unwrap();
    let failure = std::io::Error::other("dial failed");

    alice
        .record_sync_result(bob.peer_id(), topic.id(), Err(&failure))
        .unwrap();

    let status = alice.sync_status(topic.id()).unwrap();
    assert_eq!(status.len(), 1);
    assert_eq!(status[0].state, crate_storage::SyncPeerState::Failed);
    assert_eq!(status[0].pending_obligations, 1);
    assert_eq!(status[0].failed_attempts, 1);
    assert!(
        status[0]
            .last_error
            .as_deref()
            .unwrap()
            .contains("dial failed")
    );
    assert_eq!(
        alice
            .sync_state_counts(topic.id())
            .unwrap()
            .get(&crate_storage::SyncPeerState::Failed),
        Some(&1)
    );
}

#[test]
fn omits_non_member_ops() {
    let a = node(8);
    let topic = a.create_topic::<Note>(TopicConfig::default()).unwrap();
    topic
        .publish(Note {
            text: "secret".into(),
        })
        .unwrap();

    let outsider = node(9);
    let summary = outsider.sync_summary(topic.id()).unwrap();
    let data = a.plan_sync_data(outsider.peer_id(), &summary).unwrap();
    assert!(data.ops.is_empty());
}

#[test]
fn report_filters_obligations() {
    let irokle = node(12);
    let peer_a = PeerId::hash(b"peer-a");
    let peer_b = PeerId::hash(b"peer-b");
    let topic_a = TopicId::hash(b"topic-a");
    let topic_b = TopicId::hash(b"topic-b");
    let op_a = OpId::hash(b"op-a");
    let op_b = OpId::hash(b"op-b");
    let op_c = OpId::hash(b"op-c");
    irokle
        .put_sync_obligation(peer_a, topic_a, [op_a].into())
        .unwrap();
    irokle
        .put_sync_obligation(peer_a, topic_b, [op_b].into())
        .unwrap();
    irokle
        .put_sync_obligation(peer_b, topic_a, [op_c].into())
        .unwrap();

    let report = irokle.sync_report(peer_a, topic_a).unwrap();
    assert_eq!(report.obligations.len(), 1);
    assert_eq!(report.obligations[0].peer_id, peer_a);
    assert_eq!(report.obligations[0].topic_id, topic_a);
    assert!(report.obligations[0].op_ids.contains(&op_a));
}

#[test]
fn rejects_other_topic_ops() {
    let alice = node(19);
    let bob = node(20);
    let topic_a = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let topic_b = alice.create_topic::<Note>(TopicConfig::default()).unwrap();
    let op_a = oplog::topological(alice.storage(), &topic_a.id()).unwrap()[0].clone();
    let op_b = oplog::topological(alice.storage(), &topic_b.id()).unwrap()[0].clone();
    bob.receive_sync_data_from(
        alice.peer_id(),
        crate_sync::SyncData {
            topic_id: topic_a.id(),
            ops: vec![op_a],
        },
    )
    .unwrap();
    assert!(matches!(
        bob.receive_sync_data_from(
            alice.peer_id(),
            crate_sync::SyncData {
                topic_id: topic_a.id(),
                ops: vec![op_b],
            },
        ),
        Err(Error::TopicMismatch)
    ));
}

#[test]
fn exposes_sync_metadata() {
    let alice = node(92);
    let bob = node(93);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let record = topic
        .publish(Note {
            text: "seen".into(),
        })
        .unwrap();

    assert_eq!(
        alice.sync_open(topic.id()).event_type_id.as_deref(),
        Some(Note::TYPE_ID)
    );
    assert_eq!(
        alice
            .sync_summary(topic.id())
            .unwrap()
            .event_type_id
            .as_deref(),
        Some(Note::TYPE_ID)
    );
    assert!(
        !alice
            .peer_reached_op(bob.peer_id(), record.meta.op_id)
            .unwrap()
    );

    let data = crate_sync::SyncData {
        topic_id: topic.id(),
        ops: oplog::topological(alice.storage(), &topic.id()).unwrap(),
    };
    let ack = bob.receive_sync_data_from(alice.peer_id(), data).unwrap();
    alice.apply_sync_ack(&ack).unwrap();

    assert!(
        alice
            .peer_reached_op(bob.peer_id(), record.meta.op_id)
            .unwrap()
    );
    assert_eq!(
        alice.peers_reached_op(record.meta.op_id).unwrap(),
        vec![bob.peer_id()]
    );
}

#[test]
fn receive_schedules_forwarding_only_for_missing_selected_peers() {
    let alice = node(94);
    let bob = node(95);
    let charlie = node(96);
    let dana = node(97);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id(), charlie.peer_id(), dana.peer_id()].into(),
            replication_policy: ReplicationPolicy::all().with_max_sync_peers(8),
        })
        .unwrap();
    let record = topic.publish(Note { text: "fan".into() }).unwrap();
    let data = crate_sync::SyncData {
        topic_id: topic.id(),
        ops: oplog::topological(alice.storage(), &topic.id()).unwrap(),
    };
    let meta = alice
        .storage()
        .get_meta(&record.meta.op_id)
        .unwrap()
        .unwrap();
    let mut clock = ActorClock::new();
    clock.observe(meta.actor_id, meta.actor_seq);
    bob.storage()
        .apply_peer_ack(crate_storage::PeerAck {
            peer_id: charlie.peer_id(),
            topic_id: topic.id(),
            heads: [record.meta.op_id].into(),
            clock,
        })
        .unwrap();

    let ack = bob.receive_sync_data_from(alice.peer_id(), data).unwrap();

    assert!(
        bob.storage()
            .sync_obligations(&alice.peer_id(), &topic.id())
            .unwrap()
            .is_empty()
    );
    assert!(
        bob.storage()
            .sync_obligations(&bob.peer_id(), &topic.id())
            .unwrap()
            .is_empty()
    );
    assert!(
        bob.storage()
            .sync_obligations(&charlie.peer_id(), &topic.id())
            .unwrap()
            .is_empty()
    );
    let dana_obligations = bob
        .storage()
        .sync_obligations(&dana.peer_id(), &topic.id())
        .unwrap();
    assert_eq!(dana_obligations.len(), 1);
    assert_eq!(dana_obligations[0].op_ids, ack.accepted);
}

#[test]
fn failed_sync_result_keeps_obligation_pending_for_retry() {
    let alice = node(98);
    let bob = node(99);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let record = topic
        .publish(Note {
            text: "retry".into(),
        })
        .unwrap();
    alice
        .put_sync_obligation(bob.peer_id(), topic.id(), [record.meta.op_id].into())
        .unwrap();

    let error = std::io::Error::new(std::io::ErrorKind::TimedOut, "dial timed out");
    alice
        .record_sync_result(bob.peer_id(), topic.id(), Err(&error))
        .unwrap();

    let status = alice.sync_status(topic.id()).unwrap();
    assert_eq!(status.len(), 1);
    assert_eq!(status[0].peer_id, bob.peer_id());
    assert_eq!(status[0].state, crate_storage::SyncPeerState::Failed);
    assert_eq!(status[0].pending_obligations, 1);
    assert_eq!(status[0].failed_attempts, 1);
    assert!(
        status[0]
            .last_error
            .as_deref()
            .unwrap()
            .contains("dial timed out")
    );
    assert_eq!(
        alice
            .storage()
            .sync_obligations(&bob.peer_id(), &topic.id())
            .unwrap()[0]
            .op_ids,
        [record.meta.op_id].into()
    );

    alice
        .record_sync_result(bob.peer_id(), topic.id(), Ok(()))
        .unwrap();

    let status = alice.sync_status(topic.id()).unwrap();
    assert_eq!(status[0].state, crate_storage::SyncPeerState::Behind);
    assert_eq!(status[0].pending_obligations, 1);
    assert_eq!(status[0].successful_attempts, 1);
    assert_eq!(status[0].last_error, None);
}

#[test]
fn clamps_oversized_hint() {
    let alice = node(80);
    let bob = node(81);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    topic.publish(Note { text: "one".into() }).unwrap();
    topic.publish(Note { text: "two".into() }).unwrap();
    let ops = oplog::topological(alice.storage(), &topic.id()).unwrap();
    let actor_id = ops[1].signed.body.actor_id;

    // A peer-supplied hint covering the entire u64 range must not blow up
    // or iterate u64::MAX times; clamping is bounded by what we locally
    // have and by MAX_ACTOR_RANGE_HINT_SPAN.
    let response = alice
        .plan_sync_response_data(
            bob.peer_id(),
            &crate_sync::SyncRequest {
                topic_id: topic.id(),
                known: BTreeSet::new(),
                wants: BTreeSet::new(),
                actor_range_hints: vec![crate_sync::ActorRangeHint {
                    actor_id,
                    from_exclusive: 0,
                    to_inclusive: u64::MAX,
                }],
            },
        )
        .unwrap();
    // Alice only has 3 ops (genesis + two events) for this actor, so the
    // clamped hint resolves to those (closure includes genesis as well).
    assert!(response.ops.len() <= 3);
    assert!(!response.ops.is_empty());
}

#[test]
fn ignores_reversed_hint() {
    let alice = node(82);
    let bob = node(83);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    topic.publish(Note { text: "one".into() }).unwrap();
    let ops = oplog::topological(alice.storage(), &topic.id()).unwrap();
    let actor_id = ops[1].signed.body.actor_id;

    let response = alice
        .plan_sync_response_data(
            bob.peer_id(),
            &crate_sync::SyncRequest {
                topic_id: topic.id(),
                known: BTreeSet::new(),
                wants: BTreeSet::new(),
                actor_range_hints: vec![crate_sync::ActorRangeHint {
                    actor_id,
                    from_exclusive: u64::MAX,
                    to_inclusive: u64::MAX,
                }],
            },
        )
        .unwrap();
    assert!(response.ops.is_empty());
}

#[test]
fn unknown_topic_empty_plan() {
    let alice = node(84);
    let unknown_topic = TopicId::hash(b"never-heard-of-this");
    // A fabricated remote summary pointing at OpIds Alice doesn't have.
    // The old code would surface remote.heads as `need`/`want`, letting a
    // peer inject arbitrary OpIds into Alice's request set for a topic
    // she cannot authenticate. The plan must now be empty.
    let summary = crate_sync::SyncSummary {
        topic_id: unknown_topic,
        event_type_id: None,
        fingerprint: [0; 32],
        heads: [OpId::hash(b"forged-head-1"), OpId::hash(b"forged-head-2")].into(),
        actor_clock: ActorClock::new(),
        actor_tips: std::collections::BTreeMap::new(),
    };
    let plan = alice
        .negotiate_sync(PeerId::hash(b"some-remote"), &summary)
        .unwrap();
    assert!(plan.need.is_empty());
    assert!(plan.send.is_empty());
    assert!(plan.actor_range_hints.is_empty());
}
