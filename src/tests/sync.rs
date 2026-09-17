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
    let ack = b.receive_sync_data_from(a.peer_id(), data).unwrap().0;
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
fn topic_event_replication() {
    let a = node(53);
    let b = node(54);
    let (topic, record) = a
        .create_topic_with_event(
            TopicConfig {
                initial_peers: [b.peer_id()].into(),
                ..TopicConfig::default()
            },
            Note {
                text: "sync me".into(),
            },
        )
        .unwrap();

    let summary_b = b.sync_summary(topic.id()).unwrap();
    let data = a.plan_sync_data(b.peer_id(), &summary_b).unwrap();
    assert_eq!(data.ops.len(), 2);
    let ack = b.receive_sync_data_from(a.peer_id(), data).unwrap().0;
    a.apply_sync_ack(&ack).unwrap();

    let opened = b.open_topic::<Note>(topic.id()).unwrap();
    let replicated = opened.history(history::HistoryOrder::OldestFirst).unwrap();
    assert_eq!(replicated.len(), 1);
    assert_eq!(replicated[0].meta.op_id, record.meta.op_id);
    assert_eq!(
        replicated[0].event,
        Note {
            text: "sync me".into()
        }
    );
    assert_eq!(
        b.storage().heads(&topic.id()).unwrap(),
        a.storage().heads(&topic.id()).unwrap()
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
    // Bob's tip is ahead of alice's clock, so it is asked for by range.
    assert!(request_for_alice.wants.is_empty());
    assert_eq!(request_for_alice.actor_range_hints.len(), 1);

    let bob_ack = bob
        .receive_sync_data_from(alice.peer_id(), data_for_bob)
        .unwrap()
        .0;
    let data_for_alice = bob
        .plan_sync_response_data(alice.peer_id(), &request_for_alice)
        .unwrap();
    assert_eq!(data_for_alice.ops.len(), 1);
    assert!(data_for_alice.ops[0].signed.body.deps.contains(&genesis.id));

    let alice_ack = alice
        .receive_sync_data_from(bob.peer_id(), data_for_alice)
        .unwrap()
        .0;
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
    let batches = net::sync_data_messages(data.topic_id, data.ops).unwrap();
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
                genesis: None,
                credit: Default::default(),
                window: crate::sync::ActorWindow::default(),
            },
        )
        .unwrap();
    let ids = response.ops.iter().map(|op| op.id).collect::<Vec<_>>();
    assert_eq!(ids, vec![first.id, second.id]);
    assert!(response.ops[1].signed.body.deps.contains(&first.id));
}

#[test]
fn unordered_batch_admitted() {
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
        .unwrap()
        .0;

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
        .unwrap()
        .0;
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
        .unwrap()
        .0;
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
fn receive_forwarding_obligation() {
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
        .unwrap()
        .0;
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
            obligation.topic_id == topic.id()
                && obligation_covers(
                    alice.storage(),
                    std::slice::from_ref(obligation),
                    &record.meta.op_id,
                )
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
fn scopes_accepted_ops() {
    // Admission flushes ready ops of every topic; a buffered op of another
    // topic must stay out of this ack, or its reader files an obligation
    // under the wrong topic that no ack can ever satisfy.
    let alice = node(80);
    let bob = node(81);
    let first = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let second = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    bob.receive_sync_data_from(
        alice.peer_id(),
        crate_sync::SyncData {
            topic_id: second.id(),
            ops: oplog::topological(alice.storage(), &second.id()).unwrap(),
        },
    )
    .unwrap();

    let buffered = second
        .publish(Note {
            text: "other topic".into(),
        })
        .unwrap();
    let buffered_op = alice
        .storage()
        .get_op(&buffered.meta.op_id)
        .unwrap()
        .unwrap();
    let buffered_meta = alice
        .storage()
        .get_meta(&buffered.meta.op_id)
        .unwrap()
        .unwrap();
    bob.storage()
        .put_pending_op(alice.peer_id(), buffered_op, buffered_meta)
        .unwrap();

    let record = first
        .publish(Note {
            text: "mine".into(),
        })
        .unwrap();
    let ack = bob
        .receive_sync_data_from(
            alice.peer_id(),
            crate_sync::SyncData {
                topic_id: first.id(),
                ops: oplog::topological(alice.storage(), &first.id()).unwrap(),
            },
        )
        .unwrap()
        .0;

    assert!(ack.accepted.contains(&record.meta.op_id));
    assert!(!ack.accepted.contains(&buffered.meta.op_id));
    assert!(
        bob.storage()
            .get_op(&buffered.meta.op_id)
            .unwrap()
            .is_some()
    );
    assert!(
        bob.storage()
            .all_sync_obligations()
            .unwrap()
            .iter()
            .all(|obligation| obligation.topic_id != first.id()
                || !obligation_covers(
                    bob.storage(),
                    std::slice::from_ref(obligation),
                    &buffered.meta.op_id
                ))
    );
}

#[test]
fn omits_nonmember_ops() {
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
    assert_eq!(
        report.obligations[0].target,
        crate_storage::ObligationTarget::Repair([op_a].into())
    );
}

#[test]
fn rejects_foreign_ops() {
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
    let ack = bob.receive_sync_data_from(alice.peer_id(), data).unwrap().0;
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
fn missing_peer_forwarding() {
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
            // The branch bob is about to adopt: evidence names the incarnation
            // it certifies, so it starts counting once that branch is local.
            genesis: genesis_of(alice.storage(), &topic.id()),
            heads: [record.meta.op_id].into(),
            clock: clock.clone(),
        })
        .unwrap();

    let ack = bob.receive_sync_data_from(alice.peer_id(), data).unwrap().0;

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
    // Forwarded work coalesces into one clock target covering every accepted op.
    assert_eq!(dana_obligations.len(), 1);
    assert!(ack.accepted.contains(&record.meta.op_id));
    assert!(matches!(
        &dana_obligations[0].target,
        crate_storage::ObligationTarget::Clock(target) if target.dominates(&clock)
    ));
}

#[test]
fn retry_keeps_obligation() {
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
    assert!(obligation_covers(
        alice.storage(),
        &alice
            .storage()
            .sync_obligations(&bob.peer_id(), &topic.id())
            .unwrap(),
        &record.meta.op_id
    ));

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
    // have and by MAX_RANGE_SPAN.
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
                genesis: None,
                credit: Default::default(),
                window: crate::sync::ActorWindow::default(),
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
                genesis: None,
                credit: Default::default(),
                window: crate::sync::ActorWindow::default(),
            },
        )
        .unwrap();
    assert!(response.ops.is_empty());
}

#[test]
fn unknown_topic_empty() {
    let alice = node(84);
    let unknown_topic = TopicId::hash(b"never-heard-of-this");
    // A fabricated summary points at OpIds Alice cannot authenticate.
    // The plan must not expose those heads as `need` or `want`.
    let summary = crate_sync::SyncSummary {
        topic_id: unknown_topic,
        event_type_id: None,
        genesis: None,
        fingerprint: [0; 32],
        heads: [OpId::hash(b"forged-head-1"), OpId::hash(b"forged-head-2")].into(),
        actor_clock: ActorClock::new(),
        actor_tips: std::collections::BTreeMap::new(),
        staged: None,
    };
    let plan = alice
        .negotiate_sync(PeerId::hash(b"some-remote"), &summary)
        .unwrap();
    assert!(plan.need.is_empty());
    assert!(plan.send.is_empty());
    assert!(plan.actor_range_hints.is_empty());
}

#[test]
fn duplicate_sync_idempotent() {
    let alice = node(85);
    let bob = node(86);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    topic.publish(Note { text: "one".into() }).unwrap();

    let data = alice
        .plan_sync_data(bob.peer_id(), &bob.sync_summary(topic.id()).unwrap())
        .unwrap();
    let ack = bob
        .receive_sync_data_from(alice.peer_id(), data.clone())
        .unwrap()
        .0;
    assert_eq!(ack.accepted.len(), 2);

    // Full overlap: every op is already admitted.
    let ack = bob.receive_sync_data_from(alice.peer_id(), data).unwrap().0;
    assert!(ack.accepted.is_empty());
    assert_eq!(ack.heads, bob.storage().heads(&topic.id()).unwrap());

    // Partial overlap: resend the full history plus one new op.
    topic.publish(Note { text: "two".into() }).unwrap();
    let all_ops = oplog::topological(alice.storage(), &topic.id()).unwrap();
    assert_eq!(all_ops.len(), 3);
    let new_id = all_ops.last().unwrap().id;
    let ack = bob
        .receive_sync_data_from(
            alice.peer_id(),
            crate_sync::SyncData {
                topic_id: topic.id(),
                ops: all_ops,
            },
        )
        .unwrap()
        .0;
    assert_eq!(ack.accepted, [new_id].into());
    assert_eq!(
        bob.open_topic::<Note>(topic.id())
            .unwrap()
            .history(history::HistoryOrder::OldestFirst)
            .unwrap()
            .len(),
        2
    );
}

/// A backlog of unknown single-op topics exercises receive-side admission.
/// Each op must be signature-verified once even though two admission paths inspect it.
/// This protects the bulk-drain hot path.
#[test]
fn backlog_verify_once() {
    const TOPICS: usize = 1000;
    let alice = node(95);
    let bob = node(96);
    let mut batches = Vec::with_capacity(TOPICS);
    for index in 0..TOPICS {
        let topic = alice
            .create_topic::<Note>(TopicConfig {
                initial_peers: [bob.peer_id()].into(),
                ..TopicConfig::default()
            })
            .unwrap();
        topic
            .publish(Note {
                text: format!("doc-{index}"),
            })
            .unwrap();
        batches.push(crate_sync::SyncData {
            topic_id: topic.id(),
            ops: oplog::topological(alice.storage(), &topic.id()).unwrap(),
        });
    }

    let started = std::time::Instant::now();
    for data in batches {
        let ack = bob.receive_sync_data_from(alice.peer_id(), data).unwrap().0;
        assert_eq!(ack.accepted.len(), 2);
    }
    let elapsed = started.elapsed();
    println!("admitted {TOPICS} unknown single-op topics in {elapsed:?}");
    assert_eq!(bob.list_topics().unwrap().len(), TOPICS);
}

#[test]
fn stale_dedup_read() {
    // A stale dedup read must not turn a resend into an actor gap or fork. An
    // op whose record reads as absent while its index names it is treated as
    // damage and rewritten in place, so the chain still ends up intact.
    let alice = node(87);
    let bob_signer = Ed25519Signer::from_bytes(&[88; 32]);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob_signer.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    topic.publish(Note { text: "one".into() }).unwrap();
    topic.publish(Note { text: "two".into() }).unwrap();
    let mut ops = oplog::topological(alice.storage(), &topic.id()).unwrap();
    assert_eq!(ops.len(), 3);
    let newest = ops.pop().unwrap();

    let storage = StaleReadStorage::new(MemoryStorage::new());
    let receiver = oplog::Oplog::with_storage(storage.clone());
    let accepted = receiver
        .receive_ops_from_peer(Some(alice.peer_id()), ops.clone())
        .unwrap();
    assert_eq!(accepted.len(), 2);

    // Simulate admission racing dedup: the genesis record is hidden while its
    // actor index is visible (fork duplicate path). The second op hides both
    // reads once (sequence duplicate path).
    storage.mid_commit_ops.lock().unwrap().insert(ops[0].id);
    storage.hidden_ops.lock().unwrap().insert(ops[1].id);
    storage.hidden_index.lock().unwrap().insert(ops[1].id);

    let mut resend = ops.clone();
    resend.push(newest.clone());
    let accepted = receiver
        .receive_ops_from_peer(Some(alice.peer_id()), resend)
        .unwrap();
    assert!(accepted.contains(&newest.id));
    assert_eq!(
        storage
            .actor_clock(&topic.id())
            .unwrap()
            .get(&actor_id_for(topic.id(), alice.peer_id())),
        3
    );
    assert_eq!(
        storage.inner.list_op_ids(&topic.id()).unwrap().len(),
        ops.len() + 1
    );
}

#[test]
fn unknown_want_remainder() {
    // A want we cannot resolve must not abort a whole batched exchange.
    let storage = MemoryStorage::new();
    let (_, topic_id, ops) = holed_store(&storage, 93, Damage::Meta);
    let engine = crate_sync::SyncEngine::new(
        oplog::Oplog::with_storage(storage),
        Ed25519Signer::from_bytes(&[93; 32]).peer_id(),
    );
    let request = crate_sync::SyncRequest {
        topic_id,
        known: BTreeSet::new(),
        wants: [ops[0].id, OpId::hash(b"never-seen")].into(),
        actor_range_hints: Vec::new(),
        genesis: None,
        credit: Default::default(),
        window: crate::sync::ActorWindow::default(),
    };

    let data = engine
        .plan_response_data(Ed25519Signer::from_bytes(&[93; 32]).peer_id(), &request)
        .unwrap();

    assert_eq!(
        data.ops.iter().map(|op| op.id).collect::<BTreeSet<_>>(),
        [ops[0].id].into()
    );
}

#[test]
fn dangling_dep_deferred() {
    // The hole and the op standing on it are deferred; the genesis still ships.
    let storage = MemoryStorage::new();
    let (_, topic_id, ops) = holed_store(&storage, 94, Damage::Meta);
    let all = storage.list_op_ids(&topic_id).unwrap();

    let ordered = oplog::topological_subset(&storage, &all).unwrap();

    let served = ordered.iter().map(|op| op.id).collect::<BTreeSet<_>>();
    assert_eq!(served, [ops[0].id].into());
    // Nothing was dropped from storage: the deferred ops are still admitted.
    assert_eq!(storage.list_op_ids(&topic_id).unwrap(), all);
}

#[test]
fn newest_defers_child() {
    // Newest-first must withhold a child whose dependency is unresolved, or it
    // hands back history whose parents cannot be fetched while oldest-first
    // reports a different membership for the same query.
    let storage = MemoryStorage::new();
    let (_, topic_id, ops) = holed_store(&storage, 95, Damage::Meta);
    let holder = Irokle::with_storage(
        storage,
        NodeConfig {
            signer: Ed25519Signer::from_bytes(&[95; 32]),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();

    let walk = |order| {
        holder
            .raw_topic(topic_id)
            .unwrap()
            .dag(history::DagQuery {
                heads: vec![ops[2].id],
                include_heads: true,
                limit: None,
                order,
            })
            .unwrap()
            .iter()
            .map(|op| op.id)
            .collect::<BTreeSet<_>>()
    };

    let newest = walk(history::HistoryOrder::NewestFirst);

    assert!(!newest.contains(&ops[2].id));
    assert_eq!(newest, walk(history::HistoryOrder::OldestFirst));
}

#[test]
fn limit_skips_blocked() {
    // A blocked newest branch must not spend the caller's limit: the older
    // complete branch is reachable inside the same query.
    let a = node(120);
    let b = node(121);
    let topic = a
        .create_topic::<Note>(TopicConfig {
            initial_peers: [b.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let topic_id = topic.id();
    topic.publish(Note { text: "a".into() }).unwrap();
    let a_ops = oplog::topological(a.storage(), &topic_id).unwrap();

    // b branches straight off the genesis, so the store below holds two heads.
    oplog::Oplog::with_storage(b.storage().clone())
        .receive_ops(vec![a_ops[0].clone()])
        .unwrap();
    let b_topic = b.open_topic::<Note>(topic_id).unwrap();
    b_topic
        .publish(Note {
            text: "b one".into(),
        })
        .unwrap();
    b_topic
        .publish(Note {
            text: "b two".into(),
        })
        .unwrap();
    let b_ops = oplog::topological(b.storage(), &topic_id).unwrap();

    let storage = MemoryStorage::new();
    oplog::Oplog::with_storage(storage.clone())
        .receive_ops(vec![
            a_ops[0].clone(),
            a_ops[1].clone(),
            b_ops[1].clone(),
            b_ops[2].clone(),
        ])
        .unwrap();
    damage_op(&storage, &b_ops[1].id, Damage::Meta);
    let holder = Irokle::with_storage(
        storage,
        NodeConfig {
            signer: Ed25519Signer::from_bytes(&[120; 32]),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();

    let walk = |order, limit| {
        holder
            .raw_topic(topic_id)
            .unwrap()
            .dag(history::DagQuery {
                heads: vec![b_ops[2].id, a_ops[1].id],
                include_heads: true,
                limit,
                order,
            })
            .unwrap()
            .into_iter()
            .map(|op| op.id)
            .collect::<Vec<_>>()
    };

    let newest = history::HistoryOrder::NewestFirst;
    let oldest = history::HistoryOrder::OldestFirst;
    assert_eq!(walk(newest, Some(1)), vec![a_ops[1].id]);
    assert_eq!(walk(newest, Some(2)), vec![a_ops[1].id, a_ops[0].id]);
    assert_eq!(walk(oldest, Some(1)), vec![a_ops[0].id]);
    assert_eq!(
        walk(newest, None).into_iter().collect::<BTreeSet<_>>(),
        walk(oldest, None).into_iter().collect::<BTreeSet<_>>()
    );
}

fn assert_repairs_hole<S: Corrupt>(storage: S, seed: u8, damage: Damage) {
    // Start from a store that really lost records for an admitted, non-head op
    // and prove ordinary sync puts them back exactly as they were.
    let holder_signer = Ed25519Signer::from_bytes(&[seed.wrapping_add(1); 32]);
    let (source, topic_id, ops) = chain_source(seed, holder_signer.peer_id());
    oplog::Oplog::with_storage(storage.clone())
        .receive_ops(ops.clone())
        .unwrap();
    let intact = storage.get_meta(&ops[1].id).unwrap().unwrap();
    damage_op(&storage, &ops[1].id, damage);

    let holder = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: holder_signer,
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    // The damage moved neither heads nor the clock, so only an integrity check
    // tells the damaged store apart from the whole one.
    assert_eq!(
        storage.topic_fingerprint(&topic_id).unwrap(),
        source.storage().topic_fingerprint(&topic_id).unwrap()
    );
    assert_eq!(
        holder.topic_unresolved(topic_id).unwrap(),
        [ops[1].id].into()
    );

    let plan = holder
        .negotiate_sync(source.peer_id(), &source.sync_summary(topic_id).unwrap())
        .unwrap();
    assert!(plan.need.contains(&ops[1].id));
    let data = source
        .plan_sync_response_data(
            holder.peer_id(),
            &crate_sync::SyncRequest {
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
    holder
        .receive_sync_data_from(source.peer_id(), data)
        .unwrap();

    assert_eq!(storage.get_op(&ops[1].id).unwrap().as_ref(), Some(&ops[1]));
    assert_eq!(storage.get_meta(&ops[1].id).unwrap(), Some(intact));
    assert!(holder.topic_unresolved(topic_id).unwrap().is_empty());
    assert_eq!(
        oplog::topological(&storage, &topic_id).unwrap().len(),
        ops.len()
    );
}

#[test]
fn memory_repairs_hole() {
    for (seed, damage) in [(101, Damage::Meta), (103, Damage::Op), (105, Damage::Both)] {
        assert_repairs_hole(MemoryStorage::new(), seed, damage);
    }
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_repairs_hole() {
    for (seed, damage) in [(111, Damage::Meta), (113, Damage::Op), (115, Damage::Both)] {
        let dir = tempfile::tempdir().unwrap();
        assert_repairs_hole(
            crate_storage::FjallStorage::open(dir.path()).unwrap(),
            seed,
            damage,
        );
    }
}

#[test]
fn repair_dangling_dep() {
    // A store holding a hole must pull it from a peer over the ordinary
    // negotiate/request path and end up whole, even though its heads and clock
    // never revealed the gap.
    let source_signer = Ed25519Signer::from_bytes(&[96; 32]);
    let holder_signer = Ed25519Signer::from_bytes(&[97; 32]);
    let source = Irokle::new(NodeConfig {
        signer: source_signer.clone(),
        default_write_concern: WriteConcern::Local,
        ..NodeConfig::default()
    })
    .unwrap();
    let topic = source
        .create_topic::<Note>(TopicConfig {
            initial_peers: [holder_signer.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    topic.publish(Note { text: "one".into() }).unwrap();
    topic.publish(Note { text: "two".into() }).unwrap();
    let ops = oplog::topological(source.storage(), &topic.id()).unwrap();
    let topic_id = topic.id();

    // Withhold the middle op: the newest buffers with that id as a hole.
    let holder = Irokle::new(NodeConfig {
        signer: holder_signer.clone(),
        default_write_concern: WriteConcern::Local,
        ..NodeConfig::default()
    })
    .unwrap();
    holder
        .receive_sync_data_from(
            source.peer_id(),
            crate_sync::SyncData {
                topic_id,
                ops: vec![ops[0].clone(), ops[2].clone()],
            },
        )
        .unwrap();
    assert!(holder.storage().get_meta(&ops[2].id).unwrap().is_none());
    assert_eq!(
        holder.storage().pending_missing_deps(&topic_id).unwrap(),
        [ops[1].id].into()
    );

    let plan = holder
        .negotiate_sync(source.peer_id(), &source.sync_summary(topic_id).unwrap())
        .unwrap();
    assert!(plan.need.contains(&ops[1].id));

    let data = source
        .plan_sync_response_data(
            holder_signer.peer_id(),
            &crate_sync::SyncRequest {
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
    assert!(data.ops.iter().any(|op| op.id == ops[1].id));

    holder
        .receive_sync_data_from(source.peer_id(), data)
        .unwrap();

    // The hole is closed, the buffered dependent is admitted, and every
    // admitted op resolves its dependencies again.
    assert!(
        holder
            .storage()
            .pending_missing_deps(&topic_id)
            .unwrap()
            .is_empty()
    );
    for op_id in holder.storage().list_op_ids(&topic_id).unwrap() {
        let meta = holder.storage().get_meta(&op_id).unwrap().unwrap();
        for dep in &meta.deps {
            assert!(holder.storage().get_meta(dep).unwrap().is_some());
        }
    }
    assert_eq!(
        oplog::topological(holder.storage(), &topic_id)
            .unwrap()
            .len(),
        ops.len()
    );
}

#[test]
fn recheck_finds_damage() {
    // A topic verified whole is not re-scanned on every sync, so damage from
    // outside irokle stays invisible until the audit is asked for again.
    let storage = MemoryStorage::new();
    let holder_signer = Ed25519Signer::from_bytes(&[132; 32]);
    let (_, topic_id, ops) = chain_source(131, holder_signer.peer_id());
    oplog::Oplog::with_storage(storage.clone())
        .receive_ops(ops.clone())
        .unwrap();
    let holder = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: holder_signer,
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    assert!(holder.topic_unresolved(topic_id).unwrap().is_empty());

    damage_op(&storage, &ops[1].id, Damage::Meta);
    assert!(holder.topic_unresolved(topic_id).unwrap().is_empty());
    holder.recheck_topics().unwrap();

    assert_eq!(
        holder.topic_unresolved(topic_id).unwrap(),
        [ops[1].id].into()
    );
}

/// Two outcomes recorded for the same peer and topic at the same time, forced
/// to interleave by a gate inside the obligation read every status update
/// performs before it writes.
#[test]
fn retains_concurrent_counters() {
    let storage = StaleReadStorage::new(MemoryStorage::new());
    let alice = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: Ed25519Signer::from_bytes(&[152; 32]),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let peer = PeerId::hash(b"concurrent-status-peer");
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let topic_id = topic.id();
    storage.arm_gate(Arc::new(Rendezvous::new(2)));

    let handles = [true, false].map(|success| {
        let node = alice.clone();
        thread::spawn(move || {
            let failure = std::io::Error::other("concurrent sync failure");
            let outcome = if success { Ok(()) } else { Err(&failure) };
            node.record_sync_result(peer, topic_id, outcome).unwrap();
        })
    });
    for handle in handles {
        handle.join().unwrap();
    }

    let status = alice.sync_status(topic_id).unwrap();
    assert_eq!(status.len(), 1);
    assert_eq!(status[0].successful_attempts, 1);
    assert_eq!(status[0].failed_attempts, 1);
}

fn assert_resolved_targets<S: Storage>(storage: S) {
    let alice = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: Ed25519Signer::from_bytes(&[153; 32]),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let peer = PeerId::hash(b"resolved-target-peer");
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let record = topic
        .publish(Note {
            text: "resolved".into(),
        })
        .unwrap();
    let unknown = OpId::hash(b"resolved-target-unknown");

    alice
        .put_sync_obligation(peer, topic.id(), [record.meta.op_id, unknown].into())
        .unwrap();

    let obligations = storage.sync_obligations(&peer, &topic.id()).unwrap();
    // The resolved id becomes a clock target at its actor position, and the
    // unknown id an explicit repair want of its own.
    let mut position = ActorClock::new();
    position.observe(record.meta.actor_id, record.meta.actor_seq);
    assert_eq!(
        obligations,
        vec![
            crate_storage::SyncObligation::clock(peer, topic.id(), position),
            crate_storage::SyncObligation::repair(peer, topic.id(), [unknown].into()),
        ]
    );
}

#[test]
fn memory_resolved_targets() {
    assert_resolved_targets(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_resolved_targets() {
    let dir = tempfile::tempdir().unwrap();
    assert_resolved_targets(crate_storage::FjallStorage::open(dir.path()).unwrap());
}

/// Forwarding obligations are the durable part of a receive; a failed status
/// write must not stop the peers that have not been filed yet.
#[test]
fn forwards_without_bookkeeping() {
    let alice = node(154);
    let storage = StaleReadStorage::new(MemoryStorage::new());
    let bob = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: Ed25519Signer::from_bytes(&[155; 32]),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let charlie = node(156);
    let dana = node(157);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id(), charlie.peer_id(), dana.peer_id()].into(),
            replication_policy: ReplicationPolicy::all().with_max_sync_peers(8),
        })
        .unwrap();
    topic.publish(Note { text: "fan".into() }).unwrap();
    let data = crate_sync::SyncData {
        topic_id: topic.id(),
        ops: oplog::topological(alice.storage(), &topic.id()).unwrap(),
    };
    storage.fail_status(topic.id());

    bob.receive_sync_data_from(alice.peer_id(), data).unwrap();

    for peer in [charlie.peer_id(), dana.peer_id()] {
        assert!(
            !storage
                .sync_obligations(&peer, &topic.id())
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn request_skips_bodies() {
    // Request planning discards the send list, so it must not load op bodies
    // for the closure the remote is missing.
    let holder = Ed25519Signer::from_bytes(&[171; 32]);
    let (source, topic_id, ops) = chain_source(170, holder.peer_id());
    // A member that holds nothing yet: every local op is missing for it.
    let remote = crate_sync::SyncSummary {
        topic_id,
        event_type_id: None,
        genesis: None,
        fingerprint: [0; 32],
        heads: BTreeSet::new(),
        actor_clock: ActorClock::new(),
        actor_tips: std::collections::BTreeMap::new(),
        staged: None,
    };
    // Each side gets its own store so both measurements start from the same
    // cached state; a second call on one store would not rescan for holes.
    let counted_engine = || {
        let storage = StaleReadStorage::new(MemoryStorage::new());
        let log = oplog::Oplog::with_storage(storage.clone());
        log.receive_ops_from_peer(Some(source.peer_id()), ops.clone())
            .unwrap();
        let engine = crate_sync::SyncEngine::new(log, holder.peer_id());
        (engine, storage)
    };
    let reads_since = |storage: &StaleReadStorage, before: usize| {
        storage.op_reads.load(std::sync::atomic::Ordering::Relaxed) - before
    };

    let (engine, storage) = counted_engine();
    let before = storage.op_reads.load(std::sync::atomic::Ordering::Relaxed);
    let request = engine.plan_request(source.peer_id(), &remote).unwrap();
    let request_reads = reads_since(&storage, before);
    // Once the integrity scan is cached, a request reads no body at all.
    let before = storage.op_reads.load(std::sync::atomic::Ordering::Relaxed);
    engine.plan_request(source.peer_id(), &remote).unwrap();
    assert_eq!(reads_since(&storage, before), 0);

    let (engine, storage) = counted_engine();
    let before = storage.op_reads.load(std::sync::atomic::Ordering::Relaxed);
    let plan = engine.negotiate(source.peer_id(), &remote).unwrap();
    let full_reads = reads_since(&storage, before);

    assert_eq!(request.topic_id, topic_id);
    assert_eq!(plan.send.len(), ops.len());
    // Full negotiation loads every missing body and probes its resolvability;
    // request planning walks no history, beyond the one integrity scan.
    assert!(request_reads <= 2 * ops.len());
    assert!(full_reads >= request_reads + ops.len());
}

#[test]
fn request_matches_negotiation() {
    // The request asks for exactly what the full negotiation found missing,
    // for a plain chain and for a fork that merged, without walking history:
    // served page by page it brings the requester to the remote frontier.
    let alice = node(172);
    let bob = node(173);
    let same_request = |peer, remote: &crate_sync::SyncSummary| {
        let plan = alice.negotiate_sync(peer, remote).unwrap();
        let request = alice.plan_sync_request(peer, remote).unwrap();
        assert!(request.known.is_empty());
        assert_eq!(request.genesis, genesis_of(alice.storage(), &plan.topic_id));
        let local = alice.storage().actor_clock(&plan.topic_id).unwrap();
        let ahead = remote
            .actor_clock
            .iter()
            .filter(|(actor, seq)| local.get(actor) < **seq)
            .map(|(actor, seq)| (*actor, local.get(actor), *seq))
            .collect::<Vec<_>>();
        let ranges = request
            .actor_range_hints
            .iter()
            .map(|hint| (hint.actor_id, hint.from_exclusive, hint.to_inclusive))
            .collect::<Vec<_>>();
        assert_eq!(ranges, ahead);
        assert!(request.wants.is_subset(&plan.need));
        request
    };

    let chain = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    chain.publish(Note { text: "one".into() }).unwrap();
    chain.publish(Note { text: "two".into() }).unwrap();
    same_request(bob.peer_id(), &bob.sync_summary(chain.id()).unwrap());

    let forked = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    forked
        .publish(Note {
            text: "base".into(),
        })
        .unwrap();
    let data = alice
        .plan_sync_data(bob.peer_id(), &bob.sync_summary(forked.id()).unwrap())
        .unwrap();
    bob.receive_sync_data_from(alice.peer_id(), data).unwrap();
    let bob_forked = bob.open_topic::<Note>(forked.id()).unwrap();
    // Concurrent publishes fork the topic; alice then merges both sides.
    bob_forked.publish(Note { text: "bob".into() }).unwrap();
    forked
        .publish(Note {
            text: "alice".into(),
        })
        .unwrap();
    let data = bob
        .plan_sync_data(alice.peer_id(), &alice.sync_summary(forked.id()).unwrap())
        .unwrap();
    alice.receive_sync_data_from(bob.peer_id(), data).unwrap();
    forked
        .publish(Note {
            text: "merge".into(),
        })
        .unwrap();
    bob_forked
        .publish(Note {
            text: "later".into(),
        })
        .unwrap();

    let bob_summary = bob.sync_summary(forked.id()).unwrap();
    let request = same_request(bob.peer_id(), &bob_summary);
    assert!(!request.actor_range_hints.is_empty());
    let data = bob
        .plan_sync_response_data(alice.peer_id(), &request)
        .unwrap();
    alice.receive_sync_data_from(bob.peer_id(), data).unwrap();
    let alice_clock = alice.storage().actor_clock(&forked.id()).unwrap();
    assert!(alice_clock.dominates(&bob_summary.actor_clock));
}

/// Runtime reachability reaches production selection: a preferred peer that
/// keeps failing is passed over for another permitted peer without a new
/// publish, and is selected again once it answers.
#[test]
fn health_selects_alternate() {
    let alice = node(150);
    let members = (151..=154u8)
        .map(|seed| Ed25519Signer::from_bytes(&[seed; 32]).peer_id())
        .collect::<BTreeSet<_>>();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: members.clone(),
            replication_policy: ReplicationPolicy::all().with_max_sync_peers(1),
        })
        .unwrap();
    let state = alice.storage().topic_state(&topic.id()).unwrap().unwrap();

    let selected = alice.sync_peers(topic.id(), &state);
    assert_eq!(selected.len(), 1, "fanout one selects one target");
    let preferred = selected[0];

    // Unreachable attempts only; nothing new is published in between.
    for _ in 0..node::PEER_FAILURE_LIMIT {
        alice.peer_health().record_failure(preferred);
    }
    let failover = alice.sync_peers(topic.id(), &state);
    assert_eq!(failover.len(), 1);
    assert_ne!(
        failover[0], preferred,
        "a peer past its retry budget must not stay the only target"
    );
    assert!(
        members.contains(&failover[0]),
        "the alternate must be a permitted member"
    );

    // Recovery restores the preferred peer.
    alice.peer_health().record_success(&preferred);
    assert_eq!(alice.peer_health().failures(&preferred), 0);
    assert_eq!(alice.sync_peers(topic.id(), &state), vec![preferred]);
}

/// An attempt neither reached nor unreachable, such as a refused exchange, must
/// not demote a peer; an unreachable attempt adds one failure, and reaching the
/// peer clears them even when other topics of that attempt failed.
#[cfg(feature = "iroh")]
#[test]
fn refusal_keeps_health() {
    let alice = node(155);
    let bob = node(156);
    for _ in 0..node::PEER_FAILURE_LIMIT {
        assert!(!alice.note_peer_outcome(bob.peer_id(), false, false));
    }
    assert_eq!(alice.peer_health().failures(&bob.peer_id()), 0);

    assert!(alice.note_peer_outcome(bob.peer_id(), false, true));
    assert_eq!(
        alice.peer_health().failures(&bob.peer_id()),
        1,
        "one failed connection is one health failure"
    );

    assert!(alice.note_peer_outcome(bob.peer_id(), true, true));
    assert_eq!(alice.peer_health().failures(&bob.peer_id()), 0);
}

/// One topic whose records cannot be read is a per-topic outcome: maintenance
/// still visits the topics after it, and work owed for a healthy topic is still
/// scheduled rather than being lost with the first failure.
#[test]
fn faulting_topic_isolated() {
    let storage = StaleReadStorage::new(MemoryStorage::new());
    let alice = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: Ed25519Signer::from_bytes(&[160; 32]),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let peer = Ed25519Signer::from_bytes(&[161; 32]).peer_id();

    let mut topics = Vec::new();
    for _ in 0..2 {
        let topic = alice
            .create_topic::<Note>(TopicConfig {
                initial_peers: [peer].into(),
                ..TopicConfig::default()
            })
            .unwrap();
        topics.push(topic);
    }
    // Fault the topic enumeration reaches first.
    let order = alice.list_topics().unwrap();
    let faulting = order[0].topic_id;
    let healthy = order[1].topic_id;
    let healthy_topic = topics
        .iter()
        .find(|topic| topic.id() == healthy)
        .expect("healthy topic");
    let record = healthy_topic
        .publish(Note {
            text: "owed".into(),
        })
        .unwrap();
    alice
        .put_sync_obligation(peer, healthy, [record.meta.op_id].into())
        .unwrap();
    storage.fail_heads(faulting);

    // Maintenance reports no global failure and leaves the bad topic for later.
    assert!(
        alice.quarantine_topics().unwrap().is_empty(),
        "a topic-local read failure must not abort the pass"
    );

    // The healthy topic's durable work is still there to be scheduled.
    let owed = storage
        .all_sync_obligations()
        .unwrap()
        .into_iter()
        .filter(|obligation| obligation.topic_id == healthy)
        .count();
    assert_eq!(owed, 1, "healthy work must survive a topic-local failure");
    assert!(
        alice.storage().heads(&healthy).is_ok(),
        "the healthy topic stays readable"
    );
}

/// Forwarded receives of N and then 2N ops keep one clock record per peer owed
/// the work, and their status bookkeeping decodes no obligation record.
fn assert_forwards_coalesce<S: Storage>(storage: S, counters: impl Fn() -> crate::CounterSnapshot) {
    let bob_signer = Ed25519Signer::from_bytes(&[172; 32]);
    let carol = Ed25519Signer::from_bytes(&[173; 32]).peer_id();
    let alice = node(171);
    let bob = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: bob_signer.clone(),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob_signer.peer_id(), carol].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let topic_id = topic.id();
    let alice_actor = actor_id_for(topic_id, alice.peer_id());
    let rounds = 8;

    let mut sent = BTreeSet::new();
    for round in 1..=2 {
        for index in 0..rounds {
            topic
                .publish(Note {
                    text: format!("{round}-{index}"),
                })
                .unwrap();
        }
        let ops = oplog::topological(alice.storage(), &topic_id)
            .unwrap()
            .into_iter()
            .filter(|op| sent.insert(op.id))
            .collect::<Vec<_>>();
        let before = counters().obligation_reads;
        bob.receive_sync_data_from(alice.peer_id(), sync::SyncData { topic_id, ops })
            .unwrap();
        assert_eq!(counters().obligation_reads, before, "round {round}");

        assert_eq!(
            storage.topic_obligation_counts(&topic_id).unwrap(),
            [(carol, 1)].into(),
            "round {round}"
        );
        let records = storage.sync_obligations(&carol, &topic_id).unwrap();
        assert!(matches!(
            &records[..],
            [crate::storage::SyncObligation {
                target: crate::storage::ObligationTarget::Clock(clock),
                ..
            }] if clock.get(&alice_actor) == 1 + round * rounds
        ));
        let status = bob.sync_status(topic_id).unwrap();
        assert!(
            status
                .iter()
                .any(|status| status.peer_id == carol && status.pending_obligations == 1)
        );
    }
}

#[test]
fn memory_forwards_coalesce() {
    let storage = MemoryStorage::new();
    let counters = storage.clone();
    assert_forwards_coalesce(storage, move || counters.counters());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_forwards_coalesce() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate::storage::FjallStorage::open(dir.path()).unwrap();
    let counters = storage.clone();
    assert_forwards_coalesce(storage, move || counters.counters());
}

/// Status state follows the typed attempt outcome, so a partial pull with
/// nothing owed outbound stays behind, and an older attempt changes nothing.
fn assert_outcome_states<S: Storage>(storage: S) {
    let alice = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: Ed25519Signer::from_bytes(&[176; 32]),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let peer = Ed25519Signer::from_bytes(&[177; 32]).peer_id();
    let topic_id = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [peer].into(),
            ..TopicConfig::default()
        })
        .unwrap()
        .id();
    let epoch = storage.next_attempt_epoch().unwrap();
    let steps = [
        (
            crate::AttemptOutcome::Advanced,
            crate_storage::SyncPeerState::Behind,
            None,
        ),
        (
            crate::AttemptOutcome::Blocked("credit spent".into()),
            crate_storage::SyncPeerState::Behind,
            Some("credit spent"),
        ),
        (
            crate::AttemptOutcome::Failed("dial failed".into()),
            crate_storage::SyncPeerState::Failed,
            Some("dial failed"),
        ),
        (
            crate::AttemptOutcome::ReopenRequired("reopen store".into()),
            crate_storage::SyncPeerState::Behind,
            Some("reopen store"),
        ),
        (
            crate::AttemptOutcome::Complete,
            crate_storage::SyncPeerState::Healthy,
            None,
        ),
    ];
    for (sequence, (outcome, state, error)) in (1..).zip(steps) {
        alice
            .record_attempt_result(peer, topic_id, (epoch, sequence), &outcome, true)
            .unwrap();
        let status = alice.sync_status(topic_id).unwrap().remove(0);
        assert_eq!(status.pending_obligations, 0);
        assert_eq!(status.state, state, "{outcome:?}");
        assert_eq!(status.last_error.as_deref(), error, "{outcome:?}");
    }
    let old = alice
        .record_attempt_result(
            peer,
            topic_id,
            (epoch, 2),
            &crate::AttemptOutcome::Failed("late".into()),
            true,
        )
        .unwrap();
    assert_eq!(old.state, crate_storage::SyncPeerState::Healthy);
    assert_eq!((old.successful_attempts, old.failed_attempts), (2, 3));
}

#[test]
fn memory_outcome_states() {
    assert_outcome_states(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_outcome_states() {
    let dir = tempfile::tempdir().unwrap();
    assert_outcome_states(crate::storage::FjallStorage::open(dir.path()).unwrap());
}
