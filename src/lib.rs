// SPDX-License-Identifier: MIT OR Apache-2.0
//! Public facade and re-exports for Irokle's signed topic operation log.

pub mod clock;
pub mod crypto;
pub mod error;
pub mod event;
pub mod history;
pub mod ids;
pub mod net;
pub mod node;
pub mod op;
/// Advanced operation-log internals. Prefer the `Irokle` facade for normal use.
pub mod oplog;
pub mod reducer;
pub mod storage;
/// Advanced sync planning internals and message types. Prefer `Irokle` sync facade methods.
pub mod sync;
pub mod topic;

extern crate self as irokle;

pub use clock::ActorClock;
pub use crypto::{Ed25519Signer, Signer, canonical_bytes, verify};
pub use error::{Error, Result};
pub use event::{Event, EventEnvelope};
pub use ids::{ActorId, IdParseError, OpId, PeerId, TopicId, actor_id_for};
pub use irokle_derive::Event;
pub use node::{Irokle, IrokleBuilder, NodeConfig, PublishOptions, RawTopic, Topic, WriteConcern};
pub use op::{Op, OpBody, SignedOp};
#[cfg(feature = "fjall")]
pub use storage::FjallStorage;
pub use storage::{MemoryStorage, Storage, SyncPeerState, SyncPeerStatus};
pub use topic::{
    ReplicationPolicy, TopicConfig, TopicControl, TopicGenesis, TopicInfo, TopicPayload,
};

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::{Arc, Barrier};
    use std::thread;

    use bytes::Bytes;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Note {
        text: String,
    }

    impl Event for Note {
        const TYPE_ID: &'static str = "test.note";
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Other;

    impl Event for Other {
        const TYPE_ID: &'static str = "test.other";
    }

    fn node(seed: u8) -> Irokle {
        Irokle::new(NodeConfig {
            signer: Ed25519Signer::from_bytes(&[seed; 32]),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        })
        .unwrap()
    }

    #[test]
    fn op_id_validation_rejects_tamper() {
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
    fn actor_chain_sequence_and_prev_are_real() {
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
    fn concurrent_publishes_on_cloned_node_do_not_fork_actor() {
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
    fn builder_configures_signer_and_storage() {
        let signer = Ed25519Signer::from_bytes(&[77; 32]);
        let irokle = Irokle::builder()
            .with_storage(MemoryStorage::new())
            .with_signer(signer.clone())
            .build()
            .unwrap();

        assert_eq!(irokle.peer_id(), signer.peer_id());
        assert!(irokle.list_topics().unwrap().is_empty());
    }

    #[cfg(feature = "fjall")]
    #[test]
    fn builder_can_select_fjall_storage() {
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
    fn builder_can_accept_fjall_database() {
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

    #[cfg(feature = "iroh")]
    #[tokio::test]
    async fn iroh_builder_returns_internal_net_irokle() {
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .bind()
            .await
            .unwrap();
        let expected_peer = PeerId::from_bytes(*endpoint.id().as_bytes());
        let irokle = Irokle::builder()
            .with_net(endpoint)
            .without_auto_accept()
            .build()
            .unwrap();

        assert_eq!(irokle.peer_id(), expected_peer);
        assert!(irokle.endpoint().is_some());
        assert!(irokle.list_topics().unwrap().is_empty());
    }

    #[cfg(all(feature = "iroh", feature = "fjall"))]
    #[tokio::test]
    async fn iroh_builder_can_select_fjall_storage() {
        let dir = tempfile::tempdir().unwrap();
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .bind()
            .await
            .unwrap();
        let irokle = Irokle::builder()
            .with_net(endpoint)
            .with_fjall_path(dir.path())
            .unwrap()
            .without_auto_accept()
            .build()
            .unwrap();

        assert!(irokle.endpoint().is_some());
        assert!(irokle.list_topics().unwrap().is_empty());
    }

    fn concurrent_publishes_on_independent_facades_do_not_fork_actor_for_storage<S: Storage>(
        storage: S,
    ) {
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
    fn concurrent_publishes_on_independent_memory_facades_do_not_fork_actor() {
        concurrent_publishes_on_independent_facades_do_not_fork_actor_for_storage(
            MemoryStorage::new(),
        );
    }

    fn concurrent_create_topic_ids_are_distinct_for_storage<S: Storage>(storage: S) {
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
    fn concurrent_create_topic_ids_are_distinct_for_memory() {
        concurrent_create_topic_ids_are_distinct_for_storage(MemoryStorage::new());
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
    fn memory_storage_topic_publish_history() {
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

    #[test]
    fn sync_transfers_missing_ops() {
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
    fn sync_summary_exposes_cached_fingerprint() {
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
    fn sync_negotiation_short_circuits_when_fingerprints_match() {
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
    fn sync_negotiation_finds_common_ancestor_and_peer_need() {
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
            sync::SyncData {
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
    fn sync_request_converges_bidirectionally_in_one_negotiation() {
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
            sync::SyncData {
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
    fn sync_missing_closure_is_topological_and_clock_bounded() {
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
            sync::SyncData {
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
    fn sync_data_messages_batch_ops_preserving_order() {
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
            let sync::SyncMessage::Data(data) = batch else {
                panic!("expected data batch");
            };
            assert!(data.ops.len() <= net::MAX_SYNC_DATA_OPS_PER_MESSAGE);
            actual_ids.extend(data.ops.into_iter().map(|op| op.id));
        }
        assert_eq!(actual_ids, expected_ids);
    }

    #[test]
    fn sync_response_includes_actor_range_dependency_closure() {
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
                &sync::SyncRequest {
                    topic_id: topic.id(),
                    known: [genesis.id].into(),
                    wants: BTreeSet::new(),
                    actor_range_hints: vec![sync::ActorRangeHint {
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
    fn receive_sync_data_accepts_out_of_order_batch() {
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
                sync::SyncData {
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
    fn receive_sync_data_defers_pending_op_until_dependency_arrives() {
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
            sync::SyncData {
                topic_id: topic.id(),
                ops: vec![genesis.clone()],
            },
        )
        .unwrap();

        let first_ack = bob
            .receive_sync_data_from(
                alice.peer_id(),
                sync::SyncData {
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
                sync::SyncData {
                    topic_id: topic.id(),
                    ops: vec![genesis.clone(), first.clone()],
                },
            )
            .unwrap();
        assert_eq!(second_ack.accepted, [first.id, second.id].into());
        assert!(bob.storage().get_op(&second.id).unwrap().is_some());
    }

    fn pending_reconciles_after_dependency_admitted_for_storage<S: Storage>(storage: S) {
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
            .put_admitted_batch(
                topic.id(),
                storage.heads(&topic.id()).unwrap(),
                storage.topic_state(&topic.id()).unwrap(),
                vec![(first.clone(), first_meta)],
                [first.id].into(),
                None,
            )
            .unwrap();
        assert!(storage.get_op(&first.id).unwrap().is_some());
        assert!(storage.get_op(&second.id).unwrap().is_none());

        Irokle::with_storage(storage.clone(), bob_config).unwrap();
        assert!(storage.get_op(&second.id).unwrap().is_some());
        assert_eq!(storage.heads(&topic.id()).unwrap(), [second.id].into());
    }

    #[test]
    fn memory_pending_reconciles_after_dependency_admitted_on_open() {
        pending_reconciles_after_dependency_admitted_for_storage(MemoryStorage::new());
    }

    #[test]
    fn pending_op_with_too_many_missing_deps_is_rejected() {
        let signer = Ed25519Signer::from_bytes(&[49; 32]);
        let topic_id = TopicId::hash(b"pending-limit-topic");
        let deps = (0..=storage::MAX_PENDING_MISSING_DEPS)
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

    #[test]
    fn dag_tail_query_reads_from_heads_and_honors_limit() {
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
    fn bounded_sync_peer_selection_caps_member_fanout() {
        let local = PeerId::hash(b"local");
        let topic_id = TopicId::hash(b"fanout-topic");
        let mut members = [local].into_iter().collect::<BTreeSet<_>>();
        for idx in 0..24_u8 {
            members.insert(PeerId::hash([idx]));
        }
        let state = storage::TopicState {
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
    fn sync_peer_selection_has_deterministic_overlap() {
        let topic_id = TopicId::hash(b"overlap-topic");
        let mut peers = Vec::new();
        let mut members = BTreeSet::new();
        for idx in 0..64_u8 {
            let peer = PeerId::hash([idx]);
            peers.push(peer);
            members.insert(peer);
        }
        let state = storage::TopicState {
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
    fn sync_status_reports_failures_and_pending_obligations() {
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
        assert_eq!(status[0].state, storage::SyncPeerState::Failed);
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
                .get(&storage::SyncPeerState::Failed),
            Some(&1)
        );
    }

    #[test]
    fn introduced_peer_can_reject_topic_with_membership_control() {
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
    fn late_added_peer_can_accept_initial_sync_batch() {
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

    #[cfg(feature = "iroh")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn iroh_sync_now_records_ack_for_remote_peer() {
        let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let alice = Irokle::builder().with_net(alice_endpoint).build().unwrap();
        let bob = Irokle::builder()
            .with_peer_whitelist([alice.peer_id()])
            .with_net(bob_endpoint)
            .build()
            .unwrap();
        let bob_addr = ready_iroh_addr(bob.endpoint().unwrap()).await;

        let topic = alice
            .create_topic::<Note>(TopicConfig {
                initial_peers: [bob.peer_id()].into(),
                ..TopicConfig::default()
            })
            .unwrap();
        let record = topic
            .publish(Note {
                text: "iroh".into(),
            })
            .unwrap();
        alice
            .put_sync_obligation(bob.peer_id(), topic.id(), [record.meta.op_id].into())
            .unwrap();

        alice.sync_addr_now(bob_addr, topic.id()).await.unwrap();

        assert_eq!(
            bob.open_topic::<Note>(topic.id())
                .unwrap()
                .history(history::HistoryOrder::OldestFirst)
                .unwrap()
                .len(),
            1
        );
        assert!(
            alice
                .storage()
                .peer_ack(&bob.peer_id(), &topic.id())
                .unwrap()
                .is_some()
        );
        assert!(
            alice
                .storage()
                .peer_ack(&alice.peer_id(), &topic.id())
                .unwrap()
                .is_none()
        );
        assert!(
            alice
                .sync_report(bob.peer_id(), topic.id())
                .unwrap()
                .obligations
                .is_empty()
        );
    }

    #[cfg(feature = "iroh")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn iroh_open_does_not_return_summary_to_non_member() {
        let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .bind()
            .await
            .unwrap();
        let outsider_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .bind()
            .await
            .unwrap();
        let alice = Irokle::builder()
            .with_iroh_secret_key(alice_endpoint.secret_key())
            .build()
            .unwrap();
        let topic = alice.create_topic::<Note>(TopicConfig::default()).unwrap();
        let net = net::IrohNet::new(alice_endpoint, alice.clone()).unwrap();
        let outsider_peer = PeerId::from_bytes(*outsider_endpoint.id().as_bytes());

        let responses = net
            .handle_messages(
                outsider_endpoint.id(),
                vec![sync::SyncMessage::Open(
                    sync::SyncEngine::<MemoryStorage>::open(topic.id(), outsider_peer, None),
                )],
            )
            .unwrap();

        assert!(responses.is_empty());
    }

    #[cfg(feature = "iroh")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn iroh_peer_whitelist_controls_unknown_topic_admission() {
        let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .bind()
            .await
            .unwrap();
        let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .bind()
            .await
            .unwrap();
        let alice = Irokle::builder()
            .with_iroh_secret_key(alice_endpoint.secret_key())
            .build()
            .unwrap();
        let bob = Irokle::builder()
            .with_iroh_secret_key(bob_endpoint.secret_key())
            .build()
            .unwrap();
        let net = net::IrohNet::new(bob_endpoint, bob.clone()).unwrap();
        let topic = alice
            .create_topic::<Note>(TopicConfig {
                initial_peers: [bob.peer_id()].into(),
                ..TopicConfig::default()
            })
            .unwrap();
        let data = sync::SyncData {
            topic_id: topic.id(),
            ops: oplog::topological(alice.storage(), &topic.id()).unwrap(),
        };

        let err = net
            .handle_messages(
                alice_endpoint.id(),
                vec![
                    sync::SyncMessage::Open(sync::SyncEngine::<MemoryStorage>::open(
                        topic.id(),
                        alice.peer_id(),
                        None,
                    )),
                    sync::SyncMessage::Data(data.clone()),
                ],
            )
            .unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(bob.storage().topic_state(&topic.id()).unwrap().is_none());

        bob.add_peer_to_whitelist(alice.peer_id()).unwrap();
        let charlie = node(106);
        let excluded_topic = alice
            .create_topic::<Note>(TopicConfig {
                initial_peers: [charlie.peer_id()].into(),
                ..TopicConfig::default()
            })
            .unwrap();
        let excluded_data = sync::SyncData {
            topic_id: excluded_topic.id(),
            ops: oplog::topological(alice.storage(), &excluded_topic.id()).unwrap(),
        };
        let err = net
            .handle_messages(
                alice_endpoint.id(),
                vec![
                    sync::SyncMessage::Open(sync::SyncEngine::<MemoryStorage>::open(
                        excluded_topic.id(),
                        alice.peer_id(),
                        None,
                    )),
                    sync::SyncMessage::Data(excluded_data),
                ],
            )
            .unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            bob.storage()
                .topic_state(&excluded_topic.id())
                .unwrap()
                .is_none()
        );

        let responses = net
            .handle_messages(
                alice_endpoint.id(),
                vec![
                    sync::SyncMessage::Open(sync::SyncEngine::<MemoryStorage>::open(
                        topic.id(),
                        alice.peer_id(),
                        None,
                    )),
                    sync::SyncMessage::Data(data),
                ],
            )
            .unwrap();

        assert!(
            responses
                .iter()
                .any(|response| matches!(response, sync::SyncMessage::Ack(_)))
        );
        assert!(bob.storage().topic_state(&topic.id()).unwrap().is_some());
    }

    #[cfg(feature = "iroh")]
    async fn ready_iroh_addr(endpoint: &iroh::Endpoint) -> iroh::EndpointAddr {
        use futures::StreamExt;
        use iroh::Watcher;

        let addr = endpoint.addr();
        if !addr.addrs.is_empty() {
            return addr;
        }
        let mut stream = endpoint.watch_addr().stream();
        tokio::time::timeout(std::time::Duration::from_secs(5), async move {
            loop {
                let addr = stream.next().await.expect("iroh endpoint address stream");
                if !addr.addrs.is_empty() {
                    return addr;
                }
            }
        })
        .await
        .expect("iroh endpoint produced a dialable address")
    }

    #[test]
    fn concurrent_membership_controls_converge_independent_of_arrival_order() {
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
    fn sync_does_not_send_topic_ops_to_non_member() {
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
    fn receive_sync_data_from_rejects_unknown_topic_when_local_peer_not_in_genesis() {
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
    fn sync_report_filters_obligations_by_peer_and_topic() {
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

    fn ack_clears_only_satisfied_obligations_for_storage<S: Storage>(storage: S) {
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
    fn memory_ack_clears_only_satisfied_obligations() {
        ack_clears_only_satisfied_obligations_for_storage(MemoryStorage::new());
    }

    fn stale_ack_does_not_regress_stored_peer_ack_for_storage<S: Storage>(storage: S) {
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
    fn memory_stale_ack_does_not_regress_stored_peer_ack() {
        stale_ack_does_not_regress_stored_peer_ack_for_storage(MemoryStorage::new());
    }

    #[test]
    fn unsigned_sync_ack_does_not_clear_obligations() {
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
    fn ack_clears_obligation_when_clock_dominates_target_op() {
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
    fn ack_clock_cannot_claim_future_local_state() {
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
    fn ack_heads_must_reference_known_topic_ops() {
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

    #[test]
    fn newly_added_peer_cannot_backdate_event_before_add() {
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
    fn late_valid_event_before_later_remove_is_accepted() {
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
    fn received_op_rejects_author_using_another_actor_id() {
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
    fn sync_data_rejects_ops_from_another_topic() {
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
            sync::SyncData {
                topic_id: topic_a.id(),
                ops: vec![op_a],
            },
        )
        .unwrap();
        assert!(matches!(
            bob.receive_sync_data_from(
                alice.peer_id(),
                sync::SyncData {
                    topic_id: topic_a.id(),
                    ops: vec![op_b],
                },
            ),
            Err(Error::TopicMismatch)
        ));
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
    fn received_event_op_rejects_mismatched_event_type() {
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

    #[test]
    fn sync_metadata_and_ack_reachability_are_exposed() {
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

        let data = sync::SyncData {
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
    fn plan_response_data_clamps_oversized_actor_range_hint() {
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
                &sync::SyncRequest {
                    topic_id: topic.id(),
                    known: BTreeSet::new(),
                    wants: BTreeSet::new(),
                    actor_range_hints: vec![sync::ActorRangeHint {
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
    fn plan_response_data_ignores_reversed_actor_range_hint() {
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
                &sync::SyncRequest {
                    topic_id: topic.id(),
                    known: BTreeSet::new(),
                    wants: BTreeSet::new(),
                    actor_range_hints: vec![sync::ActorRangeHint {
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
    fn negotiate_for_unknown_topic_returns_empty_plan() {
        let alice = node(84);
        let unknown_topic = TopicId::hash(b"never-heard-of-this");
        // A fabricated remote summary pointing at OpIds Alice doesn't have.
        // The old code would surface remote.heads as `need`/`want`, letting a
        // peer inject arbitrary OpIds into Alice's request set for a topic
        // she cannot authenticate. The plan must now be empty.
        let summary = sync::SyncSummary {
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

    #[test]
    fn pending_op_from_non_member_is_rejected() {
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

    #[cfg(feature = "fjall")]
    #[test]
    fn concurrent_publishes_on_independent_fjall_facades_do_not_fork_actor() {
        let dir = tempfile::tempdir().unwrap();
        let storage = storage::FjallStorage::open(dir.path()).unwrap();
        concurrent_publishes_on_independent_facades_do_not_fork_actor_for_storage(storage);
    }

    #[cfg(feature = "fjall")]
    #[test]
    fn concurrent_create_topic_ids_are_distinct_for_fjall() {
        let dir = tempfile::tempdir().unwrap();
        let storage = storage::FjallStorage::open(dir.path()).unwrap();
        concurrent_create_topic_ids_are_distinct_for_storage(storage);
    }

    #[cfg(feature = "fjall")]
    #[test]
    fn fjall_storage_persists_admitted_op_heads_and_topic_state_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let signer = Ed25519Signer::from_bytes(&[7; 32]);
        let config = NodeConfig {
            signer,
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        };
        let (topic_id, genesis_id, op_id, actor_id, actor_seq) = {
            let storage = storage::FjallStorage::open(dir.path()).unwrap();
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
        let storage = storage::FjallStorage::open(dir.path()).unwrap();
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
    fn fjall_ack_clears_only_satisfied_obligations() {
        let dir = tempfile::tempdir().unwrap();
        let storage = storage::FjallStorage::open(dir.path()).unwrap();
        ack_clears_only_satisfied_obligations_for_storage(storage);
    }

    #[cfg(feature = "fjall")]
    #[test]
    fn fjall_stale_ack_does_not_regress_stored_peer_ack() {
        let dir = tempfile::tempdir().unwrap();
        let storage = storage::FjallStorage::open(dir.path()).unwrap();
        stale_ack_does_not_regress_stored_peer_ack_for_storage(storage);
    }

    #[cfg(feature = "fjall")]
    #[test]
    fn fjall_pending_reconciles_after_dependency_admitted_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let storage = storage::FjallStorage::open(dir.path()).unwrap();
        pending_reconciles_after_dependency_admitted_for_storage(storage);
    }

    #[cfg(feature = "fjall")]
    #[test]
    fn fjall_ack_clear_is_durable_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let ack_signer = Ed25519Signer::from_bytes(&[97; 32]);
        let peer = ack_signer.peer_id();
        let (topic_id, unsatisfied_id) = {
            let storage = storage::FjallStorage::open(dir.path()).unwrap();
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

        let storage = storage::FjallStorage::open(dir.path()).unwrap();
        let obligations = storage.sync_obligations(&peer, &topic_id).unwrap();
        assert_eq!(obligations.len(), 1);
        assert_eq!(obligations[0].op_ids, [unsatisfied_id].into());
    }
}
