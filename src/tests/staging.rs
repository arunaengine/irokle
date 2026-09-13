//! Bootstrap staging progresses to activation without another publish: after a
//! branch replacement, concurrent fragments, a failed activation, a reopen and
//! an expired namespace, and past the old fixed staging caps.

use super::support::*;
use crate::node::ReceiveOutcome;
use crate::storage::StagedTopic;
use crate::sync::SyncData;

fn staged(outcome: ReceiveOutcome) -> StagedTopic {
    match outcome {
        ReceiveOutcome::Staged(staged) => staged,
        ReceiveOutcome::Acked { ack, .. } => panic!("expected staging, got an ack {ack:?}"),
    }
}

fn reader_node<S: Storage>(storage: S, seed: u8) -> Irokle<S> {
    Irokle::with_storage(
        storage,
        NodeConfig {
            signer: Ed25519Signer::from_bytes(&[seed; 32]),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap()
}

/// A history of `source` with `events` notes before an invitation of `reader`,
/// the invitation last.
fn late_invite(source: &Irokle, reader: PeerId, events: usize) -> (TopicId, Vec<Op>) {
    let topic = source.create_topic::<Note>(TopicConfig::default()).unwrap();
    for index in 0..events {
        topic
            .publish(Note {
                text: format!("{index}"),
            })
            .unwrap();
    }
    topic.add_peer(reader).unwrap();
    (
        topic.id(),
        oplog::topological(source.storage(), &topic.id()).unwrap(),
    )
}

/// Fragments 2 and 3 of a history whose invitation is op 3 arrive from two
/// threads once the genesis is staged. The union activates, whichever order
/// they commit in.
fn assert_concurrent_fragments<S: Storage>(storage: S) {
    let source = node(140);
    let reader = reader_node(storage, 141);
    let (topic_id, ops) = late_invite(&source, reader.peer_id(), 1);
    assert_eq!(ops.len(), 3);
    staged(
        reader
            .receive_sync_outcome(
                source.peer_id(),
                SyncData {
                    topic_id,
                    ops: vec![ops[0].clone()],
                },
            )
            .unwrap(),
    );
    let barrier = Arc::new(Barrier::new(2));
    let fragments = [ops[1].clone(), ops[2].clone()].map(|op| {
        let reader = reader.clone();
        let barrier = Arc::clone(&barrier);
        let source = source.peer_id();
        thread::spawn(move || {
            barrier.wait();
            reader.receive_sync_outcome(
                source,
                SyncData {
                    topic_id,
                    ops: vec![op],
                },
            )
        })
    });
    for fragment in fragments {
        fragment.join().unwrap().unwrap();
    }
    assert!(reader.storage().topic_state(&topic_id).unwrap().is_some());
    assert_eq!(
        reader.storage().list_op_ids(&topic_id).unwrap(),
        source.storage().list_op_ids(&topic_id).unwrap()
    );
    assert!(reader.storage().provisional_topics().unwrap().is_empty());
}

#[test]
fn memory_concurrent_fragments() {
    assert_concurrent_fragments(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_concurrent_fragments() {
    let dir = tempfile::tempdir().unwrap();
    assert_concurrent_fragments(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// The final fragment commits but its activation fails. The staged history
/// still owes an activation: finishing the bootstrap activates the topic with
/// no further data, and the ack names the whole history.
fn assert_activation_retried<S: Storage>(inner: S) {
    let storage = StaleReadStorage::new(inner);
    let source = node(142);
    let reader = reader_node(storage.clone(), 143);
    let (topic_id, ops) = late_invite(&source, reader.peer_id(), 20);
    storage
        .failed_activations
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let failed = reader.receive_sync_outcome(
        source.peer_id(),
        SyncData {
            topic_id,
            ops: ops.clone(),
        },
    );
    assert!(failed.is_err(), "{failed:?}");
    assert!(storage.topic_state(&topic_id).unwrap().is_none());
    let staged = reader
        .staged_topic(source.peer_id(), topic_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        staged.clock.get(&actor_id_for(topic_id, source.peer_id())),
        ops.len() as u64
    );
    assert!(reader.finish_bootstrap(source.peer_id(), topic_id).unwrap());
    assert_eq!(
        storage.list_op_ids(&topic_id).unwrap(),
        source.storage().list_op_ids(&topic_id).unwrap()
    );
}

#[test]
fn memory_activation_retried() {
    assert_activation_retried(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_activation_retried() {
    let dir = tempfile::tempdir().unwrap();
    assert_activation_retried(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// A reopen after the final staging commit but before its activation activates
/// the topic while the node is built, with no further data or pull.
#[cfg(feature = "fjall")]
#[test]
fn fjall_reopen_activates() {
    let dir = tempfile::tempdir().unwrap();
    let source = node(144);
    let reader_peer = Ed25519Signer::from_bytes(&[145; 32]).peer_id();
    let (topic_id, ops) = late_invite(&source, reader_peer, 30);
    {
        let storage =
            StaleReadStorage::new(crate::storage::FjallStorage::open(dir.path()).unwrap());
        storage
            .failed_activations
            .store(1, std::sync::atomic::Ordering::SeqCst);
        let reader = reader_node(storage.clone(), 145);
        for fragment in ops.chunks(8) {
            let _ = reader.receive_sync_outcome(
                source.peer_id(),
                SyncData {
                    topic_id,
                    ops: fragment.to_vec(),
                },
            );
        }
        assert!(storage.topic_state(&topic_id).unwrap().is_none());
        assert_eq!(storage.provisional_topics().unwrap().len(), 1);
    }
    let storage = crate::storage::FjallStorage::open(dir.path()).unwrap();
    let reader = reader_node(storage.clone(), 145);
    assert!(storage.topic_state(&topic_id).unwrap().is_some());
    assert!(storage.provisional_topics().unwrap().is_empty());
    assert_eq!(
        storage.list_op_ids(&topic_id).unwrap(),
        source.storage().list_op_ids(&topic_id).unwrap()
    );
    reader.open_topic::<Note>(topic_id).unwrap();
}

/// Fixed-size fragments cost the same storage work however long the staged
/// history already is: no fragment replays the prefix.
#[test]
fn fragment_work_flat() {
    let measure = |fragments: usize| {
        let source = node(146);
        let storage = MemoryStorage::new().with_staging_limits(crate::storage::StagingLimits {
            total_bytes: u64::MAX,
            source_bytes: u64::MAX,
            namespace_bytes: u64::MAX,
            ..crate::storage::StagingLimits::MEMORY
        });
        let reader = reader_node(storage.clone(), 147);
        let (topic_id, ops) = late_invite(&source, reader.peer_id(), fragments * 64);
        let history = &ops[..fragments * 64];
        for fragment in history[..history.len() - 64].chunks(64) {
            staged(
                reader
                    .receive_sync_outcome(
                        source.peer_id(),
                        SyncData {
                            topic_id,
                            ops: fragment.to_vec(),
                        },
                    )
                    .unwrap(),
            );
        }
        let before = storage.counters();
        staged(
            reader
                .receive_sync_outcome(
                    source.peer_id(),
                    SyncData {
                        topic_id,
                        ops: history[history.len() - 64..].to_vec(),
                    },
                )
                .unwrap(),
        );
        let after = storage.counters();
        (after.meta_reads - before.meta_reads)
            + (after.op_reads - before.op_reads)
            + (after.index_reads - before.index_reads)
    };
    let short = measure(16);
    let long = measure(64);
    assert!(
        long <= short + short / 2 + 64,
        "a fragment after 4x the history cost {long} reads, after 1x {short}"
    );
}

/// Invitations beyond both old fixed caps (65,536 ops and 32 MiB per session),
/// with non-inviting fragments crossing each, staged in frame-sized messages.
/// Run explicitly: `cargo test --features fjall --lib invite_beyond_caps -- --ignored`.
fn assert_invite_beyond_caps<S: Storage>(storage: S) {
    let source = node(148);
    let reader = reader_node(storage.clone(), 149);
    let topic = source.create_topic::<Note>(TopicConfig::default()).unwrap();
    for index in 0..70_000 {
        topic
            .publish(Note {
                text: format!("{index:0>500}"),
            })
            .unwrap();
    }
    topic.add_peer(reader.peer_id()).unwrap();
    let topic_id = topic.id();
    let ops = oplog::topological(source.storage(), &topic_id).unwrap();
    let bytes = ops
        .iter()
        .map(|op| crate::storage::pending_op_bytes(op).unwrap() as u64)
        .sum::<u64>();
    assert!(
        ops.len() > 65_537 && bytes > 32 * 1024 * 1024,
        "{} ops {bytes} bytes",
        ops.len()
    );
    let (history, invite) = ops.split_at(ops.len() - 1);
    let mut staged_bytes = 0;
    for message in crate::net::sync_data_messages(topic_id, history.to_vec()).unwrap() {
        let crate::sync::SyncMessage::Data(data) = message else {
            unreachable!("data messages only")
        };
        staged_bytes = staged(reader.receive_sync_outcome(source.peer_id(), data).unwrap()).bytes;
    }
    assert!(staged_bytes > 32 * 1024 * 1024);
    assert!(storage.topic_state(&topic_id).unwrap().is_none());
    match reader
        .receive_sync_outcome(
            source.peer_id(),
            SyncData {
                topic_id,
                ops: invite.to_vec(),
            },
        )
        .unwrap()
    {
        ReceiveOutcome::Acked { ack, .. } => {
            assert_eq!(ack.clock, source.storage().actor_clock(&topic_id).unwrap());
        }
        ReceiveOutcome::Staged(staged) => panic!("still staged: {staged:?}"),
    }
    assert_eq!(storage.list_op_ids(&topic_id).unwrap().len(), ops.len());
}

#[cfg(feature = "fjall")]
#[test]
#[ignore = "stages about 40 MiB of signed history, run explicitly"]
fn fjall_invite_beyond_caps() {
    let dir = tempfile::tempdir().unwrap();
    assert_invite_beyond_caps(
        crate::storage::FjallStorage::open_with_persist_mode(
            dir.path(),
            fjall::PersistMode::Buffer,
        )
        .unwrap(),
    );
}

#[test]
#[ignore = "stages about 40 MiB of signed history, run explicitly"]
fn memory_invite_beyond_caps() {
    assert_invite_beyond_caps(MemoryStorage::new().with_staging_limits(
        crate::storage::StagingLimits {
            total_bytes: 256 * 1024 * 1024,
            source_bytes: 256 * 1024 * 1024,
            namespace_bytes: 256 * 1024 * 1024,
            ..crate::storage::StagingLimits::MEMORY
        },
    ));
}

/// Two nodes over Iroh: `source` serves streams, `reader` over `storage`
/// trusts it for unknown topics.
#[cfg(feature = "iroh")]
async fn pull_pair<S: Storage>(
    storage: S,
) -> (Irokle, Arc<net::IrohNet<MemoryStorage>>, Irokle<S>) {
    let bind = || async {
        iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
            .bind()
            .await
            .unwrap()
    };
    let (source_endpoint, reader_endpoint) = (bind().await, bind().await);
    let source = Irokle::builder()
        .with_iroh_secret_key(source_endpoint.secret_key())
        .build()
        .unwrap();
    let source_net = Arc::new(net::IrohNet::new(source_endpoint, source.clone()).unwrap());
    source_net.start_accept_loop().unwrap();
    let reader = Irokle::builder()
        .with_storage(storage)
        .with_peer_whitelist(vec![source.peer_id()])
        .with_net(reader_endpoint)
        .without_auto_accept()
        .build()
        .unwrap();
    (source, source_net, reader)
}

/// Pull `topic_id` from `source_net` until it completes, within a few attempts.
#[cfg(feature = "iroh")]
async fn pull_until_done<S: Storage>(
    reader: &Irokle<S>,
    source_net: &net::IrohNet<MemoryStorage>,
    topic_id: TopicId,
) {
    let addr = super::iroh::ready_addr(source_net.endpoint()).await;
    for _ in 0..8 {
        match reader.sync_addr_now(addr.clone(), topic_id).await {
            Ok(()) => return,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("the pull failed: {error}"),
        }
    }
    panic!("the pull did not finish");
}

/// The reader staged 100 ops of one branch from the source, which then moved
/// to a smaller genesis that invites the reader at op 2. Only the reader
/// initiates: its pull replaces the staged branch instead of comparing the
/// old staged positions with the new branch's shorter clock.
#[cfg(feature = "iroh")]
async fn assert_replaced_pull<S: Storage>(storage: S) {
    let (source, source_net, reader) = pull_pair(storage).await;
    let topic_id = TopicId::hash(b"staging-replaced-pull");
    let signer = source.signer().clone();
    let actor = actor_id_for(topic_id, source.peer_id());
    let genesis = |peers: BTreeSet<PeerId>| {
        let log = oplog::Oplog::new();
        let op = log
            .create_topic_genesis(
                topic_id,
                actor,
                TopicGenesis::new(Note::TYPE_ID, peers),
                &signer,
            )
            .unwrap();
        (log, op)
    };
    let third = node(150).peer_id();
    let fourth = node(151).peer_id();
    let first = genesis([third].into());
    let second = genesis([third, fourth].into());
    let ((old_log, old), (new_log, new)) = if first.1.id > second.1.id {
        (first, second)
    } else {
        (second, first)
    };
    for index in 0..99 {
        old_log
            .create_event_op(
                topic_id,
                actor,
                EventEnvelope::encode_event(&Note {
                    text: format!("{index}"),
                })
                .unwrap(),
                &signer,
            )
            .unwrap();
    }
    new_log
        .create_control_op(
            topic_id,
            actor,
            TopicControl::AddPeer {
                peer: reader.peer_id(),
            },
            &signer,
        )
        .unwrap();
    let old_ops = oplog::topological(old_log.storage(), &topic_id).unwrap();
    let new_ops = oplog::topological(new_log.storage(), &topic_id).unwrap();
    assert_eq!((old_ops.len(), new_ops.len()), (100, 2));
    let staged = staged(
        reader
            .receive_sync_outcome(
                source.peer_id(),
                SyncData {
                    topic_id,
                    ops: old_ops,
                },
            )
            .unwrap(),
    );
    assert_eq!(staged.genesis, Some(old.id));
    oplog::Oplog::with_storage(source.storage().clone())
        .receive_ops(new_ops.clone())
        .unwrap();

    pull_until_done(&reader, &source_net, topic_id).await;
    assert_eq!(genesis_of(reader.storage(), &topic_id), Some(new.id));
    assert_eq!(
        reader.storage().list_op_ids(&topic_id).unwrap(),
        new_ops.iter().map(|op| op.id).collect()
    );
    source_net.shutdown().await;
    reader.shutdown_iroh().await;
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_replaced_pull() {
    assert_replaced_pull(MemoryStorage::new()).await;
}

#[cfg(all(feature = "iroh", feature = "fjall"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fjall_replaced_pull() {
    let dir = tempfile::tempdir().unwrap();
    assert_replaced_pull(crate::storage::FjallStorage::open(dir.path()).unwrap()).await;
}

/// The final fragment's activation fails after its staging commit. A pull
/// that finds nothing left to fetch finishes that activation instead of
/// reporting the topic unreachable.
#[cfg(feature = "iroh")]
async fn assert_pull_activates<S: Storage>(inner: S) {
    let storage = StaleReadStorage::new(inner);
    let (source, source_net, reader) = pull_pair(storage.clone()).await;
    let (topic_id, ops) = late_invite(&source, reader.peer_id(), 12);
    storage
        .failed_activations
        .store(1, std::sync::atomic::Ordering::SeqCst);
    assert!(
        reader
            .receive_sync_outcome(source.peer_id(), SyncData { topic_id, ops })
            .is_err()
    );
    assert!(storage.topic_state(&topic_id).unwrap().is_none());
    pull_until_done(&reader, &source_net, topic_id).await;
    assert_eq!(
        storage.list_op_ids(&topic_id).unwrap(),
        source.storage().list_op_ids(&topic_id).unwrap()
    );
    source_net.shutdown().await;
    reader.shutdown_iroh().await;
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_pull_activates() {
    assert_pull_activates(MemoryStorage::new()).await;
}

#[cfg(all(feature = "iroh", feature = "fjall"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fjall_pull_activates() {
    let dir = tempfile::tempdir().unwrap();
    assert_pull_activates(crate::storage::FjallStorage::open(dir.path()).unwrap()).await;
}
