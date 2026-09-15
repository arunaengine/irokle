//! Page planning authorizes a peer, picks the branch and loads records from one
//! snapshot, so a removal or reset committing mid-plan never mixes into a page.

use std::collections::BTreeMap;

use super::branch::{branches, reset_to_new};
use super::support::*;

use crate::oplog::Oplog;
use crate::sync::{ActorRangeHint, PageBudget, SyncCredit, SyncEngine, SyncRequest, SyncSummary};

fn assert_request_views<S: Storage>(storage: S) {
    let source = super::progress::reverse_chain(storage, 40);
    let actors = source
        .log
        .storage()
        .actor_clock(&source.topic_id)
        .unwrap()
        .iter()
        .take(3)
        .map(|(actor, _)| *actor)
        .chain([ActorId::from_bytes([0; 32])])
        .collect();
    source
        .log
        .storage()
        .read_snapshot(|read| {
            let full = read.topic_view(&source.topic_id, None)?.unwrap();
            for id in read.list_op_ids(&source.topic_id)? {
                let position = read.get_position(&id)?.unwrap();
                let header = read.get_header(&id)?.unwrap();
                let (observed, clock) = read.get_observation(&id)?.unwrap();
                assert_eq!(observed, header);
                assert_eq!(clock, read.get_meta(&id)?.unwrap().observed_clock);
                assert_eq!(
                    (
                        header.topic_id,
                        header.actor_id,
                        header.actor_seq,
                        header.actor_prev,
                        header.generation
                    ),
                    (
                        position.topic_id,
                        position.actor_id,
                        position.actor_seq,
                        position.actor_prev,
                        position.generation
                    )
                );
            }
            let projected = read
                .request_view(&source.topic_id, &source.reader, &actors)?
                .unwrap();
            assert_eq!(projected.genesis, full.state.genesis);
            assert_eq!(projected.epoch, full.epoch);
            assert!(projected.member);
            assert_eq!(
                projected
                    .clock
                    .iter()
                    .map(|(actor, seq)| (*actor, *seq))
                    .collect::<Vec<_>>(),
                full.clock
                    .iter()
                    .filter(|(actor, _)| actors.contains(actor))
                    .map(|(actor, seq)| (*actor, *seq))
                    .collect::<Vec<_>>()
            );
            assert!(
                !read
                    .request_view(&source.topic_id, &PeerId::from_bytes([0; 32]), &actors)?
                    .unwrap()
                    .member
            );
            assert!(
                read.request_view(&TopicId::default(), &source.reader, &actors)?
                    .is_none()
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn memory_request_views() {
    assert_request_views(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_request_views() {
    let dir = tempfile::tempdir().unwrap();
    assert_request_views(crate::FjallStorage::open(dir.path()).unwrap());
}

#[cfg(feature = "fjall")]
#[test]
fn request_snapshot_holds() {
    let branch = branches(96);
    let dir = tempfile::tempdir().unwrap();
    let storage = crate::FjallStorage::open(dir.path()).unwrap();
    Oplog::with_storage(storage.clone())
        .receive_ops(vec![branch.old.0.clone(), branch.old.1.clone()])
        .unwrap();
    let (opened, ready) = std::sync::mpsc::channel();
    let (changed, resume) = std::sync::mpsc::channel();
    let reading = thread::spawn({
        let storage = storage.clone();
        let (topic, peer, actor) = (
            branch.topic_id,
            branch.member.peer_id(),
            branch.old.1.signed.body.actor_id,
        );
        move || {
            storage
                .read_snapshot(|read| {
                    let before = read.request_view(&topic, &peer, &[actor].into())?;
                    opened.send(()).unwrap();
                    resume
                        .recv_timeout(std::time::Duration::from_secs(60))
                        .unwrap();
                    assert_eq!(read.request_view(&topic, &peer, &[actor].into())?, before);
                    Ok(())
                })
                .unwrap()
        }
    });
    ready
        .recv_timeout(std::time::Duration::from_secs(60))
        .unwrap();
    reset_to_new(&storage, &branch);
    changed.send(()).unwrap();
    reading.join().unwrap();
    storage
        .read_snapshot(|read| {
            assert_eq!(
                read.request_view(&branch.topic_id, &branch.member.peer_id(), &BTreeSet::new())?
                    .unwrap()
                    .genesis,
                branch.new.0.id
            );
            Ok(())
        })
        .unwrap();
}

/// A summary of a reader that holds only the genesis of `topic_id`.
fn genesis_summary<S: Storage>(storage: &S, topic_id: TopicId, owner: PeerId) -> SyncSummary {
    let genesis = genesis_of(storage, &topic_id).unwrap();
    let mut clock = ActorClock::new();
    clock.observe(actor_id_for(topic_id, owner), 1);
    SyncSummary {
        topic_id,
        event_type_id: Some(Note::TYPE_ID.into()),
        genesis: Some(genesis),
        fingerprint: [0; 32],
        heads: [genesis].into(),
        actor_clock: clock,
        actor_tips: BTreeMap::new(),
        staged: None,
    }
}

/// The owner pushes a page to a member while the member is removed and a later
/// event is published. The page may carry what the member was allowed before,
/// never an op selected from the state after its removal.
fn assert_removal_excluded<S: Storage>(inner: S, isolation: Isolation) {
    let storage = StaleReadStorage::new(inner);
    let owner = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: Ed25519Signer::from_bytes(&[91; 32]),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let reader = node(92).peer_id();
    let topic = owner
        .create_topic::<Note>(TopicConfig {
            initial_peers: [reader, node(93).peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    for text in ["one", "two"] {
        topic.publish(Note { text: text.into() }).unwrap();
    }
    let topic_id = topic.id();
    let before = storage.list_op_ids(&topic_id).unwrap();
    let summary = genesis_summary(&storage, topic_id, owner.peer_id());

    let planner = owner.clone();
    let budget = PageBudget::from_credit(SyncCredit::default());
    // The first topic read authorizes the member; selection reads after it.
    let (plan, _) = interleave(
        &storage,
        (GatePoint::Topic(topic_id), 0),
        isolation,
        move || planner.negotiate_page(reader, &summary, budget).unwrap(),
        move || {
            topic.remove_peer(reader).unwrap();
            topic
                .publish(Note {
                    text: "after".into(),
                })
                .unwrap();
        },
    );
    let after = storage.list_op_ids(&topic_id).unwrap();
    assert!(after.len() > before.len(), "the removal committed");
    let sent = plan.send.iter().map(|op| op.id).collect::<BTreeSet<_>>();
    assert!(
        sent.is_subset(&before),
        "a page for a removed member carried later ops: {:?}",
        sent.difference(&before).collect::<Vec<_>>()
    );
}

#[test]
fn memory_removal_excluded() {
    assert_removal_excluded(MemoryStorage::new(), Isolation::Blocks);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_removal_excluded() {
    let dir = tempfile::tempdir().unwrap();
    assert_removal_excluded(
        crate::storage::FjallStorage::open(dir.path()).unwrap(),
        Isolation::Commits,
    );
}

/// A request accepted on the old genesis pauses before its positions are read
/// while a reset installs the new branch at the same actor positions. The page
/// holds only old-branch ops or the request is refused as stale.
fn assert_reset_excluded<S: Storage>(inner: S, isolation: Isolation) {
    let branches = branches(95);
    let topic_id = branches.topic_id;
    let storage = StaleReadStorage::new(inner);
    Oplog::with_storage(storage.clone())
        .receive_ops_from_peer(
            Some(branches.author.peer_id()),
            vec![branches.old.0.clone(), branches.old.1.clone()],
        )
        .unwrap();
    let old = [branches.old.0.id, branches.old.1.id]
        .into_iter()
        .collect::<BTreeSet<_>>();
    let request = SyncRequest {
        topic_id,
        known: BTreeSet::new(),
        wants: BTreeSet::new(),
        actor_range_hints: vec![ActorRangeHint {
            actor_id: branches.old.1.signed.body.actor_id,
            from_exclusive: 0,
            to_inclusive: u64::MAX,
        }],
        genesis: Some(branches.old.0.id),
        credit: SyncCredit::default(),
        window: crate::sync::ActorWindow::default(),
    };
    let member = branches.member.peer_id();
    let new_genesis = branches.new.0.id;
    let engine = SyncEngine::new(
        Oplog::with_storage(storage.clone()),
        branches.author.peer_id(),
    );
    let writer = storage.clone();
    let page = interleave(
        &storage,
        (GatePoint::View(topic_id), 0),
        isolation,
        move || engine.response_page(member, &request, PageBudget::from_credit(request.credit)),
        move || reset_to_new(&writer, &branches),
    );
    assert_eq!(genesis_of(&storage, &topic_id), Some(new_genesis));
    match page {
        Ok(page) => {
            let served = page.ops.iter().map(|op| op.id).collect::<BTreeSet<_>>();
            assert!(
                served.is_subset(&old),
                "a page accepted on the old genesis served new-branch ops"
            );
        }
        Err(error) => assert!(matches!(error, Error::StaleIncarnation), "{error:?}"),
    }
}

#[test]
fn memory_reset_excluded() {
    assert_reset_excluded(MemoryStorage::new(), Isolation::Blocks);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_reset_excluded() {
    let dir = tempfile::tempdir().unwrap();
    assert_reset_excluded(
        crate::storage::FjallStorage::open(dir.path()).unwrap(),
        Isolation::Commits,
    );
}

/// Records a page reads inside its snapshot are counted by the backend, so a
/// snapshot path cannot report zero work.
fn assert_snapshot_counted<S: Storage>(
    storage: S,
    counters: impl Fn(&S) -> crate::CounterSnapshot,
) {
    let owner = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: Ed25519Signer::from_bytes(&[96; 32]),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let reader = node(97).peer_id();
    let topic = owner
        .create_topic::<Note>(TopicConfig {
            initial_peers: [reader].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    for index in 0..5 {
        topic
            .publish(Note {
                text: format!("{index}"),
            })
            .unwrap();
    }
    let summary = genesis_summary(&storage, topic.id(), owner.peer_id());
    let request = SyncRequest {
        topic_id: topic.id(),
        known: BTreeSet::new(),
        wants: BTreeSet::new(),
        actor_range_hints: summary
            .actor_clock
            .iter()
            .map(|(actor_id, seq)| ActorRangeHint {
                actor_id: *actor_id,
                from_exclusive: *seq,
                to_inclusive: u64::MAX,
            })
            .collect(),
        genesis: summary.genesis,
        credit: SyncCredit::default(),
        window: crate::sync::ActorWindow::default(),
    };
    let before = counters(&storage);
    let page = owner
        .response_page(reader, &request, PageBudget::from_credit(request.credit))
        .unwrap();
    let after = counters(&storage);
    assert_eq!(page.ops.len(), 5);
    assert!(
        after.op_reads - before.op_reads >= 5,
        "{before:?} {after:?}"
    );
    assert!(
        after.meta_reads - before.meta_reads >= 5,
        "{before:?} {after:?}"
    );
    assert!(
        after.index_reads - before.index_reads >= 5,
        "{before:?} {after:?}"
    );
}

#[test]
fn memory_snapshot_counted() {
    assert_snapshot_counted(MemoryStorage::new(), MemoryStorage::counters);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_snapshot_counted() {
    let dir = tempfile::tempdir().unwrap();
    assert_snapshot_counted(
        crate::storage::FjallStorage::open(dir.path()).unwrap(),
        crate::storage::FjallStorage::counters,
    );
}

/// A reader pages toward the goal it captured while the source keeps appending
/// on the same branch between pages. The captured goal still completes.
#[test]
fn appends_keep_goal() {
    let source = node(98);
    let reader = node(99);
    let topic = source
        .create_topic::<Note>(TopicConfig {
            initial_peers: [reader.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    for index in 0..20 {
        topic
            .publish(Note {
                text: format!("{index}"),
            })
            .unwrap();
    }
    let genesis = oplog::topological(source.storage(), &topic.id()).unwrap()[0].clone();
    reader
        .receive_sync_outcome(
            source.peer_id(),
            sync::SyncData {
                topic_id: topic.id(),
                ops: vec![genesis],
            },
        )
        .unwrap();
    let captured = source.sync_summary(topic.id()).unwrap();
    let mut pages = 0;
    loop {
        let mut request = reader
            .plan_sync_request(source.peer_id(), &captured)
            .unwrap();
        if request.wants.is_empty() && request.actor_range_hints.is_empty() {
            break;
        }
        request.credit.ops = 3;
        let page = source
            .response_page(
                reader.peer_id(),
                &request,
                PageBudget::from_credit(request.credit),
            )
            .unwrap();
        assert!(!page.ops.is_empty(), "page {pages} carried nothing");
        reader
            .receive_sync_outcome(
                source.peer_id(),
                sync::SyncData {
                    topic_id: topic.id(),
                    ops: page.ops,
                },
            )
            .unwrap();
        topic
            .publish(Note {
                text: format!("append {pages}"),
            })
            .unwrap();
        pages += 1;
        assert!(pages <= 8, "the captured goal kept moving");
    }
    assert!(
        reader
            .storage()
            .actor_clock(&topic.id())
            .unwrap()
            .dominates(&captured.actor_clock)
    );
}

/// Topic reads of a sync attempt before its push is planned: the digest's
/// view, the open's state read, then the plan's own view.
#[cfg(feature = "iroh")]
const PLAN_TOPIC_READS: usize = 2;

/// A real batch push from `alice` pauses after its planner authorized `bob`,
/// while `bob` is removed and a later event is published. `bob` receives
/// nothing selected from the state after its removal.
#[cfg(feature = "iroh")]
async fn assert_push_excluded<S: Storage>(inner: S, isolation: Isolation) {
    let bind = || async {
        iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .bind()
            .await
            .unwrap()
    };
    let (alice_endpoint, bob_endpoint) = (bind().await, bind().await);
    let storage = StaleReadStorage::new(inner);
    let alice = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: Ed25519Signer::from_iroh_secret_key(alice_endpoint.secret_key()),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let bob = Irokle::with_storage(
        MemoryStorage::new(),
        NodeConfig {
            signer: Ed25519Signer::from_iroh_secret_key(bob_endpoint.secret_key()),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id(), node(94).peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let topic_id = topic.id();
    let genesis = oplog::topological(&storage, &topic_id).unwrap()[0].clone();
    bob.receive_sync_outcome(
        alice.peer_id(),
        sync::SyncData {
            topic_id,
            ops: vec![genesis],
        },
    )
    .unwrap();
    for text in ["one", "two"] {
        topic.publish(Note { text: text.into() }).unwrap();
    }
    let before = storage.list_op_ids(&topic_id).unwrap();
    let bob_net = Arc::new(net::IrohNet::new(bob_endpoint, bob.clone()).unwrap());
    bob_net.start_accept_loop().unwrap();
    let bob_addr = super::iroh::ready_addr(bob_net.endpoint()).await;
    let alice_net = Arc::new(net::IrohNet::new(alice_endpoint, alice.clone()).unwrap());

    let gate = Arc::new(Gate::default());
    let release = gate.releaser();
    storage.arm_read_after(
        GatePoint::Topic(topic_id),
        PLAN_TOPIC_READS,
        Arc::clone(&gate),
    );
    let syncing = tokio::spawn({
        let alice_net = Arc::clone(&alice_net);
        async move { alice_net.sync_now(bob_addr, topic_id).await }
    });
    let arrival = Arc::clone(&gate);
    tokio::task::spawn_blocking(move || arrival.wait_arrival())
        .await
        .unwrap();
    let reader = bob.peer_id();
    let writing = std::thread::spawn(move || {
        topic.remove_peer(reader).unwrap();
        topic
            .publish(Note {
                text: "after".into(),
            })
            .unwrap();
    });
    let mut writing = Some(writing);
    if matches!(isolation, Isolation::Commits) {
        writing.take().unwrap().join().unwrap();
    }
    drop(release);
    let _ = syncing.await.unwrap();
    if let Some(writing) = writing {
        writing.join().unwrap();
    }
    assert!(storage.list_op_ids(&topic_id).unwrap().len() > before.len());
    let received = bob.storage().list_op_ids(&topic_id).unwrap();
    assert_eq!(
        received, before,
        "the push planned before the removal carried the allowed events"
    );
    assert!(
        received.is_subset(&before),
        "a removed member received later ops: {:?}",
        received.difference(&before).collect::<Vec<_>>()
    );
    alice_net.shutdown().await;
    bob_net.shutdown().await;
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_push_excluded() {
    assert_push_excluded(MemoryStorage::new(), Isolation::Blocks).await;
}

#[cfg(all(feature = "iroh", feature = "fjall"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fjall_push_excluded() {
    let dir = tempfile::tempdir().unwrap();
    assert_push_excluded(
        crate::storage::FjallStorage::open(dir.path()).unwrap(),
        Isolation::Commits,
    )
    .await;
}
