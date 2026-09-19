//! Namespace ownership, activation and staging quotas held by the backing
//! store: a retained view, a stale scan or a second facade has no effect on
//! the namespaces and topics that replaced what it saw.

use crate::tests::support::*;

use crate::node::ReceiveOutcome;
use crate::oplog::Oplog;
use crate::storage::{AdmissionEffects, ProvisionalTopic, StagingLimits, pending_op_bytes};
use crate::sync::SyncData;

const READER_SEED: u8 = 139;

pub(super) fn reader() -> PeerId {
    Ed25519Signer::from_bytes(&[READER_SEED; 32]).peer_id()
}

pub(super) fn reader_node<S: Storage>(storage: S) -> Irokle<S> {
    Irokle::with_storage(
        storage,
        NodeConfig {
            signer: Ed25519Signer::from_bytes(&[READER_SEED; 32]),
            ..NodeConfig::default()
        },
    )
    .unwrap()
}

/// A topic of `seed` with `events` notes of `text_len` characters, then an
/// invitation for the reader, oldest first.
pub(super) fn history(seed: u8, events: usize, text_len: usize) -> (Irokle, TopicId, Vec<Op>) {
    let source = node(seed);
    let topic = source.create_topic::<Note>(TopicConfig::default()).unwrap();
    for index in 0..events {
        let text = format!("{index:0>text_len$}");
        topic.publish(Note { text }).unwrap();
    }
    topic.add_peer(reader()).unwrap();
    let ops = oplog::topological(source.storage(), &topic.id()).unwrap();
    (source, topic.id(), ops)
}

pub(super) fn now() -> u64 {
    1_000
}

pub(super) fn bytes(ops: &[Op]) -> u64 {
    ops.iter()
        .map(|op| pending_op_bytes(op).unwrap() as u64)
        .sum()
}

fn owned_event(
    previous: &Op,
    signer: &impl Signer,
    released: Arc<std::sync::atomic::AtomicBool>,
) -> Op {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Payload(Vec<u8>, Arc<AtomicBool>);
    impl AsRef<[u8]> for Payload {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            self.1.store(true, Ordering::SeqCst);
        }
    }
    let payload = bytes::Bytes::from_owner(Payload(
        postcard::to_allocvec(&Note {
            text: "owned".into(),
        })
        .unwrap(),
        released,
    ));
    Op::sign(
        OpBody {
            topic_id: previous.signed.body.topic_id,
            author: signer.peer_id(),
            actor_id: previous.signed.body.actor_id,
            actor_seq: previous.signed.body.actor_seq + 1,
            actor_prev: Some(previous.id),
            deps: [previous.id].into(),
            generation: previous.signed.body.generation + 1,
            payload: TopicPayload::Event(EventEnvelope::new::<Note>(payload)),
        },
        signer,
    )
    .unwrap()
}

#[test]
fn stale_view_releases() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let released = Arc::new(AtomicBool::new(false));
    let (source, topic, ops) = history(178, 0, 0);
    let genesis = ops[0].clone();
    let op = owned_event(&genesis, source.signer(), Arc::clone(&released));
    let (id, encoded) = (op.id, postcard::to_allocvec(&op).unwrap());
    let storage = MemoryStorage::new();
    let provisional = storage
        .open_provisional(source.peer_id(), topic, genesis.id, now())
        .unwrap();
    let view = storage.provisional_store(&provisional).unwrap().unwrap();
    Oplog::with_storage(view.clone())
        .receive_ops(vec![genesis, op])
        .unwrap();
    assert!(
        released.load(Ordering::SeqCst),
        "storage must own the visible payload bytes"
    );
    let stored = view.get_op(&id).unwrap().unwrap();
    assert_eq!(postcard::to_allocvec(&stored).unwrap(), encoded);
    stored.validate().unwrap();
    drop(stored);
    assert!(
        storage.memory_usage().unwrap().reserved[&crate::storage::MemoryDomain::Operations] > 0
    );
    let current = listed(&storage, &provisional).unwrap();
    assert!(storage.discard_provisional(&current).unwrap());
    assert!(
        released.load(Ordering::SeqCst),
        "stale capability retained payload storage"
    );
    assert!(matches!(view.stored_bytes(), Err(Error::StaleIncarnation)));
    assert!(
        storage
            .memory_usage()
            .unwrap()
            .reserved
            .values()
            .all(|bytes| *bytes == 0)
    );
}

#[test]
fn failed_activation_hidden() {
    let (source, topic, ops) = history(179, 2, 32);
    let storage = MemoryStorage::new();
    let provisional = storage
        .open_provisional(source.peer_id(), topic, ops[0].id, now())
        .unwrap();
    stage(&storage, &provisional, &ops).unwrap();
    let current = listed(&storage, &provisional).unwrap();
    let view = storage.provisional_store(&current).unwrap().unwrap();
    let state = view.topic_state(&topic).unwrap().unwrap();
    let ids = (0_u32..4098)
        .map(|n| OpId::hash(n.to_le_bytes()))
        .collect::<Vec<_>>();
    let effects = AdmissionEffects {
        sync_obligations: ids
            .chunks(2049)
            .map(|part| {
                crate::storage::SyncObligation::repair(
                    reader(),
                    topic,
                    part.iter().copied().collect(),
                )
            })
            .collect(),
    };
    assert!(
        storage
            .activate_provisional(&current, &state, effects)
            .is_err()
    );
    assert!(
        storage.topic_state(&topic).unwrap().is_none(),
        "failed activation published history"
    );
    assert!(storage.all_sync_obligations().unwrap().is_empty());
    assert_eq!(
        contents(&storage, &current).0,
        ops.iter().map(|op| op.id).collect()
    );
}

#[test]
fn activation_releases_losers() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (source, topic, common) = history(180, 0, 0);
    let storage = MemoryStorage::new();
    let mut staged = Vec::new();
    for peer in [source.peer_id(), reader()] {
        let released = Arc::new(AtomicBool::new(false));
        let event = owned_event(
            common.last().unwrap(),
            source.signer(),
            Arc::clone(&released),
        );
        let provisional = storage
            .open_provisional(peer, topic, common[0].id, now())
            .unwrap();
        let view = storage.provisional_store(&provisional).unwrap().unwrap();
        let mut ops = common.clone();
        ops.push(event);
        Oplog::with_storage(view.clone()).receive_ops(ops).unwrap();
        staged.push((listed(&storage, &provisional).unwrap(), view, released));
    }
    let state = staged[0].1.topic_state(&topic).unwrap().unwrap();
    let before =
        storage.memory_usage().unwrap().reserved[&crate::storage::MemoryDomain::Operations];
    storage
        .activate_provisional(&staged[0].0, &state, AdmissionEffects::default())
        .unwrap();
    assert!(staged[0].2.load(Ordering::SeqCst));
    assert!(staged[1].2.load(Ordering::SeqCst));
    assert_eq!(
        2 * storage.memory_usage().unwrap().reserved[&crate::storage::MemoryDomain::Operations],
        before
    );
    assert_eq!(storage.list_op_ids(&topic).unwrap().len(), common.len() + 1);
    for (_, view, _) in &staged {
        assert!(matches!(view.stored_bytes(), Err(Error::StaleIncarnation)));
    }
    storage.reset_topic(&topic).unwrap();
    assert!(staged[0].2.load(Ordering::SeqCst));
    let usage = storage.memory_usage().unwrap();
    assert_eq!(
        usage.reserved[&crate::storage::MemoryDomain::Metadata],
        4096
    );
    assert_eq!(usage.reserved.values().sum::<u64>(), 4096);
}

/// Admit `ops` into the namespace of `provisional` through a fresh view.
pub(super) fn stage<S: Storage>(
    storage: &S,
    provisional: &ProvisionalTopic,
    ops: &[Op],
) -> Result<(), Error> {
    let store = storage
        .provisional_store(provisional)?
        .ok_or(Error::StaleIncarnation)?;
    admit(&store, provisional.source, ops)
}

pub(super) fn admit<S: Storage>(store: &S, source: PeerId, ops: &[Op]) -> Result<(), Error> {
    Oplog::with_storage(store.clone())
        .receive_ops_from_peer(Some(source), ops.to_vec())
        .map(drop)
}

/// The registry's current record of `provisional`'s session.
pub(super) fn listed<S: Storage>(
    storage: &S,
    provisional: &ProvisionalTopic,
) -> Option<ProvisionalTopic> {
    storage
        .provisional_topics()
        .unwrap()
        .into_iter()
        .find(|listed| listed.session == provisional.session)
}

/// What a namespace holds, as its own view and the registry report it.
pub(super) fn contents<S: Storage>(
    storage: &S,
    provisional: &ProvisionalTopic,
) -> (BTreeSet<OpId>, ActorClock, u64, Option<ProvisionalTopic>) {
    let store = storage.provisional_store(provisional).unwrap().unwrap();
    let topic_id = provisional.topic_id;
    (
        store.list_op_ids(&topic_id).unwrap(),
        store.actor_clock(&topic_id).unwrap(),
        store.stored_bytes().unwrap(),
        listed(storage, provisional),
    )
}

/// Every registry record's bytes equal what its namespace holds.
pub(super) fn assert_bytes_exact<S: Storage>(storage: &S) {
    for provisional in storage.provisional_topics().unwrap() {
        let store = storage.provisional_store(&provisional).unwrap().unwrap();
        assert_eq!(provisional.bytes, store.stored_bytes().unwrap());
    }
}

/// A view retained past the end of its session neither reads nor writes, even
/// once its slot serves another session: the replacement stays exactly as it was.
fn assert_stale_view<S: Storage>(storage: S) {
    let (first_source, first_topic, first_ops) = history(140, 20, 8);
    let (second_source, second_topic, second_ops) = history(141, 20, 8);
    let first = storage
        .open_provisional(first_source.peer_id(), first_topic, first_ops[0].id, now())
        .unwrap();
    let retained = storage.provisional_store(&first).unwrap().unwrap();
    admit(&retained, first.source, &first_ops[..10]).unwrap();
    assert!(
        storage
            .discard_provisional(&listed(&storage, &first).unwrap())
            .unwrap()
    );
    let second = storage
        .open_provisional(
            second_source.peer_id(),
            second_topic,
            second_ops[0].id,
            now(),
        )
        .unwrap();
    stage(&storage, &second, &second_ops).unwrap();
    let before = contents(&storage, &second);

    let late = admit(&retained, first.source, &first_ops[10..]);
    let (held, clock) = (retained.stored_bytes(), retained.actor_clock(&first_topic));
    assert_eq!(contents(&storage, &second), before);
    assert!(matches!(late, Err(Error::StaleIncarnation)), "{late:?}");
    assert!(matches!(held, Err(Error::StaleIncarnation)), "{held:?}");
    assert!(matches!(clock, Err(Error::StaleIncarnation)), "{clock:?}");
    let replacement = storage.provisional_store(&second).unwrap().unwrap();
    assert!(replacement.list_op_ids(&first_topic).unwrap().is_empty());
    assert_eq!(before.3.unwrap().topic_id, second_topic);
    assert_bytes_exact(&storage);
}

#[test]
fn memory_stale_view() {
    assert_stale_view(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_stale_view() {
    let dir = tempfile::tempdir().unwrap();
    assert_stale_view(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// A discard decided on an observation ends the namespace only while it is
/// unchanged: a write or a touch since then keeps it.
fn assert_observed_discard<S: Storage>(storage: S) {
    let (source, topic_id, ops) = history(142, 12, 8);
    let provisional = storage
        .open_provisional(source.peer_id(), topic_id, ops[0].id, now())
        .unwrap();
    stage(&storage, &provisional, &ops[..5]).unwrap();
    let observed = listed(&storage, &provisional).unwrap();
    stage(&storage, &provisional, &ops[5..10]).unwrap();
    assert!(!storage.discard_provisional(&observed).unwrap());
    let observed = listed(&storage, &provisional).unwrap();
    storage.touch_provisional(&observed, now() + 1).unwrap();
    assert!(!storage.discard_provisional(&observed).unwrap());
    let current = listed(&storage, &provisional).unwrap();
    assert_eq!(current.updated_ms, now() + 1);
    assert_eq!(current.bytes, bytes(&ops[..10]));
    assert!(storage.discard_provisional(&current).unwrap());
    assert!(storage.provisional_topics().unwrap().is_empty());
}

#[test]
fn memory_observed_discard() {
    assert_observed_discard(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_observed_discard() {
    let dir = tempfile::tempdir().unwrap();
    assert_observed_discard(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// A view retained across its activation writes nothing: not into the active
/// topic, and not into the slot a later session takes.
fn assert_frozen_view<S: Storage>(storage: S) {
    let (source, topic_id, ops) = history(143, 10, 8);
    let provisional = storage
        .open_provisional(source.peer_id(), topic_id, ops[0].id, now())
        .unwrap();
    let retained = storage.provisional_store(&provisional).unwrap().unwrap();
    admit(&retained, provisional.source, &ops).unwrap();
    let state = retained.topic_state(&topic_id).unwrap().unwrap();
    storage
        .activate_provisional(&provisional, &state, AdmissionEffects::default())
        .unwrap();
    let appended = source
        .open_topic::<Note>(topic_id)
        .unwrap()
        .publish(Note {
            text: "late".into(),
        })
        .unwrap()
        .meta
        .op_id;
    let appended = source.storage().get_op(&appended).unwrap().unwrap();
    let late = admit(
        &retained,
        provisional.source,
        std::slice::from_ref(&appended),
    );
    assert!(storage.get_op(&appended.id).unwrap().is_none());
    assert_eq!(storage.topic_state(&topic_id).unwrap(), Some(state));

    let (other_source, other_topic, other_ops) = history(144, 3, 8);
    let other = storage
        .open_provisional(other_source.peer_id(), other_topic, other_ops[0].id, now())
        .unwrap();
    let fresh = storage.provisional_store(&other).unwrap().unwrap();
    assert_eq!(fresh.stored_bytes().unwrap(), 0);
    assert!(fresh.ready_pending_ops().unwrap().is_empty());
    assert_eq!(listed(&storage, &other).unwrap().bytes, 0);
    assert!(matches!(late, Err(Error::StaleIncarnation)), "{late:?}");
}

#[test]
fn memory_frozen_view() {
    assert_frozen_view(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_frozen_view() {
    let dir = tempfile::tempdir().unwrap();
    assert_frozen_view(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// The total and source limits hold where staged bytes commit: a view obtained
/// before another namespace committed is refused against that commit, stores
/// nothing, and fits once the other namespace is discarded.
fn assert_commit_quota<S: Storage>(open: impl Fn(StagingLimits) -> S) {
    let (a_source, a_topic, a_ops) = history(145, 12, 256);
    let (b_source, b_topic, b_ops) = history(146, 12, 256);
    let (a_ops, b_ops) = (&a_ops[..7], &b_ops[..7]);
    let both = bytes(a_ops) + bytes(b_ops);
    let cases = [
        (
            StagingLimits {
                total_bytes: both - 1,
                ..StagingLimits::MEMORY
            },
            b_source.peer_id(),
        ),
        (
            StagingLimits {
                source_bytes: both - 1,
                ..StagingLimits::MEMORY
            },
            a_source.peer_id(),
        ),
    ];
    for (limits, b_peer) in cases {
        let storage = open(limits);
        let a = storage
            .open_provisional(a_source.peer_id(), a_topic, a_ops[0].id, now())
            .unwrap();
        let b = storage
            .open_provisional(b_peer, b_topic, b_ops[0].id, now())
            .unwrap();
        let a_view = storage.provisional_store(&a).unwrap().unwrap();
        stage(&storage, &b, b_ops).unwrap();
        let refused = admit(&a_view, a.source, a_ops);
        assert!(
            matches!(&refused, Err(Error::StagingCapacity(_))),
            "{refused:?}"
        );
        assert_eq!(a_view.stored_bytes().unwrap(), 0);
        assert!(a_view.list_op_ids(&a_topic).unwrap().is_empty());
        assert_bytes_exact(&storage);
        let total: u64 = storage
            .provisional_topics()
            .unwrap()
            .iter()
            .map(|provisional| provisional.bytes)
            .sum();
        assert_eq!(total, bytes(b_ops));

        // A duplicate charges nothing again; the discarded room is reusable.
        stage(&storage, &b, b_ops).unwrap();
        assert_eq!(listed(&storage, &b).unwrap().bytes, bytes(b_ops));
        assert!(
            storage
                .discard_provisional(&listed(&storage, &b).unwrap())
                .unwrap()
        );
        admit(&a_view, a.source, a_ops).unwrap();
        assert_eq!(listed(&storage, &a).unwrap().bytes, bytes(a_ops));
        assert_bytes_exact(&storage);
    }
}

#[test]
fn memory_commit_quota() {
    assert_commit_quota(|limits| MemoryStorage::new().with_staging_limits(limits));
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_commit_quota() {
    let dirs = std::sync::Mutex::new(Vec::new());
    assert_commit_quota(|limits| {
        let dir = tempfile::tempdir().unwrap();
        let storage = crate::storage::FjallStorage::open(dir.path())
            .unwrap()
            .with_staging_limits(limits);
        dirs.lock().unwrap().push(dir);
        storage
    });
}

/// A fragment that fits once weaker stagings of its topic are discarded makes
/// progress; one that no discard could fit is refused and discards nothing.
fn assert_reclaim_fits<S: Storage>(open: impl Fn(StagingLimits) -> S) {
    let (_, topic_id, ops) = history(147, 40, 64);
    let strong = PeerId::hash(b"reclaim-strong");
    let middle = PeerId::hash(b"reclaim-middle");
    let weak = PeerId::hash(b"reclaim-weak");
    let limit = bytes(&ops[..23]);
    let (held, weak_bytes, middle_bytes) = (bytes(&ops[..11]), bytes(&ops[..2]), bytes(&ops[..8]));
    let incoming = bytes(&ops[11..23]);
    assert!(held + weak_bytes + middle_bytes <= limit);
    assert!(held + middle_bytes + incoming > limit);
    let receive = |reader: &Irokle<S>, source: PeerId, ops: &[Op]| {
        reader.receive_sync_outcome(
            source,
            SyncData {
                topic_id,
                ops: ops.to_vec(),
            },
        )
    };
    let staged = |outcome: Result<ReceiveOutcome, Error>| match outcome {
        Ok(ReceiveOutcome::Staged(staged)) => staged,
        other => panic!("expected staging, got {other:?}"),
    };

    let storage = open(StagingLimits {
        total_bytes: limit,
        ..StagingLimits::MEMORY
    });
    let reader = reader_node(storage.clone());
    staged(receive(&reader, strong, &ops[..11]));
    staged(receive(&reader, weak, &ops[..2]));
    staged(receive(&reader, middle, &ops[..8]));
    let progressed = staged(receive(&reader, strong, &ops[11..23]));
    assert_eq!(progressed.bytes, limit);
    let left = storage.provisional_topics().unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!((left[0].source, left[0].bytes), (strong, limit));
    assert_bytes_exact(&storage);

    let storage = open(StagingLimits {
        total_bytes: limit - 1,
        ..StagingLimits::MEMORY
    });
    let reader = reader_node(storage.clone());
    staged(receive(&reader, strong, &ops[..11]));
    staged(receive(&reader, weak, &ops[..2]));
    staged(receive(&reader, middle, &ops[..8]));
    let refused = receive(&reader, strong, &ops[11..23]);
    assert!(
        matches!(refused, Err(Error::StagingCapacity(_))),
        "{refused:?}"
    );
    assert_eq!(storage.provisional_topics().unwrap().len(), 3);
    assert_bytes_exact(&storage);
}

#[test]
fn memory_reclaim_fits() {
    assert_reclaim_fits(|limits| MemoryStorage::new().with_staging_limits(limits));
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_reclaim_fits() {
    let dirs = std::sync::Mutex::new(Vec::new());
    assert_reclaim_fits(|limits| {
        let dir = tempfile::tempdir().unwrap();
        let storage = crate::storage::FjallStorage::open(dir.path())
            .unwrap()
            .with_staging_limits(limits);
        dirs.lock().unwrap().push(dir);
        storage
    });
}

/// Two nodes over one store, each checking staging usage before the other
/// commits: the fragment committing second is refused, and staging never holds
/// more than the total.
fn assert_raced_quota<S: Storage>(open: impl Fn(StagingLimits) -> S) {
    let (a_source, a_topic, a_ops) = history(148, 12, 256);
    let (b_source, b_topic, b_ops) = history(149, 12, 256);
    let (a_ops, b_ops) = (a_ops[..7].to_vec(), b_ops[..7].to_vec());
    let limit = bytes(&a_ops).max(bytes(&b_ops)) + 64;
    let storage = StaleReadStorage::new(open(StagingLimits {
        total_bytes: limit,
        ..StagingLimits::MEMORY
    }));
    let first = reader_node(storage.clone());
    let second = reader_node(storage.clone());
    let gate = Arc::new(Gate::default());
    let release = gate.releaser();
    storage.arm_read(GatePoint::Admit(a_topic), Arc::clone(&gate));
    let racing = thread::spawn(move || {
        first.receive_sync_outcome(
            a_source.peer_id(),
            SyncData {
                topic_id: a_topic,
                ops: a_ops,
            },
        )
    });
    gate.wait_arrival();
    let committed = second
        .receive_sync_outcome(
            b_source.peer_id(),
            SyncData {
                topic_id: b_topic,
                ops: b_ops.clone(),
            },
        )
        .unwrap();
    assert!(matches!(committed, ReceiveOutcome::Staged(_)));
    drop(release);
    let refused = racing.join().unwrap();
    assert!(
        matches!(refused, Err(Error::StagingCapacity(_))),
        "{refused:?}"
    );
    let held = storage.provisional_topics().unwrap();
    let total: u64 = held.iter().map(|provisional| provisional.bytes).sum();
    assert_eq!(total, bytes(&b_ops));
    assert!(total <= limit);
    assert_bytes_exact(&storage.inner);
}

#[test]
fn memory_raced_quota() {
    assert_raced_quota(|limits| MemoryStorage::new().with_staging_limits(limits));
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_raced_quota() {
    let dir = tempfile::tempdir().unwrap();
    assert_raced_quota(|limits| {
        crate::storage::FjallStorage::open(dir.path())
            .unwrap()
            .with_staging_limits(limits)
    });
}

/// Expiry that found a namespace idle keeps it when the namespace is written
/// before the discard: the written fragment stays staged.
fn assert_raced_expiry<S: Storage>(storage: S) {
    let (source, topic_id, ops) = history(150, 12, 8);
    let storage = StaleReadStorage::new(storage);
    let reader = reader_node(storage.clone());
    let receive = |ops: &[Op]| {
        reader
            .receive_sync_outcome(
                source.peer_id(),
                SyncData {
                    topic_id,
                    ops: ops.to_vec(),
                },
            )
            .unwrap()
    };
    assert!(matches!(receive(&ops[..5]), ReceiveOutcome::Staged(_)));
    let gate = Arc::new(Gate::default());
    let release = gate.releaser();
    storage.arm_read(GatePoint::Discard(topic_id), Arc::clone(&gate));
    let expiring = thread::spawn({
        let reader = reader.clone();
        move || reader.expire_bootstraps(u64::MAX)
    });
    gate.wait_arrival();
    let ReceiveOutcome::Staged(written) = receive(&ops[5..10]) else {
        panic!("expected staging");
    };
    drop(release);
    expiring.join().unwrap().unwrap();
    let kept = reader
        .staged_topic(source.peer_id(), topic_id)
        .unwrap()
        .expect("a written namespace is not expired");
    assert_eq!(kept, written);
    assert_eq!(kept.bytes, bytes(&ops[..10]));
}

#[test]
fn memory_raced_expiry() {
    assert_raced_expiry(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_raced_expiry() {
    let dir = tempfile::tempdir().unwrap();
    assert_raced_expiry(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

#[cfg(feature = "fjall")]
pub(super) mod fjall {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::storage::{FjallStorage, Hook, TopicState};
    use crate::tests::ownership::*;

    /// Pause the `nth` arrival at `point`, counting from zero, on `gate`.
    pub(in crate::tests) fn pause_at(
        storage: &FjallStorage,
        point: Hook,
        nth: usize,
        gate: &Arc<Gate>,
    ) {
        let seen = AtomicUsize::new(0);
        let gate = Arc::clone(gate);
        storage.set_hook(move |at| {
            if at == point && seen.fetch_add(1, Ordering::SeqCst) == nth {
                gate.pass();
            }
            Ok(())
        });
    }

    /// Staged and clearing bytes together stay within the total, and every
    /// clearing slot is charged exactly what its keyspace still counts.
    fn assert_retained(storage: &FjallStorage, limits: &StagingLimits) {
        let slots = storage.slot_bytes().unwrap();
        let retained = slots.iter().map(|(_, counted, _)| counted).sum::<u64>();
        assert!(retained <= limits.total_bytes, "{retained} bytes retained");
        for (clearing, counted, charge) in slots {
            assert_eq!(charge, clearing.then_some(counted));
        }
    }

    /// Ended-session bytes remain charged while a slot clears. Across failed deletion and
    /// reopen, two facades keep admitting into open namespaces while staged plus clearing bytes
    /// stay within the total; successful deletion releases the slot and refused history stages.
    #[test]
    fn fjall_clearing_charged() {
        let histories = (0..3)
            .map(|index| history(190 + index, 6, 256))
            .collect::<Vec<_>>();
        let one = bytes(&histories[0].2);
        let limits = StagingLimits {
            total_bytes: 2 * one + one / 4,
            ..StagingLimits::MEMORY
        };
        let dir = tempfile::tempdir().unwrap();
        let open = || {
            FjallStorage::open(dir.path())
                .unwrap()
                .with_staging_limits(limits)
        };
        let fail_deletes = |storage: &FjallStorage| {
            storage.set_hook(|point| match point {
                Hook::DeleteChunk => Err(Error::Storage("deletion fails".into())),
                _ => Ok(()),
            });
        };
        let first = open();
        let second = first.clone();
        let namespaces = histories
            .iter()
            .map(|(source, topic_id, ops)| {
                first
                    .open_provisional(source.peer_id(), *topic_id, ops[0].id, now())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        stage(&first, &namespaces[0], &histories[0].2).unwrap();
        assert_retained(&first, &limits);
        fail_deletes(&first);
        let ended = listed(&second, &namespaces[0]).unwrap();
        assert!(second.discard_provisional(&ended).is_err());
        assert_retained(&first, &limits);
        let half = histories[1].2.len() / 2;
        stage(&second, &namespaces[1], &histories[1].2[..half]).unwrap();
        assert_retained(&first, &limits);
        let refused = stage(&first, &namespaces[2], &histories[2].2);
        assert!(
            matches!(refused, Err(Error::StagingCapacity(_))),
            "{refused:?}"
        );
        assert_retained(&first, &limits);
        drop((first, second));

        let reopened = open();
        fail_deletes(&reopened);
        assert_retained(&reopened, &limits);
        let refused = stage(&reopened, &namespaces[2], &histories[2].2);
        assert!(
            matches!(refused, Err(Error::StagingCapacity(_))),
            "{refused:?}"
        );
        let clearing = reopened.slot_bytes().unwrap();
        assert_eq!(
            clearing.iter().filter(|(clearing, _, _)| *clearing).count(),
            1
        );

        reopened.set_hook(|_| Ok(()));
        let (source, topic_id, ops) = &histories[0];
        let restarted = reopened
            .open_provisional(source.peer_id(), *topic_id, ops[0].id, now())
            .unwrap();
        assert!(
            reopened
                .slot_bytes()
                .unwrap()
                .iter()
                .all(|(clearing, _, _)| !clearing)
        );
        stage(&reopened, &namespaces[2], &histories[2].2).unwrap();
        assert_retained(&reopened, &limits);
        assert_bytes_exact(&reopened);
        assert_eq!(listed(&reopened, &restarted).unwrap().bytes, 0);
    }

    /// Nothing of an unpublished topic is visible to a root or snapshot read.
    pub(in crate::tests) fn assert_hidden(
        storage: &FjallStorage,
        topic_id: TopicId,
        actor: ActorId,
        ops: &[Op],
    ) {
        assert!(storage.topic_state(&topic_id).unwrap().is_none());
        assert!(storage.topic_view(&topic_id, None).unwrap().is_none());
        assert!(
            storage
                .list_topics()
                .unwrap()
                .iter()
                .all(|topic| topic.topic_id != topic_id)
        );
        assert!(storage.list_ops(&topic_id).unwrap().is_empty());
        assert!(storage.list_op_ids(&topic_id).unwrap().is_empty());
        assert!(storage.heads(&topic_id).unwrap().is_empty());
        assert!(storage.actor_clock(&topic_id).unwrap().is_empty());
        assert!(storage.actor_tip(&topic_id, &actor).unwrap().is_none());
        assert!(storage.actor_index(&topic_id, &actor, 1).unwrap().is_none());
        assert!(
            storage
                .actor_range(&topic_id, &actor, 0, 16)
                .unwrap()
                .is_empty()
        );
        for op in [&ops[0], &ops[ops.len() / 2], &ops[ops.len() - 2]] {
            assert!(storage.get_op(&op.id).unwrap().is_none());
            assert!(storage.get_meta(&op.id).unwrap().is_none());
            assert!(!storage.dep_resolvable(&op.id).unwrap());
            assert!(storage.children(&op.id).unwrap().is_empty());
        }
        storage
            .read_snapshot(|read| {
                assert!(read.topic_view(&topic_id, None)?.is_none());
                assert!(read.list_op_ids(&topic_id)?.is_empty());
                assert!(read.actor_range(&topic_id, &actor, 0, 16)?.is_empty());
                for op in ops {
                    assert!(read.get_op(&op.id)?.is_none());
                    assert!(read.get_meta(&op.id)?.is_none());
                    assert!(!read.dep_resolvable(&op.id)?);
                }
                Ok(())
            })
            .unwrap();
    }

    /// Stage `ops` into a new namespace of `source` and return it with its state.
    pub(in crate::tests) fn staged(
        storage: &FjallStorage,
        source: PeerId,
        topic_id: TopicId,
        ops: &[Op],
    ) -> (ProvisionalTopic, TopicState) {
        let provisional = storage
            .open_provisional(source, topic_id, ops[0].id, now())
            .unwrap();
        stage(storage, &provisional, ops).unwrap();
        let store = storage.provisional_store(&provisional).unwrap().unwrap();
        let state = store.topic_state(&topic_id).unwrap().unwrap();
        (listed(storage, &provisional).unwrap(), state)
    }

    /// Two passes clear one ended slot while another session takes it: the late
    /// pass's delete does nothing to the new session.
    #[test]
    fn fjall_stale_clearing() {
        stale_clearing(Hook::DeleteChunk);
    }

    #[test]
    fn snapshot_stale_clearing() {
        stale_clearing(Hook::ClearingRead);
    }

    fn stale_clearing(boundary: Hook) {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path()).unwrap();
        let (first_source, first_topic, first_ops) = history(151, 30, 64);
        let (second_source, second_topic, second_ops) = history(152, 30, 64);
        let (first, _) = staged(&storage, first_source.peer_id(), first_topic, &first_ops);
        let gate = Arc::new(Gate::default());
        let release = gate.releaser();
        pause_at(&storage, boundary, 0, &gate);
        let late = thread::spawn({
            let storage = storage.clone();
            move || storage.discard_provisional(&first)
        });
        gate.wait_arrival();
        // This open clears and releases the slot, then takes it.
        let (second, _) = staged(&storage, second_source.peer_id(), second_topic, &second_ops);
        let before = contents(&storage, &second);
        drop(release);
        assert!(late.join().unwrap().unwrap());
        assert_eq!(contents(&storage, &second), before);
        assert_eq!(before.2, bytes(&second_ops));
        assert_bytes_exact(&storage);
    }

    /// A view write that read its session before the session ended and its slot
    /// was taken fails as stale instead of committing into the new session.
    #[test]
    fn fjall_stale_commit() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path()).unwrap();
        let (first_source, first_topic, first_ops) = history(153, 20, 64);
        let (second_source, second_topic, second_ops) = history(154, 20, 64);
        let (first, _) = staged(
            &storage,
            first_source.peer_id(),
            first_topic,
            &first_ops[..10],
        );
        let retained = storage.provisional_store(&first).unwrap().unwrap();
        let gate = Arc::new(Gate::default());
        let release = gate.releaser();
        pause_at(&storage, Hook::NamespaceCommit, 0, &gate);
        let writing = thread::spawn({
            let rest = first_ops[10..].to_vec();
            move || admit(&retained, first_source.peer_id(), &rest)
        });
        gate.wait_arrival();
        assert!(
            storage
                .discard_provisional(&listed(&storage, &first).unwrap())
                .unwrap()
        );
        let (second, _) = staged(&storage, second_source.peer_id(), second_topic, &second_ops);
        let before = contents(&storage, &second);
        drop(release);
        let late = writing.join().unwrap();
        assert_eq!(contents(&storage, &second), before);
        assert!(matches!(late, Err(Error::StaleIncarnation)), "{late:?}");
        assert_bytes_exact(&storage);
    }

    /// Copies of an activation stay invisible until its publication, to root
    /// reads and to a snapshot opened before the publication and read after it.
    #[test]
    fn fjall_hidden_activation() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path()).unwrap();
        let (source, topic_id, ops) = history(155, 800, 8);
        let actor = actor_id_for(topic_id, source.peer_id());
        let (provisional, state) = staged(&storage, source.peer_id(), topic_id, &ops);
        let gate = Arc::new(Gate::default());
        let release = gate.releaser();
        pause_at(&storage, Hook::Publish, 0, &gate);
        let activating = thread::spawn({
            let storage = storage.clone();
            let state = state.clone();
            move || storage.activate_provisional(&provisional, &state, AdmissionEffects::default())
        });
        gate.wait_arrival();
        assert_hidden(&storage, topic_id, actor, &ops);
        let opened = Arc::new(Barrier::new(2));
        let published = Arc::new(Barrier::new(2));
        let reading = thread::spawn({
            let storage = storage.clone();
            let (opened, published) = (Arc::clone(&opened), Arc::clone(&published));
            let ops = ops.clone();
            move || {
                storage
                    .read_snapshot(|read| {
                        opened.wait();
                        published.wait();
                        assert!(read.topic_view(&topic_id, None)?.is_none());
                        assert!(read.list_op_ids(&topic_id)?.is_empty());
                        assert!(read.get_meta(&ops[1].id)?.is_none());
                        Ok(())
                    })
                    .unwrap();
            }
        });
        opened.wait();
        drop(release);
        activating.join().unwrap().unwrap();
        published.wait();
        reading.join().unwrap();
        assert_eq!(storage.topic_state(&topic_id).unwrap(), Some(state));
        assert_eq!(
            storage.list_op_ids(&topic_id).unwrap(),
            source.storage().list_op_ids(&topic_id).unwrap()
        );
    }

    /// An activation paused before its copy while a second facade publishes the
    /// same session, its slot still uncleared, and the topic advances writes
    /// nothing when it resumes.
    #[test]
    fn fjall_late_copy() {
        late_copy(Hook::CopyChunk);
    }

    #[test]
    fn snapshot_late_copy() {
        late_copy(Hook::CopyRead);
    }

    fn late_copy(boundary: Hook) {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path()).unwrap();
        let other = storage.clone();
        let (source, topic_id, ops) = history(156, 30, 8);
        let actor = actor_id_for(topic_id, source.peer_id());
        let (provisional, state) = staged(&storage, source.peer_id(), topic_id, &ops);
        let gate = Arc::new(Gate::default());
        let release = gate.releaser();
        let copies = AtomicUsize::new(0);
        storage.set_hook({
            let gate = Arc::clone(&gate);
            move |at| match at {
                at if at == boundary && copies.fetch_add(1, Ordering::SeqCst) == 0 => {
                    gate.pass();
                    Ok(())
                }
                Hook::DeleteChunk => Err(Error::Storage("slot kept for the late copy".into())),
                _ => Ok(()),
            }
        });
        let late = thread::spawn({
            let (provisional, state) = (provisional.clone(), state.clone());
            move || storage.activate_provisional(&provisional, &state, AdmissionEffects::default())
        });
        gate.wait_arrival();
        let published =
            other.activate_provisional(&provisional, &state, AdmissionEffects::default());
        assert!(published.is_err(), "the slot was not cleared");
        assert_eq!(other.topic_state(&topic_id).unwrap(), Some(state.clone()));
        let appended = source
            .open_topic::<Note>(topic_id)
            .unwrap()
            .publish(Note {
                text: "after".into(),
            })
            .unwrap()
            .meta
            .op_id;
        let appended = source.storage().get_op(&appended).unwrap().unwrap();
        admit(&other, source.peer_id(), &[appended]).unwrap();
        let view = |storage: &FjallStorage| {
            (
                storage.topic_view(&topic_id, None).unwrap(),
                storage.list_op_ids(&topic_id).unwrap(),
                storage.actor_tip(&topic_id, &actor).unwrap(),
                storage.actor_range(&topic_id, &actor, 0, 64).unwrap(),
            )
        };
        let before = view(&other);
        drop(release);
        let resumed = late.join().unwrap();
        assert_eq!(view(&other), before);
        assert!(
            matches!(resumed, Err(Error::AdmissionConflict)),
            "{resumed:?}"
        );
        assert_eq!(before.2.unwrap().0, ops.len() as u64 + 1);
    }

    /// While one session holds a topic's activation claim, another session of
    /// that topic, on another branch, cannot claim or copy; the claimant
    /// publishes its own branch and ends the other.
    #[test]
    fn fjall_competing_claim() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path()).unwrap();
        let topic_id = TopicId::hash(b"competing-claim");
        let side = |seed: u8| {
            let (_, _, genesis, event) =
                forked_side(MemoryStorage::new(), topic_id, seed, [reader()], "side");
            vec![genesis, event]
        };
        let (winner_ops, loser_ops) = (side(157), side(158));
        let winner_source = PeerId::hash(b"claim-winner");
        let loser_source = PeerId::hash(b"claim-loser");
        let (winner, winner_state) = staged(&storage, winner_source, topic_id, &winner_ops);
        let (loser, loser_state) = staged(&storage, loser_source, topic_id, &loser_ops);
        let gate = Arc::new(Gate::default());
        let release = gate.releaser();
        pause_at(&storage, Hook::CopyChunk, 0, &gate);
        let claiming = thread::spawn({
            let storage = storage.clone();
            let state = winner_state.clone();
            move || storage.activate_provisional(&winner, &state, AdmissionEffects::default())
        });
        gate.wait_arrival();
        let refused =
            storage.activate_provisional(&loser, &loser_state, AdmissionEffects::default());
        let loser_listed = listed(&storage, &loser);
        drop(release);
        let claimed = claiming.join().unwrap();
        assert_eq!(storage.topic_state(&topic_id).unwrap(), Some(winner_state));
        assert_eq!(
            storage.list_op_ids(&topic_id).unwrap(),
            winner_ops.iter().map(|op| op.id).collect()
        );
        assert!(storage.get_op(&loser_ops[1].id).unwrap().is_none());
        assert!(storage.provisional_topics().unwrap().is_empty());
        assert!(
            matches!(refused, Err(Error::AdmissionConflict)),
            "{refused:?}"
        );
        assert!(!loser_listed.unwrap().activating);
        claimed.unwrap();
    }

    /// A failure at each step of an activation and its reclamation, then a
    /// reopen, leaves either a hidden resumable namespace or the whole topic,
    /// and completing through the same calls leaves an empty reusable slot.
    #[test]
    fn fjall_activation_crashes() {
        let (source, topic_id, ops) = history(159, 1200, 8);
        let actor = actor_id_for(topic_id, source.peer_id());
        let steps = [
            (Hook::Claimed, 0),
            (Hook::CopyChunk, 0),
            (Hook::CopyChunk, 1),
            (Hook::Publish, 0),
            (Hook::DeleteChunk, 0),
            (Hook::ReleaseSlot, 0),
        ];
        for (point, nth) in steps {
            let dir = tempfile::tempdir().unwrap();
            let state = {
                let storage = FjallStorage::open(dir.path()).unwrap();
                let (provisional, state) = staged(&storage, source.peer_id(), topic_id, &ops);
                let seen = AtomicUsize::new(0);
                storage.set_hook(move |at| {
                    if at == point && seen.fetch_add(1, Ordering::SeqCst) == nth {
                        return Err(Error::Storage("injected crash".into()));
                    }
                    Ok(())
                });
                let crashed =
                    storage.activate_provisional(&provisional, &state, AdmissionEffects::default());
                assert!(crashed.is_err(), "{point:?} {nth}");
                state
            };
            let storage = FjallStorage::open(dir.path()).unwrap();
            if storage.topic_state(&topic_id).unwrap().is_none() {
                assert_hidden(&storage, topic_id, actor, &ops);
                let listed = storage.provisional_topics().unwrap();
                assert_eq!(listed.len(), 1, "{point:?} {nth}");
                // Every failing step comes after the claim committed.
                assert!(listed[0].activating, "{point:?} {nth}");
                storage
                    .activate_provisional(&listed[0], &state, AdmissionEffects::default())
                    .unwrap();
            }
            assert_eq!(storage.topic_state(&topic_id).unwrap(), Some(state));
            assert_eq!(
                storage.list_op_ids(&topic_id).unwrap(),
                source.storage().list_op_ids(&topic_id).unwrap()
            );
            assert!(storage.provisional_topics().unwrap().is_empty());
            let (other_source, other_topic, other_ops) = history(160, 2, 8);
            let other = storage
                .open_provisional(other_source.peer_id(), other_topic, other_ops[0].id, now())
                .unwrap();
            let fresh = storage.provisional_store(&other).unwrap().unwrap();
            assert_eq!(fresh.stored_bytes().unwrap(), 0, "{point:?} {nth}");
            assert!(fresh.list_op_ids(&topic_id).unwrap().is_empty());
        }
    }

    /// Registry bytes survive a reopen, and a failed clearing pass leaves the
    /// slot to the next pass instead of to a new session.
    #[test]
    fn fjall_reopen_accounting() {
        let dir = tempfile::tempdir().unwrap();
        let (source, topic_id, ops) = history(161, 30, 64);
        let (other_source, other_topic, other_ops) = history(162, 30, 64);
        let first = {
            let storage = FjallStorage::open(dir.path()).unwrap();
            let (first, _) = staged(&storage, source.peer_id(), topic_id, &ops[..20]);
            let seen = AtomicUsize::new(0);
            storage.set_hook(move |at| {
                if at == Hook::DeleteChunk && seen.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(Error::Storage("injected delete failure".into()));
                }
                Ok(())
            });
            assert!(storage.discard_provisional(&first).is_err());
            first
        };
        let storage = FjallStorage::open(dir.path()).unwrap();
        assert!(listed(&storage, &first).is_none());
        let (second, _) = staged(&storage, other_source.peer_id(), other_topic, &other_ops);
        assert_eq!(contents(&storage, &second).2, bytes(&other_ops));
        drop(storage);
        let storage = FjallStorage::open(dir.path()).unwrap();
        let reopened = listed(&storage, &second).unwrap();
        assert_eq!(reopened.bytes, bytes(&other_ops));
        assert_bytes_exact(&storage);
    }

    #[test]
    fn clearing_conflict_isolated() {
        clearing_isolated(false);
    }

    #[test]
    fn clearing_buffer_isolated() {
        clearing_isolated(true);
    }

    fn clearing_isolated(buffer: bool) {
        let dir = tempfile::tempdir().unwrap();
        let limits = StagingLimits {
            namespaces: 2,
            ..StagingLimits::MEMORY
        };
        let storage = FjallStorage::open(dir.path())
            .unwrap()
            .with_staging_limits(limits);
        let (source, topic, ops) = history(163, 2, 32);
        let (old, _) = staged(&storage, source.peer_id(), topic, &ops);
        let charged = bytes(&ops);
        storage.set_hook(move |at| {
            if at == Hook::DeleteChunk {
                Err(if buffer {
                    Error::StorageBuffer {
                        required: 2,
                        limit: 1,
                    }
                } else {
                    Error::AdmissionConflict
                })
            } else {
                Ok(())
            }
        });
        let error = storage.discard_provisional(&old).unwrap_err();
        assert!(if buffer {
            matches!(error, Error::StorageBuffer { .. })
        } else {
            matches!(error, Error::AdmissionConflict)
        });
        let other = storage.clone();
        let (source, topic, ops) = history(164, 2, 32);
        let fresh = other
            .open_provisional(source.peer_id(), topic, ops[0].id, now())
            .unwrap();
        stage(&other, &fresh, &ops).unwrap();
        let slots = storage.slot_bytes().unwrap();
        assert_eq!(slots.len(), 2);
        assert!(slots.contains(&(true, charged, Some(charged))));
        assert_retained(&storage, &limits);
        assert!(storage.provisional_store(&old).unwrap().is_none());
        storage.set_hook(|_| Ok(()));
        let restarted = storage
            .open_provisional(old.source, old.topic_id, old.genesis, now())
            .unwrap();
        assert_ne!(restarted.session, old.session);
        assert!(
            storage
                .slot_bytes()
                .unwrap()
                .iter()
                .all(|(clearing, _, _)| !clearing)
        );
        assert_eq!(contents(&other, &fresh).2, bytes(&ops));
    }

    #[test]
    fn clearing_fault_global() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path()).unwrap();
        let (source, topic, ops) = history(165, 2, 32);
        let (old, _) = staged(&storage, source.peer_id(), topic, &ops);
        storage.set_hook(|at| {
            if at == Hook::DeleteChunk {
                Err(Error::Fjall(::fjall::Error::Poisoned))
            } else {
                Ok(())
            }
        });
        assert!(storage.discard_provisional(&old).is_err());
        let before = storage.slot_bytes().unwrap();
        let (source, topic, ops) = history(166, 2, 32);
        let refused = storage.open_provisional(source.peer_id(), topic, ops[0].id, now());
        assert!(matches!(
            refused,
            Err(Error::Fjall(::fjall::Error::Poisoned))
        ));
        assert_eq!(storage.slot_bytes().unwrap(), before);
        assert!(storage.provisional_topics().unwrap().is_empty());
    }
}
