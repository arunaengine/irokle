//! Namespace ownership, activation and staging quotas held by the backing
//! store: a retained view, a stale scan or a second facade has no effect on
//! the namespaces and topics that replaced what it saw.

use super::support::*;

use crate::oplog::Oplog;
use crate::storage::{AdmissionEffects, ProvisionalTopic, StagingLimits, pending_op_bytes};

const READER_SEED: u8 = 139;

fn reader() -> PeerId {
    Ed25519Signer::from_bytes(&[READER_SEED; 32]).peer_id()
}

/// A topic of `seed` with `events` notes of `text_len` characters, then an
/// invitation for the reader, oldest first.
fn history(seed: u8, events: usize, text_len: usize) -> (Irokle, TopicId, Vec<Op>) {
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

fn now() -> u64 {
    1_000
}

fn bytes(ops: &[Op]) -> u64 {
    ops.iter()
        .map(|op| pending_op_bytes(op).unwrap() as u64)
        .sum()
}

/// Admit `ops` into the namespace of `provisional` through a fresh view.
fn stage<S: Storage>(storage: &S, provisional: &ProvisionalTopic, ops: &[Op]) -> Result<(), Error> {
    let store = storage
        .provisional_store(provisional)?
        .ok_or(Error::StaleIncarnation)?;
    admit(&store, provisional.source, ops)
}

fn admit<S: Storage>(store: &S, source: PeerId, ops: &[Op]) -> Result<(), Error> {
    Oplog::with_storage(store.clone())
        .receive_ops_from_peer(Some(source), ops.to_vec())
        .map(drop)
}

/// The registry's current record of `provisional`'s session.
fn listed<S: Storage>(storage: &S, provisional: &ProvisionalTopic) -> Option<ProvisionalTopic> {
    storage
        .provisional_topics()
        .unwrap()
        .into_iter()
        .find(|listed| listed.session == provisional.session)
}

/// What a namespace holds, as its own view and the registry report it.
fn contents<S: Storage>(
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
fn assert_bytes_exact<S: Storage>(storage: &S) {
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
