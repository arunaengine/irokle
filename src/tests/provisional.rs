//! Provisional bootstrap namespaces keep a candidate history apart from the
//! active records until one transaction makes it the topic.

use super::support::*;

use crate::oplog::Oplog;
use crate::storage::{
    AdmissionEffects, ProvisionalTopic, StagingLimits, SyncObligation, TopicState,
};

/// A topic of a source with `events` notes and an invitation for `invited`.
fn invited_ops(seed: u8, invited: PeerId, events: usize) -> (Irokle, TopicId, Vec<Op>) {
    let source = node(seed);
    let topic = source.create_topic::<Note>(TopicConfig::default()).unwrap();
    for index in 0..events {
        topic
            .publish(Note {
                text: format!("{index}"),
            })
            .unwrap();
    }
    topic.add_peer(invited).unwrap();
    let ops = oplog::topological(source.storage(), &topic.id()).unwrap();
    (source, topic.id(), ops)
}

fn now() -> u64 {
    1_000
}

/// Admit `ops` into the namespace of `provisional` and return its state.
fn stage<S: Storage>(storage: &S, provisional: &ProvisionalTopic, ops: &[Op]) -> TopicState {
    let store = storage.provisional_store(provisional).unwrap().unwrap();
    Oplog::with_storage(store.clone())
        .receive_ops_from_peer(Some(provisional.source), ops.to_vec())
        .unwrap();
    store.topic_state(&provisional.topic_id).unwrap().unwrap()
}

/// Staged history stays out of the active records, then one activation makes
/// the whole topic visible with its forwarding work and ends every namespace.
fn assert_activation<S: Storage>(storage: S) {
    let reader = node(121).peer_id();
    let (source, topic_id, ops) = invited_ops(120, reader, 40);
    let genesis = ops[0].id;
    let provisional = storage
        .open_provisional(source.peer_id(), topic_id, genesis, now())
        .unwrap();
    let other = storage
        .open_provisional(node(122).peer_id(), topic_id, genesis, now())
        .unwrap();
    assert_ne!(provisional.session, other.session);
    let state = stage(&storage, &provisional, &ops);
    assert!(state.members.contains(&reader));
    assert!(storage.topic_state(&topic_id).unwrap().is_none());
    assert!(storage.list_op_ids(&topic_id).unwrap().is_empty());
    assert!(storage.get_op(&ops[5].id).unwrap().is_none());
    assert!(storage.list_topics().unwrap().is_empty());
    let store = storage.provisional_store(&provisional).unwrap().unwrap();
    assert!(store.stored_bytes().unwrap() > 0);
    assert_eq!(storage.provisional_topics().unwrap().len(), 2);
    let staged_ids = store.list_op_ids(&topic_id).unwrap();
    let staged_clock = store.actor_clock(&topic_id).unwrap();
    let staged_fingerprint = store.topic_fingerprint(&topic_id).unwrap();

    let third = node(123).peer_id();
    let mut clock = ActorClock::new();
    clock.observe(actor_id_for(topic_id, source.peer_id()), ops.len() as u64);
    let effects = AdmissionEffects {
        sync_obligations: vec![SyncObligation::clock(third, topic_id, clock)],
    };
    storage
        .activate_provisional(&provisional, &state, effects)
        .unwrap();
    assert_eq!(storage.topic_state(&topic_id).unwrap(), Some(state.clone()));
    assert_eq!(storage.list_op_ids(&topic_id).unwrap(), staged_ids);
    assert_eq!(storage.actor_clock(&topic_id).unwrap(), staged_clock);
    assert_eq!(
        storage.topic_fingerprint(&topic_id).unwrap(),
        staged_fingerprint
    );
    assert!(storage.has_sync_obligations(&third, &topic_id).unwrap());
    assert!(storage.provisional_topics().unwrap().is_empty());
    assert!(storage.provisional_store(&other).unwrap().is_none());

    // The activated topic admits appends like any other.
    let next = source
        .open_topic::<Note>(topic_id)
        .unwrap()
        .publish(Note {
            text: "after".into(),
        })
        .unwrap()
        .meta
        .op_id;
    let appended = source.storage().get_op(&next).unwrap().unwrap();
    Oplog::with_storage(storage.clone())
        .receive_ops_from_peer(Some(source.peer_id()), vec![appended])
        .unwrap();
    assert!(storage.get_op(&next).unwrap().is_some());
}

#[test]
fn memory_activation() {
    assert_activation(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_activation() {
    let dir = tempfile::tempdir().unwrap();
    assert_activation(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// Activation refuses an ended session and a topic that is already active, and
/// a refused final step leaves the topic unusable to direct admission until the
/// activation is completed.
fn assert_activation_refused<S: Storage>(storage: S) {
    let reader = node(125).peer_id();
    let (source, topic_id, ops) = invited_ops(124, reader, 3);
    let (_, other_topic, other_ops) = invited_ops(126, reader, 3);

    let provisional = storage
        .open_provisional(source.peer_id(), other_topic, other_ops[0].id, now())
        .unwrap();
    let state = stage(&storage, &provisional, &other_ops);
    let stale = ProvisionalTopic {
        session: provisional.session + 100,
        ..provisional.clone()
    };
    assert!(matches!(
        storage.activate_provisional(&stale, &state, AdmissionEffects::default()),
        Err(Error::StaleIncarnation)
    ));
    Oplog::with_storage(storage.clone())
        .receive_ops(other_ops.clone())
        .unwrap();
    assert!(matches!(
        storage.activate_provisional(&provisional, &state, AdmissionEffects::default()),
        Err(Error::AdmissionConflict)
    ));
    assert!(matches!(
        storage.open_provisional(node(127).peer_id(), other_topic, other_ops[0].id, now()),
        Err(Error::AdmissionConflict)
    ));

    let provisional = storage
        .open_provisional(source.peer_id(), topic_id, ops[0].id, now())
        .unwrap();
    let early = stage(&storage, &provisional, &ops[..2]);
    let state = stage(&storage, &provisional, &ops[2..]);
    assert!(matches!(
        storage.activate_provisional(&provisional, &early, AdmissionEffects::default()),
        Err(Error::AdmissionConflict)
    ));
    assert!(storage.topic_state(&topic_id).unwrap().is_none());
    assert!(storage.list_op_ids(&topic_id).unwrap().is_empty());
    storage
        .activate_provisional(&provisional, &state, AdmissionEffects::default())
        .unwrap();
    assert_eq!(storage.topic_state(&topic_id).unwrap(), Some(state));
}

#[test]
fn memory_activation_refused() {
    assert_activation_refused(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_activation_refused() {
    let dir = tempfile::tempdir().unwrap();
    assert_activation_refused(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// Namespace counts and bytes are refused with a typed capacity error, and a
/// refused write stores nothing.
fn assert_namespace_limits<S: Storage>(storage: S) {
    let limits = storage.staging_limits();
    assert_eq!(limits.source_namespaces, 1);
    let reader = node(128).peer_id();
    let (source, topic_id, ops) = invited_ops(127, reader, 64);
    let provisional = storage
        .open_provisional(source.peer_id(), topic_id, ops[0].id, now())
        .unwrap();
    let (_, second_topic, second_ops) = invited_ops(129, reader, 1);
    assert!(matches!(
        storage.open_provisional(source.peer_id(), second_topic, second_ops[0].id, now()),
        Err(Error::StagingCapacity(_))
    ));
    let store = storage.provisional_store(&provisional).unwrap().unwrap();
    let log = Oplog::with_storage(store.clone());
    log.receive_ops_from_peer(Some(source.peer_id()), ops[..2].to_vec())
        .unwrap();
    let held = store.stored_bytes().unwrap();
    let refused = log.receive_ops_from_peer(Some(source.peer_id()), ops[2..].to_vec());
    assert!(
        matches!(&refused, Err(Error::StagingCapacity(_))),
        "{refused:?}"
    );
    assert_eq!(store.stored_bytes().unwrap(), held);
    assert!(store.get_op(&ops[40].id).unwrap().is_none());

    // A discard ends the namespace as it currently is.
    let current = storage.provisional_topics().unwrap().remove(0);
    assert!(storage.discard_provisional(&current).unwrap());
    assert!(!storage.discard_provisional(&current).unwrap());
    assert!(storage.provisional_store(&provisional).unwrap().is_none());
    let reopened = storage
        .open_provisional(source.peer_id(), topic_id, ops[0].id, now())
        .unwrap();
    assert!(reopened.session > provisional.session);
    let fresh = storage.provisional_store(&reopened).unwrap().unwrap();
    assert_eq!(fresh.stored_bytes().unwrap(), 0);
    assert!(fresh.list_op_ids(&topic_id).unwrap().is_empty());
}

fn tight_limits() -> StagingLimits {
    StagingLimits {
        namespace_bytes: 4 * 1024,
        source_namespaces: 1,
        ..StagingLimits::MEMORY
    }
}

#[test]
fn memory_namespace_limits() {
    assert_namespace_limits(MemoryStorage::new().with_staging_limits(tight_limits()));
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_namespace_limits() {
    let dir = tempfile::tempdir().unwrap();
    assert_namespace_limits(
        crate::storage::FjallStorage::open(dir.path())
            .unwrap()
            .with_staging_limits(tight_limits()),
    );
}

/// A namespace survives a reopen, and an activation interrupted after its
/// copies began resumes after the reopen without exposing the topic before.
#[cfg(feature = "fjall")]
#[test]
fn fjall_activation_resumes() {
    let dir = tempfile::tempdir().unwrap();
    let reader = node(131).peer_id();
    let (source, topic_id, ops) = invited_ops(130, reader, 5000);
    let (provisional, early, state) = {
        let storage = crate::storage::FjallStorage::open(dir.path()).unwrap();
        let provisional = storage
            .open_provisional(source.peer_id(), topic_id, ops[0].id, now())
            .unwrap();
        let early = stage(&storage, &provisional, &ops[..4096]);
        let state = stage(&storage, &provisional, &ops[4096..]);
        storage.interrupt_activation(&provisional);
        (provisional, early, state)
    };
    let storage = crate::storage::FjallStorage::open(dir.path()).unwrap();
    assert!(storage.topic_state(&topic_id).unwrap().is_none());
    assert!(storage.list_topics().unwrap().is_empty());
    // Copies already sit in the active records; direct admission refuses them.
    assert!(!storage.list_op_ids(&topic_id).unwrap().is_empty());
    assert!(matches!(
        Oplog::with_storage(storage.clone()).receive_ops(ops.clone()),
        Err(Error::AdmissionConflict)
    ));
    let listed = storage.provisional_topics().unwrap();
    assert_eq!(listed.len(), 1);
    assert!(listed[0].activating);
    assert_eq!(listed[0].session, provisional.session);
    assert_ne!(early, state);
    assert!(!storage.discard_provisional(&listed[0]).unwrap());
    storage
        .activate_provisional(&listed[0], &state, AdmissionEffects::default())
        .unwrap();
    assert_eq!(storage.topic_state(&topic_id).unwrap(), Some(state));
    assert_eq!(
        storage.list_op_ids(&topic_id).unwrap(),
        source.storage().list_op_ids(&topic_id).unwrap()
    );
    assert!(storage.provisional_topics().unwrap().is_empty());
}
