//! Memory reservations follow shared facades and survive failed publication.

use super::ownership::{history, stage};
use super::support::*;
use crate::storage::{AdmissionEffects, MemoryDomain, MemoryLimits, PeerAck, SyncObligation};

fn cap(storage: &MemoryStorage, limits: MemoryLimits) {
    storage.clone().with_memory_limits(limits).unwrap();
}

#[test]
fn metadata_refusal_atomic() {
    let (source, topic, ops) = history(169, 1, 32);
    let storage = source.storage();
    let peer = super::ownership::reader();
    let before = storage.all_sync_obligations().unwrap();
    let held = storage.memory_usage().unwrap().reserved.values().sum();
    cap(
        storage,
        MemoryLimits {
            retained_bytes: held,
            ..Default::default()
        },
    );
    let ack = PeerAck {
        peer_id: peer,
        topic_id: topic,
        genesis: Some(ops[0].id),
        heads: storage.heads(&topic).unwrap(),
        clock: storage.actor_clock(&topic).unwrap(),
    };
    assert!(matches!(
        storage.apply_peer_ack(ack.clone()),
        Err(Error::MemoryPressure { .. })
    ));
    assert!(storage.peer_ack(&peer, &topic).unwrap().is_none());
    assert_eq!(storage.all_sync_obligations().unwrap(), before);
    assert_eq!(storage.list_ops(&topic).unwrap().len(), ops.len());
    cap(storage, MemoryLimits::default());
    storage.apply_peer_ack(ack).unwrap();
    assert!(!storage.has_sync_obligations(&peer, &topic).unwrap());
}

#[test]
fn nodes_follow_holders() {
    let source = super::progress::reverse_chain(MemoryStorage::new(), 33);
    let storage = source.log.storage().clone();
    let clock = storage.actor_clock(&source.topic_id).unwrap();
    let before = storage.memory_usage().unwrap();
    let peer = super::ownership::reader();
    storage
        .put_sync_obligation(
            SyncObligation::clock(peer, source.topic_id, clock.clone()),
            Some(source.genesis.id),
        )
        .unwrap();
    assert_eq!(
        storage.memory_usage().unwrap().clock_allocations,
        before.clock_allocations
    );
    storage.reset_topic(&source.topic_id).unwrap();
    let retained = storage.memory_usage().unwrap();
    assert!(retained.reserved[&MemoryDomain::SharedNodes] > 0);
    assert_eq!(retained.reserved[&MemoryDomain::Operations], 0);
    assert_eq!(clock.len(), 34);
    drop(clock);
    drop(source);
    assert_eq!(
        storage.memory_usage().unwrap().reserved[&MemoryDomain::SharedNodes],
        0
    );
}

#[test]
fn activation_reservation_atomic() {
    let (source, topic, ops) = history(170, 4, 128);
    let storage = MemoryStorage::new();
    let other = storage.clone();
    let provisional = storage
        .open_provisional(source.peer_id(), topic, ops[0].id, 1000)
        .unwrap();
    stage(&storage, &provisional, &ops).unwrap();
    let view = storage.provisional_store(&provisional).unwrap().unwrap();
    let state = view.topic_state(&topic).unwrap().unwrap();
    let before = storage.provisional_topics().unwrap();
    cap(
        &other,
        MemoryLimits {
            activation_bytes: 0,
            ..Default::default()
        },
    );
    assert!(matches!(
        storage.activate_provisional(&provisional, &state, AdmissionEffects::default()),
        Err(Error::MemoryPressure {
            domain: MemoryDomain::Activation,
            ..
        })
    ));
    assert!(storage.topic_state(&topic).unwrap().is_none());
    assert_eq!(storage.provisional_topics().unwrap(), before);
    assert_eq!(view.list_op_ids(&topic).unwrap().len(), ops.len());
    cap(&other, MemoryLimits::default());
    storage
        .activate_provisional(&provisional, &state, AdmissionEffects::default())
        .unwrap();
    assert_eq!(
        storage.list_op_ids(&topic).unwrap(),
        ops.iter().map(|op| op.id).collect()
    );
    assert!(matches!(view.stored_bytes(), Err(Error::StaleIncarnation)));
    assert_eq!(
        storage.memory_usage().unwrap().reserved[&MemoryDomain::Activation],
        0
    );
}

#[test]
fn refusal_allows_discard() {
    let (source, topic, ops) = history(171, 4, 128);
    let storage = MemoryStorage::new();
    let provisional = storage
        .open_provisional(source.peer_id(), topic, ops[0].id, 1000)
        .unwrap();
    stage(&storage, &provisional, &ops[..2]).unwrap();
    let current = storage.provisional_topics().unwrap();
    let held = storage.memory_usage().unwrap().reserved.values().sum();
    cap(
        &storage,
        MemoryLimits {
            retained_bytes: held,
            ..Default::default()
        },
    );
    assert!(matches!(
        stage(&storage, &provisional, &ops[2..]),
        Err(Error::MemoryPressure { .. })
    ));
    assert_eq!(
        storage.provisional_topics().unwrap()[0].bytes,
        current[0].bytes
    );
    let current = storage.provisional_topics().unwrap().remove(0);
    assert!(storage.discard_provisional(&current).unwrap());
    assert_eq!(
        storage
            .memory_usage()
            .unwrap()
            .reserved
            .values()
            .sum::<u64>(),
        0
    );
    let fresh = storage
        .open_provisional(source.peer_id(), topic, ops[0].id, 2000)
        .unwrap();
    stage(&storage, &fresh, &ops[..2]).unwrap();
}

#[test]
fn merge_reserves_first() {
    let (source, topic, ops) = history(172, 2, 32);
    let store = source.storage();
    let before = store.all_sync_obligations().unwrap();
    cap(
        store,
        MemoryLimits {
            workspace_bytes: 0,
            ..Default::default()
        },
    );
    let obligation = SyncObligation::clock(
        super::ownership::reader(),
        topic,
        store.actor_clock(&topic).unwrap(),
    );
    assert!(matches!(
        store.put_sync_obligation(obligation, Some(ops[0].id)),
        Err(Error::MemoryPressure {
            domain: MemoryDomain::Workspace,
            ..
        })
    ));
    assert_eq!(store.all_sync_obligations().unwrap(), before);
    assert_eq!(
        store.memory_usage().unwrap().reserved[&MemoryDomain::Workspace],
        0
    );
    assert_eq!(store.reset_topic(&topic).unwrap(), ops.len());
    assert!(store.list_op_ids(&topic).unwrap().is_empty());
}
