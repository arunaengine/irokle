//! Shared storage admission leaves reserved room for namespace recovery.

use std::sync::atomic::{AtomicU64, Ordering};

use super::ownership::{bytes, history, stage};
use super::support::*;
use crate::storage::{FjallStorage, StorageDomain, StoragePressure};

#[test]
fn pressure_preserves_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let db = ::fjall::OptimisticTxDatabase::builder(directory.path())
        .open()
        .unwrap();
    let available = Arc::new(AtomicU64::new(1024 * 1024 * 1024));
    let free = Arc::clone(&available);
    let storage = FjallStorage::from_database(db.clone())
        .unwrap()
        .with_storage_pressure(
            StoragePressure::default().with_probe(move |_| Ok(free.load(Ordering::SeqCst))),
        )
        .unwrap();
    let other = FjallStorage::from_database(db).unwrap();
    let (source, topic, ops) = history(171, 4, 128);
    let provisional = storage
        .open_provisional(source.peer_id(), topic, ops[0].id, 1000)
        .unwrap();
    stage(&storage, &provisional, &ops[..2]).unwrap();
    let before = other.provisional_topics().unwrap();
    available.store(4 * 1024 * 1024, Ordering::SeqCst);
    assert!(matches!(
        stage(&other, &provisional, &ops[2..]),
        Err(Error::StoragePressure(_))
    ));
    assert_eq!(storage.provisional_topics().unwrap(), before);
    assert_eq!(
        storage
            .storage_usage()
            .unwrap()
            .reserved
            .values()
            .sum::<u64>(),
        0
    );
    other.discard_provisional(&before[0]).unwrap();
    assert!(storage.provisional_topics().unwrap().is_empty());
    available.store(1024 * 1024 * 1024, Ordering::SeqCst);
    let fresh = storage
        .open_provisional(source.peer_id(), topic, ops[0].id, 2000)
        .unwrap();
    stage(&other, &fresh, &ops).unwrap();
    assert_eq!(storage.provisional_topics().unwrap()[0].bytes, bytes(&ops));
    let usage = other.storage_usage().unwrap();
    assert!(usage.committed_bytes[&StorageDomain::Operations] > 0);
    assert!(usage.committed_bytes[&StorageDomain::Recovery] > 0);
    assert!(!usage.requires_reopen);
}

#[test]
fn metadata_nodes_charged() {
    let source = super::progress::reverse_chain(MemoryStorage::new(), 33);
    let ops = oplog::topological(source.log.storage(), &source.topic_id).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(directory.path()).unwrap();
    let (provisional, state) = super::ownership::fjall::staged(
        &storage,
        source.genesis.signed.body.author,
        source.topic_id,
        &ops,
    );
    let usage = storage.storage_usage().unwrap();
    assert!(usage.committed_bytes[&StorageDomain::Metadata] > 0);
    assert!(usage.committed_bytes[&StorageDomain::ClockNodes] > 0);
    storage
        .activate_provisional(
            &provisional,
            &state,
            crate::storage::AdmissionEffects::default(),
        )
        .unwrap();
    let usage = storage.storage_usage().unwrap();
    assert!(usage.committed_bytes[&StorageDomain::Activation] > 0);
    assert_eq!(usage.reserved.values().sum::<u64>(), 0);
    assert!(usage.database_bytes >= usage.journal_bytes);
}
