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

#[test]
fn small_buffers_progress() {
    let source = super::progress::reverse_chain(MemoryStorage::new(), 129);
    let topic = source.topic_id;
    let ops = oplog::topological(source.log.storage(), &topic).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(directory.path()).unwrap();
    let (provisional, state) =
        super::ownership::fjall::staged(&storage, ops[0].signed.body.author, topic, &ops);
    let mut pressure = StoragePressure::default();
    pressure.buffer_bytes = 96 * 1024;
    pressure.recovery_buffer_bytes = 96 * 1024;
    let storage = storage.with_storage_pressure(pressure).unwrap();
    storage
        .activate_provisional(
            &provisional,
            &state,
            crate::storage::AdmissionEffects::default(),
        )
        .unwrap();
    assert_eq!(
        storage.list_op_ids(&topic).unwrap(),
        source.log.storage().list_op_ids(&topic).unwrap()
    );
    assert_eq!(
        storage.actor_clock(&topic).unwrap(),
        source.log.storage().actor_clock(&topic).unwrap()
    );
    assert!(storage.slot_bytes().unwrap().is_empty());
    let usage = storage.storage_usage().unwrap();
    assert_eq!(usage.reserved.values().sum::<u64>(), 0);
    assert!(usage.peak_reserved[&StorageDomain::Activation] <= 96 * 1024);
    drop(storage);
    let reopened = FjallStorage::open(directory.path()).unwrap();
    for op in ops {
        assert_eq!(reopened.get_op(&op.id).unwrap(), Some(op.clone()));
        assert_eq!(
            reopened.get_meta(&op.id).unwrap(),
            source.log.storage().get_meta(&op.id).unwrap()
        );
    }
}

#[test]
fn small_migration_progress() {
    let source = super::progress::reverse_chain(MemoryStorage::new(), 129);
    let topic = source.topic_id;
    let ops = oplog::topological(source.log.storage(), &topic).unwrap();
    let directory = tempfile::tempdir().unwrap();
    {
        let db = ::fjall::OptimisticTxDatabase::builder(directory.path())
            .open()
            .unwrap();
        let storage = FjallStorage::from_database(db.clone()).unwrap();
        oplog::Oplog::with_storage(storage)
            .receive_ops(ops.clone())
            .unwrap();
        let records = db
            .keyspace("records", ::fjall::KeyspaceCreateOptions::default)
            .unwrap();
        let mut tx = db.write_tx().unwrap();
        crate::storage::write_legacy_metas(&mut tx, &records).unwrap();
        tx.insert(&records, b"sv", postcard::to_allocvec(&1_u32).unwrap());
        tx.commit().unwrap().unwrap();
        db.persist(::fjall::PersistMode::SyncAll).unwrap();
    }
    let mut pressure = StoragePressure::default();
    pressure.recovery_buffer_bytes = 256 * 1024;
    let storage = FjallStorage::open_with_pressure(directory.path(), pressure).unwrap();
    assert!(!storage.migrating().unwrap());
    assert!(storage.topic_state(&topic).unwrap().is_some());
    for op in ops {
        assert_eq!(
            storage.get_meta(&op.id).unwrap(),
            source.log.storage().get_meta(&op.id).unwrap()
        );
    }
    assert_eq!(
        storage
            .storage_usage()
            .unwrap()
            .reserved
            .values()
            .sum::<u64>(),
        0
    );
}
