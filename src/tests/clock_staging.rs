//! Node-backed provisional clocks across activation, clearing and migration.

use super::ownership::fjall::{assert_hidden, staged};
use super::pages::Source;
use super::progress::reverse_chain;
use super::support::*;
use crate::storage::{AdmissionEffects, FjallStorage, Hook};
use std::sync::atomic::{AtomicUsize, Ordering};

fn clock_nodes(db: &fjall::OptimisticTxDatabase, space: &str, topic: TopicId) -> usize {
    let records = db
        .keyspace(space, fjall::KeyspaceCreateOptions::default)
        .unwrap();
    let prefix = [b"cn".as_slice(), topic.as_ref()].concat();
    fjall::Readable::prefix(&db.read_tx(), &records, prefix).count()
}

fn assert_clocks(source: &Source<MemoryStorage>, store: &FjallStorage, ops: &[Op]) {
    assert_eq!(
        store.topic_view(&source.topic_id, None).unwrap(),
        source
            .log
            .storage()
            .topic_view(&source.topic_id, None)
            .unwrap()
    );
    for op in ops {
        assert_eq!(store.get_op(&op.id).unwrap().as_ref(), Some(op));
        let expected = source.log.storage().get_meta(&op.id).unwrap().unwrap();
        let actual = store.get_meta(&op.id).unwrap().unwrap();
        let (header, clock) = store.get_observation(&op.id).unwrap().unwrap();
        assert_eq!(header, crate::storage::OpHeader::from(&expected));
        assert_eq!(clock, expected.observed_clock);
        assert_eq!(actual, expected);
        assert_eq!(
            postcard::to_allocvec(&actual.observed_clock).unwrap(),
            postcard::to_allocvec(&expected.observed_clock).unwrap()
        );
        assert_eq!(
            store.get_position(&op.id).unwrap(),
            Some((&expected).into())
        );
        op.validate().unwrap();
    }
}

#[test]
fn staged_clock_boundaries() {
    for entries in [31, 32, 33] {
        let source = reverse_chain(MemoryStorage::new(), entries);
        let ops = oplog::topological(source.log.storage(), &source.topic_id).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let db = fjall::OptimisticTxDatabase::builder(dir.path())
            .open()
            .unwrap();
        let storage = FjallStorage::from_database(db.clone()).unwrap();
        let (provisional, state) = staged(
            &storage,
            source.genesis.signed.body.author,
            source.topic_id,
            &ops,
        );
        let view = storage.provisional_store(&provisional).unwrap().unwrap();
        let clock = view
            .get_meta(&ops.last().unwrap().id)
            .unwrap()
            .unwrap()
            .observed_clock;
        assert_eq!(clock.len(), entries);
        assert_eq!(
            clock_nodes(&db, "bootstrap-0", source.topic_id) > 0,
            entries > 32
        );
        assert_clocks(&source, &view, &ops);
        storage
            .activate_provisional(&provisional, &state, AdmissionEffects::default())
            .unwrap();
        assert!(matches!(
            view.get_meta(&ops[0].id),
            Err(Error::StaleIncarnation)
        ));
        assert_eq!(clock.len(), entries, "a retained clock remains readable");
        drop((view, storage, db));
        let storage = FjallStorage::open(dir.path()).unwrap();
        assert_clocks(&source, &storage, &ops);
    }
}

#[test]
fn staged_nodes_recover() {
    let source = reverse_chain(MemoryStorage::new(), 1300);
    let topic = source.topic_id;
    let ops = oplog::topological(source.log.storage(), &topic).unwrap();
    let actor = ops[0].signed.body.actor_id;
    let mut steps = vec![
        (Hook::Claimed, 0),
        (Hook::Publish, 0),
        (Hook::ReleaseSlot, 0),
    ];
    let mut index = 0;
    while index < steps.len() {
        let (point, nth) = steps[index];
        let dir = tempfile::tempdir().unwrap();
        let state = {
            let db = fjall::OptimisticTxDatabase::builder(dir.path())
                .open()
                .unwrap();
            let storage = FjallStorage::from_database(db.clone()).unwrap();
            let (provisional, state) = staged(&storage, ops[0].signed.body.author, topic, &ops);
            assert!(clock_nodes(&db, "bootstrap-0", topic) > 4096);
            if index == 0 {
                let records = db
                    .keyspace("bootstrap-0", fjall::KeyspaceCreateOptions::default)
                    .unwrap();
                let records = fjall::Readable::iter(&db.read_tx(), &records).count();
                for chunk in 0..=records / 4096 {
                    steps.extend([(Hook::CopyChunk, chunk), (Hook::DeleteChunk, chunk)]);
                }
            }
            let seen = AtomicUsize::new(0);
            storage.set_hook(move |at| {
                if at == point && seen.fetch_add(1, Ordering::SeqCst) == nth {
                    return Err(Error::Storage("injected node boundary failure".into()));
                }
                Ok(())
            });
            let result =
                storage.activate_provisional(&provisional, &state, AdmissionEffects::default());
            assert!(result.is_err(), "{point:?} {nth} was not reached");
            state
        };
        let storage = FjallStorage::open(dir.path()).unwrap();
        if storage.topic_state(&topic).unwrap().is_none() {
            assert_hidden(&storage, topic, actor, &ops);
            let provisional = storage.provisional_topics().unwrap().pop().unwrap();
            let view = storage.provisional_store(&provisional).unwrap().unwrap();
            assert_clocks(&source, &view, &ops);
            storage
                .activate_provisional(&provisional, &state, AdmissionEffects::default())
                .unwrap();
        }
        assert_clocks(&source, &storage, &ops);
        let other = reverse_chain(MemoryStorage::new(), 1);
        storage
            .open_provisional(other.reader, other.topic_id, other.genesis.id, 2_000)
            .unwrap();
        drop(storage);
        let db = fjall::OptimisticTxDatabase::builder(dir.path())
            .open()
            .unwrap();
        assert_eq!(clock_nodes(&db, "bootstrap-0", topic), 0);
        assert!(clock_nodes(&db, "records", topic) > 4096);
        index += 1;
    }
}

#[test]
fn staged_nodes_migrate() {
    let source = reverse_chain(MemoryStorage::new(), 1050);
    let topic = source.topic_id;
    let ops = oplog::topological(source.log.storage(), &topic).unwrap();
    let original = tempfile::tempdir().unwrap();
    {
        let db = fjall::OptimisticTxDatabase::builder(original.path())
            .open()
            .unwrap();
        let storage = FjallStorage::from_database(db.clone()).unwrap();
        staged(&storage, ops[0].signed.body.author, topic, &ops);
        assert!(clock_nodes(&db, "bootstrap-0", topic) > 0);
        let records = db
            .keyspace("records", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        let slot = db
            .keyspace("bootstrap-0", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        let mut tx = db.write_tx().unwrap();
        crate::storage::write_legacy_metas(&mut tx, &slot).unwrap();
        tx.insert(&records, b"sv", postcard::to_allocvec(&6_u32).unwrap());
        tx.commit().unwrap().unwrap();
        db.persist(fjall::PersistMode::SyncAll).unwrap();
    }
    let mut completed = false;
    for steps in 0..8 {
        let dir = tempfile::tempdir().unwrap();
        super::upgrade::copy_dir(original.path(), dir.path());
        FjallStorage::open_interrupted(dir.path(), steps).unwrap();
        let pending = {
            let db = fjall::OptimisticTxDatabase::builder(dir.path())
                .open()
                .unwrap();
            let records = db
                .keyspace("records", fjall::KeyspaceCreateOptions::default)
                .unwrap();
            fjall::Readable::contains_key(&db.read_tx(), &records, b"sm").unwrap()
        };
        let storage = FjallStorage::open(dir.path()).unwrap();
        assert!(!storage.migrating().unwrap());
        assert_hidden(&storage, topic, ops[0].signed.body.actor_id, &ops);
        let provisional = storage.provisional_topics().unwrap().pop().unwrap();
        let view = storage.provisional_store(&provisional).unwrap().unwrap();
        assert_clocks(&source, &view, &ops);
        let state = view.topic_state(&topic).unwrap().unwrap();
        storage
            .activate_provisional(&provisional, &state, AdmissionEffects::default())
            .unwrap();
        assert_clocks(&source, &storage, &ops);
        if !pending {
            completed = true;
            break;
        }
    }
    assert!(
        completed,
        "the fixture did not reach its final migration step"
    );
}

#[test]
fn damaged_nodes_refused() {
    let source = reverse_chain(MemoryStorage::new(), 40);
    let topic = source.topic_id;
    let ops = oplog::topological(source.log.storage(), &topic).unwrap();
    for missing in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        {
            let storage = FjallStorage::open(dir.path()).unwrap();
            staged(&storage, ops[0].signed.body.author, topic, &ops);
        }
        {
            let db = fjall::OptimisticTxDatabase::builder(dir.path())
                .open()
                .unwrap();
            let slot = db
                .keyspace("bootstrap-0", fjall::KeyspaceCreateOptions::default)
                .unwrap();
            let prefix = [b"cn".as_slice(), topic.as_ref()].concat();
            let keys = fjall::Readable::prefix(&db.read_tx(), &slot, prefix)
                .map(|item| item.key().unwrap().to_vec())
                .collect::<Vec<_>>();
            assert!(!keys.is_empty());
            let mut tx = db.write_tx().unwrap();
            for key in keys {
                if missing {
                    tx.remove(&slot, key);
                } else {
                    tx.insert(&slot, key, vec![0]);
                }
            }
            tx.commit().unwrap().unwrap();
            db.persist(fjall::PersistMode::SyncAll).unwrap();
        }
        let storage = FjallStorage::open(dir.path()).unwrap();
        let provisional = storage.provisional_topics().unwrap().pop().unwrap();
        let view = storage.provisional_store(&provisional).unwrap().unwrap();
        assert!(view.get_meta(&ops.last().unwrap().id).is_err());
        assert!(view.get_observation(&ops.last().unwrap().id).is_err());
        let state = view.topic_state(&topic).unwrap().unwrap();
        let result =
            storage.activate_provisional(&provisional, &state, AdmissionEffects::default());
        assert!(
            result.is_err(),
            "damaged node graph was published: missing={missing}"
        );
        assert_hidden(&storage, topic, ops[0].signed.body.actor_id, &ops);
        assert!(!storage.provisional_topics().unwrap().is_empty());
    }
}

#[test]
fn cached_nodes_rechecked() {
    let source = reverse_chain(MemoryStorage::new(), 40);
    let topic = source.topic_id;
    let ops = oplog::topological(source.log.storage(), &topic).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = fjall::OptimisticTxDatabase::builder(dir.path())
        .open()
        .unwrap();
    let storage = FjallStorage::from_database(db.clone()).unwrap();
    let (provisional, state) = staged(&storage, ops[0].signed.body.author, topic, &ops);
    let view = storage.provisional_store(&provisional).unwrap().unwrap();
    let held = view
        .get_meta(&ops.last().unwrap().id)
        .unwrap()
        .unwrap()
        .observed_clock;
    let slot = db
        .keyspace("bootstrap-0", fjall::KeyspaceCreateOptions::default)
        .unwrap();
    let key = [b"cn".as_slice(), topic.as_ref(), &held.root_hash().unwrap()].concat();
    let mut tx = db.write_tx().unwrap();
    tx.remove(&slot, key);
    tx.commit().unwrap().unwrap();
    let result = storage.activate_provisional(&provisional, &state, AdmissionEffects::default());
    assert!(
        result.is_err(),
        "a cached clock masked its missing persisted root"
    );
    assert_hidden(&storage, topic, ops[0].signed.body.actor_id, &ops);
    assert_eq!(held.len(), 40);
}
