use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::Path;
use std::str::FromStr;

use super::support::*;
use crate::storage::{self as crate_storage, FjallStorage, SyncObligation};
use crate::{SyncPeerState, SyncPeerStatus};

/// Databases written by old commits, see `tests/fixtures/README.md`.
const FIXTURES: [&str; 3] = [
    "fjall-schema1-24417de",
    "fjall-schema2-fb5ea2b",
    "fjall-schema2-54db4f9",
];

/// A fresh copy of a committed fixture, so no test opens the originals.
fn fixture_copy(name: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    copy_dir(&source, dir.path());
    dir
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// The ids a fixture generator printed, a flat JSON object of strings.
struct Manifest(BTreeMap<String, String>);

impl Manifest {
    fn read(dir: &Path) -> Self {
        let text = std::fs::read_to_string(dir.join("manifest.json")).unwrap();
        let quoted = text.split('"').skip(1).step_by(2).collect::<Vec<_>>();
        Self(
            quoted
                .chunks(2)
                .map(|pair| (pair[0].to_owned(), pair[1].to_owned()))
                .collect(),
        )
    }

    fn id<T: FromStr>(&self, key: &str) -> T
    where
        T::Err: Debug,
    {
        self.0[key].parse().unwrap()
    }
}

fn raw_records(path: &Path) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let db = fjall::OptimisticTxDatabase::builder(path).open().unwrap();
    let records = db
        .keyspace("records", fjall::KeyspaceCreateOptions::default)
        .unwrap();
    let tx = db.read_tx();
    fjall::Readable::iter(&tx, &records)
        .map(|item| {
            let (key, value) = item.into_inner().unwrap();
            (key.to_vec(), value.to_vec())
        })
        .collect()
}

fn raw_write(path: &Path, key: &[u8], value: Vec<u8>) {
    let db = fjall::OptimisticTxDatabase::builder(path).open().unwrap();
    let records = db
        .keyspace("records", fjall::KeyspaceCreateOptions::default)
        .unwrap();
    let mut tx = db.write_tx().unwrap();
    tx.insert(&records, key.to_vec(), value);
    tx.commit().unwrap().unwrap();
}

fn stored_version(path: &Path) -> u32 {
    postcard::from_bytes(&raw_records(path)[b"sv".as_slice()]).unwrap()
}

/// Open a copied fixture on the current code and check every record the old
/// commit wrote. `certified` tells whether that commit stored ack branches.
fn check_upgrade(name: &str, certified: bool) {
    let dir = fixture_copy(name);
    let m = Manifest::read(dir.path());
    let path = dir.path().join("db");
    drop(FjallStorage::open(&path).unwrap());
    assert_eq!(stored_version(&path), 7);
    let storage = FjallStorage::open(&path).unwrap();

    let topic: TopicId = m.id("topic");
    let [x, y, z] = ["peer_x", "peer_y", "peer_z"].map(|key| m.id::<PeerId>(key));
    let [genesis, e1, e2, add, e3, dep, p1, p2] =
        ["genesis", "e1", "e2", "add", "e3", "dep", "p1", "p2"].map(|key| m.id::<OpId>(key));
    let admitted = BTreeSet::from([genesis, e1, e2, add, e3]);
    assert_eq!(storage.list_op_ids(&topic).unwrap(), admitted);
    for id in &admitted {
        assert_eq!(storage.get_op(id).unwrap().unwrap().id, *id);
        assert_eq!(storage.get_meta(id).unwrap().unwrap().topic_id, topic);
    }
    let mut clock = ActorClock::new();
    clock.observe(actor_id_for(topic, x), 3);
    clock.observe(actor_id_for(topic, y), 2);
    let view = storage.topic_view(&topic, Some(&y)).unwrap().unwrap();
    assert_eq!(view.state.genesis, genesis);
    assert_eq!(view.state.heads, BTreeSet::from([e3]));
    assert_eq!(view.state.members, BTreeSet::from([x, y, z]));
    assert_eq!(view.clock, clock);
    let fingerprint = view.fingerprint.map(|byte| format!("{byte:02x}")).concat();
    assert_eq!(fingerprint, m.0["fingerprint"]);
    assert_eq!(view.pending_missing, BTreeSet::from([dep]));
    assert!(view.owed);

    // An ack without a stored branch migrates but certifies nothing.
    let ack = view.ack.unwrap();
    assert_eq!(ack.heads, BTreeSet::from([e2]));
    assert_eq!(ack.genesis, certified.then_some(genesis));
    assert_eq!(storage.peer_reached_op(&y, &e2).unwrap(), certified);
    assert!(!storage.peer_reached_op(&y, &e3).unwrap());

    // Clocks become clock targets, ids alone merge into one repair want, and
    // a record with neither is dropped.
    assert_eq!(
        storage.sync_obligations(&z, &topic).unwrap(),
        vec![
            SyncObligation::clock(z, topic, clock.clone()),
            SyncObligation::repair(z, topic, BTreeSet::from([e2, e3, add])),
        ]
    );
    assert_eq!(
        storage.sync_obligations(&y, &topic).unwrap(),
        vec![SyncObligation::clock(y, topic, clock)]
    );
    assert_eq!(
        storage.sync_statuses(&topic).unwrap(),
        vec![SyncPeerStatus {
            peer_id: y,
            topic_id: topic,
            state: SyncPeerState::Healthy,
            pending_obligations: 1,
            failed_attempts: 2,
            successful_attempts: 5,
            last_attempt_ms: Some(1_700_000_000_000),
            last_success_ms: Some(1_699_999_999_000),
            last_error: Some("fixture error".into()),
            ..SyncPeerStatus::default()
        }]
    );

    let waiters = storage.pending_waiters(&dep).unwrap();
    let sources = waiters
        .iter()
        .map(|(source, op)| (*source, op.id))
        .collect::<BTreeSet<_>>();
    assert_eq!(sources, BTreeSet::from([(y, p1), (z, p2)]));
    let bytes = |id: OpId| {
        let (_, op) = waiters.iter().find(|(_, op)| op.id == id).unwrap();
        crate_storage::pending_op_bytes(op).unwrap() as u64
    };
    let total = bytes(p1) + bytes(p2);
    assert_eq!(storage.pending_usage(&y), (2, total, 1, bytes(p1)));
    assert_eq!(storage.pending_usage(&z), (2, total, 1, bytes(p2)));

    let topic_b: TopicId = m.id("topic_b");
    let winner: OpId = m.id("winner_genesis");
    let evictions = storage.pending_evictions().unwrap();
    assert_eq!(evictions.len(), 1);
    assert_eq!(evictions[0].topic_id, topic_b);
    assert_eq!(evictions[0].losing_genesis, m.id::<OpId>("local_genesis"));
    assert_eq!(evictions[0].winning_genesis, winner);
    assert_eq!(
        evictions[0]
            .evicted
            .iter()
            .map(|op| op.op_id)
            .collect::<Vec<_>>(),
        vec![m.id::<OpId>("local_event")]
    );
    assert_eq!(genesis_of(&storage, &topic_b), Some(winner));
    storage.clear_eviction(&evictions[0].key()).unwrap();
    assert!(storage.pending_evictions().unwrap().is_empty());

    // Delivering the withheld dependency releases both buffered ops.
    let dep_op: Op =
        postcard::from_bytes(&std::fs::read(dir.path().join("dep.op")).unwrap()).unwrap();
    assert_eq!(dep_op.id, dep);
    let log = oplog::Oplog::with_storage(storage.clone());
    let mut accepted = log.receive_ops(vec![dep_op]).unwrap();
    accepted.extend(log.reconcile_pending_ops().unwrap());
    assert_eq!(accepted, BTreeSet::from([dep, p1, p2]));
    assert_eq!(storage.heads(&topic).unwrap(), BTreeSet::from([p1, p2]));
    assert!(storage.pending_missing_deps(&topic).unwrap().is_empty());
    assert_eq!(storage.pending_usage(&z), (0, 0, 0, 0));
}

#[test]
fn upgrades_schema_one() {
    check_upgrade(FIXTURES[0], false);
}

/// Written before pending byte counters existed under schema 2.
#[test]
fn upgrades_uncounted_pool() {
    check_upgrade(FIXTURES[1], true);
}

#[test]
fn upgrades_counted_pool() {
    check_upgrade(FIXTURES[2], true);
}

/// Two facades upgrading one database at once both open and see one result.
#[test]
fn concurrent_open_agrees() {
    for name in FIXTURES {
        let dir = fixture_copy(name);
        let m = Manifest::read(dir.path());
        let (topic, dep) = (m.id::<TopicId>("topic"), m.id::<OpId>("dep"));
        let z = m.id::<PeerId>("peer_z");
        let db = fjall::OptimisticTxDatabase::builder(dir.path().join("db"))
            .open()
            .unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let openers = (0..2)
            .map(|_| {
                let (db, barrier) = (db.clone(), Arc::clone(&barrier));
                thread::spawn(move || {
                    barrier.wait();
                    let storage = FjallStorage::from_database(db).unwrap();
                    format!(
                        "{:?}",
                        (
                            storage.list_op_ids(&topic).unwrap(),
                            storage.peer_acks(&topic).unwrap(),
                            storage.all_sync_obligations().unwrap(),
                            storage.pending_waiters(&dep).unwrap(),
                            storage.pending_usage(&z),
                            storage.pending_evictions().unwrap(),
                        )
                    )
                })
            })
            .collect::<Vec<_>>();
        let seen = openers
            .into_iter()
            .map(|opener| opener.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(seen[0], seen[1], "{name}");
        drop(db);
        assert_eq!(stored_version(&dir.path().join("db")), 7, "{name}");
    }
}

/// A buffered record the upgrade cannot decode aborts it with nothing
/// written; once the record is repaired the next open finishes the upgrade.
#[test]
fn upgrade_rolls_back() {
    for (name, version) in FIXTURES.into_iter().zip([1, 2, 2]) {
        let dir = fixture_copy(name);
        let path = dir.path().join("db");
        let (key, value) = raw_records(&path)
            .into_iter()
            .find(|(key, _)| key.starts_with(b"po"))
            .unwrap();
        raw_write(&path, &key, vec![0xff; 3]);
        let before = raw_records(&path);
        assert!(FjallStorage::open(&path).is_err(), "{name}");
        assert_eq!(raw_records(&path), before, "{name}");
        assert_eq!(stored_version(&path), version, "{name}");

        raw_write(&path, &key, value);
        drop(FjallStorage::open(&path).unwrap());
        assert_eq!(stored_version(&path), 7, "{name}");
    }
}

#[test]
fn refuses_future_version() {
    let dir = fixture_copy(FIXTURES[0]);
    let path = dir.path().join("db");
    raw_write(&path, b"sv", postcard::to_allocvec(&99_u32).unwrap());
    let before = raw_records(&path);
    assert!(matches!(
        FjallStorage::open(&path),
        Err(Error::Storage(message)) if message.contains("unsupported")
    ));
    assert_eq!(raw_records(&path), before);
}

/// Schema 4 staging named no branch or session: the upgrade discards it as
/// unacknowledged provisional state and keeps the active topic whole.
#[test]
fn upgrades_staged_schema_four() {
    let dir = fixture_copy("fjall-schema4-e112523");
    let m = Manifest::read(dir.path());
    let path = dir.path().join("db");
    let staging = |records: &BTreeMap<Vec<u8>, Vec<u8>>| {
        records
            .keys()
            .filter(|key| key.starts_with(b"bm") || key.starts_with(b"bo"))
            .count()
    };
    assert_eq!(stored_version(&path), 4);
    assert_eq!(staging(&raw_records(&path)), 3);
    let storage = FjallStorage::open(&path).unwrap();
    let active: TopicId = m.id("active");
    let expected = ["genesis", "e1", "e2"]
        .into_iter()
        .map(|key| m.id::<OpId>(key))
        .collect::<BTreeSet<_>>();
    assert_eq!(storage.list_op_ids(&active).unwrap(), expected);
    assert!(storage.provisional_topics().unwrap().is_empty());
    let staged: TopicId = m.id("staged");
    assert!(storage.topic_state(&staged).unwrap().is_none());
    drop(storage);
    assert_eq!(stored_version(&path), 7);
    assert_eq!(staging(&raw_records(&path)), 0);
}

/// Schema 5 namespaces gain a revision and the bytes their keyspace holds. An
/// activation interrupted at schema 5 keeps its claim, stays hidden, and
/// completes on the current code; reopening upgrades nothing twice.
#[test]
fn upgrades_staged_schema_five() {
    let dir = fixture_copy("fjall-schema5-68e4c19");
    let m = Manifest::read(dir.path());
    let path = dir.path().join("db");
    assert_eq!(stored_version(&path), 5);
    let storage = FjallStorage::open(&path).unwrap();
    let active: TopicId = m.id("active");
    let expected = ["genesis", "e1", "e2"]
        .into_iter()
        .map(|key| m.id::<OpId>(key))
        .collect::<BTreeSet<_>>();
    assert_eq!(storage.list_op_ids(&active).unwrap(), expected);
    let listed = storage.provisional_topics().unwrap();
    assert_eq!(listed.len(), 2);
    let session = |key: &str| {
        listed
            .iter()
            .find(|provisional| provisional.session == m.id::<u64>(key))
            .unwrap()
            .clone()
    };
    let staged = session("staged_session");
    assert_eq!(
        (staged.bytes, staged.revision, staged.activating),
        (m.id("staged_bytes"), 0, false)
    );
    let store = storage.provisional_store(&staged).unwrap().unwrap();
    assert_eq!(store.stored_bytes().unwrap(), staged.bytes);
    let activating = session("activating_session");
    assert!(activating.activating);
    assert_eq!(activating.bytes, m.id::<u64>("activating_bytes"));
    let topic: TopicId = m.id("activating");
    assert!(storage.topic_state(&topic).unwrap().is_none());
    assert!(storage.list_op_ids(&topic).unwrap().is_empty());
    assert!(storage.get_op(&m.id("activating_last")).unwrap().is_none());
    drop((store, storage));
    assert_eq!(stored_version(&path), 7);
    assert_eq!(
        FjallStorage::open(&path)
            .unwrap()
            .provisional_topics()
            .unwrap(),
        listed
    );

    let config = NodeConfig {
        signer: Ed25519Signer::from_bytes(&[1; 32]),
        ..NodeConfig::default()
    };
    let reader = Irokle::with_storage(FjallStorage::open(&path).unwrap(), config).unwrap();
    assert_eq!(reader.peer_id(), m.id("reader"));
    let storage = reader.storage();
    assert!(storage.topic_state(&topic).unwrap().is_some());
    assert_eq!(
        storage.list_op_ids(&topic).unwrap().len(),
        m.id::<usize>("activating_ops")
    );
    assert_eq!(storage.provisional_topics().unwrap(), vec![staged]);
}

/// A migrated store reopened after staging part of a late invitation, after a
/// history cursor read part of a topic, and after an admitted event is still
/// owed to another member: each is kept exactly, the migrated eviction record
/// stays until it is acknowledged, and the staging then activates.
#[test]
fn reopen_keeps_progress() {
    use crate::history::HistoryOrder;
    use crate::node::ReceiveOutcome;
    use crate::sync::SyncData;

    let dir = fixture_copy("fjall-schema2-54db4f9");
    let m = Manifest::read(dir.path());
    let path = dir.path().join("db");
    let local = Ed25519Signer::from_bytes(&[180; 32]);
    let open = || {
        let config = NodeConfig {
            signer: local.clone(),
            default_write_concern: WriteConcern::Local,
            peer_whitelist: None,
        };
        Irokle::with_storage(FjallStorage::open(&path).unwrap(), config).unwrap()
    };
    let (writer, source) = (node(181), node(183));
    let absent = node(182).peer_id();

    let reader = open();
    let journal = reader.pending_evictions().unwrap();
    assert_eq!(journal.len(), 1);
    assert_eq!(journal[0].topic_id, m.id::<TopicId>("topic_b"));
    let migrated: TopicId = m.id("topic");
    let z: PeerId = m.id("peer_z");
    let migrated_owed = reader.storage().sync_obligations(&z, &migrated).unwrap();
    assert!(!migrated_owed.is_empty());

    let topic = reader
        .create_topic::<Note>(TopicConfig {
            initial_peers: [writer.peer_id(), absent].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let topic_id = topic.id();
    topic.publish(Note { text: "one".into() }).unwrap();
    let cursor = topic.actor_clock().unwrap();
    for text in ["two", "three"] {
        topic.publish(Note { text: text.into() }).unwrap();
    }
    oplog::Oplog::with_storage(writer.storage().clone())
        .receive_ops(oplog::topological(reader.storage(), &topic_id).unwrap())
        .unwrap();
    let written = writer
        .open_topic::<Note>(topic_id)
        .unwrap()
        .publish(Note {
            text: "written".into(),
        })
        .unwrap()
        .meta
        .op_id;
    let op = writer.storage().get_op(&written).unwrap().unwrap();
    let data = SyncData {
        topic_id,
        ops: vec![op],
    };
    reader
        .receive_sync_data_from(writer.peer_id(), data)
        .unwrap();
    let owed = reader
        .storage()
        .sync_obligations(&absent, &topic_id)
        .unwrap();
    assert!(obligation_covers(reader.storage(), &owed, &written));
    let remaining = |node: &Irokle<FjallStorage>| {
        node.open_topic::<Note>(topic_id)
            .unwrap()
            .history_after(&cursor, HistoryOrder::OldestFirst)
            .unwrap()
            .into_iter()
            .map(|record| record.meta.op_id)
            .collect::<Vec<_>>()
    };
    let after_cursor = remaining(&reader);
    assert_eq!(after_cursor.len(), 3);

    let invited = source.create_topic::<Note>(TopicConfig::default()).unwrap();
    for index in 0..20 {
        invited
            .publish(Note {
                text: format!("{index}"),
            })
            .unwrap();
    }
    invited.add_peer(reader.peer_id()).unwrap();
    let invited_id = invited.id();
    let ops = oplog::topological(source.storage(), &invited_id).unwrap();
    let half = ops.len() / 2;
    let fragment = |range: std::ops::Range<usize>| SyncData {
        topic_id: invited_id,
        ops: ops[range].to_vec(),
    };
    reader
        .receive_sync_outcome(source.peer_id(), fragment(0..half))
        .unwrap();
    let staged = reader.staged_topic(source.peer_id(), invited_id).unwrap();
    assert!(staged.is_some());
    drop((topic, reader));

    let reader = open();
    assert_eq!(
        reader.staged_topic(source.peer_id(), invited_id).unwrap(),
        staged
    );
    assert!(reader.storage().topic_state(&invited_id).unwrap().is_none());
    assert_eq!(remaining(&reader), after_cursor);
    assert_eq!(
        reader
            .storage()
            .sync_obligations(&absent, &topic_id)
            .unwrap(),
        owed
    );
    assert_eq!(
        reader.storage().sync_obligations(&z, &migrated).unwrap(),
        migrated_owed
    );
    assert_eq!(reader.pending_evictions().unwrap(), journal);
    reader.clear_eviction(&journal[0].key()).unwrap();
    assert!(reader.pending_evictions().unwrap().is_empty());
    let outcome = reader
        .receive_sync_outcome(source.peer_id(), fragment(half..ops.len()))
        .unwrap();
    assert!(
        matches!(outcome, ReceiveOutcome::Acked { .. }),
        "{outcome:?}"
    );
    drop(reader);

    let reader = open();
    assert!(reader.pending_evictions().unwrap().is_empty());
    assert!(reader.storage().provisional_topics().unwrap().is_empty());
    assert_eq!(
        reader.storage().list_op_ids(&invited_id).unwrap().len(),
        ops.len()
    );
    assert_eq!(remaining(&reader), after_cursor);
    assert_eq!(
        reader
            .storage()
            .sync_obligations(&absent, &topic_id)
            .unwrap(),
        owed
    );
}

/// The bytes the one clearing slot of `slots` counts.
fn slots_cleared(slots: &[(bool, u64, Option<u64>)]) -> u64 {
    let clearing = slots
        .iter()
        .filter(|(clearing, _, _)| *clearing)
        .collect::<Vec<_>>();
    assert_eq!(clearing.len(), 1);
    assert!(clearing[0].1 > 0);
    clearing[0].1
}

/// The legacy metadata records of the keyspace `name`, before an upgrade.
fn legacy_metas(path: &Path, name: &str) -> BTreeMap<OpId, crate_storage::OpMeta> {
    let db = fjall::OptimisticTxDatabase::builder(path).open().unwrap();
    let records = db
        .keyspace(name, fjall::KeyspaceCreateOptions::default)
        .unwrap();
    let tx = db.read_tx();
    fjall::Readable::prefix(&tx, &records, b"m")
        .map(|item| item.into_inner().unwrap())
        .filter(|(key, _)| key.len() == 1 + OpId::LEN)
        .map(|(_, value)| {
            let meta: crate_storage::OpMeta = postcard::from_bytes(&value).unwrap();
            (meta.id, meta)
        })
        .collect()
}

/// Schema 6 metadata held every observed clock entry. The upgrade to schema 7
/// names each clock by its root node in the main keyspace and in the staged,
/// activating and clearing slot keyspaces. Stopped after any step, as a crash
/// would stop it, the next open finishes it; every record then reads exactly
/// as before, the interrupted activation completes and the cleared slot empties.
#[test]
fn upgrades_clock_schema_six() {
    let original = fixture_copy("fjall-schema6-512b158");
    let m = Manifest::read(original.path());
    let path = original.path().join("db");
    assert_eq!(stored_version(&path), 6);
    let main = legacy_metas(&path, "records");
    let staged = legacy_metas(&path, "bootstrap-0");
    let activating = legacy_metas(&path, "bootstrap-1");
    // The main keyspace also holds the hidden copies of the activation.
    assert_eq!(
        main.len(),
        3 + m.id::<usize>("chain_ops") + activating.len()
    );
    assert!(activating.keys().all(|id| main[id] == activating[id]));
    let chain_head = &main[&m.id::<OpId>("chain_head")];
    assert_eq!(chain_head.observed_clock.len(), 40);
    assert_eq!((staged.len(), activating.len()), (4, 6));
    for steps in 0.. {
        let dir = fixture_copy("fjall-schema6-512b158");
        let path = dir.path().join("db");
        FjallStorage::open_interrupted(&path, steps).unwrap();
        assert_eq!(stored_version(&path), 7);
        let stopped = raw_records(&path).contains_key(b"sm".as_slice());
        let storage = FjallStorage::open(&path).unwrap();
        assert!(!storage.migrating().unwrap(), "{steps} steps");
        for (id, meta) in &main {
            let shown = (!activating.contains_key(id)).then_some(meta);
            assert_eq!(
                storage.get_meta(id).unwrap().as_ref(),
                shown,
                "{steps} steps"
            );
        }
        // The slot schema 6 left clearing is charged what its keyspace counts.
        let slots = storage.slot_bytes().unwrap();
        assert!(slots.contains(&(true, slots_cleared(&slots), Some(slots_cleared(&slots)))));
        let listed = storage.provisional_topics().unwrap();
        let session = |key: &str| {
            listed
                .iter()
                .find(|provisional| provisional.session == m.id::<u64>(key))
                .unwrap()
                .clone()
        };
        let store = storage
            .provisional_store(&session("staged_session"))
            .unwrap()
            .unwrap();
        for (id, meta) in &staged {
            assert_eq!(
                store.get_meta(id).unwrap().as_ref(),
                Some(meta),
                "{steps} steps"
            );
        }
        let topic: TopicId = m.id("activating");
        assert!(storage.get_op(&m.id("activating_last")).unwrap().is_none());
        drop((store, storage));

        let config = NodeConfig {
            signer: Ed25519Signer::from_bytes(&[1; 32]),
            ..NodeConfig::default()
        };
        let reader = Irokle::with_storage(FjallStorage::open(&path).unwrap(), config).unwrap();
        let storage = reader.storage();
        assert!(storage.topic_state(&topic).unwrap().is_some());
        for (id, meta) in &activating {
            assert_eq!(
                storage.get_meta(id).unwrap().as_ref(),
                Some(meta),
                "{steps} steps"
            );
        }
        let cleared: TopicId = m.id("cleared");
        assert!(storage.list_op_ids(&cleared).unwrap().is_empty());
        drop(reader);
        if !stopped {
            // Every slot keyspace that still holds records is a live namespace.
            let db = fjall::OptimisticTxDatabase::builder(&path).open().unwrap();
            let cleared_slot = db
                .keyspace("bootstrap-2", fjall::KeyspaceCreateOptions::default)
                .unwrap();
            assert_eq!(
                fjall::Readable::iter(&db.read_tx(), &cleared_slot).count(),
                0
            );
            assert!(steps >= 4, "the upgrade took {steps} steps");
            break;
        }
    }
}
