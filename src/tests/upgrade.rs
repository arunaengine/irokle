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
    assert_eq!(stored_version(&path), 5);
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
        assert_eq!(stored_version(&dir.path().join("db")), 5, "{name}");
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
        assert_eq!(stored_version(&path), 5, "{name}");
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
    assert_eq!(stored_version(&path), 5);
    assert_eq!(staging(&raw_records(&path)), 0);
}
