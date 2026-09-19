use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::Path;
use std::str::FromStr;

use super::support::*;
use crate::storage::{self as crate_storage, FjallStorage, SyncObligation};
use crate::{SyncPeerState, SyncPeerStatus};

/// A database written by a schema 1 commit, see `tests/fixtures/README.md`.
const FIXTURE: &str = "fjall-schema1-24417de";

/// A fresh copy of a committed fixture, so no test opens the originals.
fn fixture_copy(name: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    copy_dir(&source, dir.path());
    dir
}

pub(super) fn copy_dir(from: &Path, to: &Path) {
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
/// commit wrote.
#[test]
fn upgrades_schema_one() {
    let dir = fixture_copy(FIXTURE);
    let m = Manifest::read(dir.path());
    let path = dir.path().join("db");
    drop(FjallStorage::open(&path).unwrap());
    assert_eq!(stored_version(&path), 2);
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
    assert_eq!(ack.genesis, None);
    assert!(!storage.peer_reached_op(&y, &e2).unwrap());
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

/// Two facades upgrading one database at once both open and see one result.
#[test]
fn concurrent_open_agrees() {
    let dir = fixture_copy(FIXTURE);
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
    assert_eq!(seen[0], seen[1]);
    drop(db);
    assert_eq!(stored_version(&dir.path().join("db")), 2);
}

/// A buffered record the upgrade cannot decode aborts it with nothing
/// written; once the record is repaired the next open finishes the upgrade.
#[test]
fn upgrade_rolls_back() {
    let dir = fixture_copy(FIXTURE);
    let path = dir.path().join("db");
    let (key, value) = raw_records(&path)
        .into_iter()
        .find(|(key, _)| key.starts_with(b"po"))
        .unwrap();
    raw_write(&path, &key, vec![0xff; 3]);
    let before = raw_records(&path);
    assert!(FjallStorage::open(&path).is_err());
    assert_eq!(raw_records(&path), before);
    assert_eq!(stored_version(&path), 1);

    raw_write(&path, &key, value);
    drop(FjallStorage::open(&path).unwrap());
    assert_eq!(stored_version(&path), 2);
}

#[test]
fn refuses_future_version() {
    let dir = fixture_copy(FIXTURE);
    let path = dir.path().join("db");
    raw_write(&path, b"sv", postcard::to_allocvec(&99_u32).unwrap());
    let before = raw_records(&path);
    assert!(matches!(
        FjallStorage::open(&path),
        Err(Error::Storage(message)) if message.contains("unsupported")
    ));
    assert_eq!(raw_records(&path), before);
}

/// Migration preserves staged invitations, a history cursor and an owed event.
/// The eviction record stays until acknowledged, then staging activates.
/// Reopening preserves each state exactly.
#[test]
fn reopen_keeps_progress() {
    use crate::history::HistoryOrder;
    use crate::node::ReceiveOutcome;
    use crate::sync::SyncData;

    let dir = fixture_copy(FIXTURE);
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

/// The schema 1 metadata records of a database, before its upgrade.
fn legacy_metas(path: &Path) -> BTreeMap<OpId, crate_storage::OpMeta> {
    raw_records(path)
        .into_iter()
        .filter(|(key, _)| key.len() == 1 + OpId::LEN && key[0] == b'm')
        .map(|(_, value)| {
            let meta: crate_storage::OpMeta = postcard::from_bytes(&value).unwrap();
            (meta.id, meta)
        })
        .collect()
}

/// A crash after the upgrade transaction or between two metadata rewrite steps
/// leaves a store the next open finishes, with every record readable.
#[test]
fn upgrade_resumes() {
    let original = fixture_copy(FIXTURE);
    let metas = legacy_metas(&original.path().join("db"));
    assert!(!metas.is_empty());
    for steps in 0.. {
        let dir = fixture_copy(FIXTURE);
        let path = dir.path().join("db");
        FjallStorage::open_interrupted(&path, steps).unwrap();
        assert_eq!(stored_version(&path), 2);
        let stopped = raw_records(&path).contains_key(b"sm".as_slice());
        let storage = FjallStorage::open(&path).unwrap();
        assert!(!storage.migrating().unwrap(), "{steps} steps");
        for (id, meta) in &metas {
            assert_eq!(
                storage.get_meta(id).unwrap().as_ref(),
                Some(meta),
                "{steps} steps"
            );
        }
        if !stopped {
            break;
        }
    }
}
