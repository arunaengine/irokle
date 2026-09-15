//! Actual write faults in a child process, isolated from the suite's resource limits.

use std::io::Write;
use std::process::Command;

use super::support::*;
use crate::storage::FjallStorage;

#[repr(C)]
struct Limits {
    soft: u64,
    hard: u64,
}

unsafe extern "C" {
    fn getrlimit(resource: i32, limits: *mut Limits) -> i32;
    fn setrlimit(resource: i32, limits: *const Limits) -> i32;
    fn signal(number: i32, handler: usize) -> usize;
}

struct FileLimit {
    limits: Limits,
    handler: usize,
}

impl FileLimit {
    fn zero() -> Self {
        let mut limits = Limits { soft: 0, hard: 0 };
        // Linux RLIMIT_FSIZE and SIGXFSZ; only this child changes them.
        assert_eq!(unsafe { getrlimit(1, &mut limits) }, 0);
        let handler = unsafe { signal(25, 1) };
        assert_ne!(handler, usize::MAX);
        let blocked = Limits {
            soft: 0,
            hard: limits.hard,
        };
        assert_eq!(unsafe { setrlimit(1, &blocked) }, 0);
        Self { limits, handler }
    }
}

impl Drop for FileLimit {
    fn drop(&mut self) {
        assert_eq!(unsafe { setrlimit(1, &self.limits) }, 0);
        assert_ne!(unsafe { signal(25, self.handler) }, usize::MAX);
    }
}

fn fault_child(path: &std::path::Path) {
    let config = || NodeConfig {
        signer: Ed25519Signer::from_bytes(&[177; 32]),
        ..NodeConfig::default()
    };
    let storage = FjallStorage::open(path).unwrap();
    let node = Irokle::with_storage(storage.clone(), config()).unwrap();
    let topic = node.create_topic::<Note>(TopicConfig::default()).unwrap();
    topic
        .publish(Note {
            text: "durable".into(),
        })
        .unwrap();
    let id = topic.id();
    let before = storage.list_op_ids(&id).unwrap();
    let clock = storage.actor_clock(&id).unwrap();
    let peer = node.peer_id();
    let obligation = crate::storage::SyncObligation::repair(
        peer,
        id,
        [OpId::hash(b"outstanding repair")].into(),
    );
    storage
        .put_sync_obligation(obligation.clone(), genesis_of(&storage, &id))
        .unwrap();
    let attempts = storage.counters().transaction_attempts;
    let failed = {
        let _limit = FileLimit::zero();
        topic.publish(Note {
            text: "failed write".into(),
        })
    };
    assert!(
        matches!(&failed, Err(Error::ReopenRequired(::fjall::Error::Io(error))) if error.raw_os_error() == Some(27)),
        "{failed:?}"
    );
    assert_eq!(storage.counters().transaction_attempts, attempts + 1);
    assert_eq!(storage.list_op_ids(&id).unwrap(), before);
    assert_eq!(storage.actor_clock(&id).unwrap(), clock);
    assert_eq!(
        storage.sync_obligations(&peer, &id).unwrap(),
        vec![obligation.clone()]
    );
    let refused = topic.publish(Note {
        text: "still poisoned".into(),
    });
    assert!(
        matches!(
            refused,
            Err(Error::ReopenRequired(::fjall::Error::Poisoned))
        ),
        "{refused:?}"
    );
    assert_eq!(storage.counters().transaction_attempts, attempts + 2);
    drop((topic, node, storage));
    let storage = FjallStorage::open(path).unwrap();
    let recovered = storage.list_op_ids(&id).unwrap();
    assert_eq!(
        storage.sync_obligations(&peer, &id).unwrap(),
        vec![obligation.clone()]
    );
    assert!(before.is_subset(&recovered));
    assert!(recovered.len() == before.len() || recovered.len() == before.len() + 1);
    let mut expected = clock.clone();
    for added in recovered.difference(&before) {
        let op = storage.get_op(added).unwrap().unwrap();
        op.validate().unwrap();
        let TopicPayload::Event(event) = &op.signed.body.payload else {
            panic!("unexpected recovered operation")
        };
        assert_eq!(event.decode_event::<Note>().unwrap().text, "failed write");
        let meta = storage.get_meta(added).unwrap().unwrap();
        assert_eq!(meta.observed_clock, clock);
        assert_eq!(meta.deps, op.signed.body.deps);
        assert_eq!(
            storage
                .actor_index(&id, &meta.actor_id, meta.actor_seq)
                .unwrap(),
            Some(*added)
        );
        expected.observe(meta.actor_id, meta.actor_seq);
    }
    assert_eq!(storage.actor_clock(&id).unwrap(), expected);
    let node = Irokle::with_storage(storage.clone(), config()).unwrap();
    node.open_topic::<Note>(id)
        .unwrap()
        .publish(Note {
            text: "recovered".into(),
        })
        .unwrap();
    assert_eq!(storage.list_op_ids(&id).unwrap().len(), recovered.len() + 1);
    assert_eq!(
        storage.sync_obligations(&peer, &id).unwrap(),
        vec![obligation]
    );
    println!(
        "fault_recovered prior_ops={} recovered_ops={} final_ops={}",
        before.len(),
        recovered.len(),
        recovered.len() + 1
    );
}

#[test]
fn write_fault_recovers() {
    if let Some(path) = std::env::var_os("IROKLE_WRITE_CHILD") {
        fault_child(std::path::Path::new(&path));
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "tests::storage_full::write_fault_recovers",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("IROKLE_WRITE_CHILD", directory.path())
        .output()
        .unwrap();
    if let Some(path) = std::env::var_os("IROKLE_FAULT_LOG") {
        let mut log = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap();
        log.write_all(&output.stdout).unwrap();
        log.write_all(&output.stderr).unwrap();
    }
    if !output.status.success() {
        let preserved = directory.keep();
        panic!(
            "fault child failed; database retained at {preserved:?}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .contains("fault_recovered prior_ops=2 recovered_ops=")
    );
}
