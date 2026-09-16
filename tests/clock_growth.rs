// SPDX-License-Identifier: MIT OR Apache-2.0
//! Retained bytes of reverse dependency chains of single-op actors, whose
//! observed clocks grow by one position per op. Run explicitly:
//! `cargo test --release --all-features --test clock_growth -- --ignored --nocapture --test-threads=1`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use irokle::oplog::Oplog;
use irokle::storage::Storage;
use irokle::{
    Ed25519Signer, Event, EventEnvelope, MemoryStorage, Op, OpBody, OpId, Signer, TopicGenesis,
    TopicId, TopicPayload, actor_id_for,
};
use serde::{Deserialize, Serialize};

/// Counts live heap bytes, so a phase's retained size is exact.
struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static COUNTING: Counting = Counting;

#[derive(Clone, Debug, PartialEq, Eq, irokle::Event, Serialize, Deserialize)]
#[irokle(type_id = "test.clock_growth.note")]
struct Note {
    text: String,
}

/// A genesis and `actors` single-op writers sorted by actor key, each op
/// depending on the op of the writer with the next larger key.
fn reverse_chain(actors: usize) -> (TopicId, Vec<Op>) {
    let owner = Ed25519Signer::from_bytes(&[230; 32]);
    let topic_id = TopicId::hash([b"clock-growth".as_slice(), &actors.to_le_bytes()].concat());
    let mut writers = (0..actors)
        .map(|index| {
            let mut seed = [11_u8; 32];
            seed[..8].copy_from_slice(&(index as u64).to_le_bytes());
            Ed25519Signer::from_bytes(&seed)
        })
        .collect::<Vec<_>>();
    writers.sort_by_key(|writer| actor_id_for(topic_id, writer.peer_id()));
    let members = writers
        .iter()
        .chain([&owner])
        .map(Signer::peer_id)
        .collect::<BTreeSet<_>>();
    let genesis = Op::sign(
        OpBody {
            topic_id,
            author: owner.peer_id(),
            actor_id: actor_id_for(topic_id, owner.peer_id()),
            actor_seq: 1,
            actor_prev: None,
            deps: BTreeSet::new(),
            generation: 0,
            payload: TopicPayload::Genesis(TopicGenesis::new(Note::TYPE_ID, members)),
        },
        &owner,
    )
    .unwrap();
    let mut ops = vec![genesis];
    for (generation, writer) in writers.iter().rev().enumerate() {
        let dep: OpId = ops.last().unwrap().id;
        let op = Op::sign(
            OpBody {
                topic_id,
                author: writer.peer_id(),
                actor_id: actor_id_for(topic_id, writer.peer_id()),
                actor_seq: 1,
                actor_prev: None,
                deps: [dep].into(),
                generation: generation as u64 + 1,
                payload: TopicPayload::Event(
                    EventEnvelope::encode_event(&Note { text: "x".into() }).unwrap(),
                ),
            },
            writer,
        )
        .unwrap();
        ops.push(op);
    }
    (topic_id, ops)
}

fn admit<S: Storage>(storage: S, ops: &[Op]) -> Oplog<S> {
    let log = Oplog::with_storage(storage);
    for batch in ops.chunks(4096) {
        log.receive_ops(batch.to_vec()).unwrap();
    }
    log
}

/// Observed clock entries every stored op holds, summed.
fn clock_entries<S: Storage>(log: &Oplog<S>, topic_id: &TopicId) -> usize {
    log.storage()
        .list_op_ids(topic_id)
        .unwrap()
        .iter()
        .map(|id| {
            let meta = log.storage().get_meta(id).unwrap().unwrap();
            meta.observed_clock.iter().count()
        })
        .sum()
}

/// The chain sizes to measure: `CLOCK_GROWTH_ACTORS` as a comma separated
/// list, or three doublings.
fn sizes() -> Vec<usize> {
    std::env::var("CLOCK_GROWTH_ACTORS").map_or(vec![1024, 2048, 4096], |sizes| {
        sizes
            .split(',')
            .map(|size| size.trim().parse().unwrap())
            .collect()
    })
}

fn live() -> usize {
    LIVE.load(Ordering::Relaxed)
}

/// Live heap bytes of one and two Memory stores of a reverse chain, per
/// phase, at three sizes.
#[test]
#[ignore = "measures retained bytes of large chains, run explicitly"]
fn memory_chain_bytes() {
    for actors in sizes() {
        let start = live();
        let (topic_id, ops) = reverse_chain(actors);
        let fixture = live() - start;
        let before = live();
        let one = admit(MemoryStorage::new(), &ops);
        let one_store = live() - before;
        let two = admit(MemoryStorage::new(), &ops);
        let two_stores = live() - before;
        let entries = clock_entries(&one, &topic_id);
        drop(ops);
        let idle = live() - start;
        println!(
            "memory actors={actors} fixture_bytes={fixture} one_store_bytes={one_store} \
             two_store_bytes={two_stores} idle_two_store_bytes={idle} clock_entries={entries}"
        );
        drop((one, two));
    }
}

/// Bytes of a closed Fjall store directory of a reverse chain at three sizes.
#[cfg(feature = "fjall")]
#[test]
#[ignore = "measures stored bytes of large chains, run explicitly"]
fn fjall_chain_bytes() {
    let persist_mode = match std::env::var("IROKLE_BENCH_PERSIST").as_deref() {
        Ok("buffer") => fjall::PersistMode::Buffer,
        Ok("sync_all") | Err(std::env::VarError::NotPresent) => fjall::PersistMode::SyncAll,
        _ => panic!("invalid measurement persist mode"),
    };
    fn directory_bytes(path: &std::path::Path) -> u64 {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let meta = entry.metadata().unwrap();
                if meta.is_dir() {
                    directory_bytes(&entry.path())
                } else {
                    meta.len()
                }
            })
            .sum()
    }
    for actors in sizes() {
        let (topic_id, ops) = reverse_chain(actors);
        let dir = tempfile::tempdir().unwrap();
        let started = std::time::Instant::now();
        let log = admit(
            irokle::FjallStorage::open_with_persist_mode(dir.path(), persist_mode).unwrap(),
            &ops,
        );
        let admit_ms = started.elapsed().as_millis();
        let entries = clock_entries(&log, &topic_id);
        drop(log);
        // Reopening trims the preallocated journal to its content.
        drop(irokle::FjallStorage::open(dir.path()).unwrap());
        let stored = directory_bytes(dir.path());
        let db = fjall::OptimisticTxDatabase::builder(dir.path())
            .open()
            .unwrap();
        let records = db
            .keyspace("records", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        let tx = db.read_tx();
        let value_bytes = |prefix: &[u8], len: usize| -> usize {
            fjall::Readable::prefix(&tx, &records, prefix)
                .map(|item| item.into_inner().unwrap())
                .filter(|(key, _)| key.len() == len)
                .map(|(_, value)| value.len())
                .sum()
        };
        let meta_bytes = value_bytes(b"m", 33);
        let node_bytes = value_bytes(b"cn", 66);
        println!(
            "fjall actors={actors} directory_bytes={stored} meta_value_bytes={meta_bytes} \
             clock_node_value_bytes={node_bytes} clock_entries={entries} admit_ms={admit_ms} durability={persist_mode:?}"
        );
    }
}
