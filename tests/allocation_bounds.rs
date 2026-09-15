// SPDX-License-Identifier: MIT OR Apache-2.0
//! The production clock module in an allocator-instrumented test process.
#![cfg(all(feature = "fjall", target_os = "linux", target_env = "gnu"))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

pub use irokle::{Error, Op, OpId, Result, TopicPayload, ids};

const MAX_PAGE_BYTES: usize = 32 * 1024 * 1024;

mod storage {
    pub use irokle::storage::SnapshotRead;
}

#[path = "../src/sync/records.rs"]
mod records;

mod tests {
    pub mod support {
        pub use bytes::Bytes;
        pub use irokle::{
            Ed25519Signer, Event, EventEnvelope, MemoryStorage, Signer, Storage, TopicGenesis,
            TopicId, actor_id_for, oplog,
        };
        #[derive(serde::Serialize, serde::Deserialize)]
        pub struct Note {
            pub text: String,
        }
        impl Event for Note {
            const TYPE_ID: &'static str = "test.note";
        }
    }
}

#[path = "../src/clock.rs"]
mod clock;

#[path = "../src/sync/space.rs"]
mod space;

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe extern "C" {
    fn malloc_usable_size(pointer: *mut std::ffi::c_void) -> usize;
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            let bytes = unsafe { malloc_usable_size(pointer.cast()) };
            let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE.fetch_sub(
            unsafe { malloc_usable_size(pointer.cast()) },
            Ordering::Relaxed,
        );
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static COUNTING: Counting = Counting;

#[test]
#[ignore = "allocator measurement requires its own process and one test thread"]
fn captured_clock_bounds() {
    for entries in [1024_u32, 2048, 65_536] {
        let before = LIVE.load(Ordering::Relaxed);
        let mut original = clock::ActorClock::new();
        for n in 0..entries {
            original.observe(ids::ActorId::hash(n.to_le_bytes()), 1);
        }
        let mut changed = original.clone();
        for n in 0..entries {
            changed.observe(ids::ActorId::hash(n.to_le_bytes()), 2);
        }
        let held = [original, changed];
        let allocated = LIVE.load(Ordering::Relaxed) - before;
        let reserved = 2 * clock::ActorClock::allocation_bound(entries as usize);
        assert!(allocated > 0 && allocated <= reserved);
        println!("clocks entries={entries} allocator_bytes={allocated} reserved_bytes={reserved}");
        drop(held);
    }
}

#[test]
#[ignore = "allocator measurement requires its own process and one test thread"]
fn selected_clock_bounds() {
    for entries in [1024_u32, 2048, 65_536] {
        let mut original = clock::ActorClock::new();
        for n in 0..entries {
            original.observe(ids::ActorId::hash(n.to_le_bytes()), 1);
        }
        let mut actors = original
            .iter()
            .map(|(actor, _)| *actor)
            .collect::<std::collections::BTreeSet<_>>();
        let removed = actors.pop_first().unwrap();
        let before = LIVE.load(Ordering::Relaxed);
        PEAK.store(before, Ordering::Relaxed);
        let selected = original.selected(&actors);
        let allocated = LIVE.load(Ordering::Relaxed) - before;
        let peak = PEAK.load(Ordering::Relaxed) - before;
        let bound = clock::ActorClock::allocation_bound(64);
        assert_eq!(selected.len(), entries as usize - 1);
        assert_eq!(selected.get(&removed), 0);
        assert!(
            selected
                .iter()
                .all(|(actor, seq)| original.get(actor) == *seq)
        );
        println!(
            "selected entries={entries} allocator_bytes={allocated} peak_bytes={peak} bound={bound}"
        );
        assert!(
            allocated <= bound && peak <= bound,
            "unchanged trie subtrees were copied"
        );
    }
}

#[test]
#[ignore = "allocator measurement requires its own process and one test thread"]
fn decoded_clock_bounds() {
    use irokle::sync::{SyncMessage, SyncReceipt};
    for entries in [1024_u32, 2048, 65_536] {
        let mut clock = irokle::ActorClock::new();
        for n in 0..entries {
            clock.observe(ids::ActorId::hash(n.to_le_bytes()), 1);
        }
        let message = SyncMessage::Receipt(SyncReceipt {
            topic_id: ids::TopicId::hash(b"allocation"),
            genesis: ids::OpId::hash(b"genesis"),
            session: 1,
            clock,
        });
        let bytes = irokle::net::encode_sync_message(&message).unwrap();
        drop(message);
        let before = LIVE.load(Ordering::Relaxed);
        PEAK.store(before, Ordering::Relaxed);
        let message = irokle::net::decode_sync_message(&bytes).unwrap();
        let allocated = LIVE.load(Ordering::Relaxed) - before;
        let peak = PEAK.load(Ordering::Relaxed) - before;
        let raw = unsafe { malloc_usable_size(bytes.as_ptr().cast_mut().cast()) };
        let bound = irokle::net::decoded_message_bound(&message).unwrap();
        let decoding = irokle::net::frame_decode_bound(bytes.len(), bytes[0]);
        assert!(
            allocated > 0 && allocated <= bound,
            "{entries} entries allocate {allocated}, bound {bound}"
        );
        assert!(
            peak + raw <= decoding,
            "decoding peak {peak} + raw {raw}, bound {decoding}"
        );
        println!(
            "decoded entries={entries} wire_bytes={} allocator_bytes={allocated} reserved_bytes={bound} peak_bytes={peak} decode_bound={decoding}",
            bytes.len()
        );
        assert_eq!(irokle::net::encode_sync_message(&message).unwrap(), bytes);
    }
}

#[test]
#[ignore = "allocator measurement requires its own process and one test thread"]
fn malformed_frame_bounds() {
    for (count, padding) in [(usize::MAX, 0), (32_768, 32_768)] {
        for tag in [3_u8, 4] {
            let mut bytes = vec![tag];
            bytes.extend([0; 32]);
            if tag == 3 {
                bytes.extend([0, 0]);
            }
            bytes.extend(postcard::to_allocvec(&count).unwrap());
            bytes.resize(bytes.len() + padding, 0);
            let before = LIVE.load(Ordering::Relaxed);
            PEAK.store(before, Ordering::Relaxed);
            assert!(irokle::net::decode_sync_message(&bytes).is_err());
            let peak = PEAK.load(Ordering::Relaxed) - before;
            let raw = unsafe { malloc_usable_size(bytes.as_ptr().cast_mut().cast()) };
            let bound = irokle::net::frame_decode_bound(bytes.len(), tag);
            assert!(
                peak + raw <= bound,
                "tag {tag} peak {peak} + raw {raw}, bound {bound}"
            );
            println!(
                "malformed count={count} tag={tag} wire_bytes={} peak_bytes={peak} decode_bound={bound}",
                bytes.len()
            );
        }
    }
}

#[test]
#[ignore = "allocator measurement requires its own process and one test thread"]
fn shared_cache_bounds() {
    for actors in [1024_u32, 2048, 4096] {
        let before = LIVE.load(Ordering::Relaxed);
        let cache = clock::ClockCache::default();
        let mut clock = clock::ActorClock::new();
        for n in 0..actors {
            clock.observe(
                ids::ActorId::from_bytes(*blake3::hash(&n.to_le_bytes()).as_bytes()),
                1,
            );
            cache.keep(&clock);
        }
        let allocated = LIVE.load(Ordering::Relaxed) - before;
        let modeled = cache.bytes();
        assert!(
            allocated > 0 && allocated <= modeled,
            "allocator={allocated}, model={modeled}"
        );
        println!("cache actors={actors} allocator_bytes={allocated} modeled_bytes={modeled}");
    }
}

#[test]
#[ignore = "allocator measurement requires its own process and one test thread"]
fn tree_allocation_bounds() {
    check_tree(0_u64);
    check_tree([0_u8; 128]);
    check_tree([0_u8; 384]);
}

fn check_tree<V: Clone>(value: V) {
    for entries in [1_u32, 4, 5, 6, 11, 12, 31, 32, 33, 256, 4096] {
        let before = LIVE.load(Ordering::Relaxed);
        let mut tree = std::collections::BTreeMap::new();
        for n in 0..entries {
            tree.insert(ids::ActorId::hash(n.to_le_bytes()), value.clone());
        }
        let allocated = LIVE.load(Ordering::Relaxed) - before;
        let bound = space::tree_bytes::<ids::ActorId, V>(tree.len());
        assert!(
            allocated <= bound,
            "retained tree allocation exceeds its charge"
        );
        while tree.len() > 1 {
            tree.pop_first();
        }
        let shrunk = LIVE.load(Ordering::Relaxed) - before;
        assert!(shrunk <= space::tree_bytes::<ids::ActorId, V>(tree.len()));
        tree.pop_first();
        let empty = LIVE.load(Ordering::Relaxed) - before;
        assert!(empty <= space::tree_bytes::<ids::ActorId, V>(0));
        println!(
            "tree value_bytes={} entries={entries} allocator_bytes={allocated} bound={bound} shrunk_bytes={shrunk} empty_bytes={empty}",
            size_of::<V>()
        );
    }
}

#[test]
#[ignore = "allocator measurement requires its own process and one test thread"]
fn vector_allocation_bounds() {
    for entries in [1, 4, 5, 11, 12, 33, 4096] {
        let before = LIVE.load(Ordering::Relaxed);
        let mut values = std::collections::VecDeque::new();
        for n in 0..entries {
            values.push_back([n as u64; 12]);
        }
        let allocated = LIVE.load(Ordering::Relaxed) - before;
        let bound = space::vector_bytes::<[u64; 12]>(values.capacity());
        values.clear();
        assert!(
            allocated <= bound,
            "{entries} entries allocated {allocated}, bound {bound}"
        );
        assert!(LIVE.load(Ordering::Relaxed) - before <= bound);
        println!(
            "deque entries={entries} capacity={} allocator_bytes={allocated} bound={bound}",
            values.capacity()
        );
    }
}

#[test]
#[ignore = "allocator measurement requires its own process and one test thread"]
fn record_allocation_bounds() {
    use irokle::{
        Ed25519Signer, EventEnvelope, MemoryStorage, OpBody, Signer, Storage, TopicGenesis,
        TopicId, actor_id_for, oplog,
    };
    assert_eq!(
        irokle::sync::SyncCredit::default().bytes,
        MAX_PAGE_BYTES as u64
    );
    for entries in [1, 128, 2048, 65_536] {
        let store = MemoryStorage::new();
        let log = oplog::Oplog::with_storage(store.clone());
        let signer = Ed25519Signer::from_bytes(&[218; 32]);
        let topic = TopicId::hash(b"record-allocation");
        let actor = actor_id_for(topic, signer.peer_id());
        let mut previous = log
            .create_topic_genesis(
                topic,
                actor,
                TopicGenesis::new("allocation", [signer.peer_id()]),
                &signer,
            )
            .unwrap();
        let mut deps = std::collections::BTreeSet::from([previous.id]);
        for _ in 1..entries {
            previous = log
                .create_event_op(
                    topic,
                    actor,
                    EventEnvelope {
                        type_id: "allocation".into(),
                        payload: bytes::Bytes::new(),
                    },
                    &signer,
                )
                .unwrap();
            deps.insert(previous.id);
        }
        let join = Op::sign(
            OpBody {
                topic_id: topic,
                author: signer.peer_id(),
                actor_id: actor,
                actor_seq: previous.signed.body.actor_seq + 1,
                actor_prev: Some(previous.id),
                deps,
                generation: previous.signed.body.generation + 1,
                payload: TopicPayload::Event(EventEnvelope {
                    type_id: "allocation".into(),
                    payload: bytes::Bytes::from_static(b"owned"),
                }),
            },
            &signer,
        )
        .unwrap();
        log.receive_ops(vec![join.clone()]).unwrap();
        let pool = std::sync::Arc::new(AtomicUsize::new(0));
        let mut records = records::Records::new(std::sync::Arc::clone(&pool));
        let before = LIVE.load(Ordering::Relaxed);
        PEAK.store(before, Ordering::Relaxed);
        let record = store
            .read_snapshot(|read| records.take(read, &join.id))
            .unwrap()
            .unwrap();
        records.keep(record);
        assert!(records.contains(&join.id));
        let allocated = LIVE.load(Ordering::Relaxed) - before;
        let peak = PEAK.load(Ordering::Relaxed) - before;
        let bound = pool.load(Ordering::Acquire) + 4096;
        assert!(
            allocated <= bound && peak <= bound,
            "record: {allocated} retained, {peak} peak, {bound} bound"
        );
        let record = store
            .read_snapshot(|read| records.take(read, &join.id))
            .unwrap()
            .unwrap();
        assert_eq!(record.op, join);
        drop(record);
        drop(records);
        assert_eq!(pool.load(Ordering::Acquire), 0);
        println!(
            "record dependencies={entries} allocator_bytes={allocated} peak_bytes={peak} bound={bound}"
        );
    }
}
