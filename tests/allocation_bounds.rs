// SPDX-License-Identifier: MIT OR Apache-2.0
//! The production clock module in an allocator-instrumented test process.
#![cfg(all(feature = "fjall", target_os = "linux", target_env = "gnu"))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

pub use irokle::{Error, Result, ids};

#[path = "../src/clock.rs"]
mod clock;

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
