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

unsafe extern "C" {
    fn malloc_usable_size(pointer: *mut std::ffi::c_void) -> usize;
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            LIVE.fetch_add(
                unsafe { malloc_usable_size(pointer.cast()) },
                Ordering::Relaxed,
            );
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
