// SPDX-License-Identifier: MIT OR Apache-2.0
//! Arbitrary bytes through the sync frame and message decoders.

#![no_main]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use irokle::net::{
    decode_frame, decode_frames, decode_sync_message, decoded_message_bound, encode_frame,
    encode_frames, encode_sync_message, frame_decode_bound,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    check_message(data);
    check_frames(data);
});

/// Decoding stays within the declared reservations, and a decoded message round-trips.
fn check_message(bytes: &[u8]) {
    let before = LIVE.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let decoded = decode_sync_message(bytes);
    let peak = PEAK.load(Ordering::Relaxed).saturating_sub(before);
    let retained = LIVE.load(Ordering::Relaxed).saturating_sub(before);
    let bound = frame_decode_bound(bytes.len(), bytes.first().copied().unwrap_or(0));
    assert!(peak <= bound, "decode peak {peak} exceeds bound {bound}");
    let Ok(message) = decoded else {
        return;
    };
    let kept = decoded_message_bound(&message).expect("a decoded message has a size");
    assert!(
        retained <= kept,
        "decoded message keeps {retained} above {kept}"
    );
    let encoded = encode_sync_message(&message).expect("a decoded message encodes");
    let again = decode_sync_message(&encoded).expect("an encoded message decodes");
    assert_eq!(again, message);
    assert_eq!(encode_sync_message(&again).expect("encodes again"), encoded);
}

/// Frames cover the input exactly and re-encode to the same bytes.
fn check_frames(input: &[u8]) {
    if let Ok(Some((payload, consumed))) = decode_frame(input) {
        assert_eq!(consumed, payload.len() + 4);
        assert_eq!(
            encode_frame(&payload).expect("a frame encodes"),
            &input[..consumed]
        );
    }
    let Ok(frames) = decode_frames(input) else {
        return;
    };
    let encoded = encode_frames(frames.iter().map(Vec::as_slice)).expect("frames encode");
    assert_eq!(encoded, input);
    for frame in &frames {
        check_message(frame);
    }
}

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, size) };
        if !moved.is_null() {
            // A move briefly holds both blocks.
            let live = LIVE.fetch_add(size, Ordering::Relaxed) + size;
            PEAK.fetch_max(live, Ordering::Relaxed);
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        moved
    }
}

#[global_allocator]
static COUNTING: Counting = Counting;
