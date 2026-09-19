//! Collection allocation estimates used by retained traversal state.

use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) fn reserve_bytes(pool: &AtomicUsize, bytes: usize, limit: usize) -> bool {
    let mut held = pool.load(Ordering::Acquire);
    loop {
        let Some(next) = held.checked_add(bytes).filter(|sum| *sum <= limit) else {
            return false;
        };
        match pool.compare_exchange_weak(held, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(current) => held = current,
        }
    }
}

pub(super) fn tree_bytes<K, V>(entries: usize) -> usize {
    // Rust 1.95/1.97 nodes hold eleven entries, at least five outside the root.
    // Include a possibly retained empty root, internal edges and allocator slack.
    let nodes = entries.saturating_sub(1) / 5 + 1;
    let alignment = align_of::<K>().max(align_of::<V>());
    let leaf = 11 * (size_of::<K>() + size_of::<V>()) + 48 + 2 * alignment;
    nodes
        .saturating_mul(leaf)
        .saturating_add(((nodes + 3) / 6).saturating_mul(12 * size_of::<usize>()))
}

pub(super) fn vector_bytes<T>(capacity: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    let bytes = capacity.saturating_mul(size_of::<T>());
    let slack = if bytes >= 64 * 1024 { 64 * 1024 } else { 32 };
    bytes.saturating_add(slack + 2 * align_of::<T>())
}
