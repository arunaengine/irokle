// SPDX-License-Identifier: MIT OR Apache-2.0
//! Membership projections measured with a counting allocator. One test only, so
//! no other test allocates in this process while it measures.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use irokle::oplog::Oplog;
use irokle::storage::{MemoryDomain, MemoryLimits};
use irokle::{
    Ed25519Signer, Error, EventEnvelope, MemoryStorage, Op, OpBody, OpId, PeerId,
    ReplicationPolicy, Signer, TopicControl, TopicGenesis, TopicId, TopicPayload, actor_id_for,
};

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn grow(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            grow(layout.size());
        }
        pointer
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, size) };
        if !moved.is_null() {
            if size >= layout.size() {
                grow(size - layout.size());
            } else {
                LIVE.fetch_sub(layout.size() - size, Ordering::Relaxed);
            }
        }
        moved
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) };
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The cache budget of kept projection states, as the oplog sets it.
const CACHE_BYTES: usize = 16 * 1024 * 1024;

fn event(topic: TopicId, writer: &Ed25519Signer, deps: BTreeSet<OpId>, generation: u64) -> Op {
    let body = OpBody {
        topic_id: topic,
        author: writer.peer_id(),
        actor_id: actor_id_for(topic, writer.peer_id()),
        actor_seq: 1,
        actor_prev: None,
        deps,
        generation,
        payload: TopicPayload::Event(EventEnvelope {
            type_id: "test.memory".into(),
            payload: vec![0].into(),
        }),
    };
    Op::sign(body, writer).unwrap()
}

/// 128 policy controls each select all 4,224 initial peers, and each writer's
/// first event depends on a different one. The states the receiving oplog keeps
/// stay within the cache budget, measured by what dropping it releases.
fn assert_cache_bounded() {
    let owner = Ed25519Signer::from_bytes(&[31; 32]);
    let topic = TopicId::hash(b"projection-memory-cache");
    let writers = (0..128_u8)
        .map(|index| Ed25519Signer::from_bytes(&[index.wrapping_add(64); 32]))
        .collect::<Vec<_>>();
    let mut peers = (0..4096_u32)
        .map(|index| PeerId::hash(index.to_le_bytes()))
        .collect::<BTreeSet<_>>();
    peers.extend(writers.iter().map(Signer::peer_id));
    let storage = MemoryStorage::new();
    let source = Oplog::with_storage(storage.clone());
    let actor = actor_id_for(topic, owner.peer_id());
    let created = TopicGenesis::new("test.memory", peers.clone());
    source
        .create_topic_genesis(topic, actor, created, &owner)
        .unwrap();
    let controls = (0..writers.len())
        .map(|index| {
            let policy =
                ReplicationPolicy::selected(peers.iter().copied()).with_max_sync_peers(index + 1);
            source
                .create_control_op(
                    topic,
                    actor,
                    TopicControl::SetReplicationPolicy { policy },
                    &owner,
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let receiver = Oplog::with_storage(storage.clone());
    for (writer, control) in writers.iter().zip(&controls) {
        let op = event(
            topic,
            writer,
            [control.id].into(),
            control.signed.body.generation + 1,
        );
        receiver.receive_op(op).unwrap();
    }
    let held = LIVE.load(Ordering::Relaxed);
    drop(receiver);
    let released = held - LIVE.load(Ordering::Relaxed);
    assert!(released <= CACHE_BYTES, "{released} bytes kept");
}

/// A late event joining 1,024 historical controls needs a cold projection. A
/// workspace too small for it refuses the event; a large one admits it, and
/// the whole receive then allocates no more than the workspace it reserved.
fn assert_workspace_covers() {
    for workspace_bytes in [1_u64 << 20, 64 << 20] {
        let owner = Ed25519Signer::from_bytes(&[33; 32]);
        let writer = Ed25519Signer::from_bytes(&[34; 32]);
        let topic = TopicId::hash(workspace_bytes.to_le_bytes());
        let limits = MemoryLimits {
            workspace_bytes,
            ..MemoryLimits::default()
        };
        let storage = MemoryStorage::new().with_memory_limits(limits).unwrap();
        let source = Oplog::with_storage(storage.clone());
        let actor = actor_id_for(topic, owner.peer_id());
        let created = TopicGenesis::new("test.memory", [writer.peer_id()]);
        source
            .create_topic_genesis(topic, actor, created, &owner)
            .unwrap();
        let joined = (0..1024_u32)
            .map(|index| {
                let peer = PeerId::hash(index.to_le_bytes());
                source
                    .create_control_op(topic, actor, TopicControl::AddPeer { peer }, &owner)
                    .unwrap()
                    .id
            })
            .collect::<BTreeSet<_>>();
        let late = event(topic, &writer, joined, 1025);
        let receiver = Oplog::with_storage(storage.clone());
        let before = LIVE.load(Ordering::Relaxed);
        PEAK.store(before, Ordering::Relaxed);
        let received = receiver.receive_op(late);
        let peak = (PEAK.load(Ordering::Relaxed) - before) as u64;
        if workspace_bytes == 1 << 20 {
            assert!(matches!(received, Err(Error::MemoryPressure { .. })));
            continue;
        }
        received.unwrap();
        let reserved = storage.memory_usage().unwrap().peak_reserved[&MemoryDomain::Workspace];
        assert!(
            peak <= reserved,
            "{peak} bytes allocated, {reserved} reserved"
        );
    }
}

#[test]
fn projection_memory_bounded() {
    assert_cache_bounded();
    assert_workspace_covers();
}
