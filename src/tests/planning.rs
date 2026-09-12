//! Page planning cost grows with the pages sent, not with the history the
//! peer already holds.

use super::support::*;

use crate::oplog::Oplog;
use crate::sync::{SyncEngine, SyncRequest};

/// A signed chain of `len` events after a genesis, built without admission.
fn signed_chain(seed: u8, len: usize) -> (TopicId, Ed25519Signer, PeerId, Vec<Op>) {
    let signer = Ed25519Signer::from_bytes(&[seed; 32]);
    let reader = Ed25519Signer::from_bytes(&[seed.wrapping_add(1); 32]).peer_id();
    let topic_id = TopicId::hash([b"planning".as_slice(), &[seed], &len.to_le_bytes()].concat());
    let actor_id = actor_id_for(topic_id, signer.peer_id());
    let genesis = Op::sign(
        OpBody {
            topic_id,
            author: signer.peer_id(),
            actor_id,
            actor_seq: 1,
            actor_prev: None,
            deps: BTreeSet::new(),
            generation: 0,
            payload: TopicPayload::Genesis(TopicGenesis::new(
                Note::TYPE_ID,
                [signer.peer_id(), reader],
            )),
        },
        &signer,
    )
    .unwrap();
    let mut ops = vec![genesis];
    for index in 0..len {
        let prev = ops.last().unwrap();
        let op = Op::sign(
            OpBody {
                topic_id,
                author: signer.peer_id(),
                actor_id,
                actor_seq: prev.signed.body.actor_seq + 1,
                actor_prev: Some(prev.id),
                deps: [prev.id].into(),
                generation: prev.signed.body.generation + 1,
                payload: TopicPayload::Event(
                    EventEnvelope::encode_event(&Note {
                        text: index.to_string(),
                    })
                    .unwrap(),
                ),
            },
            &signer,
        )
        .unwrap();
        ops.push(op);
    }
    (topic_id, signer, reader, ops)
}

/// Storage work a responder spends serving a whole catch-up page by page.
fn catch_up_work(len: usize) -> (u64, usize) {
    let (topic_id, signer, reader, ops) = signed_chain(7, len);
    let source = MemoryStorage::new();
    let source_log = Oplog::with_storage(source.clone());
    for batch in ops.chunks(4096) {
        source_log.receive_ops(batch.to_vec()).unwrap();
    }
    let responder = SyncEngine::new(source_log, signer.peer_id());
    let requester = Oplog::new();
    requester.receive_ops(vec![ops[0].clone()]).unwrap();
    let local = source.actor_clock(&topic_id).unwrap();

    let before = source.counters();
    let mut pages = 0;
    loop {
        let clock = requester.storage().actor_clock(&topic_id).unwrap();
        if clock.dominates(&local) {
            break;
        }
        let actor_range_hints = local
            .iter()
            .filter(|(actor, seq)| clock.get(actor) < **seq)
            .map(|(actor, seq)| sync::ActorRangeHint {
                actor_id: *actor,
                from_exclusive: clock.get(actor),
                to_inclusive: *seq,
            })
            .collect();
        let request = SyncRequest {
            topic_id,
            known: BTreeSet::new(),
            wants: BTreeSet::new(),
            actor_range_hints,
        };
        let page = responder.response_page(reader, &request).unwrap();
        assert!(!page.ops.is_empty(), "a page behind the goal must advance");
        requester.receive_ops(page.ops).unwrap();
        pages += 1;
    }
    let after = source.counters();
    let work = (after.meta_reads - before.meta_reads)
        + (after.index_reads - before.index_reads)
        + (after.op_reads - before.op_reads);
    (work, pages)
}

/// Doubling the history must not come close to quadrupling the work: a page
/// plan reads forward from the peer's position instead of walking the prefix.
#[test]
fn catch_up_is_linear() {
    let (small, small_pages) = catch_up_work(8192);
    let (large, large_pages) = catch_up_work(16384);
    assert!(large_pages <= small_pages * 2 + 1);
    assert!(
        large < small * 3,
        "work grew from {small} to {large} when the history doubled"
    );
    // Near-linear: a handful of reads per op sent, independent of history.
    assert!(large < 16384 * 8, "work {large} for 16384 ops");
}

/// Prints responder work for the long-chain catch-up sizes. Run explicitly:
/// `cargo test --features iroh --lib catch_up_costs -- --ignored --nocapture`.
#[test]
#[ignore = "measurement, run explicitly"]
fn catch_up_costs() {
    for len in [8192, 16384, 32768, 65536] {
        let started = std::time::Instant::now();
        let (work, pages) = catch_up_work(len);
        eprintln!(
            "catch_up len={len} pages={pages} storage_reads={work} elapsed_ms={}",
            started.elapsed().as_millis()
        );
    }
}
