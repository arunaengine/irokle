//! Repair cursors preserve causal roots while sharing a small work slice.

use crate::oplog::Oplog;
use crate::tests::support::*;

use crate::sync::{ActorRangeHint, PageBudget, SyncEngine, SyncRequest};

fn repair_slices<S: Storage>(storage: S) {
    let owner = Ed25519Signer::from_bytes(&[221; 32]);
    let peer = Ed25519Signer::from_bytes(&[222; 32]).peer_id();
    let writers = (31..43)
        .map(|seed| Ed25519Signer::from_bytes(&[seed; 32]))
        .collect::<Vec<_>>();
    let topic = TopicId::hash(b"repair-cursor-slices");
    let actor = actor_id_for(topic, owner.peer_id());
    let source = Oplog::with_storage(storage);
    let genesis = source
        .create_topic_genesis(
            topic,
            actor,
            TopicGenesis::new(
                Note::TYPE_ID,
                writers
                    .iter()
                    .map(Signer::peer_id)
                    .chain([owner.peer_id(), peer]),
            ),
            &owner,
        )
        .unwrap();
    let mut roots = Vec::new();
    for writer in writers {
        let op = Op::sign(
            OpBody {
                topic_id: topic,
                author: writer.peer_id(),
                actor_id: actor_id_for(topic, writer.peer_id()),
                actor_seq: 1,
                actor_prev: None,
                deps: [genesis.id].into(),
                generation: 1,
                payload: TopicPayload::Event(
                    EventEnvelope::encode_event(&Note {
                        text: "root".into(),
                    })
                    .unwrap(),
                ),
            },
            &writer,
        )
        .unwrap();
        source.receive_ops(vec![op.clone()]).unwrap();
        roots.push(op);
    }
    roots.push(
        source
            .create_event_op(
                topic,
                actor,
                EventEnvelope::encode_event(&Note {
                    text: "join".into(),
                })
                .unwrap(),
                &owner,
            )
            .unwrap(),
    );
    assert!(roots.last().unwrap().signed.body.deps.len() > 4);
    for informed in [false, true] {
        for limit in [1, 4] {
            let mut responder =
                SyncEngine::new(source.clone(), owner.peer_id()).with_page_visits(limit, 1);
            let destination = Oplog::new();
            destination.receive_ops(vec![genesis.clone()]).unwrap();
            let receiver = SyncEngine::new(destination.clone(), peer);
            let mut request = SyncRequest {
                topic_id: topic,
                known: [genesis.id].into(),
                wants: roots.iter().map(|op| op.id).collect(),
                actor_range_hints: roots
                    .iter()
                    .map(|op| ActorRangeHint {
                        actor_id: op.signed.body.actor_id,
                        from_exclusive: if op.signed.body.actor_id == actor {
                            1
                        } else {
                            0
                        },
                        to_inclusive: op.signed.body.actor_seq,
                    })
                    .collect(),
                genesis: Some(genesis.id),
                credit: Default::default(),
                window: Default::default(),
            };
            let before = responder.page_work();
            let zero = responder
                .response_page(peer, &request, PageBudget { ops: 0, bytes: 0 })
                .unwrap();
            assert!(zero.more && zero.ops.is_empty());
            let after = responder.page_work();
            assert_eq!(after.visits, before.visits);
            assert_eq!(after.actors, before.actors);
            assert_eq!(after.edges, before.edges);
            assert_eq!(after.decoded, before.decoded);
            assert_eq!(after.kept_bytes, before.kept_bytes);
            assert!(after.authorization_reads > before.authorization_reads);
            assert!(after.authorization_reads - before.authorization_reads <= 16);
            assert!(
                after.preparation - before.preparation
                    <= 16 * crate::sync::MAX_REQUEST_ITEMS as u64
            );
            let mut restarted = false;
            let mut admitted = BTreeSet::from([genesis.id]);
            let mut ended = false;
            for _ in 0..512 {
                let before = responder.page_work();
                let budget = PageBudget {
                    ops: 1,
                    bytes: crate::sync::MAX_PAGE_BYTES,
                };
                let page = if informed {
                    responder.response_with(
                        peer,
                        &request,
                        budget,
                        &receiver.summary(topic).unwrap(),
                    )
                } else {
                    responder.response_page(peer, &request, budget)
                }
                .unwrap();
                let after = responder.page_work();
                assert!(
                    after.visits - before.visits + after.actors - before.actors <= limit as u64
                );
                assert!(after.edges - before.edges <= limit as u64);
                assert!(after.kept_bytes < 8 * 1024 * 1024);
                assert!(page.ops.len() <= 1);
                assert!(page.missing.is_empty());
                assert!(page.positions.is_empty());
                for op in &page.ops {
                    assert!(op.signed.body.deps.is_subset(&admitted));
                    assert!(admitted.insert(op.id), "confirmed output was repeated");
                    request.wants.remove(&op.id);
                }
                destination.receive_ops(page.ops).unwrap();
                let clock = destination.storage().actor_clock(&topic).unwrap();
                for hint in &mut request.actor_range_hints {
                    hint.from_exclusive = clock.get(&hint.actor_id);
                }
                if !restarted && page.continued {
                    responder =
                        SyncEngine::new(source.clone(), owner.peer_id()).with_page_visits(limit, 1);
                    restarted = true;
                }
                if !page.more {
                    assert!(!page.continued);
                    ended = true;
                    break;
                }
            }
            assert!(ended, "repair cursor never completed");
            assert!(restarted);
            assert!(request.wants.is_empty());
            assert_eq!(admitted.len(), roots.len() + 1);
            assert!(responder.page_work().resumed > 0);
        }
    }
}

#[test]
fn memory_repair_slices() {
    repair_slices(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_repair_slices() {
    let directory = tempfile::tempdir().unwrap();
    repair_slices(crate::storage::FjallStorage::open(directory.path()).unwrap());
}

fn missing_barrier<S: Corrupt>(storage: S) {
    let owner = Ed25519Signer::from_bytes(&[223; 32]);
    let peer = Ed25519Signer::from_bytes(&[224; 32]).peer_id();
    let topic = TopicId::hash(b"repair-missing-barrier");
    let actor = actor_id_for(topic, owner.peer_id());
    let source = Oplog::with_storage(storage.clone());
    let genesis = source
        .create_topic_genesis(
            topic,
            actor,
            TopicGenesis::new(Note::TYPE_ID, [owner.peer_id(), peer]),
            &owner,
        )
        .unwrap();
    let mut ops = Vec::new();
    for text in ["missing", "dependent"] {
        ops.push(
            source
                .create_event_op(
                    topic,
                    actor,
                    EventEnvelope::encode_event(&Note { text: text.into() }).unwrap(),
                    &owner,
                )
                .unwrap(),
        );
    }
    storage.drop_op_record(&ops[0].id);
    for informed in [false, true] {
        let responder = SyncEngine::new(source.clone(), owner.peer_id()).with_page_visits(2, 1);
        let request = SyncRequest {
            topic_id: topic,
            known: [genesis.id].into(),
            wants: ops.iter().map(|op| op.id).collect(),
            actor_range_hints: vec![ActorRangeHint {
                actor_id: actor,
                from_exclusive: 3,
                to_inclusive: 3,
            }],
            genesis: Some(genesis.id),
            credit: Default::default(),
            window: Default::default(),
        };
        let mut missing = false;
        for _ in 0..32 {
            let page = if informed {
                responder.response_with(
                    peer,
                    &request,
                    PageBudget::from_credit(request.credit),
                    &responder.summary(topic).unwrap(),
                )
            } else {
                responder.response_page(peer, &request, PageBudget::from_credit(request.credit))
            }
            .unwrap();
            assert!(
                page.ops.is_empty(),
                "an explicit unavailable want was treated as held"
            );
            if page.missing.contains(&ops[0].id) {
                missing = true;
                break;
            }
            assert!(page.more && page.continued);
        }
        assert!(missing);
    }
}

#[test]
fn memory_missing_barrier() {
    missing_barrier(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_missing_barrier() {
    let directory = tempfile::tempdir().unwrap();
    missing_barrier(crate::storage::FjallStorage::open(directory.path()).unwrap());
}

#[test]
fn unconfirmed_holes_replay() {
    let owner = Ed25519Signer::from_bytes(&[226; 32]);
    let peer = Ed25519Signer::from_bytes(&[227; 32]).peer_id();
    let topic = TopicId::hash(b"unconfirmed-repair-holes");
    let actor = actor_id_for(topic, owner.peer_id());
    let source = Oplog::new();
    let genesis = source
        .create_topic_genesis(
            topic,
            actor,
            TopicGenesis::new(Note::TYPE_ID, [owner.peer_id(), peer]),
            &owner,
        )
        .unwrap();
    let mut ops = vec![genesis.clone()];
    for text in ["first hole", "second hole"] {
        ops.push(
            source
                .create_event_op(
                    topic,
                    actor,
                    EventEnvelope::encode_event(&Note { text: text.into() }).unwrap(),
                    &owner,
                )
                .unwrap(),
        );
    }
    for informed in [false, true] {
        let store = MemoryStorage::new();
        let destination = Oplog::with_storage(store.clone());
        destination.receive_ops(ops.clone()).unwrap();
        for op in &ops[1..] {
            store.drop_op_record(&op.id);
        }
        let receiver = SyncEngine::new(destination.clone(), peer);
        let responder = SyncEngine::new(source.clone(), owner.peer_id()).with_page_visits(2, 1);
        let mut request = SyncRequest {
            topic_id: topic,
            known: [genesis.id].into(),
            wants: ops[1..].iter().map(|op| op.id).collect(),
            actor_range_hints: vec![ActorRangeHint {
                actor_id: actor,
                from_exclusive: 3,
                to_inclusive: 3,
            }],
            genesis: Some(genesis.id),
            credit: Default::default(),
            window: Default::default(),
        };
        let next = |request: &SyncRequest| {
            for _ in 0..64 {
                let budget = PageBudget {
                    ops: 1,
                    bytes: crate::sync::MAX_PAGE_BYTES,
                };
                let page = if informed {
                    responder.response_with(
                        peer,
                        request,
                        budget,
                        &receiver.summary(topic).unwrap(),
                    )
                } else {
                    responder.response_page(peer, request, budget)
                }
                .unwrap();
                if !page.ops.is_empty() {
                    return page;
                }
                assert!(page.more && page.continued);
            }
            panic!("repair replay did not produce a data page");
        };
        let first = next(&request);
        assert_eq!(first.ops, vec![ops[1].clone()]);
        assert!(first.more);
        let replay = next(&request);
        assert_eq!(
            replay.ops, first.ops,
            "a clock cannot confirm an explicit body hole"
        );
        assert!(replay.more);
        destination.receive_ops(replay.ops).unwrap();
        request.wants.remove(&ops[1].id);
        let last = next(&request);
        assert_eq!(last.ops, vec![ops[2].clone()]);
        assert!(!last.more);
        destination.receive_ops(last.ops).unwrap();
        assert_eq!(store.get_op(&ops[1].id).unwrap(), Some(ops[1].clone()));
        assert_eq!(store.get_op(&ops[2].id).unwrap(), Some(ops[2].clone()));
    }
}
