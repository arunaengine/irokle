// SPDX-License-Identifier: MIT OR Apache-2.0
//! Response entry points: authorization and finite clock capture precede bounded page
//! traversal, and completion covers repair bodies and the requested forward goal together.

use crate::sync::*;
use crate::tests::support::{Note, StaleReadStorage, forked_side};
use crate::{
    Ed25519Signer, Event, EventEnvelope, MemoryStorage, Signer, TopicControl, TopicGenesis,
    actor_id_for,
};

fn topic(informed: bool) -> TopicId {
    TopicId::hash(if informed {
        b"capture-summary".as_slice()
    } else {
        b"capture-bare".as_slice()
    })
}

fn append<S: Storage>(log: &Oplog<S>, topic: TopicId, signer: &Ed25519Signer) {
    log.create_event_op(
        topic,
        crate::actor_id_for(topic, signer.peer_id()),
        EventEnvelope::encode_event(&Note {
            text: "bounded capture".into(),
        })
        .unwrap(),
        signer,
    )
    .unwrap();
}

fn seed<S: Storage>(store: S) {
    let signer = Ed25519Signer::from_bytes(&[188; 32]);
    let peer = Ed25519Signer::from_bytes(&[189; 32]).peer_id();
    let log = Oplog::with_storage(store);
    for informed in [false, true] {
        let topic = topic(informed);
        log.create_topic_genesis(
            topic,
            crate::actor_id_for(topic, signer.peer_id()),
            TopicGenesis::new(Note::TYPE_ID, [signer.peer_id(), peer]),
            &signer,
        )
        .unwrap();
        for _ in 0..4 {
            append(&log, topic, &signer);
        }
    }
}

fn capture_progress<S: Storage>(store: S, counters: fn(&S) -> crate::storage::CounterSnapshot) {
    let signer = Ed25519Signer::from_bytes(&[188; 32]);
    let peer = Ed25519Signer::from_bytes(&[189; 32]).peer_id();
    for informed in [false, true] {
        let topic = topic(informed);
        let actor = crate::actor_id_for(topic, signer.peer_id());
        let storage = StaleReadStorage::new(store.clone());
        let log = Oplog::with_storage(storage.clone());
        let engine = SyncEngine::new(log.clone(), signer.peer_id()).with_page_visits(1, 16);
        let genesis = store.topic_state(&topic).unwrap().unwrap().genesis;
        let receiver = Oplog::new();
        receiver
            .receive_ops(vec![store.get_op(&genesis).unwrap().unwrap()])
            .unwrap();
        let remote = SyncEngine::new(receiver.clone(), peer);
        let credit = SyncCredit {
            ops: 1,
            bytes: MAX_PAGE_BYTES as u64,
        };
        let budget = PageBudget::from_credit(credit);
        let mut request = SyncRequest {
            topic_id: topic,
            genesis: Some(genesis),
            credit,
            actor_range_hints: vec![ActorRangeHint {
                actor_id: actor,
                from_exclusive: 1,
                to_inclusive: 5,
            }],
            wants: BTreeSet::new(),
            known: BTreeSet::new(),
            window: Default::default(),
        };
        let mut calls = 0;
        loop {
            calls += 1;
            assert!(calls < 128, "captured goal did not finish");
            let before = counters(&store);
            let work = engine.page_work();
            let page = if informed {
                engine.response_with(peer, &request, budget, &remote.summary(topic).unwrap())
            } else {
                engine.response_page(peer, &request, budget)
            }
            .unwrap();
            let after = engine.page_work();
            assert!(after.visits - work.visits <= 1);
            assert!(after.edges - work.edges <= 1);
            assert!(after.authorization_reads - work.authorization_reads <= 16);
            assert!(after.preparation - work.preparation <= (16 * MAX_REQUEST_ITEMS) as u64);
            assert!(after.captured - work.captured <= MAX_PAGE_BYTES as u64);
            assert!(counters(&store).meta_reads > before.meta_reads);
            assert_eq!(storage.sync_counts.lock().unwrap()["clock_capture"], 1);
            assert_eq!(storage.sync_counts.lock().unwrap()["authorization"], calls);
            if calls == 1 {
                append(&log, topic, &signer);
            }
            assert!(page.ops.iter().all(|op| op.signed.body.actor_seq <= 5));
            receiver.receive_ops(page.ops).unwrap();
            request.actor_range_hints[0].from_exclusive =
                receiver.storage().actor_clock(&topic).unwrap().get(&actor);
            if !page.more {
                break;
            }
        }
        assert_eq!(
            receiver.storage().actor_clock(&topic).unwrap().get(&actor),
            5
        );
        assert_eq!(store.actor_clock(&topic).unwrap().get(&actor), 6);
        request.actor_range_hints[0].to_inclusive = 6;
        let first = engine.response_page(peer, &request, budget).unwrap();
        assert!(first.more);
        let captures = storage.sync_counts.lock().unwrap()["clock_capture"];
        log.create_control_op(topic, actor, TopicControl::RemovePeer { peer }, &signer)
            .unwrap();
        let denied = engine.response_page(peer, &request, budget).unwrap();
        assert!(denied.ops.is_empty() && !denied.more);
        assert_eq!(
            storage.sync_counts.lock().unwrap()["clock_capture"],
            captures
        );
    }
}

#[test]
fn memory_capture_reused() {
    let store = MemoryStorage::new();
    seed(store.clone());
    capture_progress(store, MemoryStorage::counters);
}

#[cfg(feature = "fjall")]
#[test]
fn reopened_capture_reused() {
    let directory = tempfile::tempdir().unwrap();
    seed(crate::FjallStorage::open(directory.path()).unwrap());
    capture_progress(
        crate::FjallStorage::open(directory.path()).unwrap(),
        crate::FjallStorage::counters,
    );
}

/// A response over a snapshot its caller holds admits the same snapshot, clock
/// and page work as one that opens its own snapshot.
#[cfg(feature = "iroh")]
fn held_admission<S: Storage>(store: S) {
    seed(store.clone());
    let signer = Ed25519Signer::from_bytes(&[188; 32]);
    let peer = Ed25519Signer::from_bytes(&[189; 32]).peer_id();
    let topic = topic(false);
    let request = SyncRequest {
        topic_id: topic,
        genesis: store
            .topic_state(&topic)
            .unwrap()
            .map(|state| state.genesis),
        credit: SyncCredit::default(),
        actor_range_hints: vec![ActorRangeHint {
            actor_id: actor_id_for(topic, signer.peer_id()),
            from_exclusive: 1,
            to_inclusive: 5,
        }],
        wants: BTreeSet::new(),
        known: BTreeSet::new(),
        window: Default::default(),
    };
    let budget = PageBudget::from_credit(request.credit);
    let log = Oplog::with_storage(store.clone());
    let opened = SyncEngine::new(log.clone(), signer.peer_id());
    let held = SyncEngine::new(log, signer.peer_id());
    let page = opened.response_page(peer, &request, budget).unwrap();
    let inside = store
        .read_snapshot(|read| held.response_in(read, peer, &request, budget))
        .unwrap();
    assert_eq!(page.ops.len(), 4);
    assert_eq!(inside, page);
    assert_eq!(held.page_work(), opened.page_work());
}

#[cfg(feature = "iroh")]
#[test]
fn memory_held_admission() {
    held_admission(MemoryStorage::new());
}

#[cfg(all(feature = "iroh", feature = "fjall"))]
#[test]
fn fjall_held_admission() {
    let directory = tempfile::tempdir().unwrap();
    held_admission(crate::FjallStorage::open(directory.path()).unwrap());
}

fn reset_reauthorizes<S: Storage>(store: S) {
    let signer = Ed25519Signer::from_bytes(&[190; 32]);
    let peer = Ed25519Signer::from_bytes(&[191; 32]).peer_id();
    for informed in [false, true] {
        let topic = TopicId::hash([b"capture-reset".as_slice(), &[u8::from(informed)]].concat());
        let (_, _, a, ae) = forked_side(MemoryStorage::new(), topic, 190, [peer], "a");
        let (_, _, b, be) = forked_side(
            MemoryStorage::new(),
            topic,
            190,
            [peer, Ed25519Signer::from_bytes(&[192; 32]).peer_id()],
            "b",
        );
        let (old, new) = if a.id > b.id {
            ([a, ae], [b, be])
        } else {
            ([b, be], [a, ae])
        };
        let log = Oplog::with_storage(store.clone());
        log.receive_ops(old.to_vec()).unwrap();
        let engine = SyncEngine::new(log.clone(), signer.peer_id()).with_page_visits(1, 16);
        let remote = SyncEngine::new(Oplog::new(), peer);
        let credit = SyncCredit {
            ops: 1,
            bytes: MAX_PAGE_BYTES as u64,
        };
        let budget = PageBudget::from_credit(credit);
        let request = SyncRequest {
            topic_id: topic,
            genesis: Some(old[0].id),
            credit,
            actor_range_hints: vec![ActorRangeHint {
                actor_id: old[0].signed.body.actor_id,
                from_exclusive: 0,
                to_inclusive: 2,
            }],
            wants: BTreeSet::new(),
            known: BTreeSet::new(),
            window: Default::default(),
        };
        let summary = remote.summary(topic).unwrap();
        let first = if informed {
            engine.response_with(peer, &request, budget, &summary)
        } else {
            engine.response_page(peer, &request, budget)
        }
        .unwrap();
        assert!(first.more);
        log.receive_ops_from_peer_evicting(Some(signer.peer_id()), new.to_vec())
            .unwrap();
        let result = if informed {
            engine.response_with(peer, &request, budget, &summary)
        } else {
            engine.response_page(peer, &request, budget)
        };
        assert!(matches!(result, Err(Error::StaleIncarnation)));
    }
}

#[test]
fn memory_reset_reauthorizes() {
    reset_reauthorizes(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_reset_reauthorizes() {
    let directory = tempfile::tempdir().unwrap();
    reset_reauthorizes(crate::FjallStorage::open(directory.path()).unwrap());
}

fn repair_completion<S: Storage>(storage: S) {
    let signer = Ed25519Signer::from_bytes(&[203; 32]);
    let peer = Ed25519Signer::from_bytes(&[204; 32]).peer_id();
    let topic = TopicId::hash(b"repair-page-completion");
    let actor = actor_id_for(topic, signer.peer_id());
    let source = Oplog::with_storage(storage);
    let genesis = source
        .create_topic_genesis(
            topic,
            actor,
            TopicGenesis::new(Note::TYPE_ID, [signer.peer_id(), peer]),
            &signer,
        )
        .unwrap();
    let mut chain = vec![genesis];
    for text in ["one", "two"] {
        chain.push(
            source
                .create_event_op(
                    topic,
                    actor,
                    EventEnvelope::encode_event(&Note { text: text.into() }).unwrap(),
                    &signer,
                )
                .unwrap(),
        );
    }
    let engine = SyncEngine::new(source, signer.peer_id());
    for later in [false, true] {
        for informed in [false, true] {
            for exact_bytes in [false, true] {
                for forward in [false, true] {
                    let reader = Oplog::new();
                    reader
                        .receive_ops(chain[..if later { 1 } else { 2 }].to_vec())
                        .unwrap();
                    let receiver = SyncEngine::new(reader.clone(), peer);
                    let credit = SyncCredit {
                        ops: if exact_bytes { 10 } else { 1 },
                        bytes: if exact_bytes {
                            postcard::experimental::serialized_size(&chain[1]).unwrap() as u64
                        } else {
                            crate::sync::MAX_PAGE_BYTES as u64
                        },
                    };
                    let mut request = SyncRequest {
                        topic_id: topic,
                        known: BTreeSet::new(),
                        wants: [chain[if later { 2 } else { 1 }].id].into(),
                        actor_range_hints: vec![ActorRangeHint {
                            actor_id: actor,
                            from_exclusive: if later { 1 } else { 2 },
                            to_inclusive: if forward { 3 } else { 2 },
                        }],
                        genesis: Some(chain[0].id),
                        credit,
                        window: Default::default(),
                    };
                    let mut exchanges = 0;
                    if later && forward {
                        assert!(crate::sync::forward_remaining(
                            &request,
                            &engine.summary(topic).unwrap().actor_clock,
                            &chain[2..],
                        ));
                    }
                    loop {
                        assert!(exchanges < 3, "completion requested an extra exchange");
                        let budget = PageBudget::from_credit(credit);
                        let page = if informed {
                            engine.response_with(
                                peer,
                                &request,
                                budget,
                                &receiver.summary(topic).unwrap(),
                            )
                        } else {
                            engine.response_page(peer, &request, budget)
                        }
                        .unwrap();
                        assert_eq!(page.ops.len(), 1);
                        assert_eq!(page.ops[0], chain[exchanges + 1]);
                        assert_eq!(page.more, (forward || later) && exchanges == 0);
                        assert!(!page.continued);
                        assert!(page.missing.is_empty());
                        for op in &page.ops {
                            request.wants.remove(&op.id);
                        }
                        reader.receive_ops(page.ops).unwrap();
                        request.actor_range_hints[0].from_exclusive =
                            reader.storage().actor_clock(&topic).unwrap().get(&actor);
                        exchanges += 1;
                        if !page.more {
                            break;
                        }
                    }
                    assert_eq!(exchanges, if forward || later { 2 } else { 1 });
                    request.wants.clear();
                    for pending in [false, true] {
                        request.actor_range_hints[0].from_exclusive = 2;
                        request.actor_range_hints[0].to_inclusive = if pending { 3 } else { 2 };
                        let budget = PageBudget { ops: 0, bytes: 0 };
                        let page = if informed {
                            engine.response_with(
                                peer,
                                &request,
                                budget,
                                &receiver.summary(topic).unwrap(),
                            )
                        } else {
                            engine.response_page(peer, &request, budget)
                        }
                        .unwrap();
                        assert_eq!(page.more, pending);
                        assert!(page.ops.is_empty());
                    }
                }
            }
        }
    }
}

#[test]
fn memory_repair_completion() {
    repair_completion(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_repair_completion() {
    let directory = tempfile::tempdir().unwrap();
    repair_completion(crate::storage::FjallStorage::open(directory.path()).unwrap());
}
