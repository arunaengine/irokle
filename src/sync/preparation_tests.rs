// SPDX-License-Identifier: MIT OR Apache-2.0
//! Authorization and finite clock capture precede bounded page traversal.

use super::*;
use crate::tests::support::{Note, StaleReadStorage, forked_side};
use crate::{
    Ed25519Signer, Event, EventEnvelope, MemoryStorage, Signer, TopicControl, TopicGenesis,
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
            assert!(after.auth_reads - work.auth_reads <= 16);
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

fn admission_first<S: Storage>(store: S, counters: fn(&S) -> crate::storage::CounterSnapshot) {
    seed(store.clone());
    let peer = Ed25519Signer::from_bytes(&[189; 32]).peer_id();
    for clock in [false, true] {
        let before = counters(&store);
        let mut calls = 0;
        let result = store.read_snapshot(|read| {
            let mut refuse = |_| {
                calls += 1;
                Err(Error::SyncCapacity(
                    "test preparation admission refused".into(),
                ))
            };
            if clock {
                read.sync_clock(&topic(false), None, &mut refuse).map(drop)
            } else {
                read.sync_identity(&topic(false), &peer, &mut refuse)
                    .map(drop)
            }
        });
        assert!(matches!(result, Err(Error::SyncCapacity(_))));
        assert_eq!(calls, 1);
        assert_eq!(counters(&store).meta_reads, before.meta_reads);
    }
}

#[test]
fn memory_read_admission() {
    admission_first(MemoryStorage::new(), MemoryStorage::counters);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_read_admission() {
    let directory = tempfile::tempdir().unwrap();
    admission_first(
        crate::FjallStorage::open(directory.path()).unwrap(),
        crate::FjallStorage::counters,
    );
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

#[test]
fn unsupported_capture_blocks() {
    struct Unsupported;
    impl SnapshotRead for Unsupported {
        fn topic_view(&self, _: &TopicId, _: Option<&PeerId>) -> Result<Option<TopicView>> {
            panic!("unbounded topic fallback");
        }
        fn get_op(&self, _: &OpId) -> Result<Option<Op>> {
            panic!("unexpected read");
        }
        fn get_meta(&self, _: &OpId) -> Result<Option<crate::storage::OpMeta>> {
            panic!("unexpected read");
        }
        fn dep_resolvable(&self, _: &OpId) -> Result<bool> {
            panic!("unexpected read");
        }
        fn actor_range(
            &self,
            _: &TopicId,
            _: &ActorId,
            _: u64,
            _: usize,
        ) -> Result<Vec<(u64, OpId)>> {
            panic!("unexpected read");
        }
        fn list_op_ids(&self, _: &TopicId) -> Result<BTreeSet<OpId>> {
            panic!("unexpected read");
        }
    }
    let peer = PeerId::from_bytes([1; 32]);
    assert!(matches!(
        Unsupported.sync_identity(&topic(false), &peer, &mut |_| Ok(())),
        Err(Error::SyncCapacity(_))
    ));
    assert!(matches!(
        Unsupported.sync_clock(&topic(false), None, &mut |_| Ok(())),
        Err(Error::SyncCapacity(_))
    ));
}
