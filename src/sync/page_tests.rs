//! Completion covers repair bodies and the requested forward goal together.

use crate::oplog::Oplog;
use crate::tests::support::*;

use super::{ActorRangeHint, PageBudget, SyncCredit, SyncEngine, SyncRequest};

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
                            super::MAX_PAGE_BYTES as u64
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
                        assert!(super::forward_remaining(
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
