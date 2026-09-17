//! Every page either carries data or names why it cannot: deep dependency
//! chains across a bounded actor window, explicit hole chains longer than one
//! page, records the source lacks, and request sets past the item limit.

use super::pages::{Source, independent_chains, page_through, request_for};
use super::support::*;

use crate::oplog::Oplog;
use crate::sync::{ActorRangeHint, PageBudget, SyncCredit, SyncEngine, SyncRequest, SyncSummary};

/// `actors` writers with one op each, sorted by actor key, where every writer's
/// op depends on the op of the writer with the next larger key and the last one
/// on the genesis. A window taken in key order selects dependents first.
pub(super) fn reverse_chain<S: Storage>(storage: S, actors: usize) -> Source<S> {
    let owner = Ed25519Signer::from_bytes(&[230; 32]);
    let reader = Ed25519Signer::from_bytes(&[231; 32]).peer_id();
    let topic_id = TopicId::hash([b"reverse-chain".as_slice(), &actors.to_le_bytes()].concat());
    let mut writers = (0..actors)
        .map(|index| {
            let mut seed = [11_u8; 32];
            seed[..8].copy_from_slice(&(index as u64).to_le_bytes());
            Ed25519Signer::from_bytes(&seed)
        })
        .collect::<Vec<_>>();
    writers.sort_by_key(|writer| actor_id_for(topic_id, writer.peer_id()));
    let members = writers
        .iter()
        .chain([&owner])
        .map(Signer::peer_id)
        .chain([reader])
        .collect::<BTreeSet<_>>();
    let genesis = Op::sign(
        OpBody {
            topic_id,
            author: owner.peer_id(),
            actor_id: actor_id_for(topic_id, owner.peer_id()),
            actor_seq: 1,
            actor_prev: None,
            deps: BTreeSet::new(),
            generation: 0,
            payload: TopicPayload::Genesis(TopicGenesis::new(Note::TYPE_ID, members)),
        },
        &owner,
    )
    .unwrap();
    let mut ops = vec![genesis.clone()];
    let mut dep = genesis.id;
    for (generation, writer) in writers.iter().rev().enumerate() {
        let op = Op::sign(
            OpBody {
                topic_id,
                author: writer.peer_id(),
                actor_id: actor_id_for(topic_id, writer.peer_id()),
                actor_seq: 1,
                actor_prev: None,
                deps: [dep].into(),
                generation: generation as u64 + 1,
                payload: TopicPayload::Event(
                    EventEnvelope::encode_event(&Note { text: "x".into() }).unwrap(),
                ),
            },
            writer,
        )
        .unwrap();
        dep = op.id;
        ops.push(op);
    }
    let log = Oplog::with_storage(storage);
    for batch in ops.chunks(4096) {
        log.receive_ops(batch.to_vec()).unwrap();
    }
    Source {
        engine: SyncEngine::new(log.clone(), owner.peer_id()),
        log,
        topic_id,
        reader,
        genesis,
    }
}

/// With a two-actor window, a chain through eight actors in reverse key order
/// completes in one page instead of stalling on waiters that fill the window.
fn assert_chain_suspends<S: Storage>(storage: S) {
    let mut source = reverse_chain(storage, 8);
    source.engine = source.engine.clone().with_page_actors(2);
    assert_eq!(page_through(&source, SyncCredit::default()), 1);
    // Tight pages still advance on every page.
    let credit = SyncCredit {
        ops: 3,
        bytes: u64::MAX,
    };
    assert_eq!(page_through(&source, credit), 3);
}

#[test]
fn memory_chain_suspends() {
    assert_chain_suspends(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_chain_suspends() {
    let dir = tempfile::tempdir().unwrap();
    assert_chain_suspends(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// Suspended waiters cost bounded work: the whole chain page reads a few
/// records per op and per actor, not a repeated traversal.
#[test]
fn chain_work_bounded() {
    let actors = 64;
    let mut source = reverse_chain(MemoryStorage::new(), actors);
    source.engine = source.engine.clone().with_page_actors(4);
    let reader = Oplog::new();
    reader.receive_ops(vec![source.genesis.clone()]).unwrap();
    let request = request_for(&source, &reader, SyncCredit::default());
    let before = source.log.storage().counters();
    let page = source
        .engine
        .response_page(
            source.reader,
            &request,
            PageBudget::from_credit(request.credit),
        )
        .unwrap();
    let after = source.log.storage().counters();
    assert_eq!(page.ops.len(), actors);
    let reads = (after.meta_reads - before.meta_reads)
        + (after.index_reads - before.index_reads)
        + (after.op_reads - before.op_reads);
    assert!(
        reads <= 6 * actors as u64 + 8,
        "{actors} chained ops cost {reads} reads"
    );
}

/// A chain the first slice holds whole before it sends needs more planner bytes
/// than a kept plan may hold; the slice still serves its page.
#[test]
fn deep_chain_served() {
    let source = reverse_chain(MemoryStorage::new(), 5500);
    let pages = page_through(&source, SyncCredit::default());
    assert!(pages <= 2, "5500 chained actors took {pages} pages");
}

/// The real window boundary: a reverse chain through twice the window and two
/// more actors. Run explicitly: `cargo test --lib window_chain_boundary -- --ignored`.
#[test]
#[ignore = "builds thousands of members, run explicitly"]
fn window_chain_boundary() {
    for actors in [8193, 8194] {
        let source = reverse_chain(MemoryStorage::new(), actors);
        let pages = page_through(&source, SyncCredit::default());
        assert!(pages <= 3, "{actors} chained actors took {pages} pages");
    }
}

/// A reader whose clock covers a chain but lost more positions than one page carries asks
/// for explicit wants. Pages serve oldest wants first, regardless of id order; the lost run
/// makes its largest id the top op, which an id-order walk starts from.
fn assert_hole_chain<S: Corrupt>(reader_store: S) {
    let reader_id = Ed25519Signer::from_bytes(&[250; 32]).peer_id();
    let source_log = Oplog::new();
    let (genesis, chains) = independent_chains(&source_log, reader_id, &[12_000]);
    let chain = &chains[0];
    let owner = Ed25519Signer::from_bytes(&[244; 32]).peer_id();
    let source = Source {
        engine: SyncEngine::new(source_log.clone(), owner),
        log: source_log,
        topic_id: genesis.signed.body.topic_id,
        reader: reader_id,
        genesis: genesis.clone(),
    };
    let reader = Oplog::with_storage(reader_store.clone());
    reader.receive_ops(vec![genesis]).unwrap();
    for batch in chain.chunks(4096) {
        reader.receive_ops(batch.to_vec()).unwrap();
    }
    // A walk from the wants in id order starts at the largest id; the lost range
    // ends at a position whose id exceeds every id since the range's start,
    // more than one page above that start.
    let lost = lost_range(chain).expect("a range whose largest id is high in the chain");
    for op in &chain[lost] {
        reader_store.drop_op_record(&op.id);
    }
    reader.recheck_topics().unwrap();

    let summary = source.engine.summary(source.topic_id).unwrap();
    let reader_engine = SyncEngine::new(reader.clone(), source.reader);
    let mut pages = 0;
    loop {
        let request = reader_engine.plan_request(source.reader, &summary).unwrap();
        if request.wants.is_empty() && request.actor_range_hints.is_empty() {
            break;
        }
        assert!(
            request.actor_range_hints.is_empty(),
            "no forward range rescues the holes"
        );
        assert!(pages < 4, "the hole chain did not finish");
        let page = source
            .engine
            .response_page(
                source.reader,
                &request,
                PageBudget::from_credit(request.credit),
            )
            .unwrap();
        assert!(!page.ops.is_empty(), "page {pages} carried nothing");
        assert!(page.missing.is_empty());
        reader.receive_ops(page.ops).unwrap();
        reader.recheck_topics().unwrap();
        pages += 1;
    }
    assert_eq!(pages, 3);
    assert!(
        reader
            .topic_unresolved(&source.topic_id)
            .unwrap()
            .is_empty()
    );
}

/// A run of chain positions whose largest id is its last op, more than one
/// page above the run's first op.
fn lost_range(chain: &[Op]) -> Option<std::ops::RangeInclusive<usize>> {
    let mut start = 1;
    while start < chain.len() {
        let mut top = start;
        for position in start..chain.len() {
            if chain[position].id > chain[top].id {
                top = position;
                if top - start > 4096 {
                    return Some(start..=top);
                }
            }
        }
        start = top + 1;
    }
    None
}

#[test]
fn memory_hole_chain() {
    assert_hole_chain(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_hole_chain() {
    let dir = tempfile::tempdir().unwrap();
    assert_hole_chain(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// A source that lost one record of a chain serves an independent chain beside
/// it, names the lost record, and reports nothing more once the independent
/// chain is done, so a caller never repeats an identical empty page.
#[test]
fn missing_named_beside() {
    let reader_id = Ed25519Signer::from_bytes(&[250; 32]).peer_id();
    let source_store = MemoryStorage::new();
    let source_log = Oplog::with_storage(source_store.clone());
    let (genesis, chains) = independent_chains(&source_log, reader_id, &[20, 30]);
    let lost = chains[0][9].id;
    source_store.drop_op_record(&lost);
    let owner = Ed25519Signer::from_bytes(&[244; 32]).peer_id();
    let source = Source {
        engine: SyncEngine::new(source_log.clone(), owner),
        log: source_log,
        topic_id: genesis.signed.body.topic_id,
        reader: reader_id,
        genesis: genesis.clone(),
    };
    let reader = Oplog::new();
    reader.receive_ops(vec![genesis]).unwrap();

    let request = request_for(&source, &reader, SyncCredit::default());
    let page = source
        .engine
        .response_page(
            source.reader,
            &request,
            PageBudget::from_credit(request.credit),
        )
        .unwrap();
    assert_eq!(page.missing, [lost].into());
    assert_eq!(page.ops.len(), 9 + 30);
    reader.receive_ops(page.ops).unwrap();

    let request = request_for(&source, &reader, SyncCredit::default());
    let page = source
        .engine
        .response_page(
            source.reader,
            &request,
            PageBudget::from_credit(request.credit),
        )
        .unwrap();
    assert!(page.ops.is_empty());
    assert_eq!(
        page.missing,
        [lost].into(),
        "an empty page names its reason"
    );
}

/// A want the source does not hold at all is named, not silently skipped.
#[test]
fn unknown_want_named() {
    let reader_id = Ed25519Signer::from_bytes(&[250; 32]).peer_id();
    let source_log = Oplog::new();
    let (genesis, _) = independent_chains(&source_log, reader_id, &[3]);
    let owner = Ed25519Signer::from_bytes(&[244; 32]).peer_id();
    let engine = SyncEngine::new(source_log, owner);
    let unknown = OpId::hash(b"never stored");
    let request = sync::SyncRequest {
        topic_id: genesis.signed.body.topic_id,
        known: BTreeSet::new(),
        wants: [unknown].into(),
        actor_range_hints: Vec::new(),
        genesis: Some(genesis.id),
        credit: SyncCredit::default(),
        window: crate::sync::ActorWindow::default(),
    };
    let page = engine
        .response_page(reader_id, &request, PageBudget::from_credit(request.credit))
        .unwrap();
    assert!(page.ops.is_empty() && !page.more);
    assert_eq!(page.missing, [unknown].into());
}

/// A summary naming more unresolvable heads than one request may carry yields
/// a request the responder accepts, instead of one it refuses on every attempt.
fn assert_wants_fit<S: Storage>(storage: S) {
    let reader_id = Ed25519Signer::from_bytes(&[250; 32]).peer_id();
    let (genesis, chains) = independent_chains(&Oplog::new(), reader_id, &[3]);
    let source_log = Oplog::with_storage(storage);
    source_log.receive_ops(vec![genesis.clone()]).unwrap();
    source_log.receive_ops(chains[0].clone()).unwrap();
    let topic_id = genesis.signed.body.topic_id;
    let owner = Ed25519Signer::from_bytes(&[244; 32]).peer_id();
    for informed in [false, true] {
        let source = SyncEngine::new(source_log.clone(), owner);
        let reader = Oplog::new();
        reader.receive_ops(vec![genesis.clone()]).unwrap();
        let reader_engine = SyncEngine::new(reader.clone(), reader_id);
        let mut summary: SyncSummary = source.summary(topic_id).unwrap();
        summary.heads = (0..70_000_u32)
            .map(|index| OpId::hash(index.to_le_bytes()))
            .collect();
        let request = reader_engine.plan_request(owner, &summary).unwrap();
        assert!(request.wants.len() + request.actor_range_hints.len() <= 65_536);
        assert!(!request.wants.is_empty() && request.wants.len() < summary.heads.len());
        let mut oversized = request.clone();
        oversized.wants = summary
            .heads
            .iter()
            .take(65_536 - request.actor_range_hints.len())
            .copied()
            .collect();
        let budget = PageBudget::from_credit(request.credit);
        let held = reader_engine.summary(topic_id).unwrap();
        let refused = if informed {
            source.response_with(reader_id, &oversized, budget, &held)
        } else {
            source.response_page(reader_id, &oversized, budget)
        };
        assert!(matches!(refused, Err(Error::SyncCapacity(_))));
        let mut missing = BTreeSet::new();
        let mut received = BTreeSet::new();
        let mut finished = false;
        for _ in 0..request.wants.len().div_ceil(sync::MAX_PAGE_MISSING) + 2 {
            let page = if informed {
                source.response_with(reader_id, &request, budget, &held)
            } else {
                source.response_page(reader_id, &request, budget)
            }
            .expect("a generated request is served within the workspace limit");
            missing.extend(page.missing);
            received.extend(page.ops.iter().map(|op| op.id));
            reader.receive_ops(page.ops).unwrap();
            if !page.more {
                finished = true;
                break;
            }
        }
        assert!(
            finished,
            "missing roots must not starve independent forward data"
        );
        assert_eq!(missing, request.wants);
        assert_eq!(received, chains[0].iter().map(|op| op.id).collect());
    }
}

#[test]
fn wants_fit_request() {
    assert_wants_fit(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_wants_fit() {
    let dir = tempfile::tempdir().unwrap();
    assert_wants_fit(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// A request accepted on one genesis is refused as stale once a reset replaced
/// the branch between two pages, rather than served from the new branch.
#[test]
fn reset_between_pages() {
    let branches = super::branch::branches(232);
    let storage = MemoryStorage::new();
    let log = Oplog::with_storage(storage.clone());
    log.receive_ops_from_peer(
        Some(branches.author.peer_id()),
        vec![branches.old.0.clone(), branches.old.1.clone()],
    )
    .unwrap();
    let engine = SyncEngine::new(log, branches.author.peer_id());
    let request = sync::SyncRequest {
        topic_id: branches.topic_id,
        known: BTreeSet::new(),
        wants: BTreeSet::new(),
        actor_range_hints: vec![sync::ActorRangeHint {
            actor_id: branches.old.1.signed.body.actor_id,
            from_exclusive: 0,
            to_inclusive: u64::MAX,
        }],
        genesis: Some(branches.old.0.id),
        credit: SyncCredit {
            ops: 1,
            bytes: u64::MAX,
        },
        window: crate::sync::ActorWindow::default(),
    };
    let member = branches.member.peer_id();
    let first = engine
        .response_page(member, &request, PageBudget::from_credit(request.credit))
        .unwrap();
    assert_eq!(first.ops[0].id, branches.old.0.id);
    assert!(first.more);
    super::branch::reset_to_new(&storage, &branches);
    assert!(matches!(
        engine.response_page(member, &request, PageBudget::from_credit(request.credit)),
        Err(Error::StaleIncarnation)
    ));
}

/// The served stream carries the missing record in its page result, so the
/// requester can tell a source that lacks data from one that ran out of budget.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_names_missing() {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let storage = MemoryStorage::new();
    let owner = Irokle::builder()
        .with_storage(storage.clone())
        .with_iroh_secret_key(endpoint.secret_key())
        .without_auto_accept()
        .build()
        .unwrap();
    let reader = Ed25519Signer::from_bytes(&[233; 32]).peer_id();
    let topic = owner
        .create_topic::<Note>(TopicConfig {
            initial_peers: [reader].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let records = (0..3)
        .map(|index| {
            topic
                .publish(Note {
                    text: format!("{index}"),
                })
                .unwrap()
                .meta
                .op_id
        })
        .collect::<Vec<_>>();
    storage.drop_op_record(&records[1]);
    let net = net::IrohNet::new(endpoint, owner.clone()).unwrap();
    let request = sync::SyncRequest {
        topic_id: topic.id(),
        known: BTreeSet::new(),
        wants: BTreeSet::new(),
        actor_range_hints: vec![sync::ActorRangeHint {
            actor_id: actor_id_for(topic.id(), owner.peer_id()),
            from_exclusive: 1,
            to_inclusive: u64::MAX,
        }],
        genesis: genesis_of(&storage, &topic.id()),
        credit: SyncCredit::default(),
        window: crate::sync::ActorWindow::default(),
    };
    let requester = iroh::EndpointId::from_bytes(reader.as_bytes()).unwrap();
    let responses = net
        .handle_messages(
            requester,
            vec![
                sync::SyncMessage::Open(crate::sync::SyncEngine::<MemoryStorage>::open(
                    topic.id(),
                    reader,
                    Some(Note::TYPE_ID.into()),
                )),
                sync::SyncMessage::Request(request),
            ],
        )
        .unwrap();
    let served = responses
        .iter()
        .filter_map(|message| match message {
            sync::SyncMessage::Data(data) => Some(data.ops.iter().map(|op| op.id)),
            _ => None,
        })
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(served, vec![records[0]]);
    let page = responses
        .iter()
        .find_map(|message| match message {
            sync::SyncMessage::Page(page) => Some(page.clone()),
            _ => None,
        })
        .expect("a page result");
    assert_eq!(page.missing, [records[1]].into());
    net.shutdown().await;
}

/// A page never exceeds the credit its request advertises, whatever larger
/// budget a direct caller passes, in operations or in serialized bytes.
#[test]
fn credit_binds_caller() {
    let reader_id = Ed25519Signer::from_bytes(&[250; 32]).peer_id();
    let source_log = Oplog::new();
    let (genesis, chains) = independent_chains(&source_log, reader_id, &[40]);
    let owner = Ed25519Signer::from_bytes(&[244; 32]).peer_id();
    let source = Source {
        engine: SyncEngine::new(source_log.clone(), owner),
        log: source_log,
        topic_id: genesis.signed.body.topic_id,
        reader: reader_id,
        genesis: genesis.clone(),
    };
    let reader = Oplog::new();
    reader.receive_ops(vec![genesis]).unwrap();
    let size = postcard::experimental::serialized_size(&chains[0][0]).unwrap() as u64;
    for credit in [
        SyncCredit {
            ops: 3,
            bytes: u64::MAX,
        },
        SyncCredit {
            ops: 4096,
            bytes: 2 * size + size / 2,
        },
    ] {
        let request = request_for(&source, &reader, credit);
        let page = source
            .engine
            .response_page(
                source.reader,
                &request,
                PageBudget {
                    ops: usize::MAX,
                    bytes: usize::MAX,
                },
            )
            .unwrap();
        let bytes = page
            .ops
            .iter()
            .map(|op| postcard::experimental::serialized_size(op).unwrap() as u64)
            .sum::<u64>();
        assert!(page.ops.len() <= credit.ops as usize, "{credit:?}");
        assert!(bytes <= credit.bytes, "{credit:?}");
        assert!(!page.ops.is_empty() && page.more, "{credit:?}");
    }
}

#[test]
fn finite_goals_progress() {
    struct Goal {
        peer: PeerId,
        receiver: Oplog<MemoryStorage>,
        done: bool,
    }

    let owner = Ed25519Signer::from_bytes(&[225; 32]);
    let peers = (101..118)
        .map(|seed| Ed25519Signer::from_bytes(&[seed; 32]).peer_id())
        .collect::<Vec<_>>();
    let topic = TopicId::hash(b"finite-goals-admission");
    let actor = actor_id_for(topic, owner.peer_id());
    let source = Oplog::new();
    let genesis = source
        .create_topic_genesis(
            topic,
            actor,
            TopicGenesis::new(
                Note::TYPE_ID,
                peers.iter().copied().chain([owner.peer_id()]),
            ),
            &owner,
        )
        .unwrap();
    for text in ["one", "two"] {
        source
            .create_event_op(
                topic,
                actor,
                EventEnvelope::encode_event(&Note { text: text.into() }).unwrap(),
                &owner,
            )
            .unwrap();
    }
    let responder = SyncEngine::new(source, owner.peer_id())
        .with_page_visits(1, crate::sync::MAX_CONTINUATIONS);
    let request = SyncRequest {
        topic_id: topic,
        known: [genesis.id].into(),
        wants: BTreeSet::new(),
        actor_range_hints: vec![ActorRangeHint {
            actor_id: actor,
            from_exclusive: 1,
            to_inclusive: 3,
        }],
        genesis: Some(genesis.id),
        credit: Default::default(),
        window: Default::default(),
    };
    let budget = PageBudget {
        ops: 1,
        bytes: crate::sync::MAX_PAGE_BYTES,
    };
    for peer in &peers[..crate::sync::MAX_CONTINUATIONS] {
        let page = responder.response_page(*peer, &request, budget).unwrap();
        assert!(page.ops.is_empty() && page.continued);
    }
    assert!(matches!(
        responder.response_page(peers[crate::sync::MAX_CONTINUATIONS], &request, budget),
        Err(Error::SyncCapacity(_))
    ));
    let mut goals = peers
        .into_iter()
        .map(|peer| {
            let receiver = Oplog::new();
            receiver.receive_ops(vec![genesis.clone()]).unwrap();
            Goal {
                peer,
                receiver,
                done: false,
            }
        })
        .collect::<Vec<_>>();
    let mut delivered = 0;
    let mut refused = 0;
    for _ in 0..128 {
        for goal in goals.iter_mut().filter(|goal| !goal.done) {
            let mut request = request.clone();
            request.actor_range_hints[0].from_exclusive = goal
                .receiver
                .storage()
                .actor_clock(&topic)
                .unwrap()
                .get(&actor);
            match responder.response_page(goal.peer, &request, budget) {
                Err(Error::SyncCapacity(_)) => refused += 1,
                Err(error) => panic!("unexpected admission failure: {error}"),
                Ok(page) => {
                    let count = page.ops.len();
                    assert!(count <= 1);
                    assert_eq!(goal.receiver.receive_ops(page.ops).unwrap().len(), count);
                    delivered += count;
                    goal.done = !page.more;
                    if goal.done {
                        assert_eq!(
                            goal.receiver
                                .storage()
                                .actor_clock(&topic)
                                .unwrap()
                                .get(&actor),
                            3
                        );
                    }
                }
            }
        }
        if goals.iter().all(|goal| goal.done) {
            break;
        }
    }
    assert!(
        goals.iter().all(|goal| goal.done),
        "finite goals starved under round-robin service"
    );
    assert_eq!(delivered, 34);
    assert!(refused > 0);
    assert_eq!(responder.page_work().kept_bytes, 0);
}
