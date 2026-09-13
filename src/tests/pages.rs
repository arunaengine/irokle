//! Pages serve causal prefixes of a goal within their budget, over many
//! actors, joins, large ops and repair wants.

use super::support::*;

use crate::oplog::Oplog;
use crate::sync::{PageBudget, SyncCredit, SyncEngine, SyncRequest};

pub(super) struct Source<S: Storage = MemoryStorage> {
    pub(super) log: Oplog<S>,
    pub(super) engine: SyncEngine<S>,
    pub(super) topic_id: TopicId,
    pub(super) reader: PeerId,
    pub(super) genesis: Op,
}

/// A topic with `actors` writers taking turns, every fifth op joining the
/// heads of all actors, and `text` bytes of payload per event.
fn many_actors(actors: u8, per_actor: usize, text: usize) -> Source {
    let owner = Ed25519Signer::from_bytes(&[240; 32]);
    let reader = Ed25519Signer::from_bytes(&[241; 32]).peer_id();
    let writers = (0..actors)
        .map(|index| {
            Ed25519Signer::from_bytes(&[
                index, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
                7, 7, 7, 7, 7, 7,
            ])
        })
        .collect::<Vec<_>>();
    let topic_id =
        TopicId::hash([b"pages".as_slice(), &[actors], &per_actor.to_le_bytes()].concat());
    let members = writers
        .iter()
        .map(Signer::peer_id)
        .chain([reader, owner.peer_id()])
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
    let log = Oplog::new();
    log.receive_ops(vec![genesis.clone()]).unwrap();
    let mut tips: Vec<Option<Op>> = vec![None; writers.len()];
    for round in 0..per_actor {
        for (index, writer) in writers.iter().enumerate() {
            let prev = tips[index].clone();
            let mut deps = prev.iter().map(|op| op.id).collect::<BTreeSet<_>>();
            if round % 5 == 4 {
                deps.extend(log.storage().heads(&topic_id).unwrap());
            }
            if deps.is_empty() {
                deps.insert(genesis.id);
            }
            let generation = deps
                .iter()
                .map(|dep| log.storage().get_meta(dep).unwrap().unwrap().generation + 1)
                .max()
                .unwrap();
            let op = Op::sign(
                OpBody {
                    topic_id,
                    author: writer.peer_id(),
                    actor_id: actor_id_for(topic_id, writer.peer_id()),
                    actor_seq: round as u64 + 1,
                    actor_prev: prev.as_ref().map(|op| op.id),
                    deps,
                    generation,
                    payload: TopicPayload::Event(
                        EventEnvelope::encode_event(&Note {
                            text: format!("{round}{}", "x".repeat(text)),
                        })
                        .unwrap(),
                    ),
                },
                writer,
            )
            .unwrap();
            log.receive_ops(vec![op.clone()]).unwrap();
            tips[index] = Some(op);
        }
    }
    Source {
        engine: SyncEngine::new(log.clone(), owner.peer_id()),
        log,
        topic_id,
        reader,
        genesis,
    }
}

/// The request a reader holding `reader` would send for the source's summary.
pub(super) fn request_for<S: Storage>(
    source: &Source<S>,
    reader: &Oplog,
    credit: SyncCredit,
) -> SyncRequest {
    let reader_engine = SyncEngine::new(reader.clone(), source.reader);
    let summary = source.engine.summary(source.topic_id).unwrap();
    let (plan, _) = reader_engine
        .negotiate_page(source.reader, &summary, PageBudget { ops: 0, bytes: 0 })
        .unwrap();
    SyncRequest {
        topic_id: source.topic_id,
        known: plan.common,
        wants: plan.need,
        actor_range_hints: plan.actor_range_hints,
        genesis: Some(source.genesis.id),
        credit,
    }
}

/// Page by page, every op a page carries is admitted at once: pages are
/// causally closed over the reader's clock, even with many actors and joins,
/// and the reader ends with exactly the source's frontier.
#[test]
fn pages_stay_causal() {
    let source = many_actors(24, 30, 0);
    let reader = Oplog::new();
    reader.receive_ops(vec![source.genesis.clone()]).unwrap();
    let credit = SyncCredit {
        ops: 100,
        bytes: u64::MAX,
    };
    let mut pages = 0;
    loop {
        let request = request_for(&source, &reader, credit);
        if request.actor_range_hints.is_empty() && request.wants.is_empty() {
            break;
        }
        let page = source
            .engine
            .response_page(source.reader, &request, PageBudget::from_credit(credit))
            .unwrap();
        assert!(
            !page.ops.is_empty(),
            "a request behind the source must get data"
        );
        assert!(page.ops.len() <= 100);
        let ids = page.ops.iter().map(|op| op.id).collect::<BTreeSet<_>>();
        assert_eq!(
            reader.receive_ops(page.ops).unwrap(),
            ids,
            "page {pages} was not causal"
        );
        assert!(
            reader
                .storage()
                .pending_missing_deps(&source.topic_id)
                .unwrap()
                .is_empty()
        );
        pages += 1;
    }
    assert_eq!(pages, (24 * 30_usize).div_ceil(100));
    assert_eq!(
        reader.storage().heads(&source.topic_id).unwrap(),
        source.log.storage().heads(&source.topic_id).unwrap()
    );
}

/// Large ops roll over the byte budget: each page stays within it, reports the
/// rest, and the pages together carry every op exactly once.
#[test]
fn pages_roll_over_bytes() {
    let source = many_actors(2, 6, 256 * 1024);
    let reader = Oplog::new();
    reader.receive_ops(vec![source.genesis.clone()]).unwrap();
    let credit = SyncCredit {
        ops: 4096,
        bytes: 900 * 1024,
    };
    let mut seen = BTreeSet::new();
    let mut more = true;
    while more {
        let request = request_for(&source, &reader, credit);
        let page = source
            .engine
            .response_page(source.reader, &request, PageBudget::from_credit(credit))
            .unwrap();
        let bytes = page
            .ops
            .iter()
            .map(|op| postcard::experimental::serialized_size(op).unwrap())
            .sum::<usize>();
        assert!(
            bytes <= 900 * 1024,
            "page of {bytes} bytes exceeds its credit"
        );
        for op in &page.ops {
            assert!(seen.insert(op.id), "an op was sent twice");
        }
        more = page.more;
        reader.receive_ops(page.ops).unwrap();
    }
    assert_eq!(seen.len(), 12);
}

/// A page never passes the goal the request names, so appends made after the
/// goal was captured are later work.
#[test]
fn pages_respect_goal() {
    let source = many_actors(3, 10, 0);
    let reader = Oplog::new();
    reader.receive_ops(vec![source.genesis.clone()]).unwrap();
    let mut request = request_for(&source, &reader, SyncCredit::default());
    for hint in &mut request.actor_range_hints {
        hint.to_inclusive = 4;
    }
    let page = source
        .engine
        .response_page(
            source.reader,
            &request,
            PageBudget::from_credit(request.credit),
        )
        .unwrap();
    assert!(!page.more, "the goal was served completely");
    assert!(page.ops.iter().all(|op| op.signed.body.actor_seq <= 4));
}

/// A hole behind a clock that claims the op is repaired through an explicit
/// want: the clock does not suppress it, and the topic ends whole.
#[test]
fn repair_behind_clock() {
    let source = many_actors(2, 8, 0);
    let storage = MemoryStorage::new();
    let reader = Oplog::with_storage(storage.clone());
    let ops = oplog::topological(source.log.storage(), &source.topic_id).unwrap();
    reader.receive_ops(ops.clone()).unwrap();
    let hole = ops[5].id;
    storage.drop_op_record(&hole);
    reader.recheck_topics().unwrap();
    assert!(
        reader
            .topic_unresolved(&source.topic_id)
            .unwrap()
            .contains(&hole)
    );

    let request = request_for(&source, &reader, SyncCredit::default());
    assert!(
        request.actor_range_hints.is_empty(),
        "the clocks already agree"
    );
    assert!(request.wants.contains(&hole));
    let page = source
        .engine
        .response_page(
            source.reader,
            &request,
            PageBudget::from_credit(request.credit),
        )
        .unwrap();
    assert!(page.ops.iter().any(|op| op.id == hole));
    reader.receive_ops(page.ops).unwrap();
    reader.recheck_topics().unwrap();
    assert!(
        reader
            .topic_unresolved(&source.topic_id)
            .unwrap()
            .is_empty()
    );
}

/// Catching up one new op after a long synchronized history reads a handful
/// of records, not the history.
#[test]
fn one_op_cheap() {
    let source = many_actors(1, 2048, 0);
    let reader = Oplog::new();
    let ops = oplog::topological(source.log.storage(), &source.topic_id).unwrap();
    reader.receive_ops(ops).unwrap();
    let writer = Ed25519Signer::from_bytes(&[
        0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
        7, 7,
    ]);
    let actor_id = actor_id_for(source.topic_id, writer.peer_id());
    let (tip_seq, tip) = source
        .log
        .storage()
        .actor_tip(&source.topic_id, &actor_id)
        .unwrap()
        .unwrap();
    let tip_meta = source.log.storage().get_meta(&tip).unwrap().unwrap();
    let next = Op::sign(
        OpBody {
            topic_id: source.topic_id,
            author: writer.peer_id(),
            actor_id,
            actor_seq: tip_seq + 1,
            actor_prev: Some(tip),
            deps: [tip].into(),
            generation: tip_meta.generation + 1,
            payload: TopicPayload::Event(
                EventEnvelope::encode_event(&Note {
                    text: "next".into(),
                })
                .unwrap(),
            ),
        },
        &writer,
    )
    .unwrap();
    source.log.receive_ops(vec![next.clone()]).unwrap();

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
    assert_eq!(page.ops, vec![next]);
    let reads = (after.meta_reads - before.meta_reads)
        + (after.index_reads - before.index_reads)
        + (after.op_reads - before.op_reads);
    assert!(reads <= 12, "one new op cost {reads} reads");
}

/// A want for a head far beyond the reader's clock cannot be served by one
/// repair walk. Forward ranges still advance on every page, what is sent is
/// admissible, and the pull completes; the same fixture without the far want
/// is the positive control and takes the same number of pages.
#[test]
fn repair_walk_causal() {
    let source = many_actors(1, 6000, 0);
    for far_want in [false, true] {
        let reader = Oplog::new();
        reader.receive_ops(vec![source.genesis.clone()]).unwrap();
        let mut pages = 0;
        loop {
            let mut request = request_for(&source, &reader, SyncCredit::default());
            if request.actor_range_hints.is_empty() && request.wants.is_empty() {
                break;
            }
            assert!(pages < 2, "pull with far want {far_want} did not finish");
            if far_want {
                request
                    .wants
                    .extend(source.log.storage().heads(&source.topic_id).unwrap());
            }
            let page = source
                .engine
                .response_page(
                    source.reader,
                    &request,
                    PageBudget::from_credit(request.credit),
                )
                .unwrap();
            assert!(!page.ops.is_empty(), "page {pages} carried nothing");
            assert_eq!(page.more, pages == 0);
            let ids = page.ops.iter().map(|op| op.id).collect::<BTreeSet<_>>();
            assert_eq!(reader.receive_ops(page.ops).unwrap(), ids);
            assert!(
                reader
                    .storage()
                    .pending_missing_deps(&source.topic_id)
                    .unwrap()
                    .is_empty()
            );
            pages += 1;
        }
        assert_eq!(pages, 2);
        assert_eq!(
            reader.storage().heads(&source.topic_id).unwrap(),
            source.log.storage().heads(&source.topic_id).unwrap()
        );
    }
}

/// A topic whose `actors` writers each publish one op depending on one op of
/// the writer with the largest actor key, so bounded actor windows taken in key
/// order select the dependents before their dependency.
fn late_dependency<S: Storage + Clone>(storage: S, actors: usize) -> Source<S> {
    let owner = Ed25519Signer::from_bytes(&[242; 32]);
    let reader = Ed25519Signer::from_bytes(&[243; 32]).peer_id();
    let topic_id = TopicId::hash([b"late-dependency".as_slice(), &actors.to_le_bytes()].concat());
    let mut writers = (0..actors)
        .map(|index| {
            let mut seed = [9_u8; 32];
            seed[..8].copy_from_slice(&(index as u64).to_le_bytes());
            Ed25519Signer::from_bytes(&seed)
        })
        .collect::<Vec<_>>();
    writers.sort_by_key(|writer| actor_id_for(topic_id, writer.peer_id()));
    let late = writers.pop().unwrap();
    let members = writers
        .iter()
        .chain([&late, &owner])
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
    let event = |writer: &Ed25519Signer, deps: BTreeSet<OpId>, generation| {
        Op::sign(
            OpBody {
                topic_id,
                author: writer.peer_id(),
                actor_id: actor_id_for(topic_id, writer.peer_id()),
                actor_seq: 1,
                actor_prev: None,
                deps,
                generation,
                payload: TopicPayload::Event(
                    EventEnvelope::encode_event(&Note { text: "x".into() }).unwrap(),
                ),
            },
            writer,
        )
        .unwrap()
    };
    let dependency = event(&late, [genesis.id].into(), 1);
    let mut ops = vec![genesis.clone(), dependency.clone()];
    ops.extend(
        writers
            .iter()
            .map(|writer| event(writer, [dependency.id].into(), 2)),
    );
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

/// Pages a reader at the genesis through `source`, requiring every page to
/// carry admissible data, and returns the page count.
pub(super) fn page_through<S: Storage>(source: &Source<S>, credit: SyncCredit) -> usize {
    let reader = Oplog::new();
    reader.receive_ops(vec![source.genesis.clone()]).unwrap();
    // Generous: every op is at least 64 serialized bytes.
    let per_page = (credit.ops as usize).min(credit.bytes as usize / 64).max(1);
    let cap = 4 + 3 * 8192 / per_page;
    let mut pages = 0;
    loop {
        let request = request_for(source, &reader, credit);
        if request.actor_range_hints.is_empty() && request.wants.is_empty() {
            break;
        }
        assert!(pages < cap, "paging did not finish");
        let page = source
            .engine
            .response_page(source.reader, &request, PageBudget::from_credit(credit))
            .unwrap();
        assert!(!page.ops.is_empty(), "page {pages} carried nothing");
        let ids = page.ops.iter().map(|op| op.id).collect::<BTreeSet<_>>();
        assert_eq!(
            reader.receive_ops(page.ops).unwrap(),
            ids,
            "page {pages} was not causal"
        );
        pages += 1;
    }
    assert_eq!(
        reader.storage().heads(&source.topic_id).unwrap(),
        source.log.storage().heads(&source.topic_id).unwrap()
    );
    assert_eq!(
        reader.storage().actor_clock(&source.topic_id).unwrap(),
        source.log.storage().actor_clock(&source.topic_id).unwrap()
    );
    pages
}

/// Around the bounded actor window, a dependency actor beyond the window is
/// brought into the page instead of blocking every dependent.
#[test]
fn window_admits_dependency() {
    for actors in [4095, 4096, 4097, 4098] {
        let source = late_dependency(MemoryStorage::new(), actors);
        let pages = page_through(&source, SyncCredit::default());
        assert!(pages <= 2, "{actors} actors took {pages} pages");
    }
}

/// The same deferred dependency window on a durable store.
#[cfg(feature = "fjall")]
#[test]
fn fjall_window_admits_dependency() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate::storage::FjallStorage::open(dir.path()).unwrap();
    let source = late_dependency(storage, 4097);
    assert!(page_through(&source, SyncCredit::default()) <= 2);
}

/// The public page contract without a transport: a one-op credit serves one
/// op, a request planned on another genesis is refused rather than served from
/// this branch, and the requester's own request brings it to the frontier.
#[test]
fn public_page_contract() {
    let source = many_actors(3, 4, 0);
    let reader = Oplog::new();
    reader.receive_ops(vec![source.genesis.clone()]).unwrap();
    let reader_engine = SyncEngine::new(reader.clone(), source.reader);
    let summary = source.engine.summary(source.topic_id).unwrap();

    let mut request = reader_engine.plan_request(source.reader, &summary).unwrap();
    assert!(request.known.is_empty());
    assert_eq!(request.genesis, Some(source.genesis.id));
    request.credit.ops = 1;
    let data = source
        .engine
        .plan_response_data(source.reader, &request)
        .unwrap();
    assert_eq!(data.ops.len(), 1);

    let mut other = request.clone();
    other.genesis = Some(OpId::hash(b"another branch"));
    assert!(matches!(
        source.engine.plan_response_data(source.reader, &other),
        Err(Error::StaleIncarnation)
    ));

    let mut pages = 0;
    loop {
        let request = reader_engine
            .plan_request(
                source.reader,
                &source.engine.summary(source.topic_id).unwrap(),
            )
            .unwrap();
        if request.wants.is_empty() && request.actor_range_hints.is_empty() {
            break;
        }
        assert!(pages < 2, "public paging did not finish");
        let page = source
            .engine
            .response_page(
                source.reader,
                &request,
                PageBudget::from_credit(request.credit),
            )
            .unwrap();
        assert!(!page.more);
        reader.receive_ops(page.ops).unwrap();
        pages += 1;
    }
    assert_eq!(
        reader.storage().actor_clock(&source.topic_id).unwrap(),
        source.log.storage().actor_clock(&source.topic_id).unwrap()
    );
}

/// Signed chains of `lens.len()` writers on one genesis, each op depending
/// only on its predecessor, loaded into `source`. Returns the genesis and the
/// chains in order.
pub(super) fn independent_chains(
    source: &Oplog,
    reader: PeerId,
    lens: &[usize],
) -> (Op, Vec<Vec<Op>>) {
    let owner = Ed25519Signer::from_bytes(&[244; 32]);
    let writers = (0..lens.len())
        .map(|index| Ed25519Signer::from_bytes(&[245 + index as u8; 32]))
        .collect::<Vec<_>>();
    let topic_id = TopicId::hash(b"independent-chains");
    let members = writers
        .iter()
        .map(Signer::peer_id)
        .chain([owner.peer_id(), reader])
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
    source.receive_ops(vec![genesis.clone()]).unwrap();
    let chains = writers
        .iter()
        .zip(lens)
        .map(|(writer, len)| {
            let mut chain: Vec<Op> = Vec::with_capacity(*len);
            for index in 0..*len {
                let prev = chain.last();
                let op = Op::sign(
                    OpBody {
                        topic_id,
                        author: writer.peer_id(),
                        actor_id: actor_id_for(topic_id, writer.peer_id()),
                        actor_seq: index as u64 + 1,
                        actor_prev: prev.map(|op| op.id),
                        deps: [prev.map_or(genesis.id, |op| op.id)].into(),
                        generation: index as u64 + 1,
                        payload: TopicPayload::Event(
                            EventEnvelope::encode_event(&Note {
                                text: index.to_string(),
                            })
                            .unwrap(),
                        ),
                    },
                    writer,
                )
                .unwrap();
                chain.push(op);
            }
            for batch in chain.chunks(4096) {
                source.receive_ops(batch.to_vec()).unwrap();
            }
            chain
        })
        .collect();
    (genesis, chains)
}

/// A behind-only pull of a long chain, a deep repair of a record the reader
/// lost, and a hole no peer can serve, in one topic: every page before the
/// goal carries data, the long chain and the repair complete, and only the
/// unavailable record is left, reported unresolved, with nothing more to serve.
#[test]
fn mixed_repair_pull() {
    let reader_id = Ed25519Signer::from_bytes(&[250; 32]).peer_id();
    let source_store = MemoryStorage::new();
    let source_log = Oplog::with_storage(source_store.clone());
    let (genesis, chains) = independent_chains(&source_log, reader_id, &[3000, 40, 9000]);
    let (repaired, blocked, long) = (&chains[0], &chains[1], &chains[2]);
    let owner = Ed25519Signer::from_bytes(&[244; 32]).peer_id();
    let source = Source {
        engine: SyncEngine::new(source_log.clone(), owner),
        log: source_log,
        topic_id: genesis.signed.body.topic_id,
        reader: reader_id,
        genesis: genesis.clone(),
    };
    let reader_store = MemoryStorage::new();
    let reader = Oplog::with_storage(reader_store.clone());
    reader.receive_ops(vec![genesis]).unwrap();
    reader.receive_ops(repaired.clone()).unwrap();
    reader.receive_ops(blocked.clone()).unwrap();
    let lost = repaired[5].id;
    let unavailable = blocked[10].id;
    damage_op(&reader_store, &lost, Damage::Op);
    damage_op(&reader_store, &unavailable, Damage::Op);
    damage_op(&source_store, &unavailable, Damage::Both);
    reader.recheck_topics().unwrap();
    source.log.recheck_topics().unwrap();

    let mut pages = 0;
    loop {
        let request = request_for(&source, &reader, SyncCredit::default());
        if request.actor_range_hints.is_empty() && request.wants == [unavailable].into() {
            break;
        }
        assert!(pages <= 4, "the mixed pull did not reach its goal");
        let page = source
            .engine
            .response_page(
                source.reader,
                &request,
                PageBudget::from_credit(request.credit),
            )
            .unwrap();
        assert!(!page.ops.is_empty(), "page {pages} carried nothing");
        reader.receive_ops(page.ops).unwrap();
        reader.recheck_topics().unwrap();
        pages += 1;
    }
    let long_actor = long[0].signed.body.actor_id;
    assert_eq!(
        reader
            .storage()
            .actor_clock(&source.topic_id)
            .unwrap()
            .get(&long_actor),
        long.len() as u64
    );
    assert!(reader.storage().get_op(&lost).unwrap().is_some());
    assert_eq!(
        reader.topic_unresolved(&source.topic_id).unwrap(),
        [unavailable].into()
    );
    let last = request_for(&source, &reader, SyncCredit::default());
    let page = source
        .engine
        .response_page(source.reader, &last, PageBudget::from_credit(last.credit))
        .unwrap();
    assert!(page.ops.is_empty() && !page.more);
}

/// The deferred dependency window under a tight credit: pages cut by op
/// count and by bytes still reach the frontier, each carrying admissible data.
#[test]
fn window_tight_credit() {
    let source = late_dependency(MemoryStorage::new(), 4097);
    let credit = SyncCredit {
        ops: 1024,
        bytes: 64 * 1024,
    };
    let pages = page_through(&source, credit);
    assert!(pages > 4097 / 1024, "{pages} pages for 4097 ops");
}
