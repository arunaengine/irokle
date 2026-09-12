//! Pages serve causal prefixes of a goal within their budget, over many
//! actors, joins, large ops and repair wants.

use super::support::*;

use crate::oplog::Oplog;
use crate::sync::{PageBudget, SyncCredit, SyncEngine, SyncRequest};

struct Source {
    log: Oplog,
    engine: SyncEngine<MemoryStorage>,
    topic_id: TopicId,
    reader: PeerId,
    genesis: Op,
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
fn request_for(source: &Source, reader: &Oplog, credit: SyncCredit) -> SyncRequest {
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
/// repair walk; what is sent must still be admissible, never a tail whose
/// ancestors were cut off.
#[test]
fn repair_walk_causal() {
    let source = many_actors(1, 6000, 0);
    let reader = Oplog::new();
    reader.receive_ops(vec![source.genesis.clone()]).unwrap();
    let mut request = request_for(&source, &reader, SyncCredit::default());
    request
        .wants
        .extend(source.log.storage().heads(&source.topic_id).unwrap());
    let page = source
        .engine
        .response_page(
            source.reader,
            &request,
            PageBudget::from_credit(request.credit),
        )
        .unwrap();
    assert!(page.more);
    let ids = page.ops.iter().map(|op| op.id).collect::<BTreeSet<_>>();
    assert_eq!(reader.receive_ops(page.ops).unwrap(), ids);
    assert!(
        reader
            .storage()
            .pending_missing_deps(&source.topic_id)
            .unwrap()
            .is_empty()
    );
}
