//! Requests that name only part of the actors a reader is behind on: every
//! page stays causal over what the reader holds, and paging still completes.

use super::pages::{Source, late_dependency};
use super::support::*;

use crate::oplog::Oplog;
use crate::sync::{MAX_PAGE_MISSING, PageBudget, RequestKnowledge, SyncEngine};

/// What paging a reader to the source's frontier took.
struct Paged {
    rounds: usize,
    sent: usize,
    empty: usize,
    /// Whether the reader reached the source's frontier, rather than a round
    /// that neither carried data nor advanced its request.
    complete: bool,
}

/// Page `reader` to the frontier of `source` with requests of at most `items`
/// wants and hints on both sides. Every page must be causal over what the
/// reader holds, never repeat an op, and name a blocker when it carries nothing.
fn page_truncated<S: Storage>(source: &Source<S>, reader: &Oplog, items: usize) -> Paged {
    let paged = page_bounded(source, reader, items, MAX_PAGE_MISSING);
    assert!(
        paged.complete,
        "paging stalled after {} rounds",
        paged.rounds
    );
    paged
}

/// [`page_truncated`] with at most `positions` needed positions per page and
/// in the reader's knowledge, stopping at a round the requester would report
/// as no progress.
fn page_bounded<S: Storage>(
    source: &Source<S>,
    reader: &Oplog,
    items: usize,
    positions: usize,
) -> Paged {
    page_informed(source, reader, items, positions, false)
}

fn page_informed<S: Storage, R: Storage>(
    source: &Source<S>,
    reader: &Oplog<R>,
    items: usize,
    positions: usize,
    informed: bool,
) -> Paged {
    let responder = source
        .engine
        .clone()
        .with_request_items(items)
        .with_page_positions(positions);
    let reader_engine = SyncEngine::new(reader.clone(), source.reader).with_request_items(items);
    let mut knowledge = RequestKnowledge::with_capacity(positions);
    let mut sent = BTreeSet::new();
    let (mut rounds, mut empty) = (0, 0);
    let complete = loop {
        let summary = responder.summary(source.topic_id).unwrap();
        let request = reader_engine
            .plan_request_with(source.reader, &summary, &knowledge)
            .unwrap();
        if request.actor_range_hints.is_empty() && request.wants.is_empty() {
            break true;
        }
        assert!(request.actor_range_hints.len() + request.wants.len() <= items);
        assert!(rounds < 4096, "paging did not end");
        let budget = PageBudget::from_credit(request.credit);
        let page = if informed {
            let held = reader_engine.summary(source.topic_id).unwrap();
            responder.response_with(source.reader, &request, budget, &held)
        } else {
            responder.response_page(source.reader, &request, budget)
        }
        .unwrap();
        assert!(page.positions.len() <= positions);
        let mut earlier = BTreeSet::new();
        for op in &page.ops {
            for dep in &op.signed.body.deps {
                assert!(
                    earlier.contains(dep) || reader.storage().dep_resolvable(dep).unwrap(),
                    "round {rounds}: {} needs {dep}, which the reader lacks",
                    op.id
                );
            }
            assert!(sent.insert(op.id), "round {rounds} repeated {}", op.id);
            earlier.insert(op.id);
        }
        if page.ops.is_empty() {
            assert!(
                page.continued || !page.positions.is_empty() || !page.missing.is_empty(),
                "round {rounds} carried nothing and named no blocker"
            );
            empty += 1;
        }
        assert_eq!(reader.receive_ops(page.ops).unwrap(), earlier);
        assert!(reader.storage().ready_pending_ops().unwrap().is_empty());
        let actors = summary.actor_clock.iter().count();
        let revision = knowledge.revision();
        knowledge.settle(
            &request.window,
            (&page.positions, page.continued),
            (!earlier.is_empty(), actors),
        );
        rounds += 1;
        if earlier.is_empty() && knowledge.revision() == revision {
            break false;
        }
    };
    if complete {
        let (source_storage, reader_storage) = (source.log.storage(), reader.storage());
        assert_eq!(
            reader_storage.heads(&source.topic_id).unwrap(),
            source_storage.heads(&source.topic_id).unwrap()
        );
        assert_eq!(
            reader_storage.actor_clock(&source.topic_id).unwrap(),
            source_storage.actor_clock(&source.topic_id).unwrap()
        );
    }
    Paged {
        rounds,
        sent: sent.len(),
        empty,
        complete,
    }
}

/// A signed topic of `writers` writers sorted by actor key, with `ops` after genesis; each
/// names its writer and dependency indices, with genesis at index 0. Sequences, previous ops,
/// and generations follow that order.
fn graph_source<S: Storage>(storage: S, writers: usize, ops: &[(usize, &[usize])]) -> Source<S> {
    graph_members(storage, writers, ops, true)
}

fn graph_members<S: Storage>(
    storage: S,
    writers: usize,
    ops: &[(usize, &[usize])],
    member: bool,
) -> Source<S> {
    let owner = Ed25519Signer::from_bytes(&[232; 32]);
    let reader = Ed25519Signer::from_bytes(&[233; 32]).peer_id();
    let shape = postcard::to_allocvec(&(writers, ops)).unwrap();
    let topic_id = TopicId::hash([b"graph".as_slice(), &shape].concat());
    let mut signers = (0..writers)
        .map(|index| {
            let mut seed = [13_u8; 32];
            seed[..8].copy_from_slice(&(index as u64).to_le_bytes());
            Ed25519Signer::from_bytes(&seed)
        })
        .collect::<Vec<_>>();
    signers.sort_by_key(|writer| actor_id_for(topic_id, writer.peer_id()));
    let members = signers
        .iter()
        .chain([&owner])
        .map(Signer::peer_id)
        .chain(member.then_some(reader))
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
    let mut signed = vec![genesis.clone()];
    let mut last = vec![None::<usize>; writers];
    for (writer, deps) in ops {
        let signer = &signers[*writer];
        let generation = deps
            .iter()
            .copied()
            .chain(last[*writer])
            .map(|index| signed[index].signed.body.generation + 1)
            .max()
            .unwrap_or(1);
        let mut deps = deps
            .iter()
            .map(|index| signed[*index].id)
            .collect::<BTreeSet<_>>();
        let prev = last[*writer].map(|index| signed[index].id);
        deps.extend(prev);
        let op = Op::sign(
            OpBody {
                topic_id,
                author: signer.peer_id(),
                actor_id: actor_id_for(topic_id, signer.peer_id()),
                actor_seq: last[*writer].map_or(1, |index| signed[index].signed.body.actor_seq + 1),
                actor_prev: prev,
                deps,
                generation,
                payload: TopicPayload::Event(
                    EventEnvelope::encode_event(&Note { text: "x".into() }).unwrap(),
                ),
            },
            signer,
        )
        .unwrap();
        last[*writer] = Some(signed.len());
        signed.push(op);
    }
    let log = Oplog::with_storage(storage);
    log.receive_ops(signed).unwrap();
    if !member {
        log.create_control_op(
            topic_id,
            actor_id_for(topic_id, owner.peer_id()),
            TopicControl::AddPeer { peer: reader },
            &owner,
        )
        .unwrap();
    }
    Source {
        engine: SyncEngine::new(log.clone(), owner.peer_id()),
        log,
        topic_id,
        reader,
        genesis,
    }
}

/// Twelve writers in reverse key order, each op depending on the ops of the
/// next two writers: a branching graph whose every op waits on two actors.
fn braided(storage: impl Storage) -> Source<impl Storage> {
    const WRITERS: usize = 12;
    let deps = (0..WRITERS)
        .map(|op| match op {
            0 => vec![0],
            1 => vec![1],
            _ => vec![op, op - 1],
        })
        .collect::<Vec<_>>();
    // Op k + 1 is written by writer WRITERS - 1 - k, so dependents sort first.
    let ops = deps
        .iter()
        .enumerate()
        .map(|(op, deps)| (WRITERS - 1 - op, deps.as_slice()))
        .collect::<Vec<_>>();
    graph_source(storage, WRITERS, &ops)
}

/// Four actors behind, the dependency of the other three beyond the item
/// window: pages stay causal from two items, one position beside one actor
/// behind, up to one past every actor.
fn assert_truncated_causal<S: Storage>(open: impl Fn() -> S) {
    for items in [2, 3, 4, 5] {
        let source = late_dependency(open(), 4);
        let reader = Oplog::new();
        reader.receive_ops(vec![source.genesis.clone()]).unwrap();
        let paged = page_truncated(&source, &reader, items);
        assert_eq!(paged.sent, 4, "{items} items");
        assert!(
            paged.rounds <= 4,
            "{items} items took {} rounds",
            paged.rounds
        );
        if items >= 4 {
            assert_eq!((paged.rounds, paged.empty), (1, 0), "{items} items");
        }
    }
}

#[test]
fn memory_truncated_causal() {
    assert_truncated_causal(MemoryStorage::new);
}

/// A chain or branching graph through more unknown actors than one request, page result, or
/// reader knowledge can name still completes through successive requests: every page stays
/// causal and the reader reaches the frontier in rounds that grow with actors.
fn assert_beyond_windows<S: Storage>(open: impl Fn() -> S) {
    for (items, positions) in [(2, 1), (2, 2), (3, 1), (3, 2), (3, MAX_PAGE_MISSING)] {
        let chain = super::progress::reverse_chain(open(), 12);
        let reader = Oplog::new();
        reader.receive_ops(vec![chain.genesis.clone()]).unwrap();
        let paged = page_bounded(&chain, &reader, items, positions);
        let label = format!("chain, {items} items, {positions} positions");
        assert!(
            paged.complete,
            "{label} stalled after {} rounds",
            paged.rounds
        );
        assert_eq!(paged.sent, 12, "{label}");
        assert!(
            paged.rounds <= 4 * 12,
            "{label} took {} rounds",
            paged.rounds
        );

        let graph = braided(open());
        let reader = Oplog::new();
        reader.receive_ops(vec![graph.genesis.clone()]).unwrap();
        let paged = page_bounded(&graph, &reader, items, positions);
        let label = format!("graph, {items} items, {positions} positions");
        assert!(
            paged.complete,
            "{label} stalled after {} rounds",
            paged.rounds
        );
        assert_eq!(paged.sent, 12, "{label}");
        assert!(
            paged.rounds <= 4 * 12,
            "{label} took {} rounds",
            paged.rounds
        );
    }
}

#[test]
fn memory_beyond_windows() {
    assert_beyond_windows(MemoryStorage::new);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_beyond_windows() {
    let dirs = std::sync::Mutex::new(Vec::new());
    assert_beyond_windows(|| {
        let dir = tempfile::tempdir().unwrap();
        let storage = crate::storage::FjallStorage::open(dir.path()).unwrap();
        dirs.lock().unwrap().push(dir);
        storage
    });
}

/// A page that cannot name every unknown ancestor of a chain names the deepest ones, whose
/// dependents can follow once they are served, not the nearest ones it met first.
#[test]
fn names_deepest_positions() {
    let source = super::progress::reverse_chain(MemoryStorage::new(), 40);
    let responder = source.engine.clone().with_request_items(3);
    let reader = Oplog::new();
    reader.receive_ops(vec![source.genesis.clone()]).unwrap();
    let summary = responder.summary(source.topic_id).unwrap();
    let request = SyncEngine::new(reader, source.reader)
        .with_request_items(3)
        .plan_request(source.reader, &summary)
        .unwrap();
    let budget = PageBudget::from_credit(request.credit);
    let page = responder
        .response_page(source.reader, &request, budget)
        .unwrap();
    let deepest = oplog::topological(source.log.storage(), &source.topic_id)
        .unwrap()
        .into_iter()
        .filter(|op| matches!(op.signed.body.generation, 1 | 2))
        .map(|op| op.signed.body.actor_id)
        .collect::<BTreeSet<_>>();
    assert!(page.ops.is_empty());
    assert_eq!(page.positions, deepest);
}

/// A chain beyond request and knowledge windows is planned in small read slices with one
/// kept plan and pulled by two readers. A capacity-refused reader retries, another reconnects
/// midway, and both reach the frontier without sending an op before its dependency.
#[test]
fn chain_windows_saturated() {
    let source = super::progress::reverse_chain(MemoryStorage::new(), 40);
    let responder = source
        .engine
        .clone()
        .with_request_items(3)
        .with_page_positions(2)
        .with_page_actors(2)
        .with_page_visits(4, 1);
    let state = source
        .log
        .storage()
        .topic_state(&source.topic_id)
        .unwrap()
        .unwrap();
    let second = *state
        .members
        .iter()
        .find(|peer| **peer != source.reader)
        .unwrap();
    let readers = [source.reader, second].map(|peer| {
        let log = Oplog::new();
        log.receive_ops(vec![source.genesis.clone()]).unwrap();
        let engine = SyncEngine::new(log.clone(), peer).with_request_items(3);
        (peer, log, engine, RequestKnowledge::with_capacity(2), false)
    });
    let mut readers = readers;
    let (mut rounds, mut refused, mut reconnected) = (0, 0, false);
    let summary = responder.summary(source.topic_id).unwrap();
    while readers.iter().any(|reader| !reader.4) {
        rounds += 1;
        assert!(
            rounds < 4096,
            "pulls did not finish: work={:?}, readers={:?}",
            responder.page_work(),
            readers
                .iter()
                .map(|(_, log, _, knowledge, done)| (
                    log.storage().actor_clock(&source.topic_id).unwrap().len(),
                    knowledge,
                    done
                ))
                .collect::<Vec<_>>()
        );
        for (peer, log, engine, knowledge, done) in &mut readers {
            if *done {
                continue;
            }
            let request = engine
                .plan_request_with(*peer, &summary, knowledge)
                .unwrap();
            if request.actor_range_hints.is_empty() && request.wants.is_empty() {
                *done = true;
                continue;
            }
            let budget = PageBudget::from_credit(request.credit);
            let page = match responder.response_page(*peer, &request, budget) {
                Ok(page) => page,
                Err(Error::SyncCapacity(_)) => {
                    refused += 1;
                    continue;
                }
                Err(error) => panic!("{error:?}"),
            };
            for op in &page.ops {
                for dep in &op.signed.body.deps {
                    let earlier = page.ops.iter().take_while(|sent| sent.id != op.id);
                    assert!(
                        log.storage().dep_resolvable(dep).unwrap()
                            || earlier.clone().any(|sent| sent.id == *dep)
                    );
                }
            }
            let received = !page.ops.is_empty();
            log.receive_ops(page.ops).unwrap();
            knowledge.settle(
                &request.window,
                (&page.positions, page.continued),
                (received, summary.actor_clock.iter().count()),
            );
            if *peer == second
                && !reconnected
                && log.storage().list_op_ids(&source.topic_id).unwrap().len() > 10
            {
                *knowledge = RequestKnowledge::with_capacity(2);
                reconnected = true;
            }
        }
    }
    assert!(refused > 0, "the one place for kept plans was contended");
    assert!(reconnected);
    for (_, log, _, _, _) in &readers {
        assert_eq!(
            log.storage().heads(&source.topic_id).unwrap(),
            source.log.storage().heads(&source.topic_id).unwrap()
        );
        assert!(log.storage().ready_pending_ops().unwrap().is_empty());
    }
}

#[test]
fn single_visit_chain() {
    let mut previous = None;
    for actors in [40, 80] {
        let mut source = super::progress::reverse_chain(MemoryStorage::new(), actors);
        source.engine = source.engine.with_page_visits(1, 16);
        let reader = Oplog::new();
        reader.receive_ops(vec![source.genesis.clone()]).unwrap();
        let paged = page_informed(&source, &reader, 3, 2, true);
        assert!(
            paged.complete,
            "{} rounds sent {} operations",
            paged.rounds, paged.sent
        );
        assert_eq!(paged.sent, actors);
        let work = source.engine.page_work();
        let total = work.visits + work.edges + work.actors;
        println!(
            "single_visit actors={actors} rounds={} planner_work={total}",
            paged.rounds
        );
        if let Some(previous) = previous {
            assert!(total <= 3 * previous);
        }
        previous = Some(total);
    }
}

#[test]
fn confirmed_frontier_only() {
    let source = super::progress::reverse_chain(MemoryStorage::new(), 40);
    let responder = source
        .engine
        .clone()
        .with_request_items(3)
        .with_page_visits(1, 16);
    let reader = Oplog::new();
    reader.receive_ops(vec![source.genesis.clone()]).unwrap();
    let receiver = SyncEngine::new(reader.clone(), source.reader).with_request_items(3);
    let request = receiver
        .plan_request(source.reader, &responder.summary(source.topic_id).unwrap())
        .unwrap();
    let budget = PageBudget::from_credit(request.credit);
    let mut first = None;
    for _ in 0..256 {
        let held = receiver.summary(source.topic_id).unwrap();
        let page = responder
            .response_with(source.reader, &request, budget, &held)
            .unwrap();
        if page.ops.is_empty() {
            assert!(page.continued);
            continue;
        }
        if let Some(expected) = first {
            assert_eq!(
                page.ops[0].id, expected,
                "an unconsumed page must be replayed"
            );
            reader.receive_ops(page.ops).unwrap();
            break;
        }
        first = Some(page.ops[0].id);
    }
    assert_eq!(
        reader
            .storage()
            .list_op_ids(&source.topic_id)
            .unwrap()
            .len(),
        2
    );
    let held = receiver.summary(source.topic_id).unwrap();
    let before = responder.page_work().resumed;
    responder
        .response_with(source.reader, &request, budget, &held)
        .unwrap();
    assert!(responder.page_work().resumed > before);
    let mut staged = held.clone();
    staged.genesis = None;
    staged.actor_clock = ActorClock::new();
    staged.staged = Some(crate::sync::SyncReceipt {
        topic_id: source.topic_id,
        genesis: source.genesis.id,
        session: 7,
        clock: held.actor_clock,
    });
    for session in [7, 8] {
        staged.staged.as_mut().unwrap().session = session;
        let before = responder.page_work().resumed;
        let page = responder
            .response_with(source.reader, &request, budget, &staged)
            .unwrap();
        assert!(page.continued && page.ops.is_empty());
        assert_eq!(
            responder.page_work().resumed,
            before,
            "a changed staging session cannot resume the old frontier"
        );
    }
}

/// Held older prefixes satisfy a join while its actors remain behind.
#[test]
fn fan_in_capacity() {
    // Writers 1 to 3 write roots; writer 0 joins them; each root writer then
    // follows the join.
    let ops: [(usize, &[usize]); 7] = [
        (1, &[0]),
        (2, &[0]),
        (3, &[0]),
        (0, &[1, 2, 3]),
        (1, &[4]),
        (2, &[4]),
        (3, &[4]),
    ];
    let open = || {
        let source = graph_source(MemoryStorage::new(), 4, &ops);
        let reader = Oplog::new();
        reader.receive_ops(vec![source.genesis.clone()]).unwrap();
        // The reader holds the roots, so only the positions stay unknown.
        let roots = oplog::topological(source.log.storage(), &source.topic_id).unwrap();
        reader.receive_ops(roots[1..4].to_vec()).unwrap();
        (source, reader)
    };
    let (source, reader) = open();
    let paged = page_bounded(&source, &reader, 5, 4);
    assert!(paged.complete, "stalled after {} rounds", paged.rounds);
    assert_eq!(paged.sent, 4);
    let (source, reader) = open();
    let paged = page_informed(&source, &reader, 2, 1, true);
    assert!(paged.complete, "stalled after {} rounds", paged.rounds);
    assert_eq!(paged.sent, 4);
    assert!(paged.rounds <= 4, "{} rounds", paged.rounds);
}

fn joined_source<S: Storage>(storage: S, width: usize, member: bool) -> Source<S> {
    let mut deps = vec![vec![0]; width];
    deps.push((1..=width).collect());
    deps.extend(vec![vec![width + 1]; width]);
    let writers = (1..=width).chain([0]).chain(1..=width);
    let ops = writers
        .zip(deps.iter().map(Vec::as_slice))
        .collect::<Vec<_>>();
    graph_members(storage, width + 1, &ops, member)
}

#[test]
fn named_prefix_changes() {
    let source = joined_source(MemoryStorage::new(), 3, true);
    let ops = oplog::topological(source.log.storage(), &source.topic_id).unwrap();
    let root = ops
        .iter()
        .filter(|op| op.signed.body.generation == 1)
        .max_by_key(|op| op.id)
        .unwrap();
    let join = ops
        .iter()
        .find(|op| op.signed.body.generation == 2)
        .unwrap();
    let reader = Oplog::new();
    reader
        .receive_ops(vec![source.genesis.clone(), root.clone()])
        .unwrap();
    let receiver = SyncEngine::new(reader.clone(), source.reader);
    let mut request = crate::sync::SyncRequest {
        topic_id: source.topic_id,
        known: Default::default(),
        wants: Default::default(),
        actor_range_hints: vec![crate::sync::ActorRangeHint {
            actor_id: join.signed.body.actor_id,
            from_exclusive: 0,
            to_inclusive: 1,
        }],
        genesis: Some(source.genesis.id),
        credit: crate::sync::SyncCredit {
            ops: 1,
            bytes: 32 * 1024 * 1024,
        },
        window: Default::default(),
    };
    for round in 0..16 {
        if reader
            .storage()
            .list_op_ids(&source.topic_id)
            .unwrap()
            .len()
            == 6
        {
            return;
        }
        let held = receiver.summary(source.topic_id).unwrap();
        let page = source
            .engine
            .response_with(
                source.reader,
                &request,
                PageBudget::from_credit(request.credit),
                &held,
            )
            .unwrap();
        for op in &page.ops {
            assert!(
                reader.storage().get_op(&op.id).unwrap().is_none(),
                "repeated an already-held prefix"
            );
            assert!(
                op.signed
                    .body
                    .deps
                    .iter()
                    .all(|dep| reader.storage().dep_resolvable(dep).unwrap())
            );
        }
        reader.receive_ops(page.ops).unwrap();
        if round == 0 {
            request.actor_range_hints.push(crate::sync::ActorRangeHint {
                actor_id: root.signed.body.actor_id,
                from_exclusive: 1,
                to_inclusive: 2,
            });
        }
        let clock = reader.storage().actor_clock(&source.topic_id).unwrap();
        for hint in &mut request.actor_range_hints {
            hint.from_exclusive = clock.get(&hint.actor_id);
        }
    }
    panic!("changed hint scope did not finish");
}

fn assert_batch_views<S: Storage>(storage: S, chunk: usize) {
    let source = joined_source(MemoryStorage::new(), 33, false);
    let ops = oplog::topological(source.log.storage(), &source.topic_id).unwrap();
    let reader = Oplog::with_storage(storage);
    let known = std::cell::RefCell::new(BTreeSet::new());
    let effects =
        |_, entries: &[(Op, crate::storage::OpMeta)], state: &crate::storage::TopicState| {
            let mut known = known.borrow_mut();
            known.extend(entries.iter().map(|(op, _)| op.id));
            let referenced = ops
                .iter()
                .filter(|op| known.contains(&op.id))
                .flat_map(|op| op.signed.body.deps.iter().copied())
                .collect::<BTreeSet<_>>();
            assert_eq!(
                state.heads,
                known.difference(&referenced).copied().collect()
            );
            Ok(crate::storage::AdmissionEffects::default())
        };
    for batch in ops.chunks(chunk) {
        reader
            .receive_preverified(
                Some(source.genesis.signed.body.author),
                batch.to_vec(),
                &BTreeSet::new(),
                Some(&effects),
            )
            .unwrap();
    }
    assert_eq!(
        reader.storage().topic_view(&source.topic_id, None).unwrap(),
        source
            .log
            .storage()
            .topic_view(&source.topic_id, None)
            .unwrap()
    );
    for op in ops {
        assert_eq!(reader.storage().get_op(&op.id).unwrap(), Some(op.clone()));
        assert_eq!(
            reader.storage().get_meta(&op.id).unwrap(),
            source.log.storage().get_meta(&op.id).unwrap()
        );
    }
}

#[test]
fn memory_batch_views() {
    for chunk in [1, 7, 128] {
        assert_batch_views(MemoryStorage::new(), chunk);
    }
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_batch_views() {
    for chunk in [1, 7, 128] {
        let directory = tempfile::tempdir().unwrap();
        assert_batch_views(
            crate::storage::FjallStorage::open(directory.path()).unwrap(),
            chunk,
        );
    }
}

#[test]
fn prefixes_finish_joins() {
    for width in [3, 128, 255, 256, 257, 512] {
        for (items, positions) in [(2, 1), (3, 2)] {
            let source = joined_source(MemoryStorage::new(), width, true);
            let reader = Oplog::new();
            reader.receive_ops(vec![source.genesis.clone()]).unwrap();
            let paged = page_informed(&source, &reader, items, positions, true);
            assert!(
                paged.complete,
                "{width} actors stalled after {} rounds",
                paged.rounds
            );
            assert_eq!(paged.sent, 2 * width + 1);
            let work = source.engine.page_work();
            eprintln!(
                "join width={width} items={items} rounds={} edges={} visits={}",
                paged.rounds, work.edges, work.visits
            );
            assert!(
                work.edges <= 40 * width as u64,
                "{width} actors visited {} edges",
                work.edges
            );
        }
    }
}

#[test]
#[ignore = "signed join beyond both real request and position limits, run explicitly"]
fn real_join_finishes() {
    const ITEMS: usize = 65_536;
    const WIDTH: usize = ITEMS + MAX_PAGE_MISSING + 1;
    let source = joined_source(MemoryStorage::new(), WIDTH, true);
    let reader = Oplog::new();
    reader.receive_ops(vec![source.genesis.clone()]).unwrap();
    let responder = &source.engine;
    let receiver = SyncEngine::new(reader.clone(), source.reader);
    let summary = responder.summary(source.topic_id).unwrap();
    let request = receiver.plan_request(source.reader, &summary).unwrap();
    assert_eq!(request.actor_range_hints.len(), ITEMS);
    let held = reader.storage().actor_clock(&source.topic_id).unwrap();
    let behind = summary
        .actor_clock
        .iter()
        .filter(|(actor, seq)| **seq > held.get(actor))
        .count();
    assert!(behind - request.actor_range_hints.len() > MAX_PAGE_MISSING);
    let encoded = postcard::to_allocvec(&request).unwrap();
    let decoded: crate::sync::SyncRequest = postcard::from_bytes(&encoded).unwrap();
    assert_eq!(decoded, request);
    let paged = page_informed(&source, &reader, ITEMS, MAX_PAGE_MISSING, true);
    assert!(paged.complete);
    assert_eq!(paged.sent, 2 * WIDTH + 1);
    assert_eq!(
        reader.storage().list_op_ids(&source.topic_id).unwrap(),
        source.log.storage().list_op_ids(&source.topic_id).unwrap()
    );
    let work = responder.page_work();
    assert!(
        work.edges < 128 * WIDTH as u64,
        "{} repeated edges",
        work.edges
    );
    eprintln!(
        "real_join actors={WIDTH} behind={behind} request_bytes={} rounds={} visits={} edges={}",
        encoded.len(),
        paged.rounds,
        work.visits,
        work.edges
    );
}

#[test]
fn bounded_inventory_finishes() {
    for width in [31, 32, 33, 255, 256, 257] {
        let mut source = joined_source(MemoryStorage::new(), width, true);
        source.engine = source
            .engine
            .clone()
            .with_page_actors(2)
            .with_page_visits(6, 16);
        let reader = Oplog::new();
        reader.receive_ops(vec![source.genesis.clone()]).unwrap();
        let paged = page_informed(&source, &reader, 3, 2, true);
        assert!(paged.complete);
        assert_eq!(paged.sent, 2 * width + 1);
        let work = source.engine.page_work();
        assert!(work.resumed > 0, "inventory did not span slices");
        assert_eq!(work.kept_bytes, 0);
        assert!(work.actors < 64 * width as u64, "{work:?}");
    }
}

#[test]
fn held_inventory_skipped() {
    let mut source = joined_source(MemoryStorage::new(), 33, true);
    source.engine = source.engine.clone().with_page_visits(6, 16);
    let ops = oplog::topological(source.log.storage(), &source.topic_id).unwrap();
    let last = ops
        .iter()
        .filter(|op| op.signed.body.generation == 3)
        .max_by_key(|op| op.signed.body.actor_id)
        .unwrap()
        .id;
    let reader = Oplog::new();
    reader
        .receive_ops(ops.into_iter().filter(|op| op.id != last).collect())
        .unwrap();
    let paged = page_bounded(&source, &reader, 3, 2);
    assert!(paged.complete);
    assert_eq!(paged.sent, 1);
    assert_eq!(paged.empty, 0);
    assert_eq!(source.engine.page_work().actors, 1);
    assert_eq!(source.engine.page_work().kept_bytes, 0);
}

#[test]
fn tip_work_scales() {
    for width in [128, 256, 257] {
        let source = joined_source(MemoryStorage::new(), width, true);
        let reader = Oplog::new();
        reader.receive_ops(vec![source.genesis.clone()]).unwrap();
        let engine = SyncEngine::new(reader, source.reader);
        let summary = source.engine.summary(source.topic_id).unwrap();
        let request = engine.plan_request(source.reader, &summary).unwrap();
        assert!(request.wants.is_empty());
        assert_eq!(
            request.actor_range_hints.len(),
            summary.actor_clock.len() - 1
        );
        let tips = engine.page_work().tips;
        assert!(
            tips <= 2 * summary.actor_tips.len() as u64,
            "{width} actors examined {tips} tips"
        );
    }
}

#[test]
fn inventory_fills_page() {
    let source = joined_source(MemoryStorage::new(), 33, true);
    let reader = Oplog::new();
    reader.receive_ops(vec![source.genesis.clone()]).unwrap();
    let receiver = SyncEngine::new(reader.clone(), source.reader);
    let responder = source
        .engine
        .clone()
        .with_page_actors(4)
        .with_page_visits(104, 16);
    let summary = responder.summary(source.topic_id).unwrap();
    let request = receiver.plan_request(source.reader, &summary).unwrap();
    let held = receiver.summary(source.topic_id).unwrap();
    let budget = PageBudget {
        ops: 4,
        bytes: 32 * 1024 * 1024,
    };
    for _ in 0..8 {
        let page = responder
            .response_with(source.reader, &request, budget, &held)
            .unwrap();
        if page.ops.is_empty() {
            assert!(page.continued);
            continue;
        }
        assert_eq!(page.ops.len(), 4, "inventory setup consumed the data slice");
        let ids = page.ops.iter().map(|op| op.id).collect::<BTreeSet<_>>();
        assert_eq!(reader.receive_ops(page.ops).unwrap(), ids);
        assert_eq!(responder.page_work().kept_bytes, 0);
        return;
    }
    panic!("inventory did not produce a page");
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_prefixes_finish() {
    for width in [3, 257] {
        let source_dir = tempfile::tempdir().unwrap();
        let reader_dir = tempfile::tempdir().unwrap();
        let source = joined_source(
            crate::storage::FjallStorage::open(source_dir.path()).unwrap(),
            width,
            true,
        );
        let reader =
            Oplog::with_storage(crate::storage::FjallStorage::open(reader_dir.path()).unwrap());
        reader.receive_ops(vec![source.genesis.clone()]).unwrap();
        source
            .engine
            .put_obligation(
                source.reader,
                source.topic_id,
                source.log.storage().list_op_ids(&source.topic_id).unwrap(),
            )
            .unwrap();
        let paged = page_informed(&source, &reader, 2, 1, true);
        assert!(paged.complete);
        assert_eq!(paged.sent, 2 * width + 1);
        assert!(
            source
                .log
                .storage()
                .has_sync_obligations(&source.reader, &source.topic_id)
                .unwrap()
        );
    }
}

/// A responder refuses a request past its item limit, wants and hints together.
#[test]
fn request_items_refused() {
    let source = late_dependency(MemoryStorage::new(), 4);
    let reader = Oplog::new();
    reader.receive_ops(vec![source.genesis.clone()]).unwrap();
    let summary = source.engine.summary(source.topic_id).unwrap();
    let request = SyncEngine::new(reader, source.reader)
        .plan_request(source.reader, &summary)
        .unwrap();
    assert_eq!(request.actor_range_hints.len(), 4);
    let budget = PageBudget::from_credit(request.credit);
    let refused =
        source
            .engine
            .clone()
            .with_request_items(3)
            .response_page(source.reader, &request, budget);
    assert!(
        matches!(&refused, Err(Error::SyncCapacity(detail)) if !detail.is_empty()),
        "{refused:?}"
    );
    let served = source
        .engine
        .clone()
        .with_request_items(4)
        .response_page(source.reader, &request, budget)
        .unwrap();
    assert_eq!(served.ops.len(), 4);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_truncated_causal() {
    let dirs = std::sync::Mutex::new(Vec::new());
    assert_truncated_causal(|| {
        let dir = tempfile::tempdir().unwrap();
        let storage = crate::storage::FjallStorage::open(dir.path()).unwrap();
        dirs.lock().unwrap().push(dir);
        storage
    });
}

/// A reader repairing a lost op while behind on other actors: the want and the
/// positions it needs share the items, and the reader ends whole.
#[test]
fn wants_share_items() {
    let source = late_dependency(MemoryStorage::new(), 4);
    let ops = oplog::topological(source.log.storage(), &source.topic_id).unwrap();
    let reader = Oplog::new();
    reader.receive_ops(ops[..3].to_vec()).unwrap();
    damage_op(reader.storage(), &ops[2].id, Damage::Op);
    let paged = page_truncated(&source, &reader, 2);
    assert_eq!(paged.sent, ops.len() - 2);
    assert!(paged.rounds <= 6, "{} rounds", paged.rounds);
    assert!(reader.storage().dep_resolvable(&ops[2].id).unwrap());
}

#[cfg(feature = "iroh")]
mod sessions {
    use super::*;
    use crate::storage::StagingLimits;

    /// A node keyed by `seed` over `storage`, serving on its own endpoint with
    /// requests of at most `items` wants and hints.
    async fn capped<S: Storage>(
        storage: S,
        seed: u8,
        items: usize,
    ) -> (Irokle<S>, Arc<net::IrohNet<S>>, iroh::EndpointAddr) {
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .secret_key(iroh::SecretKey::from_bytes(&[seed; 32]))
            .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let config = NodeConfig {
            signer: Ed25519Signer::from_iroh_secret_key(endpoint.secret_key()),
            peer_whitelist: None,
            ..NodeConfig::default()
        };
        let node = Irokle::with_storage(storage, config)
            .unwrap()
            .with_request_items(items);
        let net = Arc::new(net::IrohNet::new(endpoint, node.clone()).unwrap());
        net.start_accept_loop().unwrap();
        let addr = super::super::iroh::ready_addr(net.endpoint()).await;
        (node.with_net(Arc::clone(&net)), net, addr)
    }

    /// Sync `topic_id` from `addr` until it completes, within a bounded number
    /// of calls that each keep paging while they advance.
    async fn sync_through<S: Storage>(
        reader: &net::IrohNet<S>,
        addr: iroh::EndpointAddr,
        topic_id: TopicId,
    ) {
        sync_calls(reader, addr, topic_id, 8).await;
    }

    /// [`sync_through`] within `calls` calls.
    async fn sync_calls<S: Storage>(
        reader: &net::IrohNet<S>,
        addr: iroh::EndpointAddr,
        topic_id: TopicId,
        calls: usize,
    ) {
        for _ in 0..calls {
            match reader.sync_now(addr.clone(), topic_id).await {
                Ok(()) => return,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("sync failed: {error}"),
            }
        }
        panic!("sync did not finish");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn joins_finish_sessions() {
        for member in [true, false] {
            let source = joined_source(MemoryStorage::new(), 3, member);
            let (_, alice, addr) = capped(source.log.storage().clone(), 232, 2).await;
            let store = MemoryStorage::new();
            if member {
                Oplog::with_storage(store.clone())
                    .receive_ops(vec![source.genesis.clone()])
                    .unwrap();
            }
            let (_, bob, _) = capped(store.clone(), 233, 2).await;
            sync_through(&bob, addr, source.topic_id).await;
            assert_eq!(
                store.list_op_ids(&source.topic_id).unwrap(),
                source.log.storage().list_op_ids(&source.topic_id).unwrap()
            );
            assert_eq!(
                store.actor_clock(&source.topic_id).unwrap(),
                source.log.storage().actor_clock(&source.topic_id).unwrap()
            );
            assert!(store.provisional_topics().unwrap().is_empty());
            bob.shutdown().await;
            alice.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn network_work_scales() {
        let mut previous = None;
        for actors in [40, 80] {
            let source = super::super::progress::reverse_chain(MemoryStorage::new(), actors);
            let reader = MemoryStorage::new();
            Oplog::with_storage(reader.clone())
                .receive_ops(vec![source.genesis.clone()])
                .unwrap();
            let mut peers = Vec::new();
            for (store, seed) in [(source.log.storage().clone(), 230), (reader.clone(), 231)] {
                let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
                    .secret_key(iroh::SecretKey::from_bytes(&[seed; 32]))
                    .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
                    .bind()
                    .await
                    .unwrap();
                let node = Irokle::with_storage(
                    store,
                    NodeConfig {
                        signer: Ed25519Signer::from_bytes(&[seed; 32]),
                        peer_whitelist: None,
                        ..NodeConfig::default()
                    },
                )
                .unwrap()
                .with_request_items(3)
                .with_page_visits(1);
                let net = Arc::new(net::IrohNet::new(endpoint, node.clone()).unwrap());
                net.start_accept_loop().unwrap();
                let address = super::super::iroh::ready_addr(net.endpoint()).await;
                peers.push((node, net, address));
            }
            sync_calls(&peers[1].1, peers[0].2.clone(), source.topic_id, 64).await;
            let work = peers[0].0.sync_engine().page_work();
            let total = work.visits + work.edges + work.actors;
            println!("network_work actors={actors} planner_work={total}");
            assert_eq!(
                reader.list_op_ids(&source.topic_id).unwrap(),
                source.log.storage().list_op_ids(&source.topic_id).unwrap()
            );
            assert_eq!(
                reader.actor_clock(&source.topic_id).unwrap(),
                source.log.storage().actor_clock(&source.topic_id).unwrap()
            );
            for (_, net, _) in &peers {
                net.shutdown().await;
                assert_eq!(net.owned_bytes().jobs, 0);
                assert!(net.owned_bytes().current.values().all(|bytes| *bytes == 0));
                assert_eq!(net.plan_counts(), (0, 0));
                assert_eq!(net.retained_goals(), 0);
            }
            if let Some(previous) = previous {
                assert!(
                    total <= 3 * previous,
                    "network transfers rebuilt completed planning work"
                );
            }
            previous = Some(total);
        }
    }

    /// An ordinary sync through real sessions whose requests cannot name every
    /// actor: the reader buffers nothing, so no page carried an op before its
    /// dependency, and ends with the source's history.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn truncated_session() {
        let storage = MemoryStorage::new();
        let source = late_dependency(storage.clone(), 6);
        let (_, alice_net, alice_addr) = capped(storage, 242, 2).await;
        let bob_storage = StaleReadStorage::new(MemoryStorage::new());
        Oplog::with_storage(bob_storage.clone())
            .receive_ops(vec![source.genesis.clone()])
            .unwrap();
        let (bob, bob_net, _) = capped(bob_storage.clone(), 243, 2).await;
        assert_eq!(bob.peer_id(), source.reader);
        sync_through(&bob_net, alice_addr, source.topic_id).await;
        assert_eq!(
            bob.storage().list_op_ids(&source.topic_id).unwrap(),
            source.log.storage().list_op_ids(&source.topic_id).unwrap()
        );
        assert_eq!(
            bob_storage
                .pending_puts
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        bob_net.shutdown().await;
        alice_net.shutdown().await;
    }

    /// A behind-only bootstrap whose essential dependency lies beyond the item window uses small
    /// streams after staging expiry and restaging, then reconnects: staging remains empty and
    /// the topic activates with the whole history.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bootstrap_reconnect_reset() {
        let invited = Ed25519Signer::from_bytes(&[245; 32]).peer_id();
        let storage = MemoryStorage::new();
        let source = late_dependency(storage.clone(), 6);
        let owner = Ed25519Signer::from_bytes(&[242; 32]);
        source
            .log
            .create_control_op(
                source.topic_id,
                actor_id_for(source.topic_id, owner.peer_id()),
                TopicControl::AddPeer { peer: invited },
                &owner,
            )
            .unwrap();
        let ops = oplog::topological(source.log.storage(), &source.topic_id).unwrap();
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .secret_key(iroh::SecretKey::from_bytes(&[242; 32]))
            .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let config = NodeConfig {
            signer: owner.clone(),
            peer_whitelist: None,
            ..NodeConfig::default()
        };
        let alice = Irokle::with_storage(storage, config)
            .unwrap()
            .with_request_items(3);
        // Small streams cut every page to a few ops.
        let limits = crate::net::StreamLimits {
            bytes: 4 * 1024,
            messages: 8,
            ..crate::net::StreamLimits::default()
        };
        let alice_net = Arc::new(
            net::IrohNet::new(endpoint, alice)
                .unwrap()
                .with_stream_limits(limits),
        );
        alice_net.start_accept_loop().unwrap();
        let alice_addr = super::super::iroh::ready_addr(alice_net.endpoint()).await;

        let bob_storage = StaleReadStorage::new(MemoryStorage::new());
        let (bob, bob_net, _) = capped(bob_storage.clone(), 245, 3).await;
        let fragment = |ops: &[Op]| crate::sync::SyncData {
            topic_id: source.topic_id,
            ops: ops.to_vec(),
        };
        let first = bob
            .receive_sync_outcome(owner.peer_id(), fragment(&ops[..2]))
            .unwrap();
        assert!(matches!(first, crate::node::ReceiveOutcome::Staged(_)));
        let session = bob.storage().provisional_topics().unwrap()[0].session;
        bob.expire_bootstraps(u64::MAX).unwrap();
        assert!(bob.storage().provisional_topics().unwrap().is_empty());
        let restaged = bob
            .receive_sync_outcome(owner.peer_id(), fragment(&ops[..2]))
            .unwrap();
        assert!(matches!(restaged, crate::node::ReceiveOutcome::Staged(_)));
        assert!(bob.storage().provisional_topics().unwrap()[0].session > session);
        bob_net.shutdown().await;
        drop(bob_net);

        let (_, bob_net, _) = capped(bob_storage.clone(), 245, 3).await;
        sync_through(&bob_net, alice_addr, source.topic_id).await;
        assert!(
            bob.storage()
                .topic_state(&source.topic_id)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            bob.storage().list_op_ids(&source.topic_id).unwrap(),
            source.log.storage().list_op_ids(&source.topic_id).unwrap()
        );
        assert_eq!(
            bob_storage
                .pending_puts
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        bob_net.shutdown().await;
        alice_net.shutdown().await;
    }

    /// A pull of a late-invited topic through requests that cannot name every actor leaves
    /// staging empty and activates the whole history. With three items, a dependent needs
    /// positions for its dependency actor and the genesis actor beside its own.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn truncated_bootstrap() {
        let invited = Ed25519Signer::from_bytes(&[244; 32]).peer_id();
        let storage = MemoryStorage::new();
        let source = late_dependency(storage.clone(), 6);
        let owner = Ed25519Signer::from_bytes(&[242; 32]);
        source
            .log
            .create_control_op(
                source.topic_id,
                actor_id_for(source.topic_id, owner.peer_id()),
                TopicControl::AddPeer { peer: invited },
                &owner,
            )
            .unwrap();
        let (_, alice_net, alice_addr) = capped(storage, 242, 3).await;
        let bob_storage =
            StaleReadStorage::new(MemoryStorage::new().with_staging_limits(StagingLimits::MEMORY));
        let (bob, bob_net, _) = capped(bob_storage.clone(), 244, 3).await;
        sync_through(&bob_net, alice_addr, source.topic_id).await;
        assert!(
            bob.storage()
                .topic_state(&source.topic_id)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            bob.storage().list_op_ids(&source.topic_id).unwrap(),
            source.log.storage().list_op_ids(&source.topic_id).unwrap()
        );
        assert_eq!(
            bob_storage
                .pending_puts
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        bob_net.shutdown().await;
        alice_net.shutdown().await;
    }

    /// A dependency chain beyond request and page windows uses real sessions with three-item
    /// requests for ordinary catch-up from genesis and post-chain bootstrap. Neither buffers an
    /// op before its dependency, and both end with the whole history.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn chain_beyond_sessions() {
        const ACTORS: usize = MAX_PAGE_MISSING + 44;
        let storage = MemoryStorage::new();
        let source = super::super::progress::reverse_chain(storage.clone(), ACTORS);
        let owner = Ed25519Signer::from_bytes(&[230; 32]);
        let invited = Ed25519Signer::from_bytes(&[234; 32]).peer_id();
        source
            .log
            .create_control_op(
                source.topic_id,
                actor_id_for(source.topic_id, owner.peer_id()),
                TopicControl::AddPeer { peer: invited },
                &owner,
            )
            .unwrap();
        let (_, alice_net, alice_addr) = capped(storage, 230, 3).await;
        let expected = source.log.storage().list_op_ids(&source.topic_id).unwrap();

        let bob_storage = StaleReadStorage::new(MemoryStorage::new());
        Oplog::with_storage(bob_storage.clone())
            .receive_ops(vec![source.genesis.clone()])
            .unwrap();
        let (bob, bob_net, _) = capped(bob_storage.clone(), 231, 3).await;
        assert_eq!(bob.peer_id(), source.reader);
        sync_calls(&bob_net, alice_addr.clone(), source.topic_id, 64).await;
        assert_eq!(
            bob.storage().list_op_ids(&source.topic_id).unwrap(),
            expected
        );

        let carol_storage =
            StaleReadStorage::new(MemoryStorage::new().with_staging_limits(StagingLimits::MEMORY));
        let (carol, carol_net, _) = capped(carol_storage.clone(), 234, 3).await;
        sync_calls(&carol_net, alice_addr, source.topic_id, 64).await;
        assert!(
            carol
                .storage()
                .topic_state(&source.topic_id)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            carol.storage().list_op_ids(&source.topic_id).unwrap(),
            expected
        );
        for pending in [&bob_storage.pending_puts, &carol_storage.pending_puts] {
            assert_eq!(pending.load(std::sync::atomic::Ordering::Relaxed), 0);
        }
        carol_net.shutdown().await;
        bob_net.shutdown().await;
        alice_net.shutdown().await;
    }
}
