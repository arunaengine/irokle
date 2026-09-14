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
        let page = responder
            .response_page(source.reader, &request, budget)
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
                !page.positions.is_empty() || !page.missing.is_empty(),
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

/// A signed topic of `writers` writers sorted by actor key, whose ops after
/// the genesis are `ops`: each names its writer and the indices of the ops it
/// depends on, the genesis being index 0. Sequences, previous ops and
/// generations follow from that order.
fn graph_source<S: Storage>(storage: S, writers: usize, ops: &[(usize, &[usize])]) -> Source<S> {
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
    let mut signed = vec![genesis.clone()];
    let mut last = vec![None::<usize>; writers];
    for (writer, deps) in ops {
        let signer = &signers[*writer];
        let mut deps = deps
            .iter()
            .map(|index| signed[*index].id)
            .collect::<BTreeSet<_>>();
        let prev = last[*writer].map(|index| signed[index].id);
        deps.extend(prev);
        let generation = deps
            .iter()
            .map(|dep| {
                signed
                    .iter()
                    .find(|op| op.id == *dep)
                    .unwrap()
                    .signed
                    .body
                    .generation
                    + 1
            })
            .max()
            .unwrap_or(1);
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

/// A dependency chain or a branching graph through more unknown actors than
/// one request names, and than a page result or the reader's knowledge keeps,
/// still completes through successive requests: every page stays causal and
/// the reader reaches the frontier, in rounds that grow with the actors.
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

/// One op depending on three actors that stay behind until it is served
/// needs all three described at once. Within that capacity paging completes;
/// below it paging ends on a round without progress instead of cycling, and
/// never serves an op before its dependency.
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
    let paged = page_bounded(&source, &reader, 2, 1);
    assert!(!paged.complete);
    // Four actors: at most four times as many empty rounds, and a few more.
    assert!(paged.rounds <= 4 * 5 + 64 + 2, "{} rounds", paged.rounds);
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
    assert!(matches!(refused, Err(Error::Storage(_))), "{refused:?}");
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

    /// A behind-only bootstrap whose essential dependency lies beyond the item
    /// window, served through small streams, after the staged session was
    /// expired and restaged and the reader reconnected on a new endpoint:
    /// staging buffers nothing and the topic activates with the whole history.
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

    /// A pull of a topic the reader does not hold, invited last, through
    /// requests that cannot name every actor: staging buffers nothing and the
    /// topic activates with the whole history. Three items: before anything is
    /// staged, a dependent needs the positions of the dependency's actor and of
    /// the genesis actor beside its own.
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

    /// A dependency chain through far more actors than a request names and
    /// than a page result keeps, through real sessions with three-item
    /// requests: an ordinary catch-up of a reader holding only the genesis,
    /// and a bootstrap of a peer invited after the chain. Neither buffers an
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
