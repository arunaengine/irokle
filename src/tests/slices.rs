//! Page plans end their work slice after a bounded number of storage reads. A
//! slice that sent nothing keeps its plan, and the same request goes on from it.

use super::pages::Source;
use super::progress::reverse_chain;
use super::support::*;

use crate::oplog::Oplog;
use crate::sync::{MAX_CONTINUATIONS, PageBudget, RequestKnowledge, SyncEngine};

/// Storage reads one slice of the tests may make.
const VISITS: usize = 12;

/// Page a reader at the genesis through `source` with slices of [`VISITS`]
/// reads. Every page carries data or a kept plan, no slice reads much past its
/// budget, and the reader ends with the source's frontier. Returns the pages
/// and how many of them only continued.
fn page_slices<S: Storage>(source: &Source<S>) -> (usize, usize) {
    let responder = source.engine.clone().with_page_visits(VISITS, 4);
    let reader = Oplog::new();
    reader.receive_ops(vec![source.genesis.clone()]).unwrap();
    let reader_engine = SyncEngine::new(reader.clone(), source.reader);
    let mut knowledge = RequestKnowledge::default();
    let (mut pages, mut continued) = (0, 0);
    loop {
        let summary = responder.summary(source.topic_id).unwrap();
        let request = reader_engine
            .plan_request_with(source.reader, &summary, &knowledge)
            .unwrap();
        if request.actor_range_hints.is_empty() && request.wants.is_empty() {
            break;
        }
        assert!(pages < 256, "paging did not finish");
        let before = responder.page_work();
        let page = responder
            .response_page(
                source.reader,
                &request,
                PageBudget::from_credit(request.credit),
            )
            .unwrap();
        let visits = responder.page_work().visits - before.visits;
        // One head past the budget: its dependency, index and op reads.
        assert!(visits <= VISITS as u64 + 4, "page {pages} read {visits}");
        assert!(page.more || !page.continued);
        if page.ops.is_empty() {
            assert!(
                page.continued,
                "page {pages} carried nothing and kept nothing"
            );
            continued += 1;
        }
        let ids = page.ops.iter().map(|op| op.id).collect::<BTreeSet<_>>();
        let received = !ids.is_empty();
        assert_eq!(reader.receive_ops(page.ops).unwrap(), ids, "page {pages}");
        let actors = summary.actor_clock.iter().count();
        knowledge.settle(
            &request.window,
            (&page.positions, page.continued),
            (received, actors),
        );
        pages += 1;
    }
    assert_eq!(
        reader.storage().heads(&source.topic_id).unwrap(),
        source.log.storage().heads(&source.topic_id).unwrap()
    );
    let work = responder.page_work();
    assert_eq!(work.resumed, continued as u64);
    assert_eq!(work.kept_bytes, 0, "a finished plan keeps nothing");
    (pages, continued)
}

/// A chain through forty actors in reverse key order with a two-actor window:
/// the descent to its first sendable op takes several slices, each kept and
/// resumed, and the chain completes.
fn assert_slices_resume<S: Storage>(storage: S) {
    let mut source = reverse_chain(storage, 40);
    source.engine = source.engine.clone().with_page_actors(2);
    let (pages, continued) = page_slices(&source);
    assert!(continued >= 2, "{continued} of {pages} pages continued");
    assert!(pages <= continued + 40, "{pages} pages");
}

#[test]
fn memory_slices_resume() {
    assert_slices_resume(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_slices_resume() {
    let dir = tempfile::tempdir().unwrap();
    assert_slices_resume(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// A slice that sent nothing and cannot keep its plan fails its request
/// instead of reporting an empty page to be repeated forever.
#[test]
fn slice_kept_or_refused() {
    let mut source = reverse_chain(MemoryStorage::new(), 40);
    source.engine = source.engine.clone().with_page_actors(2);
    let responder = source.engine.clone().with_page_visits(VISITS, 0);
    let reader = Oplog::new();
    reader.receive_ops(vec![source.genesis.clone()]).unwrap();
    let summary = responder.summary(source.topic_id).unwrap();
    let request = SyncEngine::new(reader, source.reader)
        .plan_request(source.reader, &summary)
        .unwrap();
    let refused = responder.response_page(
        source.reader,
        &request,
        PageBudget::from_credit(request.credit),
    );
    assert!(
        matches!(&refused, Err(Error::SyncCapacity(_))),
        "{refused:?}"
    );
}

/// At the real capacity every peer below and at it keeps a plan of one deep
/// topic; one more is refused with a capacity error instead of an empty page,
/// while a request that needs no kept plan is still served. Once a kept plan
/// completes, its place serves the refused peer.
#[test]
fn plans_at_capacity() {
    let storage = MemoryStorage::new();
    let mut source = reverse_chain(storage.clone(), 40);
    let small = reverse_chain(storage, 2);
    source.engine = source.engine.clone().with_page_actors(2);
    let responder = source
        .engine
        .clone()
        .with_page_visits(VISITS, MAX_CONTINUATIONS);
    let reader = Oplog::new();
    reader.receive_ops(vec![source.genesis.clone()]).unwrap();
    let summary = responder.summary(source.topic_id).unwrap();
    let request = SyncEngine::new(reader, source.reader)
        .plan_request(source.reader, &summary)
        .unwrap();
    // The chain's writers are members, so each asks as its own peer.
    let state = source
        .log
        .storage()
        .topic_state(&source.topic_id)
        .unwrap()
        .unwrap();
    let peers = state
        .members
        .iter()
        .copied()
        .filter(|peer| *peer != source.reader)
        .take(MAX_CONTINUATIONS + 1)
        .collect::<Vec<_>>();
    let budget = PageBudget::from_credit(request.credit);
    for peer in &peers[..MAX_CONTINUATIONS] {
        let page = responder.response_page(*peer, &request, budget).unwrap();
        assert!(page.continued && page.ops.is_empty());
    }
    let refused = responder.response_page(peers[MAX_CONTINUATIONS], &request, budget);
    assert!(
        matches!(&refused, Err(Error::SyncCapacity(_))),
        "{refused:?}"
    );
    let small_reader = Oplog::new();
    small_reader
        .receive_ops(vec![small.genesis.clone()])
        .unwrap();
    let small_summary = responder.summary(small.topic_id).unwrap();
    let small_request = SyncEngine::new(small_reader, small.reader)
        .plan_request(small.reader, &small_summary)
        .unwrap();
    let served = responder
        .response_page(small.reader, &small_request, budget)
        .unwrap();
    assert_eq!(served.ops.len(), 2);

    let first = peers[0];
    let mut resumed = 0;
    while responder
        .response_page(first, &request, budget)
        .unwrap()
        .ops
        .is_empty()
    {
        resumed += 1;
        assert!(resumed < 256, "the kept plan did not complete");
    }
    let page = responder
        .response_page(peers[MAX_CONTINUATIONS], &request, budget)
        .unwrap();
    assert!(page.continued, "the freed place serves the refused peer");
}

/// Appends after a plan was kept are later work: the same request, whose goal
/// now reaches further, goes on from the kept plan, and a later request pages
/// the appends.
#[test]
fn appends_later_work() {
    let mut source = reverse_chain(MemoryStorage::new(), 40);
    source.engine = source.engine.clone().with_page_actors(2);
    let responder = source.engine.clone().with_page_visits(VISITS, 4);
    let reader = Oplog::new();
    reader.receive_ops(vec![source.genesis.clone()]).unwrap();
    let reader_engine = SyncEngine::new(reader.clone(), source.reader);
    let plan = |responder: &SyncEngine<MemoryStorage>| {
        let summary = responder.summary(source.topic_id).unwrap();
        reader_engine.plan_request(source.reader, &summary).unwrap()
    };
    let request = plan(&responder);
    let first = responder
        .response_page(
            source.reader,
            &request,
            PageBudget::from_credit(request.credit),
        )
        .unwrap();
    assert!(first.continued && first.ops.is_empty());

    let owner = Ed25519Signer::from_bytes(&[230; 32]);
    source
        .log
        .create_event_op(
            source.topic_id,
            actor_id_for(source.topic_id, owner.peer_id()),
            EventEnvelope::encode_event(&Note {
                text: "later".into(),
            })
            .unwrap(),
            &owner,
        )
        .unwrap();
    let grown = plan(&responder);
    assert_ne!(grown.actor_range_hints, request.actor_range_hints);
    let resumed = responder
        .response_page(source.reader, &grown, PageBudget::from_credit(grown.credit))
        .unwrap();
    assert_eq!(responder.page_work().resumed, 1);
    assert!(resumed.more);
    reader.receive_ops(resumed.ops).unwrap();
}

/// A destructive reset between slices drops the kept plan: the next request,
/// though it names the same positions, is planned afresh on the new epoch.
#[test]
fn reset_drops_plan() {
    let mut source = reverse_chain(MemoryStorage::new(), 40);
    source.engine = source.engine.clone().with_page_actors(2);
    let responder = source.engine.clone().with_page_visits(VISITS, 4);
    let reader = Oplog::new();
    reader.receive_ops(vec![source.genesis.clone()]).unwrap();
    let reader_engine = SyncEngine::new(reader, source.reader);
    let page = |responder: &SyncEngine<MemoryStorage>| {
        let summary = responder.summary(source.topic_id).unwrap();
        let request = reader_engine.plan_request(source.reader, &summary).unwrap();
        responder
            .response_page(
                source.reader,
                &request,
                PageBudget::from_credit(request.credit),
            )
            .unwrap()
    };
    assert!(page(&responder).continued);
    let storage = source.log.storage();
    let ops = oplog::topological(storage, &source.topic_id).unwrap();
    assert!(storage.reset_topic(&source.topic_id).unwrap() > 0);
    source.log.receive_ops(ops).unwrap();
    assert!(page(&responder).continued);
    assert_eq!(responder.page_work().resumed, 0);
}

#[cfg(feature = "iroh")]
mod sessions {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_sessions_finish() {
        use crate::sync::SyncMessage;
        use std::time::Duration;

        let storage = MemoryStorage::new();
        let source = reverse_chain(storage.clone(), 40);
        let bind = |seed| {
            iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
                .secret_key(iroh::SecretKey::from_bytes(&seed))
                .alpns(vec![net::IROKLE_SYNC_ALPN.to_vec()])
                .bind()
        };
        let config = |endpoint: &iroh::Endpoint| NodeConfig {
            signer: Ed25519Signer::from_iroh_secret_key(endpoint.secret_key()),
            peer_whitelist: None,
            ..NodeConfig::default()
        };
        let endpoint = bind([230; 32]).await.unwrap();
        let alice = Irokle::with_storage(storage, config(&endpoint))
            .unwrap()
            .with_page_visits(VISITS);
        let runtime = net::IrohRuntimeConfig {
            sync_io_timeout: Duration::from_secs(300),
            ..Default::default()
        };
        let server =
            Arc::new(net::IrohNet::new_with_config(endpoint, alice.clone(), runtime).unwrap());
        server.start_accept_loop().unwrap();
        let address = super::super::iroh::ready_addr(server.endpoint()).await;
        let held = server.hold_planners().await;
        let summary = alice.sync_summary(source.topic_id).unwrap();
        let mut clients = Vec::new();
        let mut pulls = Vec::new();
        for n in 0..32 {
            let mut seed = [11; 32];
            seed[..8].copy_from_slice(&(n as u64).to_le_bytes());
            let endpoint = bind(seed).await.unwrap();
            let store = MemoryStorage::new();
            Oplog::with_storage(store.clone())
                .receive_ops(vec![source.genesis.clone()])
                .unwrap();
            let reader = Irokle::with_storage(store, config(&endpoint)).unwrap();
            let request = reader.plan_sync_request(alice.peer_id(), &summary).unwrap();
            let open = reader.sync_open(source.topic_id);
            let client =
                Arc::new(net::IrohNet::new_with_config(endpoint, reader, runtime).unwrap());
            pulls.push(tokio::spawn({
                let (client, address) = (Arc::clone(&client), address.clone());
                async move {
                    client
                        .sync_with(
                            address,
                            &[SyncMessage::Open(open), SyncMessage::Request(request)],
                        )
                        .await
                }
            }));
            clients.push(client);
            tokio::time::timeout(Duration::from_secs(60), async {
                while server.plan_counts() != ((n + 1).min(16), n + 1) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(server.plan_counts(), ((n + 1).min(16), n + 1));
        }
        let probe = clients[0]
            .sync_with(
                address.clone(),
                &[SyncMessage::Open(
                    clients[0].node().sync_open(source.topic_id),
                )],
            )
            .await
            .unwrap();
        assert!(
            !probe.is_empty(),
            "control traffic stopped behind queued plans"
        );
        drop(held);
        let mut completions = tokio::task::JoinSet::new();
        for (client, pull) in clients.iter().zip(pulls) {
            let replies = tokio::time::timeout(Duration::from_secs(300), pull)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            for reply in replies {
                match reply {
                    SyncMessage::Data(data) => {
                        client
                            .node()
                            .receive_sync_data_from(alice.peer_id(), data)
                            .unwrap();
                    }
                    SyncMessage::Failure(failure) => panic!("accepted plan refused: {failure:?}"),
                    SyncMessage::Page(page) => assert!(!page.continued),
                    _ => {}
                }
            }
            assert!(
                client
                    .node()
                    .storage()
                    .list_op_ids(&source.topic_id)
                    .unwrap()
                    .len()
                    > 1
            );
            let (client, address) = (Arc::clone(client), address.clone());
            let topic = source.topic_id;
            completions.spawn(async move { client.sync_now(address, topic).await });
        }
        while let Some(result) = completions.join_next().await {
            result.unwrap().unwrap();
        }
        for client in &clients {
            assert_eq!(
                client
                    .node()
                    .storage()
                    .list_op_ids(&source.topic_id)
                    .unwrap(),
                source.log.storage().list_op_ids(&source.topic_id).unwrap()
            );
        }
        assert!(alice.sync_engine().page_work().resumed >= 32);
        assert_eq!(server.plan_counts(), (0, 0));
        for client in clients {
            client.shutdown().await;
        }
        server.shutdown().await;
    }

    /// A sync through real sessions whose responder ends every slice after a
    /// few reads still completes: kept plans count as progress.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sliced_session() {
        let storage = MemoryStorage::new();
        let source = reverse_chain(storage.clone(), 40);
        let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .secret_key(iroh::SecretKey::from_bytes(&[230; 32]))
            .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let config = |endpoint: &iroh::Endpoint| NodeConfig {
            signer: Ed25519Signer::from_iroh_secret_key(endpoint.secret_key()),
            peer_whitelist: None,
            ..NodeConfig::default()
        };
        let alice = Irokle::with_storage(storage, config(&alice_endpoint))
            .unwrap()
            .with_page_visits(VISITS);
        let alice_net = Arc::new(net::IrohNet::new(alice_endpoint, alice.clone()).unwrap());
        alice_net.start_accept_loop().unwrap();
        let alice_addr = super::super::iroh::ready_addr(alice_net.endpoint()).await;

        let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .secret_key(iroh::SecretKey::from_bytes(&[231; 32]))
            .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let bob_storage = MemoryStorage::new();
        Oplog::with_storage(bob_storage.clone())
            .receive_ops(vec![source.genesis.clone()])
            .unwrap();
        let bob = Irokle::with_storage(bob_storage, config(&bob_endpoint)).unwrap();
        let bob_net = Arc::new(net::IrohNet::new(bob_endpoint, bob.clone()).unwrap());
        let mut calls = 0;
        loop {
            calls += 1;
            assert!(calls <= 8, "sync did not finish");
            match bob_net.sync_now(alice_addr.clone(), source.topic_id).await {
                Ok(()) => break,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("sync failed: {error}"),
            }
        }
        assert_eq!(
            bob.storage().list_op_ids(&source.topic_id).unwrap(),
            source.log.storage().list_op_ids(&source.topic_id).unwrap()
        );
        assert!(alice.sync_engine().page_work().resumed > 0);
        bob_net.shutdown().await;
        alice_net.shutdown().await;
    }

    /// On a one-worker runtime a responder's page planning is stuck in a slow
    /// durable store while an unrelated topic's exchange with the same
    /// responder completes; once the store answers, the sliced pull finishes.
    #[cfg(feature = "fjall")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn graph_beside_control() {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            StaleReadStorage::new(crate::storage::FjallStorage::open(dir.path()).unwrap());
        let source = reverse_chain(storage.clone(), 40);
        let key = |seed: u8| iroh::SecretKey::from_bytes(&[seed; 32]);
        let bind = |seed: u8| {
            iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
                .secret_key(key(seed))
                .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
                .bind()
        };
        let config = |seed: u8| NodeConfig {
            signer: Ed25519Signer::from_iroh_secret_key(&key(seed)),
            peer_whitelist: None,
            ..NodeConfig::default()
        };
        let alice = Irokle::with_storage(storage.clone(), config(230))
            .unwrap()
            .with_page_visits(VISITS);
        let alice_net =
            Arc::new(net::IrohNet::new(bind(230).await.unwrap(), alice.clone()).unwrap());
        alice_net.start_accept_loop().unwrap();
        let alice_addr = super::super::iroh::ready_addr(alice_net.endpoint()).await;
        let bob_storage = MemoryStorage::new();
        Oplog::with_storage(bob_storage.clone())
            .receive_ops(vec![source.genesis.clone()])
            .unwrap();
        let bob = Irokle::with_storage(bob_storage, config(231)).unwrap();
        let bob_net = Arc::new(net::IrohNet::new(bind(231).await.unwrap(), bob.clone()).unwrap());
        let other = alice
            .create_topic::<Note>(TopicConfig {
                initial_peers: [bob.peer_id()].into(),
                ..TopicConfig::default()
            })
            .unwrap()
            .id();

        let ops = oplog::topological(source.log.storage(), &source.topic_id).unwrap();
        let gate = Arc::new(Gate::default());
        let release = gate.releaser();
        storage.arm_read(GatePoint::Meta(ops[ops.len() - 1].id), Arc::clone(&gate));
        let pull = tokio::spawn({
            let (bob_net, alice_addr) = (Arc::clone(&bob_net), alice_addr.clone());
            async move { bob_net.sync_now(alice_addr, source.topic_id).await }
        });
        let arrival = Arc::clone(&gate);
        tokio::task::spawn_blocking(move || arrival.wait_arrival())
            .await
            .unwrap();
        let control = alice_net.lane_times()[0]
            .jobs
            .load(std::sync::atomic::Ordering::Relaxed);
        let probe = vec![
            crate::sync::SyncMessage::Open(bob.sync_open(other)),
            crate::sync::SyncMessage::Fingerprint(bob.sync_fingerprint(other).unwrap()),
        ];
        let replies = bob_net.sync_with(alice_addr.clone(), &probe).await.unwrap();
        assert!(!replies.is_empty());
        assert!(
            !gate.has_left(),
            "the control exchange waited for the stuck plan"
        );
        assert!(
            alice_net.lane_times()[0]
                .jobs
                .load(std::sync::atomic::Ordering::Relaxed)
                > control
        );

        drop(release);
        let mut result = pull.await.unwrap();
        for _ in 0..8 {
            match result {
                Ok(()) => break,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    result = bob_net.sync_now(alice_addr.clone(), source.topic_id).await;
                }
                Err(error) => panic!("sync failed: {error}"),
            }
        }
        assert!(result.is_ok());
        assert_eq!(
            bob.storage().list_op_ids(&source.topic_id).unwrap(),
            source.log.storage().list_op_ids(&source.topic_id).unwrap()
        );
        bob_net.shutdown().await;
        alice_net.shutdown().await;
    }
}
