//! Requests that name only part of the actors a reader is behind on: every
//! page stays causal over what the reader holds, and paging still completes.

use super::pages::{Source, late_dependency};
use super::support::*;

use crate::oplog::Oplog;
use crate::sync::{PageBudget, RequestKnowledge, SyncEngine};

/// What paging a reader to the source's frontier took.
struct Paged {
    rounds: usize,
    sent: usize,
    empty: usize,
}

/// Page `reader` to the frontier of `source` with requests of at most `items`
/// wants and hints on both sides. Every page must be causal over what the
/// reader holds, never repeat an op, and name a blocker when it carries nothing.
fn page_truncated<S: Storage>(source: &Source<S>, reader: &Oplog, items: usize) -> Paged {
    let responder = source.engine.clone().with_request_items(items);
    let reader_engine = SyncEngine::new(reader.clone(), source.reader).with_request_items(items);
    let mut knowledge = RequestKnowledge::default();
    let mut sent = BTreeSet::new();
    let (mut rounds, mut empty) = (0, 0);
    loop {
        let summary = responder.summary(source.topic_id).unwrap();
        let request = reader_engine
            .plan_request_with(source.reader, &summary, &knowledge)
            .unwrap();
        if request.actor_range_hints.is_empty() && request.wants.is_empty() {
            break;
        }
        assert!(request.actor_range_hints.len() + request.wants.len() <= items);
        assert!(rounds < 64, "paging did not finish");
        let budget = PageBudget::from_credit(request.credit);
        let page = responder
            .response_page(source.reader, &request, budget)
            .unwrap();
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
        knowledge.settle(
            &request.window,
            (&page.positions, page.continued),
            (!earlier.is_empty(), actors),
        );
        rounds += 1;
    }
    let (source_storage, reader_storage) = (source.log.storage(), reader.storage());
    assert_eq!(
        reader_storage.heads(&source.topic_id).unwrap(),
        source_storage.heads(&source.topic_id).unwrap()
    );
    assert_eq!(
        reader_storage.actor_clock(&source.topic_id).unwrap(),
        source_storage.actor_clock(&source.topic_id).unwrap()
    );
    Paged {
        rounds,
        sent: sent.len(),
        empty,
    }
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
        for _ in 0..8 {
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
}
