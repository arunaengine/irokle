//! Real exchanges run through the resync scheduler's own claims, leases and
//! batch path, with the scheduler state checked directly.

use super::*;
use crate::TopicId;
use crate::net::frame::MAX_FRAME_LEN;
use crate::tests::support::{Gate, GatePoint, Note, StaleReadStorage};

/// Wide enough that a backoff step cannot pass while a test is running.
const BACKOFF: Duration = Duration::from_secs(60);

pub(super) type Lookup = iroh::address_lookup::memory::MemoryLookup;

fn runtime() -> IrohRuntimeConfig {
    IrohRuntimeConfig {
        connect_timeout: Duration::from_secs(2),
        sync_io_timeout: Duration::from_secs(120),
        resync_interval: BACKOFF,
        resync_initial_backoff: BACKOFF,
        resync_max_backoff: Duration::from_secs(600),
        full_sweep_interval: Duration::ZERO,
        ..IrohRuntimeConfig::default()
    }
}

pub(super) async fn ready_addr(endpoint: &iroh::Endpoint) -> iroh::EndpointAddr {
    use futures::StreamExt;
    use iroh::Watcher;
    let addr = endpoint.addr();
    if !addr.addrs.is_empty() {
        return addr;
    }
    let mut stream = endpoint.watch_addr().stream();
    tokio::time::timeout(Duration::from_secs(5), async move {
        loop {
            let addr = stream.next().await.expect("address stream");
            if !addr.addrs.is_empty() {
                return addr;
            }
        }
    })
    .await
    .expect("dialable address")
}

async fn bind(lookup: &Lookup) -> iroh::Endpoint {
    iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .address_lookup(lookup.clone())
        .alpns(vec![IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await
        .unwrap()
}

/// A node that only answers: its net accepts but never runs a resync loop.
pub(super) async fn server<S: Storage>(
    storage: S,
    lookup: &Lookup,
    trusted: PeerId,
    limits: StreamLimits,
) -> (Irokle<S>, Arc<IrohNet<S>>) {
    let endpoint = bind(lookup).await;
    let node = Irokle::builder()
        .with_storage(storage)
        .with_iroh_secret_key(endpoint.secret_key())
        .with_peer_whitelist([trusted])
        .build()
        .unwrap();
    let net = Arc::new(
        IrohNet::new(endpoint, node.clone())
            .unwrap()
            .with_stream_limits(limits),
    );
    net.start_accept_loop().unwrap();
    lookup.add_endpoint_info(ready_addr(net.endpoint()).await);
    (node, net)
}

/// A node whose net starts no loop, so the test owns every claim.
pub(super) async fn client(lookup: &Lookup, limits: StreamLimits) -> (Irokle, Arc<IrohNet>) {
    let endpoint = bind(lookup).await;
    let node = Irokle::builder()
        .with_iroh_secret_key(endpoint.secret_key())
        .build()
        .unwrap();
    let net = IrohNet::new_with_config(endpoint, node.clone(), runtime())
        .unwrap()
        .with_stream_limits(limits);
    (node, Arc::new(net))
}

/// A topic of `owner` shared with `member`, which holds only its genesis.
pub(super) fn shared_topic<S: Storage>(owner: &Irokle, member: &Irokle<S>) -> TopicId {
    let topic = owner
        .create_topic::<Note>(crate::TopicConfig {
            initial_peers: [member.peer_id()].into(),
            ..crate::TopicConfig::default()
        })
        .unwrap();
    let ops = crate::oplog::topological(owner.storage(), &topic.id()).unwrap();
    member
        .receive_sync_data_from(
            owner.peer_id(),
            crate::sync::SyncData {
                topic_id: topic.id(),
                ops,
            },
        )
        .unwrap();
    topic.id()
}

pub(super) fn publish<S: Storage>(
    node: &Irokle<S>,
    topic_id: TopicId,
    count: usize,
    text_len: usize,
) {
    let topic = node.open_topic::<Note>(topic_id).unwrap();
    for index in 0..count {
        topic
            .publish(Note {
                text: format!("{index:0>text_len$}"),
            })
            .unwrap();
    }
}

fn clock<S: Storage>(node: &Irokle<S>, topic_id: TopicId) -> crate::ActorClock {
    node.storage().actor_clock(&topic_id).unwrap()
}

fn status(node: &Irokle, peer_id: PeerId, topic_id: TopicId) -> crate::SyncPeerStatus {
    node.sync_status(topic_id)
        .unwrap()
        .into_iter()
        .find(|status| status.peer_id == peer_id)
        .expect("a recorded attempt")
}

/// Runs every due target through the real batch path, one peer turn at a time,
/// until nothing is due. Returns the number of turns.
async fn drain_due(net: &Arc<IrohNet>, cap: usize) -> usize {
    for turn in 0..cap {
        let Some((peer_id, claims)) = net
            .resync_scheduler
            .due_targets(1, MAX_RESYNC_TOPICS)
            .pop()
        else {
            return turn;
        };
        let lease = net.resync_scheduler.lease(claims, BACKOFF);
        net.sync_peer_batch(peer_id, lease, runtime())
            .await;
    }
    panic!("targets were still due after {cap} turns");
}

/// A client pulling from a server whose small stream budget cuts pages to a
/// few ops, and a topic longer than the manual page budget.
struct Paged {
    alice: Irokle,
    net: Arc<IrohNet>,
    bob: Irokle,
    bob_net: Arc<IrohNet>,
    bob_addr: iroh::EndpointAddr,
    topic_id: TopicId,
}

async fn paged_pull() -> Paged {
    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let limits = StreamLimits {
        bytes: 16 * 1024,
        ..StreamLimits::default()
    };
    let (bob, bob_net) = server(MemoryStorage::new(), &lookup, alice.peer_id(), limits).await;
    let topic_id = shared_topic(&alice, &bob);
    publish(&bob, topic_id, MAX_NOW_PAGES * 20, 1024);
    let bob_addr = ready_addr(bob_net.endpoint()).await;
    Paged {
        alice,
        net,
        bob,
        bob_net,
        bob_addr,
        topic_id,
    }
}

/// A manual sync that runs out of pages with work left reports `WouldBlock`,
/// counts as progress, and leaves a due continuation that the scheduler then
/// runs to the end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn budget_schedules_continuation() {
    let paged = paged_pull().await;
    let bob_peer = paged.bob.peer_id();
    let result = paged
        .net
        .sync_now(paged.bob_addr.clone(), paged.topic_id)
        .await;
    assert_eq!(
        result.as_ref().map_err(io::Error::kind),
        Err(io::ErrorKind::WouldBlock),
        "{result:?}"
    );
    let target = clock(&paged.bob, paged.topic_id);
    let pulled = clock(&paged.alice, paged.topic_id);
    assert!(
        !pulled.dominates(&target),
        "the budget ended the pull early"
    );
    assert!(covered(&pulled, &target) > MAX_NOW_PAGES as u64);
    assert_eq!(
        paged
            .net
            .resync_scheduler
            .target_state(bob_peer, paged.topic_id),
        Some((None, 0, None)),
        "the continuation is scheduled and unowned"
    );
    assert!(paged.net.resync_scheduler.next_due().unwrap() <= tokio::time::Instant::now());
    assert_eq!(paged.alice.peer_health().failures(&bob_peer), 0);
    assert_eq!(
        status(&paged.alice, bob_peer, paged.topic_id).failed_attempts,
        0
    );

    drain_due(&paged.net, 256).await;
    assert!(
        clock(&paged.alice, paged.topic_id).dominates(&target),
        "the scheduled continuation did not finish the pull"
    );
    paged.net.shutdown().await;
    paged.bob_net.shutdown().await;
}

/// The continuation a manual sync leaves behind is not new work: a target in
/// failure backoff keeps its failure count and its delay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rearm_keeps_backoff() {
    let paged = paged_pull().await;
    let bob_peer = paged.bob.peer_id();
    let scheduler = &paged.net.resync_scheduler;
    scheduler.schedule_now(bob_peer, paged.topic_id, false);
    let (_, mut claims) = scheduler.due_targets(1, 1).pop().unwrap();
    scheduler.complete_failed(claims.remove(0), BACKOFF, Duration::from_secs(600));

    let result = paged
        .net
        .sync_now(paged.bob_addr.clone(), paged.topic_id)
        .await;
    assert_eq!(
        result.as_ref().map_err(io::Error::kind),
        Err(io::ErrorKind::WouldBlock),
        "{result:?}"
    );
    let (active, failures, force) = scheduler.target_state(bob_peer, paged.topic_id).unwrap();
    assert_eq!((active, failures), (None, 1));
    assert!(force.is_some(), "the failed attempt still owes its retry");
    assert!(
        scheduler.next_due().unwrap() > tokio::time::Instant::now() + BACKOFF / 2,
        "the continuation reset the failure backoff"
    );
    paged.net.shutdown().await;
    paged.bob_net.shutdown().await;
}

/// An advancing push continues at once when nothing else waits, but behind a
/// target that became due while it ran, and without a new work revision.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn continuation_yields_turn() {
    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let (bob, bob_net) = server(
        MemoryStorage::new(),
        &lookup,
        alice.peer_id(),
        StreamLimits::default(),
    )
    .await;
    let topic_id = shared_topic(&alice, &bob);
    // More than a push page plus the page the peer requests back.
    let page = crate::sync::SyncCredit::default().ops as usize;
    publish(&alice, topic_id, 2 * page + 256, 1);
    let bob_peer = bob.peer_id();
    let scheduler = &net.resync_scheduler;
    scheduler.schedule_now(bob_peer, topic_id, false);
    let (_, claims) = scheduler.due_targets(1, 1).pop().unwrap();
    let covered_work = claims[0].covered;
    // The smallest id also wins a tie on the due instant.
    let waiting = PeerId::from_bytes([0; 32]);
    scheduler.schedule_now(waiting, topic_id, false);

    let lease = scheduler.lease(claims, BACKOFF);
    net.sync_peer_batch(bob_peer, lease, runtime())
        .await;
    assert!(!clock(&bob, topic_id).dominates(&clock(&alice, topic_id)));
    assert_eq!(
        scheduler.target_state(bob_peer, topic_id),
        Some((None, 0, None)),
        "an advancing page is not a failure"
    );
    let key = ResyncTargetKey {
        peer_id: bob_peer,
        topic_id,
    };
    assert_eq!(
        scheduler.inner.lock().unwrap()[&key].requested,
        covered_work,
        "the continuation bumped the work revision"
    );
    let next = scheduler.due_targets(1, 1).pop().unwrap();
    assert_eq!(next.0, waiting, "the continuation jumped the queue");
    let next = scheduler.due_targets(1, 1).pop().unwrap();
    assert_eq!(next.0, bob_peer, "the continuation must run next");
    drop(scheduler.lease(next.1, BACKOFF));
    net.shutdown().await;
    bob_net.shutdown().await;
}

/// A peer that keeps answering without the data it claims backs off: every
/// unchanged attempt is one failure with a growing delay, never a fast loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_peer_backoff() {
    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let (bob, bob_net) = server(
        MemoryStorage::new(),
        &lookup,
        alice.peer_id(),
        StreamLimits::default(),
    )
    .await;
    let topic_id = shared_topic(&alice, &bob);
    publish(&bob, topic_id, 20, 1);
    let ops = crate::oplog::topological(bob.storage(), &topic_id).unwrap();
    bob.storage().drop_op_record(&ops[3].id);
    bob.recheck_topics().unwrap();
    let bob_peer = bob.peer_id();

    net.resync_scheduler.schedule_now(bob_peer, topic_id, false);
    let turns = drain_due(&net, 8).await;
    assert!(turns <= 3, "{turns} turns for one unchanged peer");
    let (active, failures, force) = net
        .resync_scheduler
        .target_state(bob_peer, topic_id)
        .unwrap();
    assert_eq!((active, failures), (None, 1));
    assert!(force.is_some());
    assert!(net.resync_scheduler.next_due().unwrap() > tokio::time::Instant::now() + BACKOFF / 2);

    // A later forced retry meets the same answer and doubles the delay.
    net.resync_scheduler.schedule_now(bob_peer, topic_id, true);
    let streams = net.outbound_sync_streams();
    assert_eq!(drain_due(&net, 8).await, 1);
    assert!(
        net.outbound_sync_streams() - streams <= 4,
        "one unchanged attempt must not loop over streams"
    );
    let (_, failures, _) = net
        .resync_scheduler
        .target_state(bob_peer, topic_id)
        .unwrap();
    assert_eq!(failures, 2);
    assert!(net.resync_scheduler.next_due().unwrap() > tokio::time::Instant::now() + BACKOFF);
    assert!(!clock(&alice, topic_id).dominates(&clock(&bob, topic_id)));
    assert_eq!(status(&alice, bob_peer, topic_id).failed_attempts, 2);
    assert_eq!(
        alice.peer_health().failures(&bob_peer),
        0,
        "a refusal is not unreachability"
    );
    net.shutdown().await;
    bob_net.shutdown().await;
}

/// The first topic of a batch finishes its push, pull and ack follow-up and is
/// published. The second then stalls at the peer until the batch deadline
/// expires: only the second is failed, and the first is not reinserted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expiry_spares_settled() {
    let lookup = Lookup::new();
    // One topic per exchange, so the first settles before the second is sent.
    let limits = StreamLimits {
        batch_messages: 1,
        ..StreamLimits::default()
    };
    let (alice, net) = client(&lookup, limits).await;
    let storage = StaleReadStorage::new(MemoryStorage::new());
    let (bob, bob_net) = server(
        storage.clone(),
        &lookup,
        alice.peer_id(),
        StreamLimits::default(),
    )
    .await;
    let mut topics = [shared_topic(&alice, &bob), shared_topic(&alice, &bob)];
    topics.sort();
    let [first, second] = topics;
    publish(&bob, first, 1, 1);
    publish(&alice, first, 1, 1);
    let stalled = alice
        .open_topic::<Note>(second)
        .unwrap()
        .publish(Note {
            text: "stalled".into(),
        })
        .unwrap()
        .meta
        .op_id;
    let bob_peer = bob.peer_id();
    let scheduler = &net.resync_scheduler;
    scheduler.schedule_now(bob_peer, first, false);
    scheduler.schedule_now(bob_peer, second, false);
    let (_, claims) = scheduler.due_targets(1, 8).pop().unwrap();
    assert_eq!(claims.len(), 2);
    let lease = scheduler.lease(claims, BACKOFF);

    let gate = Arc::new(Gate::default());
    let release = gate.releaser();
    storage.arm_read(GatePoint::Meta(stalled), Arc::clone(&gate));
    // The deadline is taken from this config; the streams keep the long timeout.
    let deadline = IrohRuntimeConfig {
        connect_timeout: Duration::ZERO,
        sync_io_timeout: Duration::from_secs(2),
        ..runtime()
    };
    let batch = tokio::spawn({
        let net = Arc::clone(&net);
        async move {
            net.sync_peer_batch(bob_peer, lease, deadline)
                .await
        }
    });
    tokio::task::spawn_blocking({
        let gate = Arc::clone(&gate);
        move || gate.wait_arrival()
    })
    .await
    .unwrap();
    assert!(
        !batch.is_finished(),
        "the second topic never reached the peer"
    );
    // Pulled data may leave the finished topic due again, never owned or failed.
    let first_state = scheduler.target_state(bob_peer, first);
    assert!(
        matches!(first_state, None | Some((None, 0, None))),
        "{first_state:?}"
    );
    assert!(clock(&alice, first).dominates(&clock(&bob, first)));
    assert_eq!(status(&alice, bob_peer, first).successful_attempts, 1);

    tokio::time::timeout(Duration::from_secs(60), batch)
        .await
        .expect("the batch deadline never fired")
        .unwrap();
    assert!(
        !gate.has_left(),
        "the deadline, not the peer, ended the batch"
    );
    assert_eq!(
        scheduler.target_state(bob_peer, first),
        first_state,
        "the settled topic was reinserted"
    );
    let settled = status(&alice, bob_peer, first);
    assert_eq!(
        (settled.successful_attempts, settled.failed_attempts),
        (1, 0)
    );
    let (active, failures, force) = scheduler.target_state(bob_peer, second).unwrap();
    assert_eq!((active, failures), (None, 1));
    assert!(force.is_some());
    assert_eq!(status(&alice, bob_peer, second).failed_attempts, 1);
    assert_eq!(alice.peer_health().failures(&bob_peer), 1);

    drop(release);
    net.shutdown().await;
    bob_net.shutdown().await;
}

/// One failed connection is one health failure, however many topics the batch
/// carried.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_counted_once() {
    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let down = crate::Signer::peer_id(&crate::Ed25519Signer::generate());
    for (round, topics) in [1_usize, 300].into_iter().enumerate() {
        for _ in 0..topics {
            let topic = alice
                .create_topic::<Note>(crate::TopicConfig {
                    initial_peers: [down].into(),
                    ..crate::TopicConfig::default()
                })
                .unwrap();
            net.resync_scheduler.schedule_now(down, topic.id(), false);
        }
        let (_, claims) = net
            .resync_scheduler
            .due_targets(1, MAX_RESYNC_TOPICS)
            .pop()
            .unwrap();
        assert_eq!(claims.len(), topics);
        let lease = net.resync_scheduler.lease(claims, BACKOFF);
        net.sync_peer_batch(down, lease, runtime())
            .await;
        assert_eq!(
            alice.peer_health().failures(&down),
            round as u64 + 1,
            "a batch of {topics} topics"
        );
    }
    net.shutdown().await;
}

/// A manual sync of a target the scheduler has claimed leaves that claim to
/// its owner: it neither completes nor fails the attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_keeps_claim() {
    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let (bob, bob_net) = server(
        MemoryStorage::new(),
        &lookup,
        alice.peer_id(),
        StreamLimits::default(),
    )
    .await;
    let topic_id = shared_topic(&alice, &bob);
    publish(&alice, topic_id, 3, 1);
    let bob_peer = bob.peer_id();
    net.resync_scheduler.schedule_now(bob_peer, topic_id, false);
    let (_, claims) = net
        .resync_scheduler
        .due_targets(1, 1)
        .pop()
        .unwrap();
    let attempt = claims[0].attempt;
    let lease = net.resync_scheduler.lease(claims, Duration::ZERO);

    net.sync_now(ready_addr(bob_net.endpoint()).await, topic_id)
        .await
        .unwrap();
    assert!(clock(&bob, topic_id).dominates(&clock(&alice, topic_id)));
    assert_eq!(
        net.resync_scheduler.target_state(bob_peer, topic_id),
        Some((Some(attempt), 0, None)),
        "the manual sync took over the scheduler's attempt"
    );
    drop(lease);
    let (active, _, _) = net
        .resync_scheduler
        .target_state(bob_peer, topic_id)
        .unwrap();
    assert_eq!(active, None);
    net.shutdown().await;
    bob_net.shutdown().await;
}

/// A cached receipt planned on another branch, or one claiming the whole topic
/// while the peer staged nothing, does not stall a pushed bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_receipt_restarts() {
    for other_branch in [true, false] {
        let lookup = Lookup::new();
        let (alice, net) = client(&lookup, StreamLimits::default()).await;
        let (bob, bob_net) = server(
            MemoryStorage::new(),
            &lookup,
            alice.peer_id(),
            StreamLimits::default(),
        )
        .await;
        let topic = alice.create_topic::<Note>(Default::default()).unwrap();
        publish(&alice, topic.id(), 20, 8);
        topic.add_peer(bob.peer_id()).unwrap();
        let genesis = if other_branch {
            crate::OpId::hash(b"replaced branch")
        } else {
            alice
                .storage()
                .topic_state(&topic.id())
                .unwrap()
                .unwrap()
                .genesis
        };
        net.receipt_log().record(
            bob.peer_id(),
            crate::sync::SyncReceipt {
                topic_id: topic.id(),
                genesis,
                session: 1,
                clock: clock(&alice, topic.id()),
            },
        );
        let addr = ready_addr(bob_net.endpoint()).await;
        let mut attempts = 0;
        loop {
            attempts += 1;
            assert!(
                attempts <= 8,
                "bootstrap behind a stale receipt did not finish"
            );
            match net.sync_now(addr.clone(), topic.id()).await {
                Ok(()) => break,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("bootstrap behind a stale receipt failed: {error}"),
            }
        }
        assert_eq!(
            bob.storage().list_op_ids(&topic.id()).unwrap(),
            alice.storage().list_op_ids(&topic.id()).unwrap()
        );
        net.shutdown().await;
        bob_net.shutdown().await;
    }
}

/// Staging on the peer expired and restarted while this node still holds a
/// receipt of the expired session. The push continues from what the peer's
/// own summary says it stages now, so the delayed receipt cannot skip history.
async fn assert_expired_staging<S: Storage>(storage: S) {
    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let (bob, bob_net) = server(storage, &lookup, alice.peer_id(), StreamLimits::default()).await;
    let topic = alice.create_topic::<Note>(Default::default()).unwrap();
    publish(&alice, topic.id(), 120, 8);
    topic.add_peer(bob.peer_id()).unwrap();
    let ops = crate::oplog::topological(alice.storage(), &topic.id()).unwrap();
    let receive = |ops: &[crate::Op]| match bob
        .receive_sync_outcome(
            alice.peer_id(),
            crate::sync::SyncData {
                topic_id: topic.id(),
                ops: ops.to_vec(),
            },
        )
        .unwrap()
    {
        crate::node::ReceiveOutcome::Staged(staged) => staged,
        crate::node::ReceiveOutcome::Acked { .. } => panic!("not staged"),
    };
    let expired = receive(&ops[..100]);
    let provisional = bob.storage().provisional_topics().unwrap().remove(0);
    assert!(bob.storage().discard_provisional(&provisional).unwrap());
    let restarted = receive(&ops[..10]);
    assert!(restarted.session > expired.session);
    net.receipt_log().record(
        bob.peer_id(),
        crate::sync::SyncReceipt {
            topic_id: topic.id(),
            genesis: expired.genesis.unwrap(),
            session: expired.session,
            clock: expired.clock,
        },
    );
    let addr = ready_addr(bob_net.endpoint()).await;
    let mut attempts = 0;
    loop {
        attempts += 1;
        assert!(
            attempts <= 2,
            "the push did not continue from the restarted staging"
        );
        match net.sync_now(addr.clone(), topic.id()).await {
            Ok(()) => break,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("bootstrap behind an expired staging failed: {error}"),
        }
    }
    assert_eq!(
        bob.storage().list_op_ids(&topic.id()).unwrap(),
        alice.storage().list_op_ids(&topic.id()).unwrap()
    );
    net.shutdown().await;
    bob_net.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_expired_staging() {
    assert_expired_staging(MemoryStorage::new()).await;
}

#[cfg(feature = "fjall")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fjall_expired_staging() {
    let dir = tempfile::tempdir().unwrap();
    assert_expired_staging(crate::storage::FjallStorage::open(dir.path()).unwrap()).await;
}

/// A claimed attempt is held while more attempts than a status remembers
/// complete for the same target. Its first completion still counts, without
/// replacing the newer state and error; its repeat counts nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_attempt_counts() {
    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let peer = crate::tests::support::node(90).peer_id();
    let topic_id = alice
        .create_topic::<Note>(crate::TopicConfig {
            initial_peers: [peer].into(),
            ..crate::TopicConfig::default()
        })
        .unwrap()
        .id();
    let scheduler = &net.resync_scheduler;
    scheduler.schedule_now(peer, topic_id, false);
    let (_, claims) = scheduler.due_targets(1, 8).pop().unwrap();
    let mut lease = scheduler.lease(claims, BACKOFF);
    let key = ResyncTargetKey {
        peer_id: peer,
        topic_id,
    };
    let held = lease.take_claim(&key).unwrap();
    let held_attempt = held.expect_claim().attempt;

    let later = 2 * crate::storage::MAX_RECENT_ATTEMPTS as u64 + 8;
    for index in 0..later {
        let attempt_id = next_attempt_id().unwrap();
        let live = scheduler.begin_attempts([key], attempt_id);
        let outcome = crate::AttemptOutcome::Failed(format!("later {index}"));
        let first = live.end(key);
        alice
            .record_attempt_result(
                peer,
                topic_id,
                net.attempt_identity(Some(attempt_id)),
                &outcome,
                first,
            )
            .unwrap();
    }
    let newest = status(&alice, peer, topic_id);
    assert_eq!(
        (newest.successful_attempts, newest.failed_attempts),
        (0, later)
    );

    net.record_results(peer, vec![(topic_id, Ok(()), false, Some(held))], runtime());
    let counted = status(&alice, peer, topic_id);
    assert_eq!(
        (counted.successful_attempts, counted.failed_attempts),
        (1, later)
    );
    assert_eq!(counted.state, newest.state);
    assert_eq!(counted.last_error, newest.last_error);
    assert_eq!(counted.latest_attempt, newest.latest_attempt);

    // The same attempt recorded again is no longer live and counts nothing.
    let first = scheduler.end_attempt(key, held_attempt);
    assert!(!first);
    let repeat = alice
        .record_attempt_result(
            peer,
            topic_id,
            net.attempt_identity(Some(held_attempt)),
            &crate::AttemptOutcome::Complete,
            first,
        )
        .unwrap();
    assert_eq!(repeat, counted);
    net.shutdown().await;
}

/// A batch of many behind topics plans one stream group at a time: planned
/// pages held at once stay within two stream budgets instead of every topic's
/// push page, and every topic still reaches the peer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn planning_one_group() {
    let lookup = Lookup::new();
    let limits = StreamLimits {
        bytes: 256 * 1024,
        ..StreamLimits::default()
    };
    let (alice, net) = client(&lookup, limits).await;
    let (bob, bob_net) = server(
        MemoryStorage::new(),
        &lookup,
        alice.peer_id(),
        StreamLimits::default(),
    )
    .await;
    let topics = (0..24)
        .map(|_| {
            let topic_id = shared_topic(&alice, &bob);
            publish(&alice, topic_id, 60, 1024);
            topic_id
        })
        .collect::<Vec<_>>();
    for topic_id in &topics {
        net.resync_scheduler
            .schedule_now(bob.peer_id(), *topic_id, false);
    }
    drain_due(&net, 64).await;
    for topic_id in &topics {
        assert_eq!(clock(&bob, *topic_id), clock(&alice, *topic_id));
    }
    let peak = net.planned_peak.load(Ordering::Relaxed);
    assert!(peak > 0);
    assert!(
        peak <= 2 * limits.bytes,
        "{peak} planned bytes held at once for a {} byte stream",
        limits.bytes
    );
    net.shutdown().await;
    bob_net.shutdown().await;
}

/// Data frames held by served streams stay within the net's inbound budget
/// while their handling waits for storage workers, and a control exchange for
/// another topic still completes meanwhile.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_budget_holds() {
    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let limits = StreamLimits {
        inbound_bytes: MAX_FRAME_LEN,
        ..StreamLimits::default()
    };
    let (bob, bob_net) = server(MemoryStorage::new(), &lookup, alice.peer_id(), limits).await;
    let topic_id = shared_topic(&alice, &bob);
    let other = shared_topic(&alice, &bob);
    let frame = 6 * 1024 * 1024;
    // Enough data frames to fill bob's data pool, and one slot left for control.
    let charge = budget::ByteBudget::frame_charge(frame, true);
    let notes = bob_net.budget.capacity(budget::Pool::Data) / charge + 1;
    assert!(notes < MAX_RESYNC_PEERS);
    let topic = alice.open_topic::<Note>(topic_id).unwrap();
    for index in 0..notes {
        let text = format!("{index}{}", "x".repeat(frame));
        topic.publish(Note { text }).unwrap();
    }
    let ops = crate::oplog::topological(alice.storage(), &topic_id).unwrap();
    let bob_addr = ready_addr(bob_net.endpoint()).await;

    // Every storage worker of bob is taken, so each received frame waits.
    let workers = Arc::clone(&bob_net.bulk_lane)
        .acquire_many_owned(BULK_JOBS as u32)
        .await
        .unwrap();
    let pushes = ops[1..]
        .iter()
        .map(|op| {
            let net = Arc::clone(&net);
            let addr = bob_addr.clone();
            let messages = vec![
                SyncMessage::Open(alice.sync_open(topic_id)),
                SyncMessage::Data(crate::sync::SyncData {
                    topic_id,
                    ops: vec![op.clone()],
                }),
            ];
            tokio::spawn(async move { net.sync_with(addr, &messages).await })
        })
        .collect::<Vec<_>>();
    tokio::time::timeout(Duration::from_secs(60), async {
        while bob_net.budget.available(budget::Pool::Data) >= charge {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the data budget never filled");

    let control = vec![
        SyncMessage::Open(alice.sync_open(other)),
        SyncMessage::Fingerprint(alice.sync_fingerprint(other).unwrap()),
    ];
    tokio::time::timeout(
        Duration::from_secs(60),
        net.sync_with(bob_addr.clone(), &control),
    )
    .await
    .expect("control waited behind data frames")
    .unwrap();

    let owned = bob_net.owned_bytes();
    let frames = owned.peak[&OwnedClass::Frames] as usize;
    let capacity = bob_net.budget.capacity(budget::Pool::Data)
        + bob_net.budget.capacity(budget::Pool::Control);
    assert!(
        frames <= capacity,
        "{frames} frame bytes charged for a {capacity} byte budget"
    );
    drop(workers);
    for push in pushes {
        tokio::time::timeout(Duration::from_secs(120), push)
            .await
            .expect("a push never finished")
            .unwrap()
            .unwrap();
    }
    // Out of order frames may exceed the pending quota; a sync completes the rest.
    let mut attempts = 0;
    loop {
        attempts += 1;
        assert!(
            attempts <= 8,
            "the topic did not converge after the budget freed"
        );
        match net.sync_now(bob_addr.clone(), topic_id).await {
            Ok(()) => break,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("sync after the budget freed failed: {error}"),
        }
    }
    assert_eq!(clock(&bob, topic_id), clock(&alice, topic_id));
    net.shutdown().await;
    bob_net.shutdown().await;
    assert_eq!(bob_net.owned_bytes().current.values().sum::<u64>(), 0);
    assert_eq!(
        bob_net.budget.available(budget::Pool::Data),
        bob_net.budget.capacity(budget::Pool::Data)
    );
}

/// A carol net over `path` with the key of `secret` that accepts `peers`.
#[cfg(feature = "fjall")]
async fn staging_server(
    path: &std::path::Path,
    secret: &iroh::SecretKey,
    lookup: &Lookup,
    peers: [PeerId; 2],
    limits: crate::storage::StagingLimits,
) -> (
    Irokle<crate::storage::FjallStorage>,
    Arc<IrohNet<crate::storage::FjallStorage>>,
) {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .secret_key(secret.clone())
        .address_lookup(lookup.clone())
        .alpns(vec![IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    let storage = crate::storage::FjallStorage::open(path)
        .unwrap()
        .with_staging_limits(limits);
    let node = Irokle::builder()
        .with_storage(storage)
        .with_iroh_secret_key(endpoint.secret_key())
        .with_peer_whitelist(peers)
        .build()
        .unwrap();
    let net = Arc::new(IrohNet::new(endpoint, node.clone()).unwrap());
    net.start_accept_loop().unwrap();
    lookup.add_endpoint_info(ready_addr(net.endpoint()).await);
    (node, net)
}

/// Two sources' invitation histories exceed the shared staging budget but each fits alone.
/// After restart and a late receipt, both final fragments arrive together.
/// One activation publishes the topic, the other source completes, and staging is empty.
#[cfg(feature = "fjall")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_invite_restart() {
    let lookup = Lookup::new();
    let limits = StreamLimits {
        bytes: 16 * 1024,
        ..StreamLimits::default()
    };
    let (alice, net) = client(&lookup, limits).await;
    let (dave, dave_net) = client(&lookup, limits).await;
    let secret = iroh::SecretKey::generate();
    let carol_peer = crate::Signer::peer_id(&crate::Ed25519Signer::from_iroh_secret_key(&secret));
    let topic = alice
        .create_topic::<Note>(crate::TopicConfig {
            initial_peers: [dave.peer_id()].into(),
            ..crate::TopicConfig::default()
        })
        .unwrap();
    let topic_id = topic.id();
    publish(&alice, topic_id, 8000, 32);
    topic.add_peer(carol_peer).unwrap();
    let ops = crate::oplog::topological(alice.storage(), &topic_id).unwrap();
    crate::oplog::Oplog::with_storage(dave.storage().clone())
        .receive_ops(ops.clone())
        .unwrap();
    let history = ops
        .iter()
        .map(|op| crate::storage::pending_op_bytes(op).unwrap() as u64)
        .sum::<u64>();
    let staging = crate::storage::StagingLimits {
        total_bytes: history * 8 / 5,
        source_bytes: history * 6 / 5,
        namespace_bytes: history * 6 / 5,
        ..crate::storage::StagingLimits::DISK
    };
    let sources = [alice.peer_id(), dave.peer_id()];
    let dir = tempfile::tempdir().unwrap();
    let (carol, carol_net) = staging_server(dir.path(), &secret, &lookup, sources, staging).await;
    let carol_addr = iroh::EndpointAddr::from(carol_net.endpoint().id());

    // Part of the history from each source; one call's page budget ends first.
    for source in [&net, &dave_net] {
        let result = source.sync_now(carol_addr.clone(), topic_id).await;
        assert!(
            result
                .as_ref()
                .is_err_and(|error| error.kind() == io::ErrorKind::WouldBlock),
            "{result:?}"
        );
    }
    assert!(carol.storage().topic_state(&topic_id).unwrap().is_none());
    let staged = sources.map(|source| carol.staged_topic(source, topic_id).unwrap().unwrap());
    assert!(staged.iter().all(|staged| staged.bytes > history / 4));

    carol_net.shutdown().await;
    drop((carol, carol_net));
    let (carol, carol_net) = staging_server(dir.path(), &secret, &lookup, sources, staging).await;
    for (source, before) in sources.iter().zip(&staged) {
        let after = carol.staged_topic(*source, topic_id).unwrap().unwrap();
        assert_eq!(
            (after.session, &after.clock),
            (before.session, &before.clock)
        );
    }

    // A receipt from early in the first session arrives after later progress.
    let mut early = crate::ActorClock::new();
    early.observe(crate::actor_id_for(topic_id, alice.peer_id()), 8);
    net.receipt_log().record(
        carol_peer,
        crate::sync::SyncReceipt {
            topic_id,
            genesis: ops[0].id,
            session: staged[0].session,
            clock: early,
        },
    );

    let finish = |source: Arc<IrohNet>| {
        let addr = carol_addr.clone();
        async move {
            match source.sync_now(addr, topic_id).await {
                Ok(()) => true,
                Err(error) => {
                    tracing::debug!(%error, "a final fragment was not completed");
                    false
                }
            }
        }
    };
    let mut rounds = 0;
    loop {
        rounds += 1;
        assert!(
            rounds <= 8,
            "staging stalled: active {}, staged {:?}",
            carol.storage().topic_state(&topic_id).unwrap().is_some(),
            sources.map(|source| carol.staged_topic(source, topic_id).unwrap())
        );
        let (alice_done, dave_done) =
            tokio::join!(finish(Arc::clone(&net)), finish(Arc::clone(&dave_net)));
        if alice_done && dave_done {
            break;
        }
    }
    let state = carol.storage().topic_state(&topic_id).unwrap().unwrap();
    assert!(state.members.contains(&carol_peer));
    assert_eq!(
        carol.storage().list_op_ids(&topic_id).unwrap(),
        alice.storage().list_op_ids(&topic_id).unwrap()
    );
    assert!(carol.storage().provisional_topics().unwrap().is_empty());
    net.shutdown().await;
    dave_net.shutdown().await;
    carol_net.shutdown().await;
}
