//! Bytes a net owns while it works stay charged until their data is gone, and
//! every public entry point is admitted through the same slots and pools.

use super::scheduler_tests::{Lookup, client, publish, ready_addr, server, shared_topic};
use super::*;
use crate::TopicId;
use crate::net::frame::MAX_FRAME_LEN;
use crate::sync::{SyncAck, SyncCredit, SyncData, SyncRequest};
use crate::tests::support::{Gate, GatePoint, StaleReadStorage, clock_summary, genesis_of};

/// Every pool is back at capacity and no class or job is still counted.
fn assert_released<S: Storage>(net: &IrohNet<S>) {
    for pool in [Pool::Data, Pool::Control, Pool::Session, Pool::Results] {
        assert_eq!(
            net.budget.available(pool),
            net.budget.capacity(pool),
            "{pool:?}"
        );
    }
    let owned = net.owned_bytes();
    assert_eq!(owned.current.values().sum::<u64>(), 0, "{owned:?}");
    assert_eq!(owned.jobs, 0);
}

/// An open and a fingerprint per topic.
fn probe(node: &Irokle, topics: &[TopicId]) -> Vec<SyncMessage> {
    topics
        .iter()
        .flat_map(|topic_id| {
            [
                SyncMessage::Open(node.sync_open(*topic_id)),
                SyncMessage::Fingerprint(node.sync_fingerprint(*topic_id).unwrap()),
            ]
        })
        .collect()
}

fn framed_len(message: &SyncMessage) -> usize {
    crate::net::framed_message_len(message).unwrap()
}

async fn owned_settle<S: Storage>(net: &IrohNet<S>, class: OwnedClass, bytes: u64) {
    tokio::time::timeout(Duration::from_secs(60), async {
        while net.owned_bytes().current.get(&class).copied().unwrap_or(0) != bytes {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{class:?} never settled at {bytes} bytes"));
}

/// On a one-worker runtime, a served stream dropped while its storage job is
/// held keeps the data frame charged until that job really ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn cancelled_frame_charged() {
    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let storage = StaleReadStorage::new(MemoryStorage::new());
    let limits = StreamLimits::default();
    let (bob, bob_net) = server(storage.clone(), &lookup, alice.peer_id(), limits).await;
    let topic_id = shared_topic(&alice, &bob);
    publish(&alice, topic_id, 1, 1024);
    let op = crate::oplog::topological(alice.storage(), &topic_id)
        .unwrap()
        .pop()
        .unwrap();
    let gate = Arc::new(Gate::default());
    let release = gate.releaser();
    storage.arm_read(GatePoint::Meta(op.id), Arc::clone(&gate));
    let messages = vec![
        SyncMessage::Open(alice.sync_open(topic_id)),
        SyncMessage::Data(SyncData {
            topic_id,
            ops: vec![op],
        }),
    ];
    let bob_addr = ready_addr(bob_net.endpoint()).await;
    let push = tokio::spawn({
        let net = Arc::clone(&net);
        async move { net.sync_with(bob_addr, &messages).await.map(drop) }
    });
    let arrival = Arc::clone(&gate);
    tokio::task::spawn_blocking(move || arrival.wait_arrival())
        .await
        .unwrap();
    assert!(gate.arrived());
    let charged = bob_net.owned_bytes().current[&OwnedClass::Frames];
    assert!(charged > 0);

    // Shutdown drops the served stream, but not the job it started.
    let outcome = bob_net
        .shutdown_with_timeout(Duration::from_millis(200))
        .await;
    assert!(
        matches!(outcome, ShutdownOutcome::Incomplete { .. }),
        "{outcome:?}"
    );
    tokio::time::timeout(Duration::from_secs(60), async {
        while bob_net.tasks.running() > 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the served stream was never dropped");
    assert_eq!(bob_net.owned_bytes().current[&OwnedClass::Frames], charged);
    assert!(bob_net.budget.available(Pool::Data) < bob_net.budget.capacity(Pool::Data));

    drop(release);
    assert_eq!(
        bob_net.shutdown_with_timeout(Duration::from_secs(60)).await,
        ShutdownOutcome::Complete
    );
    assert_released(&bob_net);
    let _ = push.await.unwrap();
    net.shutdown().await;
    assert_released(&net);
}

/// Responses a caller keeps stay charged while it makes more calls. A later
/// frame that finds the result pool full fails the exchange instead of
/// waiting, and dropping the responses releases every byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_results_charged() {
    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let limits = StreamLimits::default();
    let (bob, bob_net) = server(MemoryStorage::new(), &lookup, alice.peer_id(), limits).await;
    let topics = (0..4)
        .map(|_| {
            let topic_id = shared_topic(&bob, &alice);
            publish(&bob, topic_id, 4, 256);
            topic_id
        })
        .collect::<Vec<_>>();
    let bob_addr = ready_addr(bob_net.endpoint()).await;
    let messages = probe(&alice, &topics);

    let held = net.sync_with(bob_addr.clone(), &messages).await.unwrap();
    assert!(held.len() >= 2, "{} responses", held.len());
    let decoded = held
        .iter()
        .map(|message| crate::net::decoded_message_bound(message).unwrap() as u64)
        .sum::<u64>();
    assert_eq!(net.owned_bytes().current[&OwnedClass::Results], decoded);
    let more = net.sync_with(bob_addr.clone(), &messages).await.unwrap();
    assert_eq!(net.owned_bytes().current[&OwnedClass::Results], 2 * decoded);
    drop(more);
    assert_eq!(net.owned_bytes().current[&OwnedClass::Results], decoded);

    // Room for the first frame only: the second one cannot be charged.
    let first = ByteBudget::frame_charge(framed_len(&held.messages()[0]) - 4, false);
    let free = net.budget.available(Pool::Results);
    let filler = net
        .budget
        .try_take(Pool::Results, free - first, OwnedClass::Results)
        .unwrap();
    let error = net
        .sync_with(bob_addr.clone(), &messages)
        .await
        .map(drop)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::OutOfMemory, "{error}");
    assert_eq!(
        net.owned_bytes().current[&OwnedClass::Results],
        decoded + (free - first) as u64
    );
    drop((filler, held));
    net.shutdown().await;
    bob_net.shutdown().await;
    assert_released(&net);
    assert_released(&bob_net);
}

/// Requests and acks a served stream keeps until it replies are charged to
/// the session pool. When that pool is full the stream fails without waiting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_bytes_charged() {
    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let limits = StreamLimits::default();
    let (bob, bob_net) = server(MemoryStorage::new(), &lookup, alice.peer_id(), limits).await;
    let topic_id = shared_topic(&bob, &alice);
    publish(&bob, topic_id, 8, 256);
    let genesis = genesis_of(alice.storage(), &topic_id);
    let request = SyncMessage::Request(SyncRequest {
        topic_id,
        known: BTreeSet::new(),
        wants: BTreeSet::new(),
        actor_range_hints: Vec::new(),
        genesis,
        credit: SyncCredit::default(),
        window: crate::sync::ActorWindow::default(),
    });
    let mut ack = SyncAck {
        topic_id,
        peer_id: alice.peer_id(),
        genesis,
        accepted: BTreeSet::new(),
        heads: alice.storage().heads(&topic_id).unwrap(),
        clock: alice.storage().actor_clock(&topic_id).unwrap(),
        signature: None,
    };
    ack.sign(alice.signer()).unwrap();
    let mut messages = vec![SyncMessage::Open(alice.sync_open(topic_id))];
    for _ in 0..4 {
        messages.push(request.clone());
        messages.push(SyncMessage::Ack(ack.clone()));
    }
    let retained = messages[1..]
        .iter()
        .map(|message| crate::net::decoded_message_bound(message).unwrap())
        .sum::<usize>();
    let bob_addr = ready_addr(bob_net.endpoint()).await;

    let replies = net.sync_with(bob_addr.clone(), &messages).await.unwrap();
    assert!(
        replies
            .iter()
            .any(|reply| matches!(reply, SyncMessage::Page(_)))
    );
    drop(replies);
    owned_settle(&bob_net, OwnedClass::Session, 0).await;
    let peak = bob_net.owned_bytes().peak[&OwnedClass::Session];
    assert!(
        peak >= retained as u64,
        "{peak} of {retained} bytes charged"
    );

    let free = bob_net.budget.available(Pool::Session);
    let filler = bob_net
        .budget
        .try_take(Pool::Session, free - retained / 2, OwnedClass::Session)
        .unwrap();
    if let Ok(replies) = net.sync_with(bob_addr.clone(), &messages).await {
        assert!(
            !replies
                .iter()
                .any(|reply| matches!(reply, SyncMessage::Page(_))),
            "a stream over the session budget was served"
        );
    }
    owned_settle(&bob_net, OwnedClass::Session, filler.bytes() as u64).await;
    drop(filler);
    net.shutdown().await;
    bob_net.shutdown().await;
    assert_released(&net);
    assert_released(&bob_net);
}

/// With data, result and worker capacity taken, a small control exchange
/// still completes, and shutdown ends while waiting charges cannot be granted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturated_shutdown() {
    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let limits = StreamLimits::default();
    let (bob, bob_net) = server(MemoryStorage::new(), &lookup, alice.peer_id(), limits).await;
    let pushed = shared_topic(&alice, &bob);
    publish(&alice, pushed, 1, 1024);
    let other = shared_topic(&alice, &bob);
    let op = crate::oplog::topological(alice.storage(), &pushed)
        .unwrap()
        .pop()
        .unwrap();
    let op_id = op.id;
    let bob_addr = ready_addr(bob_net.endpoint()).await;

    let capacity = bob_net.budget.capacity(Pool::Data);
    let data = bob_net
        .budget
        .try_take(Pool::Data, capacity, OwnedClass::Frames)
        .unwrap();
    let workers = Arc::clone(&bob_net.bulk_lane)
        .acquire_many_owned(BULK_JOBS as u32)
        .await
        .unwrap();
    let push = tokio::spawn({
        let net = Arc::clone(&net);
        let addr = bob_addr.clone();
        let messages = vec![
            SyncMessage::Open(alice.sync_open(pushed)),
            SyncMessage::Data(SyncData {
                topic_id: pushed,
                ops: vec![op],
            }),
        ];
        async move { net.sync_with(addr, &messages).await.map(drop) }
    });
    tokio::time::timeout(Duration::from_secs(60), async {
        while bob_net.served.available_permits() == MAX_SERVED_STREAMS {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the push never reached bob");

    let control = tokio::time::timeout(
        Duration::from_secs(60),
        net.sync_with(bob_addr.clone(), &probe(&alice, &[other])),
    )
    .await
    .expect("control waited behind full data capacity")
    .unwrap();
    assert!(!control.is_empty());
    drop(control);

    let results = net
        .budget
        .try_take(
            Pool::Results,
            net.budget.capacity(Pool::Results),
            OwnedClass::Results,
        )
        .unwrap();
    let waiting = tokio::spawn({
        let net = Arc::clone(&net);
        let messages = probe(&alice, &[other]);
        async move { net.sync_with(bob_addr, &messages).await.map(drop) }
    });
    tokio::time::timeout(Duration::from_secs(60), async {
        while net.outbound.available_permits() > MAX_RESYNC_PEERS - 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the second exchange never started");

    assert_eq!(
        net.shutdown_with_timeout(Duration::from_secs(60)).await,
        ShutdownOutcome::Complete
    );
    assert!(waiting.await.unwrap().is_err());
    assert_eq!(
        bob_net.shutdown_with_timeout(Duration::from_secs(60)).await,
        ShutdownOutcome::Complete
    );
    let _ = push.await.unwrap();
    drop((workers, data, results));
    assert_released(&net);
    assert_released(&bob_net);
    assert!(bob.storage().get_op(&op_id).unwrap().is_none());
}

/// Direct exchanges share the outbound slots, served and embedded streams
/// share the served slots, and nothing stays held afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn entry_points_admitted() {
    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let limits = StreamLimits::default();
    let (bob, bob_net) = server(MemoryStorage::new(), &lookup, alice.peer_id(), limits).await;
    let topic_id = shared_topic(&alice, &bob);
    let bob_addr = ready_addr(bob_net.endpoint()).await;
    let messages = probe(&alice, &[topic_id]);

    // Bob's control workers are taken, so every stream reaching bob stays open.
    let workers = Arc::clone(&bob_net.control_lane)
        .acquire_many_owned(CONTROL_JOBS as u32)
        .await
        .unwrap();
    let calls = (0..3 * MAX_RESYNC_PEERS)
        .map(|_| {
            let net = Arc::clone(&net);
            let addr = bob_addr.clone();
            let messages = messages.clone();
            tokio::spawn(async move { net.sync_with(addr, &messages).await.map(drop) })
        })
        .collect::<Vec<_>>();
    tokio::time::timeout(Duration::from_secs(60), async {
        while MAX_SERVED_STREAMS - bob_net.served.available_permits() < MAX_RESYNC_PEERS
            || net.outbound_sync_streams() < MAX_RESYNC_PEERS as u64
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the streams never reached bob");
    assert_eq!(net.outbound.available_permits(), 0);
    assert_eq!(net.outbound_sync_streams(), MAX_RESYNC_PEERS as u64);

    // An embedder's stream is refused, not queued, while served slots are full.
    let free = bob_net.served.available_permits() as u32;
    let served = Arc::clone(&bob_net.served)
        .try_acquire_many_owned(free)
        .unwrap();
    let alice_id = net.endpoint().id();
    let refused = bob_net
        .handle_messages(alice_id, messages.clone())
        .map(drop)
        .unwrap_err();
    assert_eq!(refused.kind(), io::ErrorKind::WouldBlock);
    drop((served, workers));

    for call in calls {
        tokio::time::timeout(Duration::from_secs(60), call)
            .await
            .expect("an exchange never finished")
            .unwrap()
            .unwrap();
    }
    assert_eq!(net.outbound_sync_streams(), 3 * MAX_RESYNC_PEERS as u64);
    let replies = bob_net.handle_messages(alice_id, messages).unwrap();
    assert!(!replies.is_empty());
    drop(replies);
    net.shutdown().await;
    bob_net.shutdown().await;
    assert_eq!(bob_net.served.available_permits(), MAX_SERVED_STREAMS);
    assert_eq!(net.outbound.available_permits(), MAX_RESYNC_PEERS);
    assert_released(&net);
    assert_released(&bob_net);
}

/// All pools and worker lanes are full; a started storage job loses its requester.
/// Shutdown waits for that job, preserving its charge until it ends.
/// Every pool, lane, and slot is whole afterward.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_budgets_shutdown() {
    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let storage = StaleReadStorage::new(MemoryStorage::new());
    let limits = StreamLimits::default();
    let (bob, bob_net) = server(storage.clone(), &lookup, alice.peer_id(), limits).await;
    let topic_id = shared_topic(&alice, &bob);
    publish(&alice, topic_id, 1, 1024);
    let op = crate::oplog::topological(alice.storage(), &topic_id)
        .unwrap()
        .pop()
        .unwrap();
    let gate = Arc::new(Gate::default());
    let release = gate.releaser();
    storage.arm_read(GatePoint::Meta(op.id), Arc::clone(&gate));
    let bob_addr = ready_addr(bob_net.endpoint()).await;
    let messages = vec![
        SyncMessage::Open(alice.sync_open(topic_id)),
        SyncMessage::Data(SyncData {
            topic_id,
            ops: vec![op],
        }),
    ];
    let push = tokio::spawn({
        let net = Arc::clone(&net);
        async move { net.sync_with(bob_addr, &messages).await.map(drop) }
    });
    let arrival = Arc::clone(&gate);
    tokio::task::spawn_blocking(move || arrival.wait_arrival())
        .await
        .unwrap();
    let charged = bob_net.owned_bytes().current[&OwnedClass::Frames];

    let fill = |pool| {
        let free = bob_net.budget.available(pool);
        bob_net
            .budget
            .try_take(pool, free, OwnedClass::Output)
            .unwrap()
    };
    let pools = [Pool::Data, Pool::Control, Pool::Session, Pool::Results].map(fill);
    let lanes = [&bob_net.control_lane, &bob_net.bulk_lane].map(|lane| {
        let free = lane.available_permits() as u32;
        Arc::clone(lane).try_acquire_many_owned(free).unwrap()
    });
    assert_eq!(bob_net.bulk_lane.available_permits(), 0);

    // The requester goes away while its job is held.
    push.abort();
    assert!(push.await.unwrap_err().is_cancelled());
    let outcome = bob_net
        .shutdown_with_timeout(Duration::from_millis(200))
        .await;
    assert!(
        matches!(outcome, ShutdownOutcome::Incomplete { .. }),
        "{outcome:?}"
    );
    tokio::time::timeout(Duration::from_secs(60), async {
        while bob_net.tasks.running() > 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("only the held job should still run");
    assert_eq!(bob_net.owned_bytes().current[&OwnedClass::Frames], charged);

    drop(release);
    assert_eq!(
        bob_net.shutdown_with_timeout(Duration::from_secs(60)).await,
        ShutdownOutcome::Complete
    );
    assert_eq!(bob_net.owned_bytes().current[&OwnedClass::Frames], 0);
    drop((pools, lanes));
    assert_eq!(bob_net.control_lane.available_permits(), CONTROL_JOBS);
    assert_eq!(bob_net.bulk_lane.available_permits(), BULK_JOBS);
    assert_eq!(bob_net.served.available_permits(), MAX_SERVED_STREAMS);
    assert_released(&bob_net);
    net.shutdown().await;
    assert_released(&net);
    assert_eq!(
        bob.storage().list_op_ids(&topic_id).unwrap(),
        alice.storage().list_op_ids(&topic_id).unwrap(),
        "the job committed after its requester left"
    );
}

/// A durable activation is paused after staging while all pools and lanes are full.
/// Shutdown waits for it; activation then publishes the topic and leaves no staging.
/// A reopen sees the complete topic.
#[cfg(feature = "fjall")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturated_activation_reopen() {
    use crate::storage::{FjallStorage, Hook};

    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(dir.path()).unwrap();
    let limits = StreamLimits::default();
    let (bob, bob_net) = server(storage.clone(), &lookup, alice.peer_id(), limits).await;
    let topic = alice
        .create_topic::<crate::tests::support::Note>(crate::TopicConfig::default())
        .unwrap();
    publish(&alice, topic.id(), 20, 64);
    topic.add_peer(bob.peer_id()).unwrap();
    let topic_id = topic.id();
    let ops = crate::oplog::topological(alice.storage(), &topic_id).unwrap();
    let gate = Arc::new(Gate::default());
    let release = gate.releaser();
    storage.set_hook({
        let gate = Arc::clone(&gate);
        let first = std::sync::atomic::AtomicBool::new(true);
        move |at| {
            if at == Hook::Publish && first.swap(false, Ordering::SeqCst) {
                gate.pass();
            }
            Ok(())
        }
    });
    let bob_addr = ready_addr(bob_net.endpoint()).await;
    let messages = vec![
        SyncMessage::Open(alice.sync_open(topic_id)),
        SyncMessage::Data(SyncData { topic_id, ops }),
    ];
    let push = tokio::spawn({
        let net = Arc::clone(&net);
        async move { net.sync_with(bob_addr, &messages).await.map(drop) }
    });
    let arrival = Arc::clone(&gate);
    tokio::task::spawn_blocking(move || arrival.wait_arrival())
        .await
        .unwrap();
    assert!(bob.storage().topic_state(&topic_id).unwrap().is_none());

    let fill = |pool| {
        let free = bob_net.budget.available(pool);
        bob_net
            .budget
            .try_take(pool, free, OwnedClass::Output)
            .unwrap()
    };
    let pools = [Pool::Data, Pool::Control, Pool::Session, Pool::Results].map(fill);
    let lanes = [&bob_net.control_lane, &bob_net.bulk_lane].map(|lane| {
        let free = lane.available_permits() as u32;
        Arc::clone(lane).try_acquire_many_owned(free).unwrap()
    });
    push.abort();
    assert!(push.await.unwrap_err().is_cancelled());
    let outcome = bob_net
        .shutdown_with_timeout(Duration::from_millis(200))
        .await;
    assert!(
        matches!(outcome, ShutdownOutcome::Incomplete { .. }),
        "{outcome:?}"
    );

    drop(release);
    assert_eq!(
        bob_net.shutdown_with_timeout(Duration::from_secs(60)).await,
        ShutdownOutcome::Complete
    );
    drop((pools, lanes));
    assert_eq!(bob_net.control_lane.available_permits(), CONTROL_JOBS);
    assert_eq!(bob_net.bulk_lane.available_permits(), BULK_JOBS);
    assert_eq!(bob_net.served.available_permits(), MAX_SERVED_STREAMS);
    assert_released(&bob_net);
    net.shutdown().await;
    assert_released(&net);
    let expected = alice.storage().list_op_ids(&topic_id).unwrap();
    assert_eq!(bob.storage().list_op_ids(&topic_id).unwrap(), expected);

    drop((bob, bob_net, storage));
    let reopened = FjallStorage::open(dir.path()).unwrap();
    assert!(reopened.topic_state(&topic_id).unwrap().is_some());
    assert_eq!(reopened.list_op_ids(&topic_id).unwrap(), expected);
    assert!(reopened.provisional_topics().unwrap().is_empty());
}

/// An open whose event type pads its frame to exactly `len` payload bytes.
fn padded_open(node: &Irokle, topic_id: TopicId, len: usize) -> SyncMessage {
    let mut open = node.sync_open(topic_id);
    open.event_type_id = Some(String::new());
    let empty = postcard::experimental::serialized_size(&SyncMessage::Open(open.clone())).unwrap();
    // The string length grows from one to four varint bytes.
    open.event_type_id = Some("x".repeat(len - empty - 3));
    let open = SyncMessage::Open(open);
    assert_eq!(postcard::experimental::serialized_size(&open).unwrap(), len);
    open
}

/// The largest legal control frames reach an idle default responder, one
/// padded to the wire maximum and one carrying the largest clock. A frame one
/// byte longer is refused as invalid before anything is charged or sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn largest_frames_served() {
    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let limits = StreamLimits::default();
    let (bob, bob_net) = server(MemoryStorage::new(), &lookup, alice.peer_id(), limits).await;
    let topic_id = shared_topic(&bob, &alice);
    let bob_addr = ready_addr(bob_net.endpoint()).await;

    let open = padded_open(&alice, topic_id, MAX_FRAME_LEN);
    let replies = net.sync_with(bob_addr.clone(), &[open]).await.unwrap();
    assert!(replies.iter().any(|reply| matches!(
        reply,
        SyncMessage::Summary(summary) if summary.topic_id == topic_id
    )));
    drop(replies);

    // The session keeps the whole clock until it replies.
    let genesis = genesis_of(bob.storage(), &topic_id);
    let summary = clock_summary(topic_id, genesis, MAX_FRAME_LEN);
    let held = crate::net::decoded_message_bound(&summary).unwrap() as u64;
    let messages = [SyncMessage::Open(alice.sync_open(topic_id)), summary];
    drop(net.sync_with(bob_addr.clone(), &messages).await.unwrap());
    assert!(bob_net.owned_bytes().peak[&OwnedClass::Session] >= held);

    let open = padded_open(&alice, topic_id, MAX_FRAME_LEN + 1);
    let error = net
        .sync_with(bob_addr, &[open])
        .await
        .map(drop)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    net.shutdown().await;
    bob_net.shutdown().await;
    assert_released(&net);
    assert_released(&bob_net);
}

/// A reply of the largest legal control frame is read by an idle default
/// requester, which keeps it charged until the reply is dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn largest_reply_read() {
    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let (_, answering) = client(&lookup, StreamLimits::default()).await;
    let answering_addr = ready_addr(answering.endpoint()).await;
    lookup.add_endpoint_info(answering_addr.clone());
    let topic_id = TopicId::hash(b"largest reply");
    let reply = clock_summary(topic_id, None, MAX_FRAME_LEN);
    let answer = tokio::spawn({
        let endpoint = answering.endpoint().clone();
        let reply = reply.clone();
        async move {
            let connection = endpoint.accept().await.unwrap().await.unwrap();
            let (mut send, mut recv) = connection.accept_bi().await.unwrap();
            recv.read_to_end(MAX_FRAME_LEN).await.unwrap();
            let timeout = Duration::from_secs(120);
            let limits = StreamLimits::default();
            write_sync_messages(&mut send, &[reply], timeout, limits, None)
                .await
                .unwrap();
            connection.closed().await;
        }
    });

    let open = SyncMessage::Open(alice.sync_open(topic_id));
    let replies = net.sync_with(answering_addr, &[open]).await.unwrap();
    assert_eq!(replies.messages(), [reply]);
    let held = crate::net::decoded_message_bound(&replies.messages()[0]).unwrap();
    assert_eq!(net.owned_bytes().current[&OwnedClass::Results], held as u64);
    drop(replies);
    net.shutdown().await;
    answer.await.unwrap();
    answering.shutdown().await;
    assert_released(&net);
}

/// Two requesters send the largest frame at once. The responder charges one
/// at a time, never more than its pool, and serves both.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_frames_bounded() {
    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let limits = StreamLimits::default();
    let (bob, bob_net) = server(MemoryStorage::new(), &lookup, alice.peer_id(), limits).await;
    let topic_id = shared_topic(&bob, &alice);
    let bob_addr = ready_addr(bob_net.endpoint()).await;
    let open = padded_open(&alice, topic_id, MAX_FRAME_LEN);
    let calls = (0..2)
        .map(|_| {
            let net = Arc::clone(&net);
            let addr = bob_addr.clone();
            let open = open.clone();
            tokio::spawn(async move { net.sync_with(addr, &[open]).await.map(drop) })
        })
        .collect::<Vec<_>>();
    for call in calls {
        call.await.unwrap().unwrap();
    }
    let peak = bob_net.owned_bytes().peak[&OwnedClass::Frames];
    let charge = ByteBudget::frame_charge(MAX_FRAME_LEN, false) as u64;
    assert!(peak >= charge);
    assert!(peak <= bob_net.budget.capacity(Pool::Data) as u64);
    net.shutdown().await;
    bob_net.shutdown().await;
    assert_released(&net);
    assert_released(&bob_net);
}

/// Frames refused while they are read, a body that claims more entries than
/// it holds and a length above the wire maximum, give back every charged byte,
/// and the responder goes on serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_frames_released() {
    let lookup = Lookup::new();
    let (alice, net) = client(&lookup, StreamLimits::default()).await;
    let limits = StreamLimits::default();
    let (bob, bob_net) = server(MemoryStorage::new(), &lookup, alice.peer_id(), limits).await;
    let bob_addr = ready_addr(bob_net.endpoint()).await;
    let connection = net
        .endpoint()
        .connect(bob_addr.clone(), IROKLE_SYNC_ALPN)
        .await
        .unwrap();
    let malformed = |len: usize| {
        let mut frame = (len as u32).to_be_bytes().to_vec();
        // A summary: tag, topic, no event type, no genesis, fingerprint, heads.
        frame.push(2);
        frame.extend([7; 32]);
        frame.extend([0, 0]);
        frame.extend([0; 32]);
        frame.extend(postcard::to_allocvec(&(u32::MAX as usize)).unwrap());
        frame.resize(4 + len, 0);
        frame
    };
    let too_long = (MAX_FRAME_LEN as u32 + 1).to_be_bytes().to_vec();
    for frame in [malformed(1024 * 1024), malformed(MAX_FRAME_LEN), too_long] {
        let (mut send, mut recv) = connection.open_bi().await.unwrap();
        // The responder may stop reading a refused frame before all of it is sent.
        if send.write_all(&frame).await.is_ok() {
            let _ = send.finish();
        }
        if let Ok(reply) = recv.read_to_end(1024).await {
            assert!(reply.is_empty());
        }
    }
    tokio::time::timeout(Duration::from_secs(60), async {
        while bob_net.served.available_permits() < MAX_SERVED_STREAMS
            || bob_net.owned_bytes().current.values().sum::<u64>() > 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("refused streams kept their charge");
    assert_released(&bob_net);

    let topic_id = shared_topic(&alice, &bob);
    let replies = net.sync_with(bob_addr, &probe(&alice, &[topic_id])).await;
    assert!(!replies.unwrap().is_empty());
    net.shutdown().await;
    bob_net.shutdown().await;
    assert_released(&net);
    assert_released(&bob_net);
}
