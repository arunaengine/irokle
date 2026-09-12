use std::time::{Duration, Instant};

use super::iroh::ready_addr;
use super::support::*;
use crate::net::StreamLimits;
use crate::sync::{ActorRangeHint, SyncCredit, SyncData, SyncMessage, SyncRequest};

async fn bind(transport: Option<iroh::endpoint::QuicTransportConfig>) -> iroh::Endpoint {
    let mut builder = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()]);
    if let Some(transport) = transport {
        builder = builder.transport_config(transport);
    }
    builder.bind().await.unwrap()
}

struct Peer {
    node: Irokle,
    net: Arc<net::IrohNet<MemoryStorage>>,
}

fn peer(endpoint: iroh::Endpoint, runtime: net::IrohRuntimeConfig, limits: StreamLimits) -> Peer {
    let node = Irokle::builder()
        .with_iroh_secret_key(endpoint.secret_key())
        .build()
        .unwrap();
    let net = net::IrohNet::new_with_config(endpoint, node.clone(), runtime)
        .unwrap()
        .with_stream_limits(limits);
    Peer {
        node,
        net: Arc::new(net),
    }
}

async fn serve(peer: &Peer) -> iroh::EndpointAddr {
    peer.net.start_accept_loop().unwrap();
    ready_addr(peer.net.endpoint()).await
}

/// Topic `topic_id` of `owner` with `events` notes of `text_len` bytes, shared
/// with `member`, which holds only the genesis. Returns the owner's ops.
fn seed_topic(
    owner: &Irokle,
    member: &Irokle,
    topic_id: TopicId,
    events: usize,
    text_len: usize,
) -> Vec<Op> {
    let log = oplog::Oplog::with_storage(owner.storage().clone());
    let actor = actor_id_for(topic_id, owner.peer_id());
    let genesis = TopicGenesis {
        event_type_id: Note::TYPE_ID.into(),
        initial_peers: [owner.peer_id(), member.peer_id()].into(),
        replication_policy: ReplicationPolicy::default(),
    };
    let mut ops = vec![
        log.create_topic_genesis(topic_id, actor, genesis, owner.signer())
            .unwrap(),
    ];
    for index in 0..events {
        let note = Note {
            text: format!("{index:0>text_len$}"),
        };
        let envelope = EventEnvelope::encode_event(&note).unwrap();
        ops.push(
            log.create_event_op(topic_id, actor, envelope, owner.signer())
                .unwrap(),
        );
    }
    let genesis = SyncData {
        topic_id,
        ops: ops[..1].to_vec(),
    };
    member
        .receive_sync_data_from(owner.peer_id(), genesis)
        .unwrap();
    ops
}

/// Notes `member` appends to its copy of `topic_id`.
fn member_events(member: &Irokle, topic_id: TopicId, count: usize, text_len: usize) -> Vec<Op> {
    let topic = member.open_topic::<Note>(topic_id).unwrap();
    (0..count)
        .map(|index| {
            let text = format!("{index:0>text_len$}");
            let record = topic.publish(Note { text }).unwrap();
            member
                .storage()
                .get_op(&record.meta.op_id)
                .unwrap()
                .unwrap()
        })
        .collect()
}

/// A request for every event of `owner` in `topic_id` after its genesis.
fn events_request(owner: &Irokle, topic_id: TopicId, credit: SyncCredit) -> SyncRequest {
    SyncRequest {
        topic_id,
        known: BTreeSet::new(),
        wants: BTreeSet::new(),
        actor_range_hints: vec![ActorRangeHint {
            actor_id: actor_id_for(topic_id, owner.peer_id()),
            from_exclusive: 1,
            to_inclusive: u64::MAX,
        }],
        genesis: genesis_of(owner.storage(), &topic_id),
        credit,
    }
}

fn open(member: &Irokle, topic_id: TopicId) -> SyncMessage {
    SyncMessage::Open(member.sync_open(topic_id))
}

fn push(topic_id: TopicId, ops: Vec<Op>) -> Vec<SyncMessage> {
    crate::net::sync_data_messages(topic_id, ops).unwrap()
}

fn data_ids(replies: &[SyncMessage], topic_id: TopicId) -> Vec<OpId> {
    replies
        .iter()
        .filter_map(|reply| match reply {
            SyncMessage::Data(data) if data.topic_id == topic_id => Some(data.ops.iter()),
            _ => None,
        })
        .flatten()
        .map(|op| op.id)
        .collect()
}

/// The page result of `topic_id`: `Some(more)`, or `None` when absent.
fn page_more(replies: &[SyncMessage], topic_id: TopicId) -> Option<bool> {
    let mut pages = replies.iter().filter_map(|reply| match reply {
        SyncMessage::Page(page) if page.topic_id == topic_id => Some(page.more),
        _ => None,
    });
    let more = pages.next();
    assert!(pages.next().is_none(), "one page result per topic");
    more
}

fn acked(replies: &[SyncMessage], topic_id: TopicId) -> bool {
    replies
        .iter()
        .any(|reply| matches!(reply, SyncMessage::Ack(ack) if ack.topic_id == topic_id))
}

fn topic(byte: u8) -> TopicId {
    TopicId::from_bytes([byte; 32])
}

/// Tiny QUIC receive windows on both sides, a request answered by a large page
/// and a large pushed suffix in the same stream: the exchange completes on
/// reads and writes, not on the I/O timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_windows_complete() {
    let transport = || {
        iroh::endpoint::QuicTransportConfig::builder()
            .stream_receive_window(iroh::endpoint::VarInt::from_u32(16 * 1024))
            .receive_window(iroh::endpoint::VarInt::from_u32(64 * 1024))
            .build()
    };
    let timeout = Duration::from_secs(120);
    let runtime = net::IrohRuntimeConfig {
        sync_io_timeout: timeout,
        ..net::IrohRuntimeConfig::default()
    };
    let alice = peer(
        bind(Some(transport())).await,
        runtime,
        StreamLimits::default(),
    );
    let bob = peer(
        bind(Some(transport())).await,
        runtime,
        StreamLimits::default(),
    );
    let alice_addr = serve(&alice).await;
    let topic_id = topic(11);
    let owned = seed_topic(&alice.node, &bob.node, topic_id, 600, 1024);
    let pushed = member_events(&bob.node, topic_id, 600, 1024);

    let mut messages = vec![
        open(&bob.node, topic_id),
        SyncMessage::Request(events_request(&alice.node, topic_id, SyncCredit::default())),
    ];
    messages.extend(push(topic_id, pushed.clone()));
    let started = Instant::now();
    let replies = bob.net.sync_with(alice_addr, &messages).await.unwrap();
    assert!(started.elapsed() < timeout / 4, "{:?}", started.elapsed());

    assert!(acked(&replies, topic_id));
    assert_eq!(page_more(&replies, topic_id), Some(false));
    let served = data_ids(&replies, topic_id);
    let expected = owned[1..].iter().map(|op| op.id).collect::<Vec<_>>();
    assert_eq!(served, expected);
    let stored = alice.node.storage().list_op_ids(&topic_id).unwrap();
    assert!(pushed.iter().all(|op| stored.contains(&op.id)));
    alice.net.shutdown().await;
    bob.net.shutdown().await;
}

/// Replies of many topics above one stream's scaled budget: every topic keeps
/// its ack and page result, and repeated manual syncs finish every topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_budget_continues() {
    let limits = StreamLimits {
        bytes: 96 * 1024,
        messages: 48,
        batch_messages: 24,
    };
    let runtime = net::IrohRuntimeConfig::default();
    let alice = peer(bind(None).await, runtime, limits);
    let bob = peer(bind(None).await, runtime, limits);
    let alice_addr = serve(&alice).await;
    let topics = (21..27).map(topic).collect::<Vec<_>>();
    let mut messages = Vec::new();
    for topic_id in &topics {
        seed_topic(&alice.node, &bob.node, *topic_id, 200, 256);
        messages.push(open(&bob.node, *topic_id));
        messages.extend(push(*topic_id, member_events(&bob.node, *topic_id, 1, 8)));
        messages.push(SyncMessage::Request(events_request(
            &alice.node,
            *topic_id,
            SyncCredit::default(),
        )));
    }

    let replies = bob
        .net
        .sync_with(alice_addr.clone(), &messages)
        .await
        .unwrap();
    let mut continued = 0;
    for topic_id in &topics {
        assert!(
            acked(&replies, *topic_id),
            "every pushed topic keeps its ack"
        );
        let more = page_more(&replies, *topic_id).expect("page result");
        assert!(more || !data_ids(&replies, *topic_id).is_empty());
        continued += usize::from(more);
    }
    assert!(continued > 0, "the replies must exceed the stream budget");

    for topic_id in &topics {
        let mut attempts = 0;
        loop {
            attempts += 1;
            assert!(attempts <= 8, "topic did not finish");
            match bob.net.sync_now(alice_addr.clone(), *topic_id).await {
                Ok(()) => break,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("sync failed: {error}"),
            }
        }
        assert_eq!(
            bob.node.storage().actor_clock(topic_id).unwrap(),
            alice.node.storage().actor_clock(topic_id).unwrap()
        );
    }
    alice.net.shutdown().await;
    bob.net.shutdown().await;
}

/// Hand-built streams served by `alice` for `bob`, without a connection.
async fn stream_pair(limits: StreamLimits) -> (Peer, Peer) {
    let runtime = net::IrohRuntimeConfig::default();
    let alice = peer(bind(None).await, runtime, limits);
    let bob = peer(bind(None).await, runtime, StreamLimits::default());
    (alice, bob)
}

fn serve_stream(alice: &Peer, bob: &Peer, messages: Vec<SyncMessage>) -> Vec<SyncMessage> {
    alice
        .net
        .handle_messages(bob.net.endpoint().id(), messages)
        .unwrap()
}

/// Controls that fill the message budget exactly leave no room for data, but
/// every ack and page result is still sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controls_fill_budget() {
    let topics = (31..34).map(topic).collect::<Vec<_>>();
    // Each topic replies with a summary for its open, an ack and a page result.
    let limits = StreamLimits {
        messages: 3 * topics.len(),
        ..StreamLimits::default()
    };
    let (alice, bob) = stream_pair(limits).await;
    let mut messages = Vec::new();
    for topic_id in &topics {
        seed_topic(&alice.node, &bob.node, *topic_id, 50, 8);
        messages.push(open(&bob.node, *topic_id));
        messages.extend(push(*topic_id, member_events(&bob.node, *topic_id, 1, 8)));
        messages.push(SyncMessage::Request(events_request(
            &alice.node,
            *topic_id,
            SyncCredit::default(),
        )));
    }

    let replies = serve_stream(&alice, &bob, messages);
    assert_eq!(replies.len(), limits.messages);
    for topic_id in &topics {
        assert!(acked(&replies, *topic_id));
        assert_eq!(page_more(&replies, *topic_id), Some(true));
        assert!(data_ids(&replies, *topic_id).is_empty());
    }
    alice.net.shutdown().await;
    bob.net.shutdown().await;
}

/// Repeated requests, a repeated open and a summary in one stream serve each
/// op once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicates_served_once() {
    let (alice, bob) = stream_pair(StreamLimits::default()).await;
    let topic_id = topic(41);
    let owned = seed_topic(&alice.node, &bob.node, topic_id, 40, 8);
    member_events(&bob.node, topic_id, 1, 8);
    let request =
        SyncMessage::Request(events_request(&alice.node, topic_id, SyncCredit::default()));
    let summary = SyncMessage::Summary(bob.node.sync_summary(topic_id).unwrap());
    let messages = vec![
        open(&bob.node, topic_id),
        request.clone(),
        request.clone(),
        summary.clone(),
        open(&bob.node, topic_id),
        summary,
        request,
    ];

    let replies = serve_stream(&alice, &bob, messages);
    let served = data_ids(&replies, topic_id);
    let expected = owned[1..].iter().map(|op| op.id).collect::<Vec<_>>();
    assert_eq!(served, expected);
    assert_eq!(page_more(&replies, topic_id), Some(false));
    alice.net.shutdown().await;
    bob.net.shutdown().await;
}

/// A hot topic served first cannot take the share of the quiet topics after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hot_topic_shares() {
    let limits = StreamLimits {
        bytes: 256 * 1024,
        messages: 16,
        batch_messages: 8,
    };
    let (alice, bob) = stream_pair(limits).await;
    // Requests are served in topic id order, so the hot topic goes first.
    let hot = topic(1);
    let quiet = (2..5).map(topic).collect::<Vec<_>>();
    seed_topic(&alice.node, &bob.node, hot, 2000, 64);
    let mut messages = vec![
        open(&bob.node, hot),
        SyncMessage::Request(events_request(&alice.node, hot, SyncCredit::default())),
    ];
    for topic_id in &quiet {
        seed_topic(&alice.node, &bob.node, *topic_id, 5, 64);
        messages.push(open(&bob.node, *topic_id));
        messages.push(SyncMessage::Request(events_request(
            &alice.node,
            *topic_id,
            SyncCredit::default(),
        )));
    }

    let replies = serve_stream(&alice, &bob, messages);
    assert_eq!(page_more(&replies, hot), Some(true));
    assert!(!data_ids(&replies, hot).is_empty());
    for topic_id in &quiet {
        assert_eq!(data_ids(&replies, *topic_id).len(), 5);
        assert_eq!(page_more(&replies, *topic_id), Some(false));
    }
    alice.net.shutdown().await;
    bob.net.shutdown().await;
}

/// A continuation planned against another branch fails its topic and serves
/// nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_branch_fails() {
    let (alice, bob) = stream_pair(StreamLimits::default()).await;
    let topic_id = topic(51);
    seed_topic(&alice.node, &bob.node, topic_id, 10, 8);
    let mut request = events_request(&alice.node, topic_id, SyncCredit::default());
    request.genesis = Some(OpId::hash(b"another branch"));

    let replies = serve_stream(
        &alice,
        &bob,
        vec![open(&bob.node, topic_id), SyncMessage::Request(request)],
    );
    assert!(data_ids(&replies, topic_id).is_empty());
    assert_eq!(page_more(&replies, topic_id), None);
    assert!(replies.iter().any(|reply| matches!(
        reply,
        SyncMessage::Failure(failure)
            if failure.topic_id == topic_id
                && failure.code == crate::sync::SyncFailureCode::Request
    )));
    alice.net.shutdown().await;
    bob.net.shutdown().await;
}

/// A credit far above the page limits is served within them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forged_credit_bounded() {
    let (alice, bob) = stream_pair(StreamLimits::default()).await;
    let topic_id = topic(61);
    let limit = SyncCredit::default();
    seed_topic(
        &alice.node,
        &bob.node,
        topic_id,
        limit.ops as usize + 100,
        8,
    );
    let forged = SyncCredit {
        ops: u32::MAX,
        bytes: u64::MAX,
    };

    let replies = serve_stream(
        &alice,
        &bob,
        vec![
            open(&bob.node, topic_id),
            SyncMessage::Request(events_request(&alice.node, topic_id, forged)),
        ],
    );
    let served = data_ids(&replies, topic_id).len();
    assert!(served > 0 && served <= limit.ops as usize, "{served}");
    let bytes = replies
        .iter()
        .filter(|reply| matches!(reply, SyncMessage::Data(_)))
        .map(|reply| crate::net::framed_message_len(reply).unwrap())
        .sum::<usize>();
    assert!(bytes as u64 <= limit.bytes);
    assert_eq!(page_more(&replies, topic_id), Some(true));
    alice.net.shutdown().await;
    bob.net.shutdown().await;
}

/// A reply lost with its connection certifies nothing: the obligation stays
/// until an exchange with the restarted peer really succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_reply_retransmits() {
    let runtime = net::IrohRuntimeConfig {
        connect_timeout: Duration::from_secs(2),
        sync_io_timeout: Duration::from_secs(10),
        ..net::IrohRuntimeConfig::default()
    };
    let alice = peer(bind(None).await, runtime, StreamLimits::default());
    let bob_endpoint = bind(None).await;
    let bob_key = bob_endpoint.secret_key().clone();
    let bob_peer = PeerId::from_bytes(*bob_endpoint.id().as_bytes());
    let bob = Irokle::builder()
        .with_iroh_secret_key(&bob_key)
        .with_peer_whitelist([alice.node.peer_id()])
        .build()
        .unwrap();
    let topic = alice
        .node
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob_peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let record = topic
        .publish(Note {
            text: "owed".into(),
        })
        .unwrap();
    alice
        .node
        .put_sync_obligation(bob_peer, topic.id(), [record.meta.op_id].into())
        .unwrap();
    let owed = || {
        alice
            .node
            .storage()
            .has_sync_obligations(&bob_peer, &topic.id())
            .unwrap()
    };

    // Bob answers the fingerprint stream, reads the data stream to its end and
    // then drops the connection and the endpoint without replying.
    let bob_net =
        Arc::new(net::IrohNet::new_with_config(bob_endpoint, bob.clone(), runtime).unwrap());
    let bob_addr = ready_addr(bob_net.endpoint()).await;
    let lossy = Arc::clone(&bob_net);
    let responder = tokio::spawn(async move {
        let incoming = lossy.endpoint().accept().await.unwrap();
        let connection = incoming.await.unwrap();
        let (send, recv) = connection.accept_bi().await.unwrap();
        lossy
            .handle_stream(connection.remote_id(), recv, send)
            .await
            .unwrap();
        let (_send, mut recv) = connection.accept_bi().await.unwrap();
        recv.read_to_end(64 * 1024 * 1024).await.unwrap();
        connection.close(0u32.into(), b"reply lost");
        lossy.shutdown().await;
    });
    assert!(alice.net.sync_now(bob_addr, topic.id()).await.is_err());
    responder.await.unwrap();
    assert!(owed());
    assert!(
        alice
            .node
            .storage()
            .peer_ack(&bob_peer, &topic.id())
            .unwrap()
            .is_none()
    );

    let restarted = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
        .secret_key(bob_key)
        .bind()
        .await
        .unwrap();
    let bob_net = Arc::new(net::IrohNet::new_with_config(restarted, bob.clone(), runtime).unwrap());
    bob_net.start_accept_loop().unwrap();
    let bob_addr = ready_addr(bob_net.endpoint()).await;
    alice.net.sync_now(bob_addr, topic.id()).await.unwrap();
    assert!(!owed());
    let ack = alice
        .node
        .storage()
        .peer_ack(&bob_peer, &topic.id())
        .unwrap()
        .expect("certified ack after the successful exchange");
    assert_eq!(ack.genesis, genesis_of(alice.node.storage(), &topic.id()));
    assert_eq!(
        bob.storage().list_op_ids(&topic.id()).unwrap(),
        alice.node.storage().list_op_ids(&topic.id()).unwrap()
    );
    alice.net.shutdown().await;
    bob_net.shutdown().await;
}

/// A peer offering only the previous protocol cannot open a sync connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn old_protocol_refused() {
    assert_eq!(crate::sync::SYNC_PROTOCOL, "irokle/sync/3");
    assert_eq!(crate::net::IROKLE_SYNC_ALPN, b"irokle/sync/3");
    let alice = peer(
        bind(None).await,
        net::IrohRuntimeConfig::default(),
        StreamLimits::default(),
    );
    let alice_addr = serve(&alice).await;
    let old = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .alpns(vec![b"irokle/sync/2".to_vec()])
        .bind()
        .await
        .unwrap();

    let connected = tokio::time::timeout(
        Duration::from_secs(30),
        old.connect(alice_addr, b"irokle/sync/2"),
    )
    .await
    .expect("the handshake ends instead of hanging");
    assert!(connected.is_err());
    old.close().await;
    alice.net.shutdown().await;
}
