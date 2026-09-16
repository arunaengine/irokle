//! Transport cost measurements over real iroh endpoints. Run explicitly, one
//! test per process so peak RSS is not shared:
//! `cargo test --features fjall,iroh --lib tests::bench_net::<name> -- --exact --ignored --nocapture`.

use std::path::Path;
use std::time::{Duration, Instant};

use iroh::address_lookup::memory::MemoryLookup;

use super::support::*;
use crate::IrokleBuilder;
use crate::storage::FjallStorage;
use crate::sync::SyncData;

const REPS: usize = 3;
/// Safety cap for one run. A run that reaches it is reported as incomplete.
const CAP: Duration = Duration::from_secs(120);

/// A backend the workloads build nodes on.
trait Store: Storage + Sized {
    const NAME: &'static str;
    fn builder(dir: &Path, name: &str) -> IrokleBuilder<Self>;
    /// Storage transaction attempts, where the revision counts them.
    fn attempts(&self) -> Option<u64>;
}

impl Store for MemoryStorage {
    const NAME: &'static str = "memory";
    fn builder(_: &Path, _: &str) -> IrokleBuilder<Self> {
        Irokle::builder()
    }
    fn attempts(&self) -> Option<u64> {
        Some(self.counters().transaction_attempts)
    }
}

impl Store for FjallStorage {
    const NAME: &'static str = "fjall";
    fn builder(dir: &Path, name: &str) -> IrokleBuilder<Self> {
        Irokle::builder().with_fjall_path(dir.join(name)).unwrap()
    }
    fn attempts(&self) -> Option<u64> {
        Some(self.counters().transaction_attempts)
    }
}

struct Run {
    ms: f64,
    done: bool,
    values: Vec<(&'static str, u64)>,
}

/// One line per workload: elapsed and every value as median and max.
fn report(name: &str, params: &str, runs: Vec<Run>) {
    for (rep, run) in runs.iter().enumerate() {
        eprintln!(
            "bench_sample name={name} {params} rep={rep} ms={:.6} completed={}",
            run.ms, run.done
        );
    }
    let sorted = |mut values: Vec<f64>| {
        values.sort_by(f64::total_cmp);
        (values[values.len() / 2], values[values.len() - 1])
    };
    let (median, max) = sorted(runs.iter().map(|run| run.ms).collect());
    let done = runs.iter().filter(|run| run.done).count();
    let mut line = format!(
        "bench name={name} {params} reps={} completed={done} median_ms={median:.1} max_ms={max:.1}",
        runs.len()
    );
    for (index, (key, _)) in runs[0].values.iter().enumerate() {
        let values = runs.iter().map(|run| run.values[index].1 as f64).collect();
        let (median, max) = sorted(values);
        line.push_str(&format!(" {key}={median} {key}_max={max}"));
    }
    eprintln!("{line}");
}

fn millis(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

fn runtime() -> net::IrohRuntimeConfig {
    net::IrohRuntimeConfig {
        connect_timeout: Duration::from_secs(2),
        sync_io_timeout: Duration::from_secs(30),
        resync_interval: Duration::from_millis(50),
        resync_initial_backoff: Duration::from_millis(10),
        resync_max_backoff: Duration::from_millis(100),
        full_sweep_interval: Duration::ZERO,
        ..net::IrohRuntimeConfig::default()
    }
}

/// A QUIC transport with 16 KiB stream and 64 KiB connection windows.
fn small_windows() -> iroh::endpoint::QuicTransportConfig {
    iroh::endpoint::QuicTransportConfig::builder()
        .stream_receive_window(iroh::endpoint::VarInt::from_u32(16 * 1024))
        .receive_window(iroh::endpoint::VarInt::from_u32(64 * 1024))
        .build()
}

async fn bind(
    lookup: &MemoryLookup,
    key: Option<iroh::SecretKey>,
    transport: Option<iroh::endpoint::QuicTransportConfig>,
) -> iroh::Endpoint {
    let mut builder = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .address_lookup(lookup.clone())
        .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()]);
    if let Some(key) = key {
        builder = builder.secret_key(key);
    }
    if let Some(transport) = transport {
        builder = builder.transport_config(transport);
    }
    builder.bind().await.unwrap()
}

async fn ready_addr(endpoint: &iroh::Endpoint) -> iroh::EndpointAddr {
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

/// UDP payload bytes this endpoint sent, as iroh's socket metrics count them.
fn sent_bytes(endpoint: &iroh::Endpoint) -> u64 {
    let socket = &endpoint.metrics().socket;
    socket.send_ipv4.get() + socket.send_ipv6.get()
}

fn status_kb(key: &str) -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    status
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .and_then(|rest| rest.trim().trim_end_matches("kB").trim().parse().ok())
        .unwrap()
}

/// Reset the process peak resident set so `VmHWM` covers only what follows.
fn reset_peak() {
    std::fs::write("/proc/self/clear_refs", "5").unwrap();
}

/// Poll `check` every 10 ms until it holds or `cap` passes.
async fn wait_until(cap: Duration, mut check: impl FnMut() -> bool) -> bool {
    let started = Instant::now();
    while !check() {
        if started.elapsed() > cap {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    true
}

fn attempt_values<S: Store>(
    pairs: [(&'static str, &S, Option<u64>); 2],
) -> Vec<(&'static str, u64)> {
    pairs
        .into_iter()
        .filter_map(|(key, storage, before)| Some((key, storage.attempts()? - before?)))
        .collect()
}

/// Pull `topic_id` from `addr` with manual syncs until `goal` is reached.
async fn pull_until<S: Store>(
    net: &net::IrohNet<S>,
    addr: &iroh::EndpointAddr,
    topic_id: TopicId,
    goal: &ActorClock,
) -> (bool, u64) {
    let started = Instant::now();
    let mut exchanges = 0;
    while !net
        .node()
        .storage()
        .actor_clock(&topic_id)
        .unwrap()
        .dominates(goal)
    {
        if exchanges == 256 || started.elapsed() > CAP {
            return (false, exchanges);
        }
        exchanges += 1;
        match net.sync_now(addr.clone(), topic_id).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => {
                eprintln!("bench note: sync_now failed: {error}");
                return (false, exchanges);
            }
        }
    }
    (true, exchanges)
}

/// `topics` topics of `ops` ops each, owed to one member and drained by the
/// resync loop from the first scheduling pass.
async fn many_topics<S: Store>(topics: usize, ops: usize) -> Run {
    let dir = tempfile::tempdir().unwrap();
    let lookup = MemoryLookup::new();
    let alice_endpoint = bind(&lookup, Some(iroh::SecretKey::from_bytes(&[60; 32])), None).await;
    let alice = S::builder(dir.path(), "alice")
        .with_iroh_secret_key(alice_endpoint.secret_key())
        .with_write_concern(WriteConcern::Local)
        .build()
        .unwrap();
    let bob = S::builder(dir.path(), "bob")
        .with_peer_whitelist([alice.peer_id()])
        .with_iroh_runtime_config(runtime())
        .with_net(bind(&lookup, Some(iroh::SecretKey::from_bytes(&[61; 32])), None).await)
        .build()
        .unwrap();
    let bob_endpoint = bob.endpoint().unwrap().clone();
    lookup.add_endpoint_info(ready_addr(&bob_endpoint).await);
    lookup.add_endpoint_info(ready_addr(&alice_endpoint).await);

    let mut goal = Vec::new();
    for index in 0..topics {
        let topic_id = TopicId::hash(format!("bench-small-topics/{index}"));
        oplog::Oplog::with_storage(alice.storage().clone())
            .create_topic_genesis(
                topic_id,
                actor_id_for(topic_id, alice.peer_id()),
                TopicGenesis::new(Note::TYPE_ID, [alice.peer_id(), bob.peer_id()]),
                alice.signer(),
            )
            .unwrap();
        let topic = alice.open_topic::<Note>(topic_id).unwrap();
        let mut last = None;
        for event in 1..ops {
            let text = format!("{index}-{event}");
            last = Some(topic.publish(Note { text }).unwrap().meta.op_id);
        }
        alice
            .put_sync_obligation(bob.peer_id(), topic.id(), last.into_iter().collect())
            .unwrap();
        goal.push((
            topic.id(),
            alice.storage().actor_clock(&topic.id()).unwrap(),
        ));
    }
    super::bench::fixture(
        "small_topics",
        alice.storage(),
        goal.iter().map(|(topic, _)| *topic),
    );
    let net =
        Arc::new(net::IrohNet::new_with_config(alice_endpoint, alice.clone(), runtime()).unwrap());
    let sent = (sent_bytes(net.endpoint()), sent_bytes(&bob_endpoint));
    let attempts = (alice.storage().attempts(), bob.storage().attempts());

    let started = super::bench::Interval::new::<S>("small_topics");
    net.start_accept_loop().unwrap();
    net.start_configured_resync_loop().unwrap();
    let done = wait_until(CAP, || {
        goal.iter()
            .all(|(id, clock)| bob.storage().actor_clock(id).unwrap().dominates(clock))
            && alice.storage().all_sync_obligations().unwrap().is_empty()
    })
    .await;
    let ms = started.millis();
    let mut values = vec![
        ("streams", net.outbound_sync_streams()),
        ("alice_sent_bytes", sent_bytes(net.endpoint()) - sent.0),
        ("bob_sent_bytes", sent_bytes(&bob_endpoint) - sent.1),
    ];
    values.extend(attempt_values([
        ("alice_tx_attempts", alice.storage(), attempts.0),
        ("bob_tx_attempts", bob.storage(), attempts.1),
    ]));
    for (lane, names) in net.lane_times().iter().zip([
        ["control_jobs", "control_wait_us", "control_run_us"],
        ["bulk_jobs", "bulk_wait_us", "bulk_run_us"],
    ]) {
        use std::sync::atomic::Ordering;
        values.extend(names.into_iter().zip([
            lane.jobs.load(Ordering::Relaxed),
            lane.waited_max_micros.load(Ordering::Relaxed),
            lane.ran_max_micros.load(Ordering::Relaxed),
        ]));
    }
    net.shutdown().await;
    bob.shutdown_iroh().await;
    Run { ms, done, values }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, run explicitly"]
async fn small_topics_sync() {
    for topics in [256, 1024] {
        for name in [MemoryStorage::NAME, FjallStorage::NAME] {
            let mut runs = Vec::new();
            for _ in 0..REPS {
                runs.push(match name {
                    "memory" => many_topics::<MemoryStorage>(topics, 8).await,
                    _ => many_topics::<FjallStorage>(topics, 8).await,
                });
            }
            report(
                "small_topics",
                &format!("backend={name} topics={topics} ops=8"),
                runs,
            );
        }
    }
}

/// A member holding only the genesis pulls `ops` notes of `bytes` each.
async fn large_payloads<S: Store>(ops: usize, bytes: usize) -> Run {
    let dir = tempfile::tempdir().unwrap();
    let lookup = MemoryLookup::new();
    let alice = S::builder(dir.path(), "alice")
        .with_write_concern(WriteConcern::Local)
        .with_iroh_runtime_config(runtime())
        .with_net(bind(&lookup, None, None).await)
        .build()
        .unwrap();
    let bob_endpoint = bind(&lookup, None, None).await;
    let bob = S::builder(dir.path(), "bob")
        .with_iroh_secret_key(bob_endpoint.secret_key())
        .with_peer_whitelist([alice.peer_id()])
        .build()
        .unwrap();
    let alice_addr = ready_addr(alice.endpoint().unwrap()).await;
    let config = TopicConfig {
        initial_peers: [bob.peer_id()].into(),
        ..TopicConfig::default()
    };
    let topic = alice.create_topic::<Note>(config).unwrap();
    let topic_id = topic.id();
    let genesis = alice.storage().list_ops(&topic_id).unwrap();
    bob.receive_sync_data_from(
        alice.peer_id(),
        SyncData {
            topic_id,
            ops: genesis,
        },
    )
    .unwrap();
    for index in 0..ops {
        let text = format!("{index}{}", "x".repeat(bytes));
        topic.publish(Note { text }).unwrap();
    }
    let goal = alice.storage().actor_clock(&topic_id).unwrap();
    let net = net::IrohNet::new_with_config(bob_endpoint, bob.clone(), runtime()).unwrap();
    let sent = (
        sent_bytes(alice.endpoint().unwrap()),
        sent_bytes(net.endpoint()),
    );
    let attempts = (alice.storage().attempts(), bob.storage().attempts());

    reset_peak();
    let rss = status_kb("VmRSS:");
    let started = Instant::now();
    let (done, exchanges) = pull_until(&net, &alice_addr, topic_id, &goal).await;
    let ms = millis(started);
    let mut values = vec![
        ("exchanges", exchanges),
        ("streams", net.outbound_sync_streams()),
        ("rss_before_kb", rss),
        ("peak_rss_kb", status_kb("VmHWM:")),
        (
            "alice_sent_bytes",
            sent_bytes(alice.endpoint().unwrap()) - sent.0,
        ),
        ("bob_sent_bytes", sent_bytes(net.endpoint()) - sent.1),
    ];
    values.extend(attempt_values([
        ("alice_tx_attempts", alice.storage(), attempts.0),
        ("bob_tx_attempts", bob.storage(), attempts.1),
    ]));
    net.shutdown().await;
    alice.shutdown_iroh().await;
    Run { ms, done, values }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, run explicitly"]
async fn large_payload_pull() {
    for name in [MemoryStorage::NAME, FjallStorage::NAME] {
        let mut runs = Vec::new();
        for _ in 0..REPS {
            runs.push(match name {
                "memory" => large_payloads::<MemoryStorage>(64, 1 << 20).await,
                _ => large_payloads::<FjallStorage>(64, 1 << 20).await,
            });
        }
        let params = format!("backend={name} ops=64 op_bytes=1048576");
        report("large_payload", &params, runs);
    }
}

/// The only selected replica is unreachable (`held`: bound but never accepts;
/// otherwise no address at all). Measures how long a member invited after
/// 256 notes takes to hold the writer's frontier.
async fn peer_fallback(held: bool) -> Run {
    let lookup = MemoryLookup::new();
    let alice = Irokle::builder()
        .with_iroh_runtime_config(runtime())
        .with_net(bind(&lookup, None, None).await)
        .build()
        .unwrap();
    let held_endpoint = bind(&lookup, None, None).await;
    let down = if held {
        lookup.add_endpoint_info(ready_addr(&held_endpoint).await);
        Ed25519Signer::from_iroh_secret_key(held_endpoint.secret_key()).peer_id()
    } else {
        Ed25519Signer::generate().peer_id()
    };
    let carol_key = iroh::SecretKey::generate();
    let carol_peer = Ed25519Signer::from_iroh_secret_key(&carol_key).peer_id();
    let carol = Irokle::builder()
        .with_peer_whitelist([alice.peer_id()])
        .with_iroh_runtime_config(runtime())
        .with_net(bind(&lookup, Some(carol_key), None).await)
        .build()
        .unwrap();
    lookup.add_endpoint_info(ready_addr(carol.endpoint().unwrap()).await);
    lookup.add_endpoint_info(ready_addr(alice.endpoint().unwrap()).await);

    let mut chosen = None;
    for _ in 0..64 {
        let config = TopicConfig {
            initial_peers: [down].into(),
            replication_policy: ReplicationPolicy::all().with_max_sync_peers(1),
        };
        let topic = alice.create_topic::<Note>(config).unwrap();
        let mut state = alice.storage().topic_state(&topic.id()).unwrap().unwrap();
        state.members.insert(carol_peer);
        if node::select_sync_peers(topic.id(), alice.peer_id(), &state) == vec![down] {
            chosen = Some(topic);
            break;
        }
    }
    let topic = chosen.expect("a topic preferring the unreachable replica");
    for index in 0..256 {
        let text = format!("note {index}");
        topic.publish(Note { text }).unwrap();
    }
    let started = Instant::now();
    topic.add_peer(carol_peer).unwrap();
    let goal = alice.storage().actor_clock(&topic.id()).unwrap();
    let done = wait_until(Duration::from_secs(60), || {
        carol
            .storage()
            .actor_clock(&topic.id())
            .unwrap()
            .dominates(&goal)
    })
    .await;
    let ms = millis(started);
    alice.shutdown_iroh().await;
    carol.shutdown_iroh().await;
    held_endpoint.close().await;
    Run {
        ms,
        done,
        values: Vec::new(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, run explicitly"]
async fn unavailable_peer_fallback() {
    for held in [false, true] {
        let mut runs = Vec::new();
        for _ in 0..REPS {
            runs.push(peer_fallback(held).await);
        }
        let down = if held { "held" } else { "unknown" };
        let params = format!("backend=memory preferred={down} notes=256 cap_ms=60000");
        report("fallback", &params, runs);
    }
}

/// Small QUIC windows on both sides: a pull of 600 KiB notes, then 200 more
/// pulled after the member restarts its endpoint.
async fn window_reconnect() -> (Run, Run) {
    let lookup = MemoryLookup::new();
    let alice_endpoint = bind(&lookup, None, Some(small_windows())).await;
    let alice = Irokle::builder()
        .with_iroh_secret_key(alice_endpoint.secret_key())
        .with_write_concern(WriteConcern::Local)
        .build()
        .unwrap();
    let alice_net =
        Arc::new(net::IrohNet::new_with_config(alice_endpoint, alice.clone(), runtime()).unwrap());
    alice_net.start_accept_loop().unwrap();
    let alice_addr = ready_addr(alice_net.endpoint()).await;
    let bob_key = iroh::SecretKey::generate();
    let bob = Irokle::builder()
        .with_iroh_secret_key(&bob_key)
        .with_peer_whitelist([alice.peer_id()])
        .build()
        .unwrap();
    let config = TopicConfig {
        initial_peers: [bob.peer_id()].into(),
        ..TopicConfig::default()
    };
    let topic = alice.create_topic::<Note>(config).unwrap();
    let topic_id = topic.id();
    let genesis = alice.storage().list_ops(&topic_id).unwrap();
    bob.receive_sync_data_from(
        alice.peer_id(),
        SyncData {
            topic_id,
            ops: genesis,
        },
    )
    .unwrap();
    let publish = |from: usize, to: usize| {
        for index in from..to {
            topic
                .publish(Note {
                    text: format!("{index:0>1024}"),
                })
                .unwrap();
        }
        alice.storage().actor_clock(&topic_id).unwrap()
    };

    let mut phases = Vec::new();
    for (from, to) in [(0, 600), (600, 800)] {
        let goal = publish(from, to);
        let endpoint = bind(&lookup, Some(bob_key.clone()), Some(small_windows())).await;
        let net = net::IrohNet::new_with_config(endpoint, bob.clone(), runtime()).unwrap();
        let sent = (sent_bytes(alice_net.endpoint()), sent_bytes(net.endpoint()));
        let started = Instant::now();
        let (done, exchanges) = pull_until(&net, &alice_addr, topic_id, &goal).await;
        let ms = millis(started);
        let values = vec![
            ("exchanges", exchanges),
            (
                "alice_sent_bytes",
                sent_bytes(alice_net.endpoint()) - sent.0,
            ),
            ("bob_sent_bytes", sent_bytes(net.endpoint()) - sent.1),
        ];
        net.shutdown().await;
        phases.push(Run { ms, done, values });
    }
    alice_net.shutdown().await;
    let reconnect = phases.pop().unwrap();
    (phases.pop().unwrap(), reconnect)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, run explicitly"]
async fn small_window_reconnect() {
    let (mut first, mut second) = (Vec::new(), Vec::new());
    for _ in 0..REPS {
        let (initial, reconnect) = window_reconnect().await;
        first.push(initial);
        second.push(reconnect);
    }
    let params = "backend=memory stream_window=16384 conn_window=65536 op_bytes=1024";
    report(
        "small_windows",
        &format!("{params} ops=600 phase=initial"),
        first,
    );
    report(
        "small_windows",
        &format!("{params} ops=200 phase=reconnect"),
        second,
    );
}

/// A request for every op of `topic_id` a member holding only the genesis lacks.
fn full_request(node: &Irokle, author: PeerId, topic_id: TopicId) -> Vec<crate::sync::SyncMessage> {
    let genesis = node
        .storage()
        .topic_state(&topic_id)
        .unwrap()
        .unwrap()
        .genesis;
    let request = crate::sync::SyncRequest {
        topic_id,
        known: BTreeSet::new(),
        wants: BTreeSet::new(),
        actor_range_hints: vec![crate::sync::ActorRangeHint {
            actor_id: actor_id_for(topic_id, author),
            from_exclusive: 1,
            to_inclusive: u64::MAX,
        }],
        genesis: Some(genesis),
        credit: crate::sync::SyncCredit::default(),
        window: crate::sync::ActorWindow::default(),
    };
    vec![
        crate::sync::SyncMessage::Open(node.sync_open(topic_id)),
        crate::sync::SyncMessage::Request(request),
    ]
}

/// `callers` concurrent direct exchanges each hold a page of `ops` notes of
/// 4 KiB while every other caller is cancelled right after it starts.
async fn held_results(callers: usize, ops: usize) -> Run {
    let lookup = MemoryLookup::new();
    let alice = Irokle::builder()
        .with_write_concern(WriteConcern::Local)
        .with_iroh_runtime_config(runtime())
        .with_net(bind(&lookup, None, None).await)
        .build()
        .unwrap();
    let bob_endpoint = bind(&lookup, None, None).await;
    let bob = Irokle::builder()
        .with_iroh_secret_key(bob_endpoint.secret_key())
        .with_peer_whitelist([alice.peer_id()])
        .build()
        .unwrap();
    let alice_addr = ready_addr(alice.endpoint().unwrap()).await;
    let config = TopicConfig {
        initial_peers: [bob.peer_id()].into(),
        ..TopicConfig::default()
    };
    let topic = alice.create_topic::<Note>(config).unwrap();
    let topic_id = topic.id();
    let genesis = alice.storage().list_ops(&topic_id).unwrap();
    let data = SyncData {
        topic_id,
        ops: genesis,
    };
    bob.receive_sync_data_from(alice.peer_id(), data).unwrap();
    for index in 0..ops {
        let text = format!("{index}{}", "x".repeat(4096));
        topic.publish(Note { text }).unwrap();
    }
    let messages = full_request(&bob, alice.peer_id(), topic_id);
    let net =
        Arc::new(net::IrohNet::new_with_config(bob_endpoint, bob.clone(), runtime()).unwrap());

    reset_peak();
    let rss = status_kb("VmRSS:");
    let started = Instant::now();
    let calls = (0..callers)
        .map(|index| {
            let (net, addr, messages) = (Arc::clone(&net), alice_addr.clone(), messages.clone());
            let call = tokio::spawn(async move { net.sync_with(addr, &messages).await });
            if index % 2 == 1 {
                call.abort();
            }
            call
        })
        .collect::<Vec<_>>();
    let mut held = Vec::new();
    let mut failed = 0;
    for call in calls {
        match tokio::time::timeout(CAP, call).await {
            Ok(Ok(Ok(responses))) => held.push(responses),
            Ok(Ok(Err(_))) | Err(_) => failed += 1,
            Ok(Err(_)) => {}
        }
    }
    let ms = millis(started);
    let done = held.len() == callers / 2 && failed == 0;
    let values = vec![
        ("held", held.len() as u64),
        (
            "held_messages",
            held.iter().map(|responses| responses.len() as u64).sum(),
        ),
        ("failed", failed),
        ("rss_before_kb", rss),
        ("peak_rss_kb", status_kb("VmHWM:")),
        ("streams", net.outbound_sync_streams()),
    ];
    drop(held);
    net.shutdown().await;
    alice.shutdown_iroh().await;
    Run { ms, done, values }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, run explicitly"]
async fn retained_results() {
    let mut runs = Vec::new();
    for _ in 0..REPS {
        runs.push(held_results(32, 1024).await);
    }
    report(
        "held_results",
        "backend=memory callers=32 cancelled=16 ops=1024 op_bytes=4096",
        runs,
    );
}

/// On a one-worker runtime, a push of `ops` notes into a store that syncs every
/// commit to disk, and a small control exchange for another topic sent while
/// the push is admitted. Elapsed is the control exchange.
async fn slow_control(ops: usize) -> Run {
    let dir = tempfile::tempdir().unwrap();
    let lookup = MemoryLookup::new();
    let alice_endpoint = bind(&lookup, None, None).await;
    let alice = Irokle::builder()
        .with_iroh_secret_key(alice_endpoint.secret_key())
        .with_write_concern(WriteConcern::Local)
        .build()
        .unwrap();
    let storage =
        FjallStorage::open_with_persist_mode(dir.path().join("bob"), fjall::PersistMode::SyncAll)
            .unwrap();
    let bob = Irokle::builder()
        .with_storage(storage)
        .with_peer_whitelist([alice.peer_id()])
        .with_iroh_runtime_config(runtime())
        .with_net(bind(&lookup, None, None).await)
        .build()
        .unwrap();
    let bob_addr = ready_addr(bob.endpoint().unwrap()).await;
    let shared = || {
        let config = TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        };
        let topic = alice.create_topic::<Note>(config).unwrap();
        let data = SyncData {
            topic_id: topic.id(),
            ops: alice.storage().list_ops(&topic.id()).unwrap(),
        };
        bob.receive_sync_data_from(alice.peer_id(), data).unwrap();
        topic
    };
    let (pushed, other) = (shared(), shared());
    for index in 0..ops {
        let text = format!("{index:0>256}");
        pushed.publish(Note { text }).unwrap();
    }
    other.publish(Note { text: "o".into() }).unwrap();
    let net =
        Arc::new(net::IrohNet::new_with_config(alice_endpoint, alice.clone(), runtime()).unwrap());
    let push_started = Instant::now();
    let push = tokio::spawn({
        let (net, addr, topic_id) = (Arc::clone(&net), bob_addr.clone(), pushed.id());
        async move { net.sync_now(addr, topic_id).await }
    });
    let reached = wait_until(CAP, || {
        bob.storage()
            .actor_clock(&pushed.id())
            .unwrap()
            .get(&actor_id_for(pushed.id(), alice.peer_id()))
            > 1
    })
    .await;
    let messages = vec![
        crate::sync::SyncMessage::Open(alice.sync_open(other.id())),
        crate::sync::SyncMessage::Fingerprint(alice.sync_fingerprint(other.id()).unwrap()),
    ];
    let started = Instant::now();
    let control = tokio::time::timeout(CAP, net.sync_with(bob_addr, &messages)).await;
    let ms = millis(started);
    let pushed_ok = matches!(
        tokio::time::timeout(CAP, push).await,
        Ok(Ok(Ok(()) | Err(_)))
    );
    let push_ms = millis(push_started);
    let done = reached && matches!(control, Ok(Ok(_))) && pushed_ok;
    let values = vec![("push_ms", push_ms as u64)];
    net.shutdown().await;
    bob.shutdown_iroh().await;
    Run { ms, done, values }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[ignore = "measurement, run explicitly"]
async fn slow_storage_control() {
    let mut runs = Vec::new();
    for _ in 0..REPS {
        runs.push(slow_control(2048).await);
    }
    report(
        "slow_control",
        "backend=fjall persist=sync_all workers=1 push_ops=2048",
        runs,
    );
}
