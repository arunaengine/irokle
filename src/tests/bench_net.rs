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
    let alice_endpoint = bind(&lookup, None, None).await;
    let alice = S::builder(dir.path(), "alice")
        .with_iroh_secret_key(alice_endpoint.secret_key())
        .with_write_concern(WriteConcern::Local)
        .build()
        .unwrap();
    let bob = S::builder(dir.path(), "bob")
        .with_peer_whitelist([alice.peer_id()])
        .with_iroh_runtime_config(runtime())
        .with_net(bind(&lookup, None, None).await)
        .build()
        .unwrap();
    let bob_endpoint = bob.endpoint().unwrap().clone();
    lookup.add_endpoint_info(ready_addr(&bob_endpoint).await);
    lookup.add_endpoint_info(ready_addr(&alice_endpoint).await);

    let mut goal = Vec::new();
    for index in 0..topics {
        let config = TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        };
        let topic = alice.create_topic::<Note>(config).unwrap();
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
    let net =
        Arc::new(net::IrohNet::new_with_config(alice_endpoint, alice.clone(), runtime()).unwrap());
    let sent = (sent_bytes(net.endpoint()), sent_bytes(&bob_endpoint));
    let attempts = (alice.storage().attempts(), bob.storage().attempts());

    let started = Instant::now();
    net.start_accept_loop().unwrap();
    net.start_configured_resync_loop().unwrap();
    let done = wait_until(CAP, || {
        goal.iter()
            .all(|(id, clock)| bob.storage().actor_clock(id).unwrap().dominates(clock))
            && alice.storage().all_sync_obligations().unwrap().is_empty()
    })
    .await;
    let ms = millis(started);
    let mut values = vec![
        ("streams", net.outbound_sync_streams()),
        ("alice_sent_bytes", sent_bytes(net.endpoint()) - sent.0),
        ("bob_sent_bytes", sent_bytes(&bob_endpoint) - sent.1),
    ];
    values.extend(attempt_values([
        ("alice_tx_attempts", alice.storage(), attempts.0),
        ("bob_tx_attempts", bob.storage(), attempts.1),
    ]));
    net.shutdown().await;
    bob.shutdown_iroh().await;
    Run { ms, done, values }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, run explicitly"]
async fn small_topics_sync() {
    for name in [MemoryStorage::NAME, FjallStorage::NAME] {
        let mut runs = Vec::new();
        for _ in 0..REPS {
            runs.push(match name {
                "memory" => many_topics::<MemoryStorage>(256, 8).await,
                _ => many_topics::<FjallStorage>(256, 8).await,
            });
        }
        report(
            "small_topics",
            &format!("backend={name} topics=256 ops=8"),
            runs,
        );
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
