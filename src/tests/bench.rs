//! Core cost measurements without a transport. Run explicitly and serially:
//! `cargo test --features fjall,iroh --lib tests::bench:: -- --ignored --nocapture --test-threads=1`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use super::support::*;
use crate::oplog::Oplog;
use crate::storage::{
    AdmissionEffects, AdmittedBatch, FjallStorage, OpMeta, PeerAck, SnapshotRead, SyncObligation,
    SyncStatusUpdate, TopicState, TopicView,
};
use crate::sync::{PageBudget, SyncData, SyncEngine};
use crate::{EvictionKey, SyncPeerStatus, TopicEviction, TopicInfo};

const REPS: usize = 3;

pub(super) fn persist_mode() -> fjall::PersistMode {
    match std::env::var("IROKLE_BENCH_PERSIST").as_deref() {
        Ok("buffer") => fjall::PersistMode::Buffer,
        Ok("sync_all") | Err(std::env::VarError::NotPresent) => fjall::PersistMode::SyncAll,
        _ => panic!("invalid measurement persist mode"),
    }
}

/// Explicit facade walks, alongside the backend's native read counters.
#[derive(Default)]
struct Reads {
    walks: AtomicU64,
}

#[derive(Clone)]
struct Counting<S> {
    inner: S,
    reads: Arc<Reads>,
}

impl<S: PayloadReads> Counting<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            reads: Arc::default(),
        }
    }

    fn snapshot(&self) -> [u64; 4] {
        let [ops, metas, index] = self.inner.read_counts();
        [ops, metas, index, self.reads.walks.load(Ordering::Relaxed)]
    }
}

fn add(counter: &AtomicU64, count: usize) {
    counter.fetch_add(count as u64, Ordering::Relaxed);
}

macro_rules! forward {
    ($($name:ident($($arg:ident: $ty:ty),*) -> $ret:ty;)*) => {
        $(fn $name(&self, $($arg: $ty),*) -> Result<$ret, Error> {
            self.inner.$name($($arg),*)
        })*
    };
}

impl<S: Storage + PayloadReads> Storage for Counting<S> {
    fn read_snapshot<R>(
        &self,
        read: impl FnOnce(&dyn SnapshotRead) -> Result<R, Error>,
    ) -> Result<R, Error> {
        self.inner.read_snapshot(read)
    }
    fn get_op(&self, id: &OpId) -> Result<Option<Op>, Error> {
        self.inner.get_op(id)
    }
    fn get_meta(&self, id: &OpId) -> Result<Option<OpMeta>, Error> {
        self.inner.get_meta(id)
    }
    fn actor_index(
        &self,
        topic: &TopicId,
        actor: &ActorId,
        seq: u64,
    ) -> Result<Option<OpId>, Error> {
        self.inner.actor_index(topic, actor, seq)
    }
    fn actor_range(
        &self,
        topic: &TopicId,
        actor: &ActorId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, OpId)>, Error> {
        let range = self.inner.actor_range(topic, actor, after, limit)?;
        Ok(range)
    }
    fn children(&self, id: &OpId) -> Result<BTreeSet<OpId>, Error> {
        let children = self.inner.children(id)?;
        add(&self.reads.walks, children.len());
        Ok(children)
    }
    fn list_ops(&self, topic: &TopicId) -> Result<Vec<Op>, Error> {
        let ops = self.inner.list_ops(topic)?;
        add(&self.reads.walks, ops.len());
        Ok(ops)
    }
    fn list_op_ids(&self, topic: &TopicId) -> Result<BTreeSet<OpId>, Error> {
        let ids = self.inner.list_op_ids(topic)?;
        add(&self.reads.walks, ids.len());
        Ok(ids)
    }
    forward! {
        put_admitted_batch(batch: AdmittedBatch) -> ();
        dep_resolvable(id: &OpId) -> bool;
        heads(topic: &TopicId) -> BTreeSet<OpId>;
        actor_tip(topic: &TopicId, actor: &ActorId) -> Option<(u64, OpId)>;
        actor_clock(topic: &TopicId) -> ActorClock;
        topic_fingerprint(topic: &TopicId) -> [u8; 32];
        max_generation(topic: &TopicId) -> u64;
        topic_state(topic: &TopicId) -> Option<TopicState>;
        list_topics() -> Vec<TopicInfo>;
        topic_view(topic: &TopicId, peer: Option<&PeerId>) -> Option<TopicView>;
        put_pending_op(source: PeerId, op: Op, meta: OpMeta) -> ();
        pending_waiters(dep: &OpId) -> Vec<(PeerId, Op)>;
        ready_pending_ops() -> Vec<(PeerId, Op)>;
        ready_pending_after(after: Option<&OpId>, limit: usize) -> Vec<(PeerId, Op)>;
        pending_missing_deps(topic: &TopicId) -> BTreeSet<OpId>;
        remove_pending_op(id: &OpId) -> ();
        purge_pending_waiters(dep: &OpId) -> usize;
        reject_pending_subtree(id: &OpId) -> usize;
        peer_ack(peer: &PeerId, topic: &TopicId) -> Option<PeerAck>;
        peer_acks(topic: &TopicId) -> Vec<PeerAck>;
        put_sync_obligation(obligation: SyncObligation, genesis: Option<OpId>) -> ();
        all_sync_obligations() -> Vec<SyncObligation>;
        apply_peer_ack(ack: PeerAck) -> usize;
        apply_peer_acks(acks: Vec<PeerAck>) -> Vec<Result<usize, Error>>;
        sync_obligations(peer: &PeerId, topic: &TopicId) -> Vec<SyncObligation>;
        has_sync_obligations(peer: &PeerId, topic: &TopicId) -> bool;
        put_sync_status(status: SyncPeerStatus) -> ();
        update_sync_status(peer: &PeerId, topic: &TopicId, update: &SyncStatusUpdate) -> SyncPeerStatus;
        sync_statuses(topic: &TopicId) -> Vec<SyncPeerStatus>;
        topic_obligation_counts(topic: &TopicId) -> BTreeMap<PeerId, usize>;
        clear_peer_sync_state(peer: &PeerId, topic: &TopicId, genesis: Option<OpId>) -> usize;
        reset_topic(topic: &TopicId) -> usize;
        reset_topic_and_admit(
            topic: &TopicId,
            expected: &TopicState,
            batch: AdmittedBatch,
            eviction: Option<&TopicEviction>
        ) -> usize;
        seal_topic(topic: &TopicId) -> bool;
        unseal_topic(topic: &TopicId) -> bool;
        pending_evictions() -> Vec<TopicEviction>;
        clear_eviction(key: &EvictionKey) -> ();
        peer_reached_op(peer: &PeerId, id: &OpId) -> bool;
        peers_reached_op(id: &OpId) -> Vec<PeerId>;
        next_attempt_epoch() -> u64;
        provisional_topics() -> Vec<crate::storage::ProvisionalTopic>;
        open_provisional(source: PeerId, topic: TopicId, genesis: OpId, now_ms: u64) -> crate::storage::ProvisionalTopic;
        stored_bytes() -> u64;
        touch_provisional(provisional: &crate::storage::ProvisionalTopic, now_ms: u64) -> ();
        activate_provisional(provisional: &crate::storage::ProvisionalTopic, expected: &TopicState, effects: AdmissionEffects) -> ();
        discard_provisional(provisional: &crate::storage::ProvisionalTopic) -> bool;
    }
    fn staging_limits(&self) -> crate::storage::StagingLimits {
        self.inner.staging_limits()
    }
    fn provisional_store(
        &self,
        provisional: &crate::storage::ProvisionalTopic,
    ) -> Result<Option<Self>, Error> {
        Ok(self
            .inner
            .provisional_store(provisional)?
            .map(|inner| Self {
                inner,
                reads: Arc::clone(&self.reads),
            }))
    }
}

/// Buffered payloads a backend decoded, where the revision can count them.
trait PayloadReads {
    fn read_counts(&self) -> [u64; 3];
    fn payload_reads(&self) -> Option<u64>;
    /// Write transactions attempted; each Fjall attempt requests a disk sync.
    fn transaction_attempts(&self) -> u64;
}

impl PayloadReads for MemoryStorage {
    fn read_counts(&self) -> [u64; 3] {
        let counts = self.counters();
        [counts.op_reads, counts.meta_reads, counts.index_reads]
    }
    fn payload_reads(&self) -> Option<u64> {
        Some(self.counters().pending_payload_reads)
    }
    fn transaction_attempts(&self) -> u64 {
        self.counters().transaction_attempts
    }
}

impl PayloadReads for FjallStorage {
    fn read_counts(&self) -> [u64; 3] {
        let counts = self.counters();
        [counts.op_reads, counts.meta_reads, counts.index_reads]
    }
    fn payload_reads(&self) -> Option<u64> {
        Some(self.counters().pending_payload_reads)
    }
    fn transaction_attempts(&self) -> u64 {
        self.counters().transaction_attempts
    }
}

struct Sample {
    ms: f64,
    counters: Vec<(&'static str, u64)>,
}

pub(super) fn fixture<S: Storage>(name: &str, storage: &S, topics: impl Iterator<Item = TopicId>) {
    let mut hash = blake3::Hasher::new();
    for topic in topics {
        hash.update(topic.as_ref());
        for id in storage.list_op_ids(&topic).unwrap() {
            hash.update(&postcard::to_allocvec(&storage.get_op(&id).unwrap().unwrap()).unwrap());
        }
    }
    eprintln!("bench_fixture name={name} blake3={}", hash.finalize());
}

fn millis(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

pub(super) struct Interval {
    started: Instant,
    #[cfg(unix)]
    profile: Option<std::os::unix::net::UnixStream>,
}

impl Interval {
    pub(super) fn new<S>(phase: &str) -> Self {
        #[cfg(unix)]
        let profile = std::env::var_os("IROKLE_BENCH_PROFILE").and_then(|path| {
            use std::io::{Read, Write};
            let backend = if std::any::type_name::<S>().contains("FjallStorage") {
                "fjall"
            } else {
                "memory"
            };
            if std::env::var("IROKLE_BENCH_PHASE").ok().as_deref()
                != Some(&format!("{phase}/{backend}"))
            {
                return None;
            }
            let mut stream = std::os::unix::net::UnixStream::connect(path).unwrap();
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(300)))
                .unwrap();
            stream.write_all(b"R").unwrap();
            let mut reply = [0];
            stream.read_exact(&mut reply).unwrap();
            assert_eq!(reply, *b"S");
            Some(stream)
        });
        Self {
            started: Instant::now(),
            #[cfg(unix)]
            profile,
        }
    }

    pub(super) fn millis(mut self) -> f64 {
        let elapsed = millis(self.started);
        #[cfg(unix)]
        if let Some(stream) = &mut self.profile {
            use std::io::{Read, Write};
            stream.write_all(b"D").unwrap();
            let mut reply = [0];
            stream.read_exact(&mut reply).unwrap();
            assert_eq!(reply, *b"E");
        }
        elapsed
    }
}

/// Reads between two snapshots, named for the report line.
fn read_delta(before: [u64; 4], after: [u64; 4]) -> Vec<(&'static str, u64)> {
    ["op_reads", "meta_reads", "index_reads", "walk_entries"]
        .into_iter()
        .zip(after.iter().zip(before).map(|(a, b)| a - b))
        .collect()
}

fn report(name: &str, params: &str, samples: Vec<Sample>) {
    let params = if params.contains("backend=fjall") {
        format!("{params} durability={:?}", persist_mode())
    } else {
        params.to_owned()
    };
    for (rep, sample) in samples.iter().enumerate() {
        eprintln!(
            "bench_sample name={name} {params} rep={rep} ms={:.6}",
            sample.ms
        );
    }
    let mut ms = samples.iter().map(|sample| sample.ms).collect::<Vec<_>>();
    ms.sort_by(f64::total_cmp);
    let vary = samples.iter().any(|s| s.counters != samples[0].counters);
    let counters = samples[0]
        .counters
        .iter()
        .map(|(key, value)| format!(" {key}={value}"))
        .collect::<String>();
    eprintln!(
        "bench name={name} {params} reps={} median_ms={:.2} max_ms={:.2}{counters}{}",
        ms.len(),
        ms[ms.len() / 2],
        ms[ms.len() - 1],
        if vary { " counters_vary=true" } else { "" }
    );
}

/// Run `f` on a fresh Memory and a fresh Fjall store for every repetition.
fn each_backend<F, G>(name: &str, params: &str, memory: F, fjall: G)
where
    F: Fn(Counting<MemoryStorage>) -> Sample,
    G: Fn(Counting<FjallStorage>) -> Sample,
{
    let samples = (0..REPS)
        .map(|_| memory(Counting::new(MemoryStorage::new())))
        .collect();
    report(name, &format!("backend=memory {params}"), samples);
    let samples = (0..REPS)
        .map(|_| {
            let dir = tempfile::tempdir().unwrap();
            fjall(Counting::new(
                FjallStorage::open_with_persist_mode(dir.path(), persist_mode()).unwrap(),
            ))
        })
        .collect();
    report(name, &format!("backend=fjall {params}"), samples);
}

fn signer(seed: u8) -> Ed25519Signer {
    Ed25519Signer::from_bytes(&[seed; 32])
}

fn note(index: usize) -> TopicPayload {
    TopicPayload::Event(
        EventEnvelope::encode_event(&Note {
            text: index.to_string(),
        })
        .unwrap(),
    )
}

/// One signed op of `author` on top of `prev` in its own actor chain.
fn next_op(author: &Ed25519Signer, prev: &Op, payload: TopicPayload) -> Op {
    let body = &prev.signed.body;
    Op::sign(
        OpBody {
            topic_id: body.topic_id,
            author: author.peer_id(),
            actor_id: body.actor_id,
            actor_seq: body.actor_seq + 1,
            actor_prev: Some(prev.id),
            deps: [prev.id].into(),
            generation: body.generation + 1,
            payload,
        },
        author,
    )
    .unwrap()
}

/// A genesis by `author` with `peers`, then `len` ops from `payload`.
pub(super) fn signed_chain(
    author: &Ed25519Signer,
    name: &str,
    peers: &[PeerId],
    len: usize,
    payload: impl Fn(usize) -> TopicPayload,
) -> Vec<Op> {
    let topic_id = TopicId::hash(name);
    let genesis = Op::sign(
        OpBody {
            topic_id,
            author: author.peer_id(),
            actor_id: actor_id_for(topic_id, author.peer_id()),
            actor_seq: 1,
            actor_prev: None,
            deps: BTreeSet::new(),
            generation: 0,
            payload: TopicPayload::Genesis(TopicGenesis::new(
                Note::TYPE_ID,
                peers.iter().copied().chain([author.peer_id()]),
            )),
        },
        author,
    )
    .unwrap();
    let mut ops = vec![genesis];
    for index in 0..len {
        let op = next_op(author, ops.last().unwrap(), payload(index));
        ops.push(op);
    }
    ops
}

fn load<S: Storage>(storage: &S, ops: &[Op]) {
    let log = Oplog::with_storage(storage.clone());
    for batch in ops.chunks(1024) {
        log.receive_ops(batch.to_vec()).unwrap();
    }
}

/// Local publishing with one replication target per peer per publish, as the
/// facade's `AsyncReplication` admission effects write them.
fn publish<S: Storage + PayloadReads>(storage: Counting<S>, len: usize) -> Sample {
    let owner = signer(11);
    let peers = [signer(12).peer_id(), signer(13).peer_id()];
    let topic = TopicId::hash("bench-publish");
    let actor = actor_id_for(topic, owner.peer_id());
    let log = Oplog::with_storage(storage.clone());
    let genesis = TopicGenesis::new(Note::TYPE_ID, peers);
    log.create_topic_genesis(topic, actor, genesis, &owner)
        .unwrap();
    let attempts = storage.inner.transaction_attempts();
    let started = Instant::now();
    for index in 0..len {
        let TopicPayload::Event(event) = note(index) else {
            unreachable!()
        };
        log.create_event_effects(topic, actor, event, &owner, |_, meta, state| {
            let mut clock = ActorClock::new();
            clock.observe(meta.actor_id, meta.actor_seq);
            let target = |peer| SyncObligation::clock(peer, state.topic_id, clock.clone());
            Ok(AdmissionEffects {
                sync_obligations: peers.into_iter().map(target).collect(),
            })
        })
        .unwrap();
    }
    let ms = millis(started);
    let attempts = storage.inner.transaction_attempts() - attempts;
    let rows = storage.all_sync_obligations().unwrap().len() as u64;
    Sample {
        ms,
        counters: vec![("rows", rows), ("tx_attempts", attempts)],
    }
}

#[test]
#[ignore = "measurement, run explicitly"]
fn offline_publish() {
    for len in [2048, 4096] {
        let params = format!("ops={len} peers=3");
        each_backend("publish", &params, |s| publish(s, len), |s| publish(s, len));
    }
}

/// A received backlog in pages of 256 while two other selected peers exist.
fn forward_backlog<S: Storage + PayloadReads>(storage: Counting<S>, len: usize) -> Sample {
    let (source, local) = (signer(21), signer(22));
    let peers = [local.peer_id(), signer(23).peer_id(), signer(24).peer_id()];
    let ops = signed_chain(&source, "bench-forward", &peers, len, note);
    let node = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: local,
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let topic_id = ops[0].signed.body.topic_id;
    let attempts = storage.inner.transaction_attempts();
    let started = Instant::now();
    for page in ops.chunks(256) {
        let data = SyncData {
            topic_id,
            ops: page.to_vec(),
        };
        node.receive_sync_data_from(source.peer_id(), data).unwrap();
    }
    let ms = millis(started);
    let attempts = storage.inner.transaction_attempts() - attempts;
    let rows = storage.all_sync_obligations().unwrap().len() as u64;
    Sample {
        ms,
        counters: vec![("rows", rows), ("tx_attempts", attempts)],
    }
}

#[test]
#[ignore = "measurement, run explicitly"]
fn forwarded_backlog() {
    for len in [2048, 4096] {
        let params = format!("ops={len} page=256 other_peers=2");
        each_backend(
            "forward",
            &params,
            |s| forward_backlog(s, len),
            |s| forward_backlog(s, len),
        );
    }
}

/// 2000 buffered ops in one topic, then 100 single admissions into a healthy
/// topic, each followed by its fingerprint and summary.
fn pending_pool<S: Storage + PayloadReads>(storage: Counting<S>) -> Sample {
    let sources = [signer(32).peer_id(), signer(33).peer_id()];
    let unrelated = signed_chain(&signer(31), "bench-pool", &sources, 2001, note);
    let log = Oplog::with_storage(storage.clone());
    log.receive_ops(vec![unrelated[0].clone()]).unwrap();
    for (index, batch) in unrelated[2..].chunks(250).enumerate() {
        log.receive_ops_from_peer(Some(sources[index % 2]), batch.to_vec())
            .unwrap();
    }
    let topic_u = unrelated[0].signed.body.topic_id;
    let missing = storage.pending_missing_deps(&topic_u).unwrap();
    assert!(missing.len() == 2000 && missing.contains(&unrelated[1].id));
    let healthy = signed_chain(&signer(34), "bench-healthy", &[], 100, note);
    let topic_h = healthy[0].signed.body.topic_id;
    log.receive_ops(vec![healthy[0].clone()]).unwrap();
    let engine = SyncEngine::new(log.clone(), signer(34).peer_id());

    let (before, payloads) = (storage.snapshot(), storage.inner.payload_reads());
    let attempts = storage.inner.transaction_attempts();
    let started = Instant::now();
    for op in &healthy[1..] {
        log.receive_ops(vec![op.clone()]).unwrap();
        engine.fingerprint(topic_h).unwrap();
        engine.summary(topic_h).unwrap();
    }
    let ms = millis(started);
    let mut counters = read_delta(before, storage.snapshot());
    if let (Some(before), Some(after)) = (payloads, storage.inner.payload_reads()) {
        counters.push(("pending_payload_reads", after - before));
    }
    counters.push((
        "tx_attempts",
        storage.inner.transaction_attempts() - attempts,
    ));
    Sample { ms, counters }
}

#[test]
#[ignore = "measurement, run explicitly"]
fn unrelated_pool() {
    let params = "pending=2000 admissions=100";
    each_backend("pending_pool", params, pending_pool, pending_pool);
}

/// One page as a requester asks for it from the responder's summary. Only the
/// responder's work is timed and counted.
fn serve_page<S: Storage + PayloadReads>(
    responder: &SyncEngine<Counting<S>>,
    requester: &SyncEngine<MemoryStorage>,
    storage: &Counting<S>,
    topic_id: TopicId,
    (author, reader): (PeerId, PeerId),
) -> (Vec<Op>, f64, [u64; 4], u64) {
    let summary = responder.summary(topic_id).unwrap();
    let build = Interval::new::<S>("request");
    let mut request = requester.plan_request(author, &summary).unwrap();
    // Range hints only: a wanted head beyond one page yields a non-causal page.
    request.wants.clear();
    let request_ns = (build.millis() * 1_000_000.0) as u64;
    let before = storage.snapshot();
    let started = Interval::new::<S>("page");
    let page = responder
        .response_page(reader, &request, PageBudget::from_credit(request.credit))
        .unwrap();
    let ms = started.millis();
    let after = storage.snapshot();
    (
        page.ops,
        ms,
        std::array::from_fn(|i| after[i] - before[i]),
        request_ns,
    )
}

/// A requester holding only the genesis catches up page by page.
fn catch_up<S: Storage + PayloadReads>(storage: Counting<S>, len: usize) -> Sample {
    let (author, reader) = (signer(7), signer(8).peer_id());
    let ops = signed_chain(&author, &format!("bench-chain-{len}"), &[reader], len, note);
    let topic_id = ops[0].signed.body.topic_id;
    load(&storage, &ops);
    fixture("catch_up", &storage, std::iter::once(topic_id));
    let responder = SyncEngine::new(Oplog::with_storage(storage.clone()), author.peer_id());
    let log = Oplog::new();
    log.receive_ops(vec![ops[0].clone()]).unwrap();
    let requester = SyncEngine::new(log.clone(), reader);
    let local = storage.actor_clock(&topic_id).unwrap();

    let (mut ms, mut pages, mut reads) = (0.0, 0, [0; 4]);
    let (mut request_ns, mut admission_ns) = (0, 0);
    while !log
        .storage()
        .actor_clock(&topic_id)
        .unwrap()
        .dominates(&local)
    {
        assert!(pages < 64, "catch-up stopped advancing");
        let peers = (author.peer_id(), reader);
        let (page, page_ms, page_reads, build_ns) =
            serve_page(&responder, &requester, &storage, topic_id, peers);
        ms += page_ms;
        request_ns += build_ns;
        reads = std::array::from_fn(|i| reads[i] + page_reads[i]);
        let admission = Interval::new::<MemoryStorage>("admission");
        log.receive_ops(page).unwrap();
        admission_ns += (admission.millis() * 1_000_000.0) as u64;
        pages += 1;
    }
    let mut counters = vec![
        ("pages", pages),
        ("request_ns", request_ns),
        ("admission_ns", admission_ns),
    ];
    counters.extend(read_delta([0; 4], reads));
    Sample { ms, counters }
}

#[test]
#[ignore = "measurement, run explicitly"]
fn chain_catch_up() {
    for len in [8192, 16384, 65536, 65537, 100_000] {
        let params = format!("ops={len} page_ops=4096 timed=response_page");
        each_backend(
            "catch_up",
            &params,
            |s| catch_up(s, len),
            |s| catch_up(s, len),
        );
    }
}

/// Serving one new op to a peer that holds the whole earlier history.
fn steady_page<S: Storage + PayloadReads>(storage: Counting<S>) -> Vec<Sample> {
    let (author, reader) = (signer(9), signer(10).peer_id());
    let mut ops = signed_chain(&author, "bench-steady", &[reader], 16384, note);
    let topic_id = ops[0].signed.body.topic_id;
    load(&storage, &ops);
    fixture("steady_page", &storage, std::iter::once(topic_id));
    let peer_log = Oplog::new();
    load(peer_log.storage(), &ops);
    let requester = SyncEngine::new(peer_log.clone(), reader);
    let log = Oplog::with_storage(storage.clone());
    let responder = SyncEngine::new(log.clone(), author.peer_id());
    (0..REPS)
        .map(|index| {
            let op = next_op(&author, ops.last().unwrap(), note(16384 + index));
            log.receive_ops(vec![op.clone()]).unwrap();
            let peers = (author.peer_id(), reader);
            let (page, ms, reads, request_ns) =
                serve_page(&responder, &requester, &storage, topic_id, peers);
            assert_eq!(page, vec![op.clone()]);
            let admission = Interval::new::<MemoryStorage>("admission");
            peer_log.receive_ops(page).unwrap();
            let admission_ns = (admission.millis() * 1_000_000.0) as u64;
            ops.push(op);
            let mut counters = read_delta([0; 4], reads);
            counters.extend([("request_ns", request_ns), ("admission_ns", admission_ns)]);
            Sample { ms, counters }
        })
        .collect()
}

#[test]
#[ignore = "measurement, run explicitly"]
fn steady_catch_up() {
    let params = "history=16384 new_ops=1";
    let samples = steady_page(Counting::new(MemoryStorage::new()));
    report("steady_page", &format!("backend=memory {params}"), samples);
    let dir = tempfile::tempdir().unwrap();
    let samples = steady_page(Counting::new(
        FjallStorage::open_with_persist_mode(dir.path(), persist_mode()).unwrap(),
    ));
    report("steady_page", &format!("backend=fjall {params}"), samples);
}

/// A joining writer's event on an old op, admitted through a fresh facade
/// (cold) and then a second writer's event on the same op (warm).
fn projection<S: Storage + PayloadReads>(storage: Counting<S>) -> (Vec<Sample>, Vec<Sample>) {
    let owner = signer(41);
    let writers = (0..2 * REPS as u8)
        .map(|i| signer(42 + i))
        .collect::<Vec<_>>();
    let peers = writers.iter().map(Signer::peer_id).collect::<Vec<_>>();
    let payload = |index: usize| match index % 65 {
        64 => TopicPayload::Control(TopicControl::AddPeer {
            peer: PeerId::hash(index.to_le_bytes()),
        }),
        _ => note(index),
    };
    let ops = signed_chain(&owner, "bench-members", &peers, 4096 + 64, payload);
    load(&storage, &ops);
    let joined = |writer: &Ed25519Signer, dep: &Op| {
        let body = &dep.signed.body;
        let op = Op::sign(
            OpBody {
                topic_id: body.topic_id,
                author: writer.peer_id(),
                actor_id: actor_id_for(body.topic_id, writer.peer_id()),
                actor_seq: 1,
                actor_prev: None,
                deps: [dep.id].into(),
                generation: body.generation + 1,
                payload: note(0),
            },
            writer,
        )
        .unwrap();
        let log = Oplog::with_storage(storage.clone());
        (log, op)
    };
    let mut cold = Vec::new();
    let mut warm = Vec::new();
    for rep in 0..REPS {
        let dep = &ops[100];
        let (log, first) = joined(&writers[2 * rep], dep);
        let (_, second) = joined(&writers[2 * rep + 1], dep);
        for (samples, op) in [(&mut cold, first), (&mut warm, second)] {
            let before = storage.snapshot();
            let started = Instant::now();
            let accepted = log.receive_ops(vec![op.clone()]).unwrap();
            let ms = millis(started);
            assert!(accepted.contains(&op.id));
            let counters = read_delta(before, storage.snapshot());
            samples.push(Sample { ms, counters });
        }
    }
    (cold, warm)
}

#[test]
#[ignore = "measurement, run explicitly"]
fn membership_projection() {
    let params = "events=4096 controls=64";
    let (cold, warm) = projection(Counting::new(MemoryStorage::new()));
    report("projection_cold", &format!("backend=memory {params}"), cold);
    report("projection_warm", &format!("backend=memory {params}"), warm);
    let dir = tempfile::tempdir().unwrap();
    let (cold, warm) = projection(Counting::new(
        FjallStorage::open_with_persist_mode(dir.path(), persist_mode()).unwrap(),
    ));
    report("projection_cold", &format!("backend=fjall {params}"), cold);
    report("projection_warm", &format!("backend=fjall {params}"), warm);
}

/// A reader holding only the genesis pages through `source` with the default
/// credit. The walk is timed; reads are the responder's.
fn walk_pages<S: Storage + PayloadReads>(
    source: &super::pages::Source<Counting<S>>,
    storage: &Counting<S>,
) -> Sample {
    let before = storage.snapshot();
    let started = Interval::new::<S>("window");
    let pages = super::pages::page_through(source, crate::sync::SyncCredit::default());
    let ms = started.millis();
    let mut counters = vec![("pages", pages as u64)];
    counters.extend(read_delta(before, storage.snapshot()));
    Sample { ms, counters }
}

/// Writers beyond the page actor window, one of them a dependency of the rest.
fn window_walk<S: Storage + PayloadReads>(storage: Counting<S>) -> Sample {
    let source = super::pages::late_dependency(storage.clone(), 4097);
    fixture("window_pages", &storage, std::iter::once(source.topic_id));
    walk_pages(&source, &storage)
}

#[test]
#[ignore = "measurement, run explicitly"]
fn window_progress() {
    let params = "actors=4097 credit=default timed=walk";
    each_backend("window_pages", params, window_walk, window_walk);
}

/// A reader that lost `lost` consecutive op records of an 8192-op chain repairs
/// them from explicit wants, page by page. Only the responder's reads count.
fn repair_walk<S: Storage + PayloadReads>(storage: Counting<S>, lost: usize) -> Sample {
    let reader_id = signer(60).peer_id();
    let (genesis, chains) = super::pages::independent_chains(&Oplog::new(), reader_id, &[8192]);
    load(&storage, std::slice::from_ref(&genesis));
    load(&storage, &chains[0]);
    let log = Oplog::with_storage(storage.clone());
    let source = super::pages::Source {
        engine: SyncEngine::new(log.clone(), signer(244).peer_id()),
        log,
        topic_id: genesis.signed.body.topic_id,
        reader: reader_id,
        genesis: genesis.clone(),
    };
    let reader_store = MemoryStorage::new();
    let reader = Oplog::with_storage(reader_store.clone());
    reader.receive_ops(vec![genesis]).unwrap();
    reader.receive_ops(chains[0].clone()).unwrap();
    for op in &chains[0][1024..1024 + lost] {
        damage_op(&reader_store, &op.id, Damage::Op);
    }
    reader.recheck_topics().unwrap();
    let credit = crate::sync::SyncCredit::default();
    let before = storage.snapshot();
    let started = Instant::now();
    let mut pages = 0;
    loop {
        let request = super::pages::request_for(&source, &reader, credit);
        if request.actor_range_hints.is_empty() && request.wants.is_empty() {
            break;
        }
        assert!(pages < 64, "repair stopped advancing");
        let page = source
            .engine
            .response_page(source.reader, &request, PageBudget::from_credit(credit))
            .unwrap();
        reader.receive_ops(page.ops).unwrap();
        reader.recheck_topics().unwrap();
        pages += 1;
    }
    let ms = millis(started);
    let mut counters = vec![("pages", pages)];
    counters.extend(read_delta(before, storage.snapshot()));
    Sample { ms, counters }
}

#[test]
#[ignore = "measurement, run explicitly"]
fn repair_pull() {
    let params = "chain=8192 lost=2048 timed=walk";
    each_backend(
        "repair_pages",
        params,
        |s| repair_walk(s, 2048),
        |s| repair_walk(s, 2048),
    );
}

/// A late invitation of `len` notes staged in 256-op fragments: the first and
/// the last eight fragments and the activating fragment, timed and counted.
fn staged_fragments<S: Storage + PayloadReads>(storage: Counting<S>, len: usize) -> [Sample; 3] {
    let source = Irokle::new(NodeConfig {
        signer: signer(61),
        ..NodeConfig::default()
    })
    .unwrap();
    let reader = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: signer(62),
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let topic = source.create_topic::<Note>(TopicConfig::default()).unwrap();
    for index in 0..len {
        let text = format!("{index:0>32}");
        topic.publish(Note { text }).unwrap();
    }
    topic.add_peer(reader.peer_id()).unwrap();
    let ops = oplog::topological(source.storage(), &topic.id()).unwrap();
    let (history, invite) = ops.split_at(ops.len() - 1);
    let fragments = history.chunks(256).chain([invite]).collect::<Vec<_>>();
    let count = fragments.len();
    let mut samples = [(0.0, [0; 4], 0), (0.0, [0; 4], 0), (0.0, [0; 4], 0)];
    for (index, fragment) in fragments.into_iter().enumerate() {
        let data = SyncData {
            topic_id: topic.id(),
            ops: fragment.to_vec(),
        };
        let (before, attempts) = (storage.snapshot(), storage.inner.transaction_attempts());
        let started = Instant::now();
        reader.receive_sync_outcome(source.peer_id(), data).unwrap();
        let ms = millis(started);
        let after = storage.snapshot();
        let attempts = storage.inner.transaction_attempts() - attempts;
        let slot = match index {
            _ if index == count - 1 => 2,
            _ if index < 8 => 0,
            _ if index >= count - 9 => 1,
            _ => continue,
        };
        samples[slot].0 += ms;
        samples[slot].1 = std::array::from_fn(|i| samples[slot].1[i] + after[i] - before[i]);
        samples[slot].2 += attempts;
    }
    assert_eq!(
        storage.list_op_ids(&topic.id()).unwrap().len(),
        ops.len(),
        "the invitation did not activate"
    );
    samples.map(|(ms, reads, attempts)| {
        let mut counters = read_delta([0; 4], reads);
        counters.push(("tx_attempts", attempts));
        Sample { ms, counters }
    })
}

#[test]
#[ignore = "measurement, run explicitly"]
fn staged_history() {
    let len = 16384;
    let params = format!("notes={len} fragment_ops=256");
    let run = |backend: &str, samples: Vec<[Sample; 3]>| {
        let mut parts: [Vec<Sample>; 3] = Default::default();
        for sample in samples {
            for (part, value) in parts.iter_mut().zip(sample) {
                part.push(value);
            }
        }
        let [early, late, activate] = parts;
        let params = format!("backend={backend} {params}");
        report("staged_first8", &params, early);
        report("staged_last8", &params, late);
        report("staged_activate", &params, activate);
    };
    let memory = (0..REPS)
        .map(|_| staged_fragments(Counting::new(MemoryStorage::new()), len))
        .collect();
    run("memory", memory);
    let fjall = (0..REPS)
        .map(|_| {
            let dir = tempfile::tempdir().unwrap();
            staged_fragments(
                Counting::new(
                    FjallStorage::open_with_persist_mode(dir.path(), persist_mode()).unwrap(),
                ),
                len,
            )
        })
        .collect();
    run("fjall", fjall);
}

/// 64 sources each stage the first 64 ops of their own late invitation.
fn many_sessions<S: Storage + PayloadReads>(storage: Counting<S>) -> Sample {
    let reader = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: signer(63),
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let fragments = (0..64u8)
        .map(|index| {
            let source = Irokle::new(NodeConfig {
                signer: signer(100 + index),
                ..NodeConfig::default()
            })
            .unwrap();
            let topic_id = TopicId::hash(format!("bench-sessions/{index}"));
            Oplog::with_storage(source.storage().clone())
                .create_topic_genesis(
                    topic_id,
                    actor_id_for(topic_id, source.peer_id()),
                    TopicGenesis::new(Note::TYPE_ID, [source.peer_id()]),
                    source.signer(),
                )
                .unwrap();
            let topic = source.open_topic::<Note>(topic_id).unwrap();
            for event in 0..128 {
                let text = format!("{event}");
                topic.publish(Note { text }).unwrap();
            }
            topic.add_peer(reader.peer_id()).unwrap();
            let ops = oplog::topological(source.storage(), &topic.id()).unwrap();
            fixture("sessions", source.storage(), std::iter::once(topic.id()));
            let data = SyncData {
                topic_id: topic.id(),
                ops: ops[..64].to_vec(),
            };
            (source.peer_id(), data)
        })
        .collect::<Vec<_>>();
    let (before, attempts) = (storage.snapshot(), storage.inner.transaction_attempts());
    let started = Interval::new::<S>("sessions");
    for (source, data) in fragments {
        reader.receive_sync_outcome(source, data).unwrap();
    }
    let ms = started.millis();
    let mut counters = read_delta(before, storage.snapshot());
    counters.push((
        "tx_attempts",
        storage.inner.transaction_attempts() - attempts,
    ));
    counters.push(("staged_topics", reader.list_topics().unwrap().len() as u64));
    Sample { ms, counters }
}

#[test]
#[ignore = "measurement, run explicitly"]
fn provisional_sessions() {
    let params = "sources=64 fragment_ops=64";
    each_backend("sessions", params, many_sessions, many_sessions);
}

#[derive(Default)]
struct SliceCost {
    planning_ns: u64,
    admission_ns: u64,
    slices: u64,
    output_ops: u64,
    output_bytes: u64,
    max_visits: u64,
    max_actors: u64,
    max_edges: u64,
    max_raw_reads: u64,
    kept_peak: u64,
    overshoots: u64,
    premature_completion: u64,
}

fn bounded_fixture(repair: bool) -> (Ed25519Signer, PeerId, Vec<Op>) {
    let author = signer(181);
    let reader = signer(182).peer_id();
    let count = if repair { 64 } else { 128 };
    let name = if repair {
        "bench-repair-forward"
    } else {
        "bench-wide-dependencies"
    };
    let mut ops = signed_chain(&author, name, &[reader], count, note);
    if !repair {
        let last = ops.last().unwrap();
        let mut body = last.signed.body.clone();
        body.actor_seq += 1;
        body.actor_prev = Some(last.id);
        body.generation += 1;
        body.deps = ops.iter().map(|op| op.id).collect();
        body.payload = note(count + 1);
        ops.push(Op::sign(body, &author).unwrap());
    }
    (author, reader, ops)
}

fn bounded_page<S: Storage + PayloadReads>(
    storage: Counting<S>,
    repair: bool,
    informed: bool,
    rep: usize,
) -> Sample {
    use crate::sync::{ActorRangeHint, SyncRequest};
    let setup = Instant::now();
    let (author, peer, ops) = bounded_fixture(repair);
    let topic = ops[0].signed.body.topic_id;
    let encoded = postcard::to_allocvec(&ops).unwrap();
    let hash = blake3::hash(&encoded);
    let name = if repair {
        "repair_forward"
    } else {
        "wide_dependencies"
    };
    let backend = if std::any::type_name::<S>().contains("FjallStorage") {
        "fjall"
    } else {
        "memory"
    };
    load(&storage, &ops);
    let log = Oplog::with_storage(storage.clone());
    let responder = SyncEngine::new(log, author.peer_id()).with_page_visits(8, 1);
    let destination = Oplog::new();
    destination
        .receive_ops(ops[..ops.len() - 1].to_vec())
        .unwrap();
    let receiver = SyncEngine::new(destination.clone(), peer);
    let actor = ops[0].signed.body.actor_id;
    let goal = ops.last().unwrap().signed.body.actor_seq;
    let mut request = SyncRequest {
        topic_id: topic,
        known: BTreeSet::new(),
        wants: if repair {
            ops[1..17].iter().map(|op| op.id).collect()
        } else {
            BTreeSet::new()
        },
        actor_range_hints: vec![ActorRangeHint {
            actor_id: actor,
            from_exclusive: goal - 1,
            to_inclusive: goal,
        }],
        genesis: Some(ops[0].id),
        credit: crate::sync::SyncCredit {
            ops: 1,
            bytes: crate::sync::MAX_PAGE_BYTES as u64,
        },
        window: Default::default(),
    };
    let setup_ns = setup.elapsed().as_nanos() as u64;
    let mut cost = SliceCost::default();
    let raw_start = storage.snapshot();
    let work_start = responder.page_work();
    let convergence = Instant::now();
    let mut complete = false;
    for _ in 0..8192 {
        let held = receiver.summary(topic).unwrap();
        let before = responder.page_work();
        let raw_before = storage.snapshot();
        let started = Instant::now();
        let budget = PageBudget::from_credit(request.credit);
        let page = if informed {
            responder.response_with(peer, &request, budget, &held)
        } else {
            responder.response_page(peer, &request, budget)
        }
        .unwrap();
        cost.planning_ns += started.elapsed().as_nanos() as u64;
        let raw_after = storage.snapshot();
        let after = responder.page_work();
        let visits = after.visits - before.visits;
        let actors = after.actors - before.actors;
        let edges = after.edges - before.edges;
        let raw_reads = (0..3)
            .map(|index| raw_after[index] - raw_before[index])
            .sum();
        cost.slices += 1;
        cost.max_visits = cost.max_visits.max(visits);
        cost.max_actors = cost.max_actors.max(actors);
        cost.max_edges = cost.max_edges.max(edges);
        cost.max_raw_reads = cost.max_raw_reads.max(raw_reads);
        cost.kept_peak = cost.kept_peak.max(after.kept_bytes);
        cost.overshoots += u64::from(visits + actors > 8 || edges > 8);
        assert!(page.missing.is_empty() && page.positions.is_empty());
        assert!(page.too_large.is_none());
        assert!(page.ops.len() <= 1);
        for op in &page.ops {
            for dependency in &op.signed.body.deps {
                assert!(destination.storage().dep_resolvable(dependency).unwrap());
            }
            request.wants.remove(&op.id);
            cost.output_bytes += postcard::experimental::serialized_size(op).unwrap() as u64;
        }
        cost.output_ops += page.ops.len() as u64;
        let admission = Instant::now();
        destination.receive_ops(page.ops).unwrap();
        cost.admission_ns += admission.elapsed().as_nanos() as u64;
        let clock = destination.storage().actor_clock(&topic).unwrap();
        request.actor_range_hints[0].from_exclusive = clock.get(&actor);
        complete = request.wants.is_empty() && clock.get(&actor) == goal;
        // Both revisions serve the captured goal even when the baseline's flag ends early.
        if !page.more && !complete {
            cost.premature_completion += 1;
        }
        if complete {
            break;
        }
    }
    let convergence_ns = convergence.elapsed().as_nanos() as u64;
    assert!(
        complete,
        "bounded fixture did not converge after 8192 slices"
    );
    for op in &ops {
        assert_eq!(
            destination.storage().get_op(&op.id).unwrap().as_ref(),
            Some(op)
        );
    }
    let work = responder.page_work();
    let mut counters = vec![
        ("setup_ns", setup_ns),
        ("admission_ns", cost.admission_ns),
        ("convergence_ns", convergence_ns),
        ("slices", cost.slices),
        ("output_ops", cost.output_ops),
        ("output_bytes", cost.output_bytes),
        ("visits", work.visits - work_start.visits),
        ("actors", work.actors - work_start.actors),
        ("edges", work.edges - work_start.edges),
        ("resumed", work.resumed - work_start.resumed),
        ("max_visits", cost.max_visits),
        ("max_actors", cost.max_actors),
        ("max_edges", cost.max_edges),
        ("max_raw_reads", cost.max_raw_reads),
        ("kept_peak_bytes", cost.kept_peak),
        ("overshoot_slices", cost.overshoots),
        ("premature_completion", cost.premature_completion),
    ];
    counters.extend(read_delta(raw_start, storage.snapshot()));
    let values = counters
        .iter()
        .map(|(key, value)| format!(" {key}={value}"))
        .collect::<String>();
    eprintln!(
        "bench_detail name={name} backend={backend} informed={informed} rep={rep} completion_policy=captured_goal visits_limit=8 credit_ops=1 fixture_blake3={hash} fixture_bytes={} planning_ns={} complete=true decoded_bytes=null workspace_peak=null{values}",
        encoded.len(),
        cost.planning_ns
    );
    Sample {
        ms: cost.planning_ns as f64 / 1_000_000.0,
        counters,
    }
}

fn bounded_backends(repair: bool, informed: bool) {
    let reps =
        std::env::var("IROKLE_BOUND_REPS").map_or(3, |value| value.parse::<usize>().unwrap());
    assert!(reps > 0, "measurement needs at least one sample");
    let name = if repair {
        "repair_forward"
    } else {
        "wide_dependencies"
    };
    let shape = if repair {
        "wants=16 chain_ops=64"
    } else {
        "dependencies=129"
    };
    let params = format!("informed={informed} {shape} visits=8 credit_ops=1");
    let memory = (0..reps)
        .map(|rep| bounded_page(Counting::new(MemoryStorage::new()), repair, informed, rep))
        .collect();
    report(name, &format!("backend=memory {params}"), memory);
    let fjall = (0..reps)
        .map(|rep| {
            let directory = tempfile::tempdir().unwrap();
            let storage =
                FjallStorage::open_with_persist_mode(directory.path(), persist_mode()).unwrap();
            bounded_page(Counting::new(storage), repair, informed, rep)
        })
        .collect();
    report(name, &format!("backend=fjall {params}"), fjall);
}

#[test]
#[ignore = "paired bounded-page measurement, run explicitly"]
fn wide_dependencies() {
    for informed in [false, true] {
        bounded_backends(false, informed);
    }
}

#[test]
#[ignore = "paired bounded-page measurement, run explicitly"]
fn repair_forward() {
    for informed in [false, true] {
        bounded_backends(true, informed);
    }
}

/// Rounds of summary, fingerprint and request plan per repeated measurement.
const INTEGRITY_ROUNDS: u64 = 8;

/// A 16384-op topic with one record lost, as its holder answers for it: the
/// first fingerprint, rounds on the unchanged store, then the repair of the
/// record with the next fingerprint.
fn integrity_round<S: Storage + PayloadReads + Corrupt>(storage: Counting<S>) -> [Sample; 3] {
    let (author, reader) = (signer(11), signer(12));
    let ops = signed_chain(&author, "bench-integrity", &[reader.peer_id()], 16384, note);
    let topic_id = ops[0].signed.body.topic_id;
    load(&storage, &ops);
    fixture("integrity", &storage, std::iter::once(topic_id));
    let lost = ops[8192].clone();
    storage.inner.drop_op_record(&lost.id);
    let log = Oplog::with_storage(storage.clone());
    let engine = SyncEngine::new(log.clone(), author.peer_id());
    let peer = Oplog::new();
    load(peer.storage(), &ops);
    let remote = SyncEngine::new(peer, reader.peer_id())
        .summary(topic_id)
        .unwrap();
    let measure = |work: &dyn Fn()| {
        let before = storage.snapshot();
        let started = Instant::now();
        work();
        let ms = millis(started);
        Sample {
            ms,
            counters: read_delta(before, storage.snapshot()),
        }
    };
    let cold = measure(&|| {
        engine.fingerprint(topic_id).unwrap();
    });
    let repeated = measure(&|| {
        for _ in 0..INTEGRITY_ROUNDS {
            engine.summary(topic_id).unwrap();
            engine.fingerprint(topic_id).unwrap();
            let request = engine.plan_request(reader.peer_id(), &remote).unwrap();
            assert!(request.wants.contains(&lost.id));
        }
    });
    let healed = measure(&|| {
        log.receive_ops(vec![lost.clone()]).unwrap();
        let fingerprint = engine.fingerprint(topic_id).unwrap();
        assert_eq!(fingerprint.fingerprint, remote.fingerprint);
    });
    [cold, repeated, healed]
}

#[test]
#[ignore = "measurement, run explicitly"]
fn integrity_costs() {
    let params = format!("history=16384 lost=1 rounds={INTEGRITY_ROUNDS}");
    let run = |backend: &str, samples: Vec<[Sample; 3]>| {
        let mut parts: [Vec<Sample>; 3] = Default::default();
        for sample in samples {
            for (part, value) in parts.iter_mut().zip(sample) {
                part.push(value);
            }
        }
        let [cold, repeated, healed] = parts;
        let params = format!("backend={backend} {params}");
        report("integrity_cold", &params, cold);
        report("integrity_repeat", &params, repeated);
        report("integrity_heal", &params, healed);
    };
    let memory = (0..REPS)
        .map(|_| integrity_round(Counting::new(MemoryStorage::new())))
        .collect();
    run("memory", memory);
    let fjall = (0..REPS)
        .map(|_| {
            let dir = tempfile::tempdir().unwrap();
            let storage = FjallStorage::open_with_persist_mode(dir.path(), persist_mode()).unwrap();
            integrity_round(Counting::new(storage))
        })
        .collect();
    run("fjall", fjall);
}

/// Admitting 256 ops one by one into a healthy topic while another thread keeps
/// asking for the fingerprint of a damaged 16384-op topic in the same store.
fn healthy_service<S: Storage + PayloadReads + Corrupt>(storage: Counting<S>) -> Sample {
    let (author, reader) = (signer(13), signer(14).peer_id());
    let damaged = signed_chain(&author, "bench-damaged", &[reader], 16384, note);
    let healthy = signed_chain(&author, "bench-healthy", &[reader], 256, note);
    let damaged_id = damaged[0].signed.body.topic_id;
    load(&storage, &damaged);
    load(&storage, &healthy[..1]);
    fixture("healthy_service", &storage, std::iter::once(damaged_id));
    storage.inner.drop_op_record(&damaged[8192].id);
    let log = Oplog::with_storage(storage.clone());
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let asking = thread::spawn({
        let (log, stop) = (log.clone(), Arc::clone(&stop));
        move || {
            let engine = SyncEngine::new(log, author.peer_id());
            let mut answers = 0_u64;
            while !stop.load(Ordering::Relaxed) {
                engine.fingerprint(damaged_id).unwrap();
                answers += 1;
            }
            answers
        }
    });
    let before = storage.snapshot();
    let started = Instant::now();
    for op in &healthy[1..] {
        log.receive_ops(vec![op.clone()]).unwrap();
    }
    let ms = millis(started);
    stop.store(true, Ordering::Relaxed);
    let answers = asking.join().unwrap();
    let mut counters = vec![("fingerprints", answers)];
    counters.extend(read_delta(before, storage.snapshot()));
    Sample { ms, counters }
}

#[test]
#[ignore = "measurement, run explicitly"]
fn healthy_during_repair() {
    let params = "damaged=16384 lost=1 healthy_ops=256";
    each_backend("healthy_service", params, healthy_service, healthy_service);
}

/// A 4096-op topic with one record lost by `damage`: oplog A scans it, and a
/// separate oplog B repairs the record after A's scan completed, or between two of
/// A's steps with `overlap`. Timed and counted: A's next complete answer.
fn freshness_round<S: Storage + PayloadReads + Corrupt>(
    storage: Counting<S>,
    damage: Damage,
    overlap: bool,
) -> Sample {
    let (author, reader) = (signer(15), signer(16).peer_id());
    let ops = signed_chain(&author, "bench-freshness", &[reader], 4096, note);
    let topic_id = ops[0].signed.body.topic_id;
    load(&storage, &ops);
    fixture("freshness", &storage, std::iter::once(topic_id));
    let lost = ops[2048].clone();
    damage_op(&storage.inner, &lost.id, damage);
    let log = Oplog::with_storage(storage.clone());
    if overlap {
        log.set_step_reads(1024);
        storage
            .read_snapshot(|read| {
                let view = read.topic_view(&topic_id, None)?.unwrap();
                log.integrity_in(read, &view)
            })
            .unwrap();
    } else {
        assert!(!log.topic_unresolved(&topic_id).unwrap().is_empty());
    }
    Oplog::with_storage(storage.clone())
        .receive_ops(vec![lost])
        .unwrap();
    let before = storage.snapshot();
    let started = Instant::now();
    let unresolved = log.topic_unresolved(&topic_id).unwrap();
    let ms = millis(started);
    let mut counters = vec![("healed", u64::from(unresolved.is_empty()))];
    counters.extend(read_delta(before, storage.snapshot()));
    Sample { ms, counters }
}

#[test]
#[ignore = "measurement, run explicitly"]
fn freshness_costs() {
    for (damage, lost) in [(Damage::Op, "body"), (Damage::Meta, "meta")] {
        let params = format!("history=4096 lost={lost}");
        for (overlap, name) in [(false, "freshness_cross"), (true, "freshness_overlap")] {
            let round = |storage| freshness_round(storage, damage, overlap);
            each_backend(name, &params, round, |storage| {
                freshness_round(storage, damage, overlap)
            });
        }
    }
}
