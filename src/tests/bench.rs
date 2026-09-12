//! Core cost measurements without a transport. Run explicitly and serially:
//! `cargo test --features fjall,iroh --lib tests::bench -- --ignored --nocapture --test-threads=1`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use super::support::*;
use crate::oplog::Oplog;
use crate::storage::{
    AdmissionEffects, AdmittedBatch, FjallStorage, OpMeta, PeerAck, SyncObligation,
    SyncStatusUpdate, TopicState, TopicView,
};
use crate::sync::{SyncData, SyncEngine};
use crate::{EvictionKey, SyncPeerStatus, TopicEviction, TopicInfo};

const REPS: usize = 3;

/// Storage reads seen at the `Storage` trait boundary, the same definition on
/// every revision measured.
#[derive(Default)]
struct Reads {
    ops: AtomicU64,
    metas: AtomicU64,
    index: AtomicU64,
    walks: AtomicU64,
}

#[derive(Clone)]
struct Counting<S> {
    inner: S,
    reads: Arc<Reads>,
}

impl<S> Counting<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            reads: Arc::default(),
        }
    }

    fn snapshot(&self) -> [u64; 4] {
        let reads = &self.reads;
        [&reads.ops, &reads.metas, &reads.index, &reads.walks].map(|c| c.load(Ordering::Relaxed))
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

impl<S: Storage> Storage for Counting<S> {
    fn get_op(&self, id: &OpId) -> Result<Option<Op>, Error> {
        add(&self.reads.ops, 1);
        self.inner.get_op(id)
    }
    fn get_meta(&self, id: &OpId) -> Result<Option<OpMeta>, Error> {
        add(&self.reads.metas, 1);
        self.inner.get_meta(id)
    }
    fn actor_index(
        &self,
        topic: &TopicId,
        actor: &ActorId,
        seq: u64,
    ) -> Result<Option<OpId>, Error> {
        add(&self.reads.index, 1);
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
        add(&self.reads.index, range.len());
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
    }
}

/// Buffered payloads a backend decoded, where the revision can count them.
trait PayloadReads {
    fn payload_reads(&self) -> Option<u64>;
}

impl PayloadReads for MemoryStorage {
    fn payload_reads(&self) -> Option<u64> {
        Some(self.counters().pending_payload_reads)
    }
}

impl PayloadReads for FjallStorage {
    fn payload_reads(&self) -> Option<u64> {
        Some(self.counters().pending_payload_reads)
    }
}

struct Sample {
    ms: f64,
    counters: Vec<(&'static str, u64)>,
}

fn millis(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

/// Reads between two snapshots, named for the report line.
fn read_delta(before: [u64; 4], after: [u64; 4]) -> Vec<(&'static str, u64)> {
    ["op_reads", "meta_reads", "index_reads", "walk_entries"]
        .into_iter()
        .zip(after.iter().zip(before).map(|(a, b)| a - b))
        .collect()
}

fn report(name: &str, params: &str, samples: Vec<Sample>) {
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
            fjall(Counting::new(FjallStorage::open(dir.path()).unwrap()))
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
fn signed_chain(
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

/// Local publishing with one replication target per peer per publish, as the
/// facade's `AsyncReplication` admission effects write them.
fn publish<S: Storage>(storage: Counting<S>, len: usize) -> Sample {
    let owner = signer(11);
    let peers = [signer(12).peer_id(), signer(13).peer_id()];
    let topic = TopicId::hash("bench-publish");
    let actor = actor_id_for(topic, owner.peer_id());
    let log = Oplog::with_storage(storage.clone());
    let genesis = TopicGenesis::new(Note::TYPE_ID, peers);
    log.create_topic_genesis(topic, actor, genesis, &owner)
        .unwrap();
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
    let rows = storage.all_sync_obligations().unwrap().len() as u64;
    Sample {
        ms,
        counters: vec![("rows", rows)],
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
fn forward_backlog<S: Storage>(storage: Counting<S>, len: usize) -> Sample {
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
    let started = Instant::now();
    for page in ops.chunks(256) {
        let data = SyncData {
            topic_id,
            ops: page.to_vec(),
        };
        node.receive_sync_data_from(source.peer_id(), data).unwrap();
    }
    let ms = millis(started);
    let rows = storage.all_sync_obligations().unwrap().len() as u64;
    Sample {
        ms,
        counters: vec![("rows", rows)],
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
    Sample { ms, counters }
}

#[test]
#[ignore = "measurement, run explicitly"]
fn unrelated_pool() {
    let params = "pending=2000 admissions=100";
    each_backend("pending_pool", params, pending_pool, pending_pool);
}
