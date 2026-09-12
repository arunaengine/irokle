pub(crate) use std::collections::BTreeSet;
pub(crate) use std::sync::{Arc, Barrier};
pub(crate) use std::thread;

pub(crate) use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub(crate) use crate::{
    ActorClock, ActorId, Ed25519Signer, Error, Event, EventEnvelope, Irokle, MemoryStorage,
    NodeConfig, Op, OpBody, OpId, PeerId, ReplicationPolicy, Signer, Storage, TopicConfig,
    TopicControl, TopicGenesis, TopicId, TopicPayload, WriteConcern, actor_id_for, history, net,
    node, oplog, sync,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Note {
    pub(crate) text: String,
}

impl Event for Note {
    const TYPE_ID: &'static str = "test.note";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Other;

impl Event for Other {
    const TYPE_ID: &'static str = "test.other";
}

/// Durable corruption a test can apply to a real store, so a regression starts
/// from records that are actually inconsistent rather than from a wrapper that
/// lies on reads.
pub(crate) trait Corrupt: Storage {
    fn drop_op_record(&self, id: &OpId);
    fn drop_meta_record(&self, id: &OpId);
    /// Leave an admitted op behind that no head reaches, the shape the pre-
    /// `reset_topic_and_admit` reset left when it removed a descendant's
    /// ancestry but not the descendant.
    fn orphan_op(&self, op: &Op, meta: &crate::storage::OpMeta);
}

/// Which record halves a test erases. `Both` leaves the topic, actor and child
/// indexes pointing at an op with no records at all, the shape an admitted
/// descendant with a lost dependency has.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Damage {
    Op,
    Meta,
    Both,
}

pub(crate) fn damage_op<S: Corrupt>(storage: &S, id: &OpId, damage: Damage) {
    match damage {
        Damage::Op => storage.drop_op_record(id),
        Damage::Meta => storage.drop_meta_record(id),
        Damage::Both => {
            storage.drop_op_record(id);
            storage.drop_meta_record(id);
        }
    }
    assert!(!storage.dep_resolvable(id).unwrap());
}

impl Corrupt for MemoryStorage {
    fn drop_op_record(&self, id: &OpId) {
        MemoryStorage::drop_op_record(self, id);
    }
    fn drop_meta_record(&self, id: &OpId) {
        MemoryStorage::drop_meta_record(self, id);
    }
    fn orphan_op(&self, op: &Op, meta: &crate::storage::OpMeta) {
        MemoryStorage::orphan_op(self, op, meta);
    }
}

#[cfg(feature = "fjall")]
impl Corrupt for crate::storage::FjallStorage {
    fn drop_op_record(&self, id: &OpId) {
        crate::storage::FjallStorage::drop_op_record(self, id);
    }
    fn drop_meta_record(&self, id: &OpId) {
        crate::storage::FjallStorage::drop_meta_record(self, id);
    }
    fn orphan_op(&self, op: &Op, meta: &crate::storage::OpMeta) {
        crate::storage::FjallStorage::orphan_op(self, op, meta);
    }
}

/// Meeting point a test arms inside a storage read so two calls provably
/// interleave without sleeping. The wait has a generous cap, so a partner that
/// never arrives fails the assertion instead of hanging the suite.
pub(crate) struct Rendezvous {
    parties: usize,
    arrived: std::sync::Mutex<usize>,
    signal: std::sync::Condvar,
}

impl Rendezvous {
    pub(crate) fn new(parties: usize) -> Self {
        Self {
            parties,
            arrived: std::sync::Mutex::new(0),
            signal: std::sync::Condvar::new(),
        }
    }

    pub(crate) fn meet(&self) {
        let mut arrived = self.arrived.lock().unwrap();
        *arrived += 1;
        if *arrived >= self.parties {
            self.signal.notify_all();
            return;
        }
        let _ = self.signal.wait_timeout_while(
            arrived,
            std::time::Duration::from_secs(60),
            |arrived| *arrived < self.parties,
        );
    }
}

/// One-shot pause a test arms at a storage read. The reader reports its
/// arrival and waits for release; every wait is capped, and the guard returned
/// by [`Gate::releaser`] releases on drop, so a failed assertion cannot leave a
/// reader parked.
#[derive(Default)]
pub(crate) struct Gate {
    state: std::sync::Mutex<(bool, bool)>,
    signal: std::sync::Condvar,
}

/// A gate and the read it waits at.
pub(crate) type ArmedGate = (GatePoint, Arc<Gate>);

/// Which storage read a [`Gate`] pauses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GatePoint {
    View(TopicId),
    Meta(OpId),
    PeerAck(PeerId),
}

impl Gate {
    /// Called by the reader: report arrival and wait for release.
    pub(crate) fn pass(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.signal.notify_all();
        let _ =
            self.signal
                .wait_timeout_while(state, std::time::Duration::from_secs(60), |state| !state.1);
    }

    /// Called by a party that finished without reaching the gate, so a waiter
    /// for arrival wakes up.
    pub(crate) fn skip(&self) {
        self.state.lock().unwrap().0 = true;
        self.signal.notify_all();
    }

    /// Wait until a reader arrived or [`Gate::skip`] ran.
    pub(crate) fn wait_arrival(&self) {
        let state = self.state.lock().unwrap();
        let _ =
            self.signal
                .wait_timeout_while(state, std::time::Duration::from_secs(60), |state| !state.0);
    }

    pub(crate) fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.signal.notify_all();
    }

    pub(crate) fn releaser(self: &Arc<Self>) -> GateRelease {
        GateRelease(Arc::clone(self))
    }
}

pub(crate) struct GateRelease(Arc<Gate>);

impl Drop for GateRelease {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// Storage wrapper that simulates the stale reads of a concurrent admission:
/// `get_op`/`actor_index` report "unknown" exactly once for ops in the
/// one-shot sets, so a duplicate slips past the batch dedup check and reaches
/// seq validation while the actor tip already covers it. Ops in
/// `mid_commit_ops` stay invisible to `get_op` permanently, modelling a
/// commit whose actor index/tip keys are visible before the op record. Writes
/// for a topic in `failed_writes` are rejected, standing in for a storage fault
/// that only affects one topic.
#[derive(Clone)]
pub(crate) struct StaleReadStorage {
    pub(crate) op_reads: Arc<std::sync::atomic::AtomicUsize>,
    pub(crate) inner: MemoryStorage,
    pub(crate) hidden_ops: Arc<std::sync::Mutex<BTreeSet<OpId>>>,
    pub(crate) hidden_index: Arc<std::sync::Mutex<BTreeSet<OpId>>>,
    pub(crate) mid_commit_ops: Arc<std::sync::Mutex<BTreeSet<OpId>>>,
    pub(crate) failed_writes: Arc<std::sync::Mutex<BTreeSet<TopicId>>>,
    pub(crate) failed_status: Arc<std::sync::Mutex<BTreeSet<TopicId>>>,
    pub(crate) obligation_gate: Arc<std::sync::Mutex<Option<Arc<Rendezvous>>>>,
    pub(crate) failed_heads: Arc<std::sync::Mutex<BTreeSet<TopicId>>>,
    pub(crate) read_gate: Arc<std::sync::Mutex<Option<ArmedGate>>>,
}

impl StaleReadStorage {
    pub(crate) fn new(inner: MemoryStorage) -> Self {
        Self {
            inner,
            op_reads: Arc::default(),
            hidden_ops: Arc::default(),
            hidden_index: Arc::default(),
            mid_commit_ops: Arc::default(),
            failed_writes: Arc::default(),
            failed_status: Arc::default(),
            obligation_gate: Arc::default(),
            failed_heads: Arc::default(),
            read_gate: Arc::default(),
        }
    }

    /// Pause the next read at `point` on `gate`, once.
    pub(crate) fn arm_read(&self, point: GatePoint, gate: Arc<Gate>) {
        *self.read_gate.lock().unwrap() = Some((point, gate));
    }

    /// Drop a gate no reader took, so the test's own reads pass freely.
    pub(crate) fn disarm_read(&self) {
        self.read_gate.lock().unwrap().take();
    }

    /// Wait at the armed gate if `point` is the armed read, disarming it.
    fn gate_read(&self, point: GatePoint) {
        let gate = {
            let mut armed = self.read_gate.lock().unwrap();
            match armed.as_ref() {
                Some((armed_point, _)) if *armed_point == point => armed.take(),
                _ => None,
            }
        };
        if let Some((_, gate)) = gate {
            gate.pass();
        }
    }

    /// Make every later head read of `topic_id` fail, standing in for one
    /// topic whose records cannot be read while the others are fine.
    pub(crate) fn fail_heads(&self, topic_id: TopicId) {
        self.failed_heads.lock().unwrap().insert(topic_id);
    }

    /// Reject every later status update for `topic_id`.
    pub(crate) fn fail_status(&self, topic_id: TopicId) {
        self.failed_status.lock().unwrap().insert(topic_id);
    }

    /// Make every later obligation read wait at `gate`.
    pub(crate) fn arm_gate(&self, gate: Arc<Rendezvous>) {
        *self.obligation_gate.lock().unwrap() = Some(gate);
    }

    #[cfg(feature = "iroh")]
    pub(crate) fn fail_writes(&self, topic_id: TopicId) {
        self.failed_writes.lock().unwrap().insert(topic_id);
    }
}

impl Storage for StaleReadStorage {
    fn put_admitted_batch(&self, batch: crate::storage::AdmittedBatch) -> Result<(), Error> {
        if self.failed_writes.lock().unwrap().contains(&batch.topic_id) {
            return Err(Error::Storage("injected admission write failure".into()));
        }
        self.inner.put_admitted_batch(batch)
    }
    fn get_op(&self, id: &OpId) -> Result<Option<Op>, Error> {
        self.op_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.mid_commit_ops.lock().unwrap().contains(id) {
            return Ok(None);
        }
        if self.hidden_ops.lock().unwrap().remove(id) {
            return Ok(None);
        }
        self.inner.get_op(id)
    }
    fn get_meta(&self, id: &OpId) -> Result<Option<crate::storage::OpMeta>, Error> {
        let meta = self.inner.get_meta(id);
        self.gate_read(GatePoint::Meta(*id));
        meta
    }
    fn list_ops(&self, topic_id: &TopicId) -> Result<Vec<Op>, Error> {
        self.inner.list_ops(topic_id)
    }
    fn list_op_ids(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>, Error> {
        self.inner.list_op_ids(topic_id)
    }
    fn heads(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>, Error> {
        if self.failed_heads.lock().unwrap().contains(topic_id) {
            return Err(Error::Storage("injected head read failure".into()));
        }
        self.inner.heads(topic_id)
    }
    fn children(&self, op_id: &OpId) -> Result<BTreeSet<OpId>, Error> {
        self.inner.children(op_id)
    }
    fn actor_tip(
        &self,
        topic_id: &TopicId,
        actor_id: &ActorId,
    ) -> Result<Option<(u64, OpId)>, Error> {
        self.inner.actor_tip(topic_id, actor_id)
    }
    fn actor_index(
        &self,
        topic_id: &TopicId,
        actor_id: &ActorId,
        seq: u64,
    ) -> Result<Option<OpId>, Error> {
        let existing = self.inner.actor_index(topic_id, actor_id, seq)?;
        if let Some(id) = existing
            && self.hidden_index.lock().unwrap().remove(&id)
        {
            return Ok(None);
        }
        Ok(existing)
    }
    fn actor_clock(&self, topic_id: &TopicId) -> Result<ActorClock, Error> {
        self.inner.actor_clock(topic_id)
    }
    fn topic_fingerprint(&self, topic_id: &TopicId) -> Result<[u8; 32], Error> {
        self.inner.topic_fingerprint(topic_id)
    }
    fn max_generation(&self, topic_id: &TopicId) -> Result<u64, Error> {
        self.inner.max_generation(topic_id)
    }
    fn topic_state(&self, topic_id: &TopicId) -> Result<Option<crate::storage::TopicState>, Error> {
        self.inner.topic_state(topic_id)
    }
    fn list_topics(&self) -> Result<Vec<crate::TopicInfo>, Error> {
        self.inner.list_topics()
    }
    fn topic_view(
        &self,
        topic_id: &TopicId,
        peer_id: Option<&PeerId>,
    ) -> Result<Option<crate::storage::TopicView>, Error> {
        let view = self.inner.topic_view(topic_id, peer_id);
        self.gate_read(GatePoint::View(*topic_id));
        view
    }
    fn peer_reached_op(&self, peer_id: &PeerId, op_id: &OpId) -> Result<bool, Error> {
        self.inner.peer_reached_op(peer_id, op_id)
    }
    fn peers_reached_op(&self, op_id: &OpId) -> Result<Vec<PeerId>, Error> {
        self.inner.peers_reached_op(op_id)
    }
    fn put_pending_op(
        &self,
        source_peer: PeerId,
        op: Op,
        meta: crate::storage::OpMeta,
    ) -> Result<(), Error> {
        self.inner.put_pending_op(source_peer, op, meta)
    }
    fn pending_waiters(&self, dep_id: &OpId) -> Result<Vec<(PeerId, Op)>, Error> {
        self.inner.pending_waiters(dep_id)
    }
    fn ready_pending_ops(&self) -> Result<Vec<(PeerId, Op)>, Error> {
        self.inner.ready_pending_ops()
    }
    fn pending_missing_deps(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>, Error> {
        self.inner.pending_missing_deps(topic_id)
    }
    fn remove_pending_op(&self, op_id: &OpId) -> Result<(), Error> {
        self.inner.remove_pending_op(op_id)
    }
    fn peer_ack(
        &self,
        peer_id: &PeerId,
        topic_id: &TopicId,
    ) -> Result<Option<crate::storage::PeerAck>, Error> {
        self.gate_read(GatePoint::PeerAck(*peer_id));
        self.inner.peer_ack(peer_id, topic_id)
    }
    fn peer_acks(&self, topic_id: &TopicId) -> Result<Vec<crate::storage::PeerAck>, Error> {
        self.inner.peer_acks(topic_id)
    }
    fn put_sync_obligation(
        &self,
        obligation: crate::storage::SyncObligation,
        expected_genesis: Option<OpId>,
    ) -> Result<(), Error> {
        self.inner.put_sync_obligation(obligation, expected_genesis)
    }
    fn all_sync_obligations(&self) -> Result<Vec<crate::storage::SyncObligation>, Error> {
        self.inner.all_sync_obligations()
    }
    fn apply_peer_ack(&self, ack: crate::storage::PeerAck) -> Result<usize, Error> {
        self.inner.apply_peer_ack(ack)
    }
    fn sync_obligations(
        &self,
        peer_id: &PeerId,
        topic_id: &TopicId,
    ) -> Result<Vec<crate::storage::SyncObligation>, Error> {
        let gate = self.obligation_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.meet();
        }
        self.inner.sync_obligations(peer_id, topic_id)
    }
    fn put_sync_status(&self, status: crate::storage::SyncPeerStatus) -> Result<(), Error> {
        self.inner.put_sync_status(status)
    }
    fn update_sync_status(
        &self,
        peer_id: &PeerId,
        topic_id: &TopicId,
        update: &crate::storage::SyncStatusUpdate,
    ) -> Result<crate::storage::SyncPeerStatus, Error> {
        if self.failed_status.lock().unwrap().contains(topic_id) {
            return Err(Error::Storage("injected status write failure".into()));
        }
        self.inner.update_sync_status(peer_id, topic_id, update)
    }
    fn topic_obligation_counts(
        &self,
        topic_id: &TopicId,
    ) -> Result<std::collections::BTreeMap<PeerId, usize>, Error> {
        self.inner.topic_obligation_counts(topic_id)
    }
    fn sync_statuses(
        &self,
        topic_id: &TopicId,
    ) -> Result<Vec<crate::storage::SyncPeerStatus>, Error> {
        self.inner.sync_statuses(topic_id)
    }
    fn clear_peer_sync_state(
        &self,
        peer_id: &PeerId,
        topic_id: &TopicId,
        expected_genesis: Option<OpId>,
    ) -> Result<usize, Error> {
        self.inner
            .clear_peer_sync_state(peer_id, topic_id, expected_genesis)
    }
    fn reset_topic(&self, topic_id: &TopicId) -> Result<usize, Error> {
        self.inner.reset_topic(topic_id)
    }
    fn reset_topic_and_admit(
        &self,
        topic_id: &TopicId,
        expected_topic_state: &crate::storage::TopicState,
        batch: crate::storage::AdmittedBatch,
        eviction: Option<&crate::TopicEviction>,
    ) -> Result<usize, Error> {
        self.inner
            .reset_topic_and_admit(topic_id, expected_topic_state, batch, eviction)
    }
    fn seal_topic(&self, topic_id: &TopicId) -> Result<bool, Error> {
        self.inner.seal_topic(topic_id)
    }
    fn unseal_topic(&self, topic_id: &TopicId) -> Result<bool, Error> {
        self.inner.unseal_topic(topic_id)
    }
    fn pending_evictions(&self) -> Result<Vec<crate::TopicEviction>, Error> {
        self.inner.pending_evictions()
    }
    fn clear_eviction(&self, key: &crate::EvictionKey) -> Result<(), Error> {
        self.inner.clear_eviction(key)
    }
    fn purge_pending_waiters(&self, dep_id: &OpId) -> Result<usize, Error> {
        self.inner.purge_pending_waiters(dep_id)
    }
    fn reject_pending_subtree(&self, op_id: &OpId) -> Result<usize, Error> {
        self.inner.reject_pending_subtree(op_id)
    }
}

/// A genesis plus one event for `topic_id`, authored in `storage` by `seed`'s
/// signer with `peers` as the other initial members. Two sides built for the
/// same topic id fork it, which is what genesis tie-break resolution decides.
pub(crate) fn forked_side<S: Storage>(
    storage: S,
    topic_id: TopicId,
    seed: u8,
    peers: impl IntoIterator<Item = PeerId>,
    text: &str,
) -> (oplog::Oplog<S>, Ed25519Signer, Op, Op) {
    let signer = Ed25519Signer::from_bytes(&[seed; 32]);
    let log = oplog::Oplog::with_storage(storage);
    let actor = actor_id_for(topic_id, signer.peer_id());
    let genesis = TopicGenesis {
        event_type_id: Note::TYPE_ID.into(),
        initial_peers: peers.into_iter().collect(),
        replication_policy: ReplicationPolicy::default(),
    };
    let genesis_op = log
        .create_topic_genesis(topic_id, actor, genesis, &signer)
        .unwrap();
    let event_op = log
        .create_event_op(
            topic_id,
            actor,
            EventEnvelope::encode_event(&Note { text: text.into() }).unwrap(),
            &signer,
        )
        .unwrap();
    (log, signer, genesis_op, event_op)
}

/// A source node holding a three-op chain that `holder_peer` may sync with.
pub(crate) fn chain_source(seed: u8, holder_peer: PeerId) -> (Irokle, TopicId, Vec<Op>) {
    let source = node(seed);
    let topic = source
        .create_topic::<Note>(TopicConfig {
            initial_peers: [holder_peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    topic.publish(Note { text: "one".into() }).unwrap();
    topic.publish(Note { text: "two".into() }).unwrap();
    let ops = oplog::topological(source.storage(), &topic.id()).unwrap();
    (source, topic.id(), ops)
}

/// A three-op chain seeded into `storage` with the middle op really damaged.
pub(crate) fn holed_store<S: Corrupt>(
    storage: &S,
    seed: u8,
    damage: Damage,
) -> (Irokle, TopicId, Vec<Op>) {
    let holder_peer = Ed25519Signer::from_bytes(&[seed.wrapping_add(1); 32]).peer_id();
    let (source, topic_id, ops) = chain_source(seed, holder_peer);
    oplog::Oplog::with_storage(storage.clone())
        .receive_ops(ops.clone())
        .unwrap();
    damage_op(storage, &ops[1].id, damage);
    (source, topic_id, ops)
}

/// Genesis of `topic_id` as `storage` currently holds it. Signed
/// acknowledgements name the incarnation they certify, so tests read it here
/// rather than repeating the topic-state lookup.
pub(crate) fn genesis_of<S: Storage>(storage: &S, topic_id: &TopicId) -> Option<OpId> {
    storage
        .topic_state(topic_id)
        .unwrap()
        .map(|state| state.genesis)
}

/// Whether `obligations` still require `op_id`, by repair id or clock position.
pub(crate) fn obligation_covers<S: Storage>(
    storage: &S,
    obligations: &[crate::storage::SyncObligation],
    op_id: &OpId,
) -> bool {
    let meta = storage.get_meta(op_id).unwrap();
    obligations
        .iter()
        .any(|obligation| match &obligation.target {
            crate::storage::ObligationTarget::Repair(ids) => ids.contains(op_id),
            crate::storage::ObligationTarget::Clock(clock) => meta.as_ref().is_some_and(|meta| {
                meta.topic_id == obligation.topic_id && clock.get(&meta.actor_id) >= meta.actor_seq
            }),
        })
}

pub(crate) fn node(seed: u8) -> Irokle {
    Irokle::new(NodeConfig {
        signer: Ed25519Signer::from_bytes(&[seed; 32]),
        default_write_concern: WriteConcern::Local,
        ..NodeConfig::default()
    })
    .unwrap()
}
