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

impl Corrupt for StaleReadStorage {
    fn drop_op_record(&self, id: &OpId) {
        self.inner.drop_op_record(id);
    }
    fn drop_meta_record(&self, id: &OpId) {
        self.inner.drop_meta_record(id);
    }
    fn orphan_op(&self, op: &Op, meta: &crate::storage::OpMeta) {
        self.inner.orphan_op(op, meta);
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

/// One-shot pause armed at a storage read. The reader reports arrival and waits for
/// release; waits are capped and the [`Gate::releaser`] guard releases on drop.
#[derive(Default)]
pub(crate) struct Gate {
    state: std::sync::Mutex<(bool, bool)>,
    signal: std::sync::Condvar,
    left: std::sync::atomic::AtomicBool,
}

/// A gate and the read it waits at.
pub(crate) type ArmedGate = (GatePoint, Arc<Gate>);

/// Which storage read a [`Gate`] pauses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GatePoint {
    View(TopicId),
    /// The first topic state or view read of the topic, whichever comes first.
    Topic(TopicId),
    Heads(TopicId),
    Meta(OpId),
    PeerAck(PeerId),
    Topics,
    /// An admission write of the topic, before it reaches the store.
    Admit(TopicId),
    /// A discard of a namespace of the topic, before it reaches the store.
    Discard(TopicId),
    Sync(TopicId, &'static str),
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
        self.left.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Whether a reader reached the gate.
    #[cfg(feature = "iroh")]
    pub(crate) fn arrived(&self) -> bool {
        self.state.lock().unwrap().0
    }

    /// Whether a reader went on past the gate, released or timed out.
    #[cfg(feature = "iroh")]
    pub(crate) fn has_left(&self) -> bool {
        self.left.load(std::sync::atomic::Ordering::SeqCst)
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

/// Whether a write can commit while a planner holds its snapshot: a store with
/// snapshot isolation lets it, a store behind one lock makes it wait.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Isolation {
    #[cfg_attr(not(feature = "fjall"), allow(dead_code))]
    Commits,
    Blocks,
}

/// Runs `plan` until it pauses at the `skip`-th later read of `point`, runs
/// `write` beside it, then lets the plan finish. Under [`Isolation::Commits`]
/// the write has committed before the plan resumes.
pub(crate) fn interleave<S: Storage, T: Send + 'static>(
    storage: &StaleReadStorage<S>,
    (point, skip): (GatePoint, usize),
    isolation: Isolation,
    plan: impl FnOnce() -> T + Send + 'static,
    write: impl FnOnce() + Send + 'static,
) -> T {
    let gate = Arc::new(Gate::default());
    let release = gate.releaser();
    storage.arm_read_after(point, skip, Arc::clone(&gate));
    let planning = thread::spawn({
        let gate = Arc::clone(&gate);
        move || {
            let planned = plan();
            gate.skip();
            planned
        }
    });
    gate.wait_arrival();
    assert!(!planning.is_finished(), "{point:?} was never paused");
    let started = Arc::new(Barrier::new(2));
    let writing = thread::spawn({
        let started = Arc::clone(&started);
        move || {
            started.wait();
            write();
        }
    });
    started.wait();
    let mut writing = Some(writing);
    if matches!(isolation, Isolation::Commits) {
        writing.take().unwrap().join().unwrap();
    }
    drop(release);
    let planned = planning.join().unwrap();
    if let Some(writing) = writing {
        writing.join().unwrap();
    }
    planned
}

pub(crate) struct AckFault {
    pub(crate) committed: bool,
    pub(crate) error: Error,
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
pub(crate) struct StaleReadStorage<S = MemoryStorage> {
    pub(crate) op_reads: Arc<std::sync::atomic::AtomicUsize>,
    pub(crate) ack_fault: Arc<std::sync::Mutex<Option<AckFault>>>,
    pub(crate) ack_calls: Arc<std::sync::atomic::AtomicUsize>,
    pub(crate) inner: S,
    pub(crate) hidden_ops: Arc<std::sync::Mutex<BTreeSet<OpId>>>,
    pub(crate) hidden_index: Arc<std::sync::Mutex<BTreeSet<OpId>>>,
    pub(crate) mid_commit_ops: Arc<std::sync::Mutex<BTreeSet<OpId>>>,
    pub(crate) failed_writes: Arc<std::sync::Mutex<BTreeSet<TopicId>>>,
    /// Admission writes holding any of these ops fail with a retryable error.
    pub(crate) failed_ops: Arc<std::sync::Mutex<BTreeSet<OpId>>>,
    pub(crate) failed_status: Arc<std::sync::Mutex<BTreeSet<TopicId>>>,
    pub(crate) obligation_gate: Arc<std::sync::Mutex<Option<Arc<Rendezvous>>>>,
    pub(crate) failed_heads: Arc<std::sync::Mutex<BTreeSet<TopicId>>>,
    pub(crate) read_gate: Arc<std::sync::Mutex<Option<ArmedGate>>>,
    /// Matching reads the armed gate lets pass before it pauses one.
    pub(crate) read_skips: Arc<std::sync::atomic::AtomicUsize>,
    /// Activations left to fail before one reaches the store.
    pub(crate) failed_activations: Arc<std::sync::atomic::AtomicUsize>,
    pub(crate) conflicts: Arc<std::sync::atomic::AtomicUsize>,
    /// Ops buffered as pending so far.
    pub(crate) pending_puts: Arc<std::sync::atomic::AtomicUsize>,
    pub(crate) sync_counts: Arc<std::sync::Mutex<std::collections::BTreeMap<&'static str, usize>>>,
}

impl<S: Storage> StaleReadStorage<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self {
            inner,
            op_reads: Arc::default(),
            ack_fault: Arc::default(),
            ack_calls: Arc::default(),
            hidden_ops: Arc::default(),
            hidden_index: Arc::default(),
            mid_commit_ops: Arc::default(),
            failed_writes: Arc::default(),
            failed_ops: Arc::default(),
            failed_status: Arc::default(),
            obligation_gate: Arc::default(),
            failed_heads: Arc::default(),
            read_gate: Arc::default(),
            read_skips: Arc::default(),
            failed_activations: Arc::default(),
            conflicts: Arc::default(),
            pending_puts: Arc::default(),
            sync_counts: Arc::default(),
        }
    }

    /// Pause the next read at `point` on `gate`, once.
    pub(crate) fn arm_read(&self, point: GatePoint, gate: Arc<Gate>) {
        self.arm_read_after(point, 0, gate);
    }

    /// Let `skip` matching reads pass, then pause the next one on `gate`.
    pub(crate) fn arm_read_after(&self, point: GatePoint, skip: usize, gate: Arc<Gate>) {
        let mut armed = self.read_gate.lock().unwrap();
        self.read_skips
            .store(skip, std::sync::atomic::Ordering::SeqCst);
        *armed = Some((point, gate));
    }

    /// Make the next `count` admission writes lose to a concurrent commit.
    pub(crate) fn conflict_writes(&self, count: usize) {
        self.conflicts
            .store(count, std::sync::atomic::Ordering::SeqCst);
    }

    /// Drop a gate no reader took, so the test's own reads pass freely.
    pub(crate) fn disarm_read(&self) {
        self.read_gate.lock().unwrap().take();
    }

    /// Wait at the armed gate if `point` is the armed read, disarming it.
    fn gate_read(&self, point: GatePoint) {
        let gate = {
            let mut armed = self.read_gate.lock().unwrap();
            let topic = match point {
                GatePoint::View(topic_id) | GatePoint::Topic(topic_id) => Some(topic_id),
                _ => None,
            };
            match armed.as_ref() {
                Some((armed_point, _))
                    if *armed_point == point
                        || topic
                            .is_some_and(|topic_id| *armed_point == GatePoint::Topic(topic_id)) =>
                {
                    let skipped = self.read_skips.try_update(
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                        |left| left.checked_sub(1),
                    );
                    if skipped.is_ok() { None } else { armed.take() }
                }
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

/// A snapshot of the wrapped store with the wrapper's read hooks applied.
struct StaleSnapshot<'a, S> {
    read: &'a dyn crate::storage::SnapshotRead,
    hooks: &'a StaleReadStorage<S>,
}

impl<S: Storage> crate::storage::SnapshotRead for StaleSnapshot<'_, S> {
    fn topic_view(
        &self,
        topic_id: &TopicId,
        peer_id: Option<&PeerId>,
    ) -> Result<Option<crate::storage::TopicView>, Error> {
        let view = self.read.topic_view(topic_id, peer_id);
        self.hooks.gate_read(GatePoint::View(*topic_id));
        view
    }
    fn get_op(&self, id: &OpId) -> Result<Option<Op>, Error> {
        if let Some(hidden) = self.hooks.op_hook(id) {
            return Ok(hidden);
        }
        self.read.get_op(id)
    }
    fn get_meta(&self, id: &OpId) -> Result<Option<crate::storage::OpMeta>, Error> {
        let meta = self.read.get_meta(id);
        self.hooks.gate_read(GatePoint::Meta(*id));
        meta
    }
    fn dep_resolvable(&self, id: &OpId) -> Result<bool, Error> {
        self.read.dep_resolvable(id)
    }
    fn actor_range(
        &self,
        topic_id: &TopicId,
        actor_id: &ActorId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, OpId)>, Error> {
        self.read.actor_range(topic_id, actor_id, after, limit)
    }
    fn list_op_ids(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>, Error> {
        self.read.list_op_ids(topic_id)
    }
}

impl<S: Storage> StaleReadStorage<S> {
    /// The injected answer of an op read, or `None` to read the store.
    fn op_hook(&self, id: &OpId) -> Option<Option<Op>> {
        self.op_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let hidden = self.mid_commit_ops.lock().unwrap().contains(id)
            || self.hidden_ops.lock().unwrap().remove(id);
        hidden.then_some(None)
    }
}

impl<S: Storage> Storage for StaleReadStorage<S> {
    fn sync_boundary(&self, topic_id: TopicId, boundary: &'static str) {
        *self
            .sync_counts
            .lock()
            .unwrap()
            .entry(boundary)
            .or_default() += 1;
        self.gate_read(GatePoint::Sync(topic_id, boundary));
    }
    fn read_snapshot<R>(
        &self,
        read: impl FnOnce(&dyn crate::storage::SnapshotRead) -> Result<R, Error>,
    ) -> Result<R, Error> {
        self.inner.read_snapshot(|inner| {
            read(&StaleSnapshot {
                read: inner,
                hooks: self,
            })
        })
    }
    fn put_admitted_batch(&self, batch: crate::storage::AdmittedBatch) -> Result<(), Error> {
        self.gate_read(GatePoint::Admit(batch.topic_id));
        if self.failed_writes.lock().unwrap().contains(&batch.topic_id) {
            return Err(Error::Storage("injected admission write failure".into()));
        }
        let failed_ops = self.failed_ops.lock().unwrap();
        if batch
            .entries
            .iter()
            .any(|(op, _)| failed_ops.contains(&op.id))
        {
            return Err(Error::Storage("injected op write failure".into()));
        }
        drop(failed_ops);
        let lost = self.conflicts.try_update(
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
            |left| left.checked_sub(1),
        );
        if lost.is_ok() {
            return Err(Error::AdmissionConflict);
        }
        self.inner.put_admitted_batch(batch)
    }
    fn get_op(&self, id: &OpId) -> Result<Option<Op>, Error> {
        if let Some(hidden) = self.op_hook(id) {
            return Ok(hidden);
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
        let heads = self.inner.heads(topic_id);
        self.gate_read(GatePoint::Heads(*topic_id));
        heads
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
    fn actor_range(
        &self,
        topic_id: &TopicId,
        actor_id: &ActorId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, OpId)>, Error> {
        self.inner.actor_range(topic_id, actor_id, after, limit)
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
        let state = self.inner.topic_state(topic_id);
        self.gate_read(GatePoint::Topic(*topic_id));
        state
    }
    fn list_topics(&self) -> Result<Vec<crate::TopicInfo>, Error> {
        self.gate_read(GatePoint::Topics);
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
        self.pending_puts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.put_pending_op(source_peer, op, meta)
    }
    fn put_pending_bound(
        &self,
        source_peer: PeerId,
        op: Op,
        meta: crate::storage::OpMeta,
        genesis: Option<OpId>,
    ) -> Result<(), Error> {
        self.sync_boundary(meta.topic_id, "pending");
        self.pending_puts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.put_pending_bound(source_peer, op, meta, genesis)
    }
    fn pending_waiters(&self, dep_id: &OpId) -> Result<Vec<(PeerId, Op)>, Error> {
        self.inner.pending_waiters(dep_id)
    }
    fn ready_pending_after(
        &self,
        after: Option<&OpId>,
        limit: usize,
    ) -> Result<Vec<(PeerId, Op)>, Error> {
        self.inner.ready_pending_after(after, limit)
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
        self.ack_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let fault = self.ack_fault.lock().unwrap().take();
        if let Some(fault) = fault {
            if fault.committed {
                self.inner.apply_peer_ack(ack)?;
            }
            return Err(fault.error);
        }
        self.inner.apply_peer_ack(ack)
    }
    fn apply_peer_acks(
        &self,
        acks: Vec<crate::storage::PeerAck>,
    ) -> Result<Vec<Result<usize, Error>>, Error> {
        self.ack_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let fault = self.ack_fault.lock().unwrap().take();
        if let Some(fault) = fault {
            if fault.committed {
                self.inner.apply_peer_acks(acks)?;
            }
            return Err(fault.error);
        }
        self.inner.apply_peer_acks(acks)
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
    fn next_attempt_epoch(&self) -> Result<u64, Error> {
        self.inner.next_attempt_epoch()
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
        let lost = self.conflicts.try_update(
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
            |left| left.checked_sub(1),
        );
        if lost.is_ok() {
            return Err(Error::AdmissionConflict);
        }
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
    fn staging_limits(&self) -> crate::storage::StagingLimits {
        self.inner.staging_limits()
    }
    fn provisional_topics(&self) -> Result<Vec<crate::storage::ProvisionalTopic>, Error> {
        self.inner.provisional_topics()
    }
    fn open_provisional(
        &self,
        source: PeerId,
        topic_id: TopicId,
        genesis: OpId,
        now_ms: u64,
    ) -> Result<crate::storage::ProvisionalTopic, Error> {
        self.inner
            .open_provisional(source, topic_id, genesis, now_ms)
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
                ..self.clone()
            }))
    }
    fn stored_bytes(&self) -> Result<u64, Error> {
        self.inner.stored_bytes()
    }
    fn touch_provisional(
        &self,
        provisional: &crate::storage::ProvisionalTopic,
        now_ms: u64,
    ) -> Result<(), Error> {
        self.inner.touch_provisional(provisional, now_ms)
    }
    fn activate_provisional(
        &self,
        provisional: &crate::storage::ProvisionalTopic,
        expected: &crate::storage::TopicState,
        effects: crate::storage::AdmissionEffects,
    ) -> Result<(), Error> {
        self.sync_boundary(provisional.topic_id, "activation");
        if self
            .failed_activations
            .try_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |left| left.checked_sub(1),
            )
            .is_ok()
        {
            return Err(Error::Storage("injected activation failure".into()));
        }
        self.inner
            .activate_provisional(provisional, expected, effects)
    }
    fn discard_provisional(
        &self,
        provisional: &crate::storage::ProvisionalTopic,
    ) -> Result<bool, Error> {
        self.gate_read(GatePoint::Discard(provisional.topic_id));
        self.inner.discard_provisional(provisional)
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
