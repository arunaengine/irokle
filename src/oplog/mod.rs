// SPDX-License-Identifier: MIT OR Apache-2.0
//! Operation-log admission, DAG validation, and topic-state materialization.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::storage::{
    AdmissionEffects, AdmittedBatch, MAX_PENDING_MISSING_DEPS, MemoryStorage, OpMeta, Storage,
    TopicState, TopicView,
};
use crate::{
    ActorId, Error, EventEnvelope, EvictionKey, Op, OpBody, OpId, PeerId, Result, SignedOp, Signer,
    TopicControl, TopicGenesis, TopicId, TopicPayload, actor_id_for,
};

mod helpers;
mod topology;

use helpers::{
    apply_control_to_state, checked_next, ensure_event_type, heads_after, is_local_admission_race,
    is_permanent_rejection, materialize_topic_state, merge_states, next_actor_position,
    pending_meta_for,
};
pub(crate) use topology::topological_ids;
pub(crate) use topology::topological_subset_entries;
use topology::{complete_ops, topological_ops};
pub use topology::{topological, topological_subset};

/// Attempts one admission job makes; storage writes on this path try once each.
pub(crate) const MAX_ADMISSION_RETRIES: usize = 64;
/// Buffered ops one admission call attempts, and how many it reads at once.
const MAX_DRAIN_OPS: usize = 4096;
const READY_SLICE: usize = 256;
const MAX_CACHED_PROJECTIONS: usize = 4096;
/// Views a whole-topic check reads before it gives up certifying one; each
/// retry means a reset committed during the hole scan.
const MAX_VIEW_ATTEMPTS: usize = 4;

#[derive(Default)]
struct MembershipCache {
    epoch: Arc<()>,
    states: BTreeMap<OpId, Arc<TopicState>>,
    order: VecDeque<OpId>,
}

enum OpAdmission {
    Admit,
    Duplicate,
}

/// What happens to a buffered op whose admission failed.
enum PendingVerdict {
    /// The failure is a property of immutable records: drop the op's subtree.
    Reject,
    /// A later arrival can still resolve it: keep the record.
    Retain,
}

/// How much of an op the local store already holds. `Repair` is an id the local
/// chain already accounts for while its records are missing or half written:
/// refilling it is not an append, so the actor-position checks that guard a new
/// op do not apply to it.
enum StoredOp {
    Absent,
    Repair,
    Complete,
}

type GenesisResolution = (Vec<Op>, Option<ResetPlan>, Option<OpId>);

/// Effects a received batch must commit together with its ops, computed from
/// the batch's source, its entries and the topic state it produces.
pub(crate) type ReceiveEffects<'a> =
    &'a dyn Fn(Option<PeerId>, &[(Op, OpMeta)], &TopicState) -> Result<AdmissionEffects>;

/// The reset an admission must fold into its own storage transaction: the state
/// that must still be current for it to proceed, and the record of the payloads
/// it discards. Pairing them keeps the record inseparable from the removal.
struct ResetPlan {
    expected_state: TopicState,
    eviction: TopicEviction,
}

/// The rebuild a quarantine is about to run: the topic state it must still
/// find, the ops that stay, and the payloads the caller may re-emit.
struct QuarantinePlan {
    state: TopicState,
    survivors: Vec<Op>,
    evicted: Vec<EvictedOp>,
}

/// The batch-local view admission validates against: the entries this batch has
/// already accepted, plus whether the topic is being reset. A reset removes the
/// stored actor slots and tips of the topic in the same transaction, so they
/// must not count towards the position of an op the batch installs.
struct BatchOverlay<'a> {
    ops: &'a BTreeMap<OpId, Op>,
    meta: &'a BTreeMap<OpId, OpMeta>,
    tips: &'a BTreeMap<(TopicId, ActorId), (u64, OpId)>,
    index: &'a BTreeMap<(TopicId, ActorId, u64), OpId>,
    reset: bool,
}

/// A validated admission not yet committed, with what follows its commit.
struct BuiltBatch {
    accepted: BTreeSet<OpId>,
    batch: AdmittedBatch,
    pending: Vec<(Op, BTreeSet<OpId>)>,
    projection_epoch: Arc<()>,
    projections: BTreeMap<OpId, Arc<TopicState>>,
    projection_tips: BTreeSet<OpId>,
}

/// A single op removed from the losing side of a genesis collision, carrying
/// enough for the application to re-emit it under the winning genesis.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvictedOp {
    pub op_id: OpId,
    pub actor_id: ActorId,
    pub author: PeerId,
    pub actor_seq: u64,
    pub payload: TopicPayload,
}

/// Reports ops discarded from a topic's local chain. A genesis tie-break sets
/// `losing_genesis` to the replaced chain's genesis and `winning_genesis` to the
/// foreign one that took its place; a quarantine of ops no head reaches has no
/// second genesis to name, so both fields carry the surviving genesis and equal
/// fields are what tells the two apart. `evicted` holds the discarded
/// non-genesis payloads ordered by `(actor_id, actor_seq)`; re-emission is the
/// embedder's responsibility.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicEviction {
    pub topic_id: TopicId,
    pub losing_genesis: OpId,
    pub winning_genesis: OpId,
    pub evicted: Vec<EvictedOp>,
}

impl TopicEviction {
    /// Identity of this eviction's durable journal record, derived from its
    /// content. The same discarded chain always names the same record, so
    /// repeating the write, the delivery, or the recovery cannot multiply
    /// entries, and a consumer can acknowledge a record from the eviction alone.
    pub fn key(&self) -> EvictionKey {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.topic_id.as_ref());
        hasher.update(self.losing_genesis.as_ref());
        hasher.update(self.winning_genesis.as_ref());
        for evicted in &self.evicted {
            hasher.update(evicted.op_id.as_ref());
        }
        EvictionKey::from_bytes(*hasher.finalize().as_bytes())
    }
}

/// Outcome of an admission pass: the accepted op ids plus any topic evictions
/// produced by genesis tie-break resolution.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Admitted {
    pub accepted: BTreeSet<OpId>,
    pub evictions: Vec<TopicEviction>,
    /// Buffered ops that became ready were left for a later pass because this
    /// one reached its work limit; [`Oplog::reconcile_pending_ops`] drains them.
    pub ready_remaining: bool,
}

/// Spread retries of writers that keep losing the same optimistic race, with
/// a short random pause that grows with the attempt. No lock is held here.
fn conflict_pause(attempt: usize) {
    if attempt < 4 {
        return;
    }
    let mut jitter = [0_u8; 8];
    let _ = getrandom::fill(&mut jitter);
    let ceiling = 200_u64 << (attempt - 4).min(5);
    let micros = u64::from_le_bytes(jitter) % ceiling;
    std::thread::sleep(std::time::Duration::from_micros(micros));
}

fn is_structural_genesis(op: &Op) -> bool {
    let body = &op.signed.body;
    matches!(body.payload, TopicPayload::Genesis(_))
        && body.actor_seq == 1
        && body.actor_prev.is_none()
        && body.deps.is_empty()
}

fn without_descendants(ops: Vec<Op>, rejected: OpId) -> Vec<Op> {
    let mut children = BTreeMap::<OpId, Vec<OpId>>::new();
    for op in &ops {
        for dep in &op.signed.body.deps {
            children.entry(*dep).or_default().push(op.id);
        }
    }
    let mut rejected_ids = BTreeSet::from([rejected]);
    let mut pending = VecDeque::from([rejected]);
    while let Some(id) = pending.pop_front() {
        for child in children.remove(&id).unwrap_or_default() {
            if rejected_ids.insert(child) {
                pending.push_back(child);
            }
        }
    }
    ops.into_iter()
        .filter(|op| !rejected_ids.contains(&op.id))
        .collect()
}

fn admission_failure(mut admitted: Admitted, error: Error) -> Error {
    let source = match error {
        Error::AdmissionCommitted {
            admitted: partial,
            source,
        } => {
            admitted.accepted.extend(partial.accepted);
            admitted.evictions.extend(partial.evictions);
            source
        }
        error => Box::new(error),
    };
    if admitted.accepted.is_empty() && admitted.evictions.is_empty() {
        *source
    } else {
        Error::AdmissionCommitted {
            admitted: Box::new(admitted),
            source,
        }
    }
}

#[derive(Clone)]
pub struct Oplog<S = MemoryStorage> {
    storage: S,
    // Branch and data epoch each topic was scanned and found whole at. Admission
    // keeps that invariant, so only a reset or damage from outside irokle can
    // reintroduce a hole; scanning once per epoch keeps sync off a full scan.
    whole_topics: Arc<Mutex<BTreeMap<TopicId, (OpId, u64)>>>,
    membership_cache: Arc<Mutex<MembershipCache>>,
}

impl Default for Oplog<MemoryStorage> {
    fn default() -> Self {
        Self::new()
    }
}

impl Oplog<MemoryStorage> {
    pub fn new() -> Self {
        Self::with_storage(MemoryStorage::new())
    }
}

impl<S: Storage> Oplog<S> {
    pub fn with_storage(storage: S) -> Self {
        Self {
            storage,
            whole_topics: Arc::new(Mutex::new(BTreeMap::new())),
            membership_cache: Arc::new(Mutex::new(MembershipCache::default())),
        }
    }
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Ids this topic references but cannot resolve: admitted ops whose own
    /// records are incomplete, dependencies of admitted ops that are not fully
    /// stored, and the holes buffered ops are still waiting for. An empty set
    /// means every admitted op is locally usable, which is what lets sync
    /// certify the topic; anything else is turned into concrete repair wants.
    pub fn topic_unresolved(&self, topic_id: &TopicId) -> Result<BTreeSet<crate::OpId>> {
        match self.storage.topic_view(topic_id, None)? {
            Some(view) => Ok(self.view_unresolved(&view)?.0),
            None => {
                let mut unresolved = self.storage.pending_missing_deps(topic_id)?;
                unresolved.extend(self.scan_stored_holes(topic_id)?);
                Ok(unresolved)
            }
        }
    }

    /// Ids `view`'s topic cannot resolve, and whether a hole scan ran. A scan
    /// is not part of the view, so its whole verdict is recorded only under the
    /// view's branch and epoch, where a later reset cannot reuse it.
    pub(crate) fn view_unresolved(&self, view: &TopicView) -> Result<(BTreeSet<OpId>, bool)> {
        let topic_id = view.state.topic_id;
        let key = (view.state.genesis, view.epoch);
        let mut unresolved = view.pending_missing.clone();
        if self.whole_topics()?.get(&topic_id) == Some(&key) {
            return Ok((unresolved, false));
        }
        let holes = self.scan_stored_holes(&topic_id)?;
        if holes.is_empty() {
            self.whole_topics()?.insert(topic_id, key);
        }
        unresolved.extend(holes);
        Ok((unresolved, true))
    }

    /// A view of the topic and whether it is whole: only a verdict recorded for
    /// this branch and epoch with no scan in between counts, so a reset during
    /// a scan cannot lend the verdict to the frontier being certified.
    pub(crate) fn whole_view(&self, topic_id: &TopicId) -> Result<Option<(TopicView, bool)>> {
        let mut last = None;
        for _ in 0..MAX_VIEW_ATTEMPTS {
            let Some(view) = self.storage.topic_view(topic_id, None)? else {
                return Ok(None);
            };
            let (unresolved, scanned) = self.view_unresolved(&view)?;
            if !unresolved.is_empty() {
                return Ok(Some((view, false)));
            }
            if !scanned {
                return Ok(Some((view, true)));
            }
            last = Some(view);
        }
        Ok(last.map(|view| (view, false)))
    }

    fn scan_stored_holes(&self, topic_id: &TopicId) -> Result<BTreeSet<crate::OpId>> {
        let mut holes = BTreeSet::new();
        for id in self.storage.list_op_ids(topic_id)? {
            let Some(meta) = self.storage.get_meta(&id)? else {
                holes.insert(id);
                continue;
            };
            if self.storage.get_op(&id)?.is_none() {
                holes.insert(id);
            }
            for dep in &meta.deps {
                if !self.storage.dep_resolvable(dep)? {
                    holes.insert(*dep);
                }
            }
        }
        Ok(holes)
    }

    /// Drop the record of which topics were found whole, so the next integrity
    /// question audits the stored records again. Admission cannot introduce a
    /// hole, but damage from outside irokle can, and nothing else would ever
    /// ask a second time.
    pub fn recheck_topics(&self) -> Result<()> {
        self.whole_topics()?.clear();
        *self.membership_cache()? = MembershipCache::default();
        Ok(())
    }

    /// Ops the topic stores that no head reaches. Admission puts every new op
    /// into `heads` and takes it out only when a later op names it as a
    /// dependency, so the head closure covers exactly the ops the local chain
    /// accounts for. Anything outside it was never part of this lineage's
    /// frontier and its ancestry cannot be validated against the current
    /// genesis: the pre-`reset_topic_and_admit` genesis reset could leave such
    /// a descendant behind after removing the losing chain it stood on.
    fn topic_orphans(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>> {
        let mut reachable = BTreeSet::new();
        let mut frontier = self
            .storage
            .heads(topic_id)?
            .into_iter()
            .collect::<Vec<_>>();
        while let Some(id) = frontier.pop() {
            if !reachable.insert(id) {
                continue;
            }
            // A head-reachable id with no metadata is an ordinary hole to
            // repair, not an orphan; it still counts as accounted for.
            let Some(meta) = self.storage.get_meta(&id)? else {
                continue;
            };
            frontier.extend(meta.deps);
        }
        let mut orphans = self.storage.list_op_ids(topic_id)?;
        orphans.retain(|id| !reachable.contains(id));
        Ok(orphans)
    }

    /// Discard the ops no head reaches and rebuild the topic from the ops that
    /// remain, in the single transaction genesis adoption already uses. The
    /// survivors are re-validated and re-admitted from the genesis up, so heads,
    /// clock, actor indexes, generation and fingerprint come back agreeing with
    /// one current-genesis DAG, and acks and obligations naming the old frontier
    /// are dropped rather than carried over. Returns the discarded payloads for
    /// re-emission, or `None` when there is nothing to quarantine.
    pub fn quarantine_orphans(&self, topic_id: &TopicId) -> Result<Option<TopicEviction>> {
        if self.topic_orphans(topic_id)?.is_empty() {
            return Ok(None);
        }
        // The rebuild commits only if the topic still matches the planned
        // state, so a concurrent tie-break or append makes it replan.
        for attempt in 0..MAX_ADMISSION_RETRIES {
            conflict_pause(attempt);
            let Some(plan) = self.plan_quarantine(topic_id)? else {
                return Ok(None);
            };
            let QuarantinePlan {
                state,
                survivors,
                evicted,
            } = plan;
            let eviction = TopicEviction {
                topic_id: *topic_id,
                losing_genesis: state.genesis,
                winning_genesis: state.genesis,
                evicted,
            };
            let reset = ResetPlan {
                expected_state: state,
                eviction: eviction.clone(),
            };
            match self.admit_ops_batch(None, survivors, &BTreeSet::new(), Some(reset), None) {
                Err(Error::AdmissionConflict) => continue,
                Err(err) => return Err(err),
                Ok(_) => {}
            }
            self.whole_topics()?.remove(topic_id);
            tracing::warn!(
                %topic_id,
                genesis = %eviction.winning_genesis,
                quarantined = eviction.evicted.len(),
                "quarantined ops no head reaches and rebuilt the topic"
            );
            return Ok(Some(eviction));
        }
        Err(Error::AdmissionConflict)
    }

    /// Split the topic's stored ops into the head closure and the orphans.
    /// Refuses while the head closure itself has a hole: that is ordinary
    /// repair work sync must finish first, and a rebuild would drop the whole
    /// branch standing on it. Refuses too when the closure does not reach the
    /// recorded genesis, since then there is no chain left to rebuild from.
    fn plan_quarantine(&self, topic_id: &TopicId) -> Result<Option<QuarantinePlan>> {
        let Some(state) = self.storage.topic_state(topic_id)? else {
            return Ok(None);
        };
        let orphans = self.topic_orphans(topic_id)?;
        if orphans.is_empty() {
            return Ok(None);
        }
        let mut survivors = Vec::new();
        for id in self.storage.list_op_ids(topic_id)? {
            if orphans.contains(&id) {
                continue;
            }
            let Some(op) = self.storage.get_op(&id)? else {
                tracing::warn!(%topic_id, %id, "deferred quarantine: repair the frontier first");
                return Ok(None);
            };
            if self.storage.get_meta(&id)?.is_none() {
                tracing::warn!(%topic_id, %id, "deferred quarantine: repair the frontier first");
                return Ok(None);
            }
            survivors.push(op);
        }
        if !survivors.iter().any(|op| op.id == state.genesis) {
            tracing::warn!(
                %topic_id,
                genesis = %state.genesis,
                "refused quarantine: no head reaches the recorded genesis"
            );
            return Ok(None);
        }
        Ok(Some(QuarantinePlan {
            evicted: self.evicted_ops(&orphans)?,
            state,
            survivors,
        }))
    }

    fn whole_topics(&self) -> Result<std::sync::MutexGuard<'_, BTreeMap<TopicId, (OpId, u64)>>> {
        self.whole_topics
            .lock()
            .map_err(|_| Error::Storage("topic integrity cache lock poisoned".into()))
    }

    fn membership_cache(&self) -> Result<std::sync::MutexGuard<'_, MembershipCache>> {
        self.membership_cache
            .lock()
            .map_err(|_| Error::Storage("membership cache lock poisoned".into()))
    }

    pub fn create_topic_genesis(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        genesis: TopicGenesis,
        signer: &impl Signer,
    ) -> Result<Op> {
        self.create_topic_effects(topic_id, actor_id, genesis, signer, |_, _, _| {
            Ok(AdmissionEffects::default())
        })
    }

    pub(crate) fn create_topic_effects<F>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        genesis: TopicGenesis,
        signer: &impl Signer,
        effects: F,
    ) -> Result<Op>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        let mut peers = genesis.initial_peers.clone();
        peers.insert(signer.peer_id());
        let genesis = TopicGenesis {
            initial_peers: peers,
            ..genesis
        };
        self.create_local_effects(
            topic_id,
            actor_id,
            TopicPayload::Genesis(genesis),
            signer,
            effects,
        )
        .map(|(op, _)| op)
    }

    /// Create a topic genesis op plus its first event op and admit both in a
    /// single storage transaction. The event op chains off the genesis
    /// (actor_seq 2, actor_prev/deps = genesis op). Returns `(genesis, event)`.
    /// Fails with [`Error::InvalidGenesis`] if the topic already exists, same
    /// as [`Self::create_topic_genesis`].
    pub fn create_topic_genesis_with_event(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        genesis: TopicGenesis,
        event: EventEnvelope,
        signer: &impl Signer,
    ) -> Result<(Op, Op)> {
        self.create_genesis_effects(topic_id, actor_id, genesis, event, signer, |_, _, _| {
            Ok(AdmissionEffects::default())
        })
        .map(|((genesis, _), (event, _))| (genesis, event))
    }

    pub(crate) fn create_genesis_effects<F>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        genesis: TopicGenesis,
        event: EventEnvelope,
        signer: &impl Signer,
        effects: F,
    ) -> Result<((Op, OpMeta), (Op, OpMeta))>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        let mut peers = genesis.initial_peers.clone();
        peers.insert(signer.peer_id());
        let genesis = TopicGenesis {
            initial_peers: peers,
            ..genesis
        };
        for attempt in 0..MAX_ADMISSION_RETRIES {
            conflict_pause(attempt);
            match self.try_genesis_effects(
                topic_id,
                actor_id,
                genesis.clone(),
                event.clone(),
                signer,
                &effects,
            ) {
                Err(err) if is_local_admission_race(&err) => continue,
                result => return result,
            }
        }
        Err(Error::AdmissionConflict)
    }

    pub fn create_event_op(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        event: EventEnvelope,
        signer: &impl Signer,
    ) -> Result<Op> {
        self.create_event_effects(topic_id, actor_id, event, signer, |_, _, _| {
            Ok(AdmissionEffects::default())
        })
        .map(|(op, _)| op)
    }

    pub(crate) fn create_event_effects<F>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        event: EventEnvelope,
        signer: &impl Signer,
        effects: F,
    ) -> Result<(Op, OpMeta)>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        self.create_local_effects(
            topic_id,
            actor_id,
            TopicPayload::Event(event),
            signer,
            effects,
        )
    }

    pub fn create_control_op(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        control: TopicControl,
        signer: &impl Signer,
    ) -> Result<Op> {
        self.create_control_effects(topic_id, actor_id, control, signer, |_, _, _| {
            Ok(AdmissionEffects::default())
        })
    }

    pub(crate) fn create_control_effects<F>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        control: TopicControl,
        signer: &impl Signer,
        effects: F,
    ) -> Result<Op>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        self.create_local_effects(
            topic_id,
            actor_id,
            TopicPayload::Control(control),
            signer,
            effects,
        )
        .map(|(op, _)| op)
    }

    pub fn receive_op(&self, op: Op) -> Result<()> {
        self.receive_ops(vec![op]).map(|_| ())
    }

    pub fn receive_ops(&self, ops: Vec<Op>) -> Result<BTreeSet<crate::OpId>> {
        self.receive_ops_from_peer(None, ops)
    }

    pub fn receive_ops_from_peer(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
    ) -> Result<BTreeSet<crate::OpId>> {
        Ok(self
            .receive_ops_admission(source_peer, ops, &BTreeSet::new(), None)?
            .accepted)
    }

    /// Like [`Self::receive_ops_from_peer`], but also returns any topic
    /// evictions produced by genesis tie-break resolution.
    pub fn receive_ops_from_peer_evicting(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
    ) -> Result<Admitted> {
        self.receive_ops_admission(source_peer, ops, &BTreeSet::new(), None)
    }

    /// Like [`Self::receive_ops_from_peer_evicting`], but skips signature checks
    /// for ids in `verified`, which the caller validated (ids are content-addressed).
    /// `effects` computes what each admitted batch commits alongside its ops.
    pub(crate) fn receive_ops_from_peer_preverified(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
        verified: &BTreeSet<crate::OpId>,
        effects: Option<ReceiveEffects<'_>>,
    ) -> Result<Admitted> {
        self.receive_ops_admission(source_peer, ops, verified, effects)
    }

    /// Admit every buffered op whose dependencies resolved, in finite passes.
    pub fn reconcile_pending_ops(&self) -> Result<BTreeSet<crate::OpId>> {
        let mut accepted = BTreeSet::new();
        loop {
            let pass = self.receive_ops_admission(None, Vec::new(), &BTreeSet::new(), None)?;
            accepted.extend(pass.accepted);
            if !pass.ready_remaining {
                return Ok(accepted);
            }
        }
    }

    pub fn receive_signed_op(&self, signed: SignedOp) -> Result<Op> {
        let op = Op::new(signed)?;
        self.receive_op(op.clone())?;
        Ok(op)
    }

    fn receive_ops_admission(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
        verified: &BTreeSet<crate::OpId>,
        effects: Option<ReceiveEffects<'_>>,
    ) -> Result<Admitted> {
        let mut admitted = Admitted::default();
        let result = (|| -> Result<()> {
            let mut queue = VecDeque::new();
            if !ops.is_empty() {
                queue.push_back((source_peer, ops, false));
            }
            // Buffered ops come from the ready index in slices, so only ops
            // whose waits resolved are read, and one call does finite work.
            let mut drained = 0;
            let mut cursor = None;
            let mut pass_admitted = false;
            loop {
                if queue.is_empty() {
                    if drained >= MAX_DRAIN_OPS {
                        admitted.ready_remaining =
                            !self.storage.ready_pending_after(None, 1)?.is_empty();
                        break;
                    }
                    let slice = self
                        .storage
                        .ready_pending_after(cursor.as_ref(), READY_SLICE)?;
                    let Some((_, last)) = slice.last() else {
                        if !pass_admitted {
                            break;
                        }
                        // Admissions of this pass may have readied ops the
                        // cursor already passed.
                        cursor = None;
                        pass_admitted = false;
                        continue;
                    };
                    cursor = Some(last.id);
                    drained += slice.len();
                    for (source, op) in slice {
                        queue.push_back((Some(source), vec![op], true));
                    }
                }
                let Some((batch_source_peer, ops, from_pending)) = queue.pop_front() else {
                    continue;
                };
                // A permanent rejection names the batch, not the op inside it,
                // so the retained copy lets one invalid record be isolated
                // without discarding the valid ops queued beside it.
                let retained = if from_pending {
                    ops.clone()
                } else {
                    Vec::new()
                };
                // Pending ops re-queued from storage are not in `verified`; they
                // get re-verified during admission like before.
                let (batch_accepted, batch_eviction) = match self.admit_ops_batch_retry(
                    batch_source_peer,
                    ops,
                    verified,
                    effects,
                ) {
                    Ok(outcome) => outcome,
                    Err(err) if from_pending && retained.len() == 1 => {
                        let op = &retained[0];
                        match self.pending_verdict(op, &err)? {
                            PendingVerdict::Reject => {
                                tracing::debug!(op_id = %op.id, error = %err, "rejecting pending subtree");
                                self.storage.reject_pending_subtree(&op.id)?;
                            }
                            // A repairable failure keeps the buffered record and
                            // must not fail the ops the caller actually sent.
                            PendingVerdict::Retain => {
                                tracing::debug!(op_id = %op.id, error = %err, "retaining pending op");
                            }
                        }
                        continue;
                    }
                    // Several buffered ops failed together: judge each alone, in
                    // dependency order, so only the offending subtree goes.
                    Err(_) if from_pending => {
                        for op in retained {
                            queue.push_back((batch_source_peer, vec![op], true));
                        }
                        continue;
                    }
                    Err(err) => return Err(err),
                };
                if let Some(eviction) = batch_eviction {
                    admitted.evictions.push(eviction);
                }
                pass_admitted |= !batch_accepted.is_empty();
                admitted.accepted.extend(batch_accepted.iter().copied());
            }

            Ok(())
        })();
        match result {
            Ok(()) => Ok(admitted),
            Err(error) => Err(admission_failure(admitted, error)),
        }
    }

    /// Whether a buffered op that failed admission with `error` can never be
    /// admitted on this branch. Only immutable facts reject: its own signed
    /// content and the stored records of its dependencies and actor slots.
    fn pending_verdict(&self, op: &Op, error: &Error) -> Result<PendingVerdict> {
        let immutable = match error {
            error if is_permanent_rejection(error) => true,
            // Raised only once every dependency is known, so they describe the
            // op's causal frontier, which no later arrival changes.
            Error::NotTopicMember
            | Error::EventTypeMismatch { .. }
            | Error::InvalidGenesis
            | Error::InvalidOpId => true,
            Error::ActorPrevMismatch | Error::ActorSeqGap { .. } => self.position_impossible(op)?,
            Error::ActorFork => self.slot_taken(op, op.signed.body.actor_seq)?.is_some(),
            _ => false,
        };
        Ok(if immutable {
            PendingVerdict::Reject
        } else {
            PendingVerdict::Retain
        })
    }

    /// Whether the op's actor position contradicts stored records: a known
    /// predecessor of another actor or sequence, or a slot before it that a
    /// different admitted op already holds.
    fn position_impossible(&self, op: &Op) -> Result<bool> {
        let body = &op.signed.body;
        let Some(prev) = body.actor_prev else {
            return Ok(body.actor_seq != 1);
        };
        if body.actor_seq < 2 || !body.deps.contains(&prev) {
            return Ok(true);
        }
        if let Some(meta) = self.storage.get_meta(&prev)?
            && self.storage.dep_resolvable(&prev)?
            && (meta.topic_id != body.topic_id
                || meta.actor_id != body.actor_id
                || checked_next(meta.actor_seq)? != body.actor_seq)
        {
            return Ok(true);
        }
        Ok(self
            .slot_taken(op, body.actor_seq - 1)?
            .is_some_and(|holder| holder != prev))
    }

    /// The admitted op holding `seq` of the op's actor, if it is not the op.
    fn slot_taken(&self, op: &Op, seq: u64) -> Result<Option<OpId>> {
        let body = &op.signed.body;
        let Some(holder) = self
            .storage
            .actor_index(&body.topic_id, &body.actor_id, seq)?
        else {
            return Ok(None);
        };
        Ok((holder != op.id && self.storage.dep_resolvable(&holder)?).then_some(holder))
    }

    fn admit_ops_batch_retry(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
        verified: &BTreeSet<crate::OpId>,
        effects: Option<ReceiveEffects<'_>>,
    ) -> Result<(BTreeSet<crate::OpId>, Option<TopicEviction>)> {
        let has_genesis = ops.iter().any(is_structural_genesis);
        // Signatures are immutable, so each is checked once for the whole job
        // rather than again on every retry.
        let mut checked = BTreeSet::new();
        for op in &ops {
            if !verified.contains(&op.id) {
                #[cfg(feature = "iroh")]
                op.validate_frame()?;
                op.validate()?;
            }
            checked.insert(op.id);
        }
        let verified = &checked;
        for attempt in 0..MAX_ADMISSION_RETRIES {
            conflict_pause(attempt);
            let (ops_to_admit, reset, rejected_genesis) = if has_genesis {
                self.resolve_genesis_collision(ops.clone(), verified)?
            } else {
                (ops.clone(), None, None)
            };
            let eviction = reset.as_ref().map(|plan| plan.eviction.clone());
            if let Some(losing) = rejected_genesis {
                self.purge_losing_pending(losing)?;
            }
            // A won foreign genesis discards the local chain: admit the winner
            // batch against a fresh topic and fold the reset into the same
            // storage transaction as its admission (`reset_topic_and_admit`).
            match self.admit_ops_batch(source_peer, ops_to_admit, verified, reset, effects) {
                Err(Error::AdmissionConflict) => continue,
                Ok(accepted) => return Ok((accepted, eviction)),
                Err(err) => return Err(err),
            }
        }
        Err(Error::AdmissionConflict)
    }

    /// Drain pending ops that transitively wait on a genesis that lost (or was
    /// rejected by) a collision resolution: the local topic keeps a different
    /// genesis, so that id will never be admitted here and nothing depending on
    /// it can ever become ready. The storage layer walks the waiter closure in
    /// one transaction so a crash cannot strand half of it.
    fn purge_losing_pending(&self, losing_genesis: OpId) -> Result<()> {
        self.storage
            .purge_pending_waiters(&losing_genesis)
            .map(drop)
    }

    /// Resolve a genesis tie-break for a batch that carries a structurally
    /// valid genesis. Returns the ops to admit (unchanged when the incoming
    /// genesis wins or there is no collision; with a losing foreign genesis
    /// filtered out when the local one wins), the reset the admission must
    /// perform when the local topic loses, and any rejected genesis to purge
    /// from pending.
    fn resolve_genesis_collision(
        &self,
        ops: Vec<Op>,
        verified: &BTreeSet<crate::OpId>,
    ) -> Result<GenesisResolution> {
        let Some(genesis) = ops.iter().find(|op| is_structural_genesis(op)).cloned() else {
            return Ok((ops, None, None));
        };
        let topic_id = genesis.signed.body.topic_id;
        let Some(state) = self.storage.topic_state(&topic_id)? else {
            // Fresh topic: normal admission accepts the genesis.
            return Ok((ops, None, None));
        };
        if state.genesis == genesis.id {
            // Same node re-sending its genesis: normal dedup handles it.
            return Ok((ops, None, None));
        }
        // Only a signature-valid genesis may win the tie-break.
        if !verified.contains(&genesis.id) {
            genesis.validate()?;
        }
        // Op ids are content-addressed 32-byte blake3 digests; the derived
        // `Ord` is lexicographic over those bytes, so both nodes pick the same
        // winner with no coordination.
        if genesis.id < state.genesis {
            // A smaller foreign genesis only wins if its author is a current
            // member of the LOCAL chain — the same membership the admission
            // path enforces for NotTopicMember (`state.members`, which folds in
            // AddPeer/RemovePeer control ops), not the genesis `initial_peers`
            // alone. Genesis op ids are grindable, so an unauthenticated
            // smaller id must not be allowed to force a topic reset.
            // Consequence: two forks with disjoint memberships never auto-
            // converge; the warn below is the intended, deliberate signal.
            if !state.members.contains(&genesis.signed.body.author) {
                tracing::warn!(
                    %topic_id,
                    local_genesis = %state.genesis,
                    foreign_genesis = %genesis.id,
                    author = %genesis.signed.body.author,
                    "rejected non-member genesis collision"
                );
                let filtered = without_descendants(ops, genesis.id);
                return Ok((filtered, None, Some(genesis.id)));
            }
            let eviction = self.extract_eviction(topic_id, &state, genesis.id)?;
            tracing::warn!(
                %topic_id,
                losing_genesis = %state.genesis,
                winning_genesis = %genesis.id,
                evicted = eviction.evicted.len(),
                "genesis collision resolved: reset local topic for smaller winning genesis"
            );
            Ok((
                ops,
                Some(ResetPlan {
                    expected_state: state,
                    eviction,
                }),
                None,
            ))
        } else {
            tracing::warn!(
                %topic_id,
                local_genesis = %state.genesis,
                foreign_genesis = %genesis.id,
                evicted = 0,
                "genesis collision resolved: kept local genesis, rejected larger foreign genesis"
            );
            let filtered = without_descendants(ops, genesis.id);
            Ok((filtered, None, Some(genesis.id)))
        }
    }

    /// Extract the local topic chain's non-genesis payloads (ordered by actor,
    /// then sequence) so the application can re-emit them under the winning
    /// genesis. The actual reset is deferred: the winner batch's admission runs
    /// the reset and the writes in one storage transaction
    /// (`reset_topic_and_admit`), so a crash cannot land between them. These
    /// reads stay outside that transaction.
    fn extract_eviction(
        &self,
        topic_id: TopicId,
        local_state: &TopicState,
        winning_genesis: OpId,
    ) -> Result<TopicEviction> {
        let mut discarded = self.storage.list_op_ids(&topic_id)?;
        discarded.remove(&local_state.genesis);
        Ok(TopicEviction {
            topic_id,
            losing_genesis: local_state.genesis,
            winning_genesis,
            evicted: self.evicted_ops(&discarded)?,
        })
    }

    /// Payloads for the ops named by `ids`, ordered by actor then sequence so
    /// re-emission preserves each actor's order. A half-stored id is reported
    /// and skipped: its payload cannot be read, and failing here would strand
    /// the whole topic instead of discarding one unreadable record.
    fn evicted_ops(&self, ids: &BTreeSet<OpId>) -> Result<Vec<EvictedOp>> {
        let mut metas = Vec::new();
        for id in ids {
            match self.storage.get_meta(id)? {
                Some(meta) => metas.push(meta),
                None => tracing::warn!(%id, "discarded op has no metadata to re-emit"),
            }
        }
        metas.sort_by_key(|meta| (meta.actor_id, meta.actor_seq));
        let mut evicted = Vec::new();
        for meta in metas {
            let Some(op) = self.storage.get_op(&meta.id)? else {
                tracing::warn!(id = %meta.id, "discarded op has no record to re-emit");
                continue;
            };
            evicted.push(EvictedOp {
                op_id: meta.id,
                actor_id: meta.actor_id,
                author: meta.author,
                actor_seq: meta.actor_seq,
                payload: op.signed.body.payload.clone(),
            });
        }
        Ok(evicted)
    }

    /// Validate `ops` against the stored topic, or against a fresh topic when
    /// `reset`, and build the batch admission would commit, without writing.
    fn build_batch(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
        verified: &BTreeSet<crate::OpId>,
        reset: bool,
        receive_effects: Option<ReceiveEffects<'_>>,
    ) -> Result<Option<BuiltBatch>> {
        let ops = topological_ops(ops)?;
        let mut accepted = BTreeSet::new();
        let Some(topic_id) = ops.first().map(|op| op.signed.body.topic_id) else {
            return Ok(None);
        };
        if ops.iter().any(|op| op.signed.body.topic_id != topic_id) {
            return Err(Error::TopicMismatch);
        }

        // On reset the wiped topic admits the self-contained winner batch atomically.
        // Heads and state come from one read: a membership verdict taken from a
        // state newer than the heads would reject an authorized op for good.
        let (expected_heads, expected_state) = if reset {
            (BTreeSet::new(), None)
        } else {
            let state = self.storage.topic_state(&topic_id)?;
            let heads = state
                .as_ref()
                .map(|state| state.heads.clone())
                .unwrap_or_default();
            (heads, state)
        };
        let mut heads = expected_heads.clone();
        let mut state = expected_state.clone();
        let mut topic_state_changed = false;
        let mut overlay_ops = BTreeMap::new();
        let mut overlay_meta = BTreeMap::new();
        let mut overlay_tips = BTreeMap::new();
        let mut overlay_index = BTreeMap::new();
        let mut entries = Vec::new();
        let mut pending = Vec::new();
        // Reuse immutable causal states only within the current genesis.
        let (projection_epoch, mut projections) = {
            let cache = self.membership_cache()?;
            let projections = cache
                .states
                .iter()
                .filter(|(_, state)| {
                    expected_state.as_ref().is_some_and(|expected| {
                        state.topic_id == topic_id && state.genesis == expected.genesis
                    })
                })
                .map(|(id, state)| (*id, Arc::clone(state)))
                .collect();
            (Arc::clone(&cache.epoch), projections)
        };
        let mut projection_tips = BTreeSet::new();

        for op in ops {
            #[cfg(feature = "iroh")]
            op.validate_frame()?;
            if !verified.contains(&op.id) {
                op.validate()?;
            }
            let stored = if reset {
                StoredOp::Absent
            } else {
                self.stored_op_state(&op)?
            };
            if matches!(stored, StoredOp::Complete) {
                continue;
            }

            let missing_deps = self.missing_deps_projected(&op, &overlay_ops, reset)?;
            // Refilling an already-accounted id changes no head, clock entry or
            // topic state: only the records themselves are rewritten, once its
            // dependencies can be resolved again.
            if matches!(stored, StoredOp::Repair) {
                if missing_deps.is_empty() {
                    let body = &op.signed.body;
                    if body.actor_id != actor_id_for(body.topic_id, body.author) {
                        return Err(Error::ActorAuthorMismatch);
                    }
                    if self
                        .storage
                        .actor_index(&body.topic_id, &body.actor_id, body.actor_seq)?
                        .is_some_and(|id| id != op.id)
                    {
                        return Err(Error::ActorFork);
                    }
                    match &body.payload {
                        TopicPayload::Genesis(_) => {
                            if !is_structural_genesis(&op)
                                || state.as_ref().is_some_and(|state| state.genesis != op.id)
                            {
                                return Err(Error::InvalidGenesis);
                            }
                        }
                        TopicPayload::Event(envelope) => {
                            let state = state.as_ref().ok_or(Error::TopicNotFound)?;
                            ensure_event_type(&state.event_type_id, &envelope.type_id)?;
                            if !self
                                .project_membership(
                                    &body.topic_id,
                                    &body.deps,
                                    &overlay_ops,
                                    &overlay_meta,
                                    &mut projections,
                                )?
                                .members
                                .contains(&body.author)
                            {
                                return Err(Error::NotTopicMember);
                            }
                        }
                        TopicPayload::Control(_) => {
                            state.as_ref().ok_or(Error::TopicNotFound)?;
                            if !self
                                .project_membership(
                                    &body.topic_id,
                                    &body.deps,
                                    &overlay_ops,
                                    &overlay_meta,
                                    &mut projections,
                                )?
                                .members
                                .contains(&body.author)
                            {
                                return Err(Error::NotTopicMember);
                            }
                        }
                    }
                    match (body.actor_seq, body.actor_prev) {
                        (1, None) => {}
                        (2.., Some(prev)) if body.deps.contains(&prev) => {
                            let prev_meta = self.meta_projected(&prev, &overlay_meta)?;
                            if prev_meta.topic_id != body.topic_id
                                || prev_meta.actor_id != body.actor_id
                                || checked_next(prev_meta.actor_seq)? != body.actor_seq
                            {
                                return Err(Error::ActorPrevMismatch);
                            }
                        }
                        _ => return Err(Error::ActorPrevMismatch),
                    }
                    let mut generation = 0;
                    for id in &body.deps {
                        let dep_meta = self.meta_projected(id, &overlay_meta)?;
                        if dep_meta.topic_id != body.topic_id {
                            return Err(Error::TopicMismatch);
                        }
                        generation = generation.max(checked_next(dep_meta.generation)?);
                    }
                    if body.generation != generation {
                        return Err(Error::GenerationMismatch {
                            expected: generation,
                            actual: body.generation,
                        });
                    }
                    let meta = self.meta_for_projected(&op, &overlay_meta)?;
                    if self
                        .storage
                        .get_meta(&op.id)?
                        .is_some_and(|stored| stored != meta)
                    {
                        return Err(Error::InvalidOpId);
                    }
                    overlay_meta.insert(op.id, meta.clone());
                    overlay_ops.insert(op.id, op.clone());
                    accepted.insert(op.id);
                    entries.push((op, meta));
                } else {
                    pending.push((op, missing_deps));
                }
                continue;
            }
            if !missing_deps.is_empty() {
                match self.validate_pending_op(
                    &op,
                    &missing_deps,
                    &BatchOverlay {
                        ops: &overlay_ops,
                        meta: &overlay_meta,
                        tips: &overlay_tips,
                        index: &overlay_index,
                        reset,
                    },
                    state.as_ref(),
                )? {
                    OpAdmission::Duplicate => {}
                    OpAdmission::Admit => pending.push((op, missing_deps)),
                }
                continue;
            }

            if let OpAdmission::Duplicate = self.validate_op_projected(
                &op,
                &BatchOverlay {
                    ops: &overlay_ops,
                    meta: &overlay_meta,
                    tips: &overlay_tips,
                    index: &overlay_index,
                    reset,
                },
                &heads,
                state.as_ref(),
                &mut projections,
            )? {
                continue;
            }
            projection_tips = op.signed.body.deps.clone();
            let meta = self.meta_for_projected(&op, &overlay_meta)?;
            heads = heads_after(&heads, &op);
            match &op.signed.body.payload {
                TopicPayload::Genesis(genesis) => {
                    state = Some(TopicState {
                        topic_id,
                        event_type_id: genesis.event_type_id.clone(),
                        genesis: op.id,
                        heads: heads.clone(),
                        members: genesis.initial_peers.clone(),
                        replication_policy: genesis.replication_policy.clone(),
                        membership_controls: BTreeMap::new(),
                        replication_policy_control: None,
                    });
                    topic_state_changed = true;
                }
                TopicPayload::Event(_) => {
                    if let Some(state) = state.as_mut() {
                        state.heads = heads.clone();
                    }
                }
                TopicPayload::Control(control) => {
                    let state = state.as_mut().ok_or(Error::TopicNotFound)?;
                    state.heads = heads.clone();
                    apply_control_to_state(state, &op, control);
                    topic_state_changed = true;
                }
            }

            overlay_index.insert((topic_id, meta.actor_id, meta.actor_seq), op.id);
            overlay_tips.insert((topic_id, meta.actor_id), (meta.actor_seq, op.id));
            overlay_meta.insert(op.id, meta.clone());
            overlay_ops.insert(op.id, op.clone());
            accepted.insert(op.id);
            entries.push((op, meta));
        }

        // Effects commit with the entries under the same expected state, so a
        // reset cannot slip between the ops and the work they create.
        let effects = match (receive_effects, state.as_ref()) {
            (Some(compute), Some(state)) if !entries.is_empty() => {
                compute(source_peer, &entries, state)?
            }
            _ => AdmissionEffects::default(),
        };
        Ok(Some(BuiltBatch {
            accepted,
            batch: AdmittedBatch {
                topic_id,
                expected_heads,
                expected_topic_state: expected_state,
                entries,
                heads,
                topic_state: topic_state_changed.then(|| state.clone()).flatten(),
                effects,
            },
            pending,
            projection_epoch,
            projections,
            projection_tips,
        }))
    }

    fn admit_ops_batch(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
        verified: &BTreeSet<crate::OpId>,
        reset_plan: Option<ResetPlan>,
        receive_effects: Option<ReceiveEffects<'_>>,
    ) -> Result<BTreeSet<crate::OpId>> {
        if reset_plan.is_some() {
            *self.membership_cache()? = MembershipCache::default();
        }
        let Some(BuiltBatch {
            accepted,
            batch,
            pending,
            projection_epoch,
            projections,
            projection_tips,
        }) = self.build_batch(
            source_peer,
            ops,
            verified,
            reset_plan.is_some(),
            receive_effects,
        )?
        else {
            return Ok(BTreeSet::new());
        };
        let topic_id = batch.topic_id;
        if let Some(plan) = &reset_plan {
            // Reset, winner admission, and the record of the discarded payloads
            // share one storage transaction, so a crash never leaves the topic
            // empty with the winner uninstalled or the payloads unrecorded.
            self.storage.reset_topic_and_admit(
                &topic_id,
                &plan.expected_state,
                batch,
                Some(&plan.eviction),
            )?;
            if let Ok(mut cache) = self.membership_cache() {
                *cache = MembershipCache::default();
            }
        } else if !batch.entries.is_empty() {
            self.storage.put_admitted_batch(batch)?;
        }

        // Buffer not-yet-ready ops last: on the reset path the reset above wipes
        // the topic's pending, so a partial winner batch's descendants must be
        // written after it to survive. A pending op's missing deps are never in
        // `entries`, so ordering after admission cannot spuriously reject it.
        for (op, missing_deps) in pending {
            let source_peer = source_peer.unwrap_or(op.signed.body.author);
            let buffered = self.storage.put_pending_op(
                source_peer,
                op.clone(),
                pending_meta_for(&op, missing_deps),
            );
            // A descendant of a rejected op can never be admitted here.
            if let Err(Error::RejectedOp(rejected)) = &buffered {
                tracing::debug!(op_id = %op.id, %rejected, "dropping op behind a rejected op");
                continue;
            }
            buffered.map_err(|error| {
                admission_failure(
                    Admitted {
                        ready_remaining: false,
                        accepted: accepted.clone(),
                        evictions: reset_plan
                            .as_ref()
                            .map(|plan| plan.eviction.clone())
                            .into_iter()
                            .collect(),
                    },
                    error,
                )
            })?;
        }

        // A reset or integrity recheck must also invalidate in-flight cache writes.
        if let Ok(mut cache) = self.membership_cache()
            && Arc::ptr_eq(&projection_epoch, &cache.epoch)
        {
            for id in projection_tips {
                if let Some(state) = projections.get(&id)
                    && !cache.states.contains_key(&id)
                {
                    if cache.states.len() == MAX_CACHED_PROJECTIONS
                        && let Some(oldest) = cache.order.pop_front()
                    {
                        cache.states.remove(&oldest);
                    }
                    cache.states.insert(id, Arc::clone(state));
                    cache.order.push_back(id);
                }
            }
        }
        Ok(accepted)
    }

    /// The batch promoting verified `staged` ops, validated like any admission
    /// against a fresh topic. `None` until their causally complete part makes
    /// both `local` and `source` members.
    pub(crate) fn bootstrap_batch(
        &self,
        local: PeerId,
        source: PeerId,
        staged: Vec<Op>,
        effects: Option<ReceiveEffects<'_>>,
    ) -> Result<Option<AdmittedBatch>> {
        let complete = complete_ops(staged);
        let invited = complete.iter().any(|op| match &op.signed.body.payload {
            TopicPayload::Genesis(genesis) => genesis.initial_peers.contains(&local),
            TopicPayload::Control(TopicControl::AddPeer { peer }) => *peer == local,
            _ => false,
        });
        if !invited {
            return Ok(None);
        }
        let verified = complete.iter().map(|op| op.id).collect();
        let Some(built) = self.build_batch(Some(source), complete, &verified, true, effects)?
        else {
            return Ok(None);
        };
        let members = built.batch.topic_state.as_ref().map(|state| &state.members);
        Ok(members
            .is_some_and(|members| members.contains(&local) && members.contains(&source))
            .then_some(built.batch))
    }

    pub fn observed_clock(&self, topic_id: &TopicId) -> Result<crate::ActorClock> {
        self.storage.actor_clock(topic_id)
    }

    fn create_local_effects<F>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        payload: TopicPayload,
        signer: &impl Signer,
        effects: F,
    ) -> Result<(Op, OpMeta)>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        let mut signed = None;
        for attempt in 0..MAX_ADMISSION_RETRIES {
            conflict_pause(attempt);
            match self.try_local_effects(
                topic_id,
                actor_id,
                payload.clone(),
                signer,
                &mut signed,
                &effects,
            ) {
                Err(err) if is_local_admission_race(&err) => continue,
                result => return result,
            }
        }
        Err(Error::AdmissionConflict)
    }

    fn try_local_effects<F>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        payload: TopicPayload,
        signer: &impl Signer,
        signed: &mut Option<Op>,
        effects: &F,
    ) -> Result<(Op, OpMeta)>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        if !matches!(payload, TopicPayload::Genesis(_)) {
            self.ensure_member(&topic_id, signer.peer_id())?;
        }
        let expected_heads = self.storage.heads(&topic_id)?;
        let expected_state = self.storage.topic_state(&topic_id)?;
        let op = self.next_local_op(
            topic_id,
            actor_id,
            expected_heads.clone(),
            payload,
            signer,
            signed,
        )?;
        #[cfg(feature = "iroh")]
        op.validate_frame()?;
        self.validate_op(&op)?;
        let meta = self.meta_for(&op)?;
        self.commit_admission(
            op.clone(),
            meta.clone(),
            expected_heads,
            expected_state,
            effects,
        )?;
        Ok((op, meta))
    }

    fn try_genesis_effects<F>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        genesis: TopicGenesis,
        event: EventEnvelope,
        signer: &impl Signer,
        effects: &F,
    ) -> Result<((Op, OpMeta), (Op, OpMeta))>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        let expected_heads = self.storage.heads(&topic_id)?;
        let expected_state = self.storage.topic_state(&topic_id)?;
        let genesis_op = self.next_local_op(
            topic_id,
            actor_id,
            expected_heads.clone(),
            TopicPayload::Genesis(genesis),
            signer,
            &mut None,
        )?;
        #[cfg(feature = "iroh")]
        genesis_op.validate_frame()?;
        self.validate_op(&genesis_op)?;
        let genesis_meta = self.meta_for(&genesis_op)?;

        let event_op = Op::sign(
            OpBody {
                topic_id,
                author: signer.peer_id(),
                actor_id,
                actor_seq: checked_next(genesis_meta.actor_seq)?,
                actor_prev: Some(genesis_op.id),
                deps: [genesis_op.id].into(),
                generation: checked_next(genesis_meta.generation)?,
                payload: TopicPayload::Event(event),
            },
            signer,
        )?;
        #[cfg(feature = "iroh")]
        event_op.validate_frame()?;
        event_op.validate()?;

        let genesis_heads = heads_after(&expected_heads, &genesis_op);
        let mut state = self
            .topic_state_after(&genesis_op, genesis_heads.clone(), expected_state.clone())?
            .ok_or(Error::TopicNotFound)?;
        let overlay_ops = BTreeMap::from([(genesis_op.id, genesis_op.clone())]);
        let overlay_meta = BTreeMap::from([(genesis_op.id, genesis_meta.clone())]);
        let overlay_tips = BTreeMap::from([(
            (topic_id, actor_id),
            (genesis_meta.actor_seq, genesis_op.id),
        )]);
        let overlay_index =
            BTreeMap::from([((topic_id, actor_id, genesis_meta.actor_seq), genesis_op.id)]);
        if let OpAdmission::Duplicate = self.validate_op_projected(
            &event_op,
            &BatchOverlay {
                ops: &overlay_ops,
                meta: &overlay_meta,
                tips: &overlay_tips,
                index: &overlay_index,
                reset: false,
            },
            &genesis_heads,
            Some(&state),
            &mut BTreeMap::new(),
        )? {
            return Err(Error::AdmissionConflict);
        }
        let event_meta = self.meta_for_projected(&event_op, &overlay_meta)?;

        let mut admission_effects = effects(&genesis_op, &genesis_meta, &state)?;
        let heads = heads_after(&genesis_heads, &event_op);
        state.heads = heads.clone();
        admission_effects
            .sync_obligations
            .extend(effects(&event_op, &event_meta, &state)?.sync_obligations);
        self.storage.put_admitted_batch(AdmittedBatch {
            topic_id,
            expected_heads,
            expected_topic_state: expected_state,
            entries: vec![
                (genesis_op.clone(), genesis_meta.clone()),
                (event_op.clone(), event_meta.clone()),
            ],
            heads,
            topic_state: Some(state),
            effects: admission_effects,
        })?;
        Ok(((genesis_op, genesis_meta), (event_op, event_meta)))
    }

    fn next_local_op(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        mut deps: BTreeSet<crate::OpId>,
        payload: TopicPayload,
        signer: &impl Signer,
        previous: &mut Option<Op>,
    ) -> Result<Op> {
        let tip = self.storage.actor_tip(&topic_id, &actor_id)?;
        let (actor_seq, actor_prev) = match tip {
            Some((seq, id)) => (
                seq.checked_add(1)
                    .ok_or_else(|| Error::Storage("actor sequence overflow".into()))?,
                Some(id),
            ),
            None => (1, None),
        };
        if let Some(prev) = actor_prev {
            deps.insert(prev);
        }
        let generation = if deps.is_empty() {
            0
        } else {
            self.storage
                .max_generation(&topic_id)?
                .checked_add(1)
                .ok_or(Error::InvalidOpId)?
        };
        let body = OpBody {
            topic_id,
            author: signer.peer_id(),
            actor_id,
            actor_seq,
            actor_prev,
            deps,
            generation,
            payload,
        };
        // A retry whose body did not change reuses the op signed before.
        if let Some(op) = previous.as_ref().filter(|op| op.signed.body == body) {
            return Ok(op.clone());
        }
        let op = Op::sign(body, signer)?;
        op.validate()?;
        *previous = Some(op.clone());
        Ok(op)
    }

    fn validate_op(&self, op: &Op) -> Result<()> {
        let body = &op.signed.body;
        if body.actor_id != actor_id_for(body.topic_id, body.author) {
            return Err(Error::ActorAuthorMismatch);
        }
        if matches!(body.payload, TopicPayload::Genesis(_))
            && self.storage.topic_state(&body.topic_id)?.is_some()
        {
            return Err(Error::InvalidGenesis);
        }
        if let Some(existing) =
            self.storage
                .actor_index(&body.topic_id, &body.actor_id, body.actor_seq)?
            && existing != op.id
        {
            return Err(Error::ActorFork);
        }
        let expected = self.storage.actor_tip(&body.topic_id, &body.actor_id)?;
        let (expected_seq, expected_prev) = next_actor_position(expected)?;
        if body.actor_seq != expected_seq {
            return Err(Error::ActorSeqGap {
                expected: expected_seq,
                actual: body.actor_seq,
            });
        }
        if body.actor_prev != expected_prev {
            return Err(Error::ActorPrevMismatch);
        }
        if let Some(prev) = body.actor_prev
            && !body.deps.contains(&prev)
        {
            return Err(Error::ActorPrevMismatch);
        }
        for dep in &body.deps {
            if self.storage.get_op(dep)?.is_none() {
                return Err(Error::MissingDependency(*dep));
            }
        }
        match &body.payload {
            TopicPayload::Genesis(_) => {
                if body.actor_seq != 1
                    || body.actor_prev.is_some()
                    || !body.deps.is_empty()
                    || self.storage.topic_state(&body.topic_id)?.is_some()
                {
                    return Err(Error::InvalidGenesis);
                }
            }
            TopicPayload::Event(envelope) => {
                let current = self
                    .storage
                    .topic_state(&body.topic_id)?
                    .ok_or(Error::TopicNotFound)?;
                ensure_event_type(&current.event_type_id, &envelope.type_id)?;
                let author_is_member = if body.deps == current.heads {
                    current.members.contains(&body.author)
                } else {
                    self.topic_state_for_deps(&body.topic_id, &body.deps)?
                        .members
                        .contains(&body.author)
                };
                if !author_is_member {
                    return Err(Error::NotTopicMember);
                }
            }
            TopicPayload::Control(_) => {
                let current = self
                    .storage
                    .topic_state(&body.topic_id)?
                    .ok_or(Error::TopicNotFound)?;
                let author_is_member = if body.deps == current.heads {
                    current.members.contains(&body.author)
                } else {
                    self.topic_state_for_deps(&body.topic_id, &body.deps)?
                        .members
                        .contains(&body.author)
                };
                if !author_is_member {
                    return Err(Error::NotTopicMember);
                }
            }
        }
        let mut generation = 0;
        for id in &body.deps {
            let meta = self
                .storage
                .get_meta(id)?
                .ok_or(Error::MissingDependency(*id))?;
            generation = generation.max(checked_next(meta.generation)?);
        }
        if body.generation != generation {
            return Err(Error::GenerationMismatch {
                expected: generation,
                actual: body.generation,
            });
        }
        Ok(())
    }

    fn meta_for(&self, op: &Op) -> Result<OpMeta> {
        let body = &op.signed.body;
        let observed_clock = self.observed_clock_for_deps(&body.topic_id, &body.deps)?;
        Ok(OpMeta {
            id: op.id,
            topic_id: body.topic_id,
            author: body.author,
            actor_id: body.actor_id,
            actor_seq: body.actor_seq,
            actor_prev: body.actor_prev,
            deps: body.deps.clone(),
            generation: body.generation,
            observed_clock,
            ready: true,
            missing_deps: BTreeSet::new(),
        })
    }

    fn meta_for_projected(
        &self,
        op: &Op,
        overlay_meta: &BTreeMap<crate::OpId, OpMeta>,
    ) -> Result<OpMeta> {
        let body = &op.signed.body;
        let mut observed_clock = crate::ActorClock::new();
        for dep in &body.deps {
            let meta = self.meta_projected(dep, overlay_meta)?;
            if meta.topic_id != body.topic_id {
                return Err(Error::TopicMismatch);
            }
            observed_clock.merge(&meta.observed_clock);
            observed_clock.observe(meta.actor_id, meta.actor_seq);
        }
        Ok(OpMeta {
            id: op.id,
            topic_id: body.topic_id,
            author: body.author,
            actor_id: body.actor_id,
            actor_seq: body.actor_seq,
            actor_prev: body.actor_prev,
            deps: body.deps.clone(),
            generation: body.generation,
            observed_clock,
            ready: true,
            missing_deps: BTreeSet::new(),
        })
    }

    fn meta_projected(
        &self,
        id: &crate::OpId,
        overlay_meta: &BTreeMap<crate::OpId, OpMeta>,
    ) -> Result<OpMeta> {
        overlay_meta.get(id).cloned().map(Ok).unwrap_or_else(|| {
            self.storage
                .get_meta(id)?
                .ok_or(Error::MissingDependency(*id))
        })
    }

    fn op_projected(
        &self,
        id: &crate::OpId,
        overlay_ops: &BTreeMap<crate::OpId, Op>,
    ) -> Result<Op> {
        overlay_ops.get(id).cloned().map(Ok).unwrap_or_else(|| {
            self.storage
                .get_op(id)?
                .ok_or(Error::MissingDependency(*id))
        })
    }

    /// Dependencies this op cannot resolve yet. A dependency counts as present
    /// only when both its op record and its metadata are stored: the DAG is
    /// traversed through metadata, so either record alone is a hole to refill,
    /// never a satisfied edge. On the reset path storage is about to be wiped,
    /// so only the batch overlay counts.
    fn missing_deps_projected(
        &self,
        op: &Op,
        overlay_ops: &BTreeMap<crate::OpId, Op>,
        reset: bool,
    ) -> Result<BTreeSet<crate::OpId>> {
        let mut missing = BTreeSet::new();
        for dep in &op.signed.body.deps {
            if overlay_ops.contains_key(dep) {
                continue;
            }
            if reset || self.storage.get_op(dep)?.is_none() || self.storage.get_meta(dep)?.is_none()
            {
                missing.insert(*dep);
            }
        }
        Ok(missing)
    }

    fn validate_pending_op(
        &self,
        op: &Op,
        missing_deps: &BTreeSet<crate::OpId>,
        overlay: &BatchOverlay<'_>,
        state: Option<&TopicState>,
    ) -> Result<OpAdmission> {
        let body = &op.signed.body;
        if missing_deps.len() > MAX_PENDING_MISSING_DEPS {
            return Err(Error::Storage(
                "pending op has too many missing deps".into(),
            ));
        }
        if body.actor_id != actor_id_for(body.topic_id, body.author) {
            return Err(Error::ActorAuthorMismatch);
        }
        if body.actor_seq == 0 {
            return Err(Error::ActorSeqGap {
                expected: 1,
                actual: 0,
            });
        }
        if let Some(existing) = self.stored_actor_index(body, overlay.reset)?.or_else(|| {
            overlay
                .index
                .get(&(body.topic_id, body.actor_id, body.actor_seq))
                .copied()
        }) {
            if existing != op.id {
                return Err(Error::ActorFork);
            }
            if self.is_admitted_duplicate(op)? {
                return Ok(OpAdmission::Duplicate);
            }
        }
        match &body.payload {
            TopicPayload::Genesis(_) => {
                if body.actor_seq != 1
                    || body.actor_prev.is_some()
                    || !body.deps.is_empty()
                    || state.is_some()
                {
                    return Err(Error::InvalidGenesis);
                }
            }
            TopicPayload::Event(envelope) => {
                if body.deps.is_empty() || body.generation == 0 {
                    return Err(Error::InvalidOpId);
                }
                // Latest membership says nothing about the op's causal frontier,
                // which is unknown while a dependency is missing; the source's
                // pending quota bounds what an unproven author can buffer.
                if let Some(state) = state {
                    ensure_event_type(&state.event_type_id, &envelope.type_id)?;
                }
            }
            TopicPayload::Control(_) => {
                if body.deps.is_empty() || body.generation == 0 {
                    return Err(Error::InvalidOpId);
                }
            }
        }
        match (body.actor_seq, body.actor_prev) {
            (1, Some(_)) => return Err(Error::ActorPrevMismatch),
            (2.., None) => return Err(Error::ActorPrevMismatch),
            _ => {}
        }
        if let Some(prev) = body.actor_prev {
            if !body.deps.contains(&prev) {
                return Err(Error::ActorPrevMismatch);
            }
            if !missing_deps.contains(&prev) {
                let prev_meta = self.meta_projected(&prev, overlay.meta)?;
                if prev_meta.topic_id != body.topic_id || prev_meta.actor_id != body.actor_id {
                    return Err(Error::ActorPrevMismatch);
                }
                if checked_next(prev_meta.actor_seq)? != body.actor_seq {
                    return Err(Error::ActorSeqGap {
                        expected: checked_next(prev_meta.actor_seq)?,
                        actual: body.actor_seq,
                    });
                }
            }
        }
        let expected = match overlay.tips.get(&(body.topic_id, body.actor_id)).copied() {
            Some(tip) => Some(tip),
            None => self.stored_actor_tip(body, overlay.reset)?,
        };
        if let Some((tip_seq, tip_id)) = expected {
            let next_seq = checked_next(tip_seq)?;
            if body.actor_seq <= tip_seq {
                if self.is_admitted_duplicate(op)? {
                    return Ok(OpAdmission::Duplicate);
                }
                return Err(Error::ActorSeqGap {
                    expected: next_seq,
                    actual: body.actor_seq,
                });
            }
            if body.actor_prev == Some(tip_id) && body.actor_seq != next_seq {
                return Err(Error::ActorSeqGap {
                    expected: next_seq,
                    actual: body.actor_seq,
                });
            }
        }
        for dep in &body.deps {
            if missing_deps.contains(dep) {
                continue;
            }
            let meta = self.meta_projected(dep, overlay.meta)?;
            if meta.topic_id != body.topic_id {
                return Err(Error::TopicMismatch);
            }
            if meta.generation >= body.generation {
                return Err(Error::GenerationMismatch {
                    expected: checked_next(meta.generation)?,
                    actual: body.generation,
                });
            }
        }
        Ok(OpAdmission::Admit)
    }

    fn validate_op_projected(
        &self,
        op: &Op,
        overlay: &BatchOverlay<'_>,
        heads: &BTreeSet<crate::OpId>,
        state: Option<&TopicState>,
        projections: &mut BTreeMap<OpId, Arc<TopicState>>,
    ) -> Result<OpAdmission> {
        let body = &op.signed.body;
        if body.actor_id != actor_id_for(body.topic_id, body.author) {
            return Err(Error::ActorAuthorMismatch);
        }
        if let Some(prev) = body.actor_prev
            && !body.deps.contains(&prev)
        {
            return Err(Error::ActorPrevMismatch);
        }
        if let Some(existing) = self.stored_actor_index(body, overlay.reset)?.or_else(|| {
            overlay
                .index
                .get(&(body.topic_id, body.actor_id, body.actor_seq))
                .copied()
        }) {
            if existing != op.id {
                return Err(Error::ActorFork);
            }
            if self.is_admitted_duplicate(op)? {
                return Ok(OpAdmission::Duplicate);
            }
        }
        match &body.payload {
            TopicPayload::Genesis(_) => {
                if body.actor_seq != 1
                    || body.actor_prev.is_some()
                    || !body.deps.is_empty()
                    || state.is_some()
                {
                    return Err(Error::InvalidGenesis);
                }
            }
            TopicPayload::Event(envelope) => {
                let state = state.ok_or(Error::TopicNotFound)?;
                ensure_event_type(&state.event_type_id, &envelope.type_id)?;
                let author_is_member = if body.deps == *heads {
                    state.members.contains(&body.author)
                } else {
                    self.project_membership(
                        &body.topic_id,
                        &body.deps,
                        overlay.ops,
                        overlay.meta,
                        projections,
                    )?
                    .members
                    .contains(&body.author)
                };
                if !author_is_member {
                    return Err(Error::NotTopicMember);
                }
            }
            TopicPayload::Control(_) => {
                let state = state.ok_or(Error::TopicNotFound)?;
                let author_is_member = if body.deps == *heads {
                    state.members.contains(&body.author)
                } else {
                    self.project_membership(
                        &body.topic_id,
                        &body.deps,
                        overlay.ops,
                        overlay.meta,
                        projections,
                    )?
                    .members
                    .contains(&body.author)
                };
                if !author_is_member {
                    return Err(Error::NotTopicMember);
                }
            }
        }
        let expected = match overlay.tips.get(&(body.topic_id, body.actor_id)).copied() {
            Some(tip) => Some(tip),
            None => self.stored_actor_tip(body, overlay.reset)?,
        };
        let (expected_seq, expected_prev) = next_actor_position(expected)?;
        if body.actor_seq != expected_seq {
            if body.actor_seq < expected_seq && self.is_admitted_duplicate(op)? {
                return Ok(OpAdmission::Duplicate);
            }
            return Err(Error::ActorSeqGap {
                expected: expected_seq,
                actual: body.actor_seq,
            });
        }
        if body.actor_prev != expected_prev {
            return Err(Error::ActorPrevMismatch);
        }
        let mut generation = 0;
        for id in &body.deps {
            let meta = self.meta_projected(id, overlay.meta)?;
            generation = generation.max(checked_next(meta.generation)?);
        }
        if body.generation != generation {
            return Err(Error::GenerationMismatch {
                expected: generation,
                actual: body.generation,
            });
        }
        Ok(OpAdmission::Admit)
    }

    /// The stored actor slot and tip, or `None` on the reset path. A reset wipes
    /// every actor index and tip of the topic in the same transaction as the
    /// admission, so validating against them would judge the batch by a chain
    /// that is about to stop existing.
    fn stored_actor_index(&self, body: &OpBody, reset: bool) -> Result<Option<OpId>> {
        if reset {
            return Ok(None);
        }
        self.storage
            .actor_index(&body.topic_id, &body.actor_id, body.actor_seq)
    }

    fn stored_actor_tip(&self, body: &OpBody, reset: bool) -> Result<Option<(u64, OpId)>> {
        if reset {
            return Ok(None);
        }
        self.storage.actor_tip(&body.topic_id, &body.actor_id)
    }

    /// Re-reads storage after a tip/seq mismatch: a concurrent admission may
    /// have committed this exact op between the batch dedup check and the
    /// validation reads. Only a completely stored op is such a duplicate; a
    /// half-stored one is damage that [`Self::stored_op_state`] routes into
    /// repair, and calling it a duplicate is what left it broken forever.
    fn is_admitted_duplicate(&self, op: &Op) -> Result<bool> {
        self.storage.dep_resolvable(&op.id)
    }

    /// Classify what the store already holds for `op`. Both records present is
    /// a duplicate; either record alone, or an actor slot or child edge naming
    /// this exact id while the records are gone, is damage the local chain
    /// already accounts for and must repair in place.
    fn stored_op_state(&self, op: &Op) -> Result<StoredOp> {
        let has_op = self.storage.get_op(&op.id)?.is_some();
        let has_meta = self.storage.get_meta(&op.id)?.is_some();
        if has_op && has_meta {
            return Ok(StoredOp::Complete);
        }
        if has_op || has_meta {
            return Ok(StoredOp::Repair);
        }
        let body = &op.signed.body;
        if self
            .storage
            .actor_index(&body.topic_id, &body.actor_id, body.actor_seq)?
            == Some(op.id)
            || !self.storage.children(&op.id)?.is_empty()
        {
            return Ok(StoredOp::Repair);
        }
        Ok(StoredOp::Absent)
    }

    fn project_membership(
        &self,
        topic_id: &TopicId,
        deps: &BTreeSet<OpId>,
        overlay_ops: &BTreeMap<OpId, Op>,
        overlay_meta: &BTreeMap<OpId, OpMeta>,
        projections: &mut BTreeMap<OpId, Arc<TopicState>>,
    ) -> Result<Arc<TopicState>> {
        let mut pending = deps.iter().map(|id| (*id, false)).collect::<Vec<_>>();
        let mut visiting = BTreeSet::new();
        while let Some((id, visited)) = pending.pop() {
            if projections.contains_key(&id) {
                continue;
            }
            let meta = self.meta_projected(&id, overlay_meta)?;
            if meta.topic_id != *topic_id {
                return Err(Error::TopicMismatch);
            }
            if !visited {
                if !visiting.insert(id) {
                    return Err(Error::Storage("cycle in op graph".into()));
                }
                pending.push((id, true));
                pending.extend(meta.deps.iter().map(|dep| (*dep, false)));
                continue;
            }
            let op = self.op_projected(&id, overlay_ops)?;
            let state = match &op.signed.body.payload {
                TopicPayload::Genesis(_) => {
                    Arc::new(materialize_topic_state(vec![op], BTreeSet::new())?)
                }
                TopicPayload::Event(_) => merge_states(&meta.deps, projections)?,
                TopicPayload::Control(control) => {
                    let mut state = (*merge_states(&meta.deps, projections)?).clone();
                    apply_control_to_state(&mut state, &op, control);
                    Arc::new(state)
                }
            };
            visiting.remove(&id);
            projections.insert(id, state);
        }
        merge_states(deps, projections)
    }

    fn observed_clock_for_deps(
        &self,
        topic_id: &TopicId,
        deps: &BTreeSet<crate::OpId>,
    ) -> Result<crate::ActorClock> {
        let mut observed_clock = crate::ActorClock::new();
        for id in deps {
            let meta = self
                .storage
                .get_meta(id)?
                .ok_or(Error::MissingDependency(*id))?;
            if meta.topic_id != *topic_id {
                return Err(Error::TopicMismatch);
            }
            observed_clock.merge(&meta.observed_clock);
            observed_clock.observe(meta.actor_id, meta.actor_seq);
        }

        Ok(observed_clock)
    }

    fn topic_state_after(
        &self,
        op: &Op,
        heads: BTreeSet<crate::OpId>,
        base_state: Option<TopicState>,
    ) -> Result<Option<TopicState>> {
        let body = &op.signed.body;
        match &body.payload {
            TopicPayload::Genesis(genesis) => Ok(Some(TopicState {
                topic_id: body.topic_id,
                event_type_id: genesis.event_type_id.clone(),
                genesis: op.id,
                heads,
                members: genesis.initial_peers.clone(),
                replication_policy: genesis.replication_policy.clone(),
                membership_controls: BTreeMap::new(),
                replication_policy_control: None,
            })),
            TopicPayload::Event(_) => Ok(None),
            TopicPayload::Control(control) => {
                let mut state = base_state.ok_or(Error::TopicNotFound)?;
                state.heads = heads;
                apply_control_to_state(&mut state, op, control);
                Ok(Some(state))
            }
        }
    }

    fn commit_admission<F>(
        &self,
        op: Op,
        meta: OpMeta,
        expected_heads: BTreeSet<crate::OpId>,
        expected_state: Option<TopicState>,
        effects: &F,
    ) -> Result<()>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        let heads = heads_after(&expected_heads, &op);
        let topic_state = self.topic_state_after(&op, heads.clone(), expected_state.clone())?;
        let effective_state = topic_state
            .as_ref()
            .or(expected_state.as_ref())
            .ok_or(Error::TopicNotFound)?;
        let effects = effects(&op, &meta, effective_state)?;
        self.storage.put_admitted_batch(AdmittedBatch {
            topic_id: op.signed.body.topic_id,
            expected_heads,
            expected_topic_state: expected_state,
            entries: vec![(op, meta)],
            heads,
            topic_state,
            effects,
        })
    }

    fn topic_state_for_deps(
        &self,
        topic_id: &TopicId,
        deps: &BTreeSet<crate::OpId>,
    ) -> Result<TopicState> {
        let mut reachable = BTreeMap::new();
        let mut stack = deps.iter().copied().collect::<Vec<_>>();
        while let Some(id) = stack.pop() {
            if reachable.contains_key(&id) {
                continue;
            }
            let op = self
                .storage
                .get_op(&id)?
                .ok_or(Error::MissingDependency(id))?;
            let body = &op.signed.body;
            if body.topic_id != *topic_id {
                return Err(Error::TopicMismatch);
            }
            stack.extend(body.deps.iter().copied());
            reachable.insert(id, op);
        }

        materialize_topic_state(reachable.into_values().collect(), BTreeSet::new())
    }

    fn ensure_member(&self, topic_id: &TopicId, peer: crate::PeerId) -> Result<()> {
        let state = self
            .storage
            .topic_state(topic_id)?
            .ok_or(Error::TopicNotFound)?;
        if state.members.contains(&peer) {
            Ok(())
        } else {
            Err(Error::NotTopicMember)
        }
    }
}
