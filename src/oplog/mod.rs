// SPDX-License-Identifier: MIT OR Apache-2.0
//! Operation-log admission, DAG validation, and topic-state materialization.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::storage::{
    AdmissionEffects, AdmittedBatch, MemoryStorage, OpMeta, SnapshotRead, Storage, TopicState,
    TopicView,
};
use crate::{
    ActorId, Error, EvictionKey, Op, OpId, PeerId, Result, SignedOp, TopicId, TopicPayload,
};

mod admission;
mod creation;
mod genesis;
mod integrity;
mod membership;
mod pending;
mod topology;

pub(crate) use genesis::is_structural_genesis;
pub(crate) use integrity::{Holes, Integrity};
pub(crate) use topology::topological_ids;
pub(crate) use topology::{subset_in, topological_subset_entries};
pub use topology::{topological, topological_subset};

/// Attempts one admission job makes; storage writes on this path try once each.
pub(crate) const MAX_ADMISSION_RETRIES: usize = 64;
const MAX_CACHED_PROJECTIONS: usize = 4096;

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

/// Batch-local validation view, including accepted entries and reset state. A
/// reset removes stored actor slots and tips, so they cannot affect new positions.
struct BatchOverlay<'a> {
    ops: &'a BTreeMap<OpId, Op>,
    meta: &'a BTreeMap<OpId, OpMeta>,
    tips: &'a BTreeMap<(TopicId, ActorId), (u64, OpId)>,
    index: &'a BTreeMap<(TopicId, ActorId, u64), OpId>,
    reset: bool,
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

/// Ops discarded from a topic's local chain, ordered by `(actor_id, actor_seq)`, for the
/// embedder to re-emit. A genesis tie-break names the replaced and the winning genesis; a
/// quarantine names the surviving genesis in both fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicEviction {
    pub topic_id: TopicId,
    pub losing_genesis: OpId,
    pub winning_genesis: OpId,
    pub evicted: Vec<EvictedOp>,
}

impl TopicEviction {
    /// Identity of this eviction's durable journal record, derived from its content: repeating the
    /// write, delivery or recovery never adds a record, and the eviction alone names it.
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
    /// Ready buffered ops were left because a whole visit window admitted
    /// none of them: each failed with a retryable error. A later admission or
    /// [`Oplog::reconcile_pending_ops`] retries them.
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

/// Branch and data epoch an integrity verdict is recorded under.
#[cfg(feature = "iroh")]
fn view_key(view: &TopicView) -> (OpId, u64) {
    (view.state.genesis, view.epoch)
}

#[derive(Clone)]
pub struct Oplog<S = MemoryStorage> {
    storage: S,
    // Scans and verdicts per branch and data epoch. Admission never creates a
    // hole, so only a reset or damage from outside irokle needs a new scan.
    integrity: Arc<integrity::Inspections>,
    membership_cache: Arc<Mutex<MembershipCache>>,
    receive_genesis: Option<(TopicId, OpId)>,
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
            integrity: Arc::default(),
            membership_cache: Arc::new(Mutex::new(MembershipCache::default())),
            receive_genesis: None,
        }
    }
    pub fn storage(&self) -> &S {
        &self.storage
    }

    #[cfg(feature = "iroh")]
    pub(crate) fn bound_genesis(mut self, topic: TopicId, genesis: OpId) -> Self {
        self.receive_genesis = Some((topic, genesis));
        self
    }

    /// An oplog over `storage` sharing this oplog's membership projections,
    /// which are keyed by op id and filtered by topic and genesis on use.
    pub(crate) fn sharing_membership<T: Storage>(&self, storage: T) -> Oplog<T> {
        Oplog {
            storage,
            integrity: Arc::default(),
            membership_cache: Arc::clone(&self.membership_cache),
            receive_genesis: self.receive_genesis,
        }
    }

    /// Ids this topic references but cannot resolve; empty means every admitted op is usable and
    /// sync may certify the topic. Kept holes are checked again on each question, so a repair
    /// stored through any facade of the store is seen without receiving it again.
    pub fn topic_unresolved(&self, topic_id: &TopicId) -> Result<BTreeSet<crate::OpId>> {
        let holes = match self.inspect(topic_id)? {
            Some((view, integrity)) => integrity.unresolved(&view),
            None => self.stateless_holes(topic_id)?,
        };
        Ok(holes.into_keys().collect())
    }

    /// Ids `view`'s topic cannot resolve, after its scan completed in later
    /// snapshots. A reset since `view` scans the new branch or epoch instead.
    #[cfg(feature = "iroh")]
    pub(crate) fn view_unresolved(&self, view: &TopicView) -> Result<BTreeSet<OpId>> {
        let Some((current, integrity)) = self.inspect(&view.state.topic_id)? else {
            return Ok(view.pending_missing.clone());
        };
        let mut unresolved = integrity.unresolved(&current);
        if view_key(&current) != view_key(view) {
            unresolved.extend(view.pending_missing.iter().map(|id| (*id, None)));
        }
        Ok(unresolved.into_keys().collect())
    }

    /// What is known of `view`'s topic, read from `read`: the verdict, or one
    /// more bounded step of its scan. `view` must come from `read`.
    pub(crate) fn integrity_in(
        &self,
        read: &dyn SnapshotRead,
        view: &TopicView,
    ) -> Result<Integrity> {
        self.integrity.step(read, view)
    }

    /// A view of the topic and whether its admitted history is whole, both from one snapshot.
    /// A buffered op is not admitted, so its missing dependency does not count.
    pub(crate) fn whole_view(&self, topic_id: &TopicId) -> Result<Option<(TopicView, bool)>> {
        Ok(self.inspect(topic_id)?.map(|(view, integrity)| {
            let whole = integrity.is_whole();
            (view, whole)
        }))
    }

    /// Whether every admitted op of the topic is usable. A buffered op that waits
    /// for a dependency that never arrives must not hide the admitted history.
    pub(crate) fn history_whole(&self, topic_id: &TopicId) -> Result<bool> {
        Ok(match self.inspect(topic_id)? {
            Some((_, integrity)) => integrity.is_whole(),
            None => self.stateless_holes(topic_id)?.is_empty(),
        })
    }

    /// The topic's integrity once its scan is complete, with the view of the
    /// snapshot that answered. Each step reads its own snapshot.
    pub(crate) fn inspect(&self, topic_id: &TopicId) -> Result<Option<(TopicView, Integrity)>> {
        loop {
            match self.ask(topic_id)? {
                Some((_, integrity)) if !integrity.is_complete() => {}
                answer => return Ok(answer),
            }
        }
    }

    /// One integrity question in a fresh snapshot. An unfinished answer returns
    /// once no step holds the scan, so the next question can step it.
    fn ask(&self, topic_id: &TopicId) -> Result<Option<(TopicView, Integrity)>> {
        let answer = self.storage.read_snapshot(|read| {
            let Some(view) = read.topic_view(topic_id, None)? else {
                return Ok(None);
            };
            let integrity = self.integrity.step(read, &view)?;
            Ok(Some((view, integrity)))
        })?;
        if answer
            .as_ref()
            .is_some_and(|(_, integrity)| !integrity.is_complete())
        {
            self.integrity.wait_idle(topic_id)?;
        }
        Ok(answer)
    }

    /// Holes of a topic without state. There is no branch to keep a verdict
    /// for, so each call scans what the stored records reference.
    fn stateless_holes(&self, topic_id: &TopicId) -> Result<Holes> {
        let mut holes = Holes::new();
        let mut cursor = integrity::Cursor::default();
        loop {
            let step = self.storage.read_snapshot(|read| {
                integrity::scan_step(read, topic_id, cursor, integrity::STEP_READS)
            })?;
            for (id, generation) in step.holes {
                let known = holes.entry(id).or_insert(generation);
                *known = known.or(generation);
            }
            if step.done {
                return Ok(holes);
            }
            cursor = step.cursor;
        }
    }

    /// Clear every integrity verdict and scan, and the membership projections, so the next
    /// question scans the stored records again: damage from outside Irokle is found no other way.
    pub fn recheck_topics(&self) -> Result<()> {
        self.integrity.clear()?;
        *self.membership_cache()? = MembershipCache::default();
        Ok(())
    }

    /// Removes the holes of `listed` that are stored now from this oplog's cache
    /// at once. Questions check cached holes again anyway, so this is only faster.
    fn fill_holes(&self, listed: Vec<integrity::Listed>) {
        let mut filled = Vec::new();
        for hole in listed {
            match self.storage.dep_resolvable(&hole.id) {
                Ok(true) => filled.push(hole),
                Ok(false) => {}
                Err(error) => tracing::warn!(id = %hole.id, %error, "kept a hole unchecked"),
            }
        }
        if let Err(error) = self.integrity.fill(&filled) {
            tracing::warn!(%error, "kept filled holes in the integrity cache");
        }
    }

    #[cfg(test)]
    pub(crate) fn set_step_reads(&self, reads: usize) {
        self.integrity.set_reads(reads);
    }

    /// Return stored ops unreachable from current heads. Reachability defines
    /// the local lineage; replaced-genesis descendants lie outside its frontier.
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
            let Some(meta) = self.storage.get_position(&id)? else {
                continue;
            };
            frontier.extend(meta.deps);
        }
        let mut orphans = self.storage.list_op_ids(topic_id)?;
        orphans.retain(|id| !reachable.contains(id));
        Ok(orphans)
    }

    /// Discard the ops no head reaches and re-admit the rest from the genesis in one transaction,
    /// dropping acks and obligations of the old frontier. `None` when nothing is orphaned, the
    /// head closure still has a hole, or no head reaches the genesis.
    pub fn quarantine_orphans(&self, topic_id: &TopicId) -> Result<Option<TopicEviction>> {
        if self.topic_orphans(topic_id)?.is_empty() {
            return Ok(None);
        }
        // The rebuild commits only if the topic still matches the planned
        // state, so a concurrent tie-break or append makes it replan. A
        // survivor's signature is checked once, however often the plan repeats.
        let mut checked = BTreeSet::new();
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
            for op in &survivors {
                if !checked.contains(&op.id) {
                    op.validate()?;
                    checked.insert(op.id);
                }
            }
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
            match self.admit_ops_batch(None, survivors, &checked, Some(reset), None) {
                Err(Error::AdmissionConflict) => continue,
                Err(err) => return Err(err),
                Ok(_) => {}
            }
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

    /// Plan a quarantine by splitting stored ops into reachable survivors and orphans.
    /// Refuse when the frontier has holes or cannot reach the recorded genesis.
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
            if self.storage.get_position(&id)?.is_none() {
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

    fn membership_cache(&self) -> Result<std::sync::MutexGuard<'_, MembershipCache>> {
        self.membership_cache
            .lock()
            .map_err(|_| Error::Storage("membership cache lock poisoned".into()))
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
    pub(crate) fn receive_preverified(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
        verified: &BTreeSet<crate::OpId>,
        effects: Option<ReceiveEffects<'_>>,
    ) -> Result<Admitted> {
        self.receive_ops_admission(source_peer, ops, verified, effects)
    }

    pub fn receive_signed_op(&self, signed: SignedOp) -> Result<Op> {
        let op = Op::new(signed)?;
        self.receive_op(op.clone())?;
        Ok(op)
    }

    /// Admit every buffered op whose dependencies resolved, in finite passes.
    pub fn reconcile_pending_ops(&self) -> Result<BTreeSet<crate::OpId>> {
        let mut accepted = BTreeSet::new();
        loop {
            let pass = self.receive_ops_admission(None, Vec::new(), &BTreeSet::new(), None)?;
            // A pass that admitted nothing met only retained ops; repeating it now spins.
            let progressed = !pass.accepted.is_empty();
            accepted.extend(pass.accepted);
            if !pass.ready_remaining || !progressed {
                return Ok(accepted);
            }
        }
    }

    /// The stored actor clock of `topic_id`: each actor's admitted position.
    pub fn observed_clock(&self, topic_id: &TopicId) -> Result<crate::ActorClock> {
        self.storage.actor_clock(topic_id)
    }
}
