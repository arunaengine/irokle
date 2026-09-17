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
    ActorId, Error, EvictionKey, Op, OpBody, OpId, PeerId, Result, SignedOp, TopicId, TopicPayload,
    actor_id_for,
};

mod admission;
mod creation;
mod genesis;
mod membership;
mod pending;
mod topology;

use admission::{checked_next, ensure_event_type, is_local_race, next_actor_position};
pub(crate) use genesis::is_structural_genesis;
use membership::{apply_control, materialize_topic_state, merge_states};
use pending::pending_meta_for;
pub(crate) use topology::topological_ids;
use topology::topological_ops;
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

/// How much of an op the local store holds. `Repair` means its id is already in
/// the chain but records are incomplete; refilling it is not an append.
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

/// Batch-local validation view, including accepted entries and reset state. A
/// reset removes stored actor slots and tips, so they cannot affect new positions.
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

/// Reports ops discarded from a topic's local chain.
///
#[doc = include_str!("contracts/topic_eviction.md")]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicEviction {
    pub topic_id: TopicId,
    pub losing_genesis: OpId,
    pub winning_genesis: OpId,
    pub evicted: Vec<EvictedOp>,
}

impl TopicEviction {
    /// Identity of this eviction's durable journal record, derived from its content.
    ///
    #[doc = include_str!("contracts/eviction_key.md")]
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

/// Branch and data epoch a whole verdict is recorded under.
fn view_key(view: &TopicView) -> (OpId, u64) {
    (view.state.genesis, view.epoch)
}

/// Ids a topic's stored records reference without resolving them.
fn scan_holes_in(read: &dyn SnapshotRead, topic_id: &TopicId) -> Result<BTreeSet<OpId>> {
    let mut holes = BTreeSet::new();
    for id in read.list_op_ids(topic_id)? {
        let Some(meta) = read.get_position(&id)? else {
            holes.insert(id);
            continue;
        };
        if read.get_op(&id)?.is_none() {
            holes.insert(id);
        }
        for dep in &meta.deps {
            if !read.dep_resolvable(dep)? {
                holes.insert(*dep);
            }
        }
    }
    Ok(holes)
}

#[derive(Clone)]
pub struct Oplog<S = MemoryStorage> {
    storage: S,
    // Branch and data epoch each topic was scanned and found whole at. Admission
    // keeps that invariant, so only a reset or damage from outside irokle can
    // reintroduce a hole; scanning once per epoch keeps sync off a full scan.
    whole_topics: Arc<Mutex<BTreeMap<TopicId, (OpId, u64)>>>,
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
            whole_topics: Arc::new(Mutex::new(BTreeMap::new())),
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
            whole_topics: Arc::new(Mutex::new(BTreeMap::new())),
            membership_cache: Arc::clone(&self.membership_cache),
            receive_genesis: self.receive_genesis,
        }
    }

    /// Holes that keep this topic from being locally complete.
    ///
    #[doc = include_str!("contracts/topic_unresolved.md")]
    pub fn topic_unresolved(&self, topic_id: &TopicId) -> Result<BTreeSet<crate::OpId>> {
        self.storage
            .read_snapshot(|read| match read.topic_view(topic_id, None)? {
                Some(view) => self.unresolved_in(read, &view),
                None => scan_holes_in(read, topic_id),
            })
    }

    /// Ids `view`'s topic cannot resolve, scanned in a later snapshot when the
    /// cache has no verdict. The verdict is recorded only when that snapshot
    /// still holds the view's branch and epoch.
    #[cfg(feature = "iroh")]
    pub(crate) fn view_unresolved(&self, view: &TopicView) -> Result<BTreeSet<OpId>> {
        if self.whole_topics()?.get(&view.state.topic_id) == Some(&view_key(view)) {
            return Ok(view.pending_missing.clone());
        }
        self.storage.read_snapshot(|read| {
            let current = read.topic_view(&view.state.topic_id, None)?;
            match current.filter(|current| view_key(current) == view_key(view)) {
                Some(current) => self.unresolved_in(read, &current),
                None => {
                    let mut unresolved = view.pending_missing.clone();
                    unresolved.extend(scan_holes_in(read, &view.state.topic_id)?);
                    Ok(unresolved)
                }
            }
        })
    }

    /// Ids `view`'s topic cannot resolve, where `view` was read from `read`. A
    /// scan of that same snapshot is recorded as whole under its branch and epoch.
    pub(crate) fn unresolved_in(
        &self,
        read: &dyn SnapshotRead,
        view: &TopicView,
    ) -> Result<BTreeSet<OpId>> {
        let topic_id = view.state.topic_id;
        let mut unresolved = view.pending_missing.clone();
        if self.whole_topics()?.get(&topic_id) == Some(&view_key(view)) {
            return Ok(unresolved);
        }
        let holes = scan_holes_in(read, &topic_id)?;
        if holes.is_empty() {
            self.whole_topics()?.insert(topic_id, view_key(view));
        }
        unresolved.extend(holes);
        Ok(unresolved)
    }

    /// A view of the topic and whether it is whole, both from one snapshot.
    pub(crate) fn whole_view(&self, topic_id: &TopicId) -> Result<Option<(TopicView, bool)>> {
        self.storage.read_snapshot(|read| {
            let Some(view) = read.topic_view(topic_id, None)? else {
                return Ok(None);
            };
            let whole = self.unresolved_in(read, &view)?.is_empty();
            Ok(Some((view, whole)))
        })
    }

    /// Clear the cache of topics known to be whole.
    ///
    #[doc = include_str!("contracts/recheck_topics.md")]
    pub fn recheck_topics(&self) -> Result<()> {
        self.whole_topics()?.clear();
        *self.membership_cache()? = MembershipCache::default();
        Ok(())
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

    /// Repair a topic by discarding the ops outside its head closure.
    ///
    #[doc = include_str!("contracts/quarantine_orphans.md")]
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

    fn admit_with_retry(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
        verified: &BTreeSet<crate::OpId>,
        effects: Option<ReceiveEffects<'_>>,
    ) -> Result<(BTreeSet<crate::OpId>, Option<TopicEviction>)> {
        if let Some((topic, genesis)) = self.receive_genesis {
            for op in ops
                .iter()
                .filter(|op| op.signed.body.topic_id == topic && is_structural_genesis(op))
            {
                if op.id != genesis {
                    return Err(Error::StaleIncarnation);
                }
            }
        }
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
        let topic_id = ops.first().map(|op| op.signed.body.topic_id);
        let heads = || {
            topic_id
                .map(|topic_id| self.storage.heads(&topic_id))
                .transpose()
        };
        for attempt in 0..MAX_ADMISSION_RETRIES {
            conflict_pause(attempt);
            let before = heads()?;
            if let Some((topic, genesis)) = self.receive_genesis
                && ops
                    .first()
                    .is_some_and(|op| op.signed.body.topic_id == topic)
                && self
                    .storage
                    .topic_state(&topic)?
                    .is_some_and(|state| state.genesis < genesis)
            {
                return Err(Error::StaleIncarnation);
            }
            let (ops_to_admit, reset, rejected_genesis) = if has_genesis {
                self.resolve_genesis_collision(ops.clone(), verified)?
            } else {
                (ops.clone(), None, None)
            };
            let eviction = reset.as_ref().map(|plan| plan.eviction.clone());
            if let Some(losing) = rejected_genesis {
                // A losing genesis is never admitted here, so its waiters can never become ready.
                self.storage.purge_pending_waiters(&losing)?;
            }
            // A won foreign genesis discards the local chain: admit the winner
            // batch against a fresh topic and fold the reset into the same
            // storage transaction as its admission (`reset_topic_and_admit`).
            match self.admit_ops_batch(source_peer, ops_to_admit, verified, reset, effects) {
                Err(Error::AdmissionConflict) => continue,
                // Validation reads the store op by op, so a commit landing meanwhile can
                // skip ops the store now holds and fake a gap; moved heads retry it.
                Err(err) if is_local_race(&err) && heads()? != before => continue,
                Ok(accepted) => return Ok((accepted, eviction)),
                Err(err) => return Err(err),
            }
        }
        Err(Error::AdmissionConflict)
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
        if let Some((topic, genesis)) = self.receive_genesis
            && topic == topic_id
            && expected_state
                .as_ref()
                .map(|state| state.genesis)
                .or_else(|| {
                    ops.iter()
                        .find(|op| is_structural_genesis(op))
                        .map(|op| op.id)
                })
                != Some(genesis)
        {
            return Err(Error::StaleIncarnation);
        }
        let mut topic_state_changed = false;
        let mut overlay_ops = BTreeMap::new();
        let mut overlay_meta = BTreeMap::new();
        let mut overlay_tips = BTreeMap::new();
        let mut overlay_index = BTreeMap::new();
        let mut admitted = Vec::new();
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
                // Records of a topic without state exist only while an
                // activation is unfinished; that history is not this batch's.
                if state.is_none() {
                    return Err(Error::AdmissionConflict);
                }
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
                            let prev_meta = self.header_projected(&prev, &overlay_meta)?;
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
                        let dep_meta = self.header_projected(id, &overlay_meta)?;
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
                    overlay_meta.insert(op.id, meta);
                    accepted.insert(op.id);
                    admitted.push(op.id);
                    overlay_ops.insert(op.id, op);
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
            for dep in &op.signed.body.deps {
                heads.remove(dep);
            }
            heads.insert(op.id);
            match &op.signed.body.payload {
                TopicPayload::Genesis(genesis) => {
                    state = Some(TopicState {
                        topic_id,
                        event_type_id: genesis.event_type_id.clone(),
                        genesis: op.id,
                        heads: BTreeSet::new(),
                        members: genesis.initial_peers.clone(),
                        replication_policy: genesis.replication_policy.clone(),
                        membership_controls: BTreeMap::new(),
                        replication_policy_control: None,
                    });
                    topic_state_changed = true;
                }
                TopicPayload::Event(_) => {}
                TopicPayload::Control(control) => {
                    let state = state.as_mut().ok_or(Error::TopicNotFound)?;
                    apply_control(state, &op, control);
                    topic_state_changed = true;
                }
            }

            overlay_index.insert((topic_id, meta.actor_id, meta.actor_seq), op.id);
            overlay_tips.insert((topic_id, meta.actor_id), (meta.actor_seq, op.id));
            overlay_meta.insert(op.id, meta);
            accepted.insert(op.id);
            admitted.push(op.id);
            overlay_ops.insert(op.id, op);
        }
        // Records move out of the overlay, so the batch holds one copy of each
        // op and its observed clock. Ops are unique by id, so each is there.
        let entries = admitted
            .into_iter()
            .map(|id| Some((overlay_ops.remove(&id)?, overlay_meta.remove(&id)?)))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| Error::Storage("admitted op left the batch overlay".into()))?;

        if let Some(state) = &mut state {
            state.heads = heads.clone();
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

    fn stored_op_state(&self, op: &Op) -> Result<StoredOp> {
        let has_op = self.storage.get_op(&op.id)?.is_some();
        let has_meta = self.storage.get_position(&op.id)?.is_some();
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

        // Buffer pending ops after admission, so a reset cannot wipe the descendants of a
        // partial winner batch. A pending op's missing deps are never in `entries`, so
        // ordering after admission cannot spuriously reject it.
        for (op, missing_deps) in pending {
            let source_peer = source_peer.unwrap_or(op.signed.body.author);
            let buffered = self.storage.put_pending_bound(
                source_peer,
                op.clone(),
                pending_meta_for(&op, missing_deps),
                self.receive_genesis
                    .filter(|(topic, _)| *topic == topic_id)
                    .map(|(_, genesis)| genesis),
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
    pub fn observed_clock(&self, topic_id: &TopicId) -> Result<crate::ActorClock> {
        self.storage.actor_clock(topic_id)
    }

    fn meta_for_projected(
        &self,
        op: &Op,
        overlay_meta: &BTreeMap<crate::OpId, OpMeta>,
    ) -> Result<OpMeta> {
        let body = &op.signed.body;
        let mut observed_clock = crate::ActorClock::new();
        for dep in &body.deps {
            let (meta, clock) = match overlay_meta.get(dep) {
                Some(meta) => (
                    crate::storage::OpHeader::from(meta),
                    meta.observed_clock.clone(),
                ),
                None => self
                    .storage
                    .get_observation(dep)?
                    .ok_or(Error::MissingDependency(*dep))?,
            };
            if meta.topic_id != body.topic_id {
                return Err(Error::TopicMismatch);
            }
            observed_clock.merge(&clock);
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

    fn header_projected(
        &self,
        id: &OpId,
        overlay_meta: &BTreeMap<OpId, OpMeta>,
    ) -> Result<crate::storage::OpHeader> {
        match overlay_meta.get(id) {
            Some(meta) => Ok(crate::storage::OpHeader::from(meta)),
            None => self
                .storage
                .get_header(id)?
                .ok_or(Error::MissingDependency(*id)),
        }
    }

    fn meta_projected<'a>(
        &self,
        id: &crate::OpId,
        overlay_meta: &'a BTreeMap<crate::OpId, OpMeta>,
    ) -> Result<std::borrow::Cow<'a, OpMeta>> {
        match overlay_meta.get(id) {
            Some(meta) => Ok(std::borrow::Cow::Borrowed(meta)),
            None => self
                .storage
                .get_meta(id)?
                .ok_or(Error::MissingDependency(*id))
                .map(std::borrow::Cow::Owned),
        }
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
            let meta = self.header_projected(id, overlay.meta)?;
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

    /// Return the stored actor slot, or `None` during reset. Reset clears actor slots
    /// and tips in the same transaction, so validation must ignore the old chain.
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

    /// Re-read storage after a tip or sequence mismatch. Only a fully stored op
    /// is a duplicate; half-stored data must remain repairable.
    fn is_admitted_duplicate(&self, op: &Op) -> Result<bool> {
        self.storage.dep_resolvable(&op.id)
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
                    apply_control(&mut state, &op, control);
                    Arc::new(state)
                }
            };
            visiting.remove(&id);
            projections.insert(id, state);
        }
        merge_states(deps, projections)
    }
}
