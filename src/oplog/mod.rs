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
mod membership;
mod pending;
mod topology;

pub(crate) use genesis::is_structural_genesis;
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

    /// The batch promoting verified `staged` ops, validated like any admission
    /// against a fresh topic. `None` until their causally complete part makes
    /// both `local` and `source` members.
    pub fn observed_clock(&self, topic_id: &TopicId) -> Result<crate::ActorClock> {
        self.storage.actor_clock(topic_id)
    }
}
