// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use crate::{
    ActorClock, ActorId, Error, EvictionKey, Op, OpId, PeerId, Result, TopicEviction, TopicId,
    TopicInfo,
};

use crate::storage::{
    AckCommit, AdmissionEffects, AdmittedBatch, CounterSnapshot, MAX_PENDING_EVICTIONS,
    MAX_PENDING_MISSING_DEPS as MAX_MISSING_DEPS, MAX_PENDING_WAITERS_PER_DEP as MAX_WAITERS,
    MAX_REJECTED_PER_TOPIC as MAX_REJECTED, ObligationTarget, OpMeta, OpPosition, PeerAck,
    PendingRecord, PendingUsage, ProvisionalTopic, SnapshotRead, StagingLimits, StagingQuota,
    Storage, StorageCounters, SyncObligation, SyncPeerStatus, SyncStatusUpdate, TopicState,
    TopicView, ack_commit, ack_covers, ack_reached_op, apply_status_update, branch_matches,
    check_pending_quota, ensure_deps_resolvable, journalled_eviction, merged_obligation,
    merged_peer_ack, new_peer_status, peer_departed, pending_op_bytes, settled_obligation,
    stored_ack_dominates, topic_fingerprint_for, validate_batch, validate_heads,
};

mod budget;
mod metadata;
mod staging;

pub use budget::{MemoryDomain, MemoryLimits, MemoryUsage};

use budget::{Budget, Charge};
use metadata::{MetadataKey, MetadataPlan};
use staging::Staging;

#[derive(Clone)]
struct RecordCharge {
    operation: Arc<Charge>,
    metadata: Arc<Charge>,
}

#[derive(Clone)]
pub struct MemoryStorage {
    inner: Arc<Mutex<MemoryInner>>,
    counters: Arc<StorageCounters>,
    limits: StagingLimits,
    /// Provisional namespaces, shared by the store and every namespace view.
    staging: Arc<Mutex<Staging>>,
    /// Source, topic and session a namespace view stages under; `None` for the main store.
    namespace: Option<(PeerId, TopicId, u64)>,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self {
            inner: Arc::default(),
            counters: Arc::default(),
            limits: StagingLimits::MEMORY,
            staging: Arc::default(),
            namespace: None,
        }
    }
}

#[derive(Clone, Default)]
struct MemoryInner {
    budget: Arc<Budget>,
    charges: BTreeMap<OpId, RecordCharge>,
    metadata: BTreeMap<MetadataKey, Arc<Charge>>,
    namespace_charge: Option<Arc<Charge>>,
    ops: BTreeMap<OpId, Op>,
    meta: BTreeMap<OpId, OpMeta>,
    topic_ops: BTreeMap<TopicId, BTreeSet<OpId>>,
    heads: BTreeMap<TopicId, BTreeSet<OpId>>,
    children: BTreeMap<OpId, BTreeSet<OpId>>,
    actor_by_seq: BTreeMap<(TopicId, ActorId, u64), OpId>,
    actor_tip: BTreeMap<(TopicId, ActorId), (u64, OpId)>,
    actor_clock: BTreeMap<TopicId, ActorClock>,
    topic_fingerprint: BTreeMap<TopicId, [u8; 32]>,
    max_generation: BTreeMap<TopicId, u64>,
    topics: BTreeMap<TopicId, TopicState>,
    pending_ops: BTreeMap<OpId, Op>,
    pending_records: BTreeMap<OpId, PendingRecord>,
    pending_by_topic: BTreeMap<TopicId, BTreeSet<OpId>>,
    pending_waiters: BTreeMap<OpId, BTreeSet<OpId>>,
    pending_ready: BTreeSet<OpId>,
    pending_usage: PendingUsage,
    source_usage: BTreeMap<PeerId, PendingUsage>,
    topic_usage: BTreeMap<TopicId, PendingUsage>,
    rejected: BTreeMap<TopicId, RejectedIds>,
    peer_acks: BTreeMap<(PeerId, TopicId), PeerAck>,
    /// Keyed topic first, so one topic's records are a range.
    obligations: BTreeMap<(TopicId, PeerId), BTreeMap<ObligationKind, SyncObligation>>,
    sync_statuses: BTreeMap<(TopicId, PeerId), SyncPeerStatus>,
    evictions: BTreeMap<EvictionKey, TopicEviction>,
    sealed_topics: BTreeSet<TopicId>,
    /// Destructive data epochs; a reset keeps and advances them.
    topic_epochs: BTreeMap<TopicId, u64>,
    attempt_epoch: u64,
    /// Serialized bytes of admitted ops, counted in a namespace store only.
    admitted_bytes: u64,
}

/// Rejected ids of one topic, bounded by dropping the oldest.
#[derive(Clone, Default)]
struct RejectedIds {
    order: VecDeque<OpId>,
    ids: BTreeSet<OpId>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// The same store applying `limits` to provisional bootstraps.
    pub fn with_staging_limits(mut self, limits: StagingLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn with_memory_limits(self, limits: MemoryLimits) -> Result<Self> {
        self.main_store()?;
        let budget = Arc::clone(&self.lock()?.budget);
        budget.configure(limits)?;
        Ok(self)
    }

    pub fn memory_usage(&self) -> Result<MemoryUsage> {
        let budget = Arc::clone(&self.lock()?.budget);
        Ok(budget.usage())
    }

    /// Work this store and its clones performed so far.
    pub fn counters(&self) -> CounterSnapshot {
        self.counters.snapshot()
    }
}

#[cfg(test)]
impl MemoryStorage {
    /// Pool usage in total and for `source_peer`: ops and bytes of each.
    #[cfg(test)]
    pub(crate) fn pending_usage(&self, source_peer: &PeerId) -> (u64, u64, u64, u64) {
        let inner = self.inner.lock().expect("memory lock");
        let source = inner
            .source_usage
            .get(source_peer)
            .copied()
            .unwrap_or_default();
        (
            inner.pending_usage.ops,
            inner.pending_usage.bytes,
            source.ops,
            source.bytes,
        )
    }

    /// Erase a stored op record, leaving its metadata and indexes behind. Tests
    /// use this to build a store that is already durably inconsistent.
    pub(crate) fn drop_op_record(&self, id: &OpId) {
        self.inner.lock().expect("memory lock").ops.remove(id);
    }

    /// Erase a stored metadata record, leaving the op and indexes behind.
    pub(crate) fn drop_meta_record(&self, id: &OpId) {
        self.inner.lock().expect("memory lock").meta.remove(id);
    }

    /// Store an ack record as is, bypassing certification. Tests use this for
    /// evidence an older schema left behind.
    #[cfg(feature = "iroh")]
    pub(crate) fn put_raw_ack(&self, ack: PeerAck) {
        self.inner
            .lock()
            .expect("memory lock")
            .peer_acks
            .insert((ack.peer_id, ack.topic_id), ack);
    }

    /// Store both records and the topic/child indexes while leaving heads and
    /// topic state alone, so the op is admitted yet reachable from no head.
    pub(crate) fn orphan_op(&self, op: &Op, meta: &OpMeta) {
        let mut inner = self.inner.lock().expect("memory lock");
        inner
            .topic_ops
            .entry(meta.topic_id)
            .or_default()
            .insert(op.id);
        for dep in &meta.deps {
            inner.children.entry(*dep).or_default().insert(op.id);
        }
        inner
            .actor_by_seq
            .insert((meta.topic_id, meta.actor_id, meta.actor_seq), op.id);
        inner
            .actor_tip
            .insert((meta.topic_id, meta.actor_id), (meta.actor_seq, op.id));
        inner.meta.insert(op.id, meta.clone());
        inner.ops.insert(op.id, op.clone());
    }
}

impl Storage for MemoryStorage {
    fn read_snapshot<R>(&self, read: impl FnOnce(&dyn SnapshotRead) -> Result<R>) -> Result<R> {
        let inner = self.lock()?;
        read(&MemorySnapshot {
            inner: &inner,
            counters: &self.counters,
        })
    }
    fn put_admitted_batch(&self, batch: AdmittedBatch) -> Result<()> {
        let mut inner = self.lock()?;
        let quota = inner.quota(&self.limits);
        admit_batch_locked(&mut inner, batch, quota)
    }

    fn get_op(&self, id: &OpId) -> Result<Option<Op>> {
        self.counters.count_op();
        Ok(self.lock()?.ops.get(id).cloned())
    }
    fn get_meta(&self, id: &OpId) -> Result<Option<OpMeta>> {
        self.counters.count_meta();
        Ok(self.lock()?.meta.get(id).cloned())
    }
    fn get_position(&self, id: &OpId) -> Result<Option<OpPosition>> {
        self.counters.count_meta();
        Ok(self.lock()?.meta.get(id).map(OpPosition::from))
    }
    fn dep_resolvable(&self, id: &OpId) -> Result<bool> {
        let inner = self.lock()?;
        Ok(dep_resolvable_locked(&inner, id))
    }
    fn list_ops(&self, topic_id: &TopicId) -> Result<Vec<Op>> {
        let inner = self.lock()?;
        Ok(inner
            .topic_ops
            .get(topic_id)
            .into_iter()
            .flatten()
            .filter_map(|id| inner.ops.get(id).cloned())
            .collect())
    }
    fn list_op_ids(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>> {
        Ok(topic_ids_locked(&*self.lock()?, topic_id))
    }
    fn heads(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>> {
        Ok(self
            .lock()?
            .heads
            .get(topic_id)
            .cloned()
            .unwrap_or_default())
    }
    fn children(&self, op_id: &OpId) -> Result<BTreeSet<OpId>> {
        Ok(self
            .lock()?
            .children
            .get(op_id)
            .cloned()
            .unwrap_or_default())
    }
    fn actor_tip(&self, topic_id: &TopicId, actor_id: &ActorId) -> Result<Option<(u64, OpId)>> {
        Ok(self.lock()?.actor_tip.get(&(*topic_id, *actor_id)).cloned())
    }
    fn actor_index(
        &self,
        topic_id: &TopicId,
        actor_id: &ActorId,
        seq: u64,
    ) -> Result<Option<OpId>> {
        Ok(self
            .lock()?
            .actor_by_seq
            .get(&(*topic_id, *actor_id, seq))
            .copied())
    }
    fn actor_range(
        &self,
        topic_id: &TopicId,
        actor_id: &ActorId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, OpId)>> {
        let range = actor_range_locked(&*self.lock()?, topic_id, actor_id, after, limit);
        self.counters.count_index(range.len());
        Ok(range)
    }
    fn actor_clock(&self, topic_id: &TopicId) -> Result<ActorClock> {
        Ok(self
            .lock()?
            .actor_clock
            .get(topic_id)
            .cloned()
            .unwrap_or_default())
    }
    fn topic_fingerprint(&self, topic_id: &TopicId) -> Result<[u8; 32]> {
        let inner = self.lock()?;
        Ok(inner
            .topic_fingerprint
            .get(topic_id)
            .copied()
            .unwrap_or_else(|| {
                topic_fingerprint_for(
                    &inner.heads.get(topic_id).cloned().unwrap_or_default(),
                    &inner.actor_clock.get(topic_id).cloned().unwrap_or_default(),
                )
                .expect("topic fingerprint serialization is infallible for in-memory ids")
            }))
    }
    fn max_generation(&self, topic_id: &TopicId) -> Result<u64> {
        Ok(self
            .lock()?
            .max_generation
            .get(topic_id)
            .copied()
            .unwrap_or_default())
    }
    fn topic_state(&self, topic_id: &TopicId) -> Result<Option<TopicState>> {
        let inner = self.lock()?;
        Ok(inner.topics.get(topic_id).cloned().map(|mut state| {
            state.heads = inner.heads.get(topic_id).cloned().unwrap_or_default();
            state
        }))
    }
    fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        Ok(self
            .lock()?
            .topics
            .values()
            .map(|s| TopicInfo {
                topic_id: s.topic_id,
                event_type_id: s.event_type_id.clone(),
                genesis: s.genesis,
            })
            .collect())
    }
    fn topic_view(
        &self,
        topic_id: &TopicId,
        peer_id: Option<&PeerId>,
    ) -> Result<Option<TopicView>> {
        topic_view_locked(&*self.lock()?, topic_id, peer_id)
    }
    fn peer_reached_op(&self, peer_id: &PeerId, op_id: &OpId) -> Result<bool> {
        let inner = self.lock()?;
        let Some((meta, genesis)) = stored_op_branch(&inner, op_id) else {
            return Ok(false);
        };
        Ok(inner
            .peer_acks
            .get(&(*peer_id, meta.topic_id))
            .is_some_and(|ack| ack_reached_op(ack, genesis, op_id, &meta.into())))
    }
    fn peers_reached_op(&self, op_id: &OpId) -> Result<Vec<PeerId>> {
        let inner = self.lock()?;
        let Some((meta, genesis)) = stored_op_branch(&inner, op_id) else {
            return Ok(Vec::new());
        };
        let position = meta.into();
        let mut peers = inner
            .peer_acks
            .values()
            .filter(|ack| {
                ack.topic_id == meta.topic_id && ack_reached_op(ack, genesis, op_id, &position)
            })
            .map(|ack| ack.peer_id)
            .collect::<Vec<_>>();
        peers.sort();
        Ok(peers)
    }
    fn put_pending_op(&self, source_peer: PeerId, op: Op, meta: OpMeta) -> Result<()> {
        self.put_pending_bound(source_peer, op, meta, None)
    }
    fn put_pending_bound(
        &self,
        source_peer: PeerId,
        mut op: Op,
        meta: OpMeta,
        genesis: Option<OpId>,
    ) -> Result<()> {
        let charge = pending_op_bytes(&op)? as u64;
        let topic_id = op.signed.body.topic_id;
        if meta.topic_id != topic_id || meta.id != op.id {
            return Err(Error::TopicMismatch);
        }
        if meta.missing_deps.len() > MAX_MISSING_DEPS {
            return Err(Error::Storage(
                "pending op has too many missing deps".into(),
            ));
        }
        let mut inner = self.lock()?;
        if let Some(genesis) = genesis {
            branch_matches(inner.topics.get(&topic_id), Some(genesis))?;
        }
        // Only a completely stored op is already admitted; a half stored one
        // still has to buffer so its repair runs once its deps resolve.
        if dep_resolvable_locked(&inner, &op.id) {
            return Ok(());
        }
        if meta
            .missing_deps
            .iter()
            .any(|dep| dep_resolvable_locked(&inner, dep))
        {
            return Err(Error::AdmissionConflict);
        }
        if let Some(rejected) = inner.rejected.get(&topic_id).and_then(|rejected| {
            std::iter::once(&op.id)
                .chain(&meta.missing_deps)
                .find(|id| rejected.ids.contains(id))
        }) {
            return Err(Error::RejectedOp(*rejected));
        }
        let previous = match inner.pending_records.get(&op.id) {
            Some(_) if inner.pending_ops.get(&op.id) != Some(&op) => {
                return Err(Error::Storage(
                    "pending op id collision with different op".into(),
                ));
            }
            Some(record) if record.missing == meta.missing_deps => return Ok(()),
            Some(record) => Some(record.missing.clone()),
            None => None,
        };
        for dep in &meta.missing_deps {
            let already = previous
                .as_ref()
                .is_some_and(|missing| missing.contains(dep));
            if !already && inner.pending_waiters.get(dep).map_or(0, BTreeSet::len) >= MAX_WAITERS {
                return Err(Error::Storage("pending waiter quota exceeded".into()));
            }
        }
        if previous.is_none()
            && let Some(quota) = inner.quota(&self.limits)
        {
            quota.check(inner.admitted_bytes + inner.pending_usage.bytes, charge)?;
        }
        if previous.is_none() {
            check_pending_quota(
                inner.pending_usage,
                inner
                    .source_usage
                    .get(&source_peer)
                    .copied()
                    .unwrap_or_default(),
                inner
                    .topic_usage
                    .get(&topic_id)
                    .copied()
                    .unwrap_or_default(),
                charge,
            )?;
        }

        // A known op keeps its stored source and charge; only its waits move.
        let reserved = match inner.charges.get(&op.id) {
            Some(charge) => charge.clone(),
            None => charge_record(&inner.budget, &op, &meta)?,
        };
        own_payload(&mut op.signed.body.payload);
        inner.charges.insert(op.id, reserved);
        let previous = previous.unwrap_or_default();
        for dep in previous.difference(&meta.missing_deps) {
            unwait_locked(&mut inner, dep, &op.id);
        }
        for dep in &meta.missing_deps {
            inner.pending_waiters.entry(*dep).or_default().insert(op.id);
        }
        if meta.missing_deps.is_empty() {
            inner.pending_ready.insert(op.id);
        } else {
            inner.pending_ready.remove(&op.id);
        }
        if let Some(record) = inner.pending_records.get_mut(&op.id) {
            record.missing = meta.missing_deps;
            return Ok(());
        }
        inner.pending_usage = inner.pending_usage.charged(charge);
        let source = inner.source_usage.entry(source_peer).or_default();
        *source = source.charged(charge);
        let topic = inner.topic_usage.entry(topic_id).or_default();
        *topic = topic.charged(charge);
        inner
            .pending_by_topic
            .entry(topic_id)
            .or_default()
            .insert(op.id);
        inner.pending_records.insert(
            op.id,
            PendingRecord {
                source: source_peer,
                topic_id,
                missing: meta.missing_deps,
                charge,
            },
        );
        inner.pending_ops.insert(op.id, op);
        Ok(())
    }
    fn pending_waiters(&self, dep_id: &OpId) -> Result<Vec<(PeerId, Op)>> {
        let inner = self.lock()?;
        let waiters = inner
            .pending_waiters
            .get(dep_id)
            .into_iter()
            .flatten()
            .filter_map(|op_id| pending_entry_locked(&inner, op_id))
            .collect::<Vec<_>>();
        self.counters.count_payloads(waiters.len());
        Ok(waiters)
    }
    fn ready_pending_after(&self, after: Option<&OpId>, limit: usize) -> Result<Vec<(PeerId, Op)>> {
        let inner = self.lock()?;
        let start = match after {
            Some(after) => std::ops::Bound::Excluded(*after),
            None => std::ops::Bound::Unbounded,
        };
        let ready = inner
            .pending_ready
            .range((start, std::ops::Bound::Unbounded))
            .take(limit)
            .filter_map(|op_id| pending_entry_locked(&inner, op_id))
            .collect::<Vec<_>>();
        self.counters.count_payloads(ready.len());
        Ok(ready)
    }
    fn pending_missing_deps(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>> {
        let inner = self.lock()?;
        Ok(pending_missing_locked(&inner, topic_id))
    }
    fn is_pending(&self, op_id: &OpId) -> Result<bool> {
        Ok(self.lock()?.pending_records.contains_key(op_id))
    }
    fn remove_pending_op(&self, op_id: &OpId) -> Result<()> {
        let mut inner = self.lock()?;
        remove_pending_locked(&mut inner, op_id)
    }
    fn purge_pending_waiters(&self, dep_id: &OpId) -> Result<usize> {
        let mut inner = self.lock()?;
        let closure = waiter_closure_locked(&inner, dep_id);
        for op_id in &closure {
            remove_pending_locked(&mut inner, op_id)?;
        }
        Ok(closure.len())
    }
    fn reject_pending_subtree(&self, op_id: &OpId) -> Result<usize> {
        let mut inner = self.lock()?;
        // One guard covers the markers, the root and its closure, so no reader
        // sees the root gone while a waiter still holds quota against it.
        let Some(topic_id) = inner
            .pending_records
            .get(op_id)
            .map(|record| record.topic_id)
        else {
            return Ok(0);
        };
        let mut subtree = waiter_closure_locked(&inner, op_id);
        subtree.insert(*op_id);
        let mut reservation = MetadataPlan::new(&inner)?;
        reservation.reserve(
            &inner,
            MetadataKey::Rejected(topic_id),
            4096 + MAX_REJECTED as u64 * 256,
        )?;
        reservation.commit(&mut inner);
        for id in &subtree {
            remove_pending_locked(&mut inner, id)?;
        }
        let rejected = inner.rejected.entry(topic_id).or_default();
        for id in &subtree {
            if rejected.ids.insert(*id) {
                rejected.order.push_back(*id);
            }
        }
        while rejected.order.len() > MAX_REJECTED {
            if let Some(oldest) = rejected.order.pop_front() {
                rejected.ids.remove(&oldest);
            }
        }
        Ok(subtree.len())
    }
    fn peer_ack(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<Option<PeerAck>> {
        Ok(self.lock()?.peer_acks.get(&(*peer_id, *topic_id)).cloned())
    }
    fn peer_acks(&self, topic_id: &TopicId) -> Result<Vec<PeerAck>> {
        Ok(self
            .lock()?
            .peer_acks
            .values()
            .filter(|ack| ack.topic_id == *topic_id)
            .cloned()
            .collect())
    }
    fn put_sync_obligation(
        &self,
        obligation: SyncObligation,
        expected_genesis: Option<OpId>,
    ) -> Result<()> {
        let mut inner = self.lock()?;
        branch_matches(inner.topics.get(&obligation.topic_id), expected_genesis)?;
        let _workspace = metadata::merge_workspace(&inner, std::iter::once(&obligation))?;
        let merged = merged_obligation_locked(&inner, &obligation)?;
        let mut reservation = MetadataPlan::new(&inner)?;
        reservation.obligation(&inner, &merged)?;
        reservation.commit(&mut inner);
        put_obligation_locked(&mut inner, merged);
        Ok(())
    }

    fn all_sync_obligations(&self) -> Result<Vec<SyncObligation>> {
        let out: Vec<_> = self
            .lock()?
            .obligations
            .values()
            .flat_map(|records| records.values().cloned())
            .collect();
        self.counters.count_obligations(out.len());
        Ok(out)
    }

    fn apply_peer_ack(&self, ack: PeerAck) -> Result<usize> {
        let mut inner = self.lock()?;
        apply_ack_locked(&mut inner, ack)
    }

    fn apply_peer_acks(&self, acks: Vec<PeerAck>) -> Result<Vec<Result<usize>>> {
        let mut inner = self.lock()?;
        Ok(acks
            .into_iter()
            .map(|ack| apply_ack_locked(&mut inner, ack))
            .collect())
    }

    fn sync_obligations(
        &self,
        peer_id: &PeerId,
        topic_id: &TopicId,
    ) -> Result<Vec<SyncObligation>> {
        let out: Vec<_> = self
            .lock()?
            .obligations
            .get(&(*topic_id, *peer_id))
            .into_iter()
            .flat_map(|records| records.values().cloned())
            .collect();
        self.counters.count_obligations(out.len());
        Ok(out)
    }

    fn sync_obligation_count(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<usize> {
        Ok(self
            .lock()?
            .obligations
            .get(&(*topic_id, *peer_id))
            .map_or(0, BTreeMap::len))
    }

    fn has_sync_obligations(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<bool> {
        Ok(self
            .lock()?
            .obligations
            .get(&(*topic_id, *peer_id))
            .is_some_and(|records| !records.is_empty()))
    }

    fn next_attempt_epoch(&self) -> Result<u64> {
        let mut inner = self.lock()?;
        inner.attempt_epoch = inner
            .attempt_epoch
            .checked_add(1)
            .ok_or_else(|| Error::Storage("attempt epoch overflow".into()))?;
        Ok(inner.attempt_epoch)
    }

    fn put_sync_status(&self, status: SyncPeerStatus) -> Result<()> {
        let mut inner = self.lock()?;
        let mut reservation = MetadataPlan::control(&inner)?;
        reservation.status(&inner, &status)?;
        reservation.commit(&mut inner);
        inner
            .sync_statuses
            .insert((status.topic_id, status.peer_id), status);
        Ok(())
    }

    fn update_sync_status(
        &self,
        peer_id: &PeerId,
        topic_id: &TopicId,
        update: &SyncStatusUpdate,
    ) -> Result<SyncPeerStatus> {
        let mut inner = self.lock()?;
        let key = (*topic_id, *peer_id);
        let _workspace = inner.budget.reserve(
            MemoryDomain::Control,
            4096 + inner
                .sync_statuses
                .get(&key)
                .and_then(|s| s.last_error.as_ref())
                .map_or(0, String::len) as u64
                + update
                    .last_error
                    .as_ref()
                    .and_then(Option::as_ref)
                    .map_or(0, String::len) as u64,
        )?;
        let mut status = inner
            .sync_statuses
            .get(&key)
            .cloned()
            .unwrap_or_else(|| new_peer_status(*peer_id, *topic_id));
        // A rejected update leaves no record behind for a peer that had none.
        if apply_status_update(&mut status, update) {
            let mut reservation = MetadataPlan::control(&inner)?;
            reservation.status(&inner, &status)?;
            reservation.commit(&mut inner);
            inner.sync_statuses.insert(key, status.clone());
        }
        Ok(status)
    }

    fn topic_obligation_counts(&self, topic_id: &TopicId) -> Result<BTreeMap<PeerId, usize>> {
        let inner = self.lock()?;
        let mut counts = BTreeMap::new();
        let first = (*topic_id, PeerId::from_bytes([0; 32]));
        for ((_, peer_id), records) in inner
            .obligations
            .range(first..)
            .take_while(|((topic, _), _)| topic == topic_id)
        {
            if !records.is_empty() {
                counts.insert(*peer_id, records.len());
            }
        }
        Ok(counts)
    }

    fn clear_peer_sync_state(
        &self,
        peer_id: &PeerId,
        topic_id: &TopicId,
        expected_genesis: Option<OpId>,
    ) -> Result<usize> {
        let mut inner = self.lock()?;
        if !peer_departed(inner.topics.get(topic_id), peer_id, expected_genesis) {
            return Ok(0);
        }
        let cleared = inner
            .obligations
            .remove(&(*topic_id, *peer_id))
            .map_or(0, |records| records.len());
        inner.sync_statuses.remove(&(*topic_id, *peer_id));
        inner.peer_acks.remove(&(*peer_id, *topic_id));
        inner
            .metadata
            .retain(|key, _| !key.peer(*topic_id, *peer_id));
        Ok(cleared)
    }

    fn sync_statuses(&self, topic_id: &TopicId) -> Result<Vec<SyncPeerStatus>> {
        Ok(self
            .lock()?
            .sync_statuses
            .values()
            .filter(|status| status.topic_id == *topic_id)
            .cloned()
            .collect())
    }

    fn reset_topic(&self, topic_id: &TopicId) -> Result<usize> {
        let mut inner = self.lock()?;
        reset_topic_locked(&mut inner, topic_id)
    }

    fn reset_topic_and_admit(
        &self,
        topic_id: &TopicId,
        expected_topic_state: &TopicState,
        batch: AdmittedBatch,
        eviction: Option<&TopicEviction>,
    ) -> Result<usize> {
        // A namespace holds one candidate branch; replacing it is the node's decision.
        if self.namespace.is_some() {
            return Err(Error::StaleIncarnation);
        }
        let mut inner = self.lock()?;
        if inner.sealed_topics.contains(topic_id) {
            return Err(Error::TopicSealed);
        }
        if topic_state_locked(&inner, topic_id).as_ref() != Some(expected_topic_state) {
            return Err(Error::AdmissionConflict);
        }
        // Stage both steps on a copy and swap only once the winner is admitted:
        // a rejected winner must leave the local chain exactly as it was, never
        // an empty topic with nothing installed in its place.
        let copy_bytes = inner
            .charges
            .values()
            .map(|charge| charge.operation.bytes + charge.metadata.bytes)
            .sum::<u64>()
            + inner
                .metadata
                .values()
                .map(|charge| charge.bytes)
                .sum::<u64>();
        let _copy = inner.budget.reserve(MemoryDomain::Recovery, copy_bytes)?;
        let mut staged = inner.clone();
        let removed = reset_topic_locked(&mut staged, topic_id)?;
        admit_batch_locked(&mut staged, batch, None)?;
        // The journal entry is part of the same swap: after it the discarded
        // payloads exist nowhere else, so no ordering here can lose them.
        if let Some((key, eviction)) = journalled_eviction(eviction) {
            if !staged.evictions.contains_key(&key)
                && staged.evictions.len() >= MAX_PENDING_EVICTIONS
            {
                return Err(Error::EvictionJournalFull);
            }
            let mut reservation = MetadataPlan::new(&staged)?;
            let bytes = postcard::experimental::serialized_size(eviction)? as u64;
            reservation.reserve(&staged, MetadataKey::Eviction(key), 4096 + bytes * 3)?;
            let mut eviction = eviction.clone();
            for op in &mut eviction.evicted {
                own_payload(&mut op.payload);
            }
            reservation.commit(&mut staged);
            staged.evictions.insert(key, eviction);
        }
        *inner = staged;
        Ok(removed)
    }

    fn seal_topic(&self, topic_id: &TopicId) -> Result<bool> {
        let mut inner = self.lock()?;
        let mut reservation = MetadataPlan::new(&inner)?;
        reservation.reserve(&inner, MetadataKey::Topic(*topic_id), 4096)?;
        reservation.commit(&mut inner);
        Ok(inner.sealed_topics.insert(*topic_id))
    }

    fn unseal_topic(&self, topic_id: &TopicId) -> Result<bool> {
        Ok(self.lock()?.sealed_topics.remove(topic_id))
    }

    fn pending_evictions(&self) -> Result<Vec<TopicEviction>> {
        Ok(self.lock()?.evictions.values().cloned().collect())
    }

    fn clear_eviction(&self, key: &EvictionKey) -> Result<()> {
        let mut inner = self.lock()?;
        inner.evictions.remove(key);
        inner.metadata.remove(&MetadataKey::Eviction(*key));
        Ok(())
    }

    fn staging_limits(&self) -> StagingLimits {
        self.limits
    }

    fn provisional_topics(&self) -> Result<Vec<ProvisionalTopic>> {
        self.read_namespaces()
    }

    fn open_provisional(
        &self,
        source: PeerId,
        topic_id: TopicId,
        genesis: OpId,
        now_ms: u64,
    ) -> Result<ProvisionalTopic> {
        self.open_namespace(source, topic_id, genesis, now_ms)
    }

    fn provisional_store(&self, provisional: &ProvisionalTopic) -> Result<Option<Self>> {
        self.namespace_store(provisional)
    }

    fn stored_bytes(&self) -> Result<u64> {
        let inner = self.lock()?;
        Ok(inner.admitted_bytes + inner.pending_usage.bytes)
    }

    fn touch_provisional(&self, provisional: &ProvisionalTopic, now_ms: u64) -> Result<()> {
        self.touch_namespace(provisional, now_ms)
    }

    fn activate_provisional(
        &self,
        provisional: &ProvisionalTopic,
        expected: &TopicState,
        effects: AdmissionEffects,
    ) -> Result<()> {
        self.activate_namespace(provisional, expected, effects)
    }

    fn discard_provisional(&self, provisional: &ProvisionalTopic) -> Result<bool> {
        self.discard_namespace(provisional)
    }
}

/// One mutex guard seen through [`SnapshotRead`].
struct MemorySnapshot<'a> {
    inner: &'a MemoryInner,
    counters: &'a StorageCounters,
}

impl SnapshotRead for MemorySnapshot<'_> {
    fn sync_identity(
        &self,
        topic: &TopicId,
        peer: &PeerId,
        reserve: &mut dyn FnMut(crate::storage::SnapshotCharge) -> Result<()>,
    ) -> Result<Option<crate::storage::RequestView>> {
        reserve(crate::storage::SnapshotCharge::Read { bytes: 0 })?;
        self.counters.count_meta();
        let Some(state) = self.inner.topics.get(topic) else {
            return Ok(None);
        };
        reserve(crate::storage::SnapshotCharge::Members(1))?;
        let member = state.members.contains(peer);
        reserve(crate::storage::SnapshotCharge::Read { bytes: 0 })?;
        let epoch = self
            .inner
            .topic_epochs
            .get(topic)
            .copied()
            .unwrap_or_default();
        Ok(Some(crate::storage::RequestView {
            genesis: state.genesis,
            epoch,
            member,
            clock: ActorClock::new(),
        }))
    }

    fn sync_clock(
        &self,
        topic: &TopicId,
        actors: Option<&BTreeSet<ActorId>>,
        reserve: &mut dyn FnMut(crate::storage::SnapshotCharge) -> Result<()>,
    ) -> Result<ActorClock> {
        reserve(crate::storage::SnapshotCharge::Read { bytes: 0 })?;
        self.counters.count_meta();
        let Some(clock) = self.inner.actor_clock.get(topic) else {
            return Ok(ActorClock::new());
        };
        reserve(crate::storage::SnapshotCharge::Clock {
            entries: clock.len(),
            workspace: actors.map_or(0, |actors| ActorClock::allocation_bound(actors.len())),
        })?;
        Ok(actors.map_or_else(|| clock.clone(), |actors| clock.selected(actors)))
    }

    fn actor_count(&self, topic_id: &TopicId) -> Result<usize> {
        Ok(self
            .inner
            .actor_clock
            .get(topic_id)
            .map_or(0, ActorClock::len))
    }
    fn get_reserved_op(
        &self,
        id: &OpId,
        reserve: &mut dyn FnMut(usize) -> Result<()>,
    ) -> Result<Option<Op>> {
        self.counters.count_op();
        let Some(op) = self.inner.ops.get(id) else {
            return Ok(None);
        };
        reserve(postcard::experimental::serialized_size(op)?)?;
        Ok(Some(op.clone()))
    }
    fn get_observation(&self, id: &OpId) -> Result<Option<(crate::storage::OpHeader, ActorClock)>> {
        self.counters.count_meta();
        Ok(self.inner.meta.get(id).map(|meta| {
            (
                crate::storage::OpHeader::from(meta),
                meta.observed_clock.clone(),
            )
        }))
    }

    fn get_header(&self, id: &OpId) -> Result<Option<crate::storage::OpHeader>> {
        self.counters.count_meta();
        Ok(self.inner.meta.get(id).map(crate::storage::OpHeader::from))
    }

    fn dependency_ids(
        &self,
        id: &OpId,
        cursor: crate::storage::DependencyCursor,
        limit: usize,
    ) -> Result<Option<Vec<OpId>>> {
        use std::ops::Bound::{Excluded, Unbounded};
        if limit == 0 {
            return Err(Error::SyncCapacity(
                "dependency read limit must be positive".into(),
            ));
        }
        self.counters.count_meta();
        let Some(meta) = self.inner.meta.get(id) else {
            return Ok(None);
        };
        if cursor.offset > meta.deps.len()
            || (cursor.offset == 0) != cursor.after.is_none()
            || cursor
                .after
                .is_some_and(|after| !meta.deps.contains(&after))
        {
            return Err(Error::Decode(
                "dependency cursor does not match metadata".into(),
            ));
        }
        let start = cursor.after.map_or(Unbounded, Excluded);
        let mut ids = Vec::with_capacity(limit.min(meta.deps.len()));
        ids.extend(meta.deps.range((start, Unbounded)).take(limit).copied());
        Ok(Some(ids))
    }

    fn request_view(
        &self,
        topic_id: &TopicId,
        peer_id: &PeerId,
        actors: &BTreeSet<ActorId>,
    ) -> Result<Option<crate::storage::RequestView>> {
        let Some(state) = self.inner.topics.get(topic_id) else {
            return Ok(None);
        };
        let clock = self
            .inner
            .actor_clock
            .get(topic_id)
            .map(|local| local.selected(actors))
            .unwrap_or_default();
        Ok(Some(crate::storage::RequestView {
            genesis: state.genesis,
            epoch: self
                .inner
                .topic_epochs
                .get(topic_id)
                .copied()
                .unwrap_or_default(),
            member: state.members.contains(peer_id),
            clock,
        }))
    }

    fn topic_view(
        &self,
        topic_id: &TopicId,
        peer_id: Option<&PeerId>,
    ) -> Result<Option<TopicView>> {
        topic_view_locked(self.inner, topic_id, peer_id)
    }
    fn get_op(&self, id: &OpId) -> Result<Option<Op>> {
        self.counters.count_op();
        Ok(self.inner.ops.get(id).cloned())
    }
    fn get_meta(&self, id: &OpId) -> Result<Option<OpMeta>> {
        self.counters.count_meta();
        Ok(self.inner.meta.get(id).cloned())
    }
    fn get_position(&self, id: &OpId) -> Result<Option<OpPosition>> {
        self.counters.count_meta();
        Ok(self.inner.meta.get(id).map(OpPosition::from))
    }
    fn dep_resolvable(&self, id: &OpId) -> Result<bool> {
        Ok(dep_resolvable_locked(self.inner, id))
    }
    fn actor_range(
        &self,
        topic_id: &TopicId,
        actor_id: &ActorId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, OpId)>> {
        let range = actor_range_locked(self.inner, topic_id, actor_id, after, limit);
        self.counters.count_index(range.len());
        Ok(range)
    }
    fn list_op_ids(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>> {
        Ok(topic_ids_locked(self.inner, topic_id))
    }
    fn topic_ids_after(
        &self,
        topic_id: &TopicId,
        after: Option<&OpId>,
        limit: usize,
    ) -> Result<Vec<OpId>> {
        use std::ops::Bound::{Excluded, Unbounded};
        let Some(ids) = self.inner.topic_ops.get(topic_id) else {
            return Ok(Vec::new());
        };
        let start = after.map_or(Unbounded, |after| Excluded(*after));
        Ok(ids.range((start, Unbounded)).take(limit).copied().collect())
    }
}

fn actor_range_locked(
    inner: &MemoryInner,
    topic_id: &TopicId,
    actor_id: &ActorId,
    after: u64,
    limit: usize,
) -> Vec<(u64, OpId)> {
    let Some(start) = after.checked_add(1) else {
        return Vec::new();
    };
    inner
        .actor_by_seq
        .range((*topic_id, *actor_id, start)..=(*topic_id, *actor_id, u64::MAX))
        .take(limit)
        .map(|((_, _, seq), id)| (*seq, *id))
        .collect()
}

fn topic_ids_locked(inner: &MemoryInner, topic_id: &TopicId) -> BTreeSet<OpId> {
    inner.topic_ops.get(topic_id).cloned().unwrap_or_default()
}

fn topic_view_locked(
    inner: &MemoryInner,
    topic_id: &TopicId,
    peer_id: Option<&PeerId>,
) -> Result<Option<TopicView>> {
    let Some(state) = topic_state_locked(inner, topic_id) else {
        return Ok(None);
    };
    let clock = inner.actor_clock.get(topic_id).cloned().unwrap_or_default();
    let tips = inner
        .actor_tip
        .range(
            (*topic_id, ActorId::from_bytes([0; 32]))
                ..=(*topic_id, ActorId::from_bytes([0xff; 32])),
        )
        .map(|((_, actor_id), tip)| (*actor_id, *tip))
        .collect();
    let fingerprint = match inner.topic_fingerprint.get(topic_id) {
        Some(fingerprint) => *fingerprint,
        None => topic_fingerprint_for(&state.heads, &clock)?,
    };
    let (ack, owed) = match peer_id {
        Some(peer_id) => (
            inner.peer_acks.get(&(*peer_id, *topic_id)).cloned(),
            inner
                .obligations
                .get(&(*topic_id, *peer_id))
                .is_some_and(|records| !records.is_empty()),
        ),
        None => (None, false),
    };
    Ok(Some(TopicView {
        epoch: inner
            .topic_epochs
            .get(topic_id)
            .copied()
            .unwrap_or_default(),
        pending_missing: pending_missing_locked(inner, topic_id),
        state,
        clock,
        tips,
        fingerprint,
        ack,
        owed,
    }))
}

fn admit_batch_locked(
    inner: &mut MemoryInner,
    batch: AdmittedBatch,
    quota: Option<StagingQuota>,
) -> Result<()> {
    validate_batch(&batch)?;
    if inner
        .heads
        .get(&batch.topic_id)
        .cloned()
        .unwrap_or_default()
        != batch.expected_heads
    {
        return Err(Error::AdmissionConflict);
    }
    if topic_state_locked(inner, &batch.topic_id) != batch.expected_topic_state {
        return Err(Error::AdmissionConflict);
    }
    validate_heads(&batch, |meta| {
        Ok(inner.ops.contains_key(&meta.id)
            || inner.meta.contains_key(&meta.id)
            || inner
                .actor_by_seq
                .get(&(meta.topic_id, meta.actor_id, meta.actor_seq))
                == Some(&meta.id)
            || inner.children.contains_key(&meta.id))
    })?;
    // Merged before any write, so a refused effect leaves the store untouched.
    let genesis = batch
        .topic_state
        .as_ref()
        .or(batch.expected_topic_state.as_ref())
        .map(|state| state.genesis);
    let _effects = metadata::merge_workspace(inner, batch.effects.sync_obligations.iter())?;
    let mut effects = BTreeMap::new();
    for obligation in &batch.effects.sync_obligations {
        let ack = inner.peer_acks.get(&(obligation.peer_id, batch.topic_id));
        if obligation.topic_id != batch.topic_id {
            return Err(Error::TopicMismatch);
        }
        if obligation.is_empty() || ack_covers(ack, genesis, obligation) {
            continue;
        }
        let key = (obligation.peer_id, ObligationKind::of(obligation));
        let merged = match effects.remove(&key) {
            Some(pending) => merged_obligation(Some(pending), obligation)?,
            None => merged_obligation_locked(inner, obligation)?,
        };
        effects.insert(key, merged);
    }
    let removed_peers = batch
        .expected_topic_state
        .as_ref()
        .zip(batch.topic_state.as_ref())
        .map(|(expected, state)| {
            expected
                .members
                .difference(&state.members)
                .copied()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let AdmittedBatch {
        topic_id,
        entries,
        heads,
        topic_state,
        ..
    } = batch;
    let mut actor_tips = BTreeMap::new();
    let mut new_entries = Vec::new();
    for (op, meta) in entries {
        if meta.topic_id != topic_id {
            return Err(Error::TopicMismatch);
        }
        let has_op = match inner.ops.get(&op.id) {
            Some(existing) if existing != &op => {
                return Err(Error::Storage("op id collision with different op".into()));
            }
            Some(_) => true,
            None => false,
        };
        let has_meta = inner.meta.contains_key(&op.id);
        if has_op && has_meta {
            continue;
        }
        let indexed = inner
            .actor_by_seq
            .get(&(meta.topic_id, meta.actor_id, meta.actor_seq))
            .copied();
        if let Some(existing) = indexed
            && existing != op.id
        {
            return Err(Error::ActorFork);
        }
        // Refilling an id the chain already accounts for is not an append: its
        // actor position is already recorded, so the checks below cannot apply.
        if has_op || has_meta || indexed == Some(op.id) || inner.children.contains_key(&op.id) {
            new_entries.push((op, meta));
            continue;
        }
        let tip = actor_tips
            .get(&(meta.topic_id, meta.actor_id))
            .copied()
            .or_else(|| {
                inner
                    .actor_tip
                    .get(&(meta.topic_id, meta.actor_id))
                    .copied()
            });
        match tip {
            Some((seq, id)) => {
                let expected = seq.checked_add(1).ok_or(Error::InvalidOpId)?;
                if meta.actor_seq != expected {
                    return Err(Error::ActorSeqGap {
                        expected,
                        actual: meta.actor_seq,
                    });
                }
                if meta.actor_prev != Some(id) {
                    return Err(Error::ActorPrevMismatch);
                }
            }
            None => {
                if meta.actor_seq != 1 {
                    return Err(Error::ActorSeqGap {
                        expected: 1,
                        actual: meta.actor_seq,
                    });
                }
                if meta.actor_prev.is_some() {
                    return Err(Error::ActorPrevMismatch);
                }
            }
        }
        actor_tips.insert((meta.topic_id, meta.actor_id), (meta.actor_seq, op.id));
        new_entries.push((op, meta));
    }

    ensure_deps_resolvable(&new_entries, |dep| Ok(dep_resolvable_locked(inner, dep)))?;
    let previous = inner
        .actor_clock
        .get(&topic_id)
        .cloned()
        .unwrap_or_default();
    let _workspace = inner.budget.reserve(
        MemoryDomain::Workspace,
        ActorClock::allocation_bound(
            previous
                .len()
                .saturating_add(new_entries.len())
                .saturating_add(64),
        ) as u64,
    )?;
    let mut projected = previous.clone();
    let mut nodes = inner.budget.nodes()?;
    let mut charges = BTreeMap::new();
    for (op, meta) in &new_entries {
        projected.observe(meta.actor_id, meta.actor_seq);
        nodes.add(&meta.observed_clock)?;
        let charge = match inner.charges.get(&op.id) {
            Some(charge) => charge.clone(),
            None => charge_record(&inner.budget, op, meta)?,
        };
        charges.insert(op.id, charge);
    }
    nodes.add(&projected)?;
    let mut reservation = MetadataPlan::new(inner)?;
    reservation.reserve(inner, MetadataKey::Topic(topic_id), 4096)?;
    for obligation in effects.values() {
        reservation.obligation(inner, obligation)?;
    }
    let fingerprint = topic_fingerprint_for(&heads, &projected)?;
    if let Some(quota) = quota {
        let mut charge = 0;
        for (op, _) in &new_entries {
            if !inner.ops.contains_key(&op.id) {
                charge += pending_op_bytes(op)? as u64;
            }
        }
        quota.check(inner.admitted_bytes + inner.pending_usage.bytes, charge)?;
        inner.admitted_bytes += charge;
    }

    nodes.commit();
    reservation.commit(inner);

    for (mut op, meta) in new_entries {
        own_payload(&mut op.signed.body.payload);
        inner
            .topic_ops
            .entry(meta.topic_id)
            .or_default()
            .insert(op.id);
        for dep in &meta.deps {
            inner.children.entry(*dep).or_default().insert(op.id);
        }
        inner
            .actor_by_seq
            .insert((meta.topic_id, meta.actor_id, meta.actor_seq), op.id);
        // A refilled op sits behind the tip, so the tip only ever advances.
        let tip = inner.actor_tip.entry((meta.topic_id, meta.actor_id));
        match tip {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                if entry.get().0 < meta.actor_seq {
                    entry.insert((meta.actor_seq, op.id));
                }
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert((meta.actor_seq, op.id));
            }
        }
        inner
            .max_generation
            .entry(meta.topic_id)
            .and_modify(|generation| *generation = (*generation).max(meta.generation))
            .or_insert(meta.generation);
        remove_pending_locked(inner, &op.id)?;
        settle_waiters_locked(inner, &op.id);
        inner.meta.insert(op.id, meta);
        inner.ops.insert(op.id, op);
    }

    inner.charges.extend(charges);
    inner.actor_clock.insert(topic_id, projected);

    inner.heads.insert(topic_id, heads.clone());
    inner.topic_fingerprint.insert(topic_id, fingerprint);
    if let Some(state) = topic_state {
        inner.topics.insert(state.topic_id, state);
    }
    for obligation in effects.into_values() {
        put_obligation_locked(inner, obligation);
    }
    for peer_id in removed_peers {
        inner.obligations.remove(&(topic_id, peer_id));
        inner.sync_statuses.remove(&(topic_id, peer_id));
        inner.peer_acks.remove(&(peer_id, topic_id));
        inner.metadata.retain(|key, _| !key.peer(topic_id, peer_id));
    }
    Ok(())
}

fn reset_topic_locked(inner: &mut MemoryInner, topic_id: &TopicId) -> Result<usize> {
    let key = MetadataKey::Topic(*topic_id);
    if !inner.metadata.contains_key(&key) {
        let charge = inner.budget.reserve(MemoryDomain::Metadata, 4096)?;
        inner.metadata.insert(key, Arc::new(charge));
    }
    *inner.topic_epochs.entry(*topic_id).or_default() += 1;
    let op_ids = inner.topic_ops.remove(topic_id).unwrap_or_default();
    let removed = op_ids.len();
    for op_id in &op_ids {
        // Edges pointing at this op go too, or a dependency the reset does not
        // reach keeps naming a child the topic no longer holds.
        let deps = inner
            .meta
            .get(op_id)
            .map(|meta| meta.deps.clone())
            .unwrap_or_default();
        for dep in deps {
            if let Some(children) = inner.children.get_mut(&dep) {
                children.remove(op_id);
                if children.is_empty() {
                    inner.children.remove(&dep);
                }
            }
        }
        if inner.admitted_bytes > 0
            && let Some(op) = inner.ops.get(op_id)
        {
            let refund = pending_op_bytes(op)? as u64;
            inner.admitted_bytes = inner.admitted_bytes.saturating_sub(refund);
        }
        inner.ops.remove(op_id);
        inner.meta.remove(op_id);
        inner.charges.remove(op_id);
        inner.children.remove(op_id);
    }
    inner.heads.remove(topic_id);
    inner.actor_clock.remove(topic_id);
    inner.topic_fingerprint.remove(topic_id);
    inner.max_generation.remove(topic_id);
    inner.topics.remove(topic_id);
    inner.actor_by_seq.retain(|(t, _, _), _| t != topic_id);
    inner.actor_tip.retain(|(t, _), _| t != topic_id);
    inner.peer_acks.retain(|(_, t), _| t != topic_id);
    inner.obligations.retain(|(t, _), _| t != topic_id);
    inner.sync_statuses.retain(|(t, _), _| t != topic_id);
    inner.rejected.remove(topic_id);
    inner.metadata.retain(|key, _| !key.reset(*topic_id));
    for op_id in inner
        .pending_by_topic
        .get(topic_id)
        .cloned()
        .unwrap_or_default()
    {
        remove_pending_locked(inner, &op_id)?;
    }
    Ok(removed)
}

fn charge_record(budget: &Arc<Budget>, op: &Op, meta: &OpMeta) -> Result<RecordCharge> {
    let encoded = pending_op_bytes(op)? as u64;
    let peers = matches!(
        op.signed.body.payload,
        crate::TopicPayload::Genesis(_)
            | crate::TopicPayload::Control(crate::TopicControl::SetReplicationPolicy { .. })
    );
    let metadata = (3 * size_of::<OpMeta>() + 4096) as u64
        + (meta.deps.len() + meta.missing_deps.len()) as u64 * 768
        + if peers { encoded.saturating_mul(3) } else { 0 };
    Ok(RecordCharge {
        operation: Arc::new(budget.reserve(MemoryDomain::Operations, encoded.saturating_mul(3))?),
        metadata: Arc::new(budget.reserve(MemoryDomain::Metadata, metadata)?),
    })
}

fn own_payload(payload: &mut crate::TopicPayload) {
    match payload {
        crate::TopicPayload::Event(event) => {
            event.payload = bytes::Bytes::copy_from_slice(&event.payload);
            event.type_id = String::from(event.type_id.as_str());
        }
        crate::TopicPayload::Genesis(genesis) => {
            genesis.event_type_id = String::from(genesis.event_type_id.as_str());
        }
        crate::TopicPayload::Control(_) => {}
    }
}

fn apply_ack_locked(inner: &mut MemoryInner, ack: PeerAck) -> Result<usize> {
    let commit = ack_commit(inner.topics.get(&ack.topic_id), &ack)?;
    let key = (ack.peer_id, ack.topic_id);
    let entries = ack.clock.len() + inner.peer_acks.get(&key).map_or(0, |old| old.clock.len());
    let outstanding = inner
        .obligations
        .get(&(ack.topic_id, ack.peer_id))
        .map_or(0, |records| {
            records
                .values()
                .map(metadata::obligation_bytes)
                .sum::<u64>()
        });
    let _workspace = inner.budget.reserve(
        MemoryDomain::Control,
        (2 * ActorClock::allocation_bound(entries) + ack.heads.len() * 256 + 4096) as u64
            + outstanding,
    )?;
    let effective_ack = match inner.peer_acks.get(&key) {
        Some(existing) if stored_ack_dominates(existing, &ack) => existing.clone(),
        Some(existing) => merged_peer_ack(existing, &ack),
        None => ack,
    };
    let mut reservation = MetadataPlan::control(inner)?;
    reservation.ack(inner, &effective_ack)?;
    let obligation_key = (effective_ack.topic_id, effective_ack.peer_id);
    let mut settled = BTreeMap::new();
    if commit != AckCommit::Retain
        && let Some(records) = inner.obligations.get(&obligation_key)
    {
        for (kind, obligation) in records {
            let rest = settled_obligation(obligation, &effective_ack, |id| {
                Ok(inner.meta.get(id).map(OpPosition::from))
            })?;
            if let Some(rest) = &rest {
                reservation.obligation(inner, rest)?;
            }
            settled.insert(*kind, rest);
        }
    }
    reservation.commit(inner);
    inner.peer_acks.insert(key, effective_ack);
    let mut cleared = 0;
    for (kind, rest) in settled {
        match rest {
            Some(rest) => put_obligation_locked(inner, rest),
            None => {
                if let Some(records) = inner.obligations.get_mut(&obligation_key) {
                    records.remove(&kind);
                }
                inner.metadata.remove(&MetadataKey::Obligation(
                    obligation_key.0,
                    obligation_key.1,
                    kind,
                ));
                cleared += 1;
            }
        }
    }
    if inner
        .obligations
        .get(&obligation_key)
        .is_some_and(BTreeMap::is_empty)
    {
        inner.obligations.remove(&obligation_key);
    }
    Ok(cleared)
}

/// Which of a peer's two records per topic an obligation belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ObligationKind {
    Clock,
    Repair,
}

impl ObligationKind {
    fn of(obligation: &SyncObligation) -> Self {
        match obligation.target {
            ObligationTarget::Clock(_) => Self::Clock,
            ObligationTarget::Repair(_) => Self::Repair,
        }
    }
}

/// `obligation` merged into the record of its kind the store already holds.
fn merged_obligation_locked(
    inner: &MemoryInner,
    obligation: &SyncObligation,
) -> Result<SyncObligation> {
    let existing = inner
        .obligations
        .get(&(obligation.topic_id, obligation.peer_id))
        .and_then(|records| records.get(&ObligationKind::of(obligation)))
        .cloned();
    merged_obligation(existing, obligation)
}

/// Store an already merged record, replacing the one of its kind.
fn put_obligation_locked(inner: &mut MemoryInner, obligation: SyncObligation) {
    if obligation.is_empty() {
        return;
    }
    inner
        .obligations
        .entry((obligation.topic_id, obligation.peer_id))
        .or_default()
        .insert(ObligationKind::of(&obligation), obligation);
}

/// Metadata of a stored op and the genesis of the topic branch holding it.
fn stored_op_branch<'a>(inner: &'a MemoryInner, op_id: &OpId) -> Option<(&'a OpMeta, OpId)> {
    let meta = inner.meta.get(op_id)?;
    let genesis = inner.topics.get(&meta.topic_id)?.genesis;
    Some((meta, genesis))
}

fn pending_missing_locked(inner: &MemoryInner, topic_id: &TopicId) -> BTreeSet<OpId> {
    inner
        .pending_by_topic
        .get(topic_id)
        .into_iter()
        .flatten()
        .filter_map(|op_id| inner.pending_records.get(op_id))
        .flat_map(|record| record.missing.iter().copied())
        .collect()
}

/// A dependency is only satisfied once both of its records are stored; either
/// one alone leaves the DAG unable to traverse the edge.
fn dep_resolvable_locked(inner: &MemoryInner, dep: &OpId) -> bool {
    inner.ops.contains_key(dep) && inner.meta.contains_key(dep)
}

fn pending_entry_locked(inner: &MemoryInner, op_id: &OpId) -> Option<(PeerId, Op)> {
    let record = inner.pending_records.get(op_id)?;
    Some((record.source, inner.pending_ops.get(op_id)?.clone()))
}

/// Every buffered op that transitively waits on `dep_id`, excluding it.
fn waiter_closure_locked(inner: &MemoryInner, dep_id: &OpId) -> BTreeSet<OpId> {
    let mut frontier = vec![*dep_id];
    let mut seen = BTreeSet::new();
    while let Some(dep) = frontier.pop() {
        for op_id in inner.pending_waiters.get(&dep).into_iter().flatten() {
            if seen.insert(*op_id) {
                frontier.push(*op_id);
            }
        }
    }
    seen.remove(dep_id);
    seen
}

/// Stop `op_id` waiting on `dep`.
fn unwait_locked(inner: &mut MemoryInner, dep: &OpId, op_id: &OpId) {
    if let Some(waiters) = inner.pending_waiters.get_mut(dep) {
        waiters.remove(op_id);
        if waiters.is_empty() {
            inner.pending_waiters.remove(dep);
        }
    }
}

/// `admitted` resolved: its waiters stop waiting on it, and a waiter with
/// nothing left to wait for joins the ready index.
fn settle_waiters_locked(inner: &mut MemoryInner, admitted: &OpId) {
    for waiter in inner.pending_waiters.remove(admitted).unwrap_or_default() {
        if let Some(record) = inner.pending_records.get_mut(&waiter) {
            record.missing.remove(admitted);
            if record.missing.is_empty() {
                inner.pending_ready.insert(waiter);
            }
        }
    }
}

fn remove_pending_locked(inner: &mut MemoryInner, op_id: &OpId) -> Result<()> {
    let Some(record) = inner.pending_records.get(op_id) else {
        return Ok(());
    };
    // Refunds are checked before anything changes, so a broken counter leaves
    // the records as they were instead of half removed.
    let usage = |usage: Option<&PendingUsage>| usage.copied().unwrap_or_default();
    let total = inner.pending_usage.refunded(record.charge)?;
    let source = usage(inner.source_usage.get(&record.source)).refunded(record.charge)?;
    let topic = usage(inner.topic_usage.get(&record.topic_id)).refunded(record.charge)?;
    let Some(record) = inner.pending_records.remove(op_id) else {
        return Ok(());
    };
    inner.pending_usage = total;
    if source == PendingUsage::default() {
        inner.source_usage.remove(&record.source);
    } else {
        inner.source_usage.insert(record.source, source);
    }
    if topic == PendingUsage::default() {
        inner.topic_usage.remove(&record.topic_id);
    } else {
        inner.topic_usage.insert(record.topic_id, topic);
    }
    for dep in &record.missing {
        unwait_locked(inner, dep, op_id);
    }
    if let std::collections::btree_map::Entry::Occupied(mut entry) =
        inner.pending_by_topic.entry(record.topic_id)
    {
        entry.get_mut().remove(op_id);
        if entry.get().is_empty() {
            entry.remove();
        }
    }
    inner.pending_ready.remove(op_id);
    inner.pending_ops.remove(op_id);
    if !inner.ops.contains_key(op_id) && !inner.meta.contains_key(op_id) {
        inner.charges.remove(op_id);
    }
    Ok(())
}

fn topic_state_locked(inner: &MemoryInner, topic_id: &TopicId) -> Option<TopicState> {
    inner.topics.get(topic_id).cloned().map(|mut state| {
        state.heads = inner.heads.get(topic_id).cloned().unwrap_or_default();
        state
    })
}
impl From<std::sync::PoisonError<std::sync::MutexGuard<'_, MemoryInner>>> for Error {
    fn from(_: std::sync::PoisonError<std::sync::MutexGuard<'_, MemoryInner>>) -> Self {
        Error::Storage("lock poisoned".into())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::memory::*;
    use crate::tests::support::*;

    #[test]
    fn bounded_string_ownership() {
        let source = node(211);
        let topic = source.create_topic::<Note>(TopicConfig::default()).unwrap();
        let record = topic
            .publish(Note {
                text: "bounded".into(),
            })
            .unwrap();
        let expected = source
            .storage()
            .get_op(&record.meta.op_id)
            .unwrap()
            .unwrap();
        for pending in [true, false] {
            let mut op = expected.clone();
            let TopicPayload::Event(event) = &mut op.signed.body.payload else {
                unreachable!()
            };
            event.type_id.reserve(1024 * 1024);
            op.validate().unwrap();
            let store = MemoryStorage::new();
            if pending {
                let mut meta = source.storage().get_meta(&op.id).unwrap().unwrap();
                meta.ready = false;
                meta.missing_deps = meta.deps.clone();
                store.put_pending_op(source.peer_id(), op, meta).unwrap();
            } else {
                let genesis = source
                    .storage()
                    .get_op(&expected.signed.body.actor_prev.unwrap())
                    .unwrap()
                    .unwrap();
                oplog::Oplog::with_storage(store.clone())
                    .receive_ops(vec![genesis, op])
                    .unwrap();
            }
            let held = store.memory_usage().unwrap().reserved[&MemoryDomain::Operations];
            let inner = store.lock().unwrap();
            let op = if pending {
                &inner.pending_ops[&expected.id]
            } else {
                &inner.ops[&expected.id]
            };
            let TopicPayload::Event(event) = &op.signed.body.payload else {
                unreachable!()
            };
            assert!(
                event.type_id.capacity() as u64 <= held,
                "unbounded caller string retained"
            );
            assert_eq!(op, &expected);
        }
    }
}
