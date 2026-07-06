// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::{ActorClock, ActorId, Error, Op, OpId, PeerId, Result, TopicId, TopicInfo};

use super::{
    AdmittedBatch, MAX_PENDING_MISSING_DEPS, MAX_PENDING_OPS_PER_SOURCE, MAX_PENDING_OPS_TOTAL,
    MAX_PENDING_WAITERS_PER_DEP, OpMeta, PeerAck, Storage, SyncObligation, SyncPeerStatus,
    TopicState, stored_ack_dominates, sync_obligation_satisfied, topic_fingerprint_for,
};

#[derive(Clone, Default)]
pub struct MemoryStorage {
    inner: Arc<Mutex<MemoryInner>>,
}

#[derive(Default)]
struct MemoryInner {
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
    pending_ops: BTreeMap<OpId, (PeerId, Op, OpMeta)>,
    pending_by_source: BTreeMap<PeerId, BTreeSet<OpId>>,
    pending_waiters: BTreeMap<OpId, BTreeSet<OpId>>,
    peer_acks: HashMap<(PeerId, TopicId), PeerAck>,
    obligations: Vec<SyncObligation>,
    sync_statuses: BTreeMap<(TopicId, PeerId), SyncPeerStatus>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> Result<MutexGuard<'_, MemoryInner>> {
        self.inner.lock().map_err(Error::from)
    }
}

impl Storage for MemoryStorage {
    fn put_admitted_batch(&self, batch: AdmittedBatch) -> Result<()> {
        let AdmittedBatch {
            topic_id,
            expected_heads,
            expected_topic_state,
            entries,
            heads,
            topic_state,
            effects,
        } = batch;
        let mut inner = self.lock()?;
        if inner.heads.get(&topic_id).cloned().unwrap_or_default() != expected_heads {
            return Err(Error::AdmissionConflict);
        }
        if memory_topic_state_locked(&inner, &topic_id) != expected_topic_state {
            return Err(Error::AdmissionConflict);
        }
        let mut actor_tips = BTreeMap::new();
        let mut new_entries = Vec::new();
        for (op, meta) in entries {
            if let Some(existing) = inner.ops.get(&op.id) {
                if existing != &op {
                    return Err(Error::Storage("op id collision with different op".into()));
                }
                continue;
            }
            if let Some(existing) =
                inner
                    .actor_by_seq
                    .get(&(meta.topic_id, meta.actor_id, meta.actor_seq))
                && *existing != op.id
            {
                return Err(Error::ActorFork);
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

        let topic_id = topic_state
            .as_ref()
            .map(|state| state.topic_id)
            .or_else(|| new_entries.first().map(|(_, meta)| meta.topic_id));
        if let Some(topic_id) = topic_id
            && new_entries
                .iter()
                .any(|(_, meta)| meta.topic_id != topic_id)
        {
            return Err(Error::TopicMismatch);
        }

        for (op, meta) in new_entries {
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
            inner
                .actor_clock
                .entry(meta.topic_id)
                .or_default()
                .observe(meta.actor_id, meta.actor_seq);
            inner
                .max_generation
                .entry(meta.topic_id)
                .and_modify(|generation| *generation = (*generation).max(meta.generation))
                .or_insert(meta.generation);
            remove_pending_locked(&mut inner, &op.id);
            inner.meta.insert(op.id, meta);
            inner.ops.insert(op.id, op);
        }

        if let Some(topic_id) = topic_id {
            inner.heads.insert(topic_id, heads.clone());
            let clock = inner
                .actor_clock
                .get(&topic_id)
                .cloned()
                .unwrap_or_default();
            inner
                .topic_fingerprint
                .insert(topic_id, topic_fingerprint_for(&heads, &clock)?);
        }
        if let Some(state) = topic_state {
            inner.topics.insert(state.topic_id, state);
        }
        for obligation in effects.sync_obligations {
            if !inner.obligations.contains(&obligation) {
                inner.obligations.push(obligation);
            }
        }
        Ok(())
    }

    fn get_op(&self, id: &OpId) -> Result<Option<Op>> {
        Ok(self.lock()?.ops.get(id).cloned())
    }
    fn get_meta(&self, id: &OpId) -> Result<Option<OpMeta>> {
        Ok(self.lock()?.meta.get(id).cloned())
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
        Ok(self
            .inner
            .lock()?
            .topic_ops
            .get(topic_id)
            .cloned()
            .unwrap_or_default())
    }
    fn heads(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>> {
        Ok(self
            .inner
            .lock()?
            .heads
            .get(topic_id)
            .cloned()
            .unwrap_or_default())
    }
    fn children(&self, op_id: &OpId) -> Result<BTreeSet<OpId>> {
        Ok(self
            .inner
            .lock()?
            .children
            .get(op_id)
            .cloned()
            .unwrap_or_default())
    }
    fn actor_tip(&self, topic_id: &TopicId, actor_id: &ActorId) -> Result<Option<(u64, OpId)>> {
        Ok(self
            .inner
            .lock()?
            .actor_tip
            .get(&(*topic_id, *actor_id))
            .cloned())
    }
    fn actor_index(
        &self,
        topic_id: &TopicId,
        actor_id: &ActorId,
        seq: u64,
    ) -> Result<Option<OpId>> {
        Ok(self
            .inner
            .lock()?
            .actor_by_seq
            .get(&(*topic_id, *actor_id, seq))
            .copied())
    }
    fn actor_clock(&self, topic_id: &TopicId) -> Result<ActorClock> {
        Ok(self
            .inner
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
            .inner
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
            .inner
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
    fn put_pending_op(&self, source_peer: PeerId, op: Op, meta: OpMeta) -> Result<()> {
        let mut inner = self.lock()?;
        if inner.ops.contains_key(&op.id) {
            return Ok(());
        }
        let replace_pending = if let Some((_, existing, _)) = inner.pending_ops.get(&op.id) {
            if existing != &op {
                return Err(Error::Storage(
                    "pending op id collision with different op".into(),
                ));
            }
            true
        } else {
            false
        };
        if meta
            .missing_deps
            .iter()
            .any(|dep| inner.ops.contains_key(dep))
        {
            return Err(Error::AdmissionConflict);
        }
        if meta.missing_deps.len() > MAX_PENDING_MISSING_DEPS {
            return Err(Error::Storage(
                "pending op has too many missing deps".into(),
            ));
        }
        if replace_pending {
            remove_pending_locked(&mut inner, &op.id);
        }
        if inner.pending_ops.len() >= MAX_PENDING_OPS_TOTAL {
            return Err(Error::Storage("pending op buffer is full".into()));
        }
        let source_pending = inner
            .pending_by_source
            .get(&source_peer)
            .map_or(0, BTreeSet::len);
        if source_pending >= MAX_PENDING_OPS_PER_SOURCE {
            return Err(Error::Storage("pending op source quota exceeded".into()));
        }
        for dep in &meta.missing_deps {
            if inner.pending_waiters.get(dep).map_or(0, BTreeSet::len)
                >= MAX_PENDING_WAITERS_PER_DEP
            {
                return Err(Error::Storage("pending waiter quota exceeded".into()));
            }
        }
        for dep in &meta.missing_deps {
            inner.pending_waiters.entry(*dep).or_default().insert(op.id);
        }
        inner
            .pending_by_source
            .entry(source_peer)
            .or_default()
            .insert(op.id);
        inner.pending_ops.insert(op.id, (source_peer, op, meta));
        Ok(())
    }
    fn pending_waiters(&self, dep_id: &OpId) -> Result<Vec<(PeerId, Op)>> {
        let inner = self.lock()?;
        Ok(inner
            .pending_waiters
            .get(dep_id)
            .into_iter()
            .flatten()
            .filter_map(|op_id| {
                inner
                    .pending_ops
                    .get(op_id)
                    .map(|(source, op, _)| (*source, op.clone()))
            })
            .collect())
    }
    fn ready_pending_ops(&self) -> Result<Vec<(PeerId, Op)>> {
        let inner = self.lock()?;
        Ok(inner
            .pending_ops
            .values()
            .filter(|(_, _, meta)| {
                meta.missing_deps
                    .iter()
                    .all(|dep| inner.ops.contains_key(dep))
            })
            .map(|(source, op, _)| (*source, op.clone()))
            .collect())
    }
    fn remove_pending_op(&self, op_id: &OpId) -> Result<()> {
        let mut inner = self.lock()?;
        remove_pending_locked(&mut inner, op_id);
        Ok(())
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
    fn put_sync_obligation(&self, obligation: SyncObligation) -> Result<()> {
        let mut inner = self.lock()?;
        if !inner.obligations.contains(&obligation) {
            inner.obligations.push(obligation);
        }
        Ok(())
    }

    fn all_sync_obligations(&self) -> Result<Vec<SyncObligation>> {
        Ok(self.lock()?.obligations.clone())
    }

    fn apply_peer_ack(&self, ack: PeerAck) -> Result<usize> {
        let mut inner = self.lock()?;
        Ok(apply_peer_ack_locked(&mut inner, ack))
    }

    fn apply_peer_acks(&self, acks: Vec<PeerAck>) -> Result<usize> {
        let mut inner = self.lock()?;
        let mut cleared = 0;
        for ack in acks {
            cleared += apply_peer_ack_locked(&mut inner, ack);
        }
        Ok(cleared)
    }

    fn sync_obligations(
        &self,
        peer_id: &PeerId,
        topic_id: &TopicId,
    ) -> Result<Vec<SyncObligation>> {
        Ok(self
            .lock()?
            .obligations
            .iter()
            .filter(|o| o.peer_id == *peer_id && o.topic_id == *topic_id)
            .cloned()
            .collect())
    }

    fn has_sync_obligations(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<bool> {
        Ok(self
            .lock()?
            .obligations
            .iter()
            .any(|o| o.peer_id == *peer_id && o.topic_id == *topic_id))
    }

    fn put_sync_status(&self, status: SyncPeerStatus) -> Result<()> {
        self.lock()?
            .sync_statuses
            .insert((status.topic_id, status.peer_id), status);
        Ok(())
    }

    fn clear_peer_sync_state(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<usize> {
        let mut inner = self.lock()?;
        let before = inner.obligations.len();
        inner
            .obligations
            .retain(|o| !(o.peer_id == *peer_id && o.topic_id == *topic_id));
        let cleared = before - inner.obligations.len();
        inner.sync_statuses.remove(&(*topic_id, *peer_id));
        inner.peer_acks.remove(&(*peer_id, *topic_id));
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
        let op_ids = inner.topic_ops.remove(topic_id).unwrap_or_default();
        let removed = op_ids.len();
        for op_id in &op_ids {
            inner.ops.remove(op_id);
            inner.meta.remove(op_id);
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
        inner.obligations.retain(|o| o.topic_id != *topic_id);
        inner.sync_statuses.retain(|(t, _), _| t != topic_id);
        let pending: Vec<OpId> = inner
            .pending_ops
            .iter()
            .filter(|(_, (_, _, meta))| meta.topic_id == *topic_id)
            .map(|(id, _)| *id)
            .collect();
        for op_id in pending {
            remove_pending_locked(&mut inner, &op_id);
        }
        Ok(removed)
    }
}

fn apply_peer_ack_locked(inner: &mut MemoryInner, ack: PeerAck) -> usize {
    let key = (ack.peer_id, ack.topic_id);
    let effective_ack = match inner.peer_acks.get(&key) {
        Some(existing) if stored_ack_dominates(existing, &ack) => existing.clone(),
        _ => {
            inner.peer_acks.insert(key, ack.clone());
            ack
        }
    };
    clear_satisfied_locked(inner, &effective_ack)
}

fn clear_satisfied_locked(inner: &mut MemoryInner, ack: &PeerAck) -> usize {
    let obligations = std::mem::take(&mut inner.obligations);
    let before = obligations.len();
    inner.obligations = obligations
        .into_iter()
        .filter(|obligation| {
            obligation.peer_id != ack.peer_id
                || obligation.topic_id != ack.topic_id
                || !sync_obligation_satisfied(obligation, ack)
        })
        .collect();
    before - inner.obligations.len()
}

fn remove_pending_locked(inner: &mut MemoryInner, op_id: &OpId) {
    let Some((_, _, meta)) = inner.pending_ops.remove(op_id) else {
        return;
    };
    for dep in meta.missing_deps {
        if let Some(waiters) = inner.pending_waiters.get_mut(&dep) {
            waiters.remove(op_id);
            if waiters.is_empty() {
                inner.pending_waiters.remove(&dep);
            }
        }
    }
    inner.pending_by_source.retain(|_, op_ids| {
        op_ids.remove(op_id);
        !op_ids.is_empty()
    });
}

fn memory_topic_state_locked(inner: &MemoryInner, topic_id: &TopicId) -> Option<TopicState> {
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
