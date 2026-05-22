// SPDX-License-Identifier: MIT OR Apache-2.0
//! Storage trait plus in-memory and Fjall-backed persistence implementations.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

#[cfg(feature = "fjall")]
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::crypto::canonical_bytes;
use crate::topic::ReplicationPolicy;
use crate::{ActorClock, ActorId, Op, OpId, PeerId, Result, TopicId};
use crate::{Error, TopicInfo};

pub const MAX_PENDING_OPS_TOTAL: usize = 4096;
pub const MAX_PENDING_OPS_PER_SOURCE: usize = 1024;
pub const MAX_PENDING_WAITERS_PER_DEP: usize = 1024;
pub const MAX_PENDING_MISSING_DEPS: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpMeta {
    pub id: OpId,
    pub topic_id: TopicId,
    pub author: PeerId,
    pub actor_id: ActorId,
    pub actor_seq: u64,
    pub actor_prev: Option<OpId>,
    pub deps: BTreeSet<OpId>,
    pub generation: u64,
    pub observed_clock: ActorClock,
    pub ready: bool,
    pub missing_deps: BTreeSet<OpId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ControlKey {
    pub generation: u64,
    pub actor_id: ActorId,
    pub actor_seq: u64,
    pub op_id: OpId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicState {
    pub topic_id: TopicId,
    pub event_type_id: String,
    pub genesis: OpId,
    pub heads: BTreeSet<OpId>,
    pub members: BTreeSet<PeerId>,
    pub replication_policy: ReplicationPolicy,
    #[serde(default)]
    pub membership_controls: BTreeMap<PeerId, (ControlKey, bool)>,
    #[serde(default)]
    pub replication_policy_control: Option<(ControlKey, ReplicationPolicy)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerAck {
    pub peer_id: PeerId,
    pub topic_id: TopicId,
    pub heads: BTreeSet<OpId>,
    pub clock: ActorClock,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncObligation {
    pub peer_id: PeerId,
    pub topic_id: TopicId,
    pub op_ids: BTreeSet<OpId>,
    #[serde(default)]
    pub target_clock: ActorClock,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SyncPeerState {
    #[default]
    Idle,
    Healthy,
    Behind,
    Failed,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncPeerStatus {
    pub peer_id: PeerId,
    pub topic_id: TopicId,
    pub state: SyncPeerState,
    pub pending_obligations: usize,
    pub failed_attempts: u64,
    pub successful_attempts: u64,
    pub last_attempt_ms: Option<u64>,
    pub last_success_ms: Option<u64>,
    pub last_error: Option<String>,
}

pub trait Storage: Clone + Send + Sync + 'static {
    fn put_admitted_batch(
        &self,
        topic_id: TopicId,
        expected_heads: BTreeSet<OpId>,
        expected_topic_state: Option<TopicState>,
        entries: Vec<(Op, OpMeta)>,
        heads: BTreeSet<OpId>,
        topic_state: Option<TopicState>,
    ) -> Result<()>;
    fn get_op(&self, id: &OpId) -> Result<Option<Op>>;
    fn get_meta(&self, id: &OpId) -> Result<Option<OpMeta>>;
    fn list_ops(&self, topic_id: &TopicId) -> Result<Vec<Op>>;
    fn list_op_ids(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>>;
    fn heads(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>>;
    fn children(&self, op_id: &OpId) -> Result<BTreeSet<OpId>>;
    fn actor_tip(&self, topic_id: &TopicId, actor_id: &ActorId) -> Result<Option<(u64, OpId)>>;
    fn actor_index(&self, topic_id: &TopicId, actor_id: &ActorId, seq: u64)
    -> Result<Option<OpId>>;
    fn actor_clock(&self, topic_id: &TopicId) -> Result<ActorClock>;
    fn topic_fingerprint(&self, topic_id: &TopicId) -> Result<[u8; 32]>;
    fn max_generation(&self, topic_id: &TopicId) -> Result<u64>;
    fn topic_state(&self, topic_id: &TopicId) -> Result<Option<TopicState>>;
    fn list_topics(&self) -> Result<Vec<TopicInfo>>;
    fn put_pending_op(&self, source_peer: PeerId, op: Op, meta: OpMeta) -> Result<()>;
    fn pending_waiters(&self, dep_id: &OpId) -> Result<Vec<(PeerId, Op)>>;
    fn ready_pending_ops(&self) -> Result<Vec<(PeerId, Op)>>;
    fn remove_pending_op(&self, op_id: &OpId) -> Result<()>;
    fn peer_ack(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<Option<PeerAck>>;
    fn peer_acks(&self, topic_id: &TopicId) -> Result<Vec<PeerAck>>;
    fn put_sync_obligation(&self, obligation: SyncObligation) -> Result<()>;
    fn all_sync_obligations(&self) -> Result<Vec<SyncObligation>>;
    /// Atomically persist `ack` and clear any obligations satisfied by it.
    /// Backends must perform both writes in one durable operation so a crash
    /// between them cannot leave the ack visible while obligations remain,
    /// or vice-versa. Returns the number of cleared obligations.
    fn apply_peer_ack(&self, ack: PeerAck) -> Result<usize>;
    fn sync_obligations(&self, peer_id: &PeerId, topic_id: &TopicId)
    -> Result<Vec<SyncObligation>>;
    fn put_sync_status(&self, status: SyncPeerStatus) -> Result<()>;
    fn sync_statuses(&self, topic_id: &TopicId) -> Result<Vec<SyncPeerStatus>>;

    fn peer_reached_op(&self, peer_id: &PeerId, op_id: &OpId) -> Result<bool> {
        let Some(meta) = self.get_meta(op_id)? else {
            return Ok(false);
        };
        let Some(ack) = self.peer_ack(peer_id, &meta.topic_id)? else {
            return Ok(false);
        };
        Ok(ack.heads.contains(op_id) || ack.clock.get(&meta.actor_id) >= meta.actor_seq)
    }

    fn peers_reached_op(&self, op_id: &OpId) -> Result<Vec<PeerId>> {
        let Some(meta) = self.get_meta(op_id)? else {
            return Ok(Vec::new());
        };
        let mut peers = self
            .peer_acks(&meta.topic_id)?
            .into_iter()
            .filter(|ack| {
                ack.heads.contains(op_id) || ack.clock.get(&meta.actor_id) >= meta.actor_seq
            })
            .map(|ack| ack.peer_id)
            .collect::<Vec<_>>();
        peers.sort();
        Ok(peers)
    }
}

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
    fn put_admitted_batch(
        &self,
        topic_id: TopicId,
        expected_heads: BTreeSet<OpId>,
        expected_topic_state: Option<TopicState>,
        entries: Vec<(Op, OpMeta)>,
        heads: BTreeSet<OpId>,
        topic_state: Option<TopicState>,
    ) -> Result<()> {
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
        let key = (ack.peer_id, ack.topic_id);
        let effective_ack = match inner.peer_acks.get(&key) {
            Some(existing) if stored_ack_dominates(existing, &ack) => existing.clone(),
            _ => {
                inner.peer_acks.insert(key, ack.clone());
                ack
            }
        };
        Ok(clear_satisfied_obligations_locked(
            &mut inner,
            &effective_ack,
        ))
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

    fn put_sync_status(&self, status: SyncPeerStatus) -> Result<()> {
        self.lock()?
            .sync_statuses
            .insert((status.topic_id, status.peer_id), status);
        Ok(())
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
}

fn clear_satisfied_obligations_locked(inner: &mut MemoryInner, ack: &PeerAck) -> usize {
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

pub(crate) fn topic_fingerprint_for(
    heads: &BTreeSet<OpId>,
    clock: &ActorClock,
) -> Result<[u8; 32]> {
    Ok(*blake3::hash(&canonical_bytes(&(heads, clock))?).as_bytes())
}

fn sync_obligation_satisfied(obligation: &SyncObligation, ack: &PeerAck) -> bool {
    if obligation.op_ids.is_subset(&ack.heads) {
        return true;
    }
    !obligation.target_clock.is_empty() && ack.clock.dominates(&obligation.target_clock)
}

fn stored_ack_dominates(existing: &PeerAck, incoming: &PeerAck) -> bool {
    existing.peer_id == incoming.peer_id
        && existing.topic_id == incoming.topic_id
        && existing.clock.dominates(&incoming.clock)
}

#[cfg(feature = "fjall")]
#[derive(Clone)]
pub struct FjallStorage {
    db: fjall::OptimisticTxDatabase,
    records: fjall::OptimisticTxKeyspace,
}

#[cfg(feature = "fjall")]
const FJALL_SCHEMA_VERSION: u32 = 1;
#[cfg(feature = "fjall")]
const FJALL_SCHEMA_VERSION_KEY: &[u8] = b"sv";

#[cfg(feature = "fjall")]
impl FjallStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = fjall::OptimisticTxDatabase::builder(path).open()?;
        Self::from_database(db)
    }

    pub fn from_database(db: fjall::OptimisticTxDatabase) -> Result<Self> {
        let storage = Self {
            records: db.keyspace("records", fjall::KeyspaceCreateOptions::default)?,
            db,
        };
        storage.ensure_schema_version()?;
        Ok(storage)
    }

    fn ensure_schema_version(&self) -> Result<()> {
        match self.get::<u32>(FJALL_SCHEMA_VERSION_KEY)? {
            Some(FJALL_SCHEMA_VERSION) => Ok(()),
            Some(version) => Err(Error::Storage(format!(
                "unsupported fjall schema version {version}"
            ))),
            None => self.put(FJALL_SCHEMA_VERSION_KEY, &FJALL_SCHEMA_VERSION),
        }
    }

    fn transaction<R>(
        &self,
        mut f: impl FnMut(&mut fjall::OptimisticWriteTx) -> Result<R>,
    ) -> Result<R> {
        for _ in 0..64 {
            let mut tx = self
                .db
                .write_tx()?
                .durability(Some(fjall::PersistMode::SyncAll));
            let result = f(&mut tx)?;
            match tx.commit()? {
                Ok(()) => return Ok(result),
                Err(_) => continue,
            }
        }
        Err(Error::AdmissionConflict)
    }

    fn key_id(prefix: &[u8], id: &impl AsRef<[u8]>) -> Vec<u8> {
        [prefix, id.as_ref()].concat()
    }

    fn put<T: Serialize>(&self, key: impl AsRef<[u8]>, value: &T) -> Result<()> {
        let key = key.as_ref().to_vec();
        let value = postcard::to_allocvec(value)?;
        self.transaction(|tx| {
            tx.insert(&self.records, key.clone(), value.clone());
            Ok(())
        })
    }

    fn tx_put<T: Serialize>(
        tx: &mut fjall::OptimisticWriteTx,
        records: &fjall::OptimisticTxKeyspace,
        key: impl AsRef<[u8]>,
        value: &T,
    ) -> Result<()> {
        tx.insert(
            records,
            key.as_ref().to_vec(),
            postcard::to_allocvec(value)?,
        );
        Ok(())
    }

    fn tx_get<T: for<'de> Deserialize<'de>>(
        tx: &fjall::OptimisticWriteTx,
        records: &fjall::OptimisticTxKeyspace,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<T>> {
        Ok(fjall::Readable::get(tx, records, key.as_ref())?
            .map(|v| postcard::from_bytes(v.as_ref()))
            .transpose()?)
    }

    fn tx_remove_pending_op(
        tx: &mut fjall::OptimisticWriteTx,
        records: &fjall::OptimisticTxKeyspace,
        op_id: &OpId,
    ) -> Result<()> {
        let Some((source_peer, _, meta)) =
            Self::tx_get::<(PeerId, Op, OpMeta)>(tx, records, Self::key_id(b"po", op_id))?
        else {
            return Ok(());
        };
        tx.remove(records, Self::key_id(b"po", op_id));
        for dep in &meta.missing_deps {
            let waiter_key = [b"pw".as_slice(), dep.as_ref(), op_id.as_ref()].concat();
            tx.remove(records, waiter_key);
            let count_key = [b"wn".as_slice(), dep.as_ref()].concat();
            let count: u64 = Self::tx_get(tx, records, count_key.as_slice())?.unwrap_or_default();
            let next = count.saturating_sub(1);
            if next == 0 {
                tx.remove(records, count_key);
            } else {
                Self::tx_put(tx, records, count_key.as_slice(), &next)?;
            }
        }
        let total: u64 = Self::tx_get(tx, records, b"pn".as_slice())?.unwrap_or_default();
        let next_total = total.saturating_sub(1);
        if next_total == 0 {
            tx.remove(records, b"pn".as_slice());
        } else {
            Self::tx_put(tx, records, b"pn".as_slice(), &next_total)?;
        }
        let source_key = [b"ps".as_slice(), source_peer.as_ref()].concat();
        let source_count: u64 =
            Self::tx_get(tx, records, source_key.as_slice())?.unwrap_or_default();
        let next_source = source_count.saturating_sub(1);
        if next_source == 0 {
            tx.remove(records, source_key);
        } else {
            Self::tx_put(tx, records, source_key.as_slice(), &next_source)?;
        }
        Ok(())
    }

    fn sync_obligation_key(obligation: &SyncObligation) -> Vec<u8> {
        let digest =
            blake3::hash(&postcard::to_allocvec(&obligation.op_ids).expect("op ids serialize"));
        [
            b"ob".as_slice(),
            obligation.peer_id.as_ref(),
            obligation.topic_id.as_ref(),
            digest.as_bytes(),
        ]
        .concat()
    }
    fn get<T: for<'de> Deserialize<'de>>(&self, key: impl AsRef<[u8]>) -> Result<Option<T>> {
        Ok(self
            .records
            .get(key)?
            .map(|v| postcard::from_bytes(v.as_ref()))
            .transpose()?)
    }

    fn op_id_from_key(key: &[u8], offset: usize) -> Result<OpId> {
        let bytes = key
            .get(offset..offset + OpId::LEN)
            .ok_or_else(|| Error::Storage("corrupt fjall op id index key".into()))?;
        let mut out = [0_u8; OpId::LEN];
        out.copy_from_slice(bytes);
        Ok(OpId::from_bytes(out))
    }
}

#[cfg(feature = "fjall")]
impl Storage for FjallStorage {
    fn put_admitted_batch(
        &self,
        topic_id: TopicId,
        expected_heads: BTreeSet<OpId>,
        expected_topic_state: Option<TopicState>,
        entries: Vec<(Op, OpMeta)>,
        heads: BTreeSet<OpId>,
        topic_state: Option<TopicState>,
    ) -> Result<()> {
        self.transaction(|tx| {
            let current_heads: BTreeSet<OpId> =
                Self::tx_get(tx, &self.records, Self::key_id(b"h", &topic_id))?.unwrap_or_default();
            if current_heads != expected_heads {
                return Err(Error::AdmissionConflict);
            }
            let current_topic_state: Option<TopicState> =
                Self::tx_get::<TopicState>(tx, &self.records, Self::key_id(b"ts", &topic_id))?.map(
                    |mut state| {
                        state.heads = current_heads.clone();
                        state
                    },
                );
            if current_topic_state != expected_topic_state {
                return Err(Error::AdmissionConflict);
            }

            let mut actor_tips = BTreeMap::new();
            let mut new_entries = Vec::new();
            for (op, meta) in &entries {
                if meta.topic_id != topic_id {
                    return Err(Error::TopicMismatch);
                }
                if let Some(existing) =
                    Self::tx_get::<Op>(tx, &self.records, Self::key_id(b"o", &op.id))?
                {
                    if existing != *op {
                        return Err(Error::Storage("op id collision with different op".into()));
                    }
                    continue;
                }
                if let Some(existing) = Self::tx_get::<OpId>(
                    tx,
                    &self.records,
                    [
                        b"as".as_slice(),
                        meta.topic_id.as_ref(),
                        meta.actor_id.as_ref(),
                        &meta.actor_seq.to_be_bytes(),
                    ]
                    .concat(),
                )? && existing != op.id
                {
                    return Err(Error::ActorFork);
                }
                let tip =
                    actor_tips
                        .get(&(meta.topic_id, meta.actor_id))
                        .copied()
                        .or(Self::tx_get::<(u64, OpId)>(
                            tx,
                            &self.records,
                            [
                                b"at".as_slice(),
                                meta.topic_id.as_ref(),
                                meta.actor_id.as_ref(),
                            ]
                            .concat(),
                        )?);
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
                new_entries.push((op.clone(), meta.clone()));
            }

            let mut clock: ActorClock =
                Self::tx_get(tx, &self.records, Self::key_id(b"ac", &topic_id))?
                    .unwrap_or_default();
            let mut max_generation: u64 =
                Self::tx_get(tx, &self.records, Self::key_id(b"mg", &topic_id))?
                    .unwrap_or_default();
            for (op, meta) in new_entries {
                Self::tx_put(tx, &self.records, Self::key_id(b"o", &op.id), &op)?;
                Self::tx_put(tx, &self.records, Self::key_id(b"m", &meta.id), &meta)?;
                Self::tx_put(
                    tx,
                    &self.records,
                    [b"to".as_slice(), meta.topic_id.as_ref(), op.id.as_ref()].concat(),
                    &(),
                )?;
                for dep in &meta.deps {
                    Self::tx_put(
                        tx,
                        &self.records,
                        [b"ch".as_slice(), dep.as_ref(), op.id.as_ref()].concat(),
                        &(),
                    )?;
                }
                Self::tx_put(
                    tx,
                    &self.records,
                    [
                        b"as".as_slice(),
                        meta.topic_id.as_ref(),
                        meta.actor_id.as_ref(),
                        &meta.actor_seq.to_be_bytes(),
                    ]
                    .concat(),
                    &op.id,
                )?;
                Self::tx_put(
                    tx,
                    &self.records,
                    [
                        b"at".as_slice(),
                        meta.topic_id.as_ref(),
                        meta.actor_id.as_ref(),
                    ]
                    .concat(),
                    &(meta.actor_seq, op.id),
                )?;
                if let Some((source_peer, _, pending_meta)) = Self::tx_get::<(PeerId, Op, OpMeta)>(
                    tx,
                    &self.records,
                    Self::key_id(b"po", &op.id),
                )? {
                    tx.remove(&self.records, Self::key_id(b"po", &op.id));
                    for dep in &pending_meta.missing_deps {
                        tx.remove(
                            &self.records,
                            [b"pw".as_slice(), dep.as_ref(), op.id.as_ref()].concat(),
                        );
                        let count_key = [b"wn".as_slice(), dep.as_ref()].concat();
                        let count: u64 = Self::tx_get(tx, &self.records, count_key.as_slice())?
                            .unwrap_or_default();
                        let next = count.saturating_sub(1);
                        if next == 0 {
                            tx.remove(&self.records, count_key);
                        } else {
                            Self::tx_put(tx, &self.records, count_key.as_slice(), &next)?;
                        }
                    }
                    let total: u64 =
                        Self::tx_get(tx, &self.records, b"pn".as_slice())?.unwrap_or_default();
                    let next_total = total.saturating_sub(1);
                    if next_total == 0 {
                        tx.remove(&self.records, b"pn".as_slice());
                    } else {
                        Self::tx_put(tx, &self.records, b"pn".as_slice(), &next_total)?;
                    }
                    let source_key = [b"ps".as_slice(), source_peer.as_ref()].concat();
                    let source_count: u64 =
                        Self::tx_get(tx, &self.records, source_key.as_slice())?.unwrap_or_default();
                    let next_source = source_count.saturating_sub(1);
                    if next_source == 0 {
                        tx.remove(&self.records, source_key);
                    } else {
                        Self::tx_put(tx, &self.records, source_key.as_slice(), &next_source)?;
                    }
                }
                clock.observe(meta.actor_id, meta.actor_seq);
                max_generation = max_generation.max(meta.generation);
            }
            Self::tx_put(tx, &self.records, Self::key_id(b"ac", &topic_id), &clock)?;
            Self::tx_put(tx, &self.records, Self::key_id(b"h", &topic_id), &heads)?;
            Self::tx_put(
                tx,
                &self.records,
                Self::key_id(b"fp", &topic_id),
                &topic_fingerprint_for(&heads, &clock)?,
            )?;
            Self::tx_put(
                tx,
                &self.records,
                Self::key_id(b"mg", &topic_id),
                &max_generation,
            )?;
            if let Some(state) = &topic_state {
                Self::tx_put(
                    tx,
                    &self.records,
                    Self::key_id(b"ts", &state.topic_id),
                    state,
                )?;
            }
            Ok(())
        })
    }
    fn get_op(&self, id: &OpId) -> Result<Option<Op>> {
        self.get(Self::key_id(b"o", id))
    }
    fn get_meta(&self, id: &OpId) -> Result<Option<OpMeta>> {
        self.get(Self::key_id(b"m", id))
    }
    fn list_ops(&self, topic_id: &TopicId) -> Result<Vec<Op>> {
        self.list_op_ids(topic_id)?
            .iter()
            .map(|id| {
                self.get_op(id)?
                    .ok_or_else(|| Error::Storage(format!("missing op indexed for topic: {id}")))
            })
            .collect()
    }
    fn list_op_ids(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>> {
        let prefix = [b"to".as_slice(), topic_id.as_ref()].concat();
        let mut out = BTreeSet::new();
        for item in self.records.inner().prefix(prefix) {
            let (key, _) = item.into_inner()?;
            out.insert(Self::op_id_from_key(key.as_ref(), 2 + TopicId::LEN)?);
        }
        Ok(out)
    }
    fn heads(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>> {
        Ok(self.get(Self::key_id(b"h", topic_id))?.unwrap_or_default())
    }
    fn children(&self, op_id: &OpId) -> Result<BTreeSet<OpId>> {
        let prefix = [b"ch".as_slice(), op_id.as_ref()].concat();
        let mut out = BTreeSet::new();
        for item in self.records.inner().prefix(prefix) {
            let (key, _) = item.into_inner()?;
            out.insert(Self::op_id_from_key(key.as_ref(), 2 + OpId::LEN)?);
        }
        Ok(out)
    }
    fn actor_tip(&self, topic_id: &TopicId, actor_id: &ActorId) -> Result<Option<(u64, OpId)>> {
        self.get([b"at".as_slice(), topic_id.as_ref(), actor_id.as_ref()].concat())
    }
    fn actor_index(
        &self,
        topic_id: &TopicId,
        actor_id: &ActorId,
        seq: u64,
    ) -> Result<Option<OpId>> {
        self.get(
            [
                b"as".as_slice(),
                topic_id.as_ref(),
                actor_id.as_ref(),
                &seq.to_be_bytes(),
            ]
            .concat(),
        )
    }
    fn actor_clock(&self, topic_id: &TopicId) -> Result<ActorClock> {
        Ok(self.get(Self::key_id(b"ac", topic_id))?.unwrap_or_default())
    }
    fn topic_fingerprint(&self, topic_id: &TopicId) -> Result<[u8; 32]> {
        Ok(self
            .get(Self::key_id(b"fp", topic_id))?
            .unwrap_or(topic_fingerprint_for(
                &self.heads(topic_id)?,
                &self.actor_clock(topic_id)?,
            )?))
    }
    fn max_generation(&self, topic_id: &TopicId) -> Result<u64> {
        Ok(self.get(Self::key_id(b"mg", topic_id))?.unwrap_or_default())
    }
    fn topic_state(&self, topic_id: &TopicId) -> Result<Option<TopicState>> {
        self.get::<TopicState>(Self::key_id(b"ts", topic_id))?
            .map(|mut state| {
                state.heads = self.heads(topic_id)?;
                Ok(state)
            })
            .transpose()
    }
    fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        // v0 keeps this simple: scan durable topic records instead of maintaining a second index.
        let mut out = Vec::new();
        for item in self.records.inner().prefix(b"ts") {
            let value = item.value()?;
            let s: TopicState = postcard::from_bytes(value.as_ref())?;
            out.push(TopicInfo {
                topic_id: s.topic_id,
                event_type_id: s.event_type_id,
                genesis: s.genesis,
            });
        }
        Ok(out)
    }
    fn put_pending_op(&self, source_peer: PeerId, op: Op, meta: OpMeta) -> Result<()> {
        self.transaction(|tx| {
            if Self::tx_get::<Op>(tx, &self.records, Self::key_id(b"o", &op.id))?.is_some() {
                return Ok(());
            }
            if let Some((_, existing, _)) = Self::tx_get::<(PeerId, Op, OpMeta)>(
                tx,
                &self.records,
                Self::key_id(b"po", &op.id),
            )? {
                if existing != op {
                    return Err(Error::Storage(
                        "pending op id collision with different op".into(),
                    ));
                }
                Self::tx_remove_pending_op(tx, &self.records, &op.id)?;
            }
            for dep in &meta.missing_deps {
                if Self::tx_get::<Op>(tx, &self.records, Self::key_id(b"o", dep))?.is_some() {
                    return Err(Error::AdmissionConflict);
                }
            }
            if meta.missing_deps.len() > MAX_PENDING_MISSING_DEPS {
                return Err(Error::Storage(
                    "pending op has too many missing deps".into(),
                ));
            }

            let total_pending: u64 =
                Self::tx_get(tx, &self.records, b"pn".as_slice())?.unwrap_or_default();
            if total_pending as usize >= MAX_PENDING_OPS_TOTAL {
                return Err(Error::Storage("pending op buffer is full".into()));
            }
            let source_key = [b"ps".as_slice(), source_peer.as_ref()].concat();
            let source_pending: u64 =
                Self::tx_get(tx, &self.records, source_key.as_slice())?.unwrap_or_default();
            if source_pending as usize >= MAX_PENDING_OPS_PER_SOURCE {
                return Err(Error::Storage("pending op source quota exceeded".into()));
            }
            let mut waiter_keys: Vec<(Vec<u8>, u64)> = Vec::with_capacity(meta.missing_deps.len());
            for dep in &meta.missing_deps {
                let key = [b"wn".as_slice(), dep.as_ref()].concat();
                let count: u64 =
                    Self::tx_get(tx, &self.records, key.as_slice())?.unwrap_or_default();
                if count as usize >= MAX_PENDING_WAITERS_PER_DEP {
                    return Err(Error::Storage("pending waiter quota exceeded".into()));
                }
                waiter_keys.push((key, count));
            }

            Self::tx_put(
                tx,
                &self.records,
                Self::key_id(b"po", &op.id),
                &(source_peer, op.clone(), meta.clone()),
            )?;
            for dep in &meta.missing_deps {
                Self::tx_put(
                    tx,
                    &self.records,
                    [b"pw".as_slice(), dep.as_ref(), op.id.as_ref()].concat(),
                    &(),
                )?;
            }
            Self::tx_put(tx, &self.records, b"pn".as_slice(), &(total_pending + 1))?;
            Self::tx_put(
                tx,
                &self.records,
                source_key.as_slice(),
                &(source_pending + 1),
            )?;
            for (key, count) in waiter_keys {
                Self::tx_put(tx, &self.records, key.as_slice(), &(count + 1))?;
            }
            Ok(())
        })
    }
    fn pending_waiters(&self, dep_id: &OpId) -> Result<Vec<(PeerId, Op)>> {
        let prefix = [b"pw".as_slice(), dep_id.as_ref()].concat();
        let mut out = Vec::new();
        let read_tx = self.db.read_tx();
        for item in fjall::Readable::prefix(&read_tx, &self.records, prefix) {
            let (key, _) = item.into_inner()?;
            let op_id = Self::op_id_from_key(key.as_ref(), 2 + OpId::LEN)?;
            if let Some((source, op, _)) =
                fjall::Readable::get(&read_tx, &self.records, Self::key_id(b"po", &op_id))?
                    .map(|value| postcard::from_bytes::<(PeerId, Op, OpMeta)>(value.as_ref()))
                    .transpose()?
            {
                out.push((source, op));
            }
        }
        Ok(out)
    }
    fn ready_pending_ops(&self) -> Result<Vec<(PeerId, Op)>> {
        let mut out = Vec::new();
        let read_tx = self.db.read_tx();
        for item in fjall::Readable::prefix(&read_tx, &self.records, b"po".as_slice()) {
            let value = item.value()?;
            let (source, op, meta): (PeerId, Op, OpMeta) = postcard::from_bytes(value.as_ref())?;
            let mut ready = true;
            for dep in &meta.missing_deps {
                if fjall::Readable::get(&read_tx, &self.records, Self::key_id(b"o", dep))?.is_none()
                {
                    ready = false;
                    break;
                }
            }
            if ready {
                out.push((source, op));
            }
        }
        Ok(out)
    }
    fn remove_pending_op(&self, op_id: &OpId) -> Result<()> {
        self.transaction(|tx| Self::tx_remove_pending_op(tx, &self.records, op_id))
    }
    fn peer_ack(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<Option<PeerAck>> {
        self.get([b"ak".as_slice(), peer_id.as_ref(), topic_id.as_ref()].concat())
    }
    fn peer_acks(&self, topic_id: &TopicId) -> Result<Vec<PeerAck>> {
        let mut out = Vec::new();
        let read_tx = self.db.read_tx();
        for item in fjall::Readable::prefix(&read_tx, &self.records, b"ak".as_slice()) {
            let value = item.value()?;
            let ack: PeerAck = postcard::from_bytes(value.as_ref())?;
            if ack.topic_id == *topic_id {
                out.push(ack);
            }
        }
        Ok(out)
    }
    fn put_sync_obligation(&self, obligation: SyncObligation) -> Result<()> {
        self.put(Self::sync_obligation_key(&obligation), &obligation)
    }

    fn all_sync_obligations(&self) -> Result<Vec<SyncObligation>> {
        let mut out = Vec::new();
        let read_tx = self.db.read_tx();
        for item in fjall::Readable::prefix(&read_tx, &self.records, b"ob".as_slice()) {
            let value = item.value()?;
            out.push(postcard::from_bytes(value.as_ref())?);
        }
        Ok(out)
    }

    fn apply_peer_ack(&self, ack: PeerAck) -> Result<usize> {
        self.transaction(|tx| {
            let ack_key = [
                b"ak".as_slice(),
                ack.peer_id.as_ref(),
                ack.topic_id.as_ref(),
            ]
            .concat();
            let effective_ack =
                match Self::tx_get::<PeerAck>(tx, &self.records, ack_key.as_slice())? {
                    Some(existing) if stored_ack_dominates(&existing, &ack) => existing,
                    _ => {
                        Self::tx_put(tx, &self.records, ack_key, &ack)?;
                        ack.clone()
                    }
                };
            clear_satisfied_obligations_tx(tx, &self.records, &effective_ack)
        })
    }

    fn sync_obligations(
        &self,
        peer_id: &PeerId,
        topic_id: &TopicId,
    ) -> Result<Vec<SyncObligation>> {
        let prefix = [b"ob".as_slice(), peer_id.as_ref(), topic_id.as_ref()].concat();
        let mut out = Vec::new();
        let read_tx = self.db.read_tx();
        for item in fjall::Readable::prefix(&read_tx, &self.records, prefix) {
            let value = item.value()?;
            out.push(postcard::from_bytes(value.as_ref())?);
        }
        Ok(out)
    }

    fn put_sync_status(&self, status: SyncPeerStatus) -> Result<()> {
        self.put(
            [
                b"ss".as_slice(),
                status.topic_id.as_ref(),
                status.peer_id.as_ref(),
            ]
            .concat(),
            &status,
        )
    }

    fn sync_statuses(&self, topic_id: &TopicId) -> Result<Vec<SyncPeerStatus>> {
        let prefix = [b"ss".as_slice(), topic_id.as_ref()].concat();
        let mut out = Vec::new();
        let read_tx = self.db.read_tx();
        for item in fjall::Readable::prefix(&read_tx, &self.records, prefix) {
            let value = item.value()?;
            out.push(postcard::from_bytes(value.as_ref())?);
        }
        Ok(out)
    }
}

#[cfg(feature = "fjall")]
fn clear_satisfied_obligations_tx(
    tx: &mut fjall::OptimisticWriteTx,
    records: &fjall::OptimisticTxKeyspace,
    ack: &PeerAck,
) -> Result<usize> {
    let prefix = [
        b"ob".as_slice(),
        ack.peer_id.as_ref(),
        ack.topic_id.as_ref(),
    ]
    .concat();
    let mut keys = Vec::new();
    for item in fjall::Readable::prefix(tx, records, prefix) {
        let (key, value) = item.into_inner()?;
        let obligation: SyncObligation = postcard::from_bytes(value.as_ref())?;
        if sync_obligation_satisfied(&obligation, ack) {
            keys.push(key.to_vec());
        }
    }
    let cleared = keys.len();
    for key in keys {
        tx.remove(records, key);
    }
    Ok(cleared)
}

impl From<std::sync::PoisonError<std::sync::MutexGuard<'_, MemoryInner>>> for Error {
    fn from(_: std::sync::PoisonError<std::sync::MutexGuard<'_, MemoryInner>>) -> Self {
        Error::Storage("lock poisoned".into())
    }
}
