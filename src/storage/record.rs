// SPDX-License-Identifier: MIT OR Apache-2.0
//! Stored metadata, snapshots, and admission records.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::topic::ReplicationPolicy;
use crate::{ActorClock, ActorId, Op, OpId, PeerId, Result, TopicId};
use super::{
    MAX_PENDING_BYTES_PER_SOURCE as MAX_SOURCE_BYTES, MAX_PENDING_BYTES_PER_TOPIC as MAX_TOPIC_BYTES,
    MAX_PENDING_BYTES_TOTAL as MAX_TOTAL_BYTES, MAX_PENDING_MISSING_DEPS as MAX_MISSING_DEPS,
    MAX_PENDING_OPS_PER_SOURCE as MAX_SOURCE_OPS, MAX_PENDING_OPS_PER_TOPIC as MAX_TOPIC_OPS,
    MAX_PENDING_OPS_TOTAL as MAX_TOTAL_OPS, MAX_PENDING_WAITERS_PER_DEP as MAX_WAITERS,
    MAX_REJECTED_PER_TOPIC as MAX_REJECTED, PeerAck, SyncObligation,
};

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

/// Where an admitted op sits in its topic's graph: the part of [`OpMeta`] that
/// page planning reads, without the op's observed clock, whose size grows with
/// the actors behind the op.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpPosition {
    pub topic_id: TopicId,
    pub actor_id: ActorId,
    pub actor_seq: u64,
    pub actor_prev: Option<OpId>,
    pub deps: BTreeSet<OpId>,
    pub generation: u64,
}

/// Position fields needed without materializing dependencies or an observed clock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OpHeader {
    pub topic_id: TopicId,
    pub actor_id: ActorId,
    pub actor_seq: u64,
    pub actor_prev: Option<OpId>,
    pub generation: u64,
}

impl From<&OpMeta> for OpHeader {
    fn from(meta: &OpMeta) -> Self {
        Self {
            topic_id: meta.topic_id,
            actor_id: meta.actor_id,
            actor_seq: meta.actor_seq,
            actor_prev: meta.actor_prev,
            generation: meta.generation,
        }
    }
}

impl From<&OpPosition> for OpHeader {
    fn from(meta: &OpPosition) -> Self {
        Self {
            topic_id: meta.topic_id,
            actor_id: meta.actor_id,
            actor_seq: meta.actor_seq,
            actor_prev: meta.actor_prev,
            generation: meta.generation,
        }
    }
}

impl From<&OpMeta> for OpPosition {
    fn from(meta: &OpMeta) -> Self {
        Self {
            topic_id: meta.topic_id,
            actor_id: meta.actor_id,
            actor_seq: meta.actor_seq,
            actor_prev: meta.actor_prev,
            deps: meta.deps.clone(),
            generation: meta.generation,
        }
    }
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

/// Diagnostic counts of work a backend performed: op records and metadata
/// read through [`super::Storage`], buffered payloads and obligation records decoded
/// and write transactions attempted.
#[derive(Debug, Default)]
pub struct StorageCounters {
    op_reads: std::sync::atomic::AtomicU64,
    meta_reads: std::sync::atomic::AtomicU64,
    index_reads: std::sync::atomic::AtomicU64,
    pending_payload_reads: std::sync::atomic::AtomicU64,
    obligation_reads: std::sync::atomic::AtomicU64,
    transaction_attempts: std::sync::atomic::AtomicU64,
}

/// A copy of [`StorageCounters`] at one moment.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CounterSnapshot {
    pub op_reads: u64,
    pub meta_reads: u64,
    pub index_reads: u64,
    pub pending_payload_reads: u64,
    pub obligation_reads: u64,
    pub transaction_attempts: u64,
}

impl StorageCounters {
    pub(crate) fn count_op(&self) {
        self.op_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn count_meta(&self) {
        self.meta_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn count_index(&self, count: usize) {
        self.index_reads
            .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(feature = "fjall")]
    pub(crate) fn count_attempt(&self) {
        self.transaction_attempts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn count_obligations(&self, count: usize) {
        self.obligation_reads
            .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn count_payloads(&self, count: usize) {
        self.pending_payload_reads
            .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> CounterSnapshot {
        let read = |counter: &std::sync::atomic::AtomicU64| {
            counter.load(std::sync::atomic::Ordering::Relaxed)
        };
        CounterSnapshot {
            op_reads: read(&self.op_reads),
            meta_reads: read(&self.meta_reads),
            index_reads: read(&self.index_reads),
            pending_payload_reads: read(&self.pending_payload_reads),
            obligation_reads: read(&self.obligation_reads),
            transaction_attempts: read(&self.transaction_attempts),
        }
    }
}

/// One coherent read of a topic, taken under one lock or read transaction so
/// every part describes the same commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicView {
    /// Topic state carrying the current heads.
    pub state: TopicState,
    pub clock: ActorClock,
    /// Tip of every actor the topic stores.
    pub tips: BTreeMap<ActorId, (u64, OpId)>,
    pub fingerprint: [u8; 32],
    /// Destructive data epoch, see [`super::Storage::topic_view`].
    pub epoch: u64,
    /// Dependencies that buffered ops of this topic still wait for.
    pub pending_missing: BTreeSet<OpId>,
    /// Stored ack of the peer the view was read for.
    pub ack: Option<PeerAck>,
    /// Whether that peer holds outstanding obligations for this topic.
    pub owed: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionEffects {
    pub sync_obligations: Vec<SyncObligation>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmittedBatch {
    pub topic_id: TopicId,
    pub expected_heads: BTreeSet<OpId>,
    pub expected_topic_state: Option<TopicState>,
    pub entries: Vec<(Op, OpMeta)>,
    pub heads: BTreeSet<OpId>,
    pub topic_state: Option<TopicState>,
    pub effects: AdmissionEffects,
}
/// Branch, authorization and selected positions from one snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestView {
    pub genesis: OpId,
    pub epoch: u64,
    pub member: bool,
    pub clock: ActorClock,
}

/// Resume dependency reads within the same operation and branch snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DependencyCursor {
    pub offset: usize,
    pub after: Option<OpId>,
}

pub(crate) fn validate_batch(batch: &AdmittedBatch) -> Result<()> {
    for state in [&batch.expected_topic_state, &batch.topic_state]
        .into_iter()
        .flatten()
    {
        if state.topic_id != batch.topic_id {
            return Err(crate::Error::TopicMismatch);
        }
    }
    if batch
        .topic_state
        .as_ref()
        .is_some_and(|state| state.heads != batch.heads)
    {
        return Err(crate::Error::Storage(
            "topic state frontier mismatch".into(),
        ));
    }
    for (op, meta) in &batch.entries {
        let body = &op.signed.body;
        if body.topic_id != batch.topic_id || meta.topic_id != batch.topic_id {
            return Err(crate::Error::TopicMismatch);
        }
        if meta.id != op.id
            || meta.author != body.author
            || meta.actor_id != body.actor_id
            || meta.actor_seq != body.actor_seq
            || meta.actor_prev != body.actor_prev
            || meta.deps != body.deps
            || meta.generation != body.generation
            || !meta.ready
            || !meta.missing_deps.is_empty()
        {
            return Err(crate::Error::Storage("operation metadata mismatch".into()));
        }
    }
    Ok(())
}

pub(crate) fn validate_heads(
    batch: &AdmittedBatch,
    mut accounted: impl FnMut(&OpMeta) -> Result<bool>,
) -> Result<()> {
    let mut heads = batch.expected_heads.clone();
    let mut consumed = BTreeSet::new();
    for (op, meta) in &batch.entries {
        // Repairs and duplicates retain their existing position in the frontier.
        if !accounted(meta)? {
            heads.insert(op.id);
            consumed.extend(op.signed.body.deps.iter().copied());
        }
    }
    heads.retain(|id| !consumed.contains(id));
    if heads != batch.heads {
        return Err(crate::Error::Storage("admitted frontier mismatch".into()));
    }
    Ok(())
}

/// Serialized size charged against the pending byte budgets, counted without
/// allocating the encoding. Backends store the charge and refund that value.
pub(crate) fn pending_op_bytes(op: &Op) -> Result<usize> {
    Ok(postcard::experimental::serialized_size(op)?)
}

/// Pending ops and serialized bytes one scope holds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PendingUsage {
    pub(crate) ops: u64,
    pub(crate) bytes: u64,
}

impl PendingUsage {
    pub(crate) fn charged(self, bytes: u64) -> Self {
        Self {
            ops: self.ops + 1,
            bytes: self.bytes + bytes,
        }
    }

    /// Usage after refunding one op of `bytes`. Underflow means the counters
    /// no longer describe the records, which is corruption, not a clean pool.
    pub(crate) fn refunded(self, bytes: u64) -> Result<Self> {
        match (self.ops.checked_sub(1), self.bytes.checked_sub(bytes)) {
            (Some(ops), Some(bytes)) => Ok(Self { ops, bytes }),
            _ => Err(crate::Error::Storage(
                "pending accounting does not match the buffered records".into(),
            )),
        }
    }
}

/// A buffered op's description, stored apart from its payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PendingRecord {
    pub(crate) source: PeerId,
    pub(crate) topic_id: TopicId,
    pub(crate) missing: BTreeSet<OpId>,
    pub(crate) charge: u64,
}

/// Refuse a new pending op of `charge` bytes that would push the total, its
/// source or its topic past their limits.
pub(crate) fn check_pending_quota(
    total: PendingUsage,
    source: PendingUsage,
    topic: PendingUsage,
    charge: u64,
) -> Result<()> {
    let refuse = |message: &str| Err(crate::Error::Storage(message.into()));
    if total.ops >= MAX_TOTAL_OPS as u64 {
        return refuse("pending op buffer is full");
    }
    if total.bytes + charge > MAX_TOTAL_BYTES as u64 {
        return refuse("pending byte budget is full");
    }
    if source.ops >= MAX_SOURCE_OPS as u64 {
        return refuse("pending op source quota exceeded");
    }
    if source.bytes + charge > MAX_SOURCE_BYTES as u64 {
        return refuse("pending byte quota exceeded for source");
    }
    if topic.ops >= MAX_TOPIC_OPS as u64 {
        return refuse("pending op topic quota exceeded");
    }
    if topic.bytes + charge > MAX_TOPIC_BYTES as u64 {
        return refuse("pending byte quota exceeded for topic");
    }
    Ok(())
}

/// Reject an entry whose dependency is incomplete in the same transaction that
/// writes it. This keeps admission, retries, and genesis reset from committing
/// dangling DAG edges.
pub(crate) fn ensure_deps_resolvable(
    entries: &[(Op, OpMeta)],
    mut stored_dep: impl FnMut(&OpId) -> Result<bool>,
) -> Result<()> {
    let batch = entries.iter().map(|(op, _)| op.id).collect::<BTreeSet<_>>();
    for (_, meta) in entries {
        for dep in &meta.deps {
            if !batch.contains(dep) && !stored_dep(dep)? {
                return Err(crate::Error::MissingDependency(*dep));
            }
        }
    }
    Ok(())
}
