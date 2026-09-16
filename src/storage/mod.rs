// SPDX-License-Identifier: MIT OR Apache-2.0
//! Storage trait plus in-memory and Fjall-backed persistence implementations.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::crypto::canonical_bytes;
use crate::topic::ReplicationPolicy;
use crate::{
    ActorClock, ActorId, EvictionKey, Op, OpId, PeerId, Result, TopicEviction, TopicId, TopicInfo,
};

pub const MAX_PENDING_OPS_TOTAL: usize = 4096;
pub const MAX_PENDING_OPS_PER_SOURCE: usize = 1024;
pub const MAX_PENDING_WAITERS_PER_DEP: usize = 1024;
/// Serialized bytes of buffered pending operations a store may hold, in total
/// and per authenticated source. A count limit alone does not bound memory: one
/// operation may be megabytes, so the count budget multiplied by the frame
/// limit is far larger than any node should buffer. Enforced in core, because
/// buffering happens with or without a transport feature.
pub const MAX_PENDING_BYTES_TOTAL: usize = 64 * 1024 * 1024;
pub const MAX_PENDING_BYTES_PER_SOURCE: usize = 16 * 1024 * 1024;
pub const MAX_PENDING_MISSING_DEPS: usize = 128;
/// Pending ops and bytes one topic may hold, so one busy topic leaves room in
/// the shared pool for the others.
pub const MAX_PENDING_OPS_PER_TOPIC: usize = 2048;
pub const MAX_PENDING_BYTES_PER_TOPIC: usize = 32 * 1024 * 1024;
/// Rejected op ids a topic remembers, oldest dropped first.
pub const MAX_REJECTED_PER_TOPIC: usize = 4096;
/// Eviction records a store may hold unacknowledged. A healthy consumer
/// acknowledges each record as soon as it owns the payloads durably, so this
/// only bounds a store whose consumer stopped draining; the reset that would
/// exceed it is refused rather than discarding a payload nothing else holds.
pub const MAX_PENDING_EVICTIONS: usize = 1024;
/// Limits of bootstrap staging: data for a topic this node does not hold yet,
/// kept per source and topic until its history proves membership.
pub const MAX_STAGED_BYTES_TOTAL: u64 = 64 * 1024 * 1024;
pub const MAX_STAGED_BYTES_PER_SESSION: u64 = 32 * 1024 * 1024;
pub const MAX_STAGED_SESSIONS: usize = 64;
pub const MAX_STAGED_SESSIONS_PER_SOURCE: usize = 8;
/// A session with no write for this long may be expired.
pub const MAX_STAGED_IDLE_MS: u64 = 10 * 60 * 1000;

/// Resources bootstrap staging may take before a history proves membership.
/// Each source and topic stages into its own namespace; bytes count every
/// serialized op a namespace holds, admitted or buffered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StagingLimits {
    pub total_bytes: u64,
    pub source_bytes: u64,
    pub namespace_bytes: u64,
    pub namespaces: usize,
    pub source_namespaces: usize,
}

impl StagingLimits {
    /// The envelope of a store that keeps staged history in memory.
    pub const MEMORY: Self = Self {
        total_bytes: MAX_STAGED_BYTES_TOTAL,
        source_bytes: MAX_STAGED_BYTES_PER_SESSION,
        namespace_bytes: MAX_STAGED_BYTES_PER_SESSION,
        namespaces: MAX_STAGED_SESSIONS,
        source_namespaces: MAX_STAGED_SESSIONS_PER_SOURCE,
    };

    /// The envelope of a store that keeps staged history on disk.
    pub const DISK: Self = Self {
        total_bytes: 16 * 1024 * 1024 * 1024,
        source_bytes: 4 * 1024 * 1024 * 1024,
        namespace_bytes: 4 * 1024 * 1024 * 1024,
        namespaces: MAX_STAGED_SESSIONS,
        source_namespaces: MAX_STAGED_SESSIONS_PER_SOURCE,
    };
}

/// A provisional bootstrap: history one source served for a topic this store
/// does not hold, kept in its own namespace and invisible to every topic query
/// until it proves this node's membership and is activated.
///
/// The value is also the capability of the namespace: a store of it
/// ([`Storage::provisional_store`]) reads and writes only while the backing
/// store still registers `session`, and writes only until activation begins.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvisionalTopic {
    pub source: PeerId,
    pub topic_id: TopicId,
    /// The candidate branch this namespace holds.
    pub genesis: OpId,
    /// Durable identity of the namespace; a replacement gets a new one.
    pub session: u64,
    /// Last touch, for idle expiry. Wall-clock milliseconds, kept across a restart.
    pub updated_ms: u64,
    /// Activation began: the namespace is frozen at the state it validated.
    pub activating: bool,
    /// Grows with every write to the namespace, so a decision taken on an
    /// older observation of it is refused.
    pub revision: u64,
    /// Serialized op bytes the namespace holds, admitted and buffered.
    pub bytes: u64,
}

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerAck {
    pub peer_id: PeerId,
    pub topic_id: TopicId,
    /// Genesis of the incarnation this evidence was signed against. `None` is
    /// a record migrated from a schema that did not identify its branch; it is
    /// retained but certifies nothing, because genesis replacement reuses the
    /// same actor ids and sequence numbers on the new branch.
    #[serde(default)]
    pub genesis: Option<OpId>,
    pub heads: BTreeSet<OpId>,
    pub clock: ActorClock,
}

/// Diagnostic counts of work a backend performed: op records and metadata
/// read through [`Storage`], buffered payloads and obligation records decoded
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
    /// Destructive data epoch, see [`Storage::topic_view`].
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

/// What one source staged for a topic this node does not hold. It is a
/// receipt, not an ack: it certifies nothing and clears no obligation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StagedTopic {
    /// The branch the namespace holds; `None` when nothing is staged.
    pub genesis: Option<OpId>,
    /// The namespace session this receipt belongs to.
    pub session: u64,
    /// Highest contiguous sequence staged per actor.
    pub clock: ActorClock,
    /// Serialized bytes the namespace holds, admitted and buffered.
    pub bytes: u64,
}

/// Explicit repair ids one peer may owe for one topic.
pub const MAX_REPAIR_IDS: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncObligation {
    pub peer_id: PeerId,
    pub topic_id: TopicId,
    pub target: ObligationTarget,
}

/// What a peer must prove before an obligation clears.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObligationTarget {
    /// A certified clock must reach this clock. One coalesced record per peer
    /// and topic.
    Clock(ActorClock),
    /// Ids without a local actor position. An ack settles an id it names as a
    /// head, or covers by clock once the id's metadata is known.
    Repair(BTreeSet<OpId>),
}

impl SyncObligation {
    pub fn clock(peer_id: PeerId, topic_id: TopicId, clock: ActorClock) -> Self {
        Self {
            peer_id,
            topic_id,
            target: ObligationTarget::Clock(clock),
        }
    }

    pub fn repair(peer_id: PeerId, topic_id: TopicId, ids: BTreeSet<OpId>) -> Self {
        Self {
            peer_id,
            topic_id,
            target: ObligationTarget::Repair(ids),
        }
    }

    /// Whether the record requires nothing; such a record is never stored.
    pub fn is_empty(&self) -> bool {
        match &self.target {
            ObligationTarget::Clock(clock) => clock.is_empty(),
            ObligationTarget::Repair(ids) => ids.is_empty(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SyncPeerState {
    #[default]
    Idle,
    Healthy,
    Behind,
    Failed,
}

/// How one sync attempt ended, measured against the goal it captured.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptOutcome {
    /// The captured goal was reached.
    Complete,
    /// Moved toward the goal without reaching it, such as a partial pull.
    Advanced,
    /// Could not move toward the goal, for the given reason.
    Blocked(String),
    /// The exchange failed, for the given reason.
    Failed(String),
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
    /// Newest attempt identity, `(epoch, sequence)`, whose outcome set the
    /// state, error and pending gauge.
    pub latest_attempt: Option<(u64, u64)>,
    /// The newest identities already counted, so a repeat of one of them
    /// counts nothing. Bounded by `MAX_RECENT_ATTEMPTS`. An older identity
    /// counts: the recorder counts each attempt once, see the transport's
    /// live attempts.
    pub recent_attempts: Vec<(u64, u64)>,
}

/// Attempt identities a status remembers to ignore their repeats. Exactly once
/// counting of every attempt belongs to the recorder, which ends a live
/// attempt on its first completion.
pub(crate) const MAX_RECENT_ATTEMPTS: usize = 32;

/// How one update moves the stored sync state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SyncStateUpdate {
    #[default]
    Keep,
    Set(SyncPeerState),
    /// Record falling behind without erasing an already recorded failure.
    BehindUnlessFailed,
}

/// One atomic change to a peer's sync status: attempt counts are deltas, the rest
/// are gauges left alone when unset. Updates are ordered by `attempt` when set, else
/// by timestamps, and `expected_attempts` drops a late update whose total changed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncStatusUpdate {
    pub successful_attempts: u64,
    pub failed_attempts: u64,
    pub state: SyncStateUpdate,
    pub pending_obligations: Option<usize>,
    pub last_attempt_ms: Option<u64>,
    pub last_success_ms: Option<u64>,
    /// Outer `None` keeps the stored error, inner `None` clears it.
    pub last_error: Option<Option<String>>,
    pub expected_attempts: Option<u64>,
    /// `(epoch, sequence)` of the attempt this outcome belongs to. The epoch
    /// comes from [`Storage::next_attempt_epoch`], so identities are never
    /// reused across restarts.
    pub attempt: Option<(u64, u64)>,
}

/// Branch, authorization and selected positions from one snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestView {
    pub genesis: OpId,
    pub epoch: u64,
    pub member: bool,
    pub clock: ActorClock,
}

/// Reads of one coherent snapshot of a store. Every method sees the same commit,
/// so a planner can authorize a peer, select positions and load records without
/// mixing a state before and after a concurrent write.
pub trait SnapshotRead {
    /// Actor count of this snapshot's topic, used to reserve a finite goal's workspace.
    fn actor_count(&self, topic_id: &TopicId) -> Result<usize> {
        Ok(self
            .topic_view(topic_id, None)?
            .map_or(0, |view| view.clock.len()))
    }
    /// See [`Storage::topic_view`].
    fn topic_view(&self, topic_id: &TopicId, peer_id: Option<&PeerId>)
    -> Result<Option<TopicView>>;
    /// Branch, authorization and selected actor positions from this snapshot.
    fn request_view(
        &self,
        topic_id: &TopicId,
        peer_id: &PeerId,
        actors: &BTreeSet<ActorId>,
    ) -> Result<Option<RequestView>> {
        Ok(self.topic_view(topic_id, None)?.map(|view| {
            let clock = view.clock.selected(actors);
            RequestView {
                genesis: view.state.genesis,
                epoch: view.epoch,
                member: view.state.members.contains(peer_id),
                clock,
            }
        }))
    }
    fn get_op(&self, id: &OpId) -> Result<Option<Op>>;
    /// Reserve before cloning or decoding an operation. Backends may size it exactly.
    fn get_reserved_op(
        &self,
        id: &OpId,
        reserve: &mut dyn FnMut(usize) -> Result<()>,
    ) -> Result<Option<Op>> {
        reserve(crate::sync::MAX_PAGE_BYTES)?;
        self.get_op(id)
    }
    fn get_meta(&self, id: &OpId) -> Result<Option<OpMeta>>;
    /// The position of `id`, as [`Self::get_meta`] would give it. Backends
    /// override it to leave the observed clock unread.
    fn get_position(&self, id: &OpId) -> Result<Option<OpPosition>> {
        Ok(self.get_meta(id)?.as_ref().map(OpPosition::from))
    }
    /// Position fields without dependency collections; see [`OpHeader`].
    fn get_header(&self, id: &OpId) -> Result<Option<OpHeader>> {
        Ok(self.get_position(id)?.as_ref().map(OpHeader::from))
    }
    /// Header and observed clock without copying the dependency collection.
    fn get_observation(&self, id: &OpId) -> Result<Option<(OpHeader, ActorClock)>> {
        Ok(self
            .get_meta(id)?
            .map(|meta| (OpHeader::from(&meta), meta.observed_clock)))
    }
    /// See [`Storage::dep_resolvable`].
    fn dep_resolvable(&self, id: &OpId) -> Result<bool>;
    /// See [`Storage::actor_range`].
    fn actor_range(
        &self,
        topic_id: &TopicId,
        actor_id: &ActorId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, OpId)>>;
    fn list_op_ids(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>>;
}

pub trait Storage: Clone + Send + Sync + 'static {
    #[cfg(test)]
    fn sync_boundary(&self, _topic_id: TopicId, _boundary: &'static str) {}

    /// Run `read` over one snapshot: one lock or one read transaction, released
    /// when `read` returns. `read` must not call back into this store. Required
    /// rather than defaulted: separate live reads can mix two commits.
    fn read_snapshot<R>(&self, read: impl FnOnce(&dyn SnapshotRead) -> Result<R>) -> Result<R>;
    /// Durably admit `batch`: each op and its [`OpMeta`] atomically, refusing deps without
    /// metadata, and clearing sync state of members the batch removes. A lost optimistic
    /// commit returns [`crate::Error::AdmissionConflict`]; the caller owns the retry budget.
    fn put_admitted_batch(&self, batch: AdmittedBatch) -> Result<()>;
    fn get_op(&self, id: &OpId) -> Result<Option<Op>>;
    fn get_meta(&self, id: &OpId) -> Result<Option<OpMeta>>;
    /// Compact position fields from one read context.
    fn get_header(&self, id: &OpId) -> Result<Option<OpHeader>> {
        self.read_snapshot(|read| read.get_header(id))
    }
    /// Header and observed clock from one read context.
    fn get_observation(&self, id: &OpId) -> Result<Option<(OpHeader, ActorClock)>> {
        self.read_snapshot(|read| read.get_observation(id))
    }
    /// The position of `id`, as [`Self::get_meta`] would give it. Backends
    /// override it to leave the observed clock unread.
    fn get_position(&self, id: &OpId) -> Result<Option<OpPosition>> {
        Ok(self.get_meta(id)?.as_ref().map(OpPosition::from))
    }
    /// Whether `id` is stored completely enough to stand as a dependency. The
    /// DAG is traversed through metadata and served from op records, so either
    /// half alone is a hole to refill, never a resolved edge. This is the one
    /// predicate every caller must use; backends override it to read both keys
    /// from a single snapshot.
    fn dep_resolvable(&self, id: &OpId) -> Result<bool> {
        Ok(self.get_op(id)?.is_some() && self.get_meta(id)?.is_some())
    }
    fn list_ops(&self, topic_id: &TopicId) -> Result<Vec<Op>>;
    fn list_op_ids(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>>;
    fn heads(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>>;
    fn children(&self, op_id: &OpId) -> Result<BTreeSet<OpId>>;
    fn actor_tip(&self, topic_id: &TopicId, actor_id: &ActorId) -> Result<Option<(u64, OpId)>>;
    fn actor_index(&self, topic_id: &TopicId, actor_id: &ActorId, seq: u64)
    -> Result<Option<OpId>>;
    /// Up to `limit` indexed positions of `actor_id` after `after`, ascending.
    fn actor_range(
        &self,
        topic_id: &TopicId,
        actor_id: &ActorId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, OpId)>>;
    fn actor_clock(&self, topic_id: &TopicId) -> Result<ActorClock>;
    fn topic_fingerprint(&self, topic_id: &TopicId) -> Result<[u8; 32]>;
    fn max_generation(&self, topic_id: &TopicId) -> Result<u64>;
    fn topic_state(&self, topic_id: &TopicId) -> Result<Option<TopicState>>;
    fn list_topics(&self) -> Result<Vec<TopicInfo>>;
    /// Read state, heads, clock, tips, fingerprint, data epoch, pending holes, `peer_id`'s
    /// ack and owed flag as one view. The epoch grows with every reset, never with an append.
    /// Required rather than defaulted: separate reads can mix two commits.
    fn topic_view(&self, topic_id: &TopicId, peer_id: Option<&PeerId>)
    -> Result<Option<TopicView>>;
    /// Buffer `op` until `meta.missing_deps` resolve. A duplicate keeps its
    /// stored source and charge. An op that is or waits on a rejected id of
    /// its topic is refused with [`crate::Error::RejectedOp`].
    fn put_pending_op(&self, source_peer: PeerId, op: Op, meta: OpMeta) -> Result<()>;
    /// Buffer under a captured genesis checked atomically with the write.
    /// `None` keeps the unscoped contract; scoped writes require backend support.
    fn put_pending_bound(
        &self,
        source_peer: PeerId,
        op: Op,
        meta: OpMeta,
        genesis: Option<OpId>,
    ) -> Result<()> {
        if genesis.is_some() {
            return Err(crate::Error::Storage(
                "backend lacks scoped pending writes".into(),
            ));
        }
        self.put_pending_op(source_peer, op, meta)
    }
    fn pending_waiters(&self, dep_id: &OpId) -> Result<Vec<(PeerId, Op)>>;
    fn ready_pending_ops(&self) -> Result<Vec<(PeerId, Op)>> {
        self.ready_pending_after(None, usize::MAX)
    }
    /// Up to `limit` buffered ops whose dependencies all resolved, in id order
    /// after `after`. Backends keep a ready index, so no payload of a waiting
    /// op is read.
    fn ready_pending_after(&self, after: Option<&OpId>, limit: usize) -> Result<Vec<(PeerId, Op)>>;
    /// Dependencies that buffered ops of `topic_id` are still waiting for.
    /// Sync planning turns these into wants, so a hole a peer never pushes is
    /// actively pulled instead of stranding its dependents forever.
    fn pending_missing_deps(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>>;
    fn remove_pending_op(&self, op_id: &OpId) -> Result<()>;
    /// Atomically drop every pending op that transitively waits on `dep_id`.
    /// Genesis tie-break resolution uses this for a genesis that will never be
    /// admitted here; a partial walk would strand waiters holding pending quota
    /// against a dependency that can never arrive. Returns the number removed.
    /// Required rather than defaulted: a composition of single removals is
    /// correct but not atomic, and a backend must not inherit that silently.
    fn purge_pending_waiters(&self, dep_id: &OpId) -> Result<usize>;
    /// Atomically drop a permanently invalid pending `op_id` and every pending
    /// op transitively waiting on it, or nothing at all once `op_id` is no
    /// longer buffered, so a concurrent admission keeps its waiters. Required
    /// rather than defaulted for the same atomicity reason as above; the count
    /// returned includes `op_id`.
    fn reject_pending_subtree(&self, op_id: &OpId) -> Result<usize>;
    fn peer_ack(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<Option<PeerAck>>;
    fn peer_acks(&self, topic_id: &TopicId) -> Result<Vec<PeerAck>>;
    /// Merge `obligation` into the stored record of its kind, conditioned in
    /// the writing transaction on the topic's genesis still being
    /// `expected_genesis` (`None`: no topic); otherwise [`crate::Error::StaleIncarnation`].
    fn put_sync_obligation(
        &self,
        obligation: SyncObligation,
        expected_genesis: Option<OpId>,
    ) -> Result<()>;
    fn all_sync_obligations(&self) -> Result<Vec<SyncObligation>>;
    /// Atomically persist `ack` and clear any obligations satisfied by it.
    /// Backends must perform both writes in one durable operation so a crash
    /// between them cannot leave the ack visible while obligations remain,
    /// or vice-versa. Returns the number of cleared obligations.
    ///
    /// The topic's current identity and membership must be read in that same
    /// operation and the write conditioned on them, so evidence validated
    /// before a concurrent reset or peer removal cannot still commit. Evidence
    /// naming another incarnation is refused with
    /// [`crate::Error::StaleIncarnation`]; a removed peer's with
    /// [`crate::Error::NotTopicMember`].
    fn apply_peer_ack(&self, ack: PeerAck) -> Result<usize>;
    /// Apply acks in order as [`Storage::apply_peer_ack`] would, one result per ack, so one
    /// uncertifiable ack neither commits nor discards the rest. A batching backend returns a
    /// backend failure as the outer error and commits nothing.
    fn apply_peer_acks(&self, acks: Vec<PeerAck>) -> Result<Vec<Result<usize>>> {
        Ok(acks
            .into_iter()
            .map(|ack| self.apply_peer_ack(ack))
            .collect())
    }
    fn sync_obligations(&self, peer_id: &PeerId, topic_id: &TopicId)
    -> Result<Vec<SyncObligation>>;
    /// How many obligation records `peer_id` holds for `topic_id`, one per
    /// target kind. Backends answer from keys without decoding the records.
    fn sync_obligation_count(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<usize> {
        Ok(self.sync_obligations(peer_id, topic_id)?.len())
    }
    fn has_sync_obligations(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<bool> {
        Ok(!self.sync_obligations(peer_id, topic_id)?.is_empty())
    }
    /// Durably advance and return the attempt epoch, in one transaction, so
    /// every start of a transport gets an epoch no earlier start used.
    fn next_attempt_epoch(&self) -> Result<u64>;
    fn put_sync_status(&self, status: SyncPeerStatus) -> Result<()>;
    /// Atomically fold `update` into the status of `peer_id` on `topic_id` and
    /// return the result. Backends must read, apply and write in one lock or
    /// transaction: two outcomes recorded at once would otherwise each miss the
    /// other's counter increment and the later writer's state would win.
    /// Required rather than defaulted: a composition of a read and a blind
    /// write is not atomic, and a backend must not inherit that silently.
    fn update_sync_status(
        &self,
        peer_id: &PeerId,
        topic_id: &TopicId,
        update: &SyncStatusUpdate,
    ) -> Result<SyncPeerStatus>;
    fn sync_statuses(&self, topic_id: &TopicId) -> Result<Vec<SyncPeerStatus>>;
    /// How many obligation records each peer holds for `topic_id`. Reports read
    /// this instead of every stored obligation; a record count is a count of
    /// outstanding targets, not of missing operations.
    fn topic_obligation_counts(&self, topic_id: &TopicId) -> Result<BTreeMap<PeerId, usize>>;
    /// Drop obligations, sync status and the ack of a peer that left the topic,
    /// only if the genesis is still `expected_genesis` and the peer is not a
    /// member, checked in the writing transaction. Returns cleared obligations.
    fn clear_peer_sync_state(
        &self,
        peer_id: &PeerId,
        topic_id: &TopicId,
        expected_genesis: Option<OpId>,
    ) -> Result<usize>;

    /// Atomically remove every local record for `topic_id`: topic/genesis
    /// registration, all ops and their metadata, actor indexes/tips, heads,
    /// fingerprint, max generation, the topic's actor clock, buffered pending
    /// ops targeting the topic, and every peer's stored acks, sync
    /// obligations, and sync statuses for the topic. Used by genesis tie-break
    /// resolution to adopt a winning foreign genesis; a partial reset would
    /// leave stale actor tips or acks that keep sync clocks diverging, so
    /// backends must clear all per-topic keyspaces. Returns the number of
    /// admitted ops removed.
    fn reset_topic(&self, topic_id: &TopicId) -> Result<usize>;

    /// Atomically verify that the current topic state is exactly
    /// `expected_topic_state`, then [`Storage::reset_topic`] and apply `batch`
    /// in one durable operation. Genesis tie-break adoption uses this to
    /// discard the local chain and install the winning foreign genesis with no
    /// crash window between the two: a crash either leaves the whole local
    /// chain or the fully installed winner, never an empty topic. The expected
    /// state check prevents a stale resolver from overwriting a smaller genesis
    /// admitted by another facade. `batch` must be built against a fresh topic
    /// (empty `expected_heads`, `None` `expected_topic_state`). Returns the
    /// number of admitted ops the reset removed. A rejected `batch` must leave
    /// the local chain exactly as it was. Required rather than defaulted: a
    /// reset followed by a separate admission is not atomic, and a backend must
    /// not inherit that silently.
    ///
    /// `eviction` describes the payloads this reset discards. When it carries
    /// any, backends must journal it under [`TopicEviction::key`] in this same
    /// transaction: the reset is the moment those payloads stop existing
    /// anywhere else, so a record written afterwards would leave a crash window
    /// that loses acknowledged writes. The record is released by
    /// [`Storage::clear_eviction`], never by the reset itself. A reset that
    /// would push the store past [`MAX_PENDING_EVICTIONS`] outstanding records
    /// must be refused with [`crate::Error::EvictionJournalFull`], leaving the
    /// local chain in place.
    fn reset_topic_and_admit(
        &self,
        topic_id: &TopicId,
        expected_topic_state: &TopicState,
        batch: AdmittedBatch,
        eviction: Option<&TopicEviction>,
    ) -> Result<usize>;

    /// Persist a drain seal that makes the topic's reset-and-admit operation
    /// fail closed until the holder is again allowed to publish.
    fn seal_topic(&self, topic_id: &TopicId) -> Result<bool>;
    fn unseal_topic(&self, topic_id: &TopicId) -> Result<bool>;

    /// Journalled evictions no consumer has acknowledged yet. A restart drains
    /// these before eviction recovery can be considered complete; each is the
    /// only remaining copy of the payloads its reset removed.
    fn pending_evictions(&self) -> Result<Vec<TopicEviction>>;

    /// Release the journalled eviction named by `key`. The consumer calls this
    /// only once it durably owns the payloads, so a crash before that point
    /// leaves the record for the next restart. Releasing an absent key is not
    /// an error: acknowledgement is idempotent.
    fn clear_eviction(&self, key: &EvictionKey) -> Result<()>;

    /// Whether `peer_id` holds `op_id` on the branch that currently stores it. Required:
    /// the op's metadata, the genesis and the ack must come from one view, or a reset in
    /// between lets evidence prove an op of the discarded branch.
    fn peer_reached_op(&self, peer_id: &PeerId, op_id: &OpId) -> Result<bool>;

    /// Every peer [`Storage::peer_reached_op`] would confirm, sorted, read
    /// from one view.
    fn peers_reached_op(&self, op_id: &OpId) -> Result<Vec<PeerId>>;

    /// The limits this store applies to provisional bootstraps. Every handle
    /// checks its own limits against the bytes all namespaces of the backing
    /// store hold, when those bytes commit.
    fn staging_limits(&self) -> StagingLimits;
    /// Every provisional bootstrap namespace, read at one moment.
    fn provisional_topics(&self) -> Result<Vec<ProvisionalTopic>>;
    /// The namespace of `source` for `topic_id`, opened empty for `genesis` when
    /// the source has none. An existing namespace is returned unchanged, whatever
    /// genesis it holds. Refuses an active topic with
    /// [`crate::Error::AdmissionConflict`] and a namespace past the count limits
    /// with [`crate::Error::StagingCapacity`].
    fn open_provisional(
        &self,
        source: PeerId,
        topic_id: TopicId,
        genesis: OpId,
        now_ms: u64,
    ) -> Result<ProvisionalTopic>;
    /// The store holding the history of `provisional`, or `None` once its
    /// session ended. It sees only that namespace. Each of its reads checks that
    /// the session is still registered and each write, in the transaction that
    /// commits it, that the session still stages; otherwise
    /// [`crate::Error::StaleIncarnation`] before any effect. A write past the
    /// namespace, total or source byte limit is refused with
    /// [`crate::Error::StagingCapacity`].
    fn provisional_store(&self, provisional: &ProvisionalTopic) -> Result<Option<Self>>;
    /// Serialized op bytes this store holds, admitted and buffered.
    fn stored_bytes(&self) -> Result<u64>;
    /// Record a write to the namespace while its session is current.
    fn touch_provisional(&self, provisional: &ProvisionalTopic, now_ms: u64) -> Result<()>;
    /// Make the history of `provisional` the active topic. The first step
    /// claims the topic's one activation for this session, which refuses a
    /// claim of another session with [`crate::Error::AdmissionConflict`], and
    /// freezes the namespace at `expected`. Nothing of the history is visible
    /// to a read of the store until one transaction installs the state, heads,
    /// clock and `effects` and ends every namespace of the topic. An
    /// interrupted activation resumes when called again.
    fn activate_provisional(
        &self,
        provisional: &ProvisionalTopic,
        expected: &TopicState,
        effects: AdmissionEffects,
    ) -> Result<()>;
    /// End the namespace of `provisional` while it is still exactly as
    /// observed, same session, revision and touch, and not activating. Returns
    /// whether it ended.
    fn discard_provisional(&self, provisional: &ProvisionalTopic) -> Result<bool>;
}

mod memory;
pub use memory::{MemoryDomain, MemoryLimits, MemoryStorage, MemoryUsage};

#[cfg(feature = "fjall")]
mod fjall;
#[cfg(feature = "fjall")]
mod pressure;
#[cfg(feature = "fjall")]
pub use fjall::FjallStorage;
#[cfg(all(test, feature = "fjall"))]
pub(crate) use fjall::Hook;
#[cfg(all(test, feature = "fjall"))]
pub(crate) use fjall::write_legacy_metas;
#[cfg(feature = "fjall")]
pub use pressure::{StorageDomain, StoragePressure, StorageUsage};

pub(super) fn validate_batch(batch: &AdmittedBatch) -> Result<()> {
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

pub(super) fn validate_heads(
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
pub(super) struct PendingUsage {
    pub(super) ops: u64,
    pub(super) bytes: u64,
}

impl PendingUsage {
    pub(super) fn charged(self, bytes: u64) -> Self {
        Self {
            ops: self.ops + 1,
            bytes: self.bytes + bytes,
        }
    }

    /// Usage after refunding one op of `bytes`. Underflow means the counters
    /// no longer describe the records, which is corruption, not a clean pool.
    pub(super) fn refunded(self, bytes: u64) -> Result<Self> {
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
pub(super) struct PendingRecord {
    pub(super) source: PeerId,
    pub(super) topic_id: TopicId,
    pub(super) missing: BTreeSet<OpId>,
    pub(super) charge: u64,
}

/// Refuse a new pending op of `charge` bytes that would push the total, its
/// source or its topic past their limits.
pub(super) fn check_pending_quota(
    total: PendingUsage,
    source: PendingUsage,
    topic: PendingUsage,
    charge: u64,
) -> Result<()> {
    let refuse = |message: &str| Err(crate::Error::Storage(message.into()));
    if total.ops >= MAX_PENDING_OPS_TOTAL as u64 {
        return refuse("pending op buffer is full");
    }
    if total.bytes + charge > MAX_PENDING_BYTES_TOTAL as u64 {
        return refuse("pending byte budget is full");
    }
    if source.ops >= MAX_PENDING_OPS_PER_SOURCE as u64 {
        return refuse("pending op source quota exceeded");
    }
    if source.bytes + charge > MAX_PENDING_BYTES_PER_SOURCE as u64 {
        return refuse("pending byte quota exceeded for source");
    }
    if topic.ops >= MAX_PENDING_OPS_PER_TOPIC as u64 {
        return refuse("pending op topic quota exceeded");
    }
    if topic.bytes + charge > MAX_PENDING_BYTES_PER_TOPIC as u64 {
        return refuse("pending byte quota exceeded for topic");
    }
    Ok(())
}

/// Refuse a new provisional namespace past the total or per-source count.
pub(super) fn check_namespaces(
    limits: &StagingLimits,
    namespaces: usize,
    from_source: usize,
) -> Result<()> {
    if namespaces >= limits.namespaces {
        return Err(crate::Error::StagingCapacity(
            "bootstrap namespaces are full".into(),
        ));
    }
    if from_source >= limits.source_namespaces {
        return Err(crate::Error::StagingCapacity(
            "bootstrap namespace quota exceeded for source".into(),
        ));
    }
    Ok(())
}

/// Bytes one namespace may hold: its own limit and what the other namespaces
/// leave of the total and of its source's quota, read where the bytes commit.
#[derive(Clone, Copy, Debug)]
pub(super) struct StagingQuota {
    namespace: u64,
    total: u64,
    source: u64,
}

impl StagingQuota {
    /// The quota of a namespace beside others holding `others` bytes in
    /// total, of which `source` belong to its own source.
    pub(super) fn new(limits: &StagingLimits, others: u64, source: u64) -> Self {
        Self {
            namespace: limits.namespace_bytes,
            total: limits.total_bytes.saturating_sub(others),
            source: limits.source_bytes.saturating_sub(source),
        }
    }

    /// Refuse `charge` more bytes in a namespace holding `held`.
    pub(super) fn check(&self, held: u64, charge: u64) -> Result<()> {
        if charge == 0 {
            return Ok(());
        }
        let refuse = |message: &str| Err(crate::Error::StagingCapacity(message.into()));
        let Some(after) = held.checked_add(charge) else {
            return refuse("bootstrap staging byte count overflow");
        };
        if after > self.namespace {
            return refuse("bootstrap namespace byte quota exceeded");
        }
        if after > self.total {
            return refuse("bootstrap staging byte budget is full");
        }
        if after > self.source {
            return refuse("bootstrap staging byte quota exceeded for source");
        }
        Ok(())
    }
}

pub(crate) fn topic_fingerprint_for(
    heads: &BTreeSet<OpId>,
    clock: &ActorClock,
) -> Result<[u8; 32]> {
    Ok(*blake3::hash(&canonical_bytes(&(heads, clock))?).as_bytes())
}

/// Whether a write expecting `expected` may proceed against topic `state`.
pub(super) fn branch_matches(state: Option<&TopicState>, expected: Option<OpId>) -> Result<()> {
    if state.map(|state| state.genesis) == expected {
        Ok(())
    } else {
        Err(crate::Error::StaleIncarnation)
    }
}

/// Whether sync state of `peer_id` may be dropped from topic `state`.
pub(super) fn peer_departed(
    state: Option<&TopicState>,
    peer_id: &PeerId,
    expected: Option<OpId>,
) -> bool {
    branch_matches(state, expected).is_ok()
        && state.is_none_or(|state| !state.members.contains(peer_id))
}

/// Whether stored evidence certified for `genesis` already covers an ordinary
/// target, so writing it would only create work that is already done.
pub(super) fn ack_covers(
    ack: Option<&PeerAck>,
    genesis: Option<OpId>,
    obligation: &SyncObligation,
) -> bool {
    let ObligationTarget::Clock(target) = &obligation.target else {
        return false;
    };
    genesis.is_some()
        && ack.is_some_and(|ack| ack.genesis == genesis && ack.clock.dominates(target))
}

/// Merge `incoming` into the stored record of the same kind. A repair record
/// past [`MAX_REPAIR_IDS`] is refused rather than growing without bound.
pub(super) fn merged_obligation(
    existing: Option<SyncObligation>,
    incoming: &SyncObligation,
) -> Result<SyncObligation> {
    if matches!(&incoming.target, ObligationTarget::Repair(ids) if ids.len() > MAX_REPAIR_IDS) {
        return Err(crate::Error::Storage(
            "repair obligation exceeds its id limit".into(),
        ));
    }
    let Some(mut merged) = existing else {
        return Ok(incoming.clone());
    };
    match (&mut merged.target, &incoming.target) {
        (ObligationTarget::Clock(stored), ObligationTarget::Clock(clock)) => stored.merge(clock),
        (ObligationTarget::Repair(stored), ObligationTarget::Repair(ids)) => {
            stored.extend(ids.iter().copied());
            if stored.len() > MAX_REPAIR_IDS {
                return Err(crate::Error::Storage(
                    "repair obligation exceeds its id limit".into(),
                ));
            }
        }
        _ => {
            return Err(crate::Error::Storage(
                "obligation kinds differ under one key".into(),
            ));
        }
    }
    Ok(merged)
}

/// What remains of `obligation` after certified `ack`, or `None` when nothing
/// does. Each covered clock entry or id is dropped even while the rest stays
/// outstanding; `meta` resolves a repair id's actor position.
pub(super) fn settled_obligation(
    obligation: &SyncObligation,
    ack: &PeerAck,
    mut meta: impl FnMut(&OpId) -> Result<Option<OpPosition>>,
) -> Result<Option<SyncObligation>> {
    let target = match &obligation.target {
        ObligationTarget::Clock(target) => {
            let mut rest = ActorClock::new();
            for (actor_id, seq) in target.iter() {
                if ack.clock.get(actor_id) < *seq {
                    rest.observe(*actor_id, *seq);
                }
            }
            ObligationTarget::Clock(rest)
        }
        ObligationTarget::Repair(ids) => {
            let mut rest = BTreeSet::new();
            for id in ids {
                let covered = ack.heads.contains(id)
                    || meta(id)?.is_some_and(|meta| {
                        meta.topic_id == obligation.topic_id
                            && ack.clock.get(&meta.actor_id) >= meta.actor_seq
                    });
                if !covered {
                    rest.insert(*id);
                }
            }
            ObligationTarget::Repair(rest)
        }
    };
    let rest = SyncObligation {
        target,
        ..obligation.clone()
    };
    Ok((!rest.is_empty()).then_some(rest))
}

/// Fold one update into `status`; returns whether it changed. Counts always accumulate;
/// gauges install only from a newer update: by `attempt` identity when set, else by
/// timestamp, where a stored success wins a tie with a failure.
pub(super) fn apply_status_update(status: &mut SyncPeerStatus, update: &SyncStatusUpdate) -> bool {
    let attempts = status
        .successful_attempts
        .saturating_add(status.failed_attempts);
    let expected = update.expected_attempts.is_none_or(|want| want == attempts);
    let current = match update.attempt {
        Some(attempt) => {
            if status.latest_attempt == Some(attempt) || status.recent_attempts.contains(&attempt) {
                return false;
            }
            status.recent_attempts.push(attempt);
            status.recent_attempts.sort_unstable();
            let excess = status
                .recent_attempts
                .len()
                .saturating_sub(MAX_RECENT_ATTEMPTS);
            status.recent_attempts.drain(..excess);
            let current = expected && status.latest_attempt.is_none_or(|latest| attempt > latest);
            if current {
                status.latest_attempt = Some(attempt);
            }
            current
        }
        None => expected && !stale_outcome(status, update),
    };
    let counted =
        update.successful_attempts > 0 || update.failed_attempts > 0 || update.attempt.is_some();
    status.successful_attempts = status
        .successful_attempts
        .saturating_add(update.successful_attempts);
    status.failed_attempts = status
        .failed_attempts
        .saturating_add(update.failed_attempts);
    if let Some(attempt_ms) = update.last_attempt_ms {
        status.last_attempt_ms = Some(status.last_attempt_ms.unwrap_or(attempt_ms).max(attempt_ms));
    }
    if let Some(success_ms) = update.last_success_ms {
        status.last_success_ms = Some(status.last_success_ms.unwrap_or(success_ms).max(success_ms));
    }
    if !current {
        return counted;
    }
    if let Some(pending) = update.pending_obligations {
        status.pending_obligations = pending;
    }
    if let Some(error) = &update.last_error {
        status.last_error = error.clone();
    }
    match update.state {
        SyncStateUpdate::Keep => {}
        SyncStateUpdate::Set(state) => status.state = state,
        SyncStateUpdate::BehindUnlessFailed => {
            if status.state != SyncPeerState::Failed {
                status.state = SyncPeerState::Behind;
            }
        }
    }
    true
}

/// Whether `update` describes an attempt older than the record's newest, in
/// either direction. Its counters still apply; only the state, error and
/// pending gauge it would install are dropped.
fn stale_outcome(status: &SyncPeerStatus, update: &SyncStatusUpdate) -> bool {
    let Some(attempt_ms) = update.last_attempt_ms else {
        return false;
    };
    if status
        .last_attempt_ms
        .is_some_and(|newest| attempt_ms < newest)
    {
        return true;
    }
    // Same millisecond as a recorded success: keep the success, because
    // `Failed` is the stronger claim and the next attempt re-marks a peer that
    // really is failing.
    update.failed_attempts > 0
        && update.successful_attempts == 0
        && status.last_success_ms.is_some_and(|ok| attempt_ms <= ok)
}

/// The status a backend starts from when a peer has no record yet.
pub(super) fn new_peer_status(peer_id: PeerId, topic_id: TopicId) -> SyncPeerStatus {
    SyncPeerStatus {
        peer_id,
        topic_id,
        ..SyncPeerStatus::default()
    }
}

/// Reject a batch whose entry depends on an op that is not stored completely,
/// checked against the same transaction that will write it. `stored_dep` must
/// apply the [`Storage::dep_resolvable`] predicate inside that transaction.
/// Enforcing this at the durability boundary is what keeps every admission path
/// (batch admission, admission retry, genesis reset) from committing a dangling
/// DAG edge.
pub(super) fn ensure_deps_resolvable(
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

/// The eviction a reset must journal, with the key it takes. An eviction with
/// no payloads leaves nothing to recover, so it takes no record and no
/// acknowledgement.
pub(super) fn journalled_eviction(
    eviction: Option<&TopicEviction>,
) -> Option<(EvictionKey, &TopicEviction)> {
    eviction
        .filter(|eviction| !eviction.evicted.is_empty())
        .map(|eviction| (eviction.key(), eviction))
}

/// Whether `ack` proves the peer holds the operation `meta` describes. Only
/// evidence certified against the topic's current genesis counts: a record from
/// a replaced branch names the same actor sequences without covering them.
pub(super) fn ack_reached_op(
    ack: &PeerAck,
    genesis: OpId,
    id: &OpId,
    position: &OpPosition,
) -> bool {
    ack.genesis == Some(genesis)
        && (ack.heads.contains(id) || ack.clock.get(&position.actor_id) >= position.actor_seq)
}

/// What one acknowledgement may do to stored state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AckCommit {
    /// The record proves the current branch: store it and clear what it covers.
    Certify,
    /// The record is kept under the genesis it names but proves nothing here,
    /// because no local topic holds that branch yet.
    Retain,
}

/// How to commit `ack` given `state`, which backends must read in the same
/// transaction that writes it. Evidence proves something about one branch and
/// one member, so a replaced genesis or a removed peer makes it uncertifiable
/// rather than merely stale, and only a matching branch may clear obligations.
pub(super) fn ack_commit(state: Option<&TopicState>, ack: &PeerAck) -> Result<AckCommit> {
    let Some(state) = state else {
        return Ok(AckCommit::Retain);
    };
    if ack.genesis != Some(state.genesis) {
        return Err(crate::Error::StaleIncarnation);
    }
    if !state.members.contains(&ack.peer_id) {
        return Err(crate::Error::NotTopicMember);
    }
    Ok(AckCommit::Certify)
}

pub(super) fn stored_ack_dominates(existing: &PeerAck, incoming: &PeerAck) -> bool {
    same_incarnation(existing, incoming) && existing.clock.dominates(&incoming.clock)
}

/// Whether two records describe the same peer, topic, and certified branch. A
/// pair with no certified genesis is never the same incarnation: an unidentified
/// record must not lend its clock to a certified one.
fn same_incarnation(existing: &PeerAck, incoming: &PeerAck) -> bool {
    existing.peer_id == incoming.peer_id
        && existing.topic_id == incoming.topic_id
        && existing.genesis.is_some()
        && existing.genesis == incoming.genesis
}

/// The ack to store once `incoming` is not covered by `existing`. Clock
/// components the stored ack already proved are kept, so evidence that is
/// merely incomparable adds to the record instead of regressing it. The stored
/// frontier follows the newer ack: its clock still carries the older heads.
pub(super) fn merged_peer_ack(existing: &PeerAck, incoming: &PeerAck) -> PeerAck {
    let mut merged = incoming.clone();
    if same_incarnation(existing, incoming) {
        merged.clock.merge(&existing.clock);
    }
    merged
}
