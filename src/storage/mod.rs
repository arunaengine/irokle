// SPDX-License-Identifier: MIT OR Apache-2.0
//! Storage trait plus in-memory and Fjall-backed persistence implementations.

use std::collections::{BTreeMap, BTreeSet};

use crate::crypto::canonical_bytes;
use crate::{
    ActorClock, ActorId, EvictionKey, Op, OpId, PeerId, Result, TopicEviction, TopicId, TopicInfo,
};

pub const MAX_PENDING_OPS_TOTAL: usize = 4096;
pub const MAX_PENDING_OPS_PER_SOURCE: usize = 1024;
pub const MAX_PENDING_WAITERS_PER_DEP: usize = 1024;
/// Total byte limit on buffered ops, which core enforces for every backend. A count limit
/// alone cannot bound memory, since one op may be megabytes.
pub const MAX_PENDING_BYTES_TOTAL: usize = 64 * 1024 * 1024;
pub const MAX_PENDING_BYTES_PER_SOURCE: usize = 16 * 1024 * 1024;
pub const MAX_PENDING_MISSING_DEPS: usize = 128;
/// Pending ops and bytes one topic may hold, so one busy topic leaves room in
/// the shared pool for the others.
pub const MAX_PENDING_OPS_PER_TOPIC: usize = 2048;
pub const MAX_PENDING_BYTES_PER_TOPIC: usize = 32 * 1024 * 1024;
/// Rejected op ids a topic remembers, oldest dropped first.
pub const MAX_REJECTED_PER_TOPIC: usize = 4096;
/// Eviction records a store may hold unacknowledged. It bounds only a consumer that stopped
/// draining: a reset that would exceed it is refused rather than losing a payload.
pub const MAX_PENDING_EVICTIONS: usize = 1024;
/// How long a buffered op may wait for its dependencies before it expires, so one
/// source cannot hold pending capacity and repair requests forever.
pub const PENDING_IDLE_MS: u64 = 60 * 60 * 1000;

/// Reads of one coherent snapshot of a store. Every method sees the same commit,
/// so a planner can authorize a peer, select positions and load records without
/// mixing a state before and after a concurrent write.
pub trait SnapshotRead {
    /// Current branch and membership without loading a clock or unrelated topic indices.
    fn sync_identity(
        &self,
        _topic: &TopicId,
        _peer: &PeerId,
        _reserve: &mut dyn FnMut(SnapshotCharge) -> Result<()>,
    ) -> Result<Option<RequestView>> {
        Err(crate::Error::SyncCapacity(
            "backend must implement admitted sync authorization reads".into(),
        ))
    }

    /// Capture a finite clock only after its read and allocation bounds are admitted.
    fn sync_clock(
        &self,
        _topic: &TopicId,
        _actors: Option<&BTreeSet<ActorId>>,
        _reserve: &mut dyn FnMut(SnapshotCharge) -> Result<()>,
    ) -> Result<ActorClock> {
        Err(crate::Error::SyncCapacity(
            "backend must implement admitted sync clock capture".into(),
        ))
    }

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
    /// Read at most `limit` dependency IDs after this cursor, without decoding payloads.
    /// Advance both cursor fields by the returned IDs; a shorter page ends the set.
    /// Unsupported backends must implement bounded reads before serving sliced goals.
    fn dependency_ids(
        &self,
        _id: &OpId,
        _cursor: DependencyCursor,
        _limit: usize,
    ) -> Result<Option<Vec<OpId>>> {
        Err(crate::Error::SyncCapacity(
            "backend must implement bounded dependency reads".into(),
        ))
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
    /// At most `limit` ids of [`Self::list_op_ids`] after `after`, in id order. Backends
    /// override it to read one range; integrity scans list topics through it in steps.
    fn topic_ids_after(
        &self,
        topic_id: &TopicId,
        after: Option<&OpId>,
        limit: usize,
    ) -> Result<Vec<OpId>> {
        use std::ops::Bound::{Excluded, Unbounded};
        let start = after.map_or(Unbounded, |after| Excluded(*after));
        let ids = self.list_op_ids(topic_id)?;
        Ok(ids.range((start, Unbounded)).take(limit).copied().collect())
    }
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
    /// Whether `id` is stored completely enough to stand as a dependency: both its op and its
    /// metadata, read by backends from one snapshot. Every caller must use this one predicate.
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
    /// Whether an op with this id is buffered here. The default answers no, so a
    /// store without the lookup only charges a buffered op again, never too little.
    fn is_pending(&self, _op_id: &OpId) -> Result<bool> {
        Ok(false)
    }
    fn remove_pending_op(&self, op_id: &OpId) -> Result<()>;
    /// Drop buffered ops, and their waiters, that waited more than `max_idle_ms` since
    /// the first call that saw them, and return how many went. They were never
    /// acknowledged, so a later sync can resend them. The default keeps everything.
    fn expire_pending(&self, _now_ms: u64, _max_idle_ms: u64) -> Result<usize> {
        Ok(0)
    }
    /// Atomically drop every pending op that transitively waits on `dep_id`, a genesis that will
    /// never be admitted here, and return how many. Required: single removals are not atomic.
    fn purge_pending_waiters(&self, dep_id: &OpId) -> Result<usize>;
    /// Remove an invalid pending subtree in one step: `op_id` and every op waiting on it, or
    /// nothing once `op_id` is no longer buffered. The count includes `op_id`; required, since
    /// single removals are not atomic.
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
    /// Atomically persist `ack` and clear the obligations it satisfies, returning how many, in one
    /// durable write conditioned on the topic's current incarnation and membership; otherwise
    /// [`crate::Error::StaleIncarnation`] or [`crate::Error::NotTopicMember`].
    fn apply_peer_ack(&self, ack: PeerAck) -> Result<usize>;
    /// Apply acks in order as [`Storage::apply_peer_ack`] would, one result per ack, so one
    /// uncertifiable ack neither commits nor discards the rest. Backend failures use the outer
    /// error; uncertain commits require reopening and reconciliation before retry or cleanup.
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
    /// Atomically fold `update` into the status of `peer_id` on `topic_id` and return the result.
    /// Required: a read then a blind write would lose one of two concurrent outcomes.
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

    /// Atomically clear every local record of `topic_id`: ops, indexes, clock, buffered ops and
    /// every peer's acks, obligations and statuses, since a partial reset prevents convergence.
    /// Returns the number of admitted ops removed.
    fn reset_topic(&self, topic_id: &TopicId) -> Result<usize>;

    /// If the topic state is still `expected_topic_state`, replace the local chain with `batch`
    /// and journal `eviction` under [`TopicEviction::key`] in one durable operation, or change
    /// nothing; refused with [`crate::Error::EvictionJournalFull`] past [`MAX_PENDING_EVICTIONS`].
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

    /// Release the journalled eviction named by `key`, once the consumer durably owns its
    /// payloads. Releasing an absent key is not an error.
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
    /// Open the staging namespace of `source` for `topic_id` empty for `genesis`, or return the
    /// existing one unchanged. Refuses an active topic with [`crate::Error::AdmissionConflict`]
    /// and a namespace past the count limits with [`crate::Error::StagingCapacity`].
    fn open_provisional(
        &self,
        source: PeerId,
        topic_id: TopicId,
        genesis: OpId,
        now_ms: u64,
    ) -> Result<ProvisionalTopic>;
    /// The store holding only the history of `provisional`, or `None` once its session ended. Its
    /// reads and writes check that the session is still registered and staging, else
    /// [`crate::Error::StaleIncarnation`]; writes past a byte limit are `StagingCapacity`.
    fn provisional_store(&self, provisional: &ProvisionalTopic) -> Result<Option<Self>>;
    /// Serialized op bytes this store holds, admitted and buffered.
    fn stored_bytes(&self) -> Result<u64>;
    /// Record a write to the namespace while its session is current.
    fn touch_provisional(&self, provisional: &ProvisionalTopic, now_ms: u64) -> Result<()>;
    /// Make the history of `provisional` the active topic: claim its one activation, freezing the
    /// namespace at `expected`, then install state, heads, clock and `effects` and end every
    /// namespace in one transaction. An interrupted activation resumes when called again.
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

mod staging;
pub use staging::{
    MAX_STAGED_BYTES_PER_SESSION, MAX_STAGED_BYTES_TOTAL, MAX_STAGED_IDLE_MS, MAX_STAGED_SESSIONS,
    MAX_STAGED_SESSIONS_PER_SOURCE, ProvisionalTopic, StagedTopic, StagingLimits,
};
pub(super) use staging::{StagingQuota, check_namespaces};

mod evidence;
#[cfg(test)]
pub(crate) use evidence::MAX_RECENT_ATTEMPTS;
pub(super) use evidence::{
    AckCommit, ack_commit, ack_covers, ack_reached_op, apply_status_update, merged_obligation,
    merged_peer_ack, new_peer_status, settled_obligation, stored_ack_dominates,
};
pub use evidence::{
    AttemptOutcome, MAX_REPAIR_IDS, ObligationTarget, PeerAck, SyncObligation, SyncPeerState,
    SyncPeerStatus, SyncStateUpdate, SyncStatusUpdate,
};

mod record;
pub(crate) use record::pending_op_bytes;
pub use record::{
    AdmissionEffects, AdmittedBatch, ControlKey, CounterSnapshot, DependencyCursor, OpHeader,
    OpMeta, OpPosition, RequestView, SnapshotCharge, StorageCounters, TopicState, TopicView,
};
pub(super) use record::{
    PendingRecord, PendingUsage, check_pending_quota, ensure_deps_resolvable, validate_batch,
    validate_heads,
};

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
