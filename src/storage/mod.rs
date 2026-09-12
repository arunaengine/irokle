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
/// Eviction records a store may hold unacknowledged. A healthy consumer
/// acknowledges each record as soon as it owns the payloads durably, so this
/// only bounds a store whose consumer stopped draining; the reset that would
/// exceed it is refused rather than discarding a payload nothing else holds.
pub const MAX_PENDING_EVICTIONS: usize = 1024;

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
    /// Genesis of the incarnation this evidence was signed against. `None` is
    /// a record migrated from a schema that did not identify its branch; it is
    /// retained but certifies nothing, because genesis replacement reuses the
    /// same actor ids and sequence numbers on the new branch.
    #[serde(default)]
    pub genesis: Option<OpId>,
    pub heads: BTreeSet<OpId>,
    pub clock: ActorClock,
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

/// How one update moves the stored sync state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SyncStateUpdate {
    #[default]
    Keep,
    Set(SyncPeerState),
    /// Record falling behind without erasing an already recorded failure.
    BehindUnlessFailed,
}

/// One atomic change to a peer's sync status. `successful_attempts` and
/// `failed_attempts` are deltas added to the stored counters; the rest are
/// gauges that leave the stored value alone when unset. Timestamps only move
/// forward and `expected_attempts` drops the whole update unless the stored
/// attempt total still matches, so a late outcome cannot overwrite a newer one.
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
}

pub trait Storage: Clone + Send + Sync + 'static {
    /// Durably admit `batch`. Backends must write each entry's op record and
    /// its [`OpMeta`] in one atomic unit and must reject a batch whose entry
    /// depends on an op with no metadata, so a committed op can never reference
    /// a dependency the DAG cannot resolve.
    /// The same transaction must clear sync state for members present in the
    /// expected topic state but absent from the committed topic state.
    fn put_admitted_batch(&self, batch: AdmittedBatch) -> Result<()>;
    fn get_op(&self, id: &OpId) -> Result<Option<Op>>;
    fn get_meta(&self, id: &OpId) -> Result<Option<OpMeta>>;
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
    fn actor_clock(&self, topic_id: &TopicId) -> Result<ActorClock>;
    fn topic_fingerprint(&self, topic_id: &TopicId) -> Result<[u8; 32]>;
    fn max_generation(&self, topic_id: &TopicId) -> Result<u64>;
    fn topic_state(&self, topic_id: &TopicId) -> Result<Option<TopicState>>;
    fn list_topics(&self) -> Result<Vec<TopicInfo>>;
    /// Read the topic's state, heads, clock, tips, fingerprint, data epoch
    /// and pending holes as one view, plus `peer_id`'s stored ack and whether
    /// it is owed work. The epoch grows with every destructive change of the
    /// topic (reset, reset-and-admit) and never with an append, so anything
    /// keyed by genesis and epoch cannot outlive the data it describes.
    /// Required rather than defaulted: separate reads can mix two commits.
    fn topic_view(&self, topic_id: &TopicId, peer_id: Option<&PeerId>)
    -> Result<Option<TopicView>>;
    fn put_pending_op(&self, source_peer: PeerId, op: Op, meta: OpMeta) -> Result<()>;
    fn pending_waiters(&self, dep_id: &OpId) -> Result<Vec<(PeerId, Op)>>;
    fn ready_pending_ops(&self) -> Result<Vec<(PeerId, Op)>>;
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
    /// Apply many peer acks in order, equivalent to calling
    /// [`Storage::apply_peer_ack`] per ack. Backends may batch all writes into
    /// one durable operation. Returns one result per input ack, in order, so a
    /// single uncertifiable record neither commits nor discards the rest. A
    /// reported failure must never leave that ack's writes committed: a backend
    /// that batches returns a backend failure as the outer error and commits
    /// nothing of the batch.
    fn apply_peer_acks(&self, acks: Vec<PeerAck>) -> Result<Vec<Result<usize>>> {
        Ok(acks
            .into_iter()
            .map(|ack| self.apply_peer_ack(ack))
            .collect())
    }
    fn sync_obligations(&self, peer_id: &PeerId, topic_id: &TopicId)
    -> Result<Vec<SyncObligation>>;
    fn has_sync_obligations(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<bool> {
        Ok(!self.sync_obligations(peer_id, topic_id)?.is_empty())
    }
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

    /// Whether `peer_id` holds `op_id` on the branch that currently stores it.
    /// Required rather than defaulted: the op's metadata, the topic genesis
    /// and the ack must come from one view, or evidence installed by a reset
    /// in between proves an op of the discarded branch.
    fn peer_reached_op(&self, peer_id: &PeerId, op_id: &OpId) -> Result<bool>;

    /// Every peer [`Storage::peer_reached_op`] would confirm, sorted, read
    /// from one view.
    fn peers_reached_op(&self, op_id: &OpId) -> Result<Vec<PeerId>>;
}

mod memory;
pub use memory::MemoryStorage;

#[cfg(feature = "fjall")]
mod fjall;
#[cfg(feature = "fjall")]
pub use fjall::FjallStorage;

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

/// Serialized size charged against the pending byte budgets. Deterministic for
/// a given operation, so the charge on insertion and the refund on removal
/// always match.
pub(crate) fn pending_op_bytes(op: &Op) -> Result<usize> {
    Ok(postcard::to_allocvec(op)?.len())
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
    mut meta: impl FnMut(&OpId) -> Result<Option<OpMeta>>,
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

/// Fold one status update into `status`, reporting whether the record changed
/// and must be persisted. Counters accumulate and timestamps only advance, so
/// two concurrent outcomes keep both increments and a late one cannot rewind
/// the record.
///
/// A stale update still counts its attempt: the attempt did happen, and losing
/// the increment would undercount work. Only the state, error and pending gauge
/// it would install are dropped, because those describe a moment that has since
/// passed. A failure no newer than the stored success is stale in that sense; on
/// an equal timestamp the success is kept, since `Failed` is the stronger claim
/// and a genuinely failing peer is marked again by its next attempt.
pub(super) fn apply_status_update(status: &mut SyncPeerStatus, update: &SyncStatusUpdate) -> bool {
    let attempts = status
        .successful_attempts
        .saturating_add(status.failed_attempts);
    let current = update.expected_attempts.is_none_or(|want| want == attempts)
        && !stale_outcome(status, update);
    let counted = update.successful_attempts > 0 || update.failed_attempts > 0;
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
pub(super) fn ack_reached_op(ack: &PeerAck, genesis: OpId, meta: &OpMeta) -> bool {
    ack.genesis == Some(genesis)
        && (ack.heads.contains(&meta.id) || ack.clock.get(&meta.actor_id) >= meta.actor_seq)
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
