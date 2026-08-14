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
    pub heads: BTreeSet<OpId>,
    pub clock: ActorClock,
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
    /// Durably admit `batch`. Backends must write each entry's op record and
    /// its [`OpMeta`] in one atomic unit and must reject a batch whose entry
    /// depends on an op with no metadata, so a committed op can never reference
    /// a dependency the DAG cannot resolve.
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
    fn peer_ack(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<Option<PeerAck>>;
    fn peer_acks(&self, topic_id: &TopicId) -> Result<Vec<PeerAck>>;
    fn put_sync_obligation(&self, obligation: SyncObligation) -> Result<()>;
    fn all_sync_obligations(&self) -> Result<Vec<SyncObligation>>;
    /// Atomically persist `ack` and clear any obligations satisfied by it.
    /// Backends must perform both writes in one durable operation so a crash
    /// between them cannot leave the ack visible while obligations remain,
    /// or vice-versa. Returns the number of cleared obligations.
    fn apply_peer_ack(&self, ack: PeerAck) -> Result<usize>;
    /// Apply many peer acks in order, equivalent to calling
    /// [`Storage::apply_peer_ack`] per ack. Backends may batch all writes into
    /// one durable operation. Returns the total number of cleared obligations.
    fn apply_peer_acks(&self, acks: Vec<PeerAck>) -> Result<usize> {
        let mut cleared = 0;
        for ack in acks {
            cleared += self.apply_peer_ack(ack)?;
        }
        Ok(cleared)
    }
    fn sync_obligations(&self, peer_id: &PeerId, topic_id: &TopicId)
    -> Result<Vec<SyncObligation>>;
    fn has_sync_obligations(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<bool> {
        Ok(!self.sync_obligations(peer_id, topic_id)?.is_empty())
    }
    fn put_sync_status(&self, status: SyncPeerStatus) -> Result<()>;
    fn sync_statuses(&self, topic_id: &TopicId) -> Result<Vec<SyncPeerStatus>>;
    /// Drop obligations, sync status, and the stored ack for a peer that left
    /// a topic. Returns the number of cleared obligations.
    fn clear_peer_sync_state(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<usize>;

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

    /// Journalled evictions no consumer has acknowledged yet. A restart drains
    /// these before eviction recovery can be considered complete; each is the
    /// only remaining copy of the payloads its reset removed.
    fn pending_evictions(&self) -> Result<Vec<TopicEviction>>;

    /// Release the journalled eviction named by `key`. The consumer calls this
    /// only once it durably owns the payloads, so a crash before that point
    /// leaves the record for the next restart. Releasing an absent key is not
    /// an error: acknowledgement is idempotent.
    fn clear_eviction(&self, key: &EvictionKey) -> Result<()>;

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

mod memory;
pub use memory::MemoryStorage;

#[cfg(feature = "fjall")]
mod fjall;
#[cfg(feature = "fjall")]
pub use fjall::FjallStorage;

pub(crate) fn topic_fingerprint_for(
    heads: &BTreeSet<OpId>,
    clock: &ActorClock,
) -> Result<[u8; 32]> {
    Ok(*blake3::hash(&canonical_bytes(&(heads, clock))?).as_bytes())
}

pub(super) fn sync_obligation_satisfied(obligation: &SyncObligation, ack: &PeerAck) -> bool {
    if obligation.op_ids.is_subset(&ack.heads) {
        return true;
    }
    !obligation.target_clock.is_empty() && ack.clock.dominates(&obligation.target_clock)
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

pub(super) fn stored_ack_dominates(existing: &PeerAck, incoming: &PeerAck) -> bool {
    existing.peer_id == incoming.peer_id
        && existing.topic_id == incoming.topic_id
        && existing.clock.dominates(&incoming.clock)
}
