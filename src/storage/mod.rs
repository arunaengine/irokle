// SPDX-License-Identifier: MIT OR Apache-2.0
//! Storage trait plus in-memory and Fjall-backed persistence implementations.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::crypto::canonical_bytes;
use crate::topic::ReplicationPolicy;
use crate::{ActorClock, ActorId, Op, OpId, PeerId, Result, TopicId, TopicInfo};

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
    fn put_admitted_batch(&self, batch: AdmittedBatch) -> Result<()>;
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
    fn has_sync_obligations(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<bool> {
        Ok(!self.sync_obligations(peer_id, topic_id)?.is_empty())
    }
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

pub(super) fn stored_ack_dominates(existing: &PeerAck, incoming: &PeerAck) -> bool {
    existing.peer_id == incoming.peer_id
        && existing.topic_id == incoming.topic_id
        && existing.clock.dominates(&incoming.clock)
}
