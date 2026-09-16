// SPDX-License-Identifier: MIT OR Apache-2.0
//! Stored metadata, snapshots, and admission records.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::topic::ReplicationPolicy;
use crate::{ActorClock, ActorId, Op, OpId, PeerId, TopicId};
use super::{PeerAck, SyncObligation};

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
