// SPDX-License-Identifier: MIT OR Apache-2.0
//! Branch evidence, obligations, and sync status records.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{ActorClock, OpId, PeerId, Result, TopicId};

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
    /// comes from [`super::Storage::next_attempt_epoch`], so identities are never
    /// reused across restarts.
    pub attempt: Option<(u64, u64)>,
}
