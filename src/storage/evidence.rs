// SPDX-License-Identifier: MIT OR Apache-2.0
//! Branch evidence, obligations, and sync status records.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::{OpPosition, TopicState};
use crate::{ActorClock, OpId, PeerId, Result, TopicId};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerAck {
    pub peer_id: PeerId,
    pub topic_id: TopicId,
    /// Genesis this evidence certifies. Missing genesis is legacy data that
    /// remains stored but certifies nothing because branch replacement reuses
    /// actor sequence numbers.
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
    /// Attempt identities already counted, bounded by `MAX_RECENT_ATTEMPTS`.
    /// Repeats count nothing; older identities still count once when recorded
    /// by a live attempt.
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

pub(crate) fn ack_covers(
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
pub(crate) fn merged_obligation(
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
pub(crate) fn settled_obligation(
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
pub(crate) fn apply_status_update(status: &mut SyncPeerStatus, update: &SyncStatusUpdate) -> bool {
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
pub(crate) fn new_peer_status(peer_id: PeerId, topic_id: TopicId) -> SyncPeerStatus {
    SyncPeerStatus {
        peer_id,
        topic_id,
        ..SyncPeerStatus::default()
    }
}

/// Whether `ack` proves the peer holds the operation `meta` describes. Only
/// evidence certified against the topic's current genesis counts: a record from
/// a replaced branch names the same actor sequences without covering them.
pub(crate) fn ack_reached_op(
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
pub(crate) enum AckCommit {
    /// The record proves the current branch: store it and clear what it covers.
    Certify,
    /// The record is kept under the genesis it names but proves nothing here,
    /// because no local topic holds that branch yet.
    Retain,
}

/// Decide what `ack` may change, from `state` read in the transaction that writes it.
/// Without a topic the record is only retained; another branch or a removed peer is
/// refused.
pub(crate) fn ack_commit(state: Option<&TopicState>, ack: &PeerAck) -> Result<AckCommit> {
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

pub(crate) fn stored_ack_dominates(existing: &PeerAck, incoming: &PeerAck) -> bool {
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

/// Merge the stored clock into `incoming` when both certify the same branch. A
/// record of another branch is replaced, not merged.
pub(crate) fn merged_peer_ack(existing: &PeerAck, incoming: &PeerAck) -> PeerAck {
    let mut merged = incoming.clone();
    if same_incarnation(existing, incoming) {
        merged.clock.merge(&existing.clock);
    }
    merged
}
