// SPDX-License-Identifier: MIT OR Apache-2.0
//! Resync scheduling: due targets with their work revisions, claims owned by
//! leases, live attempt registrations, and peer-fair turns.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::PeerId;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct ResyncTargetKey {
    pub(super) peer_id: PeerId,
    pub(super) topic_id: crate::TopicId,
}

/// Identifies one resync attempt for the lifetime of the process. Ids are never
/// reused, so a completion from a finished attempt cannot match a live one.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct AttemptId(pub(super) u64);

/// Hands out the next attempt id. Exhaustion permanently refuses new claims
/// instead of wrapping into an id a stale completion could match.
pub(super) fn next_attempt_id() -> Option<AttemptId> {
    static NEXT_ATTEMPT: AtomicU64 = AtomicU64::new(1);
    let mut current = NEXT_ATTEMPT.load(Ordering::Relaxed);
    loop {
        let next = current.checked_add(1)?;
        match NEXT_ATTEMPT.compare_exchange_weak(
            current,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Some(AttemptId(current)),
            Err(observed) => current = observed,
        }
    }
}

/// One claimed target: the attempt that owns it plus the requested work
/// revision and force request that attempt covers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ResyncTarget {
    pub(super) key: ResyncTargetKey,
    pub(super) attempt: AttemptId,
    pub(super) covered: u64,
    pub(super) force: Option<u64>,
}

#[derive(Debug)]
pub(super) struct ScheduledResync {
    pub(super) next_due: tokio::time::Instant,
    pub(super) failures: u32,
    /// Bumped by every work invalidation, including during an attempt.
    pub(super) requested: u64,
    pub(super) active: Option<AttemptId>,
    /// Requested revision of the newest force request still to be covered.
    pub(super) force: Option<u64>,
}

#[derive(Clone, Default)]
pub(super) struct ResyncScheduler {
    pub(super) inner: Arc<Mutex<BTreeMap<ResyncTargetKey, ScheduledResync>>>,
    pub(super) notify: Arc<tokio::sync::Notify>,
    /// Attempts started and not yet recorded or released. A completion counts
    /// only while its attempt is live, so a repeat counts nothing however old.
    pub(super) live: Arc<Mutex<BTreeSet<(ResyncTargetKey, AttemptId)>>>,
}

/// Live manual attempts one sync owns; dropping it ends those not recorded.
pub(super) struct LiveAttempts {
    pub(super) scheduler: ResyncScheduler,
    pub(super) attempt: AttemptId,
}

impl LiveAttempts {
    /// Ends the attempt of `key`, returning whether it was still live.
    pub(super) fn end(&self, key: ResyncTargetKey) -> bool {
        self.scheduler.end_attempt(key, self.attempt)
    }
}

impl Drop for LiveAttempts {
    fn drop(&mut self) {
        // Runs from `Drop`: a poisoned registry only loses bookkeeping.
        if let Ok(mut live) = self.scheduler.live.lock() {
            live.retain(|(_, attempt)| *attempt != self.attempt);
        }
    }
}

/// Peers that already own a claimed target. A peer takes one turn at a time, so
/// the rest of its work waits for the next one.
pub(super) fn busy_peers(targets: &BTreeMap<ResyncTargetKey, ScheduledResync>) -> BTreeSet<PeerId> {
    targets
        .iter()
        .filter(|(_, target)| target.active.is_some())
        .map(|(key, _)| key.peer_id)
        .collect()
}

/// The claims one batch task owns. Dropping the lease releases exactly the
/// claims it still holds, so a panicking, aborted or timed-out task cannot
/// leave a target in flight forever.
pub(super) struct ResyncLease {
    pub(super) scheduler: ResyncScheduler,
    pub(super) retry_after: Duration,
    pub(super) claims: BTreeMap<ResyncTargetKey, ResyncTarget>,
}

impl ResyncLease {
    pub(super) fn targets(&self) -> Vec<ResyncTarget> {
        self.claims.values().copied().collect()
    }

    /// Takes a claim out of the lease so its own result is recorded once.
    /// Ownership moves into a guard, so losing the guard hands the claim back
    /// instead of leaving the target owned by nobody.
    pub(super) fn take_claim(&mut self, key: &ResyncTargetKey) -> Option<ClaimGuard> {
        self.claims.remove(key).map(|claim| self.guard(claim))
    }

    /// The claims whose result was never recorded, for a timeout to consume.
    pub(super) fn drain_claims(&mut self) -> Vec<ClaimGuard> {
        std::mem::take(&mut self.claims)
            .into_values()
            .map(|claim| ClaimGuard {
                scheduler: self.scheduler.clone(),
                claim: Some(claim),
                retry_after: self.retry_after,
            })
            .collect()
    }

    fn guard(&self, claim: ResyncTarget) -> ClaimGuard {
        ClaimGuard {
            scheduler: self.scheduler.clone(),
            claim: Some(claim),
            retry_after: self.retry_after,
        }
    }
}

/// One claim taken out of a lease, owned until a terminal transition consumes
/// it. Dropping it first hands the claim back, so a panic or early return
/// between taking a claim and recording its result cannot strand the target in
/// flight forever.
pub(super) struct ClaimGuard {
    scheduler: ResyncScheduler,
    claim: Option<ResyncTarget>,
    retry_after: Duration,
}

impl ClaimGuard {
    pub(super) fn key(&self) -> ResyncTargetKey {
        self.expect_claim().key
    }

    /// Consumes the guard for a terminal transition. Call it immediately before
    /// the scheduler transition so nothing can fail in between.
    pub(super) fn settle(mut self) -> ResyncTarget {
        self.claim.take().expect("claim guard settled twice")
    }

    pub(super) fn expect_claim(&self) -> &ResyncTarget {
        self.claim.as_ref().expect("claim guard already settled")
    }
}

impl Drop for ClaimGuard {
    fn drop(&mut self) {
        if let Some(claim) = self.claim.take() {
            self.scheduler.release_claim(claim, self.retry_after);
        }
    }
}

impl Drop for ResyncLease {
    fn drop(&mut self) {
        for (_, claim) in std::mem::take(&mut self.claims) {
            self.scheduler.release_claim(claim, self.retry_after);
        }
    }
}
