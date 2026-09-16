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
    scheduler: ResyncScheduler,
    attempt: AttemptId,
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

impl ResyncScheduler {
    /// Registers `attempt` as live for every target of `keys`.
    pub(super) fn begin_attempts(
        &self,
        keys: impl IntoIterator<Item = ResyncTargetKey>,
        attempt: AttemptId,
    ) -> LiveAttempts {
        if let Ok(mut live) = self.live.lock() {
            live.extend(keys.into_iter().map(|key| (key, attempt)));
        }
        LiveAttempts {
            scheduler: self.clone(),
            attempt,
        }
    }

    /// Ends a live attempt, returning whether it was live: only its first
    /// completion is counted.
    pub(super) fn end_attempt(&self, key: ResyncTargetKey, attempt: AttemptId) -> bool {
        self.live
            .lock()
            .is_ok_and(|mut live| live.remove(&(key, attempt)))
    }

    pub(super) fn notifier(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.notify)
    }

    pub(super) fn schedule_now(&self, peer_id: PeerId, topic_id: crate::TopicId, force: bool) {
        self.schedule_after(peer_id, topic_id, Duration::ZERO, force);
    }

    /// Records new work for a target. A claimed target keeps its attempt and
    /// only gains the new revision, so its completion cannot erase this request.
    fn schedule_after(
        &self,
        peer_id: PeerId,
        topic_id: crate::TopicId,
        after: Duration,
        force: bool,
    ) {
        let key = ResyncTargetKey { peer_id, topic_id };
        let next_due = tokio::time::Instant::now() + after;
        let mut targets = self.inner.lock().expect("resync scheduler lock poisoned");
        match targets.get_mut(&key) {
            Some(target) => {
                target.requested = target.requested.saturating_add(1);
                if target.active.is_none()
                    && (target.failures == 0 || force)
                    && next_due < target.next_due
                {
                    target.next_due = next_due;
                }
                if force {
                    target.force = Some(target.requested);
                }
            }
            None => {
                targets.insert(
                    key,
                    ScheduledResync {
                        next_due,
                        failures: 0,
                        requested: 1,
                        active: None,
                        force: force.then_some(1),
                    },
                );
            }
        }
        drop(targets);
        self.notify.notify_one();
    }

    /// Re-arms a target after peer evidence changed what it needs. A claimed
    /// target is left to its owner, and the failure backoff is preserved.
    pub(super) fn reconsider(&self, peer_id: PeerId, topic_id: crate::TopicId) {
        let key = ResyncTargetKey { peer_id, topic_id };
        let now = tokio::time::Instant::now();
        let mut targets = self.inner.lock().expect("resync scheduler lock poisoned");
        match targets.get_mut(&key) {
            Some(target) => {
                if target.active.is_some() {
                    return;
                }
                if target.failures == 0 && now < target.next_due {
                    target.next_due = now;
                }
            }
            None => {
                targets.insert(
                    key,
                    ScheduledResync {
                        next_due: now,
                        failures: 0,
                        requested: 1,
                        active: None,
                        force: None,
                    },
                );
            }
        }
        drop(targets);
        self.notify.notify_one();
    }

    pub(super) fn due_targets(
        &self,
        max_peers: usize,
        max_targets: usize,
    ) -> Vec<(PeerId, Vec<ResyncTarget>)> {
        let now = tokio::time::Instant::now();
        let mut targets = self.inner.lock().expect("resync scheduler lock poisoned");
        let mut due: BTreeMap<PeerId, Vec<ResyncTargetKey>> = BTreeMap::new();
        let busy = busy_peers(&targets);
        let mut ready: Vec<_> = targets
            .iter()
            .filter(|(key, target)| {
                target.active.is_none() && target.next_due <= now && !busy.contains(&key.peer_id)
            })
            .map(|(key, target)| (target.next_due, *key))
            .collect();
        ready.sort_unstable();
        for (_, key) in ready {
            match due.get_mut(&key.peer_id) {
                Some(keys) => {
                    if keys.len() < max_targets {
                        keys.push(key);
                    }
                }
                None => {
                    if due.len() < max_peers {
                        due.insert(key.peer_id, vec![key]);
                    }
                }
            }
        }
        let mut out = Vec::with_capacity(due.len());
        let mut exhausted = false;
        for (peer_id, keys) in due {
            let mut batch = Vec::with_capacity(keys.len());
            for key in keys {
                let Some(target) = targets.get_mut(&key) else {
                    continue;
                };
                let Some(attempt) = next_attempt_id() else {
                    exhausted = true;
                    break;
                };
                target.active = Some(attempt);
                if let Ok(mut live) = self.live.lock() {
                    live.insert((key, attempt));
                }
                batch.push(ResyncTarget {
                    key,
                    attempt,
                    covered: target.requested,
                    force: target.force,
                });
            }
            if !batch.is_empty() {
                out.push((peer_id, batch));
            }
            if exhausted {
                break;
            }
        }
        if exhausted {
            tracing::error!("resync attempt ids are exhausted; refusing new claims");
        }
        out
    }

    /// The next wake deadline. A peer taking its turn is skipped so an expired
    /// deadline behind that turn cannot spin the loop.
    pub(super) fn next_due(&self) -> Option<tokio::time::Instant> {
        let targets = self.inner.lock().expect("resync scheduler lock poisoned");
        let busy = busy_peers(&targets);
        targets
            .iter()
            .filter(|(key, target)| target.active.is_none() && !busy.contains(&key.peer_id))
            .map(|(_, target)| target.next_due)
            .min()
    }

    /// Deletes the entry only when the finished attempt covered the newest
    /// requested work, so work that arrived mid-attempt survives.
    pub(super) fn complete_clean(&self, claim: ResyncTarget) {
        let mut targets = self.inner.lock().expect("resync scheduler lock poisoned");
        let Some(target) = targets.get_mut(&claim.key) else {
            return;
        };
        if target.active != Some(claim.attempt) {
            return;
        }
        target.active = None;
        if target.requested != claim.covered || target.force != claim.force {
            target.failures = 0;
            target.next_due = tokio::time::Instant::now();
            drop(targets);
            self.notify.notify_one();
            return;
        }
        targets.remove(&claim.key);
    }

    pub(super) fn complete_dirty(&self, claim: ResyncTarget, after: Duration) {
        let now = tokio::time::Instant::now();
        let mut targets = self.inner.lock().expect("resync scheduler lock poisoned");
        let Some(target) = targets.get_mut(&claim.key) else {
            return;
        };
        if target.active != Some(claim.attempt) {
            return;
        }
        target.active = None;
        target.failures = 0;
        target.next_due = if target.requested == claim.covered {
            now + after
        } else {
            now
        };
        // Only the force request this attempt covered is served; a newer one waits.
        if target.force == claim.force {
            target.force = None;
        }
        drop(targets);
        self.notify.notify_one();
    }

    /// Up to `limit` topics queued for `peer_id`.
    pub(super) fn peer_topics(&self, peer_id: PeerId, limit: usize) -> Vec<crate::TopicId> {
        let targets = self.inner.lock().expect("resync scheduler lock poisoned");
        let start = ResyncTargetKey {
            peer_id,
            topic_id: crate::TopicId::from_bytes([0; 32]),
        };
        targets
            .range(start..)
            .take_while(|(key, _)| key.peer_id == peer_id)
            .take(limit)
            .map(|(key, _)| key.topic_id)
            .collect()
    }

    pub(super) fn peer_reachable(&self, peer_id: PeerId) {
        let now = tokio::time::Instant::now();
        let mut targets = self.inner.lock().expect("resync scheduler lock poisoned");
        let mut changed = false;
        for (key, target) in targets.iter_mut() {
            if key.peer_id == peer_id && target.failures > 0 {
                target.failures = 0;
                if target.active.is_none() && target.next_due > now {
                    target.next_due = now;
                }
                changed = true;
            }
        }
        drop(targets);
        if changed {
            self.notify.notify_one();
        }
    }

    pub(super) fn complete_failed(
        &self,
        claim: ResyncTarget,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) {
        let mut targets = self.inner.lock().expect("resync scheduler lock poisoned");
        let Some(target) = targets.get_mut(&claim.key) else {
            return;
        };
        if target.active != Some(claim.attempt) {
            return;
        }
        target.failures = target.failures.saturating_add(1);
        let shift = target.failures.saturating_sub(1).min(20);
        let multiplier = 1_u32 << shift;
        let backoff = initial_backoff.saturating_mul(multiplier).min(max_backoff);
        target.next_due = tokio::time::Instant::now() + backoff;
        target.active = None;
        // A failed exchange retries even when local evidence reads clean.
        target.force = Some(target.requested);
        drop(targets);
        self.notify.notify_one();
    }

    /// Hands a claim back without judging the peer. Runs from `Drop`, so it
    /// must not panic, await or touch storage.
    fn release_claim(&self, claim: ResyncTarget, after: Duration) {
        self.end_attempt(claim.key, claim.attempt);
        let Ok(mut targets) = self.inner.lock() else {
            return;
        };
        let Some(target) = targets.get_mut(&claim.key) else {
            return;
        };
        if target.active != Some(claim.attempt) {
            return;
        }
        target.active = None;
        target.next_due = tokio::time::Instant::now() + after;
        drop(targets);
        self.notify.notify_one();
    }

    pub(super) fn lease(&self, claims: Vec<ResyncTarget>, retry_after: Duration) -> ResyncLease {
        ResyncLease {
            scheduler: self.clone(),
            retry_after,
            claims: claims.into_iter().map(|claim| (claim.key, claim)).collect(),
        }
    }

    /// Test-only view of a target's ownership, backoff and pending force.
    #[cfg(test)]
    pub(super) fn target_state(
        &self,
        peer_id: PeerId,
        topic_id: crate::TopicId,
    ) -> Option<(Option<AttemptId>, u32, Option<u64>)> {
        self.inner
            .lock()
            .expect("resync scheduler lock poisoned")
            .get(&ResyncTargetKey { peer_id, topic_id })
            .map(|target| (target.active, target.failures, target.force))
    }
}

/// Peers that already own a claimed target. A peer takes one turn at a time, so
/// the rest of its work waits for the next one.
fn busy_peers(targets: &BTreeMap<ResyncTargetKey, ScheduledResync>) -> BTreeSet<PeerId> {
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
    retry_after: Duration,
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

/// A lease claim stays owned until a terminal transition consumes it. Dropping
/// it first returns the claim, so panic or early return cannot strand a target.
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
