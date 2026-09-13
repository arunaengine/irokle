// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::sync::{SyncMessage, SyncSummary};
use crate::{Irokle, MemoryStorage, PeerId, ReceiveOutcome, Storage, TopicEviction};

use super::frame::{MAX_FRAME_LEN, MAX_SYNC_DATA_OPS_PER_MESSAGE};
use super::{
    _message_type_name, IROKLE_SYNC_ALPN, decode_sync_message, encode_frame, encode_sync_message,
    invalid_data,
};

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_SYNC_IO_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_RESYNC_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_RESYNC_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const DEFAULT_RESYNC_MAX_BACKOFF: Duration = Duration::from_secs(10 * 60);
const DEFAULT_FULL_SWEEP_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_FULL_SWEEP_TIME_OF_DAY: Duration = Duration::from_secs(3 * 60 * 60);
const SECONDS_PER_DAY: u64 = 24 * 60 * 60;
const EMPTY_RESYNC_SLEEP: Duration = Duration::from_secs(24 * 60 * 60 * 365);
const MAX_ACCEPT_CONNECTIONS: usize = 128;
const MAX_ACCEPT_CONNECTIONS_PER_PEER: usize = 4;
/// Handshakes the accept loop carries at once, before any identity is known.
/// Completing them one at a time let a single slow peer hold off every other
/// inbound connection for a whole connect timeout. A per-peer limit cannot
/// apply yet, so this global bound is what keeps pre-authentication work finite.
const MAX_PENDING_HANDSHAKES: usize = 32;
const MAX_RESYNC_PEER_CONCURRENCY: usize = 8;
const MAX_TOPICS_PER_RESYNC_BATCH: usize = 1024;
const MAX_SYNC_MESSAGES_PER_STREAM: usize = 4096;
// Keep batched streams at half the per-stream message cap so the responder's
// reply (which can echo up to two messages per topic) stays under its own cap.
const MAX_BATCH_STREAM_MESSAGES: usize = MAX_SYNC_MESSAGES_PER_STREAM / 2;
const MAX_SYNC_STREAM_BYTES: usize = 256 * 1024 * 1024;
/// Inbound frame bytes all served streams of a net may hold at once.
const MAX_INBOUND_FRAME_BYTES: usize = 256 * 1024 * 1024;
/// Frames up to this size draw on a separate pool of `CONTROL_INBOUND_BYTES`.
const CONTROL_FRAME_BYTES: usize = 64 * 1024;
const CONTROL_INBOUND_BYTES: usize = 16 * 1024 * 1024;
/// Delay before a topic that advanced but still owes work is served again. It
/// is due at once but behind every target that became due earlier, so other
/// work takes its turn first and an idle queue continues immediately.
const RESYNC_PROGRESS_TURN: Duration = Duration::ZERO;
/// Pages one `sync_now` call will fetch while each is really advancing, before
/// it reports what it reached. Bounds the caller's wait instead of paging on
/// until the peer stops publishing.
const MAX_SYNC_NOW_PAGES: usize = 64;
/// Staging receipts remembered per peer and topic, oldest dropped first.
const MAX_BOOTSTRAP_RECEIPTS: usize = 1024;
/// Storage jobs running at once for acks, fingerprints, summaries and status.
const CONTROL_JOBS: usize = 4;
/// Storage jobs running at once for admission and page planning.
const BULK_JOBS: usize = 2;
const NO_PROGRESS: &str = "sync exchange made no progress";

/// Bounds of one sync stream. Tests scale them down; everything else uses the
/// defaults.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StreamLimits {
    pub(crate) bytes: usize,
    pub(crate) messages: usize,
    /// Messages one batched request stream may carry.
    pub(crate) batch_messages: usize,
    /// Inbound data frame bytes all served streams may hold at once.
    pub(crate) inbound_bytes: usize,
}

impl Default for StreamLimits {
    fn default() -> Self {
        Self {
            bytes: MAX_SYNC_STREAM_BYTES,
            messages: MAX_SYNC_MESSAGES_PER_STREAM,
            batch_messages: MAX_BATCH_STREAM_MESSAGES,
            inbound_bytes: MAX_INBOUND_FRAME_BYTES,
        }
    }
}

/// Node-wide reservations for inbound frames, taken before a frame is
/// allocated and held until its message was handled. Small frames use their
/// own pool, so control messages still flow while data frames fill theirs.
struct InboundBudget {
    data: Arc<tokio::sync::Semaphore>,
    control: Arc<tokio::sync::Semaphore>,
    /// Most data frame bytes reserved at once, and the data pool size.
    #[cfg(test)]
    peak: (std::sync::atomic::AtomicUsize, usize),
}

impl InboundBudget {
    fn new(limits: StreamLimits) -> Self {
        Self {
            data: Arc::new(tokio::sync::Semaphore::new(
                limits.inbound_bytes.max(MAX_FRAME_LEN),
            )),
            control: Arc::new(tokio::sync::Semaphore::new(CONTROL_INBOUND_BYTES)),
            #[cfg(test)]
            peak: (Default::default(), limits.inbound_bytes.max(MAX_FRAME_LEN)),
        }
    }

    async fn reserve(&self, len: usize) -> io::Result<tokio::sync::OwnedSemaphorePermit> {
        let pool = if len <= CONTROL_FRAME_BYTES {
            &self.control
        } else {
            &self.data
        };
        let bytes = u32::try_from(len).map_err(|_| invalid_data("sync frame length overflow"))?;
        let permit = Arc::clone(pool)
            .acquire_many_owned(bytes)
            .await
            .map_err(|_| io::Error::other("inbound frame budget closed"))?;
        #[cfg(test)]
        if len > CONTROL_FRAME_BYTES {
            let in_use = self.peak.1 - self.data.available_permits();
            self.peak.0.fetch_max(in_use, Ordering::Relaxed);
        }
        Ok(permit)
    }
}

/// Result of [`IrohNet::shutdown_with_timeout`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownOutcome {
    /// Every task the net spawned has ended.
    Complete,
    /// Tasks were still running at the timeout; the net keeps owning them.
    Incomplete { running: usize },
}

/// Owns the tasks a net runs. Root work registers and shutdown seals under one
/// lock, so once shutdown has closed registration and seen zero tasks, no work
/// a caller starts later can run.
#[derive(Default)]
struct TaskTracker {
    state: Mutex<TrackerState>,
    idle: tokio::sync::Notify,
}

#[derive(Default)]
struct TrackerState {
    running: usize,
    closed: bool,
}

impl TaskTracker {
    fn state(&self) -> std::sync::MutexGuard<'_, TrackerState> {
        // The state is two plain fields updated atomically, so a poisoned lock is still consistent.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Registers work started by a caller outside the net. Refused once
    /// shutdown has begun.
    fn enter(self: &Arc<Self>) -> io::Result<TaskGuard> {
        let mut state = self.state();
        if state.closed {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "irokle net is shut down",
            ));
        }
        state.running += 1;
        Ok(TaskGuard(Arc::clone(self)))
    }

    /// Registers work an already registered task starts, which may still run
    /// while shutdown drains it.
    fn track(self: &Arc<Self>) -> TaskGuard {
        self.state().running += 1;
        TaskGuard(Arc::clone(self))
    }

    fn close(&self) {
        self.state().closed = true;
    }

    fn is_closed(&self) -> bool {
        self.state().closed
    }

    fn running(&self) -> usize {
        self.state().running
    }

    async fn wait_idle(&self) {
        loop {
            let idle = self.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if self.running() == 0 {
                return;
            }
            idle.await;
        }
    }
}

/// Owned by a spawned task future, so the count drops when the task really ends.
struct TaskGuard(Arc<TaskTracker>);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        let mut state = self.0.state();
        state.running -= 1;
        if state.running == 0 {
            drop(state);
            self.0.idle.notify_waiters();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IrohRuntimeConfig {
    pub connect_timeout: Duration,
    pub sync_io_timeout: Duration,
    pub resync_interval: Duration,
    pub resync_initial_backoff: Duration,
    pub resync_max_backoff: Duration,
    pub full_sweep_interval: Duration,
    pub full_sweep_time_of_day: Duration,
}

impl Default for IrohRuntimeConfig {
    fn default() -> Self {
        Self {
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            sync_io_timeout: DEFAULT_SYNC_IO_TIMEOUT,
            resync_interval: DEFAULT_RESYNC_INTERVAL,
            resync_initial_backoff: DEFAULT_RESYNC_INITIAL_BACKOFF,
            resync_max_backoff: DEFAULT_RESYNC_MAX_BACKOFF,
            full_sweep_interval: DEFAULT_FULL_SWEEP_INTERVAL,
            full_sweep_time_of_day: DEFAULT_FULL_SWEEP_TIME_OF_DAY,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ResyncTargetKey {
    peer_id: PeerId,
    topic_id: crate::TopicId,
}

/// Identifies one resync attempt for the lifetime of the process. Ids are never
/// reused, so a completion from a finished attempt cannot match a live one.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct AttemptId(u64);

/// Hands out the next attempt id. Exhaustion permanently refuses new claims
/// instead of wrapping into an id a stale completion could match.
fn next_attempt_id() -> Option<AttemptId> {
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
struct ResyncTarget {
    key: ResyncTargetKey,
    attempt: AttemptId,
    covered: u64,
    force: Option<u64>,
}

#[derive(Debug)]
struct ScheduledResync {
    next_due: tokio::time::Instant,
    failures: u32,
    /// Bumped by every work invalidation, including during an attempt.
    requested: u64,
    active: Option<AttemptId>,
    /// Requested revision of the newest force request still to be covered.
    force: Option<u64>,
}

#[derive(Clone, Default)]
struct ResyncScheduler {
    inner: Arc<Mutex<BTreeMap<ResyncTargetKey, ScheduledResync>>>,
    notify: Arc<tokio::sync::Notify>,
    /// Attempts started and not yet recorded or released. A completion counts
    /// only while its attempt is live, so a repeat counts nothing however old.
    live: Arc<Mutex<BTreeSet<(ResyncTargetKey, AttemptId)>>>,
}

/// Live manual attempts one sync owns; dropping it ends those not recorded.
struct LiveAttempts {
    scheduler: ResyncScheduler,
    attempt: AttemptId,
}

impl LiveAttempts {
    /// Ends the attempt of `key`, returning whether it was still live.
    fn end(&self, key: ResyncTargetKey) -> bool {
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
    fn begin_attempts(
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
    fn end_attempt(&self, key: ResyncTargetKey, attempt: AttemptId) -> bool {
        self.live
            .lock()
            .is_ok_and(|mut live| live.remove(&(key, attempt)))
    }

    fn notifier(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.notify)
    }

    fn schedule_now(&self, peer_id: PeerId, topic_id: crate::TopicId, force: bool) {
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
    fn reconsider(&self, peer_id: PeerId, topic_id: crate::TopicId) {
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

    fn due_targets_by_peer(
        &self,
        max_peers: usize,
        max_targets_per_peer: usize,
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
                    if keys.len() < max_targets_per_peer {
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
    fn next_due(&self) -> Option<tokio::time::Instant> {
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
    fn complete_clean(&self, claim: ResyncTarget) {
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

    fn complete_dirty(&self, claim: ResyncTarget, after: Duration) {
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
    fn peer_topics(&self, peer_id: PeerId, limit: usize) -> Vec<crate::TopicId> {
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

    fn peer_reachable(&self, peer_id: PeerId) {
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

    fn complete_failed(
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

    fn lease(&self, claims: Vec<ResyncTarget>, retry_after: Duration) -> ResyncLease {
        ResyncLease {
            scheduler: self.clone(),
            retry_after,
            claims: claims.into_iter().map(|claim| (claim.key, claim)).collect(),
        }
    }

    /// Test-only view of a target's ownership, backoff and pending force.
    #[cfg(test)]
    fn target_state(
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
struct ResyncLease {
    scheduler: ResyncScheduler,
    retry_after: Duration,
    claims: BTreeMap<ResyncTargetKey, ResyncTarget>,
}

impl ResyncLease {
    fn targets(&self) -> Vec<ResyncTarget> {
        self.claims.values().copied().collect()
    }

    /// Takes a claim out of the lease so its own result is recorded once.
    /// Ownership moves into a guard, so losing the guard hands the claim back
    /// instead of leaving the target owned by nobody.
    fn take_claim(&mut self, key: &ResyncTargetKey) -> Option<ClaimGuard> {
        self.claims.remove(key).map(|claim| self.guard(claim))
    }

    /// The claims whose result was never recorded, for a timeout to consume.
    fn drain_claims(&mut self) -> Vec<ClaimGuard> {
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
struct ClaimGuard {
    scheduler: ResyncScheduler,
    claim: Option<ResyncTarget>,
    retry_after: Duration,
}

impl ClaimGuard {
    fn key(&self) -> ResyncTargetKey {
        self.expect_claim().key
    }

    /// Consumes the guard for a terminal transition. Call it immediately before
    /// the scheduler transition so nothing can fail in between.
    fn settle(mut self) -> ResyncTarget {
        self.claim.take().expect("claim guard settled twice")
    }

    fn expect_claim(&self) -> &ResyncTarget {
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

#[derive(Clone)]
struct ConnectionPool {
    endpoint: iroh::Endpoint,
    connections: Arc<RwLock<HashMap<iroh::EndpointId, iroh::endpoint::Connection>>>,
    dialing: Arc<Mutex<HashMap<iroh::EndpointId, Weak<tokio::sync::Mutex<()>>>>>,
}

impl ConnectionPool {
    fn new(endpoint: iroh::Endpoint) -> Self {
        Self {
            endpoint,
            connections: Arc::new(RwLock::new(HashMap::new())),
            dialing: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn endpoint(&self) -> &iroh::Endpoint {
        &self.endpoint
    }

    fn insert(&self, connection: iroh::endpoint::Connection) -> io::Result<iroh::EndpointId> {
        let peer = connection.remote_id();
        self.connections
            .write()
            .map_err(|_| io::Error::other("connection pool write lock poisoned"))?
            .insert(peer, connection);
        Ok(peer)
    }

    fn remove(&self, connection: &iroh::endpoint::Connection) -> io::Result<()> {
        let mut connections = self
            .connections
            .write()
            .map_err(|_| io::Error::other("connection pool write lock poisoned"))?;
        let peer = connection.remote_id();
        if connections
            .get(&peer)
            .is_some_and(|pooled| pooled.stable_id() == connection.stable_id())
        {
            connections.remove(&peer);
        }
        Ok(())
    }

    fn get(&self, peer: &iroh::EndpointId) -> io::Result<Option<iroh::endpoint::Connection>> {
        let mut connections = self
            .connections
            .write()
            .map_err(|_| io::Error::other("connection pool write lock poisoned"))?;
        match connections.get(peer) {
            Some(connection) if connection.close_reason().is_none() => Ok(Some(connection.clone())),
            Some(_) => {
                connections.remove(peer);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    async fn get_or_connect(
        &self,
        peer: iroh::EndpointAddr,
        connect_timeout: Duration,
    ) -> io::Result<iroh::endpoint::Connection> {
        if let Some(connection) = self.get(&peer.id)? {
            return Ok(connection);
        }
        let dialing = {
            let mut pending = self
                .dialing
                .lock()
                .map_err(|_| io::Error::other("connection dial lock poisoned"))?;
            pending.retain(|_, lock| lock.strong_count() > 0);
            match pending.get(&peer.id).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(tokio::sync::Mutex::new(()));
                    pending.insert(peer.id, Arc::downgrade(&lock));
                    lock
                }
            }
        };
        let _dialing = tokio::time::timeout(connect_timeout, dialing.lock())
            .await
            .map_err(|_| timed_out("connection pool wait timed out"))?;
        if let Some(connection) = self.get(&peer.id)? {
            return Ok(connection);
        }
        let connection = tokio::time::timeout(
            connect_timeout,
            self.endpoint.connect(peer, IROKLE_SYNC_ALPN),
        )
        .await
        .map_err(|_| timed_out("iroh connect timed out"))?
        .map_err(other)?;
        self.insert(connection.clone())?;
        Ok(connection)
    }
}

/// The newest staging receipt each peer returned for a topic it does not hold,
/// kept in memory only to measure staged progress within an attempt. Plans
/// continue from the staged state the peer's own summary names.
#[derive(Default)]
struct ReceiptLog {
    clocks: BTreeMap<(PeerId, crate::TopicId), crate::sync::SyncReceipt>,
    order: std::collections::VecDeque<(PeerId, crate::TopicId)>,
}

impl ReceiptLog {
    fn record(&mut self, peer_id: PeerId, receipt: crate::sync::SyncReceipt) {
        let key = (peer_id, receipt.topic_id);
        if self.clocks.insert(key, receipt).is_none() {
            self.order.push_back(key);
            if self.order.len() > MAX_BOOTSTRAP_RECEIPTS
                && let Some(oldest) = self.order.pop_front()
            {
                self.clocks.remove(&oldest);
            }
        }
    }

    fn clear(&mut self, key: &(PeerId, crate::TopicId)) {
        if self.clocks.remove(key).is_some() {
            self.order.retain(|kept| kept != key);
        }
    }
}

pub struct IrohNet<S: Storage = MemoryStorage> {
    pool: ConnectionPool,
    accept_started: AtomicBool,
    resync_started: AtomicBool,
    quarantine_started: AtomicBool,
    outbound_streams: AtomicU64,
    shared: Arc<SharedNet<S>>,
    #[cfg(test)]
    accept_hooks: AcceptHooks,
}

/// Test-only accept loop knobs: a lower connection cap, and a semaphore every
/// handshake waits on before it runs, so tests can hold handshakes pending.
#[cfg(test)]
#[derive(Default)]
struct AcceptHooks {
    connections: Option<usize>,
    handshakes: Option<Arc<tokio::sync::Semaphore>>,
}

/// The part of a net that storage jobs use off the async executor.
pub struct SharedNet<S: Storage> {
    node: Irokle<S>,
    runtime: IrohRuntimeConfig,
    resync_scheduler: ResyncScheduler,
    limits: StreamLimits,
    inbound: InboundBudget,
    /// Outbound peer attempts, automatic batches and manual syncs alike.
    outbound: Arc<tokio::sync::Semaphore>,
    receipts: Mutex<ReceiptLog>,
    shutdown: tokio::sync::watch::Sender<bool>,
    tasks: Arc<TaskTracker>,
    control_lane: Arc<tokio::sync::Semaphore>,
    bulk_lane: Arc<tokio::sync::Semaphore>,
    /// Durable epoch of this net's start; attempts are `(epoch, sequence)`.
    attempt_epoch: u64,
    // Optional sink for genesis tie-break evictions produced while admitting
    // remote sync data. The embedder consumes these to re-emit the discarded
    // payloads under the winning genesis; when unset they are recovered from
    // the eviction journal instead.
    eviction_sink: Option<tokio::sync::mpsc::UnboundedSender<TopicEviction>>,
    /// Most framed bytes of planned topic messages held at once.
    #[cfg(test)]
    planned_peak: std::sync::atomic::AtomicUsize,
}

impl<S: Storage> std::ops::Deref for IrohNet<S> {
    type Target = SharedNet<S>;

    fn deref(&self) -> &SharedNet<S> {
        &self.shared
    }
}

/// Which bounded worker lane a storage job runs in. Control work has its own
/// permits, so acks and status never wait behind admission or page planning.
#[derive(Clone, Copy, Debug)]
enum Lane {
    Control,
    Bulk,
}

impl<S: Storage> IrohNet<S> {
    pub fn new(endpoint: iroh::Endpoint, node: Irokle<S>) -> io::Result<Self> {
        Self::new_with_alpns(endpoint, node, Vec::new())
    }

    pub fn new_with_config(
        endpoint: iroh::Endpoint,
        node: Irokle<S>,
        runtime: IrohRuntimeConfig,
    ) -> io::Result<Self> {
        Self::new_with_alpns_and_config(endpoint, node, Vec::new(), runtime)
    }

    pub fn new_with_alpns(
        endpoint: iroh::Endpoint,
        node: Irokle<S>,
        alpns: Vec<Vec<u8>>,
    ) -> io::Result<Self> {
        Self::new_with_alpns_and_config(endpoint, node, alpns, IrohRuntimeConfig::default())
    }

    pub fn new_with_alpns_and_config(
        endpoint: iroh::Endpoint,
        node: Irokle<S>,
        alpns: Vec<Vec<u8>>,
        runtime: IrohRuntimeConfig,
    ) -> io::Result<Self> {
        Self::new_with_alpns_config_and_sink(endpoint, node, alpns, runtime, None)
    }

    /// Like [`Self::new_with_alpns_and_config`], but also wires an optional
    /// eviction sink. When set, every [`TopicEviction`] produced while admitting
    /// remote sync data (genesis tie-break resolution) is forwarded to the sink
    /// so the embedder can re-emit the discarded payloads under the winning
    /// genesis. The sink only makes recovery prompt: with or without it, the
    /// payloads are journalled and drained through [`Irokle::pending_evictions`].
    pub fn new_with_alpns_config_and_sink(
        endpoint: iroh::Endpoint,
        node: Irokle<S>,
        alpns: Vec<Vec<u8>>,
        runtime: IrohRuntimeConfig,
        eviction_sink: Option<tokio::sync::mpsc::UnboundedSender<TopicEviction>>,
    ) -> io::Result<Self> {
        let endpoint_peer = peer_id_from_endpoint_id(endpoint.id());
        if endpoint_peer != node.peer_id() {
            return Err(invalid_data("iroh endpoint id does not match node signer"));
        }
        let alpns = extend_alpns(alpns);
        if !alpns.is_empty() {
            endpoint.set_alpns(alpns);
        }
        let attempt_epoch = node.storage().next_attempt_epoch().map_err(invalid_data)?;
        let (shutdown, _) = tokio::sync::watch::channel(false);
        Ok(Self {
            pool: ConnectionPool::new(endpoint),
            accept_started: AtomicBool::new(false),
            resync_started: AtomicBool::new(false),
            quarantine_started: AtomicBool::new(false),
            outbound_streams: AtomicU64::new(0),
            #[cfg(test)]
            accept_hooks: AcceptHooks::default(),
            shared: Arc::new(SharedNet {
                node,
                runtime,
                resync_scheduler: ResyncScheduler::default(),
                limits: StreamLimits::default(),
                inbound: InboundBudget::new(StreamLimits::default()),
                outbound: Arc::new(tokio::sync::Semaphore::new(MAX_RESYNC_PEER_CONCURRENCY)),
                receipts: Mutex::default(),
                shutdown,
                tasks: Arc::default(),
                control_lane: Arc::new(tokio::sync::Semaphore::new(CONTROL_JOBS)),
                bulk_lane: Arc::new(tokio::sync::Semaphore::new(BULK_JOBS)),
                attempt_epoch,
                eviction_sink,
                #[cfg(test)]
                planned_peak: Default::default(),
            }),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_stream_limits(mut self, limits: StreamLimits) -> Self {
        let shared =
            Arc::get_mut(&mut self.shared).expect("limits are set before the net is shared");
        shared.limits = limits;
        shared.inbound = InboundBudget::new(limits);
        self
    }

    /// Run storage work on a blocking thread in `lane`. The permit and the
    /// task guard move into the job, so both are held until the job really
    /// ends, even when the awaiting caller is cancelled first.
    async fn run_job<T, F>(&self, lane: Lane, job: F) -> io::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&SharedNet<S>) -> T + Send + 'static,
    {
        let permits = match lane {
            Lane::Control => &self.control_lane,
            Lane::Bulk => &self.bulk_lane,
        };
        let permit = Arc::clone(permits)
            .acquire_owned()
            .await
            .map_err(|_| io::Error::other("storage job lane closed"))?;
        let task = self.tasks.track();
        let shared = Arc::clone(&self.shared);
        // Locals drop in reverse order: the permit is back before the task
        // stops counting, so a completed shutdown never sees a held permit.
        tokio::task::spawn_blocking(move || {
            let _task = task;
            let _permit = permit;
            job(&shared)
        })
        .await
        .map_err(|error| io::Error::other(format!("storage job failed: {error}")))
    }
}

impl<S: Storage> SharedNet<S> {
    /// Forwards evictions to the configured sink as the fast path. The sink is
    /// an optimization, not the handoff: every payload is already journalled by
    /// the transaction that discarded it, so an undelivered eviction stays
    /// recoverable through [`Irokle::pending_evictions`].
    fn forward_evictions(&self, evictions: Vec<TopicEviction>) {
        if evictions.is_empty() {
            return;
        }
        for eviction in evictions {
            let undelivered = match &self.eviction_sink {
                Some(sink) => sink.send(eviction.clone()).is_err(),
                None => true,
            };
            if undelivered {
                tracing::warn!(
                    topic_id = %eviction.topic_id,
                    key = %eviction.key(),
                    evicted = eviction.evicted.len(),
                    "eviction not delivered to a sink; recover it from the journal"
                );
            }
        }
    }
}

impl<S: Storage> IrohNet<S> {
    pub fn node(&self) -> &Irokle<S> {
        &self.node
    }

    pub fn endpoint(&self) -> &iroh::Endpoint {
        self.pool.endpoint()
    }

    pub fn runtime_config(&self) -> IrohRuntimeConfig {
        self.runtime
    }

    /// Number of outbound sync streams opened so far. One stream is one
    /// request/response round trip with a peer.
    pub fn outbound_sync_streams(&self) -> u64 {
        self.outbound_streams.load(Ordering::Relaxed)
    }

    pub async fn shutdown(&self) {
        // `send` reports failure and leaves the stored value alone when no
        // receiver exists, which loses the intent entirely if shutdown runs
        // before any loop subscribes. `send_replace` always stores it, so a
        // loop started afterwards still sees the terminal state.
        // Sealed first: no caller can register work the drain below would miss.
        self.tasks.close();
        self.shutdown.send_replace(true);
        self.endpoint().close().await;
        // Returns only once the loops and every task they spawned have ended.
        // Awaiting this from inside such a task would wait for itself.
        self.tasks.wait_idle().await;
    }

    /// Like [`Self::shutdown`], but gives up waiting after `timeout` and
    /// reports how many owned tasks are still running.
    pub async fn shutdown_with_timeout(&self, timeout: Duration) -> ShutdownOutcome {
        match tokio::time::timeout(timeout, self.shutdown()).await {
            Ok(()) => ShutdownOutcome::Complete,
            Err(_) => ShutdownOutcome::Incomplete {
                running: self.tasks.running(),
            },
        }
    }

    fn is_shutdown(&self) -> bool {
        self.tasks.is_closed()
    }

    pub async fn sync_peer_now(&self, peer_id: PeerId, topic_id: crate::TopicId) -> io::Result<()> {
        self.sync_now(peer_id_to_endpoint_addr(peer_id)?, topic_id)
            .await
    }
}

impl<S: Storage> SharedNet<S> {
    pub fn schedule_resync(&self, peer_id: PeerId, topic_id: crate::TopicId) {
        self.resync_scheduler.schedule_now(peer_id, topic_id, false);
    }

    pub fn note_peer_reachable(&self, peer_id: PeerId) {
        if self.tasks.is_closed() {
            return;
        }
        self.resync_scheduler.peer_reachable(peer_id);
        self.note_outcome(peer_id, [Ok(())]);
    }

    /// Feed one attempt's outcome to peer health. When that changes which
    /// peers are selected, the topics queued for the peer are rechecked now, so
    /// an alternate gets real work without a new publish or a full sweep.
    fn note_outcome<'a>(
        &self,
        peer_id: PeerId,
        results: impl IntoIterator<Item = std::result::Result<(), &'a io::Error>>,
    ) {
        if !self.node.note_peer_outcome(peer_id, results) {
            return;
        }
        for topic_id in self
            .resync_scheduler
            .peer_topics(peer_id, MAX_TOPICS_PER_RESYNC_BATCH)
        {
            if let Err(error) = self.schedule_topic_recheck(topic_id) {
                tracing::warn!(%peer_id, %topic_id, %error, "failed to recheck topic after a health change");
            }
        }
    }

    /// Marks the peer on an externally accepted connection as reachable.
    /// Outbound sync dials separately because reverse stream support is not guaranteed.
    pub fn register_connection(&self, connection: iroh::endpoint::Connection) -> io::Result<()> {
        let _task = self.tasks.enter()?;
        self.note_peer_reachable(peer_id_from_endpoint_id(connection.remote_id()));
        Ok(())
    }

    pub fn schedule_topic_recheck(&self, topic_id: crate::TopicId) -> io::Result<usize> {
        let mut scheduled = 0;
        for peer_id in self.dirty_selected_targets(topic_id)? {
            self.schedule_resync(peer_id, topic_id);
            scheduled += 1;
        }
        Ok(scheduled)
    }
}

impl<S: Storage> IrohNet<S> {
    pub async fn sync_endpoint_now(
        &self,
        endpoint_id: iroh::EndpointId,
        topic_id: crate::TopicId,
    ) -> io::Result<()> {
        self.sync_now(iroh::EndpointAddr::from(endpoint_id), topic_id)
            .await
    }

    pub fn start_accept_loop(self: &Arc<Self>) -> io::Result<()> {
        let _ = self.spawn_accept_loop()?;
        Ok(())
    }

    pub fn spawn_accept_loop(self: &Arc<Self>) -> io::Result<Option<tokio::task::JoinHandle<()>>> {
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::other("iroh auto accept requires a Tokio runtime"))?;
        let task = self.tasks.enter()?;
        if self.accept_started.swap(true, Ordering::SeqCst) {
            return Ok(None);
        }
        let net = Arc::downgrade(self);
        // Created before spawning, so aborting a loop that never ran still
        // clears the latch.
        let running = LoopGuard {
            net: Weak::clone(&net),
            latch: |net| &net.accept_started,
        };
        let tracker = Arc::clone(&self.tasks);
        #[cfg(not(test))]
        let max_connections = MAX_ACCEPT_CONNECTIONS;
        #[cfg(test)]
        let max_connections = self
            .accept_hooks
            .connections
            .unwrap_or(MAX_ACCEPT_CONNECTIONS);
        #[cfg(test)]
        let hold = self.accept_hooks.handshakes.clone();
        let endpoint = self.endpoint().clone();
        let mut shutdown = self.shutdown.subscribe();
        Ok(Some(handle.spawn(async move {
            let _task = task;
            let _running = running;
            let mut connections = tokio::task::JoinSet::new();
            let mut handshakes =
                tokio::task::JoinSet::<io::Result<iroh::endpoint::Connection>>::new();
            let mut peer_connections = HashMap::<iroh::EndpointId, usize>::new();
            let mut connection_tasks = HashMap::<tokio::task::Id, iroh::EndpointId>::new();
            'accept: loop {
                while connections.len() >= max_connections {
                    tokio::select! {
                        Some(result) = connections.join_next_with_id() => {
                            let task_id = match &result {
                                Ok((task_id, ())) => *task_id,
                                Err(error) => error.id(),
                            };
                            if let Some(peer) = connection_tasks.remove(&task_id)
                                && let Some(count) = peer_connections.get_mut(&peer)
                            {
                                *count = count.saturating_sub(1);
                                if *count == 0 {
                                    peer_connections.remove(&peer);
                                }
                            }
                            if let Err(error) = result {
                                tracing::warn!(%error, "iroh connection task failed");
                            }
                        }
                        changed = shutdown.changed() => {
                            // Pending handshakes are reaped with the connections below.
                            if changed.is_err() || *shutdown.borrow() {
                                break 'accept;
                            }
                        }
                    }
                }
                let Some(current) = net.upgrade() else {
                    break;
                };
                if current.is_shutdown() || endpoint.is_closed() {
                    break;
                }
                drop(current);

                let incoming = tokio::select! {
                    Some(result) = handshakes.join_next(), if !handshakes.is_empty() => {
                        match result {
                            Ok(Ok(connection)) => {
                                if connection.alpn() != IROKLE_SYNC_ALPN {
                                    connection.close(0u32.into(), b"unsupported protocol");
                                    continue;
                                }
                                // Identity is known only now, so the per-peer
                                // limit is applied here rather than on accept.
                                let peer = connection.remote_id();
                                let peer_count = peer_connections.entry(peer).or_default();
                                if *peer_count >= MAX_ACCEPT_CONNECTIONS_PER_PEER {
                                    tracing::warn!(
                                        %peer,
                                        "rejecting excess inbound iroh connection"
                                    );
                                    continue;
                                }
                                *peer_count += 1;
                                let connection_net = Weak::clone(&net);
                                let connection_shutdown = shutdown.clone();
                                let owned = tracker.track();
                                let task = connections.spawn(async move {
                                    let _task = owned;
                                    handle_connection(
                                        connection_net,
                                        connection_shutdown,
                                        peer,
                                        connection,
                                    )
                                    .await
                                });
                                connection_tasks.insert(task.id(), peer);
                            }
                            Ok(Err(error)) => {
                                tracing::warn!(%error, "failed to accept iroh connection");
                            }
                            Err(error) => tracing::warn!(%error, "iroh handshake task failed"),
                        }
                        continue;
                    }
                    Some(result) = connections.join_next_with_id(), if !connections.is_empty() => {
                        let task_id = match &result {
                            Ok((task_id, ())) => *task_id,
                            Err(error) => error.id(),
                        };
                        if let Some(peer) = connection_tasks.remove(&task_id)
                            && let Some(count) = peer_connections.get_mut(&peer)
                        {
                            *count = count.saturating_sub(1);
                            if *count == 0 {
                                peer_connections.remove(&peer);
                            }
                        }
                        if let Err(error) = result {
                            tracing::warn!(%error, "iroh connection task failed");
                        }
                        continue;
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                        continue;
                    }
                    incoming = endpoint.accept() => incoming,
                };
                let Some(incoming) = incoming else {
                    break;
                };
                let Some(current) = net.upgrade() else {
                    break;
                };
                if current.is_shutdown() {
                    break;
                }
                let connect_timeout = current.runtime.connect_timeout;
                drop(current);
                if handshakes.len() >= MAX_PENDING_HANDSHAKES {
                    tracing::warn!("refusing inbound iroh connection: handshakes are saturated");
                    continue;
                }
                let task = tracker.track();
                #[cfg(test)]
                let hold = hold.clone();
                handshakes.spawn(async move {
                    let _task = task;
                    #[cfg(test)]
                    if let Some(hold) = hold {
                        let _ = hold.acquire_owned().await.map(|permit| permit.forget());
                    }
                    tokio::time::timeout(connect_timeout, incoming)
                        .await
                        .map_err(|_| timed_out("iroh accept timed out"))
                        .and_then(|accepted| accepted.map_err(other))
                });
            }
            connections.abort_all();
            handshakes.abort_all();
            while connections.join_next().await.is_some() {}
            while handshakes.join_next().await.is_some() {}
        })))
    }

    pub fn start_resync_loop(self: &Arc<Self>, interval: Duration) -> io::Result<()> {
        let _ = self.spawn_resync_loop(interval)?;
        Ok(())
    }

    pub fn start_configured_resync_loop(self: &Arc<Self>) -> io::Result<()> {
        self.start_resync_loop(self.runtime.resync_interval)
    }

    pub fn spawn_resync_loop(
        self: &Arc<Self>,
        interval: Duration,
    ) -> io::Result<Option<tokio::task::JoinHandle<()>>> {
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::other("iroh resync requires a Tokio runtime"))?;
        let task = self.tasks.enter()?;
        if self.resync_started.swap(true, Ordering::SeqCst) {
            return Ok(None);
        }
        let net = Arc::downgrade(self);
        let notify = self.resync_scheduler.notifier();
        let runtime = IrohRuntimeConfig {
            resync_interval: interval,
            ..self.runtime
        };
        let mut shutdown = self.shutdown.subscribe();
        // Captured before the first poll so that aborting a loop that never ran
        // still clears the latch.
        let running = LoopGuard {
            net: Weak::clone(&net),
            latch: |net| &net.resync_started,
        };
        Ok(Some(handle.spawn(async move {
            let _task = task;
            let _running = running;
            // A sweep discovers targets in its own task, so dispatch keeps joining
            // and refilling peer slots while the sweep waits on storage.
            let mut sweeps = tokio::task::JoinSet::new();
            let mut startup = true;
            if let Some(current) = net.upgrade() {
                sweeps.spawn(async move { current.schedule_startup_resync().await });
            }
            let mut sweep_pending = false;
            let mut sweep_backoff = runtime.resync_initial_backoff.max(Duration::from_millis(1));
            let mut full_sweep = Box::pin(tokio::time::sleep_until(
                tokio::time::Instant::now() + EMPTY_RESYNC_SLEEP,
            ));
            let mut syncs = tokio::task::JoinSet::new();
            loop {
                if !dispatch_due_resyncs(&net, &mut syncs, runtime) {
                    break;
                }
                let next_due = net
                    .upgrade()
                    .map(|current| {
                        let busy = MAX_RESYNC_PEER_CONCURRENCY
                            .saturating_sub(current.outbound.available_permits())
                            .max(syncs.len());
                        next_resync_wake(&current.resync_scheduler, busy)
                    })
                    .unwrap_or_else(|| tokio::time::Instant::now() + EMPTY_RESYNC_SLEEP);
                let due_sleep = tokio::time::sleep_until(next_due);
                tokio::pin!(due_sleep);
                tokio::select! {
                    biased;
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                        continue;
                    }
                    _ = &mut full_sweep, if sweeps.is_empty() && (sweep_pending || !runtime.full_sweep_interval.is_zero()) => {
                        match net.upgrade() {
                            Some(current) => {
                                sweeps.spawn(async move { current.schedule_full_sweep_resync().await });
                            }
                            None => break,
                        }
                    }
                    Some(swept) = sweeps.join_next(), if !sweeps.is_empty() => {
                        let failed = !matches!(swept, Ok(Ok(_)));
                        if let Ok(Err(error)) = &swept {
                            tracing::warn!(%error, startup, "failed to schedule resync sweep");
                        } else if let Err(error) = &swept {
                            tracing::warn!(%error, startup, "resync sweep task failed");
                        }
                        sweep_pending = failed;
                        let deadline = if failed {
                            if !startup {
                                sweep_backoff = sweep_backoff.saturating_mul(2)
                                    .min(runtime.resync_max_backoff.max(Duration::from_millis(1)));
                            }
                            tokio::time::Instant::now() + sweep_backoff
                        } else {
                            sweep_backoff = runtime.resync_initial_backoff.max(Duration::from_millis(1));
                            if startup {
                                next_full_sweep_deadline(runtime.full_sweep_interval, runtime.full_sweep_time_of_day)
                            } else {
                                tokio::time::Instant::now() + runtime.full_sweep_interval
                            }
                        };
                        startup = false;
                        full_sweep.as_mut().reset(deadline);
                    }
                    Some(result) = syncs.join_next(), if !syncs.is_empty() => {
                        if let Err(error) = result {
                            tracing::warn!(%error, "resync batch task failed");
                        }
                    }
                    _ = notify.notified() => {}
                    _ = &mut due_sleep => {}
                }
            }
            syncs.abort_all();
            sweeps.abort_all();
            // Draining lets every aborted batch release its own claims before a
            // replacement loop may start.
            while syncs.join_next().await.is_some() {}
            while sweeps.join_next().await.is_some() {}
        })))
    }
}

impl<S: Storage> SharedNet<S> {
    /// Completes one owned attempt. Only the holder of the claim may call this.
    fn finish_resync_attempt(
        &self,
        claim: ClaimGuard,
        result: std::result::Result<(), &io::Error>,
        runtime: IrohRuntimeConfig,
        advanced: bool,
    ) {
        let peer_id = claim.key().peer_id;
        let topic_id = claim.key().topic_id;
        let needs_sync = match self.target_needs_sync(peer_id, topic_id) {
            Ok(needs_sync) => needs_sync,
            Err(error) => {
                tracing::warn!(%peer_id, %topic_id, %error, "failed to evaluate resync target");
                true
            }
        };

        // An advancing page continues even when outbound evidence reads clean:
        // the peer may still hold more of the inbound goal.
        if advanced && result.is_ok() {
            self.resync_scheduler
                .complete_dirty(claim.settle(), RESYNC_PROGRESS_TURN);
            return;
        }
        if !needs_sync && result.is_ok() {
            self.resync_scheduler.complete_clean(claim.settle());
            return;
        }

        match result {
            Ok(()) => self
                .resync_scheduler
                .complete_dirty(claim.settle(), runtime.resync_interval),
            Err(_) => self.resync_scheduler.complete_failed(
                claim.settle(),
                runtime.resync_initial_backoff,
                runtime.resync_max_backoff,
            ),
        }
    }

    /// Reevaluates a target after peer evidence changed. Evidence may re-arm a
    /// target, but never completes or deletes an attempt it does not own.
    fn reconsider_target(&self, peer_id: PeerId, topic_id: crate::TopicId) {
        let needs_sync = match self.target_needs_sync(peer_id, topic_id) {
            Ok(needs_sync) => needs_sync,
            Err(error) => {
                tracing::warn!(%peer_id, %topic_id, %error, "failed to reevaluate resync target");
                true
            }
        };
        if needs_sync {
            self.resync_scheduler.reconsider(peer_id, topic_id);
        }
    }

    fn should_attempt_resync_target(&self, target: ResyncTarget) -> io::Result<bool> {
        if target.force.is_some() {
            return self.target_is_selected(target.key.peer_id, target.key.topic_id);
        }
        self.target_needs_sync(target.key.peer_id, target.key.topic_id)
    }

    fn target_is_selected(&self, peer_id: PeerId, topic_id: crate::TopicId) -> io::Result<bool> {
        let Some(state) = self
            .node
            .storage()
            .topic_state(&topic_id)
            .map_err(invalid_data)?
        else {
            return Ok(false);
        };
        if !state.members.contains(&peer_id)
            || (!state.members.contains(&self.node.peer_id())
                && self.local_leave_op(&state)?.is_none())
        {
            return Ok(false);
        }
        // Durable work owed to this peer keeps it a target, blocked with its
        // backoff, even while selection routes other work around it.
        Ok(self.node.sync_peers(topic_id, &state).contains(&peer_id)
            || self
                .node
                .storage()
                .has_sync_obligations(&peer_id, &topic_id)
                .map_err(invalid_data)?)
    }

    fn local_leave_op(
        &self,
        state: &crate::storage::TopicState,
    ) -> io::Result<Option<crate::OpId>> {
        let Some((key, false)) = state.membership_controls.get(&self.node.peer_id()) else {
            return Ok(None);
        };
        let meta = self
            .node
            .storage()
            .get_meta(&key.op_id)
            .map_err(invalid_data)?;
        Ok(meta
            .filter(|meta| meta.ready && meta.author == self.node.peer_id())
            .map(|_| key.op_id))
    }

    /// Whether the local topic may be certified as synchronized. A topic
    /// holding an unresolved id must keep negotiating even when the
    /// fingerprints match, or the hole survives every sweep.
    fn topic_is_whole(&self, topic_id: crate::TopicId) -> bool {
        match self.node.topic_unresolved(topic_id) {
            Ok(unresolved) => unresolved.is_empty(),
            Err(error) => {
                tracing::warn!(%topic_id, %error, "failed to check local topic integrity");
                false
            }
        }
    }

    /// Whether `peer_id` still needs this topic, judged from one view. Only an
    /// ack certified for the view's own branch can show the peer caught up;
    /// evidence that names no branch or another one proves nothing.
    fn target_needs_sync(&self, peer_id: PeerId, topic_id: crate::TopicId) -> io::Result<bool> {
        let Some(view) = self
            .node
            .storage()
            .topic_view(&topic_id, Some(&peer_id))
            .map_err(invalid_data)?
        else {
            return Ok(false);
        };
        let state = &view.state;
        if !state.members.contains(&peer_id) {
            return Ok(false);
        }
        if !state.members.contains(&self.node.peer_id()) {
            let Some(op_id) = self.local_leave_op(state)? else {
                return Ok(false);
            };
            return Ok(self.node.sync_peers(topic_id, state).contains(&peer_id)
                && !self
                    .node
                    .peer_reached_op(peer_id, op_id)
                    .map_err(invalid_data)?);
        }
        if view.owed {
            return Ok(true);
        }
        if !self.node.sync_peers(topic_id, state).contains(&peer_id) {
            return Ok(false);
        }
        // A hole clears no obligation and moves no clock, so nothing else here
        // would ever mark the target dirty again.
        match self.node.view_unresolved(&view) {
            Ok(unresolved) if unresolved.is_empty() => {}
            Ok(_) => return Ok(true),
            Err(error) => {
                tracing::warn!(%topic_id, %error, "failed to check local topic integrity");
                return Ok(true);
            }
        }
        Ok(!view.ack.as_ref().is_some_and(|ack| {
            ack.genesis == Some(state.genesis) && ack.clock.dominates(&view.clock)
        }))
    }

    fn dirty_selected_targets(&self, topic_id: crate::TopicId) -> io::Result<Vec<PeerId>> {
        let Some(state) = self
            .node
            .storage()
            .topic_state(&topic_id)
            .map_err(invalid_data)?
        else {
            return Ok(Vec::new());
        };
        if !state.members.contains(&self.node.peer_id()) && self.local_leave_op(&state)?.is_none() {
            return Ok(Vec::new());
        }
        let mut targets = Vec::new();
        for peer_id in self.node.sync_peers(topic_id, &state) {
            if self.target_needs_sync(peer_id, topic_id)? {
                targets.push(peer_id);
            }
        }
        Ok(targets)
    }
}

impl<S: Storage> IrohNet<S> {
    async fn schedule_startup_resync(self: &Arc<Self>) -> io::Result<usize> {
        self.schedule_full_sweep_resync().await
    }
}

impl<S: Storage> SharedNet<S> {
    fn schedule_persisted_obligations(&self) -> io::Result<usize> {
        let targets = self
            .node
            .storage()
            .all_sync_obligations()
            .map_err(invalid_data)?
            .into_iter()
            .map(|obligation| (obligation.peer_id, obligation.topic_id))
            .collect::<BTreeSet<_>>();
        for (peer_id, topic_id) in &targets {
            self.resync_scheduler
                .schedule_now(*peer_id, *topic_id, false);
        }
        Ok(targets.len())
    }
}

impl<S: Storage> IrohNet<S> {
    async fn schedule_full_sweep_resync(self: &Arc<Self>) -> io::Result<usize> {
        // Durable work is scheduled before maintenance starts, and maintenance
        // runs as its own job, so no topic it visits can delay that work.
        let scheduled = self
            .run_job(Lane::Control, |shared| {
                // The sweep is the one pass that revisits every topic, so let it
                // audit stored records again rather than reuse a whole verdict.
                if let Err(error) = shared.node.recheck_topics() {
                    tracing::warn!(%error, "sweep could not refresh topic caches");
                }
                let mut scheduled = shared.schedule_persisted_obligations()?;
                for (peer_id, topic_id) in shared.full_sweep_resync_targets()? {
                    shared
                        .resync_scheduler
                        .schedule_now(peer_id, topic_id, true);
                    scheduled += 1;
                }
                Ok(scheduled)
            })
            .await
            .and_then(|scheduled| scheduled);
        self.spawn_quarantine();
        scheduled
    }

    /// Quarantine every topic in one owned background job, one topic per
    /// blocking step, forwarding each eviction as soon as its topic commits.
    /// A failed topic is left for the next sweep; one job runs at a time.
    fn spawn_quarantine(self: &Arc<Self>) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        if self.tasks.is_closed() || self.quarantine_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let net = Arc::downgrade(self);
        let running = LoopGuard {
            net: Weak::clone(&net),
            latch: |net| &net.quarantine_started,
        };
        let task = self.tasks.track();
        handle.spawn(async move {
            let _task = task;
            let _running = running;
            let Some(node) = net.upgrade().map(|current| current.node.clone()) else {
                return;
            };
            let topics = match tokio::task::spawn_blocking(move || node.list_topics()).await {
                Ok(Ok(topics)) => topics,
                Ok(Err(error)) => {
                    tracing::warn!(%error, "sweep could not list topics to quarantine");
                    return;
                }
                Err(error) => {
                    tracing::warn!(%error, "sweep topic listing job failed");
                    return;
                }
            };
            for (index, topic) in topics.into_iter().enumerate() {
                let Some(current) = net.upgrade() else {
                    return;
                };
                if current.is_shutdown() {
                    return;
                }
                let node = current.node.clone();
                drop(current);
                let topic_id = topic.topic_id;
                let result =
                    tokio::task::spawn_blocking(move || node.quarantine_orphans(topic_id)).await;
                match result {
                    Ok(Ok(Some(eviction))) => {
                        if let Some(current) = net.upgrade() {
                            current.forward_evictions(vec![eviction]);
                        }
                    }
                    Ok(Ok(None)) => {}
                    Ok(Err(error)) => tracing::warn!(
                        %topic_id,
                        index,
                        %error,
                        "leaving topic quarantine for a later sweep"
                    ),
                    Err(error) => {
                        tracing::warn!(%topic_id, index, %error, "topic quarantine job failed")
                    }
                }
            }
        });
    }

    pub async fn sync_with(
        &self,
        peer: iroh::EndpointAddr,
        messages: &[SyncMessage],
    ) -> io::Result<Vec<SyncMessage>> {
        let _task = self.tasks.enter()?;
        let mut last_error = None;
        for _ in 0..2 {
            let connection = match self
                .pool
                .get_or_connect(peer.clone(), self.runtime.connect_timeout)
                .await
            {
                Ok(connection) => connection,
                Err(error) => return Err(error),
            };
            match self
                .sync_with_connection(connection.clone(), messages)
                .await
            {
                Ok(responses) => return Ok(responses),
                Err(error) => {
                    // Keep healthy connections pooled on stream-level failures;
                    // drop them when closed or unresponsive (timed out).
                    if connection.close_reason().is_some()
                        || error.kind() == io::ErrorKind::TimedOut
                    {
                        let _ = self.pool.remove(&connection);
                    }
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| io::Error::other("sync failed")))
    }

    async fn sync_with_connection(
        &self,
        connection: iroh::endpoint::Connection,
        messages: &[SyncMessage],
    ) -> io::Result<Vec<SyncMessage>> {
        tokio::time::timeout(self.runtime.sync_io_timeout, async {
            let (mut send, mut recv) = connection.open_bi().await.map_err(other)?;
            self.outbound_streams.fetch_add(1, Ordering::Relaxed);
            let timeout = self.runtime.sync_io_timeout;
            write_sync_messages(&mut send, messages, timeout, self.limits).await?;
            read_sync_messages(&mut recv, timeout, self.limits).await
        })
        .await
        .map_err(|_| timed_out("sync exchange timed out"))?
    }

    pub async fn sync_now(
        &self,
        peer: iroh::EndpointAddr,
        topic_id: crate::TopicId,
    ) -> io::Result<()> {
        self.sync_topics_now(peer, &[topic_id])
            .await
            .remove(&topic_id)
            .unwrap_or(Ok(()))
    }

    /// Manually syncs `topic_ids` with one peer through the same batched page
    /// exchange the resync loop uses, paging each topic while it advances up to
    /// a page budget. Per topic: `Ok` when its goal completed, `WouldBlock`
    /// when it advanced but the budget ran out and the rest is scheduled, and
    /// the error of an exchange that failed or made no progress.
    pub async fn sync_topics_now(
        &self,
        peer: iroh::EndpointAddr,
        topic_ids: &[crate::TopicId],
    ) -> BTreeMap<crate::TopicId, io::Result<()>> {
        let _task = match self.tasks.enter() {
            Ok(task) => task,
            Err(error) => {
                return topic_ids
                    .iter()
                    .map(|topic_id| (*topic_id, Err(clone_error(&error))))
                    .collect();
            }
        };
        // A manual sync takes one of the outbound peer slots the resync loop
        // uses, and wakes the loop when it gives the slot back.
        let _slot = match Arc::clone(&self.outbound).acquire_owned().await {
            Ok(permit) => OutboundSlot {
                _permit: permit,
                wake: self.resync_scheduler.notifier(),
            },
            Err(_) => {
                let error = io::Error::other("outbound sync slots closed");
                return topic_ids
                    .iter()
                    .map(|topic_id| (*topic_id, Err(clone_error(&error))))
                    .collect();
            }
        };
        let attempt_id = next_attempt_id();
        let attempt = self.attempt_identity(attempt_id);
        let remote_peer_id = peer_id_from_endpoint_id(peer.id);
        let live = attempt_id.map(|attempt_id| {
            self.resync_scheduler.begin_attempts(
                topic_ids.iter().map(|topic_id| ResyncTargetKey {
                    peer_id: remote_peer_id,
                    topic_id: *topic_id,
                }),
                attempt_id,
            )
        });
        let endpoint_id = peer.id;
        // A bounded page is not the goal: keep paging while the exchange really
        // advances, up to a caller budget, so catching up is not reported as an
        // I/O error merely because another page is needed.
        let mut settled = BTreeMap::new();
        let mut paging = topic_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        for _ in 0..MAX_SYNC_NOW_PAGES {
            if paging.is_empty() {
                break;
            }
            let mut outcomes = self.run_topic_batch(peer.clone(), &paging, None).await;
            for topic_id in std::mem::take(&mut paging) {
                let result = outcomes.results.remove(&topic_id).unwrap_or(Ok(()));
                let advancing = outcomes.advanced.contains(&topic_id);
                if result.is_ok() && advancing {
                    paging.push(topic_id);
                }
                settled.insert(topic_id, (result, advancing));
            }
        }
        let mut results = BTreeMap::new();
        let mut finished = Vec::with_capacity(settled.len());
        let mut noted = Vec::with_capacity(settled.len());
        for (topic_id, (mut result, advancing)) in settled {
            // An exhausted budget is progress, not an unreachable peer.
            let outcome = attempt_outcome(result.as_ref().copied(), advancing);
            // Work still outstanding after the page budget is not a completed sync.
            if result.is_ok() && advancing {
                result = Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "sync page budget exhausted; the rest is scheduled",
                ));
            }
            noted.push(match &result {
                Err(error) if !advancing => Err(clone_error(error)),
                _ => Ok(()),
            });
            finished.push((topic_id, outcome, advancing));
            results.insert(topic_id, result);
        }
        if results.values().any(Result::is_err) {
            // Drops the pooled connection only when it is already closed.
            let _ = self.pool.get(&endpoint_id);
        }
        let recorded = self
            .run_job(Lane::Control, move |shared| {
                shared.note_outcome(remote_peer_id, noted.iter().map(|noted| noted.as_ref().copied()));
                for (topic_id, outcome, advancing) in finished {
                    let key = ResyncTargetKey {
                        peer_id: remote_peer_id,
                        topic_id,
                    };
                    let first = live.as_ref().is_some_and(|live| live.end(key));
                    if let Err(error) = shared.node.record_attempt_result(
                        remote_peer_id,
                        topic_id,
                        attempt,
                        &outcome,
                        first,
                    ) {
                        tracing::warn!(%remote_peer_id, %topic_id, %error, "failed to record sync attempt");
                    }
                    // A manual sync holds no claim, so it reports evidence instead of
                    // completing an attempt the resync loop may own.
                    if advancing {
                        shared.resync_scheduler.reconsider(remote_peer_id, topic_id);
                    } else {
                        shared.reconsider_target(remote_peer_id, topic_id);
                    }
                }
            })
            .await;
        if let Err(error) = recorded {
            tracing::warn!(%remote_peer_id, %error, "failed to finish manual sync");
        }
        results
    }

    /// Services a peer's due resync targets as multi-topic batches over the
    /// pooled connection, recording per-topic results.
    async fn sync_peer_batch_with_runtime(
        &self,
        peer_id: PeerId,
        mut lease: ResyncLease,
        runtime: IrohRuntimeConfig,
    ) {
        let deadline = tokio::time::Instant::now()
            + runtime
                .connect_timeout
                .saturating_add(runtime.sync_io_timeout)
                .saturating_mul(4);
        let claimed = lease.targets();
        let fail_all = |error: &io::Error| {
            claimed
                .iter()
                .map(|target| (target.key.topic_id, Err(clone_error(error)), false))
                .collect::<Vec<_>>()
        };
        let addr = match peer_id_to_endpoint_addr(peer_id) {
            Ok(addr) => addr,
            Err(error) => {
                self.publish_results(peer_id, fail_all(&error), &mut lease, runtime)
                    .await;
                return;
            }
        };
        let targets = claimed.clone();
        let decided = self
            .run_job(Lane::Control, move |shared| {
                targets
                    .into_iter()
                    .map(|target| {
                        let topic_id = target.key.topic_id;
                        let decision = shared.should_attempt_resync_target(target);
                        if matches!(decision, Ok(false))
                            && let Err(error) = shared.gc_stale_obligations(peer_id, topic_id)
                        {
                            tracing::warn!(%peer_id, %topic_id, %error, "failed to gc stale sync obligations");
                        }
                        (target, decision)
                    })
                    .collect::<Vec<_>>()
            })
            .await;
        let decided = match decided {
            Ok(decided) => decided,
            Err(error) => {
                self.publish_results(peer_id, fail_all(&error), &mut lease, runtime)
                    .await;
                return;
            }
        };
        let mut topics = Vec::with_capacity(decided.len());
        let mut failed = Vec::new();
        for (target, decision) in decided {
            match decision {
                Ok(true) => topics.push(target.key.topic_id),
                Ok(false) => {
                    if let Some(claim) = lease.take_claim(&target.key) {
                        self.resync_scheduler.complete_clean(claim.settle());
                    }
                }
                Err(error) => failed.push((target.key.topic_id, Err(error), false)),
            }
        }
        self.publish_results(peer_id, failed, &mut lease, runtime)
            .await;
        for chunk in topics.chunks(MAX_TOPICS_PER_RESYNC_BATCH) {
            if tokio::time::timeout_at(
                deadline,
                self.sync_topic_chunk(addr.clone(), chunk, &mut lease, runtime),
            )
            .await
            .is_err()
            {
                // Only the claims this batch never finished belong to the
                // timeout; a released chunk keeps its recorded result.
                let error = timed_out("peer sync batch timed out");
                let results = lease
                    .drain_claims()
                    .into_iter()
                    .map(|claim| {
                        (
                            claim.key().topic_id,
                            Err(clone_error(&error)),
                            false,
                            Some(claim),
                        )
                    })
                    .collect::<Vec<_>>();
                let recorded = self
                    .run_job(Lane::Control, move |shared| {
                        shared.note_outcome(peer_id, [Err(&error)]);
                        shared.record_results(peer_id, results, runtime);
                    })
                    .await;
                if let Err(error) = recorded {
                    tracing::warn!(%peer_id, %error, "failed to record timed out sync batch");
                }
                return;
            }
        }
    }
}

impl<S: Storage> SharedNet<S> {
    /// Drops persisted obligations toward a peer that is no longer a topic
    /// member (or whose topic state is gone) so sweeps stop rescheduling them.
    fn gc_stale_obligations(&self, peer_id: PeerId, topic_id: crate::TopicId) -> io::Result<()> {
        if !self
            .node
            .storage()
            .has_sync_obligations(&peer_id, &topic_id)
            .map_err(invalid_data)?
        {
            return Ok(());
        }
        // Storage repeats the branch and membership check in its transaction.
        let genesis = self
            .node
            .storage()
            .topic_state(&topic_id)
            .map_err(invalid_data)?
            .map(|state| state.genesis);
        self.node
            .storage()
            .clear_peer_sync_state(&peer_id, &topic_id, genesis)
            .map_err(invalid_data)?;
        Ok(())
    }
}

impl<S: Storage> IrohNet<S> {
    async fn sync_topic_chunk(
        &self,
        peer: iroh::EndpointAddr,
        topic_ids: &[crate::TopicId],
        lease: &mut ResyncLease,
        runtime: IrohRuntimeConfig,
    ) {
        let remote_peer_id = peer_id_from_endpoint_id(peer.id);
        let endpoint_id = peer.id;
        let outcomes = self
            .run_topic_batch(peer, topic_ids, Some((lease, runtime)))
            .await;
        let noted = outcomes
            .results
            .values()
            .map(copy_result)
            .collect::<Vec<_>>();
        let health = self
            .run_job(Lane::Control, move |shared| {
                shared.note_outcome(
                    remote_peer_id,
                    noted.iter().map(|result| result.as_ref().copied()),
                );
            })
            .await;
        if let Err(error) = health {
            tracing::warn!(peer_id = %remote_peer_id, %error, "failed to record peer health");
        }
        let mut failures = 0_usize;
        let mut first_error = None;
        let mut unsettled = Vec::new();
        for (topic_id, outcome) in &outcomes.results {
            if let Err(error) = outcome {
                failures += 1;
                if first_error.is_none() {
                    first_error = Some(clone_error(error));
                }
            }
            // Outcomes decided before any exchange ran, such as a planning
            // failure, are the only ones left to publish here.
            if !outcomes.settled.contains(topic_id) {
                unsettled.push((
                    *topic_id,
                    copy_result(outcome),
                    outcomes.advanced.contains(topic_id),
                ));
            }
        }
        self.publish_results(remote_peer_id, unsettled, lease, runtime)
            .await;
        if let Some(error) = first_error {
            // Drops the pooled connection only when it is already closed.
            let _ = self.pool.get(&endpoint_id);
            tracing::warn!(
                peer_id = %remote_peer_id,
                failures,
                %error,
                "failed to resync topics with peer"
            );
        }
    }

    /// Publishes every decided outcome this batch has not published yet:
    /// records it and releases the claim the batch owns for it. Called between
    /// exchanges, so ownership of finished work is handed back immediately.
    async fn settle_known_results(
        &self,
        remote_peer_id: PeerId,
        outcomes: &BTreeMap<crate::TopicId, io::Result<()>>,
        advanced: &BTreeSet<crate::TopicId>,
        settled: &mut BTreeSet<crate::TopicId>,
        lease: &mut ResyncLease,
        runtime: IrohRuntimeConfig,
    ) {
        let results = outcomes
            .iter()
            .filter(|(topic_id, _)| settled.insert(**topic_id))
            .map(|(topic_id, outcome)| {
                (*topic_id, copy_result(outcome), advanced.contains(topic_id))
            })
            .collect();
        self.publish_results(remote_peer_id, results, lease, runtime)
            .await;
    }

    /// Takes this batch's claims for `results` and records them in a control
    /// job. A job that never runs drops the claims, which releases them.
    async fn publish_results(
        &self,
        remote_peer_id: PeerId,
        results: Vec<(crate::TopicId, io::Result<()>, bool)>,
        lease: &mut ResyncLease,
        runtime: IrohRuntimeConfig,
    ) {
        if results.is_empty() {
            return;
        }
        let results = results
            .into_iter()
            .map(|(topic_id, result, advanced)| {
                let key = ResyncTargetKey {
                    peer_id: remote_peer_id,
                    topic_id,
                };
                (topic_id, result, advanced, lease.take_claim(&key))
            })
            .collect::<Vec<_>>();
        let published = self
            .run_job(Lane::Control, move |shared| {
                shared.record_results(remote_peer_id, results, runtime);
            })
            .await;
        if let Err(error) = published {
            tracing::warn!(peer_id = %remote_peer_id, %error, "failed to publish sync results");
        }
    }

    /// Syncs every topic in `topic_ids` with one peer using batched streams:
    /// one fingerprint round trip for the whole batch, then data/request and
    /// ack round trips that carry only the diverged topics. Returns an outcome
    /// per topic.
    async fn run_topic_batch(
        &self,
        peer: iroh::EndpointAddr,
        topic_ids: &[crate::TopicId],
        mut settle: Option<(&mut ResyncLease, IrohRuntimeConfig)>,
    ) -> BatchOutcomes {
        let remote_peer_id = peer_id_from_endpoint_id(peer.id);
        let mut outcomes = BTreeMap::new();
        let mut advanced = BTreeSet::new();
        let mut settled = BTreeSet::new();

        let mut fingerprints = BTreeMap::new();
        let mut request = Vec::with_capacity(topic_ids.len() * 2);
        let topics = topic_ids.to_vec();
        let prepared = self
            .run_job(Lane::Control, move |shared| {
                topics
                    .into_iter()
                    .map(|topic_id| {
                        let prepared = shared
                            .node
                            .sync_fingerprint(topic_id)
                            .map(|fingerprint| (shared.node.sync_open(topic_id), fingerprint));
                        (topic_id, prepared)
                    })
                    .collect::<Vec<_>>()
            })
            .await;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                for topic_id in topic_ids {
                    outcomes.insert(*topic_id, Err(clone_error(&error)));
                }
                return BatchOutcomes::new(outcomes, advanced, settled);
            }
        };
        for (topic_id, prepared) in prepared {
            match prepared {
                Ok((open, fingerprint)) => {
                    request.push(SyncMessage::Open(open));
                    fingerprints.insert(topic_id, fingerprint.fingerprint);
                    request.push(SyncMessage::Fingerprint(fingerprint));
                }
                Err(error) => {
                    outcomes.insert(topic_id, Err(invalid_data(error)));
                }
            }
        }
        if fingerprints.is_empty() {
            return BatchOutcomes::new(outcomes, advanced, settled);
        }
        let responses = match self.sync_with(peer.clone(), &request).await {
            Ok(responses) => responses,
            Err(error) => {
                for topic_id in fingerprints.keys() {
                    outcomes.insert(*topic_id, Err(clone_error(&error)));
                }
                return BatchOutcomes::new(outcomes, advanced, settled);
            }
        };

        let mut matching = BTreeMap::new();
        let mut summaries = BTreeMap::new();
        for response in responses {
            match response {
                SyncMessage::Fingerprint(remote) => {
                    if fingerprints.get(&remote.topic_id) != Some(&remote.fingerprint) {
                        continue;
                    }
                    matching.insert(remote.topic_id, remote.fingerprint);
                }
                SyncMessage::Summary(summary) if fingerprints.contains_key(&summary.topic_id) => {
                    summaries.insert(summary.topic_id, summary);
                }
                SyncMessage::Failure(failure) if fingerprints.contains_key(&failure.topic_id) => {
                    outcomes.insert(failure.topic_id, Err(topic_failed(&failure)));
                    summaries.remove(&failure.topic_id);
                }
                other => {
                    let error = invalid_data(format!(
                        "unexpected sync response {}",
                        _message_type_name(&other)
                    ));
                    for topic_id in fingerprints.keys() {
                        outcomes
                            .entry(*topic_id)
                            .or_insert_with(|| Err(clone_error(&error)));
                    }
                    return BatchOutcomes::new(outcomes, advanced, settled);
                }
            }
        }
        // Two identically damaged stores still match, so the local integrity
        // check decides whether a matching fingerprint counts as synced.
        let decided = self
            .run_job(Lane::Control, move |shared| {
                matching
                    .into_iter()
                    .map(|(topic_id, fingerprint)| {
                        if !shared.topic_is_whole(topic_id) {
                            return (
                                topic_id,
                                Err(invalid_data(
                                    "local topic is incomplete despite a matching fingerprint",
                                )),
                            );
                        }
                        let outcome = shared
                            .node
                            .record_fingerprint(remote_peer_id, topic_id, fingerprint)
                            .map_err(invalid_data)
                            .and_then(|matched| {
                                if matched {
                                    Ok(())
                                } else {
                                    Err(invalid_data("topic changed during fingerprint exchange"))
                                }
                            });
                        (topic_id, outcome)
                    })
                    .collect::<Vec<_>>()
            })
            .await;
        let decided = match decided {
            Ok(decided) => decided,
            Err(error) => {
                for topic_id in fingerprints.keys() {
                    outcomes
                        .entry(*topic_id)
                        .or_insert_with(|| Err(clone_error(&error)));
                }
                return BatchOutcomes::new(outcomes, advanced, settled);
            }
        };
        for (topic_id, outcome) in decided {
            summaries.remove(&topic_id);
            outcomes.insert(topic_id, outcome);
        }
        for topic_id in fingerprints.keys() {
            if !outcomes.contains_key(topic_id) && !summaries.contains_key(topic_id) {
                outcomes
                    .entry(*topic_id)
                    .or_insert_with(|| Err(invalid_data("peer did not return a sync summary")));
            }
        }

        // Topics are planned one group at a time: only the group being sent,
        // plus the one planned topic that did not fit it, is held in memory.
        let limits = self.limits;
        let mut queue = summaries.into_iter().collect::<VecDeque<_>>();
        let mut carried = None;
        while !queue.is_empty() || carried.is_some() {
            let unplanned = queue
                .iter()
                .map(|(topic_id, _)| *topic_id)
                .chain(
                    carried
                        .as_ref()
                        .map(|planned: &PlannedTopicSync| planned.topic_id),
                )
                .collect::<Vec<_>>();
            let taken = std::mem::take(&mut queue);
            let held = carried.take();
            let planned = self
                .run_job(Lane::Bulk, move |shared| {
                    shared.plan_group(remote_peer_id, taken, held, limits)
                })
                .await;
            let PlannedGroup {
                group,
                next,
                rest,
                outcomes: planned_outcomes,
            } = match planned {
                Ok(planned) => planned,
                Err(error) => {
                    for topic_id in unplanned {
                        outcomes.insert(topic_id, Err(clone_error(&error)));
                    }
                    break;
                }
            };
            outcomes.extend(planned_outcomes);
            queue = rest;
            carried = next;
            if group.is_empty() {
                continue;
            }
            self.run_topic_batch_exchange(
                peer.clone(),
                remote_peer_id,
                group,
                &mut outcomes,
                &mut advanced,
            )
            .await;
            // Each topic's durable outcome is published before the next
            // exchange awaits, so a later failure or the batch deadline
            // cannot re-run work this batch already finished.
            if let Some((lease, runtime)) = settle.as_mut() {
                self.settle_known_results(
                    remote_peer_id,
                    &outcomes,
                    &advanced,
                    &mut settled,
                    lease,
                    *runtime,
                )
                .await;
            }
        }
        BatchOutcomes::new(outcomes, advanced, settled)
    }
}

impl<S: Storage> SharedNet<S> {
    /// Plans topics from `queue`, after `held`, into one group that fits a
    /// stream. The first planned topic that does not fit is returned as `next`
    /// with the unplanned rest, so planning never runs ahead of sending.
    fn plan_group(
        &self,
        remote_peer_id: PeerId,
        mut queue: VecDeque<(crate::TopicId, SyncSummary)>,
        mut held: Option<PlannedTopicSync>,
        limits: StreamLimits,
    ) -> PlannedGroup {
        let mut planned_group = PlannedGroup {
            group: Vec::new(),
            next: None,
            rest: VecDeque::new(),
            outcomes: Vec::new(),
        };
        let (mut messages, mut responses, mut bytes) = (0_usize, 0_usize, 0_usize);
        loop {
            let planned = match held.take() {
                Some(planned) => planned,
                None => {
                    let Some((topic_id, summary)) = queue.pop_front() else {
                        break;
                    };
                    match self.plan_topic_messages(remote_peer_id, topic_id, &summary) {
                        Ok(Some(planned)) => planned,
                        Ok(None) => {
                            planned_group.outcomes.push((topic_id, Ok(())));
                            continue;
                        }
                        Err(error) => {
                            planned_group.outcomes.push((topic_id, Err(error)));
                            continue;
                        }
                    }
                }
            };
            let size = match planned.messages.iter().try_fold(0usize, |bytes, message| {
                super::framed_message_len(message).map(|len| bytes.saturating_add(len))
            }) {
                Ok(size) if size <= limits.bytes => size,
                Ok(_) => {
                    let error = invalid_data("sync plan exceeds stream byte limit");
                    planned_group.outcomes.push((planned.topic_id, Err(error)));
                    continue;
                }
                Err(error) => {
                    planned_group.outcomes.push((planned.topic_id, Err(error)));
                    continue;
                }
            };
            if !planned_group.group.is_empty()
                && (bytes.saturating_add(size) > limits.bytes
                    || messages + planned.messages.len() > limits.batch_messages
                    || responses + planned.estimated_responses > limits.batch_messages)
            {
                #[cfg(test)]
                self.planned_peak
                    .fetch_max(bytes + size, std::sync::atomic::Ordering::Relaxed);
                planned_group.next = Some(planned);
                break;
            }
            bytes += size;
            messages += planned.messages.len();
            responses += planned.estimated_responses;
            planned_group.group.push(planned);
        }
        #[cfg(test)]
        self.planned_peak
            .fetch_max(bytes, std::sync::atomic::Ordering::Relaxed);
        planned_group.rest = queue;
        planned_group
    }

    fn plan_topic_messages(
        &self,
        remote_peer_id: PeerId,
        topic_id: crate::TopicId,
        summary: &SyncSummary,
    ) -> io::Result<Option<PlannedTopicSync>> {
        let budget = crate::sync::PageBudget::from_credit(crate::sync::SyncCredit::default());
        let receipt = summary.staged.clone();
        // Authorization, branch, pages and the summary sent all come from one
        // snapshot; the evidence write below re-checks its own preconditions.
        let Some(read) = self
            .node
            .storage()
            .read_snapshot(|read| {
                self.snapshot_plan(read, remote_peer_id, summary, receipt, budget)
            })
            .map_err(invalid_data)?
        else {
            return self.plan_pull(remote_peer_id, topic_id, summary);
        };
        let SnapshotPlan {
            state,
            clock,
            mut plan,
            mut push_more,
            converged,
            leave,
            local_summary,
        } = read;
        // A peer outside the membership is owed nothing and serves nothing.
        let member = state.members.contains(&remote_peer_id);
        // Both sides may have converged since the fingerprints were compared,
        // through the peer's own push; the summary then proves the same match.
        if converged
            && self
                .node
                .record_fingerprint(remote_peer_id, topic_id, summary.fingerprint)
                .map_err(invalid_data)?
        {
            return Ok(None);
        }
        // Across two branches only the winner's namespace is a goal: the loser
        // expects the winner's clock, the winner expects its own certified.
        let branch = summary.genesis.filter(|remote| *remote != state.genesis);
        let planned = crate::sync::request_genesis(state.genesis, branch);
        let mut goal = TopicGoal {
            pull: false,
            genesis: Some(planned),
            replaces: (planned != state.genesis).then_some(state.genesis),
            inbound: if member && (branch.is_none() || planned != state.genesis) {
                summary.actor_clock.clone()
            } else {
                Default::default()
            },
            outbound: if member && planned == state.genesis {
                clock
            } else {
                Default::default()
            },
        };
        let terminal = leave.is_some();
        if let Some((page, position)) = leave {
            plan.send = page.ops;
            push_more = page.more;
            plan.need.clear();
            plan.actor_range_hints.clear();
            goal.inbound = crate::ActorClock::new();
            goal.outbound = crate::ActorClock::new();
            if let Some((actor_id, seq)) = position {
                goal.outbound.observe(actor_id, seq);
            }
        }
        let send = std::mem::take(&mut plan.send);
        let wants = !plan.need.is_empty() || !plan.actor_range_hints.is_empty();
        let open = crate::sync::SyncEngine::<S>::open(
            topic_id,
            self.node.peer_id(),
            Some(state.event_type_id),
        );
        let mut controls = vec![SyncMessage::Open(open)];
        let mut credit_ops = 0;
        if wants {
            let request = crate::sync::page_request(plan, Some(planned));
            credit_ops = request.credit.ops as usize;
            controls.push(SyncMessage::Request(request));
        }
        if let Some(local_summary) = local_summary.filter(|_| !terminal) {
            controls.push(SyncMessage::Summary(local_summary));
        }
        // The pushed page is cut to what one stream holds beside this topic's controls.
        let mut control_bytes = 0_usize;
        for message in &controls {
            control_bytes = control_bytes.saturating_add(super::framed_message_len(message)?);
        }
        let data = super::sync_data_page(
            topic_id,
            send,
            self.limits.messages.saturating_sub(controls.len()),
            self.limits.bytes.saturating_sub(control_bytes),
        )?;
        push_more |= data.cut;
        let pushes = !data.messages.is_empty();
        let mut messages = Vec::with_capacity(controls.len() + data.messages.len());
        let mut controls = controls.into_iter();
        messages.extend(controls.next());
        messages.extend(data.messages);
        messages.extend(controls);
        // A summary for the open, one ack for the pushed data, the peer's own
        // request, and at most one page of data frames plus its page result.
        let estimated_responses = 3 + if wants {
            credit_ops.div_ceil(MAX_SYNC_DATA_OPS_PER_MESSAGE) + 1
        } else {
            0
        };
        Ok(Some(PlannedTopicSync {
            topic_id,
            goal,
            pushes,
            push_more,
            messages,
            estimated_responses,
        }))
    }

    /// Everything one topic's push, request and summary read, from `read`.
    /// `None` when the topic is not held here.
    fn snapshot_plan(
        &self,
        read: &dyn crate::storage::SnapshotRead,
        remote_peer_id: PeerId,
        summary: &SyncSummary,
        receipt: Option<crate::sync::SyncReceipt>,
        budget: crate::sync::PageBudget,
    ) -> crate::Result<Option<SnapshotPlan>> {
        let topic_id = summary.topic_id;
        let Some(view) = read.topic_view(&topic_id, None)? else {
            return Ok(None);
        };
        let sync = self.node.sync_engine();
        // A peer still staging this topic continues from its newest receipt on
        // this branch.
        let staged = match (summary.genesis, receipt) {
            (None, Some(receipt)) if receipt.genesis == view.state.genesis => Some(SyncSummary {
                actor_clock: receipt.clock,
                ..summary.clone()
            }),
            _ => None,
        };
        let (plan, push_more) = sync.negotiate_in(
            read,
            remote_peer_id,
            staged.as_ref().unwrap_or(summary),
            budget,
        )?;
        let converged = view.state.members.contains(&remote_peer_id)
            && summary.genesis == Some(view.state.genesis)
            && summary.heads == view.state.heads
            && self.node.unresolved_in(read, &view)?.is_empty();
        let mut leave = None;
        if !view.state.members.contains(&self.node.peer_id())
            && let Some((op_id, position)) = self.leave_in(read, &view.state)?
        {
            let request = crate::sync::SyncRequest {
                topic_id,
                known: BTreeSet::new(),
                wants: BTreeSet::from([op_id]),
                actor_range_hints: Vec::new(),
                genesis: None,
                credit: crate::sync::SyncCredit::default(),
            };
            let page = sync.response_in(read, remote_peer_id, &request, budget)?;
            leave = Some((page, position));
        }
        let local_summary = match leave {
            Some(_) => None,
            None => Some(sync.summary_in(read, topic_id)?),
        };
        Ok(Some(SnapshotPlan {
            state: view.state,
            clock: view.clock,
            plan,
            push_more,
            converged,
            leave,
            local_summary,
        }))
    }

    /// This node's own signed leave, and its actor position when stored, read
    /// from `read`.
    #[allow(clippy::type_complexity)]
    fn leave_in(
        &self,
        read: &dyn crate::storage::SnapshotRead,
        state: &crate::storage::TopicState,
    ) -> crate::Result<Option<(crate::OpId, Option<(crate::ActorId, u64)>)>> {
        let Some((key, false)) = state.membership_controls.get(&self.node.peer_id()) else {
            return Ok(None);
        };
        let meta = read.get_meta(&key.op_id)?;
        Ok(meta
            .filter(|meta| meta.ready && meta.author == self.node.peer_id())
            .map(|meta| (key.op_id, Some((meta.actor_id, meta.actor_seq)))))
    }

    /// Pull a topic this node does not hold from a peer that does. Its pages are
    /// staged until the history makes this node a member, then promoted.
    fn plan_pull(
        &self,
        remote_peer_id: PeerId,
        topic_id: crate::TopicId,
        summary: &SyncSummary,
    ) -> io::Result<Option<PlannedTopicSync>> {
        let Some(genesis) = summary.genesis else {
            return Ok(None);
        };
        let probe = crate::sync::SyncData {
            topic_id,
            ops: Vec::new(),
        };
        self.node
            .ensure_iroh_peer_whitelisted(remote_peer_id, &probe)
            .map_err(invalid_data)?;
        // Staging of another branch from this peer is continued only by a
        // smaller genesis, whose first fragment replaces it.
        let staged = match self
            .node
            .staged_topic(remote_peer_id, topic_id)
            .map_err(invalid_data)?
            .and_then(|staged| {
                staged
                    .genesis
                    .map(|staged_genesis| (staged_genesis, staged))
            }) {
            Some((staged_genesis, staged)) if staged_genesis == genesis => staged.clock,
            Some((staged_genesis, _)) if genesis < staged_genesis => crate::ActorClock::new(),
            Some(_) => {
                return Err(invalid_data(
                    "peer offers a larger branch than the one staged from it",
                ));
            }
            None => crate::ActorClock::new(),
        };
        let actor_range_hints = summary
            .actor_clock
            .iter()
            .filter(|(actor_id, seq)| staged.get(actor_id) < **seq)
            .map(|(actor_id, seq)| crate::sync::ActorRangeHint {
                actor_id: *actor_id,
                from_exclusive: staged.get(actor_id),
                to_inclusive: *seq,
            })
            .collect::<Vec<_>>();
        // Everything the peer holds is staged: finish the activation that
        // history owes instead of reporting nothing left to pull.
        if actor_range_hints.is_empty() {
            if self
                .node
                .finish_bootstrap(remote_peer_id, topic_id)
                .map_err(invalid_data)?
            {
                return self.plan_topic_messages(remote_peer_id, topic_id, summary);
            }
            return Err(invalid_data(
                "staged topic history does not make this node a member",
            ));
        }
        let plan = crate::sync::SyncPlan {
            topic_id,
            common: BTreeSet::new(),
            have: BTreeSet::new(),
            send: Vec::new(),
            need: BTreeSet::new(),
            actor_range_hints,
        };
        let request = crate::sync::page_request(plan, Some(genesis));
        let credit_ops = request.credit.ops as usize;
        Ok(Some(PlannedTopicSync {
            topic_id,
            goal: TopicGoal {
                pull: true,
                genesis: Some(genesis),
                replaces: None,
                inbound: summary.actor_clock.clone(),
                outbound: crate::ActorClock::new(),
            },
            pushes: false,
            push_more: false,
            messages: vec![
                SyncMessage::Open(self.node.sync_open(topic_id)),
                SyncMessage::Request(request),
            ],
            estimated_responses: 3 + credit_ops.div_ceil(MAX_SYNC_DATA_OPS_PER_MESSAGE),
        }))
    }

    /// The contiguous prefix per actor that `peer_id` staged here for
    /// `topic_id` on `genesis`. Staging of another branch holds nothing of it.
    fn staged_clock(
        &self,
        peer_id: PeerId,
        topic_id: crate::TopicId,
        genesis: Option<crate::OpId>,
    ) -> io::Result<crate::ActorClock> {
        Ok(self
            .node
            .staged_topic(peer_id, topic_id)
            .map_err(invalid_data)?
            .filter(|staged| staged.genesis.is_some() && staged.genesis == genesis)
            .map(|staged| staged.clock)
            .unwrap_or_default())
    }
}

impl<S: Storage> IrohNet<S> {
    async fn run_topic_batch_exchange(
        &self,
        peer: iroh::EndpointAddr,
        remote_peer_id: PeerId,
        group: Vec<PlannedTopicSync>,
        outcomes: &mut BTreeMap<crate::TopicId, io::Result<()>>,
        advanced: &mut BTreeSet<crate::TopicId>,
    ) {
        let group_topics = group
            .iter()
            .map(|planned| planned.topic_id)
            .collect::<BTreeSet<_>>();
        let fail_group = |outcomes: &mut BTreeMap<crate::TopicId, io::Result<()>>,
                          error: &io::Error| {
            for topic_id in &group_topics {
                outcomes.insert(*topic_id, Err(clone_error(error)));
            }
        };
        // Progress is measured toward each topic's captured goal only. Bytes
        // moved, repeated ids and unrelated local writes are not progress.
        let measured = self
            .run_job(Lane::Control, move |shared| {
                group
                    .into_iter()
                    .map(|planned| {
                        let before =
                            shared.goal_progress(remote_peer_id, planned.topic_id, &planned.goal);
                        (planned, before)
                    })
                    .collect::<Vec<_>>()
            })
            .await;
        let measured = match measured {
            Ok(measured) => measured,
            Err(error) => {
                fail_group(outcomes, &error);
                return;
            }
        };
        let mut goals = BTreeMap::new();
        let mut owed_acks = BTreeSet::new();
        let mut more = BTreeSet::new();
        let mut messages = Vec::new();
        for (planned, before) in measured {
            match before {
                Ok(before) => {
                    goals.insert(planned.topic_id, (planned.goal, before));
                }
                Err(error) => {
                    outcomes.insert(planned.topic_id, Err(error));
                    continue;
                }
            }
            if planned.pushes {
                owed_acks.insert(planned.topic_id);
            }
            if planned.push_more {
                more.insert(planned.topic_id);
            }
            messages.extend(planned.messages);
        }
        let responses = match self.sync_with(peer.clone(), &messages).await {
            Ok(responses) => responses,
            Err(error) => {
                fail_group(outcomes, &error);
                return;
            }
        };

        let topics = group_topics.clone();
        let geneses = goals
            .iter()
            .map(|(topic_id, (goal, _))| (*topic_id, goal.genesis))
            .collect::<BTreeMap<_, _>>();
        let replies = self
            .run_job(Lane::Bulk, move |shared| {
                shared.batch_replies(
                    remote_peer_id,
                    &topics,
                    &geneses,
                    responses,
                    owed_acks,
                    more,
                )
            })
            .await;
        let BatchReplies {
            acks,
            followups,
            outcomes: replied,
            owed_acks,
            more,
            unexpected,
        } = match replies {
            Ok(replies) => replies,
            Err(error) => {
                fail_group(outcomes, &error);
                return;
            }
        };
        outcomes.extend(replied);
        if let Some(error) = unexpected {
            fail_group(outcomes, &error);
            return;
        }
        let applied = self
            .run_job(Lane::Control, move |shared| {
                let results = shared.node.apply_sync_acks(&acks);
                (acks, results)
            })
            .await;
        match applied {
            Ok((acks, results)) => {
                for (ack, result) in acks.iter().zip(results) {
                    if let Err(error) = result {
                        outcomes.insert(ack.topic_id, Err(invalid_data(error)));
                    }
                }
            }
            Err(error) => fail_group(outcomes, &error),
        }
        for topic_id in owed_acks {
            outcomes
                .entry(topic_id)
                .or_insert_with(|| Err(invalid_data("peer omitted sync acknowledgement")));
        }
        self.send_followups(peer, remote_peer_id, followups, outcomes)
            .await;

        let goals = goals
            .into_iter()
            .filter(|(topic_id, _)| !outcomes.contains_key(topic_id))
            .collect::<Vec<_>>();
        let goal_topics = goals
            .iter()
            .map(|(topic_id, _)| *topic_id)
            .collect::<Vec<_>>();
        let measured = self
            .run_job(Lane::Control, move |shared| {
                goals
                    .into_iter()
                    .map(|(topic_id, (goal, before))| {
                        let after = shared.goal_progress(remote_peer_id, topic_id, &goal);
                        (topic_id, goal, before, after)
                    })
                    .collect::<Vec<_>>()
            })
            .await;
        let measured = match measured {
            Ok(measured) => measured,
            Err(error) => {
                for topic_id in goal_topics {
                    outcomes.insert(topic_id, Err(clone_error(&error)));
                }
                return;
            }
        };
        for (topic_id, goal, before, after) in measured {
            let outcome = match after {
                Ok(after) if after.reached(&goal) && !more.contains(&topic_id) => Ok(()),
                // A page that moved toward the goal is served again at the fair
                // tail of the queue; it is not a failed attempt.
                Ok(after) if after.advanced_from(&before) => {
                    advanced.insert(topic_id);
                    Ok(())
                }
                Ok(_) => Err(invalid_data(NO_PROGRESS)),
                Err(error) => Err(error),
            };
            outcomes.insert(topic_id, outcome);
        }
    }

    /// Send acks for received pages and data for the peer's requests, one
    /// stream per group, and apply the peer's acks for that data.
    async fn send_followups(
        &self,
        peer: iroh::EndpointAddr,
        remote_peer_id: PeerId,
        followups: BTreeMap<crate::TopicId, Vec<SyncMessage>>,
        outcomes: &mut BTreeMap<crate::TopicId, io::Result<()>>,
    ) {
        let topics = followups
            .iter()
            .filter(|(topic_id, replies)| {
                !matches!(outcomes.get(topic_id), Some(Err(_))) && !replies.is_empty()
            })
            .map(|(topic_id, _)| *topic_id)
            .collect::<Vec<_>>();
        let failed = topics.clone();
        let opens = self
            .run_job(Lane::Control, move |shared| {
                topics
                    .into_iter()
                    .map(|topic_id| (topic_id, shared.node.sync_open(topic_id)))
                    .collect::<BTreeMap<_, _>>()
            })
            .await;
        let mut opens = match opens {
            Ok(opens) => opens,
            Err(error) => {
                for topic_id in failed {
                    outcomes.insert(topic_id, Err(clone_error(&error)));
                }
                return;
            }
        };
        let mut groups: Vec<(BTreeSet<crate::TopicId>, Vec<SyncMessage>)> = Vec::new();
        let mut current_topics = BTreeSet::new();
        let mut current_messages: Vec<SyncMessage> = Vec::new();
        for (topic_id, replies) in followups {
            let Some(open) = opens.remove(&topic_id) else {
                continue;
            };
            let carries_data = |messages: &[SyncMessage]| {
                messages
                    .iter()
                    .any(|message| matches!(message, SyncMessage::Data(_)))
            };
            if !current_messages.is_empty()
                && (current_messages.len() + replies.len() + 1 > self.limits.batch_messages
                    || carries_data(&replies)
                    || carries_data(&current_messages))
            {
                groups.push((
                    std::mem::take(&mut current_topics),
                    std::mem::take(&mut current_messages),
                ));
            }
            current_messages.push(SyncMessage::Open(open));
            current_messages.extend(replies);
            current_topics.insert(topic_id);
        }
        if !current_messages.is_empty() {
            groups.push((current_topics, current_messages));
        }
        for (topics, messages) in groups {
            let mut summaries = topics.clone();
            let mut owed_acks = messages
                .iter()
                .filter_map(|message| match message {
                    SyncMessage::Data(data) => Some(data.topic_id),
                    _ => None,
                })
                .collect::<BTreeSet<_>>();
            let mut acks = Vec::new();
            match self.sync_with(peer.clone(), &messages).await {
                Ok(responses) => {
                    for response in responses {
                        match response {
                            SyncMessage::Summary(summary) if topics.contains(&summary.topic_id) => {
                                summaries.remove(&summary.topic_id);
                            }
                            SyncMessage::Ack(ack) if topics.contains(&ack.topic_id) => {
                                if ack.peer_id != remote_peer_id {
                                    outcomes.insert(
                                        ack.topic_id,
                                        Err(invalid_data("sync ack does not match remote peer")),
                                    );
                                    continue;
                                }
                                owed_acks.remove(&ack.topic_id);
                                acks.push(ack);
                            }
                            SyncMessage::Failure(failure) if topics.contains(&failure.topic_id) => {
                                outcomes.insert(failure.topic_id, Err(topic_failed(&failure)));
                            }
                            other => {
                                let error = invalid_data(format!(
                                    "unexpected sync ack response {}",
                                    _message_type_name(&other)
                                ));
                                for topic_id in &topics {
                                    outcomes.insert(*topic_id, Err(clone_error(&error)));
                                }
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    for topic_id in &topics {
                        outcomes.insert(*topic_id, Err(clone_error(&error)));
                    }
                }
            }
            if !acks.is_empty() {
                let applied = self
                    .run_job(Lane::Control, move |shared| {
                        let results = shared.node.apply_sync_acks(&acks);
                        (acks, results)
                    })
                    .await;
                match applied {
                    Ok((acks, results)) => {
                        for (ack, result) in acks.iter().zip(results) {
                            if let Err(error) = result {
                                outcomes.insert(ack.topic_id, Err(invalid_data(error)));
                            }
                        }
                    }
                    Err(error) => {
                        for topic_id in &topics {
                            outcomes.insert(*topic_id, Err(clone_error(&error)));
                        }
                    }
                }
            }
            for topic_id in summaries {
                outcomes.entry(topic_id).or_insert_with(|| {
                    Err(invalid_data("peer omitted sync acknowledgement summary"))
                });
            }
            for topic_id in owed_acks {
                outcomes
                    .entry(topic_id)
                    .or_insert_with(|| Err(invalid_data("peer omitted sync acknowledgement")));
            }
        }
    }
}

impl<S: Storage> SharedNet<S> {
    /// Fold the responses of one batch stream: admit received data, serve
    /// requested pages and collect acks, per topic.
    fn batch_replies(
        &self,
        remote_peer_id: PeerId,
        group_topics: &BTreeSet<crate::TopicId>,
        geneses: &BTreeMap<crate::TopicId, Option<crate::OpId>>,
        responses: Vec<SyncMessage>,
        mut owed_acks: BTreeSet<crate::TopicId>,
        mut more: BTreeSet<crate::TopicId>,
    ) -> BatchReplies {
        let mut acks = Vec::new();
        let mut followups: BTreeMap<crate::TopicId, Vec<SyncMessage>> = BTreeMap::new();
        let mut pages = BTreeMap::new();
        let mut outcomes = BTreeMap::new();
        for response in responses {
            match response {
                SyncMessage::Ack(ack) if group_topics.contains(&ack.topic_id) => {
                    // An ack that is not validly bound fails its own topic; the
                    // other topics in the stream keep their valid work.
                    if ack.peer_id != remote_peer_id {
                        outcomes.insert(
                            ack.topic_id,
                            Err(invalid_data("sync ack does not match remote peer")),
                        );
                        continue;
                    }
                    owed_acks.remove(&ack.topic_id);
                    self.receipt_log().clear(&(remote_peer_id, ack.topic_id));
                    acks.push(ack);
                }
                // Staging continues on the peer: nothing is certified, but the
                // goal is not reached and the next page goes on from here.
                SyncMessage::Receipt(receipt) if group_topics.contains(&receipt.topic_id) => {
                    owed_acks.remove(&receipt.topic_id);
                    more.insert(receipt.topic_id);
                    if geneses.get(&receipt.topic_id).copied().flatten() == Some(receipt.genesis) {
                        self.receipt_log().record(remote_peer_id, receipt);
                    }
                }
                SyncMessage::Failure(failure) if group_topics.contains(&failure.topic_id) => {
                    outcomes.insert(failure.topic_id, Err(topic_failed(&failure)));
                }
                SyncMessage::Summary(summary) if group_topics.contains(&summary.topic_id) => {}
                SyncMessage::Page(page) if group_topics.contains(&page.topic_id) => {
                    if page.more {
                        more.insert(page.topic_id);
                    }
                }
                SyncMessage::Request(request) if group_topics.contains(&request.topic_id) => {
                    let topic_id = request.topic_id;
                    let budget = crate::sync::PageBudget::from_credit(request.credit);
                    match self.node.response_page(remote_peer_id, &request, budget) {
                        Ok(page) => {
                            if page.more {
                                more.insert(topic_id);
                            }
                            pages.insert(topic_id, page.ops);
                        }
                        Err(error) => {
                            outcomes.insert(topic_id, Err(invalid_data(error)));
                        }
                    }
                }
                SyncMessage::Data(data) if group_topics.contains(&data.topic_id) => {
                    let data_topic_id = data.topic_id;
                    let received = self
                        .node
                        .ensure_iroh_peer_whitelisted(remote_peer_id, &data)
                        .and_then(|()| self.node.receive_sync_outcome(remote_peer_id, data));
                    match received {
                        // A staged page of a pulled topic owes no ack; the next request continues it.
                        Ok(ReceiveOutcome::Staged(_)) => {}
                        Ok(ReceiveOutcome::Acked { ack, evictions }) => {
                            let ack = *ack;
                            self.forward_evictions(evictions);
                            if let Err(error) = self.schedule_topic_recheck(data_topic_id) {
                                tracing::warn!(%data_topic_id, %error, "failed to schedule received topic resync");
                            }
                            let replies = followups.entry(data_topic_id).or_default();
                            // Only the newest frontier needs certifying; earlier
                            // acks of this exchange are covered by it.
                            replies.retain(|message| !matches!(message, SyncMessage::Ack(_)));
                            replies.push(SyncMessage::Ack(ack));
                        }
                        Err(crate::Error::ReceiveCommitted {
                            evictions, source, ..
                        }) => {
                            self.forward_evictions(evictions);
                            self.resync_scheduler
                                .schedule_now(remote_peer_id, data_topic_id, true);
                            outcomes.insert(data_topic_id, Err(invalid_data(source)));
                        }
                        Err(error) => {
                            outcomes.insert(data_topic_id, Err(invalid_data(error)));
                        }
                    }
                }
                other => {
                    let error = invalid_data(format!(
                        "unexpected sync response {}",
                        _message_type_name(&other)
                    ));
                    return BatchReplies {
                        acks,
                        followups,
                        outcomes,
                        owed_acks,
                        more,
                        unexpected: Some(error),
                    };
                }
            }
        }
        // Each follow-up stream carries an open, the topic's ack and its page,
        // so the page is cut to what the stream holds beside the other two.
        for (topic_id, ops) in pages {
            let replies = followups.entry(topic_id).or_default();
            let mut used =
                super::framed_message_len(&SyncMessage::Open(self.node.sync_open(topic_id)));
            for reply in replies.iter() {
                used = used.and_then(|used| Ok(used + super::framed_message_len(reply)?));
            }
            let data = used.and_then(|used| {
                super::sync_data_page(
                    topic_id,
                    ops,
                    self.limits.messages.saturating_sub(replies.len() + 1),
                    self.limits.bytes.saturating_sub(used),
                )
            });
            match data {
                Ok(data) => {
                    if data.cut {
                        more.insert(topic_id);
                    }
                    replies.extend(data.messages);
                }
                Err(error) => {
                    outcomes.insert(topic_id, Err(error));
                }
            }
        }
        BatchReplies {
            acks,
            followups,
            outcomes,
            owed_acks,
            more,
            unexpected: None,
        }
    }

    /// Records each topic's attempt under the identity its claim started
    /// with, then completes the claim. Topics without a claim take a new one.
    fn record_results(
        &self,
        remote_peer_id: PeerId,
        results: Vec<TopicResult>,
        runtime: IrohRuntimeConfig,
    ) {
        for (topic_id, result, advanced, claim) in results {
            // A claim's completion counts once; a topic without one takes a new identity.
            let first = claim.as_ref().is_none_or(|claim| {
                self.resync_scheduler
                    .end_attempt(claim.key(), claim.expect_claim().attempt)
            });
            let attempt =
                self.attempt_identity(claim.as_ref().map(|claim| claim.expect_claim().attempt));
            let outcome = attempt_outcome(result.as_ref().copied(), advanced);
            if let Err(error) =
                self.node
                    .record_attempt_result(remote_peer_id, topic_id, attempt, &outcome, first)
            {
                tracing::warn!(%remote_peer_id, %topic_id, %error, "failed to record sync attempt");
            }
            if let Some(claim) = claim {
                self.finish_resync_attempt(claim, result.as_ref().copied(), runtime, advanced);
            }
        }
    }

    /// `(epoch, sequence)` of an attempt started under scheduler id `attempt`,
    /// or of a new attempt.
    fn attempt_identity(&self, attempt: Option<AttemptId>) -> (u64, u64) {
        let sequence = attempt.or_else(next_attempt_id).map_or(u64::MAX, |id| id.0);
        (self.attempt_epoch, sequence)
    }

    /// How far the topic has come toward `goal`: local positions covered of the
    /// peer's clock, positions the peer certified of the local clock, and holes
    /// left. Only this branch's certified evidence counts.
    fn goal_progress(
        &self,
        peer_id: PeerId,
        topic_id: crate::TopicId,
        goal: &TopicGoal,
    ) -> io::Result<GoalProgress> {
        let Some(view) = self
            .node
            .storage()
            .topic_view(&topic_id, Some(&peer_id))
            .map_err(invalid_data)?
        else {
            if !goal.pull {
                return Err(invalid_data("topic disappeared during sync"));
            }
            // Until promotion a pull moves only by staging more of the peer's history.
            let staged = self.staged_clock(peer_id, topic_id, goal.genesis)?;
            return Ok(GoalProgress {
                staged: covered(&staged, &goal.inbound),
                ..GoalProgress::default()
            });
        };
        if goal.replaces == Some(view.state.genesis) {
            return Ok(GoalProgress::default());
        }
        if goal.genesis != Some(view.state.genesis) {
            return Err(invalid_data("topic branch changed during sync"));
        }
        let certified = view
            .ack
            .as_ref()
            .filter(|ack| ack.genesis == Some(view.state.genesis))
            .map(|ack| ack.clock.clone())
            .unwrap_or_default();
        let staged = self
            .receipt_clock(peer_id, topic_id, view.state.genesis)
            .map_or(0, |clock| covered(&clock, &goal.outbound));
        Ok(GoalProgress {
            inbound: covered(&view.clock, &goal.inbound),
            outbound: covered(&certified, &goal.outbound),
            staged,
            holes: self
                .node
                .view_unresolved(&view)
                .map_err(invalid_data)?
                .len(),
        })
    }

    fn receipt_log(&self) -> std::sync::MutexGuard<'_, ReceiptLog> {
        // The log is only a planning hint, so a poisoned one is still usable.
        self.receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The peer's staged clock for pages planned on `genesis`. A receipt for
    /// pages of a replaced branch says nothing about this one.
    fn receipt_clock(
        &self,
        peer_id: PeerId,
        topic_id: crate::TopicId,
        genesis: crate::OpId,
    ) -> Option<crate::ActorClock> {
        self.receipt_log()
            .clocks
            .get(&(peer_id, topic_id))
            .filter(|receipt| receipt.genesis == genesis)
            .map(|receipt| receipt.clock.clone())
    }
}

impl<S: Storage> IrohNet<S> {
    pub async fn accept_one(&self) -> io::Result<Option<iroh::EndpointId>> {
        let _task = self.tasks.enter()?;
        let Some(incoming) = self.endpoint().accept().await else {
            return Ok(None);
        };
        let connection = tokio::time::timeout(self.runtime.connect_timeout, incoming)
            .await
            .map_err(|_| timed_out("iroh accept timed out"))?
            .map_err(other)?;
        if connection.alpn() != IROKLE_SYNC_ALPN {
            connection.close(0u32.into(), b"unsupported protocol");
            return Err(invalid_data("unsupported sync protocol"));
        }
        let peer = connection.remote_id();
        let (send, recv) =
            tokio::time::timeout(self.runtime.sync_io_timeout, connection.accept_bi())
                .await
                .map_err(|_| timed_out("sync stream accept timed out"))?
                .map_err(other)?;
        self.handle_stream(peer, recv, send).await?;
        Ok(Some(peer))
    }

    /// Serve one inbound stream. The whole request is read before anything is
    /// written, so neither side waits on the other's unread bytes, and the reply
    /// carries every control message plus data within the requesters' credits.
    pub async fn handle_stream(
        &self,
        peer: iroh::EndpointId,
        mut recv: iroh::endpoint::RecvStream,
        mut send: iroh::endpoint::SendStream,
    ) -> io::Result<()> {
        let _task = self.tasks.enter()?;
        tokio::time::timeout(self.runtime.sync_io_timeout, async {
            let mut session = SyncSession::new(peer);
            let mut limits = SyncReadLimits::new(self.limits);
            // The frame's reservation is held until its message was handled.
            while let Some((frame, _reserved)) =
                read_next_frame(&mut recv, self.runtime.sync_io_timeout, Some(&self.inbound))
                    .await?
            {
                let frame_index = limits.observe_frame(frame.len())?;
                let message = decode_sync_message(&frame).map_err(|err| {
                    invalid_data(format!(
                        "invalid sync message frame {frame_index} ({} bytes): {err}",
                        frame.len()
                    ))
                })?;
                // Messages are handled one job at a time, in stream order.
                let lane = match message {
                    SyncMessage::Data(_) | SyncMessage::Summary(_) => Lane::Bulk,
                    _ => Lane::Control,
                };
                let handled;
                (session, handled) = self
                    .run_job(lane, move |shared| {
                        let handled = session.handle(shared, message);
                        (session, handled)
                    })
                    .await?;
                handled?;
            }
            let lane = if session.requests.is_empty() {
                Lane::Control
            } else {
                Lane::Bulk
            };
            let responses = self
                .run_job(lane, move |shared| session.finish(shared))
                .await??;
            let timeout = self.runtime.sync_io_timeout;
            write_sync_messages(&mut send, &responses, timeout, self.limits).await?;
            Ok(())
        })
        .await
        .map_err(|_| timed_out("sync stream timed out"))?
    }
}

impl<S: Storage> SharedNet<S> {
    pub fn handle_messages(
        &self,
        peer: iroh::EndpointId,
        messages: Vec<SyncMessage>,
    ) -> io::Result<Vec<SyncMessage>> {
        let _task = self.tasks.enter()?;
        let mut session = SyncSession::new(peer);
        for message in messages {
            session.handle(self, message)?;
        }
        session.finish(self)
    }

    fn full_sweep_resync_targets(&self) -> io::Result<BTreeSet<(PeerId, crate::TopicId)>> {
        let mut targets = BTreeSet::new();
        for topic in self.node.storage().list_topics().map_err(invalid_data)? {
            let state = match self.node.storage().topic_state(&topic.topic_id) {
                Ok(Some(state)) => state,
                Ok(None) => continue,
                Err(error) => {
                    tracing::warn!(
                        topic_id = %topic.topic_id,
                        %error,
                        "skipping one unreadable topic while planning the sweep"
                    );
                    continue;
                }
            };
            if !state.members.contains(&self.node.peer_id()) {
                match self.dirty_selected_targets(topic.topic_id) {
                    Ok(dirty) => {
                        targets.extend(dirty.into_iter().map(|peer_id| (peer_id, topic.topic_id)))
                    }
                    Err(error) => tracing::warn!(
                        topic_id = %topic.topic_id,
                        %error,
                        "skipping one topic while planning the sweep"
                    ),
                }
                continue;
            }
            targets.extend(
                self.node
                    .sync_peers(topic.topic_id, &state)
                    .into_iter()
                    .map(|peer_id| (peer_id, topic.topic_id)),
            );
        }
        Ok(targets)
    }

    fn handle_message(
        &self,
        message: SyncMessage,
        remote_peer_id: Option<PeerId>,
    ) -> io::Result<Vec<SyncMessage>> {
        match message {
            SyncMessage::Open(open) => {
                let peer_id = remote_peer_id
                    .ok_or_else(|| invalid_data("sync open requires authenticated peer context"))?;
                // Unknown topics return an empty local summary so an inviter can
                // bootstrap a new member by pushing the signed genesis/history.
                // The permission and the summary come from one snapshot.
                let summary = self
                    .node
                    .storage()
                    .read_snapshot(|read| {
                        if let Some(view) = read.topic_view(&open.topic_id, None)?
                            && !peer_may_open_topic(&view.state, peer_id)
                        {
                            return Ok(None);
                        }
                        self.node
                            .sync_engine()
                            .summary_in(read, open.topic_id)
                            .map(Some)
                    })
                    .map_err(invalid_data)?;
                let Some(mut summary) = summary else {
                    return Ok(Vec::new());
                };
                // A topic not held here names what this peer already staged.
                if summary.genesis.is_none()
                    && let Some(staged) = self
                        .node
                        .staged_topic(peer_id, open.topic_id)
                        .map_err(invalid_data)?
                    && let Some(genesis) = staged.genesis
                {
                    summary.staged = Some(crate::sync::SyncReceipt {
                        topic_id: open.topic_id,
                        genesis,
                        session: staged.session,
                        clock: staged.clock,
                    });
                }
                Ok(vec![SyncMessage::Summary(summary)])
            }
            SyncMessage::Fingerprint(fingerprint) => {
                let peer_id = remote_peer_id.ok_or_else(|| {
                    invalid_data("sync fingerprint requires a preceding SyncOpen with peer_id")
                })?;
                let topic_id = fingerprint.topic_id;
                let sync = self.node.sync_engine();
                let compared = self
                    .node
                    .storage()
                    .read_snapshot(|read| {
                        let Some(view) = read.topic_view(&topic_id, None)? else {
                            return Ok(None);
                        };
                        if !peer_may_open_topic(&view.state, peer_id) {
                            return Ok(None);
                        }
                        let local = sync.digest_in(read, &view)?;
                        let whole = self.node.unresolved_in(read, &view)?.is_empty();
                        let member = view.state.members.contains(&peer_id);
                        Ok(Some((
                            local,
                            whole,
                            member,
                            sync.summary_in(read, topic_id)?,
                        )))
                    })
                    .map_err(invalid_data)?;
                let Some((local, whole, member, summary)) = compared else {
                    return Ok(Vec::new());
                };
                // A damaged responder must fall through to the summary path so
                // the requester can serve what this side cannot resolve. The
                // evidence write re-checks the frontier it certifies.
                if local == fingerprint.fingerprint
                    && whole
                    && (!member
                        || self
                            .node
                            .record_fingerprint(peer_id, topic_id, fingerprint.fingerprint)
                            .map_err(invalid_data)?)
                {
                    if member {
                        self.reconsider_target(peer_id, topic_id);
                    }
                    Ok(vec![SyncMessage::Fingerprint(
                        crate::sync::SyncFingerprint {
                            topic_id,
                            fingerprint: local,
                        },
                    )])
                } else {
                    Ok(vec![SyncMessage::Summary(summary)])
                }
            }
            SyncMessage::Summary(summary) => {
                let peer_id = remote_peer_id.ok_or_else(|| {
                    invalid_data("sync summary requires a preceding SyncOpen with peer_id")
                })?;
                // A summary only yields a request: data the peer lacks is served
                // against its own request, so nothing is sent twice.
                let request = self
                    .node
                    .plan_sync_request(peer_id, &summary)
                    .map_err(invalid_data)?;
                if request.wants.is_empty() && request.actor_range_hints.is_empty() {
                    return Ok(Vec::new());
                }
                Ok(vec![SyncMessage::Request(request)])
            }
            SyncMessage::Request(_) => {
                Err(invalid_data("sync request must be served by the session"))
            }
            SyncMessage::Data(data) => {
                let data_topic_id = data.topic_id;
                let source_peer = remote_peer_id.ok_or_else(|| {
                    invalid_data("sync data requires a preceding SyncOpen with peer_id")
                })?;
                self.node
                    .ensure_iroh_peer_whitelisted(source_peer, &data)
                    .map_err(invalid_data)?;
                let outcome = self
                    .node
                    .receive_sync_outcome(source_peer, data)
                    .map_err(|mut error| {
                        if let crate::Error::ReceiveCommitted { evictions, .. } = &mut error {
                            self.forward_evictions(std::mem::take(evictions));
                            if let Err(retry) = self.schedule_topic_recheck(data_topic_id) {
                                tracing::warn!(%data_topic_id, %retry, "failed to schedule received topic resync");
                            }
                        }
                        invalid_data(error)
                    })?;
                let (ack, evictions) = match outcome {
                    ReceiveOutcome::Acked { ack, evictions } => (*ack, evictions),
                    ReceiveOutcome::Staged(staged) => {
                        // Data no staged branch anchors fails its topic visibly.
                        let genesis = staged
                            .genesis
                            .ok_or_else(|| invalid_data("sync data names no staged branch"))?;
                        return Ok(vec![SyncMessage::Receipt(crate::sync::SyncReceipt {
                            topic_id: data_topic_id,
                            genesis,
                            session: staged.session,
                            clock: staged.clock,
                        })]);
                    }
                };
                self.forward_evictions(evictions);
                if let Err(error) = self.schedule_topic_recheck(data_topic_id) {
                    tracing::warn!(%data_topic_id, %error, "failed to schedule received topic resync");
                }
                Ok(vec![SyncMessage::Ack(ack)])
            }
            // Acks are collected by the session and applied together in
            // `SyncSession::finish`, so one rejected ack cannot discard the
            // rest; there is deliberately no second path that applies one.
            SyncMessage::Ack(_) => Err(invalid_data("sync ack must be applied by the session")),
            SyncMessage::Failure(_) | SyncMessage::Page(_) | SyncMessage::Receipt(_) => Err(
                invalid_data("sync failure, page and receipt are response-only messages"),
            ),
        }
    }
}

/// The failure a sync message reports when its handling must be contained to
/// one topic instead of aborting the stream. Every message that names a topic
/// and does real work belongs here; the session validates the protocol and the
/// peer binding before this point, so those failures still indict the peer.
fn per_topic_failure_scope(message: &SyncMessage) -> Option<crate::sync::SyncFailure> {
    let (topic_id, code) = match message {
        SyncMessage::Open(open) => (open.topic_id, crate::sync::SyncFailureCode::Open),
        SyncMessage::Fingerprint(fingerprint) => (
            fingerprint.topic_id,
            crate::sync::SyncFailureCode::Fingerprint,
        ),
        SyncMessage::Summary(summary) => (summary.topic_id, crate::sync::SyncFailureCode::Summary),
        SyncMessage::Request(request) => (request.topic_id, crate::sync::SyncFailureCode::Request),
        SyncMessage::Data(data) => (data.topic_id, crate::sync::SyncFailureCode::Data),
        SyncMessage::Ack(_)
        | SyncMessage::Failure(_)
        | SyncMessage::Page(_)
        | SyncMessage::Receipt(_) => return None,
    };
    Some(crate::sync::SyncFailure { topic_id, code })
}

/// Per-topic results of one batch, with the topics that made durable progress
/// but still owe more work. Advancing pages are kept apart from failures so a
/// catch-up that is working is not retried as an unreachable peer.
struct BatchOutcomes {
    results: BTreeMap<crate::TopicId, io::Result<()>>,
    advanced: BTreeSet<crate::TopicId>,
    /// Topics already recorded and released during the batch, so the caller
    /// does not publish them a second time.
    settled: BTreeSet<crate::TopicId>,
}

impl BatchOutcomes {
    fn new(
        results: BTreeMap<crate::TopicId, io::Result<()>>,
        advanced: BTreeSet<crate::TopicId>,
        settled: BTreeSet<crate::TopicId>,
    ) -> Self {
        Self {
            results,
            advanced,
            settled,
        }
    }
}

/// What the responses to one batch stream settled.
struct BatchReplies {
    acks: Vec<crate::sync::SyncAck>,
    followups: BTreeMap<crate::TopicId, Vec<SyncMessage>>,
    outcomes: BTreeMap<crate::TopicId, io::Result<()>>,
    owed_acks: BTreeSet<crate::TopicId>,
    more: BTreeSet<crate::TopicId>,
    /// A response outside the protocol, which fails the whole group.
    unexpected: Option<io::Error>,
}

/// One group of planned topics that fits a stream, and where planning stopped.
struct PlannedGroup {
    group: Vec<PlannedTopicSync>,
    /// A planned topic that did not fit, first in the next group.
    next: Option<PlannedTopicSync>,
    rest: VecDeque<(crate::TopicId, SyncSummary)>,
    outcomes: Vec<(crate::TopicId, io::Result<()>)>,
}

/// What one topic plan read from a single snapshot.
struct SnapshotPlan {
    state: crate::storage::TopicState,
    clock: crate::ActorClock,
    plan: crate::sync::SyncPlan,
    push_more: bool,
    /// The summary matches this branch's whole frontier, pending the write.
    converged: bool,
    /// This node's leave page and its position, when it left the topic.
    leave: Option<(crate::sync::PlannedPage, Option<(crate::ActorId, u64)>)>,
    local_summary: Option<SyncSummary>,
}

struct PlannedTopicSync {
    topic_id: crate::TopicId,
    goal: TopicGoal,
    /// Whether the stream carries data the peer must acknowledge.
    pushes: bool,
    /// Whether the push page left data behind for a later page.
    push_more: bool,
    messages: Vec<SyncMessage>,
    estimated_responses: usize,
}

/// What one attempt set out to reach, captured when it was planned. Later
/// appends on either side are later work, not a moving target.
#[derive(Clone, Debug)]
struct TopicGoal {
    /// Whether the topic was not held locally when planned.
    pull: bool,
    genesis: Option<crate::OpId>,
    /// The losing local genesis a branch pull replaces; until the winner is
    /// admitted the topic has made no progress toward the goal.
    replaces: Option<crate::OpId>,
    /// The peer's clock from its summary.
    inbound: crate::ActorClock,
    /// The local clock the peer should certify.
    outbound: crate::ActorClock,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct GoalProgress {
    inbound: u64,
    outbound: u64,
    holes: usize,
    /// Staged positions: the peer's receipts for a push, or this node's own
    /// staging for a pull.
    staged: u64,
}

impl GoalProgress {
    fn reached(&self, goal: &TopicGoal) -> bool {
        self.inbound == covered(&goal.inbound, &goal.inbound)
            && self.outbound == covered(&goal.outbound, &goal.outbound)
            && self.holes == 0
    }

    fn advanced_from(&self, before: &GoalProgress) -> bool {
        self.inbound > before.inbound
            || self.outbound > before.outbound
            || self.holes < before.holes
            || self.staged > before.staged
    }
}

/// Positions of `target` that `clock` covers, summed over actors.
fn covered(clock: &crate::ActorClock, target: &crate::ActorClock) -> u64 {
    target
        .iter()
        .map(|(actor_id, seq)| clock.get(actor_id).min(*seq))
        .sum()
}

struct SyncSession {
    authenticated_peer_id: PeerId,
    remote_peer_id: Option<PeerId>,
    open_topic_id: Option<crate::TopicId>,
    open_allowed: bool,
    acks: Vec<crate::sync::SyncAck>,
    controls: Vec<SyncMessage>,
    /// One ack per topic that received data, covering every message of it.
    replies: BTreeMap<crate::TopicId, crate::sync::SyncAck>,
    /// Newest staging receipt per topic that has no ack in this stream.
    receipts: BTreeMap<crate::TopicId, crate::sync::SyncReceipt>,
    /// Requests to serve once the whole stream is read, latest per topic.
    requests: BTreeMap<crate::TopicId, crate::sync::SyncRequest>,
}

impl SyncSession {
    fn new(peer: iroh::EndpointId) -> Self {
        Self {
            authenticated_peer_id: peer_id_from_endpoint_id(peer),
            remote_peer_id: None,
            open_topic_id: None,
            open_allowed: false,
            acks: Vec::new(),
            controls: Vec::new(),
            replies: BTreeMap::new(),
            receipts: BTreeMap::new(),
            requests: BTreeMap::new(),
        }
    }

    fn handle<S: Storage>(&mut self, net: &SharedNet<S>, message: SyncMessage) -> io::Result<()> {
        if let SyncMessage::Open(open) = &message {
            if open.protocol.as_bytes() != IROKLE_SYNC_ALPN {
                return Err(invalid_data("unsupported sync protocol"));
            }
            if open.peer_id != self.authenticated_peer_id {
                return Err(invalid_data(
                    "sync open peer_id does not match iroh endpoint id",
                ));
            }
            self.remote_peer_id = Some(open.peer_id);
            self.open_topic_id = Some(open.topic_id);
            self.open_allowed = false;
            let allowed = match net.node.storage().topic_state(&open.topic_id) {
                Ok(state) => state.is_none_or(|state| peer_may_open_topic(&state, open.peer_id)),
                Err(error) => {
                    tracing::warn!(topic_id = %open.topic_id, %error, "failed to authorize sync topic");
                    false
                }
            };
            // Deny silently, like the non-member path in handle_message, rather
            // than replying with a failure code.
            if !allowed {
                return Ok(());
            }
            self.open_allowed = true;
        } else {
            if self.remote_peer_id.is_none() {
                return Err(invalid_data(
                    "sync message requires a preceding SyncOpen with peer_id",
                ));
            }
            if let Some(topic_id) = message_topic_id(&message)
                && self.open_topic_id != Some(topic_id)
            {
                return Err(invalid_data(
                    "sync message topic does not match SyncOpen topic",
                ));
            }
        }

        if !self.open_allowed {
            self.controls
                .push(SyncMessage::Failure(crate::sync::SyncFailure {
                    topic_id: message_topic_id(&message)
                        .ok_or_else(|| invalid_data("sync message requires a topic"))?,
                    code: crate::sync::SyncFailureCode::Open,
                }));
            return Ok(());
        }

        match message {
            SyncMessage::Data(data) if data.ops.len() > MAX_SYNC_DATA_OPS_PER_MESSAGE => {
                Err(invalid_data("sync data has too many operations"))
            }
            SyncMessage::Ack(ack) => {
                self.acks.push(ack);
                Ok(())
            }
            SyncMessage::Request(request) => {
                self.requests.insert(request.topic_id, request);
                Ok(())
            }
            SyncMessage::Page(_) | SyncMessage::Receipt(_) => Err(invalid_data(
                "sync page and receipt are response-only messages",
            )),
            message => {
                // A data-plane failure fails only its topic, explicitly, so the
                // other topics batched into the stream keep their replies.
                // Framing and authentication failures above stay fatal.
                let failure = per_topic_failure_scope(&message);
                match net.handle_message(message, self.remote_peer_id) {
                    Ok(responses) => {
                        for response in responses {
                            self.keep_reply(net, response)?;
                        }
                        Ok(())
                    }
                    Err(error) => {
                        let failure = failure.ok_or(error)?;
                        tracing::warn!(topic_id = %failure.topic_id, "failing one sync topic");
                        self.controls.push(SyncMessage::Failure(failure));
                        Ok(())
                    }
                }
            }
        }
    }

    /// Queue one reply, folding acks and receipts of a topic into the newest.
    fn keep_reply<S: Storage>(&mut self, net: &SharedNet<S>, reply: SyncMessage) -> io::Result<()> {
        let mut ack = match reply {
            SyncMessage::Ack(ack) => ack,
            SyncMessage::Receipt(receipt) => {
                self.receipts.insert(receipt.topic_id, receipt);
                return Ok(());
            }
            reply => {
                self.controls.push(reply);
                return Ok(());
            }
        };
        // Promotion supersedes the staging this stream reported before.
        self.receipts.remove(&ack.topic_id);
        if let Some(earlier) = self.replies.remove(&ack.topic_id) {
            ack.accepted.extend(earlier.accepted);
            ack.sign(net.node.signer()).map_err(invalid_data)?;
        }
        self.replies.insert(ack.topic_id, ack);
        Ok(())
    }

    /// Apply the stream's acks independently, then reply: every control first,
    /// then one bounded page per request, sharing what is left of the stream
    /// budget among the requests still to serve.
    fn finish<S: Storage>(&mut self, net: &SharedNet<S>) -> io::Result<Vec<SyncMessage>> {
        let mut responses = std::mem::take(&mut self.controls);
        responses.extend(self.apply_acks(net)?);
        responses.extend(
            std::mem::take(&mut self.replies)
                .into_values()
                .map(SyncMessage::Ack),
        );
        responses.extend(
            std::mem::take(&mut self.receipts)
                .into_values()
                .map(SyncMessage::Receipt),
        );
        let requests = std::mem::take(&mut self.requests);
        let Some(peer_id) = self.remote_peer_id else {
            return Ok(responses);
        };
        let page_len = super::framed_message_len(&SyncMessage::Page(crate::sync::SyncPage {
            topic_id: crate::TopicId::default(),
            more: false,
            missing: BTreeSet::new(),
        }))?;
        let mut bytes = requests.len() * page_len;
        for response in &responses {
            bytes += super::framed_message_len(response)?;
        }
        let mut messages = responses.len() + requests.len();
        let limits = net.limits;
        if bytes > limits.bytes || messages > limits.messages {
            return Err(invalid_data("sync reply controls exceed the stream budget"));
        }
        // Data is sized in framed wire bytes against what the controls and one
        // page result per request left. A request whose next op does not fit
        // its share is served again from what every other request left over.
        let mut left = requests.len();
        let mut queue = requests.into_iter().collect::<Vec<_>>();
        let mut deferred = Vec::new();
        for pass in [false, true] {
            if pass {
                left = deferred.len();
                queue = std::mem::take(&mut deferred);
            }
            for (topic_id, request) in std::mem::take(&mut queue) {
                let share_bytes = (limits.bytes - bytes) / left;
                let share_messages = (limits.messages - messages) / left;
                left -= 1;
                let mut budget = crate::sync::PageBudget::from_credit(request.credit);
                budget.bytes = budget.bytes.min(share_bytes);
                budget.ops = budget
                    .ops
                    .min(share_messages.saturating_mul(MAX_SYNC_DATA_OPS_PER_MESSAGE));
                let page = match net.node.response_page(peer_id, &request, budget) {
                    Ok(page) => page,
                    Err(error) => {
                        tracing::warn!(%topic_id, %error, "failing one sync request");
                        responses.push(SyncMessage::Failure(crate::sync::SyncFailure {
                            topic_id,
                            code: crate::sync::SyncFailureCode::Request,
                        }));
                        continue;
                    }
                };
                let data = super::sync_data_page(topic_id, page.ops, share_messages, share_bytes)?;
                let more = page.more || data.cut;
                if !pass && data.messages.is_empty() && more && page.missing.is_empty() {
                    deferred.push((topic_id, request));
                    continue;
                }
                bytes += data.bytes;
                messages += data.messages.len();
                responses.extend(data.messages);
                // Missing ids are advisory: they take only bytes no share needs.
                let mut result = crate::sync::SyncPage {
                    topic_id,
                    more,
                    missing: page.missing,
                };
                let mut extra =
                    super::framed_message_len(&SyncMessage::Page(result.clone()))? - page_len;
                while extra > limits.bytes - bytes {
                    result.missing.pop_last();
                    extra =
                        super::framed_message_len(&SyncMessage::Page(result.clone()))? - page_len;
                }
                bytes += extra;
                responses.push(SyncMessage::Page(result));
            }
        }
        reply_fits(&responses, limits)?;
        Ok(responses)
    }

    /// One rejected ack, a stale clock after a reset or one bound to another
    /// peer, must not discard the others. Each rejection names its own topic.
    fn apply_acks<S: Storage>(&mut self, net: &SharedNet<S>) -> io::Result<Vec<SyncMessage>> {
        let acks = std::mem::take(&mut self.acks);
        if acks.is_empty() {
            return Ok(Vec::new());
        }
        let peer_id = self
            .remote_peer_id
            .ok_or_else(|| invalid_data("sync ack requires a preceding SyncOpen with peer_id"))?;
        let mut responses = Vec::new();
        let mut bound = Vec::new();
        for ack in acks {
            if ack.peer_id == peer_id {
                bound.push(ack);
                continue;
            }
            let topic_id = ack.topic_id;
            tracing::warn!(%topic_id, "dropping sync ack bound to another peer");
            responses.push(SyncMessage::Failure(crate::sync::SyncFailure {
                topic_id,
                code: crate::sync::SyncFailureCode::Ack,
            }));
        }
        for (ack, result) in bound.iter().zip(net.node.apply_sync_acks(&bound)) {
            match result {
                Ok(()) => {
                    net.receipt_log().clear(&(peer_id, ack.topic_id));
                    net.reconsider_target(peer_id, ack.topic_id);
                }
                Err(error) => {
                    let topic_id = ack.topic_id;
                    tracing::warn!(%topic_id, %error, "skipping rejected sync ack");
                    responses.push(SyncMessage::Failure(crate::sync::SyncFailure {
                        topic_id,
                        code: crate::sync::SyncFailureCode::Ack,
                    }));
                }
            }
        }
        Ok(responses)
    }
}

/// Refuses a reply the stream writer would refuse, before any of it is queued.
fn reply_fits(messages: &[SyncMessage], stream_limits: StreamLimits) -> io::Result<()> {
    let mut limits = SyncReadLimits::new(stream_limits);
    for message in messages {
        limits.observe_frame(super::framed_message_len(message)? - 4)?;
    }
    Ok(())
}

struct SyncReadLimits {
    limits: StreamLimits,
    messages: usize,
    bytes: usize,
}

impl SyncReadLimits {
    fn new(limits: StreamLimits) -> Self {
        Self {
            limits,
            messages: 0,
            bytes: 0,
        }
    }

    fn observe_frame(&mut self, frame_len: usize) -> io::Result<usize> {
        if self.messages >= self.limits.messages {
            return Err(invalid_data("sync stream has too many messages"));
        }
        self.bytes = self
            .bytes
            .checked_add(frame_len + 4)
            .ok_or_else(|| invalid_data("sync stream byte count overflow"))?;
        if self.bytes > self.limits.bytes {
            return Err(invalid_data("sync stream exceeds maximum byte length"));
        }
        let frame_index = self.messages;
        self.messages += 1;
        Ok(frame_index)
    }
}

/// One outbound peer slot held by a manual sync. Releasing it wakes the resync
/// loop, which may have parked with every slot taken.
struct OutboundSlot {
    _permit: tokio::sync::OwnedSemaphorePermit,
    wake: Arc<tokio::sync::Notify>,
}

impl Drop for OutboundSlot {
    fn drop(&mut self) {
        self.wake.notify_one();
    }
}

/// Clears a loop's start latch when the loop task actually ends, including on
/// abort, so a replacement loop can be started.
struct LoopGuard<S: Storage> {
    net: Weak<IrohNet<S>>,
    latch: fn(&IrohNet<S>) -> &AtomicBool,
}

impl<S: Storage> Drop for LoopGuard<S> {
    fn drop(&mut self) {
        if let Some(current) = self.net.upgrade() {
            (self.latch)(&current).store(false, Ordering::SeqCst);
        }
    }
}

/// The loop's next wake deadline. With every slot taken there is nothing to
/// dispatch, so no due deadline is armed and an expired one cannot spin.
fn next_resync_wake(scheduler: &ResyncScheduler, in_flight: usize) -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    if in_flight >= MAX_RESYNC_PEER_CONCURRENCY {
        return now + EMPTY_RESYNC_SLEEP;
    }
    scheduler.next_due().unwrap_or(now + EMPTY_RESYNC_SLEEP)
}

/// Fills the free peer slots from the due queue and returns false when the loop
/// must stop. Claims are taken here, one turn per peer, so a slow peer cannot
/// hold capacity another due peer could use.
fn dispatch_due_resyncs<S: Storage>(
    net: &Weak<IrohNet<S>>,
    syncs: &mut tokio::task::JoinSet<()>,
    runtime: IrohRuntimeConfig,
) -> bool {
    let Some(current) = net.upgrade() else {
        return false;
    };
    if current.is_shutdown() || current.endpoint().is_closed() {
        return false;
    }
    // Slots are taken before targets are claimed, so manual syncs holding
    // slots leave nothing claimed that cannot run.
    let mut slots = Vec::new();
    while slots.len() < MAX_RESYNC_PEER_CONCURRENCY.saturating_sub(syncs.len()) {
        match Arc::clone(&current.outbound).try_acquire_owned() {
            Ok(slot) => slots.push(slot),
            Err(_) => break,
        }
    }
    if slots.is_empty() {
        return true;
    }
    let due = current
        .resync_scheduler
        .due_targets_by_peer(slots.len(), MAX_TOPICS_PER_RESYNC_BATCH);
    for ((peer_id, targets), slot) in due.into_iter().zip(slots) {
        // The lease owns the claims before the task is spawned, so an abort
        // releases them instead of wedging the targets in flight.
        let lease = current
            .resync_scheduler
            .lease(targets, runtime.resync_interval);
        let peer_net = Arc::clone(&current);
        let task = current.tasks.track();
        syncs.spawn(async move {
            let _task = task;
            let _slot = slot;
            peer_net
                .sync_peer_batch_with_runtime(peer_id, lease, runtime)
                .await;
        });
    }
    true
}

fn next_full_sweep_deadline(interval: Duration, time_of_day: Duration) -> tokio::time::Instant {
    if interval.is_zero() {
        return tokio::time::Instant::now() + EMPTY_RESYNC_SLEEP;
    }
    tokio::time::Instant::now() + initial_full_sweep_delay(interval, time_of_day)
}

fn initial_full_sweep_delay(interval: Duration, time_of_day: Duration) -> Duration {
    if interval < Duration::from_secs(SECONDS_PER_DAY) {
        return interval;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let current_day_second = now % SECONDS_PER_DAY;
    let target_day_second = time_of_day.as_secs() % SECONDS_PER_DAY;
    let delay_secs = if current_day_second < target_day_second {
        target_day_second - current_day_second
    } else {
        SECONDS_PER_DAY - current_day_second + target_day_second
    };
    Duration::from_secs(delay_secs)
}

fn peer_may_open_topic(state: &crate::storage::TopicState, peer_id: PeerId) -> bool {
    state.members.contains(&peer_id)
        || state
            .membership_controls
            .get(&peer_id)
            .is_some_and(|(_, is_member)| !*is_member)
}

async fn handle_connection<S: Storage>(
    net: Weak<IrohNet<S>>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    peer: iroh::EndpointId,
    connection: iroh::endpoint::Connection,
) {
    let Some(current) = net.upgrade() else {
        return;
    };
    let idle_timeout = current.runtime.sync_io_timeout;
    drop(current);
    let mut tasks = tokio::task::JoinSet::new();
    let idle = tokio::time::sleep(idle_timeout);
    tokio::pin!(idle);
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            _ = &mut idle, if tasks.is_empty() => break,
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(error) = result {
                    tracing::warn!(%peer, %error, "iroh sync stream task failed");
                }
                idle.as_mut().reset(tokio::time::Instant::now() + idle_timeout);
            }
            streams = connection.accept_bi(), if tasks.len() < 8 => {
                let (send, recv) = match streams {
                    Ok(streams) => streams,
                    Err(error) => {
                        tracing::debug!(%peer, %error, "iroh connection stopped accepting streams");
                        break;
                    }
                };
                let Some(current) = net.upgrade() else {
                    break;
                };
                if current.is_shutdown() {
                    break;
                }
                let task = current.tasks.track();
                tasks.spawn(async move {
                    let _task = task;
                    if let Err(error) = current.handle_stream(peer, recv, send).await {
                        tracing::warn!(%peer, %error, "failed to handle iroh sync stream");
                    } else if let Err(error) = current
                        .run_job(Lane::Control, move |shared| {
                            shared.note_peer_reachable(peer_id_from_endpoint_id(peer));
                        })
                        .await
                    {
                        tracing::warn!(%peer, %error, "failed to record a reachable peer");
                    }
                });
            }
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    connection.close(0u32.into(), b"sync connection idle or closed");
}

impl<S: Storage> Drop for IrohNet<S> {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

async fn read_sync_messages(
    recv: &mut iroh::endpoint::RecvStream,
    sync_io_timeout: Duration,
    stream_limits: StreamLimits,
) -> io::Result<Vec<SyncMessage>> {
    let mut messages = Vec::new();
    let mut limits = SyncReadLimits::new(stream_limits);
    while let Some((frame, _)) = read_next_frame(recv, sync_io_timeout, None).await? {
        let frame_index = limits.observe_frame(frame.len())?;
        messages.push(decode_sync_message(&frame).map_err(|err| {
            invalid_data(format!(
                "invalid sync message frame {frame_index} ({} bytes): {err}",
                frame.len()
            ))
        })?);
    }
    Ok(messages)
}

async fn write_sync_messages(
    send: &mut iroh::endpoint::SendStream,
    messages: &[SyncMessage],
    sync_io_timeout: Duration,
    stream_limits: StreamLimits,
) -> io::Result<()> {
    reply_fits(messages, stream_limits)?;
    for message in messages {
        let payload = encode_sync_message(message)?;
        let frame = encode_frame(&payload)?;
        tokio::time::timeout(sync_io_timeout, send.write_all(&frame))
            .await
            .map_err(|_| timed_out("sync write timed out"))?
            .map_err(other)?;
    }
    send.finish().map_err(other)
}

/// Reads one frame. With a `budget`, its bytes are reserved before the frame is
/// allocated and the reservation is returned with it.
async fn read_next_frame(
    recv: &mut iroh::endpoint::RecvStream,
    sync_io_timeout: Duration,
    budget: Option<&InboundBudget>,
) -> io::Result<Option<(Vec<u8>, Option<tokio::sync::OwnedSemaphorePermit>)>> {
    let mut len_buf = [0_u8; 4];
    let Some(first_read) = read_some_with_timeout(recv, &mut len_buf[..1], sync_io_timeout).await?
    else {
        return Ok(None);
    };
    if first_read == 0 {
        return Ok(None);
    }

    let mut read = first_read;
    while read < len_buf.len() {
        let Some(n) = read_some_with_timeout(recv, &mut len_buf[read..], sync_io_timeout).await?
        else {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete sync frame length",
            ));
        };
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete sync frame length",
            ));
        }
        read += n;
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "sync frame exceeds maximum length",
        ));
    }
    let reservation = match budget {
        Some(budget) if len > 0 => Some(budget.reserve(len).await?),
        _ => None,
    };
    let mut payload = vec![0_u8; len];
    if len > 0 {
        tokio::time::timeout(sync_io_timeout, recv.read_exact(&mut payload))
            .await
            .map_err(|_| timed_out("sync read timed out"))?
            .map_err(other)?;
    }
    Ok(Some((payload, reservation)))
}

async fn read_some_with_timeout(
    recv: &mut iroh::endpoint::RecvStream,
    buf: &mut [u8],
    sync_io_timeout: Duration,
) -> io::Result<Option<usize>> {
    tokio::time::timeout(sync_io_timeout, recv.read(buf))
        .await
        .map_err(|_| timed_out("sync read timed out"))?
        .map_err(other)
}

fn peer_id_from_endpoint_id(peer: iroh::EndpointId) -> PeerId {
    PeerId::from_bytes(*peer.as_bytes())
}

fn peer_id_to_endpoint_addr(peer_id: PeerId) -> io::Result<iroh::EndpointAddr> {
    Ok(iroh::EndpointAddr::from(
        iroh::EndpointId::from_bytes(peer_id.as_bytes()).map_err(invalid_data)?,
    ))
}

fn extend_alpns(mut alpns: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    let irokle = IROKLE_SYNC_ALPN.to_vec();
    if !alpns.contains(&irokle) {
        alpns.push(irokle);
    }
    alpns
}

fn message_topic_id(message: &SyncMessage) -> Option<crate::TopicId> {
    match message {
        SyncMessage::Open(open) => Some(open.topic_id),
        SyncMessage::Fingerprint(fingerprint) => Some(fingerprint.topic_id),
        SyncMessage::Summary(summary) => Some(summary.topic_id),
        SyncMessage::Request(request) => Some(request.topic_id),
        SyncMessage::Data(data) => Some(data.topic_id),
        SyncMessage::Ack(ack) => Some(ack.topic_id),
        SyncMessage::Failure(failure) => Some(failure.topic_id),
        SyncMessage::Page(page) => Some(page.topic_id),
        SyncMessage::Receipt(receipt) => Some(receipt.topic_id),
    }
}

/// One topic's result, whether it advanced, and the claim held for it.
type TopicResult = (crate::TopicId, io::Result<()>, bool, Option<ClaimGuard>);

/// The typed outcome of one topic attempt: an exchange that stopped without
/// moving toward its goal is blocked, any other error failed it.
fn attempt_outcome(
    result: std::result::Result<(), &io::Error>,
    advanced: bool,
) -> crate::AttemptOutcome {
    match result {
        Ok(()) if advanced => crate::AttemptOutcome::Advanced,
        Ok(()) => crate::AttemptOutcome::Complete,
        Err(error)
            if error.kind() == io::ErrorKind::InvalidData && error.to_string() == NO_PROGRESS =>
        {
            crate::AttemptOutcome::Blocked(error.to_string())
        }
        Err(error) => crate::AttemptOutcome::Failed(error.to_string()),
    }
}

fn copy_result(result: &io::Result<()>) -> io::Result<()> {
    result.as_ref().copied().map_err(clone_error)
}

fn topic_failed(failure: &crate::sync::SyncFailure) -> io::Error {
    invalid_data(format!("peer failed this topic at {:?}", failure.code))
}

fn timed_out(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, message)
}

fn clone_error(error: &io::Error) -> io::Error {
    io::Error::new(error.kind(), error.to_string())
}

fn other(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

#[cfg(test)]
#[path = "../tests/scheduler.rs"]
mod scheduler_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TopicId;

    /// Wide enough that the second backoff step cannot be mistaken for the
    /// first on a slow machine.
    const BACKOFF: Duration = Duration::from_secs(60);

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Ping;

    impl crate::Event for Ping {
        const TYPE_ID: &'static str = "test.ping";
    }

    fn peer(byte: u8) -> PeerId {
        PeerId::from_bytes([byte; 32])
    }

    fn topic(byte: u8) -> TopicId {
        TopicId::from_bytes([byte; 32])
    }

    #[test]
    fn scheduler_deduplicates_targets() {
        let scheduler = ResyncScheduler::default();
        scheduler.schedule_now(peer(1), topic(2), false);
        scheduler.schedule_now(peer(1), topic(2), false);

        let due = scheduler.due_targets_by_peer(8, 8);

        assert_eq!(due.len(), 1);
        let (peer_id, targets) = &due[0];
        assert_eq!(*peer_id, peer(1));
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].key.topic_id, topic(2));
        scheduler.complete_clean(targets[0]);
        assert!(scheduler.next_due().is_none());
    }

    #[test]
    fn scheduler_groups_due_targets_by_peer() {
        let scheduler = ResyncScheduler::default();
        scheduler.schedule_now(peer(1), topic(1), false);
        scheduler.schedule_now(peer(1), topic(2), false);
        scheduler.schedule_now(peer(1), topic(3), false);
        scheduler.schedule_now(peer(2), topic(1), false);

        let due = scheduler.due_targets_by_peer(8, 2);

        assert_eq!(due.len(), 2);
        let (first_peer, first_targets) = &due[0];
        assert_eq!(*first_peer, peer(1));
        assert_eq!(first_targets.len(), 2);
        let (second_peer, second_targets) = &due[1];
        assert_eq!(*second_peer, peer(2));
        assert_eq!(second_targets.len(), 1);

        // Targets handed out stay in flight until completed, and a peer taking
        // its turn is not given a second one.
        assert!(scheduler.due_targets_by_peer(8, 8).is_empty());
    }

    #[test]
    fn scheduler_uses_capped_failure_backoff() {
        let scheduler = ResyncScheduler::default();
        let peer_id = peer(3);
        let topic_id = topic(4);
        scheduler.schedule_now(peer_id, topic_id, false);
        let mut due = scheduler.due_targets_by_peer(8, 8);
        assert_eq!(due.len(), 1);

        scheduler.complete_failed(
            due.remove(0).1[0],
            Duration::from_secs(1),
            Duration::from_secs(600),
        );
        let first_delay = scheduler
            .next_due()
            .unwrap()
            .saturating_duration_since(tokio::time::Instant::now());
        assert!(first_delay <= Duration::from_secs(1));

        for _ in 0..16 {
            // Each failure needs its own claim: a completion must own one.
            scheduler.schedule_now(peer_id, topic_id, true);
            let mut due = scheduler.due_targets_by_peer(8, 8);
            assert_eq!(due.len(), 1);
            scheduler.complete_failed(
                due.remove(0).1[0],
                Duration::from_secs(1),
                Duration::from_secs(600),
            );
        }
        let capped_delay = scheduler
            .next_due()
            .unwrap()
            .saturating_duration_since(tokio::time::Instant::now());
        assert!(capped_delay <= Duration::from_secs(600));
    }

    /// A claim for the single due target of one peer.
    fn one_claim(scheduler: &ResyncScheduler) -> ResyncTarget {
        let mut due = scheduler.due_targets_by_peer(8, 8);
        assert_eq!(due.len(), 1);
        let targets = due.remove(0).1;
        assert_eq!(targets.len(), 1);
        targets[0]
    }

    /// A panic between taking a claim and recording its result must not leave
    /// the target owned by nobody: the guard hands the claim back on unwind, so
    /// the target becomes dispatchable again.
    #[test]
    fn panic_returns_claim() {
        let scheduler = ResyncScheduler::default();
        scheduler.schedule_now(peer(71), topic(72), false);
        let claim = one_claim(&scheduler);
        let mut lease = scheduler.lease(vec![claim], BACKOFF);
        let key = ResyncTargetKey {
            peer_id: peer(71),
            topic_id: topic(72),
        };

        let taken = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let guard = lease.take_claim(&key).expect("claim");
            assert_eq!(guard.key(), key);
            panic!("result recording failed");
        }));
        assert!(taken.is_err(), "the work between take and settle panicked");

        let (active, _, _) = scheduler
            .target_state(peer(71), topic(72))
            .expect("the target must survive");
        assert!(
            active.is_none(),
            "an orphan claim would leave the target owned forever"
        );
        // It is dispatchable again rather than stuck in flight.
        assert!(scheduler.next_due().is_some());
    }

    /// A drained collection releases every claim it still holds when the task
    /// finishing them unwinds part way through.
    #[test]
    fn panic_returns_drained() {
        let scheduler = ResyncScheduler::default();
        scheduler.schedule_now(peer(73), topic(74), false);
        scheduler.schedule_now(peer(73), topic(75), false);
        let mut due = scheduler.due_targets_by_peer(8, 8);
        let claims = due.remove(0).1;
        assert_eq!(claims.len(), 2);
        let mut lease = scheduler.lease(claims, BACKOFF);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let drained = lease.drain_claims();
            assert_eq!(drained.len(), 2);
            panic!("failed part way through the drained claims");
        }));
        assert!(result.is_err());

        for topic_id in [topic(74), topic(75)] {
            let (active, _, _) = scheduler
                .target_state(peer(73), topic_id)
                .expect("target survives");
            assert!(active.is_none(), "every drained claim is handed back");
        }
    }

    /// A peer waiting behind a full set of slots is served as soon as one
    /// frees, and peers still taking a turn are never claimed twice.
    #[test]
    fn free_slot_serves_waiter() {
        let scheduler = ResyncScheduler::default();
        let waiting = MAX_RESYNC_PEER_CONCURRENCY + 1;
        for index in 0..waiting {
            scheduler.schedule_now(peer(50 + index as u8), topic(60), false);
        }

        let first =
            scheduler.due_targets_by_peer(MAX_RESYNC_PEER_CONCURRENCY, MAX_TOPICS_PER_RESYNC_BATCH);
        assert_eq!(
            first.len(),
            MAX_RESYNC_PEER_CONCURRENCY,
            "every slot is filled"
        );
        assert!(
            scheduler
                .due_targets_by_peer(0, MAX_TOPICS_PER_RESYNC_BATCH)
                .is_empty(),
            "no slot is free, so nothing more is claimed"
        );

        // The first peer finishes; the waiting peer takes the freed slot.
        let (done_peer, claims) = first.into_iter().next().expect("one claimed peer");
        for claim in claims {
            scheduler.complete_clean(claim);
        }
        let next = scheduler.due_targets_by_peer(1, MAX_TOPICS_PER_RESYNC_BATCH);
        assert_eq!(next.len(), 1, "the freed slot is refilled at once");
        assert_ne!(
            next[0].0, done_peer,
            "the finished peer is not reclaimed for work it completed"
        );
        assert_eq!(
            next[0].0,
            peer(50 + MAX_RESYNC_PEER_CONCURRENCY as u8),
            "the peer that was waiting is the one served"
        );
    }

    /// Work that arrives while an attempt is in flight must survive that
    /// attempt's clean completion, and repeated reevaluation must not keep
    /// inventing work revisions.
    #[test]
    fn publish_survives_clean() {
        let scheduler = ResyncScheduler::default();
        scheduler.schedule_now(peer(41), topic(42), false);
        let claim = one_claim(&scheduler);

        // Reevaluating the same evidence adds no work.
        for _ in 0..8 {
            scheduler.reconsider(peer(41), topic(42));
        }
        // A publish during the attempt does.
        scheduler.schedule_now(peer(41), topic(42), false);
        scheduler.complete_clean(claim);

        let (active, failures, _) = scheduler
            .target_state(peer(41), topic(42))
            .expect("work from mid-attempt must survive a clean completion");
        assert!(active.is_none(), "the attempt is finished");
        assert_eq!(failures, 0, "surviving work is not a failure");

        // The next dispatch serves it, and completing that removes the target.
        let again = one_claim(&scheduler);
        assert_eq!(again.key.topic_id, topic(42));
        scheduler.complete_clean(again);
        assert!(
            scheduler.target_state(peer(41), topic(42)).is_none(),
            "a completion covering the newest request clears the target"
        );
    }

    /// A page cut to the message share keeps a causal prefix and says more
    /// remains, instead of silently dropping the tail or the page result.
    #[test]
    fn page_fits_messages() {
        let node = Irokle::in_memory().unwrap();
        let topic = node
            .create_topic::<Ping>(crate::TopicConfig::default())
            .unwrap();
        for _ in 0..(3 * MAX_SYNC_DATA_OPS_PER_MESSAGE) {
            topic.publish(Ping).unwrap();
        }
        let ops = crate::oplog::topological(node.storage(), &topic.id()).unwrap();
        let whole = super::super::sync_data_page(topic.id(), ops.clone(), 8, usize::MAX).unwrap();
        assert!(!whole.cut);
        assert_eq!(whole.messages.len(), 4);

        let cut = super::super::sync_data_page(topic.id(), ops.clone(), 2, usize::MAX).unwrap();
        assert!(cut.cut, "a cut page must report the rest");
        assert!(cut.messages.len() <= 2);
        let sent = cut
            .messages
            .iter()
            .flat_map(|message| match message {
                SyncMessage::Data(data) => data.ops.clone(),
                _ => Vec::new(),
            })
            .collect::<Vec<_>>();
        assert!(!sent.is_empty());
        assert_eq!(sent, ops[..sent.len()], "the kept part is a prefix");
    }

    /// A topic settled during a batch leaves the lease, so the batch deadline
    /// recovers only the claims still unfinished. A completed topic keeps its
    /// one terminal result instead of being reinserted as a forced retry.
    #[test]
    fn timeout_spares_settled() {
        let scheduler = ResyncScheduler::default();
        scheduler.schedule_now(peer(31), topic(32), false);
        scheduler.schedule_now(peer(31), topic(33), false);
        let mut due = scheduler.due_targets_by_peer(8, 8);
        assert_eq!(due.len(), 1);
        let claims = due.remove(0).1;
        assert_eq!(claims.len(), 2);
        let mut lease = scheduler.lease(claims, BACKOFF);

        // The first topic finishes cleanly inside the batch.
        let done = lease
            .take_claim(&ResyncTargetKey {
                peer_id: peer(31),
                topic_id: topic(32),
            })
            .expect("first claim");
        scheduler.complete_clean(done.settle());
        assert!(
            scheduler.target_state(peer(31), topic(32)).is_none(),
            "a clean completion removes the target"
        );

        // The deadline then expires while the second topic is still in flight.
        let timed_out_claims = lease.drain_claims();
        assert_eq!(
            timed_out_claims.len(),
            1,
            "only unfinished work is recovered"
        );
        assert_eq!(timed_out_claims[0].key().topic_id, topic(33));
        let mut recovered = timed_out_claims;
        scheduler.complete_failed(
            recovered.remove(0).settle(),
            BACKOFF,
            Duration::from_secs(600),
        );
        assert!(
            scheduler.target_state(peer(31), topic(32)).is_none(),
            "the settled topic must not be reinserted by the timeout"
        );
        let (_, failures, force) = scheduler.target_state(peer(31), topic(33)).unwrap();
        assert_eq!(failures, 1);
        assert!(force.is_some(), "the unfinished topic retries");
    }

    /// A bounded page that really advanced is served again after a short turn.
    /// It must not grow the network backoff or count as a failed attempt: that
    /// is what turned working catch-up into a failing peer.
    #[test]
    fn progress_keeps_backoff() {
        let scheduler = ResyncScheduler::default();
        scheduler.schedule_now(peer(21), topic(22), false);
        let advancing = one_claim(&scheduler);
        scheduler.complete_dirty(advancing, RESYNC_PROGRESS_TURN);
        let (active, failures, _) = scheduler.target_state(peer(21), topic(22)).unwrap();
        assert!(active.is_none(), "the attempt is finished");
        assert_eq!(failures, 0, "progress must not grow the network backoff");

        // A no-progress exchange does back off, so repeats cannot spin.
        let blocked_scheduler = ResyncScheduler::default();
        blocked_scheduler.schedule_now(peer(23), topic(24), false);
        let blocked = one_claim(&blocked_scheduler);
        blocked_scheduler.complete_failed(blocked, BACKOFF, Duration::from_secs(600));
        let (_, failures, _) = blocked_scheduler.target_state(peer(23), topic(24)).unwrap();
        assert_eq!(failures, 1, "a no-progress exchange must back off");
    }

    #[test]
    fn keeps_force_request() {
        let scheduler = ResyncScheduler::default();
        scheduler.schedule_now(peer(5), topic(6), false);
        let claim = one_claim(&scheduler);

        // A force request that arrives during the attempt is not covered by it.
        scheduler.schedule_now(peer(5), topic(6), true);
        scheduler.complete_clean(claim);

        assert!(scheduler.next_due().is_some());
        let (active, _, force) = scheduler.target_state(peer(5), topic(6)).unwrap();
        assert_eq!(active, None);
        assert!(force.is_some());
    }

    #[test]
    fn ignores_stale_failure() {
        let scheduler = ResyncScheduler::default();
        scheduler.schedule_now(peer(7), topic(8), false);
        let claim = one_claim(&scheduler);

        scheduler.complete_clean(claim);
        scheduler.complete_failed(claim, Duration::from_secs(1), Duration::from_secs(600));

        assert!(scheduler.next_due().is_none());
        assert_eq!(scheduler.target_state(peer(7), topic(8)), None);
    }

    #[test]
    fn ignores_stale_completion() {
        let scheduler = ResyncScheduler::default();
        scheduler.schedule_now(peer(9), topic(10), false);
        let stale = one_claim(&scheduler);
        scheduler.complete_clean(stale);
        scheduler.schedule_now(peer(9), topic(10), true);
        let live = one_claim(&scheduler);

        scheduler.complete_dirty(stale, Duration::from_secs(5));
        scheduler.complete_clean(stale);
        scheduler.complete_failed(stale, Duration::from_secs(1), Duration::from_secs(600));

        assert!(scheduler.due_targets_by_peer(8, 8).is_empty());
        assert!(scheduler.next_due().is_none());
        let (active, failures, force) = scheduler.target_state(peer(9), topic(10)).unwrap();
        assert_eq!(active, Some(live.attempt));
        assert_eq!(failures, 0);
        assert_eq!(force, live.force);
    }

    #[test]
    fn panic_releases_lease() {
        let scheduler = ResyncScheduler::default();
        scheduler.schedule_now(peer(13), topic(14), false);
        let claim = one_claim(&scheduler);
        let lease = scheduler.lease(vec![claim], Duration::ZERO);

        let batch = std::thread::spawn(move || {
            let _lease = lease;
            panic!("batch task panicked");
        });
        assert!(batch.join().is_err());

        let released = one_claim(&scheduler);
        assert_ne!(released.attempt, claim.attempt);
    }

    #[test]
    fn timeout_spares_completed() {
        let scheduler = ResyncScheduler::default();
        scheduler.schedule_now(peer(15), topic(1), false);
        scheduler.schedule_now(peer(15), topic(2), false);
        let mut due = scheduler.due_targets_by_peer(8, 8);
        assert_eq!(due.len(), 1);
        let mut lease = scheduler.lease(due.remove(0).1, Duration::ZERO);
        let done = lease
            .take_claim(&ResyncTargetKey {
                peer_id: peer(15),
                topic_id: topic(1),
            })
            .unwrap();
        scheduler.complete_clean(done.settle());

        let unfinished = lease.drain_claims();

        assert_eq!(unfinished.len(), 1);
        assert_eq!(unfinished[0].key().topic_id, topic(2));
        for claim in unfinished {
            scheduler.complete_failed(
                claim.settle(),
                Duration::from_secs(1),
                Duration::from_secs(600),
            );
        }
        assert_eq!(scheduler.target_state(peer(15), topic(1)), None);
        let (active, failures, _) = scheduler.target_state(peer(15), topic(2)).unwrap();
        assert_eq!(active, None);
        assert_eq!(failures, 1);
    }

    /// A node whose iroh endpoint matches its signer, for the scheduler paths
    /// that need a real `IrohNet`.
    async fn test_net() -> Arc<IrohNet<MemoryStorage>> {
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .bind()
            .await
            .unwrap();
        let node = Irokle::builder()
            .with_iroh_secret_key(endpoint.secret_key())
            .without_auto_accept()
            .build()
            .unwrap();
        Arc::new(IrohNet::new(endpoint, node).unwrap())
    }

    /// A net over gated storage, with no loops running.
    async fn stale_net() -> (
        Arc<IrohNet<crate::tests::support::StaleReadStorage>>,
        crate::tests::support::StaleReadStorage,
    ) {
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .bind()
            .await
            .unwrap();
        let storage = crate::tests::support::StaleReadStorage::new(MemoryStorage::new());
        let node = Irokle::builder()
            .with_storage(storage.clone())
            .with_iroh_secret_key(endpoint.secret_key())
            .without_auto_accept()
            .build()
            .unwrap();
        // A peer that went away fails a dial quickly; nothing here measures it.
        let runtime = IrohRuntimeConfig {
            connect_timeout: Duration::from_secs(2),
            ..IrohRuntimeConfig::default()
        };
        let net = IrohNet::new_with_config(endpoint, node, runtime).unwrap();
        (Arc::new(net), storage)
    }

    /// On a one-worker runtime, a control job completes while every bulk permit
    /// is taken, one of them by a job held inside a storage read.
    #[tokio::test]
    async fn control_passes_bulk() {
        use crate::tests::support::{Gate, GatePoint, Note, node};

        let (net, storage) = stale_net().await;
        let remote = node(61);
        let topic_id = net
            .node
            .create_topic::<Note>(crate::TopicConfig {
                initial_peers: [remote.peer_id()].into(),
                ..crate::TopicConfig::default()
            })
            .unwrap()
            .id();
        let gate = Arc::new(Gate::default());
        let release = gate.releaser();
        storage.arm_read(GatePoint::Heads(topic_id), Arc::clone(&gate));
        let held = {
            let net = Arc::clone(&net);
            tokio::spawn(async move {
                net.run_job(Lane::Bulk, move |shared| {
                    shared.node.storage().heads(&topic_id).map(drop)
                })
                .await
            })
        };
        let (waiting, parked) = std::sync::mpsc::channel::<()>();
        let filler = {
            let net = Arc::clone(&net);
            tokio::spawn(async move {
                net.run_job(Lane::Bulk, move |_| {
                    let _ = parked.recv_timeout(Duration::from_secs(60));
                })
                .await
            })
        };
        let arrival = Arc::clone(&gate);
        tokio::task::spawn_blocking(move || arrival.wait_arrival())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(60), async {
            while net.bulk_lane.available_permits() > 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the bulk lane never filled");

        let open = vec![SyncMessage::Open(remote.sync_open(topic_id))];
        let endpoint = peer_id_to_endpoint_addr(remote.peer_id()).unwrap().id;
        let replies = tokio::time::timeout(
            Duration::from_secs(60),
            net.run_job(Lane::Control, move |shared| {
                shared.handle_messages(endpoint, open)
            }),
        )
        .await
        .expect("control work waited behind bulk work")
        .unwrap()
        .unwrap();
        assert!(matches!(&replies[..], [SyncMessage::Summary(_)]));
        assert!(
            !gate.has_left(),
            "control work finished only after bulk work"
        );

        drop(release);
        drop(waiting);
        held.await.unwrap().unwrap().unwrap();
        filler.await.unwrap().unwrap();
        net.shutdown().await;
    }

    /// A requester that goes away does not stop a started bulk job: it still
    /// commits, keeps its permit until it ends, and shutdown waits for it. A
    /// requester dropped before its first poll starts nothing.
    #[tokio::test]
    async fn cancelled_job_commits() {
        use crate::tests::support::{Gate, GatePoint, chain_source};

        let (net, storage) = stale_net().await;
        let (source, topic_id, ops) = chain_source(62, net.node.peer_id());
        let admit = |net: &Arc<IrohNet<crate::tests::support::StaleReadStorage>>| {
            let net = Arc::clone(net);
            let source_peer = source.peer_id();
            let data = crate::sync::SyncData {
                topic_id,
                ops: ops.clone(),
            };
            tokio::spawn(async move {
                net.run_job(Lane::Bulk, move |shared| {
                    shared.node.storage().heads(&topic_id)?;
                    shared.node.receive_sync_data_from(source_peer, data)
                })
                .await
            })
        };

        let unpolled = admit(&net);
        unpolled.abort();
        assert!(unpolled.await.unwrap_err().is_cancelled());
        assert_eq!(net.bulk_lane.available_permits(), BULK_JOBS);
        assert_eq!(net.tasks.running(), 0);
        assert!(storage.topic_state(&topic_id).unwrap().is_none());

        let gate = Arc::new(Gate::default());
        let release = gate.releaser();
        storage.arm_read(GatePoint::Heads(topic_id), Arc::clone(&gate));
        let requester = admit(&net);
        let arrival = Arc::clone(&gate);
        tokio::task::spawn_blocking(move || arrival.wait_arrival())
            .await
            .unwrap();
        requester.abort();
        assert!(requester.await.unwrap_err().is_cancelled());
        assert_eq!(net.bulk_lane.available_permits(), BULK_JOBS - 1);

        let outcome = net.shutdown_with_timeout(Duration::from_millis(200)).await;
        assert!(
            matches!(outcome, ShutdownOutcome::Incomplete { running } if running >= 1),
            "{outcome:?}"
        );
        drop(release);
        assert_eq!(
            net.shutdown_with_timeout(Duration::from_secs(60)).await,
            ShutdownOutcome::Complete
        );
        assert_eq!(net.bulk_lane.available_permits(), BULK_JOBS);
        assert_eq!(
            storage.list_op_ids(&topic_id).unwrap().len(),
            ops.len(),
            "the job committed after its requester left"
        );
    }

    /// End to end, a manual attempt that started first but finishes last
    /// cannot overwrite the status of a newer attempt that already finished,
    /// though its failure still counts.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_attempt_loses() {
        use crate::tests::support::{Gate, GatePoint, Note};
        use futures::StreamExt;
        use iroh::Watcher;

        let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .bind()
            .await
            .unwrap();
        let alice = Irokle::builder()
            .with_iroh_secret_key(alice_endpoint.secret_key())
            .without_auto_accept()
            .build()
            .unwrap();
        let alice_net = Arc::new(IrohNet::new(alice_endpoint, alice.clone()).unwrap());
        alice_net.start_accept_loop().unwrap();
        let mut alice_addr = alice_net.endpoint().addr();
        let mut addrs = alice_net.endpoint().watch_addr().stream();
        while alice_addr.addrs.is_empty() {
            alice_addr = tokio::time::timeout(Duration::from_secs(60), addrs.next())
                .await
                .unwrap()
                .unwrap();
        }

        let (bob_net, storage) = stale_net().await;
        let topic = alice
            .create_topic::<Note>(crate::TopicConfig {
                initial_peers: [bob_net.node.peer_id()].into(),
                ..crate::TopicConfig::default()
            })
            .unwrap();
        topic.publish(Note { text: "one".into() }).unwrap();
        let topic_id = topic.id();
        let ops = crate::oplog::topological(alice.storage(), &topic_id).unwrap();
        bob_net
            .node
            .receive_sync_data_from(alice.peer_id(), crate::sync::SyncData { topic_id, ops })
            .unwrap();

        // The older attempt pauses while preparing its fingerprints, at the open's
        // live state read after the digest's snapshot, so the newer one can read.
        let gate = Arc::new(Gate::default());
        let release = gate.releaser();
        storage.arm_read_after(GatePoint::Topic(topic_id), 1, Arc::clone(&gate));
        let older = {
            let net = Arc::clone(&bob_net);
            let addr = alice_addr.clone();
            tokio::spawn(async move { net.sync_now(addr, topic_id).await })
        };
        let arrival = Arc::clone(&gate);
        tokio::task::spawn_blocking(move || arrival.wait_arrival())
            .await
            .unwrap();

        bob_net.sync_now(alice_addr, topic_id).await.unwrap();
        let newer = bob_net.node.sync_status(topic_id).unwrap().remove(0);
        assert_eq!(newer.state, crate::SyncPeerState::Healthy);

        // The older attempt now fails against a peer that has gone away.
        alice_net.shutdown().await;
        drop(release);
        assert!(older.await.unwrap().is_err());
        let status = bob_net.node.sync_status(topic_id).unwrap().remove(0);
        assert_eq!(status.state, crate::SyncPeerState::Healthy, "{status:?}");
        assert_eq!(status.latest_attempt, newer.latest_attempt);
        assert_eq!((status.successful_attempts, status.failed_attempts), (1, 1));
        bob_net.shutdown().await;
    }

    /// Shutdown before any loop subscribes must still be recorded. The watch
    /// channel has no receivers at that point, so a plain send would drop the
    /// intent and let a loop started later run as if the net were live.
    #[tokio::test]
    async fn shutdown_without_receivers() {
        let net = test_net().await;
        assert!(!net.is_shutdown());
        net.shutdown().await;
        assert!(
            net.is_shutdown(),
            "terminal shutdown must be retained with no watch receivers"
        );

        // Repeating it is safe and stays terminal.
        net.shutdown().await;
        assert!(net.is_shutdown());
        assert!(
            !dispatch_due_resyncs(
                &Arc::downgrade(&net),
                &mut tokio::task::JoinSet::new(),
                IrohRuntimeConfig::default(),
            ),
            "a terminally closed net must not dispatch new work"
        );
    }

    /// A new resync loop must not take back a claim a live lease still holds.
    #[tokio::test]
    async fn restart_keeps_claims() {
        let net = test_net().await;
        let scheduler = &net.resync_scheduler;
        scheduler.schedule_now(peer(16), topic(17), false);
        let claim = one_claim(scheduler);
        let _lease = scheduler.lease(vec![claim], Duration::ZERO);
        // A second due target shows when the new loop has dispatched.
        scheduler.schedule_now(peer(18), topic(17), false);
        let idle = scheduler.target_state(peer(18), topic(17));

        let resync = net
            .spawn_resync_loop(BACKOFF)
            .unwrap()
            .expect("loop starts");
        tokio::time::timeout(Duration::from_secs(60), async {
            while scheduler.target_state(peer(18), topic(17)) == idle {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the new loop never dispatched");

        assert_eq!(
            scheduler
                .target_state(peer(16), topic(17))
                .map(|state| state.0),
            Some(Some(claim.attempt)),
            "the new loop took over a claim a live lease holds"
        );
        resync.abort();
        let _ = resync.await;
        net.shutdown().await;
    }

    /// A slow maintenance topic must not hold the resync loop: durable work for
    /// a healthy topic is scheduled and dispatched while quarantine of the
    /// first topic is held inside a storage read.
    #[tokio::test]
    async fn sweep_isolates_quarantine() {
        use crate::tests::support::{Gate, GatePoint, Note, StaleReadStorage};

        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .bind()
            .await
            .unwrap();
        let storage = StaleReadStorage::new(MemoryStorage::new());
        let node = Irokle::with_storage(
            storage.clone(),
            crate::NodeConfig {
                signer: crate::Ed25519Signer::from_iroh_secret_key(endpoint.secret_key()),
                default_write_concern: crate::WriteConcern::Local,
                ..crate::NodeConfig::default()
            },
        )
        .unwrap();
        let remote = crate::Signer::peer_id(&crate::Ed25519Signer::from_bytes(&[41; 32]));
        let healthy = node
            .create_topic::<Note>(crate::TopicConfig {
                initial_peers: [remote].into(),
                ..crate::TopicConfig::default()
            })
            .unwrap()
            .id();
        // Topics are visited in id order, so the held topic must sort first.
        let slow = (0..256)
            .map(|_| {
                node.create_topic::<Note>(crate::TopicConfig::default())
                    .unwrap()
                    .id()
            })
            .find(|topic_id| *topic_id < healthy)
            .expect("a topic sorting before the healthy one");
        let genesis = storage
            .topic_state(&healthy)
            .unwrap()
            .map(|state| state.genesis);
        let mut clock = crate::ActorClock::new();
        clock.observe(crate::actor_id_for(healthy, node.peer_id()), 1);
        storage
            .put_sync_obligation(
                crate::storage::SyncObligation::clock(remote, healthy, clock),
                genesis,
            )
            .unwrap();

        let gate = Arc::new(Gate::default());
        let release = gate.releaser();
        storage.arm_read(GatePoint::Heads(slow), Arc::clone(&gate));
        let net = Arc::new(IrohNet::new(endpoint, node).unwrap());
        net.spawn_resync_loop(BACKOFF)
            .unwrap()
            .expect("loop starts");
        let arrival = Arc::clone(&gate);
        tokio::task::spawn_blocking(move || arrival.wait_arrival())
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(60), async {
            while !net
                .resync_scheduler
                .target_state(remote, healthy)
                .is_some_and(|(active, failures, _)| active.is_some() || failures > 0)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the healthy target was not dispatched while maintenance was held");
        assert!(
            !gate.has_left(),
            "the target was dispatched only after maintenance"
        );

        drop(release);
        net.shutdown().await;
    }

    /// Manual syncs and resync batches share the outbound peer slots: with
    /// every slot held the loop claims nothing, and a running manual sync holds
    /// a slot the loop cannot use.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn manual_shares_slots() {
        use crate::tests::support::{Note, node};

        let (net, _storage) = stale_net().await;
        let remote = node(74).peer_id();
        let topic_id = net
            .node
            .create_topic::<Note>(crate::TopicConfig {
                initial_peers: [remote].into(),
                ..crate::TopicConfig::default()
            })
            .unwrap()
            .id();
        let runtime = net.runtime_config();
        let weak = Arc::downgrade(&net);
        let mut syncs = tokio::task::JoinSet::new();

        let held = Arc::clone(&net.outbound)
            .acquire_many_owned(MAX_RESYNC_PEER_CONCURRENCY as u32)
            .await
            .unwrap();
        let manual = tokio::spawn({
            let net = Arc::clone(&net);
            async move { net.sync_peer_now(remote, topic_id).await }
        });
        net.schedule_resync(remote, topic_id);
        assert!(dispatch_due_resyncs(&weak, &mut syncs, runtime));
        assert!(syncs.is_empty(), "the loop dispatched without a free slot");
        assert!(
            !manual.is_finished(),
            "a manual sync ran without a free slot"
        );

        drop(held);
        tokio::time::timeout(Duration::from_secs(60), async {
            while net.outbound.available_permits() == MAX_RESYNC_PEER_CONCURRENCY {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the manual sync never took a slot");
        assert!(dispatch_due_resyncs(&weak, &mut syncs, runtime));
        assert!(syncs.len() < MAX_RESYNC_PEER_CONCURRENCY);
        let _ = manual.await.unwrap();
        syncs.abort_all();
        while syncs.join_next().await.is_some() {}
        assert_eq!(
            net.outbound.available_permits(),
            MAX_RESYNC_PEER_CONCURRENCY
        );
        net.shutdown().await;
    }

    /// Target discovery held inside its storage read does not hold dispatch: a
    /// target scheduled meanwhile is claimed while the sweep still waits.
    #[tokio::test]
    async fn sweep_discovery_isolated() {
        use crate::tests::support::{Gate, GatePoint, Note, node};

        let (net, storage) = stale_net().await;
        let remote = node(73).peer_id();
        let healthy = net
            .node
            .create_topic::<Note>(crate::TopicConfig {
                initial_peers: [remote].into(),
                ..crate::TopicConfig::default()
            })
            .unwrap()
            .id();
        let gate = Arc::new(Gate::default());
        let release = gate.releaser();
        storage.arm_read(GatePoint::Topics, Arc::clone(&gate));
        net.spawn_resync_loop(BACKOFF)
            .unwrap()
            .expect("loop starts");
        let arrival = Arc::clone(&gate);
        tokio::task::spawn_blocking(move || arrival.wait_arrival())
            .await
            .unwrap();

        net.schedule_resync(remote, healthy);
        tokio::time::timeout(Duration::from_secs(60), async {
            while !net
                .resync_scheduler
                .target_state(remote, healthy)
                .is_some_and(|(active, failures, _)| active.is_some() || failures > 0)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the target was not dispatched while discovery was held");
        assert!(
            !gate.has_left(),
            "the target was dispatched only after discovery"
        );

        drop(release);
        net.shutdown().await;
    }

    /// Aborting the accept loop before its first poll still clears its latch.
    #[tokio::test]
    async fn accept_abort_replaces() {
        let net = test_net().await;
        let first = net
            .spawn_accept_loop()
            .unwrap()
            .expect("the first accept loop starts");
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());

        assert!(net.spawn_accept_loop().unwrap().is_some());
        net.shutdown().await;
    }

    #[tokio::test]
    async fn refills_free_slots() {
        let net = test_net().await;
        let weak = Arc::downgrade(&net);
        let mut syncs = tokio::task::JoinSet::new();
        net.resync_scheduler.schedule_now(peer(20), topic(1), false);
        net.resync_scheduler.schedule_now(peer(21), topic(1), false);
        assert!(dispatch_due_resyncs(&weak, &mut syncs, net.runtime));
        assert_eq!(syncs.len(), 2);

        // A peer that becomes due while other turns run takes a free slot now.
        net.resync_scheduler.schedule_now(peer(22), topic(1), false);
        assert!(dispatch_due_resyncs(&weak, &mut syncs, net.runtime));

        assert_eq!(syncs.len(), 3);
        let (active, _, _) = net
            .resync_scheduler
            .target_state(peer(22), topic(1))
            .unwrap();
        assert!(active.is_some());
    }

    #[test]
    fn full_slots_park() {
        let scheduler = ResyncScheduler::default();
        scheduler.schedule_now(peer(24), topic(1), false);

        let armed = next_resync_wake(&scheduler, MAX_RESYNC_PEER_CONCURRENCY - 1);
        let parked = next_resync_wake(&scheduler, MAX_RESYNC_PEER_CONCURRENCY);

        assert!(armed <= tokio::time::Instant::now());
        assert!(parked > tokio::time::Instant::now() + Duration::from_secs(60));
    }

    #[tokio::test]
    async fn skips_busy_peers() {
        let net = test_net().await;
        let weak = Arc::downgrade(&net);
        let mut syncs = tokio::task::JoinSet::new();
        net.resync_scheduler.schedule_now(peer(23), topic(1), false);
        assert!(dispatch_due_resyncs(&weak, &mut syncs, net.runtime));
        assert_eq!(syncs.len(), 1);

        // More work for a peer mid-turn waits for its next turn instead of
        // opening a second exchange or spinning on its expired deadline.
        net.resync_scheduler.schedule_now(peer(23), topic(2), false);
        assert!(dispatch_due_resyncs(&weak, &mut syncs, net.runtime));

        assert_eq!(syncs.len(), 1);
        let (active, _, _) = net
            .resync_scheduler
            .target_state(peer(23), topic(2))
            .unwrap();
        assert_eq!(active, None);
        assert!(net.resync_scheduler.next_due().is_none());
    }

    #[tokio::test]
    async fn ack_preserves_claim() {
        let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .bind()
            .await
            .unwrap();
        let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .bind()
            .await
            .unwrap();
        let alice = Irokle::builder()
            .with_iroh_secret_key(alice_endpoint.secret_key())
            .without_auto_accept()
            .build()
            .unwrap();
        let bob = Irokle::builder()
            .with_iroh_secret_key(bob_endpoint.secret_key())
            .without_auto_accept()
            .build()
            .unwrap();
        let net = IrohNet::new(alice_endpoint, alice.clone()).unwrap();
        let topic = alice
            .create_topic::<Ping>(crate::TopicConfig {
                initial_peers: [bob.peer_id()].into(),
                ..crate::TopicConfig::default()
            })
            .unwrap();
        net.resync_scheduler
            .schedule_now(bob.peer_id(), topic.id(), false);
        let failed = one_claim(&net.resync_scheduler);
        net.resync_scheduler
            .complete_failed(failed, BACKOFF, Duration::from_secs(600));
        net.resync_scheduler
            .schedule_now(bob.peer_id(), topic.id(), true);
        let claim = one_claim(&net.resync_scheduler);

        let mut ack = crate::sync::SyncAck {
            topic_id: topic.id(),
            peer_id: bob.peer_id(),
            genesis: alice
                .storage()
                .topic_state(&topic.id())
                .unwrap()
                .map(|state| state.genesis),
            accepted: BTreeSet::new(),
            heads: BTreeSet::new(),
            clock: crate::ActorClock::new(),
            signature: None,
        };
        ack.sign(bob.signer()).unwrap();
        net.handle_messages(
            bob_endpoint.id(),
            vec![
                SyncMessage::Open(crate::sync::SyncEngine::<MemoryStorage>::open(
                    topic.id(),
                    bob.peer_id(),
                    Some(<Ping as crate::Event>::TYPE_ID.into()),
                )),
                SyncMessage::Ack(ack),
            ],
        )
        .unwrap();

        assert!(net.resync_scheduler.due_targets_by_peer(8, 8).is_empty());
        assert_eq!(
            net.resync_scheduler.target_state(bob.peer_id(), topic.id()),
            Some((Some(claim.attempt), 1, claim.force))
        );
        net.resync_scheduler
            .complete_failed(claim, BACKOFF, Duration::from_secs(600));
        let delay = net
            .resync_scheduler
            .next_due()
            .expect("the target stays scheduled")
            .saturating_duration_since(tokio::time::Instant::now());
        assert!(delay > BACKOFF, "peer evidence reset the failure backoff");
    }

    /// Evidence migrated without a branch certifies nothing, so a clock it
    /// carries must not mark the target as synchronized.
    #[tokio::test]
    async fn legacy_ack_needs_sync() {
        let net = test_net().await;
        let peer_id = peer(90);
        let topic = net
            .node
            .create_topic::<Ping>(crate::TopicConfig {
                initial_peers: [peer_id].into(),
                ..crate::TopicConfig::default()
            })
            .unwrap();
        let clock = net.node.storage().actor_clock(&topic.id()).unwrap();
        net.node.storage().put_raw_ack(crate::storage::PeerAck {
            peer_id,
            topic_id: topic.id(),
            genesis: None,
            heads: net.node.storage().heads(&topic.id()).unwrap(),
            clock,
        });
        assert!(
            net.target_needs_sync(peer_id, topic.id()).unwrap(),
            "an uncertified clock hid work the peer still needs"
        );
    }

    #[tokio::test]
    async fn notify_keeps_permit() {
        let scheduler = ResyncScheduler::default();
        let notify = scheduler.notifier();
        assert!(scheduler.due_targets_by_peer(8, 8).is_empty());

        scheduler.schedule_now(peer(11), topic(12), false);
        tokio::time::timeout(Duration::from_secs(60), notify.notified())
            .await
            .expect("a schedule after an empty scan must leave a wake permit");

        assert_eq!(scheduler.due_targets_by_peer(8, 8).len(), 1);
    }

    /// Shutdown while the accept loop sits at its connection cap also ends
    /// the handshakes still pending: no owned task is left and the latch clears.
    #[tokio::test]
    async fn capacity_shutdown_reaps() {
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .bind()
            .await
            .unwrap();
        let node = Irokle::builder()
            .with_iroh_secret_key(endpoint.secret_key())
            .without_auto_accept()
            .build()
            .unwrap();
        let runtime = IrohRuntimeConfig {
            connect_timeout: Duration::from_secs(600),
            sync_io_timeout: Duration::from_secs(600),
            ..IrohRuntimeConfig::default()
        };
        let hold = Arc::new(tokio::sync::Semaphore::new(0));
        let mut net = IrohNet::new_with_config(endpoint, node, runtime).unwrap();
        net.accept_hooks = AcceptHooks {
            connections: Some(1),
            handshakes: Some(Arc::clone(&hold)),
        };
        let net = Arc::new(net);
        let accept = net
            .spawn_accept_loop()
            .unwrap()
            .expect("accept loop starts");
        let addr = {
            use futures::StreamExt;
            use iroh::Watcher;
            let mut addr = net.endpoint().addr();
            let mut addrs = net.endpoint().watch_addr().stream();
            while addr.addrs.is_empty() {
                addr = tokio::time::timeout(Duration::from_secs(60), addrs.next())
                    .await
                    .unwrap()
                    .unwrap();
            }
            addr
        };
        let held_by_net = Arc::strong_count(&hold);
        let mut clients = Vec::new();
        for _ in 0..2 {
            let client = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
                .bind()
                .await
                .unwrap();
            let addr = addr.clone();
            clients.push(tokio::spawn(async move {
                let connection = client.connect(addr, IROKLE_SYNC_ALPN).await;
                (client, connection)
            }));
        }
        /// Waits for observable progress, with a generous lost-progress cap.
        async fn settle(condition: impl Fn() -> bool) {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
            while !condition() {
                assert!(tokio::time::Instant::now() < deadline, "lost progress");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        // Both handshakes wait on the hold, owned by the net.
        settle(|| Arc::strong_count(&hold) == held_by_net + 2 && net.tasks.running() == 3).await;

        // One handshake finishes into the only connection slot; the other stays
        // pending while the loop waits at capacity.
        hold.add_permits(1);
        settle(|| {
            Arc::strong_count(&hold) == held_by_net + 1
                && net.tasks.running() == 3
                && clients.iter().any(|client| client.is_finished())
        })
        .await;

        // The loop must have ended its handshakes by the time it reports done,
        // not leave them to be cancelled after it.
        let stopping = {
            let net = Arc::clone(&net);
            tokio::spawn(async move { net.shutdown_with_timeout(Duration::from_secs(60)).await })
        };
        accept.await.unwrap();
        let alive_at_exit = Arc::strong_count(&hold);
        assert_eq!(stopping.await.unwrap(), ShutdownOutcome::Complete);
        assert_eq!(
            alive_at_exit,
            held_by_net - 1,
            "a handshake outlived the loop"
        );
        assert_eq!(net.tasks.running(), 0);
        // The loop's own copy of the hold is gone too.
        assert_eq!(
            Arc::strong_count(&hold),
            held_by_net - 1,
            "a handshake outlived the loop"
        );
        assert!(!net.accept_started.load(Ordering::SeqCst));
        for client in clients {
            client.abort();
            let _ = client.await;
        }
    }

    /// Aborting the resync loop does not orphan the storage job its batch
    /// started: the job stays owned, shutdown reports it until it ends, and
    /// the target is not left owned by the aborted batch.
    #[tokio::test]
    async fn parent_abort_owns_child() {
        use crate::tests::support::{Gate, GatePoint, Note, node};

        let (net, storage) = stale_net().await;
        let remote = node(71).peer_id();
        let topic_id = net
            .node
            .create_topic::<Note>(crate::TopicConfig {
                initial_peers: [remote].into(),
                ..crate::TopicConfig::default()
            })
            .unwrap()
            .id();
        let gate = Arc::new(Gate::default());
        let release = gate.releaser();
        storage.arm_read(GatePoint::View(topic_id), Arc::clone(&gate));
        let resync = net
            .spawn_resync_loop(BACKOFF)
            .unwrap()
            .expect("loop starts");
        let arrival = Arc::clone(&gate);
        tokio::task::spawn_blocking(move || arrival.wait_arrival())
            .await
            .unwrap();

        resync.abort();
        assert!(resync.await.unwrap_err().is_cancelled());
        assert!(!net.resync_started.load(Ordering::SeqCst));
        assert!(net.tasks.running() >= 1, "the held job lost its owner");
        let outcome = net.shutdown_with_timeout(Duration::from_millis(200)).await;
        assert!(
            matches!(outcome, ShutdownOutcome::Incomplete { running } if running >= 1),
            "{outcome:?}"
        );

        drop(release);
        assert_eq!(
            net.shutdown_with_timeout(Duration::from_secs(60)).await,
            ShutdownOutcome::Complete
        );
        assert_eq!(net.tasks.running(), 0);
        assert!(
            net.resync_scheduler
                .target_state(remote, topic_id)
                .is_none_or(|(active, _, _)| active.is_none()),
            "the aborted batch still owns its target"
        );
    }

    /// Loops that end without shutdown, because the endpoint closed, clear
    /// their latches and leave no owned task, so they can be started again.
    #[tokio::test]
    async fn restart_after_exit() {
        let net = test_net().await;
        let accept = net
            .spawn_accept_loop()
            .unwrap()
            .expect("accept loop starts");
        let resync = net
            .spawn_resync_loop(BACKOFF)
            .unwrap()
            .expect("resync loop starts");

        net.endpoint().close().await;
        net.resync_scheduler.notifier().notify_one();
        tokio::time::timeout(Duration::from_secs(60), async {
            accept.await.unwrap();
            resync.await.unwrap();
        })
        .await
        .expect("loops did not exit after the endpoint closed");
        assert!(!net.is_shutdown());
        tokio::time::timeout(Duration::from_secs(60), net.tasks.wait_idle())
            .await
            .expect("a task outlived its loop");

        for replacement in [
            net.spawn_accept_loop().unwrap(),
            net.spawn_resync_loop(BACKOFF).unwrap(),
        ] {
            let replacement = replacement.expect("an exited loop can be started again");
            replacement.await.unwrap();
        }
    }

    /// Once shutdown completes no caller can start network-owned work: every
    /// root entry point refuses, a loop start runs no startup storage job, and
    /// nothing new is registered. A repeated shutdown still completes.
    #[tokio::test]
    async fn closed_refuses_roots() {
        use crate::tests::support::{Gate, GatePoint, Note, node};

        let (net, storage) = stale_net().await;
        let remote = node(72).peer_id();
        let topic_id = net
            .node
            .create_topic::<Note>(crate::TopicConfig {
                initial_peers: [remote].into(),
                ..crate::TopicConfig::default()
            })
            .unwrap()
            .id();
        assert_eq!(
            net.shutdown_with_timeout(Duration::from_secs(60)).await,
            ShutdownOutcome::Complete
        );
        let gate = Arc::new(Gate::default());
        let release = gate.releaser();
        storage.arm_read(GatePoint::View(topic_id), Arc::clone(&gate));
        let refused = |error: io::Error| error.kind() == io::ErrorKind::NotConnected;
        assert!(net.spawn_resync_loop(BACKOFF).is_err_and(refused));
        assert!(net.spawn_accept_loop().is_err_and(refused));
        let addr = peer_id_to_endpoint_addr(remote).unwrap();
        assert!(
            net.sync_now(addr.clone(), topic_id)
                .await
                .is_err_and(refused)
        );
        assert!(net.sync_with(addr, &[]).await.is_err_and(refused));
        assert!(net.accept_one().await.is_err_and(refused));
        let endpoint_id = iroh::EndpointId::from_bytes(remote.as_bytes()).unwrap();
        assert!(
            net.handle_messages(endpoint_id, Vec::new())
                .is_err_and(refused)
        );
        assert_eq!(net.tasks.running(), 0);
        assert!(
            !gate.arrived(),
            "a storage job ran after shutdown completed"
        );
        drop(release);
        assert_eq!(
            net.shutdown_with_timeout(Duration::from_secs(60)).await,
            ShutdownOutcome::Complete
        );
    }

    /// Callers racing shutdown are either registered before the seal and
    /// drained by it, or refused; none is left running after completion.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn registration_races_shutdown() {
        let net = test_net().await;
        let peer = iroh::SecretKey::generate().public();
        let barrier = Arc::new(tokio::sync::Barrier::new(33));
        let callers = (0..32)
            .map(|_| {
                let net = Arc::clone(&net);
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    net.handle_messages(peer, Vec::new()).map(drop)
                })
            })
            .collect::<Vec<_>>();
        barrier.wait().await;
        assert_eq!(
            net.shutdown_with_timeout(Duration::from_secs(60)).await,
            ShutdownOutcome::Complete
        );
        assert_eq!(net.tasks.running(), 0);
        for caller in callers {
            if let Err(error) = caller.await.unwrap() {
                assert_eq!(error.kind(), io::ErrorKind::NotConnected);
            }
        }
        assert_eq!(net.tasks.running(), 0);
        assert!(net.handle_messages(peer, Vec::new()).is_err());
    }

    /// A loop that panics clears its latch on unwind and leaves no owned
    /// task, so it can be started again.
    #[tokio::test]
    async fn restart_after_panic() {
        let net = test_net().await;
        // A poisoned scheduler makes the loop's next dispatch panic.
        let scheduler = net.resync_scheduler.clone();
        let poisoned = std::thread::spawn(move || {
            let _held = scheduler.inner.lock().unwrap();
            panic!("poison the resync scheduler");
        });
        assert!(poisoned.join().is_err());

        let resync = net
            .spawn_resync_loop(BACKOFF)
            .unwrap()
            .expect("loop starts");
        let ended = tokio::time::timeout(Duration::from_secs(60), resync)
            .await
            .expect("the loop did not reach its dispatch");
        assert!(ended.unwrap_err().is_panic());
        assert!(!net.resync_started.load(Ordering::SeqCst));
        tokio::time::timeout(Duration::from_secs(60), net.tasks.wait_idle())
            .await
            .expect("a task outlived the panicked loop");

        let replacement = net
            .spawn_resync_loop(BACKOFF)
            .unwrap()
            .expect("a panicked loop can be started again");
        replacement.abort();
        let _ = replacement.await;
        net.shutdown().await;
    }
}
