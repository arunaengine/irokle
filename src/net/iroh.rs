// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::sync::{SyncMessage, SyncSummary};
use crate::{Irokle, MemoryStorage, PeerId, Storage, TopicEviction};

use super::frame::{MAX_FRAME_LEN, MAX_SYNC_DATA_OPS_PER_MESSAGE};
use super::{
    _message_type_name, IROKLE_SYNC_ALPN, decode_sync_message, encode_frame, encode_sync_message,
    invalid_data, sync_data_messages,
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
/// Delay before a topic that advanced but still owes work is served again.
/// Short enough to keep catching up, long enough to let other peers take a
/// turn on the shared slots.
const RESYNC_PROGRESS_TURN: Duration = Duration::from_millis(50);
/// Pages one `sync_now` call will fetch while each is really advancing, before
/// it reports what it reached. Bounds the caller's wait instead of paging on
/// until the peer stops publishing.
const MAX_SYNC_NOW_PAGES: usize = 64;

/// Result of [`IrohNet::shutdown_with_timeout`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownOutcome {
    /// Every task the net spawned has ended.
    Complete,
    /// Tasks were still running at the timeout; the net keeps owning them.
    Incomplete { running: usize },
}

/// Counts the tasks a net spawned that have not ended yet.
#[derive(Default)]
struct TaskTracker {
    running: AtomicUsize,
    idle: tokio::sync::Notify,
}

impl TaskTracker {
    /// Taken before a task is spawned and moved into its future.
    fn track(self: &Arc<Self>) -> TaskGuard {
        self.running.fetch_add(1, Ordering::SeqCst);
        TaskGuard(Arc::clone(self))
    }

    fn running(&self) -> usize {
        self.running.load(Ordering::SeqCst)
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
        if self.0.running.fetch_sub(1, Ordering::SeqCst) == 1 {
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
}

impl ResyncScheduler {
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

pub struct IrohNet<S: Storage = MemoryStorage> {
    pool: ConnectionPool,
    node: Irokle<S>,
    runtime: IrohRuntimeConfig,
    resync_scheduler: ResyncScheduler,
    accept_started: AtomicBool,
    resync_started: AtomicBool,
    outbound_streams: AtomicU64,
    shutdown: tokio::sync::watch::Sender<bool>,
    tasks: Arc<TaskTracker>,
    // Optional sink for genesis tie-break evictions produced while admitting
    // remote sync data. The embedder consumes these to re-emit the discarded
    // payloads under the winning genesis; when unset they are recovered from
    // the eviction journal instead.
    eviction_sink: Option<tokio::sync::mpsc::UnboundedSender<TopicEviction>>,
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
        let (shutdown, _) = tokio::sync::watch::channel(false);
        Ok(Self {
            pool: ConnectionPool::new(endpoint),
            node,
            runtime,
            resync_scheduler: ResyncScheduler::default(),
            accept_started: AtomicBool::new(false),
            resync_started: AtomicBool::new(false),
            outbound_streams: AtomicU64::new(0),
            shutdown,
            tasks: Arc::default(),
            eviction_sink,
        })
    }

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
        *self.shutdown.borrow()
    }

    pub async fn sync_peer_now(&self, peer_id: PeerId, topic_id: crate::TopicId) -> io::Result<()> {
        self.sync_now(peer_id_to_endpoint_addr(peer_id)?, topic_id)
            .await
    }

    pub fn schedule_resync(&self, peer_id: PeerId, topic_id: crate::TopicId) {
        self.resync_scheduler.schedule_now(peer_id, topic_id, false);
    }

    pub fn note_peer_reachable(&self, peer_id: PeerId) {
        self.resync_scheduler.peer_reachable(peer_id);
    }

    /// Marks the peer on an externally accepted connection as reachable.
    /// Outbound sync dials separately because reverse stream support is not guaranteed.
    pub fn register_connection(&self, connection: iroh::endpoint::Connection) -> io::Result<()> {
        let peer = connection.remote_id();
        self.resync_scheduler
            .peer_reachable(peer_id_from_endpoint_id(peer));
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
        let task = tracker.track();
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
            loop {
                while connections.len() >= MAX_ACCEPT_CONNECTIONS {
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
                            if changed.is_err() || *shutdown.borrow() {
                                connections.abort_all();
                                while connections.join_next().await.is_some() {}
                                return;
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
                handshakes.spawn(async move {
                    let _task = task;
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
        let task = self.tasks.track();
        Ok(Some(handle.spawn(async move {
            let _task = task;
            let _running = running;
            let mut sweep_pending = net.upgrade().is_some_and(|current| {
                current.schedule_startup_resync().inspect_err(|error| {
                    tracing::warn!(%error, "failed to schedule startup resync sweep");
                }).is_err()
            });
            let mut sweep_backoff = runtime.resync_initial_backoff.max(Duration::from_millis(1));
            let mut full_sweep = Box::pin(tokio::time::sleep_until(if sweep_pending {
                tokio::time::Instant::now() + sweep_backoff
            } else {
                next_full_sweep_deadline(runtime.full_sweep_interval, runtime.full_sweep_time_of_day)
            }));
            let mut syncs = tokio::task::JoinSet::new();
            loop {
                if !dispatch_due_resyncs(&net, &mut syncs, runtime) {
                    break;
                }
                let next_due = net
                    .upgrade()
                    .map(|current| next_resync_wake(&current.resync_scheduler, syncs.len()))
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
                    _ = &mut full_sweep, if sweep_pending || !runtime.full_sweep_interval.is_zero() => {
                        sweep_pending = net.upgrade().is_some_and(|current| {
                            current.schedule_full_sweep_resync().inspect_err(|error| {
                                tracing::warn!(%error, "failed to schedule full resync sweep");
                            }).is_err()
                        });
                        let delay = if sweep_pending {
                            sweep_backoff = sweep_backoff.saturating_mul(2)
                                .min(runtime.resync_max_backoff.max(Duration::from_millis(1)));
                            sweep_backoff
                        } else {
                            sweep_backoff = runtime.resync_initial_backoff.max(Duration::from_millis(1));
                            runtime.full_sweep_interval
                        };
                        full_sweep.as_mut().reset(tokio::time::Instant::now() + delay);
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
            // Draining lets every aborted batch release its own claims before a
            // replacement loop may start.
            while syncs.join_next().await.is_some() {}
        })))
    }

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

        if !needs_sync && result.is_ok() {
            self.resync_scheduler.complete_clean(claim.settle());
            return;
        }

        match result {
            Ok(()) if advanced => self
                .resync_scheduler
                .complete_dirty(claim.settle(), RESYNC_PROGRESS_TURN),
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
        Ok(self.node.sync_peers(topic_id, &state).contains(&peer_id))
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

    fn schedule_startup_resync(&self) -> io::Result<usize> {
        self.schedule_full_sweep_resync()
    }

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

    fn schedule_full_sweep_resync(&self) -> io::Result<usize> {
        // The sweep is the one pass that revisits every topic, so let it audit
        // stored records again rather than reuse an earlier whole verdict, and
        // clear damage no peer can repair before planning the resyncs.
        // Maintenance is separate from scheduling: durable work owed for
        // healthy topics must be scheduled even when maintenance cannot run.
        if let Err(error) = self.node.recheck_topics() {
            tracing::warn!(%error, "sweep could not refresh topic caches");
        }
        match self.node.quarantine_topics() {
            Ok(evictions) => self.forward_evictions(evictions),
            Err(error) => tracing::warn!(%error, "sweep could not quarantine topics"),
        }
        let mut scheduled = self.schedule_persisted_obligations()?;
        for (peer_id, topic_id) in self.full_sweep_resync_targets()? {
            self.resync_scheduler.schedule_now(peer_id, topic_id, true);
            scheduled += 1;
        }
        Ok(scheduled)
    }

    pub async fn sync_with(
        &self,
        peer: iroh::EndpointAddr,
        messages: &[SyncMessage],
    ) -> io::Result<Vec<SyncMessage>> {
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
            write_sync_messages(&mut send, messages, self.runtime.sync_io_timeout).await?;
            read_sync_messages(&mut recv, self.runtime.sync_io_timeout).await
        })
        .await
        .map_err(|_| timed_out("sync exchange timed out"))?
    }

    pub async fn sync_now(
        &self,
        peer: iroh::EndpointAddr,
        topic_id: crate::TopicId,
    ) -> io::Result<()> {
        let remote_peer_id = peer_id_from_endpoint_id(peer.id);
        let endpoint_id = peer.id;
        // A bounded page is not the goal: keep paging while the exchange really
        // advances, up to a caller budget, so catching up is not reported as an
        // I/O error merely because another page is needed.
        let mut result = Ok(());
        for _ in 0..MAX_SYNC_NOW_PAGES {
            let mut outcomes = self.run_topic_batch(peer.clone(), &[topic_id], None).await;
            result = outcomes.results.remove(&topic_id).unwrap_or(Ok(()));
            if result.is_err() || !outcomes.advanced.contains(&topic_id) {
                break;
            }
        }
        if result.is_err() {
            // Drops the pooled connection only when it is already closed.
            let _ = self.pool.get(&endpoint_id);
        }
        let record_result = match &result {
            Ok(()) => Ok(()),
            Err(error) => Err(error),
        };
        let _ = self
            .node
            .record_sync_result(remote_peer_id, topic_id, record_result);
        // A manual sync holds no claim, so it reports evidence instead of
        // completing an attempt the resync loop may own.
        self.reconsider_target(remote_peer_id, topic_id);
        result
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
        let addr = match peer_id_to_endpoint_addr(peer_id) {
            Ok(addr) => addr,
            Err(error) => {
                for claim in lease.drain_claims() {
                    self.finish_resync_attempt(claim, Err(&error), runtime, false);
                }
                return;
            }
        };
        let claimed = lease.targets();
        let mut topics = Vec::with_capacity(claimed.len());
        for target in claimed {
            let topic_id = target.key.topic_id;
            match self.should_attempt_resync_target(target) {
                Ok(true) => topics.push(topic_id),
                Ok(false) => {
                    if let Err(error) = self.gc_stale_obligations(peer_id, topic_id) {
                        tracing::warn!(%peer_id, %topic_id, %error, "failed to gc stale sync obligations");
                    }
                    if let Some(claim) = lease.take_claim(&target.key) {
                        self.resync_scheduler.complete_clean(claim.settle());
                    }
                }
                Err(error) => {
                    if let Some(claim) = lease.take_claim(&target.key) {
                        self.finish_resync_attempt(claim, Err(&error), runtime, false);
                    }
                }
            }
        }
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
                for claim in lease.drain_claims() {
                    let _ =
                        self.node
                            .record_sync_result(peer_id, claim.key().topic_id, Err(&error));
                    self.finish_resync_attempt(claim, Err(&error), runtime, false);
                }
                return;
            }
        }
    }

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
        let mut failures = 0_usize;
        let mut first_error = None;
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
                self.publish_topic_result(
                    remote_peer_id,
                    *topic_id,
                    outcome,
                    outcomes.advanced.contains(topic_id),
                    lease,
                    runtime,
                );
            }
        }
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
        for topic_id in topic_ids {
            match self.node.sync_fingerprint(*topic_id) {
                Ok(fingerprint) => {
                    request.push(SyncMessage::Open(self.node.sync_open(*topic_id)));
                    fingerprints.insert(*topic_id, fingerprint.fingerprint);
                    request.push(SyncMessage::Fingerprint(fingerprint));
                }
                Err(error) => {
                    outcomes.insert(*topic_id, Err(invalid_data(error)));
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

        let mut matched = BTreeSet::new();
        let mut incomplete = BTreeSet::new();
        let mut summaries = BTreeMap::new();
        for response in responses {
            match response {
                SyncMessage::Fingerprint(remote) => {
                    if fingerprints.get(&remote.topic_id) != Some(&remote.fingerprint) {
                        continue;
                    }
                    // Two identically damaged stores still match, so the local
                    // integrity check decides whether this counts as synced.
                    if self.topic_is_whole(remote.topic_id) {
                        matched.insert(remote.topic_id);
                    } else {
                        incomplete.insert(remote.topic_id);
                    }
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
        for topic_id in &matched {
            summaries.remove(topic_id);
            let outcome = self
                .node
                .record_fingerprint(remote_peer_id, *topic_id, fingerprints[topic_id])
                .map_err(invalid_data)
                .and_then(|matched| {
                    if matched {
                        Ok(())
                    } else {
                        Err(invalid_data("topic changed during fingerprint exchange"))
                    }
                });
            outcomes.insert(*topic_id, outcome);
        }
        for topic_id in &incomplete {
            summaries.remove(topic_id);
            outcomes.insert(
                *topic_id,
                Err(invalid_data(
                    "local topic is incomplete despite a matching fingerprint",
                )),
            );
        }
        for topic_id in fingerprints.keys() {
            if !matched.contains(topic_id)
                && !incomplete.contains(topic_id)
                && !summaries.contains_key(topic_id)
            {
                outcomes
                    .entry(*topic_id)
                    .or_insert_with(|| Err(invalid_data("peer did not return a sync summary")));
            }
        }

        let mut pending = Vec::with_capacity(summaries.len());
        for (topic_id, summary) in summaries {
            match self.plan_topic_messages(remote_peer_id, topic_id, &summary) {
                Ok(Some(planned)) => pending.push(planned),
                Ok(None) => {
                    outcomes.insert(topic_id, Ok(()));
                }
                Err(error) => {
                    outcomes.insert(topic_id, Err(error));
                }
            }
        }

        let mut group = Vec::new();
        let mut group_messages = 0_usize;
        let mut group_responses = 0_usize;
        let mut group_bytes = 0_usize;
        for planned in pending {
            let bytes = match planned.messages.iter().try_fold(0usize, |bytes, message| {
                super::framed_message_len(message).map(|len| bytes.saturating_add(len))
            }) {
                Ok(bytes) if bytes <= MAX_SYNC_STREAM_BYTES => bytes,
                Ok(_) => {
                    outcomes.insert(
                        planned.topic_id,
                        Err(invalid_data("sync plan exceeds stream byte limit")),
                    );
                    continue;
                }
                Err(error) => {
                    outcomes.insert(planned.topic_id, Err(error));
                    continue;
                }
            };
            if !group.is_empty()
                && (group_bytes.saturating_add(bytes) > MAX_SYNC_STREAM_BYTES
                    || group_messages + planned.messages.len() > MAX_BATCH_STREAM_MESSAGES
                    || group_responses + planned.estimated_responses > MAX_BATCH_STREAM_MESSAGES)
            {
                self.run_topic_batch_exchange(
                    peer.clone(),
                    remote_peer_id,
                    std::mem::take(&mut group),
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
                    );
                }
                group_messages = 0;
                group_responses = 0;
                group_bytes = 0;
            }
            group_bytes += bytes;
            group_messages += planned.messages.len();
            group_responses += planned.estimated_responses;
            group.push(planned);
        }
        if !group.is_empty() {
            self.run_topic_batch_exchange(
                peer,
                remote_peer_id,
                group,
                &mut outcomes,
                &mut advanced,
            )
            .await;
        }
        if let Some((lease, runtime)) = settle.as_mut() {
            self.settle_known_results(
                remote_peer_id,
                &outcomes,
                &advanced,
                &mut settled,
                lease,
                *runtime,
            );
        }
        BatchOutcomes::new(outcomes, advanced, settled)
    }

    fn plan_topic_messages(
        &self,
        remote_peer_id: PeerId,
        topic_id: crate::TopicId,
        summary: &SyncSummary,
    ) -> io::Result<Option<PlannedTopicSync>> {
        let (mut plan, _) = self
            .node
            .negotiate_page(
                remote_peer_id,
                summary,
                crate::sync::PageBudget::from_credit(crate::sync::SyncCredit::default()),
            )
            .map_err(invalid_data)?;
        let mut terminal = false;
        if let Some(state) = self
            .node
            .storage()
            .topic_state(&topic_id)
            .map_err(invalid_data)?
            && !state.members.contains(&self.node.peer_id())
            && let Some(op_id) = self.local_leave_op(&state)?
        {
            terminal = true;
            plan.send = self
                .node
                .response_page(
                    remote_peer_id,
                    &crate::sync::SyncRequest {
                        topic_id,
                        known: plan.common.clone(),
                        wants: BTreeSet::from([op_id]),
                        actor_range_hints: Vec::new(),
                        genesis: None,
                        credit: crate::sync::SyncCredit::default(),
                    },
                    crate::sync::PageBudget::from_credit(crate::sync::SyncCredit::default()),
                )
                .map_err(invalid_data)?
                .ops;
            plan.need.clear();
            plan.actor_range_hints.clear();
        }
        let request = crate::sync::SyncRequest {
            topic_id: plan.topic_id,
            known: plan.common,
            wants: plan.need,
            actor_range_hints: plan.actor_range_hints,
            genesis: summary.genesis,
            credit: crate::sync::SyncCredit::default(),
        };
        let mut messages = vec![SyncMessage::Open(self.node.sync_open(topic_id))];
        messages.extend(sync_data_messages(plan.topic_id, plan.send)?);
        let data_count = messages.len() - 1;
        let wants = !request.wants.is_empty() || !request.actor_range_hints.is_empty();
        let requested_ops = request.wants.len() as u64
            + request
                .actor_range_hints
                .iter()
                .map(|hint| hint.to_inclusive.saturating_sub(hint.from_exclusive))
                .sum::<u64>();
        if wants {
            messages.push(SyncMessage::Request(request));
        }
        if !terminal {
            messages.push(SyncMessage::Summary(
                self.node.sync_summary(topic_id).map_err(invalid_data)?,
            ));
        }
        // One summary per open, one ack per data message we send, plus the
        // data messages the peer may send for what we requested.
        let estimated_responses = 1
            + data_count
            + if wants {
                requested_ops.div_ceil(MAX_SYNC_DATA_OPS_PER_MESSAGE as u64) as usize + 1
            } else {
                0
            };
        Ok(Some(PlannedTopicSync {
            topic_id,
            remote_clock: if terminal {
                crate::ActorClock::new()
            } else {
                summary.actor_clock.clone()
            },
            messages,
            estimated_responses,
        }))
    }

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
        let remote_clocks = group
            .iter()
            .map(|planned| (planned.topic_id, planned.remote_clock.clone()))
            .collect::<BTreeMap<_, _>>();
        // Progress is read from durable local state before the exchange. Bytes
        // moved, repeated ids and unchanged cursors are not progress.
        let before = group_topics
            .iter()
            .map(|topic_id| {
                (
                    *topic_id,
                    self.topic_progress_mark(remote_peer_id, *topic_id),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut expected_acks = BTreeMap::<crate::TopicId, usize>::new();
        let mut expected_data = BTreeSet::new();
        let mut messages = Vec::new();
        for planned in group {
            for message in &planned.messages {
                match message {
                    SyncMessage::Data(_) => {
                        *expected_acks.entry(planned.topic_id).or_default() += 1
                    }
                    SyncMessage::Request(_) => {
                        expected_data.insert(planned.topic_id);
                    }
                    _ => {}
                }
            }
            messages.extend(planned.messages);
        }
        let fail_group = |outcomes: &mut BTreeMap<crate::TopicId, io::Result<()>>,
                          error: &io::Error| {
            for topic_id in &group_topics {
                outcomes.insert(*topic_id, Err(clone_error(error)));
            }
        };
        let responses = match self.sync_with(peer.clone(), &messages).await {
            Ok(responses) => responses,
            Err(error) => {
                fail_group(outcomes, &error);
                return;
            }
        };

        let mut acks = Vec::new();
        let mut followups: BTreeMap<crate::TopicId, Vec<SyncMessage>> = BTreeMap::new();
        for response in responses {
            match response {
                SyncMessage::Ack(ack) => {
                    // An ack that is not validly bound fails its own topic; the
                    // other topics in the stream keep their valid work.
                    if ack.peer_id != remote_peer_id {
                        let error = invalid_data("sync ack does not match remote peer");
                        if group_topics.contains(&ack.topic_id) {
                            outcomes.insert(ack.topic_id, Err(error));
                        }
                        continue;
                    }
                    if !group_topics.contains(&ack.topic_id) {
                        tracing::warn!(topic_id = %ack.topic_id, "ignoring ack outside the exchange");
                        continue;
                    }
                    if let Some(remaining) = expected_acks.get_mut(&ack.topic_id) {
                        *remaining = remaining.saturating_sub(1);
                    }
                    acks.push(ack);
                }
                SyncMessage::Failure(failure) if group_topics.contains(&failure.topic_id) => {
                    outcomes.insert(failure.topic_id, Err(topic_failed(&failure)));
                }
                SyncMessage::Summary(summary) if group_topics.contains(&summary.topic_id) => {}
                SyncMessage::Request(request) if group_topics.contains(&request.topic_id) => {
                    let topic_id = request.topic_id;
                    match self
                        .node
                                                .response_page(remote_peer_id, &request, crate::sync::PageBudget::from_credit(crate::sync::SyncCredit::default()))
                        .map_err(invalid_data)
                        .and_then(|data| sync_data_messages(topic_id, data.ops))
                    {
                        Ok(messages) => followups.entry(topic_id).or_default().extend(messages),
                        Err(error) => {
                            outcomes.insert(topic_id, Err(error));
                        }
                    }
                }
                SyncMessage::Data(data) if group_topics.contains(&data.topic_id) => {
                    let data_topic_id = data.topic_id;
                    if !data.ops.is_empty() {
                        expected_data.remove(&data_topic_id);
                    }
                    match self
                        .node
                        .receive_sync_data_from_evicting(remote_peer_id, data)
                    {
                        Ok((ack, evictions)) => {
                            self.forward_evictions(evictions);
                            if let Err(error) = self.schedule_topic_recheck(data_topic_id) {
                                tracing::warn!(%data_topic_id, %error, "failed to schedule received topic resync");
                            }
                            followups
                                .entry(data_topic_id)
                                .or_default()
                                .push(SyncMessage::Ack(ack));
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
                    fail_group(outcomes, &error);
                    return;
                }
            }
        }
        let ack_results = self.node.apply_sync_acks(&acks);
        for (ack, result) in acks.iter().zip(ack_results) {
            if let Err(error) = result {
                outcomes.insert(ack.topic_id, Err(invalid_data(error)));
            }
        }

        let mut followup_groups: Vec<(BTreeSet<crate::TopicId>, Vec<SyncMessage>)> = Vec::new();
        let mut current_topics = BTreeSet::new();
        let mut current_messages: Vec<SyncMessage> = Vec::new();
        for (topic_id, topic_acks) in followups {
            if matches!(outcomes.get(&topic_id), Some(Err(_))) {
                continue;
            }
            if !current_messages.is_empty()
                && (current_messages.len() + topic_acks.len() + 1 > MAX_BATCH_STREAM_MESSAGES
                    || topic_acks
                        .iter()
                        .any(|message| matches!(message, SyncMessage::Data(_)))
                    || current_messages
                        .iter()
                        .any(|message| matches!(message, SyncMessage::Data(_))))
            {
                followup_groups.push((
                    std::mem::take(&mut current_topics),
                    std::mem::take(&mut current_messages),
                ));
            }
            current_messages.push(SyncMessage::Open(self.node.sync_open(topic_id)));
            current_messages.extend(topic_acks);
            current_topics.insert(topic_id);
        }
        if !current_messages.is_empty() {
            followup_groups.push((current_topics, current_messages));
        }
        for (topics, messages) in followup_groups {
            let mut summaries = topics.clone();
            let mut remaining = BTreeMap::<crate::TopicId, usize>::new();
            for message in &messages {
                if let SyncMessage::Data(data) = message {
                    *remaining.entry(data.topic_id).or_default() += 1;
                }
            }
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
                                } else {
                                    if let Some(count) = remaining.get_mut(&ack.topic_id) {
                                        *count = count.saturating_sub(1);
                                    }
                                    for result in
                                        self.node.apply_sync_acks(std::slice::from_ref(&ack))
                                    {
                                        if let Err(error) = result {
                                            outcomes.insert(ack.topic_id, Err(invalid_data(error)));
                                        }
                                    }
                                }
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
            for topic_id in summaries {
                outcomes.insert(
                    topic_id,
                    Err(invalid_data("peer omitted sync acknowledgement summary")),
                );
            }
            for (topic_id, count) in remaining {
                if count > 0 {
                    outcomes.insert(
                        topic_id,
                        Err(invalid_data("peer omitted sync acknowledgement")),
                    );
                }
            }
        }

        for topic_id in group_topics {
            if outcomes.contains_key(&topic_id) {
                continue;
            }
            let owes_more = (|| -> io::Result<bool> {
                Ok(
                    expected_acks.get(&topic_id).copied().unwrap_or_default() > 0
                    || expected_data.contains(&topic_id)
                    || !self
                        .node
                        .sync_summary(topic_id)
                        .map_err(invalid_data)?
                        .actor_clock
                        .dominates(&remote_clocks[&topic_id])
                    || !self.topic_is_whole(topic_id)
                    // Only unsent obligations remain: a missing peer ack is not
                    // this exchange's failure when it had nothing to push.
                    || self
                        .node
                        .storage()
                        .has_sync_obligations(&remote_peer_id, &topic_id)
                        .map_err(invalid_data)?,
                )
            })();
            let outcome = match owes_more {
                Ok(false) => Ok(()),
                Ok(true) => {
                    self.schedule_resync(remote_peer_id, topic_id);
                    if self.topic_progress_mark(remote_peer_id, topic_id) == before[&topic_id] {
                        // Nothing durable moved, so retrying at once would spin.
                        // Back off with an explicit reason instead.
                        Err(invalid_data("sync exchange made no progress"))
                    } else {
                        // A bounded page that really advanced is served again
                        // after a fair turn; it is not a failed attempt.
                        advanced.insert(topic_id);
                        Ok(())
                    }
                }
                Err(error) => Err(error),
            };
            outcomes.insert(topic_id, outcome);
        }
    }

    /// Publishes every decided outcome this batch has not published yet:
    /// records it and releases the claim the batch owns for it. Called between
    /// exchanges, so ownership of finished work is handed back immediately.
    fn settle_known_results(
        &self,
        remote_peer_id: PeerId,
        outcomes: &BTreeMap<crate::TopicId, io::Result<()>>,
        advanced: &BTreeSet<crate::TopicId>,
        settled: &mut BTreeSet<crate::TopicId>,
        lease: &mut ResyncLease,
        runtime: IrohRuntimeConfig,
    ) {
        for (topic_id, outcome) in outcomes {
            if !settled.insert(*topic_id) {
                continue;
            }
            self.publish_topic_result(
                remote_peer_id,
                *topic_id,
                outcome,
                advanced.contains(topic_id),
                lease,
                runtime,
            );
        }
    }

    /// Records one topic's attempt and completes the claim this batch holds for
    /// it, if any.
    fn publish_topic_result(
        &self,
        remote_peer_id: PeerId,
        topic_id: crate::TopicId,
        outcome: &io::Result<()>,
        advanced: bool,
        lease: &mut ResyncLease,
        runtime: IrohRuntimeConfig,
    ) {
        let record_result = outcome.as_ref().copied();
        let _ = self
            .node
            .record_sync_result(remote_peer_id, topic_id, record_result);
        if let Some(claim) = lease.take_claim(&ResyncTargetKey {
            peer_id: remote_peer_id,
            topic_id,
        }) {
            self.finish_resync_attempt(claim, record_result, runtime, advanced);
        }
    }

    /// Durable state a topic exchange can be judged against: the local clock,
    /// whether the topic is whole, and whether work is still owed to this peer.
    /// A change in any of these is real progress; an unchanged mark is not.
    fn topic_progress_mark(
        &self,
        peer_id: PeerId,
        topic_id: crate::TopicId,
    ) -> (Option<crate::ActorClock>, bool, bool) {
        (
            self.node.storage().actor_clock(&topic_id).ok(),
            self.topic_is_whole(topic_id),
            self.node
                .storage()
                .has_sync_obligations(&peer_id, &topic_id)
                .unwrap_or(true),
        )
    }

    pub async fn accept_one(&self) -> io::Result<Option<iroh::EndpointId>> {
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

    pub async fn handle_stream(
        &self,
        peer: iroh::EndpointId,
        mut recv: iroh::endpoint::RecvStream,
        mut send: iroh::endpoint::SendStream,
    ) -> io::Result<()> {
        tokio::time::timeout(self.runtime.sync_io_timeout, async {
            let mut session = SyncSession::new(peer);
            let mut limits = SyncReadLimits::default();
            let mut responses = Vec::new();
            let mut response_limits = SyncReadLimits::default();
            while let Some(frame) = read_next_frame(&mut recv, self.runtime.sync_io_timeout).await?
            {
                let frame_index = limits.observe_frame(frame.len())?;
                let message = decode_sync_message(&frame).map_err(|err| {
                    invalid_data(format!(
                        "invalid sync message frame {frame_index} ({} bytes): {err}",
                        frame.len()
                    ))
                })?;
                if push_responses(
                    &mut responses,
                    session.handle(self, message)?,
                    &mut response_limits,
                )? {
                    tracing::debug!(
                        %peer,
                        "reply reached the stream budget; sending the legal prefix"
                    );
                    break;
                }
            }
            let _ = push_responses(&mut responses, session.finish(self)?, &mut response_limits)?;
            write_sync_messages(&mut send, &responses, self.runtime.sync_io_timeout).await?;
            Ok(())
        })
        .await
        .map_err(|_| timed_out("sync stream timed out"))?
    }

    pub fn handle_messages(
        &self,
        peer: iroh::EndpointId,
        messages: Vec<SyncMessage>,
    ) -> io::Result<Vec<SyncMessage>> {
        let mut session = SyncSession::new(peer);
        let mut responses = Vec::new();
        let mut response_limits = SyncReadLimits::default();
        for message in messages {
            if push_responses(
                &mut responses,
                session.handle(self, message)?,
                &mut response_limits,
            )? {
                break;
            }
        }
        let _ = push_responses(&mut responses, session.finish(self)?, &mut response_limits)?;
        Ok(responses)
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
                if let Some(state) = self
                    .node
                    .storage()
                    .topic_state(&open.topic_id)
                    .map_err(invalid_data)?
                    && !peer_may_open_topic(&state, peer_id)
                {
                    return Ok(Vec::new());
                }
                // Unknown topics return an empty local summary so an inviter can
                // bootstrap a new member by pushing the signed genesis/history.
                self.node
                    .sync_summary(open.topic_id)
                    .map(SyncMessage::Summary)
                    .map(|message| vec![message])
                    .map_err(invalid_data)
            }
            SyncMessage::Fingerprint(fingerprint) => {
                let peer_id = remote_peer_id.ok_or_else(|| {
                    invalid_data("sync fingerprint requires a preceding SyncOpen with peer_id")
                })?;
                let Some(state) = self
                    .node
                    .storage()
                    .topic_state(&fingerprint.topic_id)
                    .map_err(invalid_data)?
                else {
                    return Ok(Vec::new());
                };
                if !peer_may_open_topic(&state, peer_id) {
                    return Ok(Vec::new());
                }
                let local = self
                    .node
                    .sync_fingerprint(fingerprint.topic_id)
                    .map_err(invalid_data)?;
                // A damaged responder must fall through to the summary path so
                // the requester can serve what this side cannot resolve.
                if local.fingerprint == fingerprint.fingerprint
                    && self.topic_is_whole(fingerprint.topic_id)
                    && (!state.members.contains(&peer_id)
                        || self
                            .node
                            .record_fingerprint(
                                peer_id,
                                fingerprint.topic_id,
                                fingerprint.fingerprint,
                            )
                            .map_err(invalid_data)?)
                {
                    if state.members.contains(&peer_id) {
                        self.reconsider_target(peer_id, fingerprint.topic_id);
                    }
                    Ok(vec![SyncMessage::Fingerprint(local)])
                } else {
                    self.node
                        .sync_summary(fingerprint.topic_id)
                        .map(SyncMessage::Summary)
                        .map(|message| vec![message])
                        .map_err(invalid_data)
                }
            }
            SyncMessage::Summary(summary) => {
                let peer_id = remote_peer_id.ok_or_else(|| {
                    invalid_data("sync summary requires a preceding SyncOpen with peer_id")
                })?;
                let (plan, _) = self
                    .node
                    .negotiate_page(
                        peer_id,
                        &summary,
                        crate::sync::PageBudget::from_credit(crate::sync::SyncCredit::default()),
                    )
                    .map_err(invalid_data)?;
                let mut responses = Vec::new();
                if !plan.send.is_empty() {
                    responses.extend(sync_data_messages(plan.topic_id, plan.send)?);
                }
                if !plan.need.is_empty() || !plan.actor_range_hints.is_empty() {
                    responses.push(SyncMessage::Request(crate::sync::SyncRequest {
                        topic_id: plan.topic_id,
                        known: plan.common,
                        wants: plan.need,
                        actor_range_hints: plan.actor_range_hints,
                        genesis: summary.genesis,
                        credit: crate::sync::SyncCredit::default(),
                    }));
                }
                Ok(responses)
            }
            SyncMessage::Request(request) => {
                let peer_id = remote_peer_id.ok_or_else(|| {
                    invalid_data("sync request requires a preceding SyncOpen with peer_id")
                })?;
                let data = self
                    .node
                    .response_page(
                        peer_id,
                        &request,
                        crate::sync::PageBudget::from_credit(crate::sync::SyncCredit::default()),
                    )
                    .map_err(invalid_data)?;
                sync_data_messages(request.topic_id, data.ops)
            }
            SyncMessage::Data(data) => {
                let data_topic_id = data.topic_id;
                let source_peer = remote_peer_id.ok_or_else(|| {
                    invalid_data("sync data requires a preceding SyncOpen with peer_id")
                })?;
                self.node
                    .ensure_iroh_peer_whitelisted(source_peer, &data)
                    .map_err(invalid_data)?;
                let (ack, evictions) = self
                    .node
                    .receive_sync_data_from_evicting(source_peer, data)
                    .map_err(|mut error| {
                        if let crate::Error::ReceiveCommitted { evictions, .. } = &mut error {
                            self.forward_evictions(std::mem::take(evictions));
                            if let Err(retry) = self.schedule_topic_recheck(data_topic_id) {
                                tracing::warn!(%data_topic_id, %retry, "failed to schedule received topic resync");
                            }
                        }
                        invalid_data(error)
                    })?;
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
            SyncMessage::Failure(_) | SyncMessage::Page(_) => Err(invalid_data(
                "sync failure and page are response-only messages",
            )),
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
        SyncMessage::Ack(_) | SyncMessage::Failure(_) | SyncMessage::Page(_) => return None,
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

struct PlannedTopicSync {
    topic_id: crate::TopicId,
    remote_clock: crate::ActorClock,
    messages: Vec<SyncMessage>,
    estimated_responses: usize,
}

struct SyncSession {
    authenticated_peer_id: PeerId,
    remote_peer_id: Option<PeerId>,
    open_topic_id: Option<crate::TopicId>,
    open_allowed: bool,
    acks: Vec<crate::sync::SyncAck>,
}

impl SyncSession {
    fn new(peer: iroh::EndpointId) -> Self {
        Self {
            authenticated_peer_id: peer_id_from_endpoint_id(peer),
            remote_peer_id: None,
            open_topic_id: None,
            open_allowed: false,
            acks: Vec::new(),
        }
    }

    fn handle<S: Storage>(
        &mut self,
        net: &IrohNet<S>,
        message: SyncMessage,
    ) -> io::Result<Vec<SyncMessage>> {
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
                return Ok(Vec::new());
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
            return Ok(vec![SyncMessage::Failure(crate::sync::SyncFailure {
                topic_id: message_topic_id(&message)
                    .ok_or_else(|| invalid_data("sync message requires a topic"))?,
                code: crate::sync::SyncFailureCode::Open,
            })]);
        }

        if let SyncMessage::Data(data) = &message
            && data.ops.len() > MAX_SYNC_DATA_OPS_PER_MESSAGE
        {
            return Err(invalid_data("sync data has too many operations"));
        }

        if let SyncMessage::Ack(ack) = message {
            self.acks.push(ack);
            return Ok(Vec::new());
        }

        // A data-plane failure for one topic must not abort the whole stream:
        // the other topics batched into it would lose their summaries, data and
        // acks, the caller marks every one of them failed, and the sender
        // resends the identical ranges forever. It must not read as success
        // either, so the topic gets an explicit terminal failure. Framing and
        // authentication failures above stay fatal - those indict the peer.
        if let Some(failure) = per_topic_failure_scope(&message) {
            return match net.handle_message(message, self.remote_peer_id) {
                Ok(responses) => Ok(responses),
                Err(error) => {
                    let topic_id = failure.topic_id;
                    tracing::warn!(%topic_id, %error, "failing one sync topic");
                    Ok(vec![SyncMessage::Failure(failure)])
                }
            };
        }

        net.handle_message(message, self.remote_peer_id)
    }

    /// Apply the stream's acks independently. One rejected ack - a stale clock
    /// after a topic reset, or one bound to another peer - must not discard the
    /// others, or their obligations never clear and the peer resends the same
    /// ranges forever. Each rejection is reported against its own topic.
    fn finish<S: Storage>(&mut self, net: &IrohNet<S>) -> io::Result<Vec<SyncMessage>> {
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
                Ok(()) => net.reconsider_target(peer_id, ack.topic_id),
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

#[derive(Default)]
struct SyncReadLimits {
    messages: usize,
    bytes: usize,
}

impl SyncReadLimits {
    /// Whether one more frame of `frame_len` fits in the stream budget. Used
    /// for outgoing replies, where crossing the cap must stop the reply rather
    /// than fail it: the prefix already built is legal and useful.
    fn accept_frame(&mut self, frame_len: usize) -> io::Result<bool> {
        if self.messages >= MAX_SYNC_MESSAGES_PER_STREAM {
            return Ok(false);
        }
        let total = self
            .bytes
            .checked_add(frame_len + 4)
            .ok_or_else(|| invalid_data("sync stream byte count overflow"))?;
        if total > MAX_SYNC_STREAM_BYTES {
            return Ok(false);
        }
        self.bytes = total;
        self.messages += 1;
        Ok(true)
    }

    fn observe_frame(&mut self, frame_len: usize) -> io::Result<usize> {
        if self.messages >= MAX_SYNC_MESSAGES_PER_STREAM {
            return Err(invalid_data("sync stream has too many messages"));
        }
        self.bytes = self
            .bytes
            .checked_add(frame_len + 4)
            .ok_or_else(|| invalid_data("sync stream byte count overflow"))?;
        if self.bytes > MAX_SYNC_STREAM_BYTES {
            return Err(invalid_data("sync stream exceeds maximum byte length"));
        }
        let frame_index = self.messages;
        self.messages += 1;
        Ok(frame_index)
    }
}

/// Appends the replies that fit in the stream budget and reports whether any
/// were left out. A reply that would cross the cap belongs to the next
/// exchange: failing here would discard a legal prefix the peer can use and
/// turn a bounded page into a protocol error.
fn push_responses(
    out: &mut Vec<SyncMessage>,
    responses: Vec<SyncMessage>,
    limits: &mut SyncReadLimits,
) -> io::Result<bool> {
    for response in responses {
        let frame_len = super::framed_message_len(&response)? - 4;
        if !limits.accept_frame(frame_len)? {
            return Ok(true);
        }
        out.push(response);
    }
    Ok(false)
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
    let free = MAX_RESYNC_PEER_CONCURRENCY.saturating_sub(syncs.len());
    if free == 0 {
        return true;
    }
    let due = current
        .resync_scheduler
        .due_targets_by_peer(free, MAX_TOPICS_PER_RESYNC_BATCH);
    for (peer_id, targets) in due {
        // The lease owns the claims before the task is spawned, so an abort
        // releases them instead of wedging the targets in flight.
        let lease = current
            .resync_scheduler
            .lease(targets, runtime.resync_interval);
        let peer_net = Arc::clone(&current);
        let task = current.tasks.track();
        syncs.spawn(async move {
            let _task = task;
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
                    } else {
                        current
                            .resync_scheduler
                            .peer_reachable(peer_id_from_endpoint_id(peer));
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
) -> io::Result<Vec<SyncMessage>> {
    let mut messages = Vec::new();
    let mut limits = SyncReadLimits::default();
    while let Some(frame) = read_next_frame(recv, sync_io_timeout).await? {
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
) -> io::Result<()> {
    let mut limits = SyncReadLimits::default();
    for message in messages {
        limits.observe_frame(super::framed_message_len(message)? - 4)?;
    }
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

async fn read_next_frame(
    recv: &mut iroh::endpoint::RecvStream,
    sync_io_timeout: Duration,
) -> io::Result<Option<Vec<u8>>> {
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
    let mut payload = vec![0_u8; len];
    if len > 0 {
        tokio::time::timeout(sync_io_timeout, recv.read_exact(&mut payload))
            .await
            .map_err(|_| timed_out("sync read timed out"))?
            .map_err(other)?;
    }
    Ok(Some(payload))
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
    }
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

    /// A reply that reaches the stream budget keeps the legal prefix instead of
    /// failing the whole response. Exercised here through the message cap; the
    /// byte cap takes the same branch, but a fixture at the default 256 MiB
    /// reply limit is not run as a unit test.
    #[test]
    fn reply_cap_keeps_prefix() {
        let mut limits = SyncReadLimits::default();
        let mut out = Vec::new();
        let responses = (0..=MAX_SYNC_MESSAGES_PER_STREAM)
            .map(|index| {
                SyncMessage::Fingerprint(crate::sync::SyncFingerprint {
                    topic_id: topic(index as u8),
                    fingerprint: [0; 32],
                })
            })
            .collect::<Vec<_>>();
        let truncated = push_responses(&mut out, responses, &mut limits).unwrap();
        assert!(truncated, "the reply must report what it left out");
        assert_eq!(
            out.len(),
            MAX_SYNC_MESSAGES_PER_STREAM,
            "the legal prefix must be kept, not discarded"
        );

        // A following push adds nothing and still reports truncation.
        let more = vec![SyncMessage::Fingerprint(crate::sync::SyncFingerprint {
            topic_id: topic(0),
            fingerprint: [1; 32],
        })];
        assert!(push_responses(&mut out, more, &mut limits).unwrap());
        assert_eq!(out.len(), MAX_SYNC_MESSAGES_PER_STREAM);
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
}
