// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use smallvec::{SmallVec, smallvec};

use crate::node::select_sync_peers;
use crate::sync::SyncMessage;
use crate::{Irokle, MemoryStorage, PeerId, Storage};

use super::frame::MAX_FRAME_LEN;
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
const MAX_RESYNC_CONCURRENCY: usize = 8;
const MAX_SYNC_MESSAGES_PER_STREAM: usize = 4096;
const MAX_SYNC_STREAM_BYTES: usize = 256 * 1024 * 1024;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResyncTarget {
    key: ResyncTargetKey,
    force: bool,
}

#[derive(Debug)]
struct ScheduledResync {
    next_due: tokio::time::Instant,
    failures: u32,
    in_flight: bool,
    force: bool,
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
                if !target.in_flight && next_due < target.next_due {
                    target.next_due = next_due;
                }
                target.force |= force;
            }
            None => {
                targets.insert(
                    key,
                    ScheduledResync {
                        next_due,
                        failures: 0,
                        in_flight: false,
                        force,
                    },
                );
            }
        }
        drop(targets);
        self.notify.notify_waiters();
    }

    fn due_targets(&self, limit: usize) -> Vec<ResyncTarget> {
        let now = tokio::time::Instant::now();
        let mut targets = self.inner.lock().expect("resync scheduler lock poisoned");
        let due = targets
            .iter()
            .filter(|(_, target)| !target.in_flight && target.next_due <= now)
            .map(|(key, _)| *key)
            .take(limit)
            .collect::<Vec<_>>();
        let mut out = Vec::with_capacity(due.len());
        for key in due {
            if let Some(target) = targets.get_mut(&key) {
                target.in_flight = true;
                out.push(ResyncTarget {
                    key,
                    force: std::mem::take(&mut target.force),
                });
            }
        }
        out
    }

    fn next_due(&self) -> Option<tokio::time::Instant> {
        self.inner
            .lock()
            .expect("resync scheduler lock poisoned")
            .values()
            .filter(|target| !target.in_flight)
            .map(|target| target.next_due)
            .min()
    }

    fn complete_clean(&self, peer_id: PeerId, topic_id: crate::TopicId) {
        self.inner
            .lock()
            .expect("resync scheduler lock poisoned")
            .remove(&ResyncTargetKey { peer_id, topic_id });
    }

    fn complete_dirty(&self, peer_id: PeerId, topic_id: crate::TopicId, after: Duration) {
        let key = ResyncTargetKey { peer_id, topic_id };
        let next_due = tokio::time::Instant::now() + after;
        let mut targets = self.inner.lock().expect("resync scheduler lock poisoned");
        let target = targets.entry(key).or_insert_with(|| ScheduledResync {
            next_due,
            failures: 0,
            in_flight: false,
            force: false,
        });
        target.next_due = next_due;
        target.failures = 0;
        target.in_flight = false;
        drop(targets);
        self.notify.notify_waiters();
    }

    fn complete_failed(
        &self,
        peer_id: PeerId,
        topic_id: crate::TopicId,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) {
        let key = ResyncTargetKey { peer_id, topic_id };
        let mut targets = self.inner.lock().expect("resync scheduler lock poisoned");
        let target = targets.entry(key).or_insert_with(|| ScheduledResync {
            next_due: tokio::time::Instant::now(),
            failures: 0,
            in_flight: false,
            force: false,
        });
        target.failures = target.failures.saturating_add(1);
        let shift = target.failures.saturating_sub(1).min(20);
        let multiplier = 1_u32 << shift;
        let backoff = initial_backoff.saturating_mul(multiplier).min(max_backoff);
        target.next_due = tokio::time::Instant::now() + backoff;
        target.in_flight = false;
        target.force = false;
        drop(targets);
        self.notify.notify_waiters();
    }
}

#[derive(Clone)]
struct ConnectionPool {
    endpoint: iroh::Endpoint,
    connections: Arc<RwLock<HashMap<iroh::EndpointId, iroh::endpoint::Connection>>>,
}

impl ConnectionPool {
    fn new(endpoint: iroh::Endpoint) -> Self {
        Self {
            endpoint,
            connections: Arc::new(RwLock::new(HashMap::new())),
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

    fn remove(&self, peer: &iroh::EndpointId) -> io::Result<()> {
        self.connections
            .write()
            .map_err(|_| io::Error::other("connection pool write lock poisoned"))?
            .remove(peer);
        Ok(())
    }

    fn get(&self, peer: &iroh::EndpointId) -> io::Result<Option<iroh::endpoint::Connection>> {
        Ok(self
            .connections
            .read()
            .map_err(|_| io::Error::other("connection pool read lock poisoned"))?
            .get(peer)
            .filter(|connection| connection.close_reason().is_none())
            .cloned())
    }

    async fn get_or_connect(
        &self,
        peer: iroh::EndpointAddr,
        connect_timeout: Duration,
    ) -> io::Result<iroh::endpoint::Connection> {
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
    shutdown: tokio::sync::watch::Sender<bool>,
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
            shutdown,
        })
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

    pub async fn shutdown(&self) {
        let _ = self.shutdown.send(true);
        self.endpoint().close().await;
    }

    fn is_shutdown(&self) -> bool {
        *self.shutdown.borrow()
    }

    pub async fn sync_peer_now(&self, peer_id: PeerId, topic_id: crate::TopicId) -> io::Result<()> {
        self.sync_peer_now_with_runtime(peer_id, topic_id, self.runtime)
            .await
    }

    async fn sync_peer_now_with_runtime(
        &self,
        peer_id: PeerId,
        topic_id: crate::TopicId,
        runtime: IrohRuntimeConfig,
    ) -> io::Result<()> {
        let addr = match peer_id_to_endpoint_addr(peer_id) {
            Ok(addr) => addr,
            Err(error) => {
                self.finish_resync_attempt(peer_id, topic_id, Err(&error), runtime);
                return Err(error);
            }
        };
        self.sync_now_with_runtime(addr, topic_id, runtime).await
    }

    pub fn schedule_resync(&self, peer_id: PeerId, topic_id: crate::TopicId) {
        self.resync_scheduler.schedule_now(peer_id, topic_id, false);
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
        self.sync_now_with_runtime(
            iroh::EndpointAddr::from(endpoint_id),
            topic_id,
            self.runtime,
        )
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
        let endpoint = self.endpoint().clone();
        let mut shutdown = self.shutdown.subscribe();
        Ok(Some(handle.spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                while connections.len() >= MAX_ACCEPT_CONNECTIONS {
                    tokio::select! {
                        Some(result) = connections.join_next() => {
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
                    Some(result) = connections.join_next(), if !connections.is_empty() => {
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
                drop(current);
                let accepted = tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                        continue;
                    }
                    accepted = incoming => accepted.map_err(other),
                };
                match accepted {
                    Ok(connection) => {
                        let peer = connection.remote_id();
                        let connection_net = Weak::clone(&net);
                        let connection_shutdown = shutdown.clone();
                        connections.spawn(async move {
                            handle_connection(
                                connection_net,
                                connection_shutdown,
                                peer,
                                connection,
                            )
                            .await;
                        });
                    }
                    Err(error) => {
                        tracing::warn!(%error, "failed to accept iroh connection");
                        continue;
                    }
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
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
        Ok(Some(handle.spawn(async move {
            if let Some(current) = net.upgrade()
                && let Err(error) = current.schedule_startup_resync() {
                    tracing::warn!(%error, "failed to schedule startup resync sweep");
                }
            let mut full_sweep = Box::pin(tokio::time::sleep_until(next_full_sweep_deadline(
                runtime.full_sweep_interval,
                runtime.full_sweep_time_of_day,
            )));
            loop {
                if !run_due_resyncs(&net, &mut shutdown, runtime).await {
                    break;
                }
                let next_due = net
                    .upgrade()
                    .and_then(|current| current.resync_scheduler.next_due())
                    .unwrap_or_else(|| tokio::time::Instant::now() + EMPTY_RESYNC_SLEEP);
                let due_sleep = tokio::time::sleep_until(next_due);
                tokio::pin!(due_sleep);
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                        continue;
                    }
                    _ = notify.notified() => {}
                    _ = &mut due_sleep => {}
                    _ = &mut full_sweep, if !runtime.full_sweep_interval.is_zero() => {
                        if let Some(current) = net.upgrade()
                            && let Err(error) = current.schedule_full_sweep_resync() {
                                tracing::warn!(%error, "failed to schedule full resync sweep");
                            }
                        full_sweep.as_mut().reset(tokio::time::Instant::now() + runtime.full_sweep_interval);
                    }
                }
            }
        })))
    }

    fn finish_resync_attempt(
        &self,
        peer_id: PeerId,
        topic_id: crate::TopicId,
        result: std::result::Result<(), &io::Error>,
        runtime: IrohRuntimeConfig,
    ) {
        let needs_sync = match self.target_needs_sync(peer_id, topic_id) {
            Ok(needs_sync) => needs_sync,
            Err(error) => {
                tracing::warn!(%peer_id, %topic_id, %error, "failed to evaluate resync target");
                true
            }
        };

        if !needs_sync {
            self.resync_scheduler.complete_clean(peer_id, topic_id);
            return;
        }

        match result {
            Ok(()) => {
                self.resync_scheduler
                    .complete_dirty(peer_id, topic_id, runtime.resync_interval)
            }
            Err(_) => self.resync_scheduler.complete_failed(
                peer_id,
                topic_id,
                runtime.resync_initial_backoff,
                runtime.resync_max_backoff,
            ),
        }
    }

    fn should_attempt_resync_target(&self, target: ResyncTarget) -> io::Result<bool> {
        if target.force {
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
        if !state.members.contains(&self.node.peer_id()) || !state.members.contains(&peer_id) {
            return Ok(false);
        }
        Ok(select_sync_peers(topic_id, self.node.peer_id(), &state).contains(&peer_id))
    }

    fn target_needs_sync(&self, peer_id: PeerId, topic_id: crate::TopicId) -> io::Result<bool> {
        let Some(state) = self
            .node
            .storage()
            .topic_state(&topic_id)
            .map_err(invalid_data)?
        else {
            return Ok(false);
        };
        if !state.members.contains(&self.node.peer_id()) || !state.members.contains(&peer_id) {
            return Ok(false);
        }
        if self
            .node
            .storage()
            .has_sync_obligations(&peer_id, &topic_id)
            .map_err(invalid_data)?
        {
            return Ok(true);
        }
        if !select_sync_peers(topic_id, self.node.peer_id(), &state).contains(&peer_id) {
            return Ok(false);
        }
        let local_clock = self
            .node
            .storage()
            .actor_clock(&topic_id)
            .map_err(invalid_data)?;
        let Some(ack) = self
            .node
            .storage()
            .peer_ack(&peer_id, &topic_id)
            .map_err(invalid_data)?
        else {
            return Ok(true);
        };
        Ok(!ack.clock.dominates(&local_clock))
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
        if !state.members.contains(&self.node.peer_id()) {
            return Ok(Vec::new());
        }
        let mut targets = Vec::new();
        for peer_id in select_sync_peers(topic_id, self.node.peer_id(), &state) {
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
        let peer_id = peer.id;
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
            match self.sync_with_connection(connection, messages).await {
                Ok(responses) => return Ok(responses),
                Err(error) => {
                    let _ = self.pool.remove(&peer_id);
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
        let (mut send, mut recv) =
            tokio::time::timeout(self.runtime.sync_io_timeout, connection.open_bi())
                .await
                .map_err(|_| timed_out("sync stream open timed out"))?
                .map_err(other)?;
        write_sync_messages(&mut send, messages, self.runtime.sync_io_timeout).await?;
        read_sync_messages(&mut recv, self.runtime.sync_io_timeout).await
    }

    pub async fn sync_now(
        &self,
        peer: iroh::EndpointAddr,
        topic_id: crate::TopicId,
    ) -> io::Result<()> {
        self.sync_now_with_runtime(peer, topic_id, self.runtime)
            .await
    }

    async fn sync_now_with_runtime(
        &self,
        peer: iroh::EndpointAddr,
        topic_id: crate::TopicId,
        runtime: IrohRuntimeConfig,
    ) -> io::Result<()> {
        let remote_peer_id = peer_id_from_endpoint_id(peer.id);
        let endpoint_id = peer.id;
        let result = self.sync_now_inner(peer, topic_id).await;
        if result.is_err() {
            let _ = self.pool.remove(&endpoint_id);
        }
        let record_result = match &result {
            Ok(()) => Ok(()),
            Err(error) => Err(error),
        };
        let _ = self
            .node
            .record_sync_result(remote_peer_id, topic_id, record_result);
        self.finish_resync_attempt(remote_peer_id, topic_id, record_result, runtime);
        result
    }

    async fn sync_now_inner(
        &self,
        peer: iroh::EndpointAddr,
        topic_id: crate::TopicId,
    ) -> io::Result<()> {
        let remote_peer_id = peer_id_from_endpoint_id(peer.id);
        let local_fingerprint = self.node.sync_fingerprint(topic_id).map_err(invalid_data)?;
        let responses = self
            .sync_with(
                peer.clone(),
                &[
                    SyncMessage::Open(self.node.sync_open(topic_id)),
                    SyncMessage::Fingerprint(local_fingerprint.clone()),
                ],
            )
            .await?;
        let mut summary = None;
        for response in responses {
            match response {
                SyncMessage::Fingerprint(remote)
                    if remote.topic_id == topic_id
                        && remote.fingerprint == local_fingerprint.fingerprint =>
                {
                    self.node
                        .record_peer_synced(remote_peer_id, topic_id)
                        .map_err(invalid_data)?;
                    return Ok(());
                }
                SyncMessage::Summary(remote_summary) if remote_summary.topic_id == topic_id => {
                    summary = Some(remote_summary)
                }
                other => {
                    return Err(invalid_data(format!(
                        "unexpected sync response {}",
                        _message_type_name(&other)
                    )));
                }
            }
        }
        let summary = summary.ok_or_else(|| invalid_data("peer did not return a sync summary"))?;
        let plan = self
            .node
            .negotiate_sync(remote_peer_id, &summary)
            .map_err(invalid_data)?;
        let request = crate::sync::SyncRequest {
            topic_id: plan.topic_id,
            known: plan.common,
            wants: plan.need,
            actor_range_hints: plan.actor_range_hints,
        };

        let mut messages: SmallVec<[SyncMessage; 3]> =
            smallvec![SyncMessage::Open(self.node.sync_open(topic_id))];
        messages.extend(sync_data_messages(plan.topic_id, plan.send));
        if !request.wants.is_empty() || !request.actor_range_hints.is_empty() {
            messages.push(SyncMessage::Request(request));
        }
        if messages.len() == 1 {
            return Ok(());
        }

        let responses = self.sync_with(peer.clone(), &messages).await?;
        let mut followup: SmallVec<[SyncMessage; 2]> =
            smallvec![SyncMessage::Open(self.node.sync_open(topic_id))];
        let mut acks = Vec::new();
        for response in responses {
            match response {
                SyncMessage::Ack(ack) => {
                    if ack.peer_id != remote_peer_id || ack.topic_id != topic_id {
                        return Err(invalid_data("sync ack does not match remote peer/topic"));
                    }
                    acks.push(ack);
                }
                SyncMessage::Summary(summary) if summary.topic_id == topic_id => {}
                SyncMessage::Data(data) => {
                    let data_topic_id = data.topic_id;
                    let ack = self
                        .node
                        .receive_sync_data_from(remote_peer_id, data)
                        .map_err(invalid_data)?;
                    if let Err(error) = self.schedule_topic_recheck(data_topic_id) {
                        tracing::warn!(%data_topic_id, %error, "failed to schedule received topic resync");
                    }
                    followup.push(SyncMessage::Ack(ack));
                }
                other => {
                    return Err(invalid_data(format!(
                        "unexpected sync response {}",
                        _message_type_name(&other)
                    )));
                }
            }
        }
        for ack in acks {
            self.node.apply_sync_ack(&ack).map_err(invalid_data)?;
        }
        if followup.len() > 1 {
            let responses = self.sync_with(peer, &followup).await?;
            for response in responses {
                match response {
                    SyncMessage::Summary(summary) if summary.topic_id == topic_id => {}
                    other => {
                        return Err(invalid_data(format!(
                            "unexpected sync ack response {}",
                            _message_type_name(&other)
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn accept_one(&self) -> io::Result<Option<iroh::EndpointId>> {
        let Some(incoming) = self.endpoint().accept().await else {
            return Ok(None);
        };
        let connection = incoming.await.map_err(other)?;
        let peer = connection.remote_id();
        let (send, recv) = connection.accept_bi().await.map_err(other)?;
        self.handle_stream(peer, recv, send).await?;
        Ok(Some(peer))
    }

    pub async fn handle_stream(
        &self,
        peer: iroh::EndpointId,
        mut recv: iroh::endpoint::RecvStream,
        mut send: iroh::endpoint::SendStream,
    ) -> io::Result<()> {
        let mut session = SyncSession::new(peer);
        let mut limits = SyncReadLimits::default();
        let mut responses = Vec::new();
        while let Some(frame) = read_next_frame(&mut recv, self.runtime.sync_io_timeout).await? {
            let frame_index = limits.observe_frame(frame.len())?;
            let message = decode_sync_message(&frame).map_err(|err| {
                invalid_data(format!(
                    "invalid sync message frame {frame_index} ({} bytes): {err}",
                    frame.len()
                ))
            })?;
            push_responses(&mut responses, session.handle(self, message)?)?;
        }
        push_responses(&mut responses, session.finish(self)?)?;
        write_sync_messages(&mut send, &responses, self.runtime.sync_io_timeout).await?;
        Ok(())
    }

    pub fn handle_messages(
        &self,
        peer: iroh::EndpointId,
        messages: Vec<SyncMessage>,
    ) -> io::Result<Vec<SyncMessage>> {
        let mut session = SyncSession::new(peer);
        let mut responses = Vec::new();
        for message in messages {
            push_responses(&mut responses, session.handle(self, message)?)?;
        }
        push_responses(&mut responses, session.finish(self)?)?;
        Ok(responses)
    }

    fn full_sweep_resync_targets(&self) -> io::Result<BTreeSet<(PeerId, crate::TopicId)>> {
        let mut targets = BTreeSet::new();
        for topic in self.node.storage().list_topics().map_err(invalid_data)? {
            let Some(state) = self
                .node
                .storage()
                .topic_state(&topic.topic_id)
                .map_err(invalid_data)?
            else {
                continue;
            };
            if !state.members.contains(&self.node.peer_id()) {
                continue;
            }
            targets.extend(
                select_sync_peers(topic.topic_id, self.node.peer_id(), &state)
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
                if local.fingerprint == fingerprint.fingerprint {
                    if state.members.contains(&peer_id) {
                        self.node
                            .record_peer_synced(peer_id, fingerprint.topic_id)
                            .map_err(invalid_data)?;
                        self.finish_resync_attempt(
                            peer_id,
                            fingerprint.topic_id,
                            Ok(()),
                            self.runtime,
                        );
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
                let plan = self
                    .node
                    .negotiate_sync(peer_id, &summary)
                    .map_err(invalid_data)?;
                let mut responses = Vec::new();
                if !plan.send.is_empty() {
                    responses.extend(sync_data_messages(plan.topic_id, plan.send));
                }
                if !plan.need.is_empty() || !plan.actor_range_hints.is_empty() {
                    responses.push(SyncMessage::Request(crate::sync::SyncRequest {
                        topic_id: plan.topic_id,
                        known: plan.common,
                        wants: plan.need,
                        actor_range_hints: plan.actor_range_hints,
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
                    .plan_sync_response_data(peer_id, &request)
                    .map_err(invalid_data)?;
                Ok(sync_data_messages(data.topic_id, data.ops))
            }
            SyncMessage::Data(data) => {
                let data_topic_id = data.topic_id;
                let source_peer = remote_peer_id.ok_or_else(|| {
                    invalid_data("sync data requires a preceding SyncOpen with peer_id")
                })?;
                self.node
                    .ensure_iroh_peer_whitelisted(source_peer, &data)
                    .map_err(invalid_data)?;
                self.node
                    .receive_sync_data_from(source_peer, data)
                    .map(SyncMessage::Ack)
                    .map(|message| {
                        if let Err(error) = self.schedule_topic_recheck(data_topic_id) {
                            tracing::warn!(%data_topic_id, %error, "failed to schedule received topic resync");
                        }
                        vec![message]
                    })
                    .map_err(invalid_data)
            }
            SyncMessage::Ack(ack) => {
                let peer_id = remote_peer_id.ok_or_else(|| {
                    invalid_data("sync ack requires a preceding SyncOpen with peer_id")
                })?;
                if ack.peer_id != peer_id {
                    return Err(invalid_data(
                        "sync ack peer_id does not match SyncOpen peer_id",
                    ));
                }
                let topic_id = ack.topic_id;
                self.node
                    .apply_sync_ack(&ack)
                    .map(|()| {
                        self.finish_resync_attempt(peer_id, topic_id, Ok(()), self.runtime);
                        Vec::new()
                    })
                    .map_err(invalid_data)
            }
        }
    }
}

struct SyncSession {
    authenticated_peer_id: PeerId,
    remote_peer_id: Option<PeerId>,
    open_topic_id: Option<crate::TopicId>,
    acks: Vec<crate::sync::SyncAck>,
}

impl SyncSession {
    fn new(peer: iroh::EndpointId) -> Self {
        Self {
            authenticated_peer_id: peer_id_from_endpoint_id(peer),
            remote_peer_id: None,
            open_topic_id: None,
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
        } else if let Some(topic_id) = message_topic_id(&message)
            && self.open_topic_id != Some(topic_id)
        {
            return Err(invalid_data(
                "sync message topic does not match SyncOpen topic",
            ));
        }

        if let SyncMessage::Ack(ack) = message {
            if self.remote_peer_id.is_none() {
                return Err(invalid_data(
                    "sync ack requires a preceding SyncOpen with peer_id",
                ));
            }
            self.acks.push(ack);
            return Ok(Vec::new());
        }

        net.handle_message(message, self.remote_peer_id)
    }

    fn finish<S: Storage>(&mut self, net: &IrohNet<S>) -> io::Result<Vec<SyncMessage>> {
        let mut responses = Vec::new();
        for ack in std::mem::take(&mut self.acks) {
            push_responses(
                &mut responses,
                net.handle_message(SyncMessage::Ack(ack), self.remote_peer_id)?,
            )?;
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

fn push_responses(out: &mut Vec<SyncMessage>, responses: Vec<SyncMessage>) -> io::Result<()> {
    if out.len().saturating_add(responses.len()) > MAX_SYNC_MESSAGES_PER_STREAM {
        return Err(invalid_data("sync response has too many messages"));
    }
    out.extend(responses);
    Ok(())
}

fn log_resync_join_result(
    result: std::result::Result<(PeerId, crate::TopicId, io::Result<()>), tokio::task::JoinError>,
) {
    match result {
        Ok((peer_id, topic_id, Err(error))) => {
            tracing::warn!(%peer_id, %topic_id, %error, "failed to resync peer");
        }
        Ok((_, _, Ok(()))) => {}
        Err(error) => tracing::warn!(%error, "resync task failed"),
    }
}

async fn run_due_resyncs<S: Storage>(
    net: &Weak<IrohNet<S>>,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
    runtime: IrohRuntimeConfig,
) -> bool {
    loop {
        let Some(current) = net.upgrade() else {
            return false;
        };
        if current.is_shutdown() || current.endpoint().is_closed() {
            return false;
        }
        let targets = current.resync_scheduler.due_targets(MAX_RESYNC_CONCURRENCY);
        drop(current);
        if targets.is_empty() {
            return true;
        }

        let mut syncs = tokio::task::JoinSet::new();
        for target in targets {
            let Some(current) = net.upgrade() else {
                return false;
            };
            if current.is_shutdown() {
                return false;
            }
            syncs.spawn(async move {
                let peer_id = target.key.peer_id;
                let topic_id = target.key.topic_id;
                let result = match current.should_attempt_resync_target(target) {
                    Ok(true) => {
                        current
                            .sync_peer_now_with_runtime(peer_id, topic_id, runtime)
                            .await
                    }
                    Ok(false) => {
                        current.resync_scheduler.complete_clean(peer_id, topic_id);
                        Ok(())
                    }
                    Err(error) => {
                        current.finish_resync_attempt(peer_id, topic_id, Err(&error), runtime);
                        Err(error)
                    }
                };
                (peer_id, topic_id, result)
            });
        }

        while !syncs.is_empty() {
            tokio::select! {
                Some(result) = syncs.join_next() => log_resync_join_result(result),
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        syncs.abort_all();
                        while syncs.join_next().await.is_some() {}
                        return false;
                    }
                }
            }
        }
    }
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
    loop {
        let streams = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
            streams = connection.accept_bi() => streams,
        };
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
        if let Err(error) = current.handle_stream(peer, recv, send).await {
            tracing::warn!(%peer, %error, "failed to handle iroh sync stream");
        }
    }
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
    }
}

fn timed_out(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, message)
}

fn other(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TopicId;

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

        let due = scheduler.due_targets(8);

        assert_eq!(due.len(), 1);
        assert_eq!(due[0].key.peer_id, peer(1));
        assert_eq!(due[0].key.topic_id, topic(2));
        scheduler.complete_clean(peer(1), topic(2));
        assert!(scheduler.next_due().is_none());
    }

    #[test]
    fn scheduler_uses_capped_failure_backoff() {
        let scheduler = ResyncScheduler::default();
        let peer_id = peer(3);
        let topic_id = topic(4);
        scheduler.schedule_now(peer_id, topic_id, false);
        assert_eq!(scheduler.due_targets(8).len(), 1);

        scheduler.complete_failed(
            peer_id,
            topic_id,
            Duration::from_secs(1),
            Duration::from_secs(600),
        );
        let first_delay = scheduler
            .next_due()
            .unwrap()
            .saturating_duration_since(tokio::time::Instant::now());
        assert!(first_delay <= Duration::from_secs(1));

        for _ in 0..16 {
            scheduler.complete_failed(
                peer_id,
                topic_id,
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
}
