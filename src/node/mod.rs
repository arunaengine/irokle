// SPDX-License-Identifier: MIT OR Apache-2.0
//! High-level node, topic, publishing, and sync facade APIs.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

mod builder;
mod peers;
mod topic;

pub(crate) use peers::select_sync_peers;
pub use topic::{RawTopic, Topic};

#[cfg(feature = "iroh")]
use crate::ActorClock;
use crate::history::{DagQuery, HistoryOrder, ordered};
use crate::oplog::{Oplog, topological};
use crate::reducer::EventRecord;
#[cfg(feature = "iroh")]
use crate::storage::{AdmissionEffects, OpMeta, SyncObligation, TopicState};
use crate::storage::{MemoryStorage, Storage, SyncPeerState, SyncPeerStatus};
use crate::sync::{
    SyncAck, SyncData, SyncEngine, SyncFingerprint, SyncOpen, SyncPlan, SyncReport, SyncRequest,
    SyncSummary,
};
use crate::{
    ActorId, Ed25519Signer, Error, Event, EventEnvelope, Op, OpId, PeerId, Result, Signer,
    TopicConfig, TopicControl, TopicGenesis, TopicId, actor_id_for,
};

static TOPIC_NONCE: AtomicU64 = AtomicU64::new(0);
const SYNC_PEER_SHARED_OVERLAP: usize = 2;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum WriteConcern {
    #[default]
    Local,
    AsyncReplication,
}

#[derive(Clone)]
pub struct NodeConfig {
    pub signer: Ed25519Signer,
    pub default_write_concern: WriteConcern,
    pub peer_whitelist: Option<BTreeSet<PeerId>>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            signer: Ed25519Signer::generate(),
            default_write_concern: WriteConcern::Local,
            peer_whitelist: Some(BTreeSet::new()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishOptions {
    pub write_concern: WriteConcern,
}

impl Default for PublishOptions {
    fn default() -> Self {
        Self {
            write_concern: WriteConcern::Local,
        }
    }
}

#[derive(Clone)]
pub struct Irokle<S: Storage = MemoryStorage> {
    oplog: Oplog<S>,
    sync: SyncEngine<S>,
    config: NodeConfig,
    peer_whitelist: Arc<RwLock<Option<BTreeSet<PeerId>>>>,
    #[cfg(feature = "iroh")]
    net: Option<Arc<crate::net::IrohNet<S>>>,
}

pub struct IrokleBuilder<S = MemoryStorage> {
    storage: S,
    config: NodeConfig,
    signer_explicit: bool,
    write_concern_explicit: bool,
    #[cfg(feature = "iroh")]
    endpoint: Option<iroh::Endpoint>,
    #[cfg(feature = "iroh")]
    alpns: Vec<Vec<u8>>,
    #[cfg(feature = "iroh")]
    auto_accept: bool,
    #[cfg(feature = "iroh")]
    iroh_runtime: crate::net::IrohRuntimeConfig,
}

impl<S: Storage> Irokle<S> {
    pub fn with_storage(storage: S, config: NodeConfig) -> Result<Self> {
        let oplog = Oplog::with_storage(storage);
        oplog.reconcile_pending_ops()?;
        let sync = SyncEngine::new(oplog.clone());
        Ok(Self {
            oplog,
            sync,
            peer_whitelist: Arc::new(RwLock::new(config.peer_whitelist.clone())),
            config,
            #[cfg(feature = "iroh")]
            net: None,
        })
    }

    #[cfg(feature = "iroh")]
    pub(crate) fn with_net(mut self, net: Arc<crate::net::IrohNet<S>>) -> Self {
        self.net = Some(net);
        self
    }
    pub fn storage(&self) -> &S {
        self.oplog.storage()
    }
    pub fn signer(&self) -> &Ed25519Signer {
        &self.config.signer
    }
    pub fn peer_id(&self) -> PeerId {
        self.config.signer.peer_id()
    }

    #[cfg(feature = "iroh")]
    pub fn endpoint(&self) -> Option<&iroh::Endpoint> {
        self.net.as_ref().map(|net| net.endpoint())
    }

    #[cfg(feature = "iroh")]
    pub fn iroh_runtime_config(&self) -> Option<crate::net::IrohRuntimeConfig> {
        self.net.as_ref().map(|net| net.runtime_config())
    }

    #[cfg(feature = "iroh")]
    pub async fn shutdown_iroh(&self) {
        if let Some(net) = &self.net {
            net.shutdown().await;
        }
    }

    #[cfg(feature = "iroh")]
    pub fn start_accept_loop(&self) -> std::io::Result<()> {
        self.net
            .as_ref()
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotConnected, "iroh is not configured")
            })?
            .start_accept_loop()
    }

    #[cfg(feature = "iroh")]
    pub async fn accept_one(&self) -> std::io::Result<Option<iroh::EndpointId>> {
        self.net
            .as_ref()
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotConnected, "iroh is not configured")
            })?
            .accept_one()
            .await
    }

    #[cfg(feature = "iroh")]
    pub async fn sync_now(&self, peer_id: PeerId, topic_id: TopicId) -> std::io::Result<()> {
        self.net
            .as_ref()
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotConnected, "iroh is not configured")
            })?
            .sync_peer_now(peer_id, topic_id)
            .await
    }

    #[cfg(feature = "iroh")]
    pub async fn sync_addr_now(
        &self,
        addr: iroh::EndpointAddr,
        topic_id: TopicId,
    ) -> std::io::Result<()> {
        self.net
            .as_ref()
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotConnected, "iroh is not configured")
            })?
            .sync_now(addr, topic_id)
            .await
    }

    #[cfg(feature = "iroh")]
    pub async fn sync_endpoint_now(
        &self,
        endpoint_id: iroh::EndpointId,
        topic_id: TopicId,
    ) -> std::io::Result<()> {
        self.net
            .as_ref()
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotConnected, "iroh is not configured")
            })?
            .sync_endpoint_now(endpoint_id, topic_id)
            .await
    }

    #[cfg(feature = "iroh")]
    pub async fn sync_topic_now(&self, topic_id: TopicId) -> std::io::Result<()> {
        let net = self.net.as_ref().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "iroh is not configured")
        })?;
        let state = self
            .storage()
            .topic_state(&topic_id)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()))?
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "topic not found"))?;
        let peers = select_sync_peers(topic_id, self.peer_id(), &state);
        let mut first_error = None;
        for peer in peers {
            if let Err(error) = net.sync_peer_now(peer, topic_id).await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    pub fn create_topic<E: Event>(&self, mut config: TopicConfig) -> Result<Topic<E, S>> {
        config.initial_peers.insert(self.peer_id());
        let topic_id = self.next_topic_id::<E>()?;
        let actor_id = actor_id_for(topic_id, self.peer_id());
        let genesis = TopicGenesis {
            event_type_id: E::TYPE_ID.to_owned(),
            initial_peers: config.initial_peers,
            replication_policy: config.replication_policy,
        };
        #[cfg(feature = "iroh")]
        let op = self.oplog.create_topic_genesis_with_effects(
            topic_id,
            actor_id,
            genesis,
            &self.config.signer,
            |op, meta, state| {
                self.replication_admission_effects(
                    topic_id,
                    op.id,
                    meta,
                    state,
                    &self.config.default_write_concern,
                )
            },
        )?;
        #[cfg(not(feature = "iroh"))]
        self.oplog
            .create_topic_genesis(topic_id, actor_id, genesis, &self.config.signer)?;
        #[cfg(feature = "iroh")]
        self.wake_async_replication(
            topic_id,
            op.id,
            &self.config.default_write_concern,
            "topic genesis replication wake failed",
        )?;
        Ok(Topic::new(self.clone(), topic_id, actor_id))
    }

    fn next_topic_id<E: Event>(&self) -> Result<TopicId> {
        for _ in 0..16 {
            let counter = TOPIC_NONCE.fetch_add(1, Ordering::Relaxed);
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|err| Error::Storage(format!("system time before unix epoch: {err}")))?
                .as_nanos();
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"irokle-topic-v1");
            hasher.update(self.peer_id().as_ref());
            hasher.update(E::TYPE_ID.as_bytes());
            hasher.update(&std::process::id().to_le_bytes());
            hasher.update(&counter.to_le_bytes());
            hasher.update(&now.to_le_bytes());
            let topic_id = TopicId::from_bytes(*hasher.finalize().as_bytes());
            if self.storage().topic_state(&topic_id)?.is_none() {
                return Ok(topic_id);
            }
        }
        Err(Error::Storage("failed to allocate unique topic id".into()))
    }

    pub fn open_topic<E: Event>(&self, topic_id: TopicId) -> Result<Topic<E, S>> {
        let state = self
            .storage()
            .topic_state(&topic_id)?
            .ok_or(Error::TopicNotFound)?;
        if state.event_type_id != E::TYPE_ID {
            return Err(Error::EventTypeMismatch {
                expected: E::TYPE_ID.to_owned(),
                actual: state.event_type_id,
            });
        }
        if !state.members.contains(&self.peer_id()) {
            return Err(Error::NotTopicMember);
        }
        let actor_id = actor_id_for(topic_id, self.peer_id());
        Ok(Topic::new(self.clone(), topic_id, actor_id))
    }

    pub fn list_topics(&self) -> Result<Vec<crate::TopicInfo>> {
        self.storage().list_topics()
    }
    pub fn raw_topic(&self, topic_id: TopicId) -> Result<RawTopic<S>> {
        Ok(RawTopic {
            oplog: self.oplog.clone(),
            topic_id,
        })
    }

    pub fn reject_topic(&self, topic_id: TopicId) -> Result<()> {
        let state = self
            .storage()
            .topic_state(&topic_id)?
            .ok_or(Error::TopicNotFound)?;
        if !state.members.contains(&self.peer_id()) {
            return Err(Error::NotTopicMember);
        }
        let actor_id = actor_id_for(topic_id, self.peer_id());
        self.publish_control(
            topic_id,
            actor_id,
            TopicControl::RemovePeer {
                peer: self.peer_id(),
            },
        )
    }

    pub fn sync_open(&self, topic_id: TopicId) -> SyncOpen {
        let event_type_id = self
            .storage()
            .topic_state(&topic_id)
            .ok()
            .flatten()
            .map(|state| state.event_type_id);
        SyncEngine::<S>::open(topic_id, self.peer_id(), event_type_id)
    }

    pub fn sync_summary(&self, topic_id: TopicId) -> Result<SyncSummary> {
        self.sync.summary(topic_id)
    }

    pub fn sync_fingerprint(&self, topic_id: TopicId) -> Result<SyncFingerprint> {
        self.sync.fingerprint(topic_id)
    }

    pub fn negotiate_sync(&self, peer_id: PeerId, remote: &SyncSummary) -> Result<SyncPlan> {
        self.sync.negotiate(peer_id, remote)
    }

    pub fn plan_sync_data(&self, peer_id: PeerId, remote: &SyncSummary) -> Result<SyncData> {
        self.sync.plan_data(peer_id, remote)
    }

    pub fn plan_sync_request(&self, peer_id: PeerId, remote: &SyncSummary) -> Result<SyncRequest> {
        self.sync.plan_request(peer_id, remote)
    }

    pub fn plan_sync_response_data(
        &self,
        peer_id: PeerId,
        request: &SyncRequest,
    ) -> Result<SyncData> {
        self.sync.plan_response_data(peer_id, request)
    }

    pub fn receive_sync_data_from(
        &self,
        source_peer_id: PeerId,
        data: SyncData,
    ) -> Result<SyncAck> {
        self.check_unknown_topic(source_peer_id, &data)?;
        let mut ack = self
            .sync
            .receive_data(source_peer_id, self.peer_id(), data)?;
        self.put_receive_forward_obligations(source_peer_id, ack.topic_id, &ack.accepted)?;
        ack.sign(&self.config.signer)?;
        Ok(ack)
    }

    pub fn receive_sync_data_as_local(&self, data: SyncData) -> Result<SyncAck> {
        let mut ack = self
            .sync
            .receive_data(self.peer_id(), self.peer_id(), data)?;
        ack.sign(&self.config.signer)?;
        Ok(ack)
    }

    pub fn apply_sync_ack(&self, ack: &SyncAck) -> Result<()> {
        self.sync.apply_ack(ack)
    }

    pub fn peer_whitelist(&self) -> Result<Option<BTreeSet<PeerId>>> {
        Ok(self
            .peer_whitelist
            .read()
            .map_err(|_| Error::Storage("peer whitelist read lock poisoned".into()))?
            .clone())
    }

    pub fn set_peer_whitelist(&self, peer_whitelist: Option<BTreeSet<PeerId>>) -> Result<()> {
        *self
            .peer_whitelist
            .write()
            .map_err(|_| Error::Storage("peer whitelist write lock poisoned".into()))? =
            peer_whitelist;
        Ok(())
    }

    pub fn add_peer_to_whitelist(&self, peer_id: PeerId) -> Result<()> {
        let mut peer_whitelist = self
            .peer_whitelist
            .write()
            .map_err(|_| Error::Storage("peer whitelist write lock poisoned".into()))?;
        peer_whitelist
            .get_or_insert_with(BTreeSet::new)
            .insert(peer_id);
        Ok(())
    }

    pub fn add_peers_to_whitelist<I>(&self, peer_ids: I) -> Result<()>
    where
        I: IntoIterator<Item = PeerId>,
    {
        let mut peer_whitelist = self
            .peer_whitelist
            .write()
            .map_err(|_| Error::Storage("peer whitelist write lock poisoned".into()))?;
        peer_whitelist
            .get_or_insert_with(BTreeSet::new)
            .extend(peer_ids);
        Ok(())
    }

    #[cfg(feature = "iroh")]
    pub(crate) fn record_peer_synced(&self, peer_id: PeerId, topic_id: TopicId) -> Result<()> {
        self.sync.record_peer_synced(peer_id, topic_id)
    }

    #[cfg(feature = "iroh")]
    pub(crate) fn ensure_iroh_peer_whitelisted(
        &self,
        source_peer_id: PeerId,
        data: &SyncData,
    ) -> Result<()> {
        if self.storage().topic_state(&data.topic_id)?.is_some() {
            return Ok(());
        }
        let peer_allowed = {
            let peer_whitelist = self
                .peer_whitelist
                .read()
                .map_err(|_| Error::Storage("peer whitelist read lock poisoned".into()))?;
            match &*peer_whitelist {
                Some(peer_whitelist) => peer_whitelist.contains(&source_peer_id),
                None => true,
            }
        };
        if !peer_allowed {
            return Err(Error::PeerNotWhitelisted(source_peer_id));
        }
        self.check_unknown_topic(source_peer_id, data)
    }

    fn check_unknown_topic(&self, source_peer_id: PeerId, data: &SyncData) -> Result<()> {
        if self.storage().topic_state(&data.topic_id)?.is_some() {
            return Ok(());
        }
        let dry_storage = MemoryStorage::new();
        let dry_oplog = Oplog::with_storage(dry_storage.clone());
        dry_oplog.receive_ops_from_peer(Some(source_peer_id), data.ops.clone())?;
        let Some(state) = dry_storage.topic_state(&data.topic_id)? else {
            return Err(Error::InvalidGenesis);
        };
        if !state.members.contains(&self.peer_id()) || !state.members.contains(&source_peer_id) {
            return Err(Error::NotTopicMember);
        }
        Ok(())
    }

    pub fn peer_reached_op(&self, peer_id: PeerId, op_id: OpId) -> Result<bool> {
        self.storage().peer_reached_op(&peer_id, &op_id)
    }

    pub fn peers_reached_op(&self, op_id: OpId) -> Result<Vec<PeerId>> {
        self.storage().peers_reached_op(&op_id)
    }

    pub fn put_sync_obligation(
        &self,
        peer_id: PeerId,
        topic_id: TopicId,
        op_ids: BTreeSet<OpId>,
    ) -> Result<()> {
        self.sync.put_obligation(peer_id, topic_id, op_ids)
    }

    fn put_receive_forward_obligations(
        &self,
        source_peer_id: PeerId,
        topic_id: TopicId,
        accepted: &BTreeSet<OpId>,
    ) -> Result<()> {
        if accepted.is_empty() {
            return Ok(());
        }
        let state = self
            .storage()
            .topic_state(&topic_id)?
            .ok_or(Error::TopicNotFound)?;
        for peer_id in select_sync_peers(topic_id, self.peer_id(), &state) {
            if peer_id == source_peer_id || peer_id == self.peer_id() {
                continue;
            }
            let mut missing = BTreeSet::new();
            for op_id in accepted {
                if !self.peer_reached_op(peer_id, *op_id)? {
                    missing.insert(*op_id);
                }
            }
            if !missing.is_empty() {
                self.put_sync_obligation(peer_id, topic_id, missing)?;
                self.record_replication_scheduled(peer_id, topic_id)?;
            }
        }
        Ok(())
    }

    #[cfg(feature = "iroh")]
    fn replication_admission_effects(
        &self,
        topic_id: TopicId,
        op_id: OpId,
        meta: &OpMeta,
        state: &TopicState,
        write_concern: &WriteConcern,
    ) -> Result<AdmissionEffects> {
        if !matches!(write_concern, WriteConcern::AsyncReplication) || self.net.is_none() {
            return Ok(AdmissionEffects::default());
        }

        let mut target_clock = ActorClock::new();
        target_clock.observe(meta.actor_id, meta.actor_seq);
        Ok(AdmissionEffects {
            sync_obligations: select_sync_peers(topic_id, self.peer_id(), state)
                .into_iter()
                .map(|peer_id| SyncObligation {
                    peer_id,
                    topic_id,
                    op_ids: [op_id].into(),
                    target_clock: target_clock.clone(),
                })
                .collect(),
        })
    }

    #[cfg(feature = "iroh")]
    fn wake_async_replication(
        &self,
        topic_id: TopicId,
        _op_id: OpId,
        write_concern: &WriteConcern,
        wake_failed_message: &'static str,
    ) -> Result<()> {
        if !matches!(write_concern, WriteConcern::AsyncReplication) || self.net.is_none() {
            return Ok(());
        }

        let state = self
            .storage()
            .topic_state(&topic_id)?
            .ok_or(Error::TopicNotFound)?;
        let peers = select_sync_peers(topic_id, self.peer_id(), &state);
        for peer_id in peers.iter().copied() {
            self.record_replication_scheduled(peer_id, topic_id)?;
        }
        if peers.is_empty() {
            return Ok(());
        }

        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                let node = self.clone();
                handle.spawn(async move {
                    if let Err(error) = node.sync_topic_now(topic_id).await {
                        tracing::warn!(%topic_id, %error, "{}", wake_failed_message);
                    }
                });
            }
            Err(error) => {
                let error =
                    std::io::Error::other(format!("async replication not started: {error}"));
                for peer_id in peers {
                    self.record_sync_result(peer_id, topic_id, Err(&error))?;
                }
            }
        }

        Ok(())
    }

    fn record_replication_scheduled(&self, peer_id: PeerId, topic_id: TopicId) -> Result<()> {
        let mut status = self
            .storage()
            .sync_statuses(&topic_id)?
            .into_iter()
            .find(|status| status.peer_id == peer_id)
            .unwrap_or(SyncPeerStatus {
                peer_id,
                topic_id,
                ..SyncPeerStatus::default()
            });
        status.pending_obligations = self.storage().sync_obligations(&peer_id, &topic_id)?.len();
        if status.pending_obligations > 0 && status.state != SyncPeerState::Failed {
            status.state = SyncPeerState::Behind;
        }
        self.storage().put_sync_status(status)
    }

    pub fn sync_report(&self, peer_id: PeerId, topic_id: TopicId) -> Result<SyncReport> {
        self.sync.report(peer_id, topic_id)
    }

    pub fn sync_status(&self, topic_id: TopicId) -> Result<Vec<SyncPeerStatus>> {
        let mut by_peer = self
            .storage()
            .sync_statuses(&topic_id)?
            .into_iter()
            .map(|status| (status.peer_id, status))
            .collect::<BTreeMap<_, _>>();
        for status in by_peer.values_mut() {
            status.pending_obligations = 0;
        }

        for obligation in self
            .storage()
            .all_sync_obligations()?
            .into_iter()
            .filter(|obligation| obligation.topic_id == topic_id)
        {
            by_peer
                .entry(obligation.peer_id)
                .or_insert_with(|| SyncPeerStatus {
                    peer_id: obligation.peer_id,
                    topic_id,
                    state: SyncPeerState::Behind,
                    ..SyncPeerStatus::default()
                })
                .pending_obligations += 1;
        }

        let mut statuses = by_peer.into_values().collect::<Vec<_>>();
        for status in &mut statuses {
            if status.pending_obligations > 0 && status.state == SyncPeerState::Healthy {
                status.state = SyncPeerState::Behind;
            }
        }
        Ok(statuses)
    }

    pub fn sync_state_counts(&self, topic_id: TopicId) -> Result<BTreeMap<SyncPeerState, usize>> {
        let mut counts = BTreeMap::new();
        for status in self.sync_status(topic_id)? {
            *counts.entry(status.state).or_default() += 1;
        }
        Ok(counts)
    }

    #[cfg(any(feature = "iroh", test))]
    pub(crate) fn record_sync_result(
        &self,
        peer_id: PeerId,
        topic_id: TopicId,
        result: std::result::Result<(), &std::io::Error>,
    ) -> Result<()> {
        let mut status = self
            .storage()
            .sync_statuses(&topic_id)?
            .into_iter()
            .find(|status| status.peer_id == peer_id)
            .unwrap_or(SyncPeerStatus {
                peer_id,
                topic_id,
                ..SyncPeerStatus::default()
            });
        status.last_attempt_ms = Some(now_millis()?);
        status.pending_obligations = self.storage().sync_obligations(&peer_id, &topic_id)?.len();
        match result {
            Ok(()) => {
                status.successful_attempts = status.successful_attempts.saturating_add(1);
                status.last_success_ms = status.last_attempt_ms;
                status.last_error = None;
                status.state = if status.pending_obligations == 0 {
                    SyncPeerState::Healthy
                } else {
                    SyncPeerState::Behind
                };
            }
            Err(error) => {
                status.failed_attempts = status.failed_attempts.saturating_add(1);
                status.last_error = Some(error.to_string());
                status.state = SyncPeerState::Failed;
            }
        }
        self.storage().put_sync_status(status)
    }

    pub(crate) fn publish_event<E: Event>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        event: E,
        options: PublishOptions,
    ) -> Result<EventRecord<E>> {
        let envelope = EventEnvelope::encode_event(&event)?;
        #[cfg(feature = "iroh")]
        let op = self.oplog.create_event_op_with_effects(
            topic_id,
            actor_id,
            envelope,
            &self.config.signer,
            |op, meta, state| {
                self.replication_admission_effects(
                    topic_id,
                    op.id,
                    meta,
                    state,
                    &options.write_concern,
                )
            },
        )?;
        #[cfg(not(feature = "iroh"))]
        let op = self
            .oplog
            .create_event_op(topic_id, actor_id, envelope, &self.config.signer)?;
        let meta = self
            .oplog
            .storage()
            .get_meta(&op.id)?
            .ok_or(Error::Storage("missing op meta after publish".into()))?;
        let record = EventRecord::new(
            event,
            op.id,
            meta.actor_id,
            meta.actor_seq,
            meta.observed_clock,
        );
        #[cfg(not(feature = "iroh"))]
        let _ = &options;
        #[cfg(feature = "iroh")]
        self.wake_async_replication(
            topic_id,
            op.id,
            &options.write_concern,
            "async replication wake failed",
        )?;
        Ok(record)
    }

    pub(crate) fn publish_control(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        control: TopicControl,
    ) -> Result<()> {
        #[cfg(feature = "iroh")]
        let op = self.oplog.create_control_op_with_effects(
            topic_id,
            actor_id,
            control,
            &self.config.signer,
            |op, meta, state| {
                self.replication_admission_effects(
                    topic_id,
                    op.id,
                    meta,
                    state,
                    &self.config.default_write_concern,
                )
            },
        )?;
        #[cfg(not(feature = "iroh"))]
        self.oplog
            .create_control_op(topic_id, actor_id, control, &self.config.signer)?;
        #[cfg(feature = "iroh")]
        self.wake_async_replication(
            topic_id,
            op.id,
            &self.config.default_write_concern,
            "topic control replication wake failed",
        )?;
        Ok(())
    }

    pub(crate) fn topic_history<E: Event>(
        &self,
        topic_id: TopicId,
        order: HistoryOrder,
    ) -> Result<Vec<EventRecord<E>>> {
        let mut records = Vec::new();
        for op in topological(self.oplog.storage(), &topic_id)? {
            if let crate::TopicPayload::Event(envelope) = &op.signed.body.payload {
                let meta = self
                    .oplog
                    .storage()
                    .get_meta(&op.id)?
                    .ok_or(Error::Storage("missing op meta".into()))?;
                records.push(EventRecord::new(
                    envelope.decode_event::<E>()?,
                    op.id,
                    meta.actor_id,
                    meta.actor_seq,
                    meta.observed_clock,
                ));
            }
        }
        Ok(ordered(records, order))
    }

    pub(crate) fn topic_dag(&self, topic_id: TopicId, query: DagQuery<OpId>) -> Result<Vec<Op>> {
        topic::dag_ops(self.oplog.storage(), topic_id, query)
    }

    pub(crate) fn topic_heads(&self, topic_id: TopicId) -> Result<BTreeSet<OpId>> {
        self.oplog.storage().heads(&topic_id)
    }
}

#[cfg(feature = "fjall")]
impl Irokle<crate::FjallStorage> {
    pub fn open_fjall(path: impl AsRef<std::path::Path>, config: NodeConfig) -> Result<Self> {
        Irokle::with_storage(crate::FjallStorage::open(path)?, config)
    }

    pub fn open_fjall_database(
        db: fjall::OptimisticTxDatabase,
        config: NodeConfig,
    ) -> Result<Self> {
        Irokle::with_storage(crate::FjallStorage::from_database(db)?, config)
    }
}

#[cfg(any(feature = "iroh", test))]
fn now_millis() -> Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| Error::Storage(format!("system time before unix epoch: {err}")))?
        .as_millis();
    millis
        .try_into()
        .map_err(|_| Error::Storage("system time does not fit in u64 milliseconds".into()))
}
