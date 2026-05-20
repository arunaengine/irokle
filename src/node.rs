// SPDX-License-Identifier: MIT OR Apache-2.0
//! High-level node, topic, publishing, and sync facade APIs.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(any(feature = "iroh", test))]
use smallvec::SmallVec;

use crate::history::{DagQuery, HistoryOrder, limited, ordered};
use crate::oplog::{Oplog, topological, topological_subset};
use crate::reducer::EventRecord;
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
#[cfg(any(feature = "iroh", test))]
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
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            signer: Ed25519Signer::generate(),
            default_write_concern: WriteConcern::Local,
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
    #[cfg(feature = "iroh")]
    net: Option<std::sync::Arc<crate::net::IrohNet<S>>>,
}

pub struct IrokleBuilder<S = MemoryStorage> {
    storage: S,
    config: NodeConfig,
    signer_explicit: bool,
    #[cfg(feature = "iroh")]
    endpoint: Option<iroh::Endpoint>,
    #[cfg(feature = "iroh")]
    alpns: Vec<Vec<u8>>,
    #[cfg(feature = "iroh")]
    auto_accept: bool,
}

impl Irokle<MemoryStorage> {
    pub fn builder() -> IrokleBuilder<MemoryStorage> {
        IrokleBuilder {
            storage: MemoryStorage::new(),
            config: NodeConfig::default(),
            signer_explicit: false,
            #[cfg(feature = "iroh")]
            endpoint: None,
            #[cfg(feature = "iroh")]
            alpns: Vec::new(),
            #[cfg(feature = "iroh")]
            auto_accept: true,
        }
    }

    pub fn new(config: NodeConfig) -> Result<Self> {
        Self::with_storage(MemoryStorage::new(), config)
    }
    pub fn in_memory() -> Result<Self> {
        Self::new(NodeConfig::default())
    }
}

impl<S: Storage> IrokleBuilder<S> {
    pub fn with_storage<T: Storage>(self, storage: T) -> IrokleBuilder<T> {
        IrokleBuilder {
            storage,
            config: self.config,
            signer_explicit: self.signer_explicit,
            #[cfg(feature = "iroh")]
            endpoint: self.endpoint,
            #[cfg(feature = "iroh")]
            alpns: self.alpns,
            #[cfg(feature = "iroh")]
            auto_accept: self.auto_accept,
        }
    }

    pub fn with_config(mut self, config: NodeConfig) -> Self {
        self.config = config;
        self.signer_explicit = true;
        self
    }

    pub fn with_signer(mut self, signer: Ed25519Signer) -> Self {
        self.config.signer = signer;
        self.signer_explicit = true;
        self
    }

    pub fn with_write_concern(mut self, write_concern: WriteConcern) -> Self {
        self.config.default_write_concern = write_concern;
        self
    }

    #[cfg(feature = "iroh")]
    pub fn with_iroh_secret_key(mut self, secret_key: &iroh::SecretKey) -> Self {
        self.config.signer = Ed25519Signer::from_iroh_secret_key(secret_key);
        self.signer_explicit = true;
        self
    }

    #[cfg(feature = "iroh")]
    pub fn with_net(mut self, endpoint: iroh::Endpoint) -> Self {
        if !self.signer_explicit {
            self.config.signer = Ed25519Signer::from_iroh_secret_key(endpoint.secret_key());
        }
        self.endpoint = Some(endpoint);
        self.auto_accept = true;
        self
    }

    #[cfg(feature = "iroh")]
    pub fn with_alpn(mut self, alpn: impl AsRef<[u8]>) -> Self {
        let alpn = alpn.as_ref().to_vec();
        if !self.alpns.contains(&alpn) {
            self.alpns.push(alpn);
        }
        self
    }

    #[cfg(feature = "iroh")]
    pub fn with_alpns<I, A>(mut self, alpns: I) -> Self
    where
        I: IntoIterator<Item = A>,
        A: AsRef<[u8]>,
    {
        for alpn in alpns {
            let alpn = alpn.as_ref().to_vec();
            if !self.alpns.contains(&alpn) {
                self.alpns.push(alpn);
            }
        }
        self
    }

    #[cfg(feature = "iroh")]
    pub fn without_auto_accept(mut self) -> Self {
        self.auto_accept = false;
        self
    }

    #[cfg(feature = "fjall")]
    pub fn with_fjall_path(
        self,
        path: impl AsRef<std::path::Path>,
    ) -> Result<IrokleBuilder<crate::FjallStorage>> {
        Ok(IrokleBuilder {
            storage: crate::FjallStorage::open(path)?,
            config: self.config,
            signer_explicit: self.signer_explicit,
            #[cfg(feature = "iroh")]
            endpoint: self.endpoint,
            #[cfg(feature = "iroh")]
            alpns: self.alpns,
            #[cfg(feature = "iroh")]
            auto_accept: self.auto_accept,
        })
    }

    #[cfg(feature = "fjall")]
    pub fn with_fjall_database(
        self,
        db: fjall::OptimisticTxDatabase,
    ) -> Result<IrokleBuilder<crate::FjallStorage>> {
        Ok(IrokleBuilder {
            storage: crate::FjallStorage::from_database(db)?,
            config: self.config,
            signer_explicit: self.signer_explicit,
            #[cfg(feature = "iroh")]
            endpoint: self.endpoint,
            #[cfg(feature = "iroh")]
            alpns: self.alpns,
            #[cfg(feature = "iroh")]
            auto_accept: self.auto_accept,
        })
    }

    pub fn build(self) -> Result<Irokle<S>> {
        #[cfg(feature = "iroh")]
        if let Some(endpoint) = self.endpoint {
            let node = Irokle::with_storage(self.storage, self.config)?;
            let net = std::sync::Arc::new(
                crate::net::IrohNet::new_with_alpns(endpoint, node.clone(), self.alpns)
                    .map_err(|err| Error::Storage(format!("failed to configure iroh: {err}")))?,
            );
            if self.auto_accept {
                net.start_accept_loop().map_err(|err| {
                    Error::Storage(format!("failed to start iroh accept loop: {err}"))
                })?;
                net.start_resync_loop(std::time::Duration::from_secs(30))
                    .map_err(|err| {
                        Error::Storage(format!("failed to start iroh resync loop: {err}"))
                    })?;
            }
            return Ok(node.with_net(net));
        }

        let node = Irokle::with_storage(self.storage, self.config)?;
        Ok(node)
    }
}

impl<S: Storage> Irokle<S> {
    pub fn with_storage(storage: S, config: NodeConfig) -> Result<Self> {
        let oplog = Oplog::with_storage(storage);
        let sync = SyncEngine::new(oplog.clone());
        Ok(Self {
            oplog,
            sync,
            config,
            #[cfg(feature = "iroh")]
            net: None,
        })
    }

    #[cfg(feature = "iroh")]
    pub(crate) fn with_net(mut self, net: std::sync::Arc<crate::net::IrohNet<S>>) -> Self {
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
        for peer in peers {
            net.sync_peer_now(peer, topic_id).await?;
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
        self.oplog
            .create_topic_genesis(topic_id, actor_id, genesis, &self.config.signer)?;
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

    pub fn receive_sync_data(&self, peer_id: PeerId, data: SyncData) -> Result<SyncAck> {
        let mut ack = self.sync.receive_data(peer_id, peer_id, data)?;
        if ack.peer_id == self.peer_id() {
            ack.sign(&self.config.signer)?;
        }
        Ok(ack)
    }

    pub fn receive_sync_data_from(
        &self,
        source_peer_id: PeerId,
        data: SyncData,
    ) -> Result<SyncAck> {
        let mut ack = self
            .sync
            .receive_data(source_peer_id, self.peer_id(), data)?;
        ack.sign(&self.config.signer)?;
        Ok(ack)
    }

    pub fn receive_sync_data_as_local(&self, data: SyncData) -> Result<SyncAck> {
        self.receive_sync_data(self.peer_id(), data)
    }

    pub fn apply_sync_ack(&self, ack: &SyncAck) -> Result<()> {
        self.sync.apply_ack(ack)
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

    #[cfg(feature = "iroh")]
    fn put_replication_obligations(&self, topic_id: TopicId, op_id: OpId) -> Result<()> {
        let state = self
            .storage()
            .topic_state(&topic_id)?
            .ok_or(Error::TopicNotFound)?;
        for peer_id in select_sync_peers(topic_id, self.peer_id(), &state) {
            self.put_sync_obligation(peer_id, topic_id, [op_id].into())?;
        }
        Ok(())
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
        if matches!(options.write_concern, WriteConcern::AsyncReplication) && self.net.is_some() {
            self.put_replication_obligations(topic_id, op.id)?;
            let node = self.clone();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = node.sync_topic_now(topic_id).await;
                });
            }
        }
        Ok(record)
    }

    pub(crate) fn publish_control(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        control: TopicControl,
    ) -> Result<()> {
        self.oplog
            .create_control_op(topic_id, actor_id, control, &self.config.signer)
            .map(|_| ())
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
        dag_ops(self.oplog.storage(), topic_id, query)
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

#[derive(Clone)]
pub struct Topic<E: Event, S: Storage = MemoryStorage> {
    node: Irokle<S>,
    topic_id: TopicId,
    actor_id: ActorId,
    _event: PhantomData<E>,
}

impl<E: Event, S: Storage> Topic<E, S> {
    fn new(node: Irokle<S>, topic_id: TopicId, actor_id: ActorId) -> Self {
        Self {
            node,
            topic_id,
            actor_id,
            _event: PhantomData,
        }
    }
    pub fn id(&self) -> TopicId {
        self.topic_id
    }
    pub fn publish(&self, event: E) -> Result<EventRecord<E>> {
        self.publish_with(
            event,
            PublishOptions {
                write_concern: self.node.config.default_write_concern.clone(),
            },
        )
    }
    pub fn publish_with(&self, event: E, options: PublishOptions) -> Result<EventRecord<E>> {
        self.node
            .publish_event(self.topic_id, self.actor_id, event, options)
    }
    pub fn add_peer(&self, peer: PeerId) -> Result<()> {
        self.node
            .publish_control(self.topic_id, self.actor_id, TopicControl::AddPeer { peer })
    }
    pub fn remove_peer(&self, peer: PeerId) -> Result<()> {
        self.node.publish_control(
            self.topic_id,
            self.actor_id,
            TopicControl::RemovePeer { peer },
        )
    }
    pub fn leave(&self) -> Result<()> {
        self.node.reject_topic(self.topic_id)
    }
    pub fn set_replication_policy(&self, policy: crate::ReplicationPolicy) -> Result<()> {
        self.node.publish_control(
            self.topic_id,
            self.actor_id,
            TopicControl::SetReplicationPolicy { policy },
        )
    }
    pub fn history(&self, order: HistoryOrder) -> Result<Vec<EventRecord<E>>> {
        self.node.topic_history(self.topic_id, order)
    }
    pub fn dag(&self, query: DagQuery<OpId>) -> Result<Vec<Op>> {
        self.node.topic_dag(self.topic_id, query)
    }
    pub fn heads(&self) -> Result<BTreeSet<OpId>> {
        self.node.topic_heads(self.topic_id)
    }

    pub fn peer_reached_op(&self, peer_id: PeerId, op_id: OpId) -> Result<bool> {
        self.node.peer_reached_op(peer_id, op_id)
    }

    pub fn peers_reached_op(&self, op_id: OpId) -> Result<Vec<PeerId>> {
        self.node.peers_reached_op(op_id)
    }

    #[cfg(feature = "iroh")]
    pub async fn sync_now(&self) -> std::io::Result<()> {
        self.node.sync_topic_now(self.topic_id).await
    }
}

#[derive(Clone)]
pub struct RawTopic<S: Storage = MemoryStorage> {
    oplog: Oplog<S>,
    topic_id: TopicId,
}

impl<S: Storage> RawTopic<S> {
    pub fn id(&self) -> TopicId {
        self.topic_id
    }
    pub fn history(&self) -> Result<Vec<Op>> {
        topological(self.oplog.storage(), &self.topic_id)
    }
    pub fn dag(&self, query: DagQuery<OpId>) -> Result<Vec<Op>> {
        dag_ops(self.oplog.storage(), self.topic_id, query)
    }
    pub fn heads(&self) -> Result<BTreeSet<OpId>> {
        self.oplog.storage().heads(&self.topic_id)
    }

    pub fn peer_reached_op(&self, peer_id: PeerId, op_id: OpId) -> Result<bool> {
        self.oplog.storage().peer_reached_op(&peer_id, &op_id)
    }

    pub fn peers_reached_op(&self, op_id: OpId) -> Result<Vec<PeerId>> {
        self.oplog.storage().peers_reached_op(&op_id)
    }
}

fn dag_ops<S: Storage>(storage: &S, topic_id: TopicId, query: DagQuery<OpId>) -> Result<Vec<Op>> {
    if query.order == HistoryOrder::NewestFirst || !query.heads.is_empty() {
        let starts = if query.heads.is_empty() {
            storage.heads(&topic_id)?.into_iter().collect::<Vec<_>>()
        } else {
            query.heads
        };
        let mut seen = BTreeSet::new();
        let mut queue = starts
            .into_iter()
            .map(|head| (head, true))
            .collect::<VecDeque<_>>();
        let mut ids = Vec::new();
        while let Some((id, is_head)) = queue.pop_front() {
            if !seen.insert(id) {
                continue;
            }
            let meta = storage
                .get_meta(&id)?
                .ok_or_else(|| Error::Storage(format!("missing op meta for {id}")))?;
            if meta.topic_id != topic_id {
                return Err(Error::TopicMismatch);
            }
            if query.include_heads || !is_head {
                ids.push(id);
                if query.limit.is_some_and(|limit| ids.len() >= limit) {
                    break;
                }
            }
            for dep in meta.deps {
                queue.push_back((dep, false));
            }
        }
        if query.order == HistoryOrder::OldestFirst {
            let subset = ids.into_iter().collect::<BTreeSet<_>>();
            topological_subset(storage, &subset)
        } else {
            ids.into_iter()
                .map(|id| {
                    storage
                        .get_op(&id)?
                        .ok_or_else(|| Error::Storage(format!("missing op {id}")))
                })
                .collect()
        }
    } else {
        Ok(limited(topological(storage, &topic_id)?, query.limit))
    }
}

#[cfg(any(feature = "iroh", test))]
pub(crate) fn select_sync_peers(
    topic_id: TopicId,
    local_peer: PeerId,
    state: &crate::storage::TopicState,
) -> Vec<PeerId> {
    let max = state.replication_policy.max_sync_peers;
    if max == 0 {
        return Vec::new();
    }

    let mut scope = if state.replication_policy.selected_peers.is_empty() {
        state.members.clone()
    } else {
        state
            .replication_policy
            .selected_peers
            .intersection(&state.members)
            .copied()
            .collect()
    };
    scope.insert(local_peer);

    let candidates: SmallVec<[PeerId; 16]> = scope
        .iter()
        .copied()
        .filter(|peer| *peer != local_peer)
        .collect();
    if candidates.len() <= max {
        return candidates.into_vec();
    }

    let mut selected = BTreeSet::new();
    let mut shared = candidates
        .iter()
        .copied()
        .map(|peer| {
            (
                sync_peer_score(topic_id, PeerId::hash(b"shared"), peer),
                peer,
            )
        })
        .collect::<SmallVec<[_; 16]>>();
    shared.sort_by(|(left_score, left_peer), (right_score, right_peer)| {
        right_score
            .cmp(left_score)
            .then_with(|| left_peer.cmp(right_peer))
    });
    let shared_budget = SYNC_PEER_SHARED_OVERLAP
        .saturating_add(1)
        .min(max)
        .min(candidates.len());
    for (_, peer) in shared.into_iter().take(shared_budget) {
        selected.insert(peer);
        if selected.len() >= max {
            break;
        }
    }

    let ring = scope.into_iter().collect::<SmallVec<[PeerId; 16]>>();
    if let Some(local_index) = ring.iter().position(|peer| *peer == local_peer) {
        for offset in 1..ring.len() {
            if selected.len() >= max {
                break;
            }
            selected.insert(ring[(local_index + offset) % ring.len()]);
            if selected.len() >= max {
                break;
            }
            selected.insert(ring[(local_index + ring.len() - offset) % ring.len()]);
        }
    }

    let mut remaining = candidates
        .into_iter()
        .filter(|peer| !selected.contains(peer))
        .map(|peer| (sync_peer_score(topic_id, local_peer, peer), peer))
        .collect::<Vec<_>>();
    remaining.sort_by(|(left_score, left_peer), (right_score, right_peer)| {
        right_score
            .cmp(left_score)
            .then_with(|| left_peer.cmp(right_peer))
    });
    for (_, peer) in remaining {
        if selected.len() >= max {
            break;
        }
        selected.insert(peer);
    }

    selected.into_iter().collect()
}

#[cfg(any(feature = "iroh", test))]
fn sync_peer_score(topic_id: TopicId, local_peer: PeerId, peer: PeerId) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"irokle-sync-peer-v1");
    hasher.update(topic_id.as_ref());
    hasher.update(local_peer.as_ref());
    hasher.update(peer.as_ref());
    *hasher.finalize().as_bytes()
}

fn now_millis() -> Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| Error::Storage(format!("system time before unix epoch: {err}")))?
        .as_millis();
    millis
        .try_into()
        .map_err(|_| Error::Storage("system time does not fit in u64 milliseconds".into()))
}
