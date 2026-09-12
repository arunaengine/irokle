// SPDX-License-Identifier: MIT OR Apache-2.0
//! High-level node, topic, publishing, and sync facade APIs.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

mod builder;
mod peers;
mod topic;

pub(crate) use peers::select_sync_peers;
pub use topic::{RawTopic, Topic};

use crate::ActorClock;
use crate::history::{DagQuery, HistoryOrder, ordered};
use crate::oplog::{Oplog, topological_subset_entries};
use crate::reducer::EventRecord;
use crate::storage::{AdmissionEffects, OpMeta, SyncObligation, TopicState};
use crate::storage::{
    MemoryStorage, Storage, SyncPeerState, SyncPeerStatus, SyncStateUpdate, SyncStatusUpdate,
};
use crate::sync::{
    SyncAck, SyncData, SyncEngine, SyncFingerprint, SyncOpen, SyncPlan, SyncReport, SyncRequest,
    SyncSummary,
};
use crate::{
    ActorId, Ed25519Signer, Error, Event, EventEnvelope, EvictionKey, Op, OpId, PeerId, Result,
    Signer, TopicConfig, TopicControl, TopicEviction, TopicGenesis, TopicId, actor_id_for,
};

static TOPIC_NONCE: AtomicU64 = AtomicU64::new(0);
const SYNC_PEER_SHARED_OVERLAP: usize = 2;
#[cfg(feature = "iroh")]
const SYNC_TOPIC_CONCURRENCY: usize = 8;

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
    #[cfg(feature = "iroh")]
    eviction_sink: Option<tokio::sync::mpsc::UnboundedSender<TopicEviction>>,
}

impl<S: Storage> Irokle<S> {
    pub fn with_storage(storage: S, config: NodeConfig) -> Result<Self> {
        let oplog = Oplog::with_storage(storage);
        oplog.reconcile_pending_ops()?;
        let sync = SyncEngine::new(oplog.clone(), config.signer.peer_id());
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
        let mut syncs = tokio::task::JoinSet::new();
        let mut first_error = None;
        for peer in peers {
            while syncs.len() >= SYNC_TOPIC_CONCURRENCY {
                if let Some(result) = syncs.join_next().await {
                    record_sync_topic_join_result(result, &mut first_error);
                }
            }
            let net = Arc::clone(net);
            syncs.spawn(async move { (peer, net.sync_peer_now(peer, topic_id).await) });
        }
        while let Some(result) = syncs.join_next().await {
            record_sync_topic_join_result(result, &mut first_error);
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    pub fn create_topic<E: Event>(&self, mut config: TopicConfig) -> Result<Topic<E, S>> {
        self.validate_concern(&self.config.default_write_concern)?;
        config.initial_peers.insert(self.peer_id());
        let topic_id = self.next_topic_id::<E>()?;
        let actor_id = actor_id_for(topic_id, self.peer_id());
        let genesis = TopicGenesis {
            event_type_id: E::TYPE_ID.to_owned(),
            initial_peers: config.initial_peers,
            replication_policy: config.replication_policy,
        };
        #[cfg(feature = "iroh")]
        let op = self.oplog.create_topic_effects(
            topic_id,
            actor_id,
            genesis,
            &self.config.signer,
            |_op, meta, state| {
                self.replication_admission_effects(
                    topic_id,
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
        );
        Ok(Topic::new(self.clone(), topic_id, actor_id))
    }

    /// Create a topic and publish its first event in one storage transaction.
    pub fn create_topic_with_event<E: Event>(
        &self,
        mut config: TopicConfig,
        event: E,
    ) -> Result<(Topic<E, S>, EventRecord<E>)> {
        self.validate_concern(&self.config.default_write_concern)?;
        config.initial_peers.insert(self.peer_id());
        let topic_id = self.next_topic_id::<E>()?;
        let actor_id = actor_id_for(topic_id, self.peer_id());
        let genesis = TopicGenesis {
            event_type_id: E::TYPE_ID.to_owned(),
            initial_peers: config.initial_peers,
            replication_policy: config.replication_policy,
        };
        let envelope = EventEnvelope::encode_event(&event)?;
        let (_, (event_op, meta)) = self.oplog.create_genesis_effects(
            topic_id,
            actor_id,
            genesis,
            envelope,
            &self.config.signer,
            |_op, meta, state| {
                self.replication_admission_effects(
                    topic_id,
                    meta,
                    state,
                    &self.config.default_write_concern,
                )
            },
        )?;
        let record = EventRecord::new(
            event,
            event_op.id,
            meta.actor_id,
            meta.actor_seq,
            meta.observed_clock,
        );
        #[cfg(feature = "iroh")]
        self.wake_async_replication(
            topic_id,
            event_op.id,
            &self.config.default_write_concern,
            "topic genesis replication wake failed",
        );
        Ok((Topic::new(self.clone(), topic_id, actor_id), record))
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

    /// Ids this node references in `topic_id` but cannot resolve locally. An
    /// empty set means the topic is causally whole here; anything else is a
    /// hole sync must repair before the topic can be certified to a peer.
    pub fn topic_unresolved(&self, topic_id: TopicId) -> Result<BTreeSet<OpId>> {
        self.oplog.topic_unresolved(&topic_id)
    }

    /// Audit stored records again on the next integrity question instead of
    /// trusting the earlier verdict. Admission keeps topics whole, so this only
    /// matters for damage that happened outside irokle.
    pub fn recheck_topics(&self) -> Result<()> {
        self.oplog.recheck_topics()
    }

    /// Discard ops of `topic_id` that no head reaches and rebuild the topic from
    /// the ops that remain. A store damaged by the pre-`reset_topic_and_admit`
    /// genesis reset can hold a descendant whose ancestry belongs to the
    /// replaced chain; no peer can supply that ancestry under the current
    /// genesis, so the topic stays unresolved until the descendant goes. The
    /// returned payloads are the embedder's to re-emit.
    pub fn quarantine_orphans(&self, topic_id: TopicId) -> Result<Option<TopicEviction>> {
        self.oplog.quarantine_orphans(&topic_id)
    }

    /// Evictions this node recorded durably and no consumer has acknowledged
    /// yet. Each was written in the same transaction that discarded the
    /// payloads, so this is what a restart must drain before it can treat
    /// eviction recovery as complete: an eviction delivered only through the
    /// in-memory sink and lost to a crash is still here.
    pub fn pending_evictions(&self) -> Result<Vec<TopicEviction>> {
        self.storage().pending_evictions()
    }

    /// Seal a topic so a concurrent genesis reset cannot discard writes while
    /// its departing holder proves the drain boundary.
    pub fn seal_topic(&self, topic_id: TopicId) -> Result<bool> {
        self.storage().seal_topic(&topic_id)
    }

    pub fn unseal_topic(&self, topic_id: TopicId) -> Result<bool> {
        self.storage().unseal_topic(&topic_id)
    }

    /// Release a journalled eviction, named by [`TopicEviction::key`]. Call this
    /// only once the payloads are durably owned elsewhere; until then the record
    /// is their only copy. Acknowledging twice is harmless.
    pub fn clear_eviction(&self, key: &EvictionKey) -> Result<()> {
        self.storage().clear_eviction(key)
    }

    /// Run [`Irokle::quarantine_orphans`] over every local topic.
    pub fn quarantine_topics(&self) -> Result<Vec<TopicEviction>> {
        let mut quarantined = Vec::new();
        for info in self.list_topics()? {
            if let Some(eviction) = self.oplog.quarantine_orphans(&info.topic_id)? {
                quarantined.push(eviction);
            }
        }
        Ok(quarantined)
    }

    pub fn negotiate_sync(&self, peer_id: PeerId, remote: &SyncSummary) -> Result<SyncPlan> {
        self.sync.negotiate(peer_id, remote)
    }

    #[cfg(feature = "iroh")]
    pub(crate) fn negotiate_page(&self, peer_id: PeerId, remote: &SyncSummary) -> Result<SyncPlan> {
        self.sync.negotiate_page(peer_id, remote)
    }

    #[cfg(feature = "iroh")]
    pub(crate) fn response_page(&self, peer_id: PeerId, request: &SyncRequest) -> Result<SyncData> {
        self.sync.response_page(peer_id, request)
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

    /// Admit sync data from `source_peer_id` and return the signed ack payload
    /// plus any topic evictions produced by genesis tie-break resolution.
    pub fn receive_sync_data_from(
        &self,
        source_peer_id: PeerId,
        data: SyncData,
    ) -> Result<(SyncAck, Vec<TopicEviction>)> {
        self.receive_sync_data_from_evicting(source_peer_id, data)
    }

    /// Alias for [`Self::receive_sync_data_from`] with an explicit name for
    /// callers handling genesis tie-break evictions. The embedder consumes
    /// evictions to re-emit discarded payloads under the winning genesis;
    /// re-emission itself is out of scope for irokle.
    pub fn receive_sync_data_from_evicting(
        &self,
        source_peer_id: PeerId,
        data: SyncData,
    ) -> Result<(SyncAck, Vec<TopicEviction>)> {
        // Verify each op once up front; both the unknown-topic dry run and
        // the real admission below reuse the result instead of re-running
        // the ed25519 verification per pass.
        for op in &data.ops {
            if op.signed.body.topic_id != data.topic_id {
                return Err(Error::TopicMismatch);
            }
        }
        let mut verified = BTreeSet::new();
        for op in &data.ops {
            op.validate()?;
            verified.insert(op.id);
        }
        self.check_unknown_topic(source_peer_id, &data, &verified)?;
        let (mut ack, evictions) = match self.sync.receive_data_preverified(
            source_peer_id,
            self.peer_id(),
            data,
            &verified,
        ) {
            Ok(received) => received,
            Err(error) => {
                #[cfg(feature = "iroh")]
                if let Error::ReceiveCommitted { ack, .. } = &error
                    && let Some(net) = &self.net
                {
                    net.schedule_resync(source_peer_id, ack.topic_id);
                    if let Err(error) = net.schedule_topic_recheck(ack.topic_id) {
                        tracing::warn!(topic_id = %ack.topic_id, %error, "committed receive recheck failed");
                    }
                }
                return Err(error);
            }
        };
        let result = (|| -> Result<()> {
            self.put_receive_forward_obligations(source_peer_id, ack.topic_id, &ack.accepted)?;
            ack.sign(&self.config.signer)
        })();
        if let Err(source) = result {
            #[cfg(feature = "iroh")]
            if let Some(net) = &self.net {
                net.schedule_resync(source_peer_id, ack.topic_id);
                if let Err(error) = net.schedule_topic_recheck(ack.topic_id) {
                    tracing::warn!(topic_id = %ack.topic_id, %error, "committed receive recheck failed");
                }
            }
            return Err(Error::ReceiveCommitted {
                ack: Box::new(ack),
                evictions,
                source: Box::new(source),
            });
        }
        Ok((ack, evictions))
    }

    pub fn receive_sync_data_as_local(
        &self,
        data: SyncData,
    ) -> Result<(SyncAck, Vec<TopicEviction>)> {
        let (mut ack, evictions) = self
            .sync
            .receive_data(self.peer_id(), self.peer_id(), data)?;
        if let Err(source) = ack.sign(&self.config.signer) {
            return Err(Error::ReceiveCommitted {
                ack: Box::new(ack),
                evictions,
                source: Box::new(source),
            });
        }
        Ok((ack, evictions))
    }

    pub fn apply_sync_ack(&self, ack: &SyncAck) -> Result<()> {
        self.sync.apply_ack(ack)
    }

    /// Apply many sync acks with batched storage writes. Each ack is verified
    /// and validated individually; a failed ack does not block the others.
    /// Returns one result per input ack, in order.
    pub fn apply_sync_acks(&self, acks: &[SyncAck]) -> Vec<Result<()>> {
        self.sync.apply_acks(acks)
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
    pub(crate) fn record_fingerprint(
        &self,
        peer_id: PeerId,
        topic_id: TopicId,
        fingerprint: [u8; 32],
    ) -> Result<bool> {
        self.sync.record_fingerprint(peer_id, topic_id, fingerprint)
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
        Ok(())
    }

    fn check_unknown_topic(
        &self,
        source_peer_id: PeerId,
        data: &SyncData,
        verified: &BTreeSet<crate::OpId>,
    ) -> Result<()> {
        if self.storage().topic_state(&data.topic_id)?.is_some() {
            return Ok(());
        }
        let dry_storage = MemoryStorage::new();
        let dry_oplog = Oplog::with_storage(dry_storage.clone());
        dry_oplog.receive_ops_from_peer_preverified(
            Some(source_peer_id),
            data.ops.clone(),
            verified,
        )?;
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
        self.sync.put_obligation(peer_id, topic_id, op_ids)?;
        #[cfg(feature = "iroh")]
        if let Some(net) = &self.net {
            net.schedule_resync(peer_id, topic_id);
        }
        Ok(())
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
                // Status is bookkeeping: its failure must not drop the
                // obligations the remaining peers still need.
                if let Err(error) = self.record_replication_scheduled(peer_id, topic_id) {
                    tracing::warn!(%topic_id, %peer_id, %error, "forward replication bookkeeping failed");
                }
            }
        }
        Ok(())
    }

    fn validate_concern(&self, concern: &WriteConcern) -> Result<()> {
        if matches!(concern, WriteConcern::AsyncReplication) {
            #[cfg(feature = "iroh")]
            if self.net.is_some() {
                return Ok(());
            }
            return Err(Error::ReplicationUnavailable);
        }
        Ok(())
    }

    fn replication_admission_effects(
        &self,
        topic_id: TopicId,
        meta: &OpMeta,
        state: &TopicState,
        write_concern: &WriteConcern,
    ) -> Result<AdmissionEffects> {
        if !matches!(write_concern, WriteConcern::AsyncReplication) {
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
                    op_ids: BTreeSet::new(),
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
    ) {
        let Some(net) = &self.net else {
            return;
        };
        let result = (|| -> Result<()> {
            let state = self
                .storage()
                .topic_state(&topic_id)?
                .ok_or(Error::TopicNotFound)?;
            if matches!(write_concern, WriteConcern::AsyncReplication) {
                for peer_id in select_sync_peers(topic_id, self.peer_id(), &state) {
                    self.record_replication_scheduled(peer_id, topic_id)?;
                }
            }
            Ok(())
        })();
        if let Err(error) = result {
            tracing::warn!(%topic_id, %error, "committed replication bookkeeping failed");
        }
        if let Err(error) = net.schedule_topic_recheck(topic_id) {
            tracing::warn!(%topic_id, %error, "{}", wake_failed_message);
        }
    }

    fn record_replication_scheduled(&self, peer_id: PeerId, topic_id: TopicId) -> Result<()> {
        let pending = self.storage().sync_obligations(&peer_id, &topic_id)?.len();
        let state = if pending > 0 {
            SyncStateUpdate::BehindUnlessFailed
        } else {
            SyncStateUpdate::Keep
        };
        self.storage().update_sync_status(
            &peer_id,
            &topic_id,
            &SyncStatusUpdate {
                pending_obligations: Some(pending),
                state,
                ..SyncStatusUpdate::default()
            },
        )?;
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

        for (peer_id, pending) in self.storage().topic_obligation_counts(&topic_id)? {
            by_peer
                .entry(peer_id)
                .or_insert_with(|| SyncPeerStatus {
                    peer_id,
                    topic_id,
                    state: SyncPeerState::Behind,
                    ..SyncPeerStatus::default()
                })
                .pending_obligations = pending;
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
        let attempt_ms = now_millis()?;
        let pending = self.storage().sync_obligations(&peer_id, &topic_id)?.len();
        let mut update = SyncStatusUpdate {
            pending_obligations: Some(pending),
            last_attempt_ms: Some(attempt_ms),
            ..SyncStatusUpdate::default()
        };
        match result {
            Ok(()) => {
                update.successful_attempts = 1;
                update.last_success_ms = Some(attempt_ms);
                update.last_error = Some(None);
                update.state = SyncStateUpdate::Set(if pending == 0 {
                    SyncPeerState::Healthy
                } else {
                    SyncPeerState::Behind
                });
            }
            Err(error) => {
                update.failed_attempts = 1;
                update.last_error = Some(Some(error.to_string()));
                update.state = SyncStateUpdate::Set(SyncPeerState::Failed);
            }
        }
        self.storage()
            .update_sync_status(&peer_id, &topic_id, &update)?;
        Ok(())
    }

    pub(crate) fn publish_event<E: Event>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        event: E,
        options: PublishOptions,
    ) -> Result<EventRecord<E>> {
        self.validate_concern(&options.write_concern)?;
        let envelope = EventEnvelope::encode_event(&event)?;
        let (op, meta) = self.oplog.create_event_effects(
            topic_id,
            actor_id,
            envelope,
            &self.config.signer,
            |_op, meta, state| {
                self.replication_admission_effects(topic_id, meta, state, &options.write_concern)
            },
        )?;
        let record = EventRecord::new(
            event,
            op.id,
            meta.actor_id,
            meta.actor_seq,
            meta.observed_clock,
        );
        #[cfg(feature = "iroh")]
        self.wake_async_replication(
            topic_id,
            op.id,
            &options.write_concern,
            "async replication wake failed",
        );
        Ok(record)
    }

    pub(crate) fn publish_control(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        control: TopicControl,
    ) -> Result<()> {
        self.validate_concern(&self.config.default_write_concern)?;
        #[cfg(feature = "iroh")]
        let op = self.oplog.create_control_effects(
            topic_id,
            actor_id,
            control,
            &self.config.signer,
            |_op, meta, state| {
                self.replication_admission_effects(
                    topic_id,
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
        );
        Ok(())
    }

    pub(crate) fn topic_history<E: Event>(
        &self,
        topic_id: TopicId,
        order: HistoryOrder,
    ) -> Result<Vec<EventRecord<E>>> {
        let storage = self.oplog.storage();
        let ids = storage.list_op_ids(&topic_id)?;
        let entries = topological_subset_entries(storage, &ids)?;
        if entries.len() != ids.len() || !self.oplog.topic_unresolved(&topic_id)?.is_empty() {
            return Err(Error::Storage("incomplete topic history".into()));
        }
        let mut records = Vec::new();
        for (op, meta) in entries {
            if let crate::TopicPayload::Event(envelope) = &op.signed.body.payload {
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

    pub(crate) fn history_after_clock<E: Event>(
        &self,
        topic_id: TopicId,
        clock: &ActorClock,
        order: HistoryOrder,
    ) -> Result<Vec<EventRecord<E>>> {
        let storage = self.oplog.storage();
        let mut seen = BTreeSet::new();
        let mut queue = storage
            .heads(&topic_id)?
            .into_iter()
            .collect::<VecDeque<_>>();

        while let Some(op_id) = queue.pop_front() {
            if !seen.insert(op_id) {
                continue;
            }
            let meta = storage
                .get_meta(&op_id)?
                .ok_or_else(|| Error::Storage(format!("missing op meta for {op_id}")))?;
            if meta.topic_id != topic_id {
                return Err(Error::TopicMismatch);
            }
            queue.extend(meta.deps);
        }

        let entries = topological_subset_entries(storage, &seen)?;
        if entries.len() != seen.len() || !self.oplog.topic_unresolved(&topic_id)?.is_empty() {
            return Err(Error::Storage("incomplete topic history".into()));
        }
        let mut records = Vec::new();
        for (op, meta) in entries {
            if clock.get(&meta.actor_id) >= meta.actor_seq {
                continue;
            }
            if let crate::TopicPayload::Event(envelope) = &op.signed.body.payload {
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

    pub(crate) fn topic_actor_clock(&self, topic_id: TopicId) -> Result<ActorClock> {
        let storage = self.oplog.storage();
        storage
            .topic_state(&topic_id)?
            .ok_or(Error::TopicNotFound)?;
        storage.actor_clock(&topic_id)
    }

    pub(crate) fn topic_observed_clock(&self, topic_id: TopicId) -> Result<ActorClock> {
        let storage = self.oplog.storage();
        let state = storage
            .topic_state(&topic_id)?
            .ok_or(Error::TopicNotFound)?;
        let local_peer = self.peer_id();
        let mut clock = storage.actor_clock(&topic_id)?;
        for peer in &state.members {
            if *peer == local_peer {
                continue;
            }
            match storage.peer_ack(peer, &topic_id)? {
                Some(ack) => clock = clock.intersect(&ack.clock),
                None => return Ok(ActorClock::new()),
            }
        }
        Ok(clock)
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

#[cfg(feature = "iroh")]
fn record_sync_topic_join_result(
    result: std::result::Result<(PeerId, std::io::Result<()>), tokio::task::JoinError>,
    first_error: &mut Option<std::io::Error>,
) {
    match result {
        Ok((_, Ok(()))) => {}
        Ok((_, Err(error))) => {
            if first_error.is_none() {
                *first_error = Some(error);
            }
        }
        Err(error) => {
            if first_error.is_none() {
                *first_error = Some(std::io::Error::other(error.to_string()));
            }
        }
    }
}
