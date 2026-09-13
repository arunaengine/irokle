// SPDX-License-Identifier: MIT OR Apache-2.0
//! High-level node, topic, publishing, and sync facade APIs.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

mod builder;
mod peers;
mod topic;

#[cfg(test)]
pub(crate) use peers::{PEER_FAILURE_LIMIT, select_sync_peers};
use peers::{PeerHealthStore, select_sync_targets};
pub use topic::{RawTopic, Topic};

use crate::ActorClock;
use crate::history::{DagQuery, HistoryOrder, ordered};
use crate::oplog::{Oplog, topological_subset_entries};
use crate::reducer::EventRecord;
use crate::storage::{
    AdmissionEffects, MAX_STAGED_IDLE_MS, OpMeta, StagedTopic, SyncObligation, TopicState,
};
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

/// Whether a failed attempt says the peer could not be reached, rather than
/// that one exchange was refused. Protocol rejections are topic-local, so they
/// must not demote a peer that answers other topics fine.
#[cfg(feature = "iroh")]
fn is_unreachable(error: &std::io::Error) -> bool {
    !matches!(
        error.kind(),
        std::io::ErrorKind::InvalidData | std::io::ErrorKind::InvalidInput
    )
}
const SYNC_PEER_SHARED_OVERLAP: usize = 2;
#[cfg(feature = "iroh")]
const SYNC_TOPIC_CONCURRENCY: usize = 8;

/// What receiving sync data did.
#[derive(Clone, Debug)]
pub enum ReceiveOutcome {
    /// The data reached the active topic; the signed ack speaks for it.
    Acked {
        ack: Box<SyncAck>,
        evictions: Vec<TopicEviction>,
    },
    /// The topic is not held here and its staged history does not prove
    /// membership yet. This is no ack and certifies nothing.
    Staged(StagedTopic),
}

/// Where data for a possibly unknown topic stands.
enum Bootstrap {
    /// The topic is active, with the ids a promotion just admitted.
    Active(BTreeSet<OpId>),
    Staged(StagedTopic),
}

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
    peer_health: Arc<PeerHealthStore>,
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
            peer_health: Arc::new(PeerHealthStore::default()),
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

    /// Sync targets for `topic_id` under the topic's replication policy and
    /// this node's runtime peer health. Every scheduling and eligibility path
    /// reads the same view, so an alternate chosen because a preferred peer is
    /// unreachable is not rejected elsewhere as an unselected target.
    pub(crate) fn sync_peers(&self, topic_id: TopicId, state: &TopicState) -> Vec<PeerId> {
        self.peer_health
            .with_view(|health| select_sync_targets(topic_id, self.peer_id(), state, health).peers)
    }

    /// Runtime reachability observations, updated from real attempt outcomes by
    /// [`Irokle::record_sync_result`].
    #[cfg(test)]
    pub(crate) fn peer_health(&self) -> &PeerHealthStore {
        &self.peer_health
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
        let peers = self.sync_peers(topic_id, &state);
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

    /// Ids `view`'s topic cannot resolve, with any hole scan recorded under the
    /// view's own branch and epoch.
    #[cfg(feature = "iroh")]
    pub(crate) fn view_unresolved(
        &self,
        view: &crate::storage::TopicView,
    ) -> Result<BTreeSet<OpId>> {
        self.oplog.view_unresolved(view)
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

    /// Run [`Irokle::quarantine_orphans`] over every local topic. One topic that
    /// cannot be quarantined, because it is sealed or its records are
    /// unreadable, is a per-topic outcome: the remaining topics are still
    /// visited and every eviction already committed is still returned, because
    /// each was written in the transaction that discarded its payloads. Only a
    /// failure to enumerate topics at all is reported as a global failure.
    pub fn quarantine_topics(&self) -> Result<Vec<TopicEviction>> {
        let mut quarantined = Vec::new();
        for info in self.list_topics()? {
            match self.oplog.quarantine_orphans(&info.topic_id) {
                Ok(Some(eviction)) => quarantined.push(eviction),
                Ok(None) => {}
                Err(error) => tracing::warn!(
                    topic_id = %info.topic_id,
                    %error,
                    "leaving topic quarantine for a later sweep"
                ),
            }
        }
        Ok(quarantined)
    }

    /// The whole missing closure against `remote`, unbounded: an export of
    /// history, not a sync step. Sync uses [`Self::negotiate_page`].
    pub fn negotiate_sync(&self, peer_id: PeerId, remote: &SyncSummary) -> Result<SyncPlan> {
        self.sync.negotiate(peer_id, remote)
    }

    /// Bounded push page and request against `remote`; see
    /// [`crate::sync::SyncEngine::negotiate_page`].
    pub fn negotiate_page(
        &self,
        peer_id: PeerId,
        remote: &SyncSummary,
        budget: crate::sync::PageBudget,
    ) -> Result<(SyncPlan, bool)> {
        self.sync.negotiate_page(peer_id, remote, budget)
    }

    /// One bounded page of `request`; see [`crate::sync::SyncEngine::response_page`].
    ///
    /// A transport repeats request and page until the page reports no more:
    ///
    /// ```
    /// use irokle::sync::{PageBudget, SyncCredit, SyncData};
    /// use irokle::{Ed25519Signer, Irokle, Storage, TopicConfig};
    ///
    /// #[derive(Clone, irokle::Event, serde::Deserialize, serde::Serialize)]
    /// #[irokle(type_id = "doc.note")]
    /// struct Note(u32);
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let alice = Irokle::builder().with_signer(Ed25519Signer::from_bytes(&[1; 32])).build()?;
    /// let bob = Irokle::builder().with_signer(Ed25519Signer::from_bytes(&[2; 32])).build()?;
    /// let topic = alice.create_topic::<Note>(TopicConfig {
    ///     initial_peers: [bob.peer_id()].into(),
    ///     ..TopicConfig::default()
    /// })?;
    /// // Bob holds the genesis first; pages then carry the events.
    /// let genesis = alice.plan_sync_data(bob.peer_id(), &bob.sync_summary(topic.id())?)?;
    /// bob.receive_sync_outcome(alice.peer_id(), genesis)?;
    /// for index in 0..10 {
    ///     topic.publish(Note(index))?;
    /// }
    /// let mut pages = 0;
    /// loop {
    ///     let mut request = bob.plan_sync_request(alice.peer_id(), &alice.sync_summary(topic.id())?)?;
    ///     if request.wants.is_empty() && request.actor_range_hints.is_empty() {
    ///         break;
    ///     }
    ///     request.credit = SyncCredit { ops: 4, ..request.credit };
    ///     let page = alice.response_page(bob.peer_id(), &request, PageBudget::from_credit(request.credit))?;
    ///     assert!(page.ops.len() <= 4);
    ///     let data = SyncData { topic_id: topic.id(), ops: page.ops };
    ///     bob.receive_sync_outcome(alice.peer_id(), data)?;
    ///     pages += 1;
    /// }
    /// assert_eq!(pages, 3);
    /// assert_eq!(bob.storage().actor_clock(&topic.id())?, alice.storage().actor_clock(&topic.id())?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn response_page(
        &self,
        peer_id: PeerId,
        request: &SyncRequest,
        budget: crate::sync::PageBudget,
    ) -> Result<crate::sync::PlannedPage> {
        self.sync.response_page(peer_id, request, budget)
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

    /// Admit sync data from `source_peer_id` and return the signed ack plus any
    /// genesis tie-break evictions. Data for a topic this node does not hold
    /// fails with [`Error::BootstrapPending`] while it stays staged.
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
        match self.receive_sync_outcome(source_peer_id, data)? {
            ReceiveOutcome::Acked { ack, evictions } => Ok((*ack, evictions)),
            ReceiveOutcome::Staged(staged) => Err(Error::BootstrapPending {
                staged: staged.clock,
            }),
        }
    }

    /// Receive sync data. Data for a topic this node does not hold is staged
    /// per source until the staged history makes this node and the source
    /// members; then the whole history is admitted atomically and acked.
    pub fn receive_sync_outcome(
        &self,
        source_peer_id: PeerId,
        data: SyncData,
    ) -> Result<ReceiveOutcome> {
        // Verify each op once up front; staging and the real admission below
        // reuse the result instead of re-running the ed25519 verification.
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
        let forwarded = std::cell::RefCell::new(BTreeSet::new());
        let forward = |source: Option<PeerId>, entries: &[(Op, OpMeta)], state: &TopicState| {
            forwarded.borrow_mut().insert(state.topic_id);
            self.forward_effects(source, entries, state)
        };
        let promoted = match self.bootstrap_unknown(source_peer_id, &data, &forward)? {
            Bootstrap::Active(promoted) => promoted,
            Bootstrap::Staged(staged) => return Ok(ReceiveOutcome::Staged(staged)),
        };
        let received = self.sync.receive_data_preverified(
            source_peer_id,
            self.peer_id(),
            data,
            &verified,
            Some(&forward),
        );
        #[cfg(feature = "iroh")]
        if let Some(net) = &self.net {
            for topic_id in forwarded.borrow().iter() {
                if let Err(error) = net.schedule_topic_recheck(*topic_id) {
                    tracing::warn!(%topic_id, %error, "forwarded replication wake failed");
                }
            }
        }
        for topic_id in forwarded.borrow().iter() {
            self.note_forwarded(source_peer_id, *topic_id);
        }
        let (mut ack, evictions) = match received {
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
        ack.accepted
            .extend(promoted.intersection(&verified).copied());
        if let Err(source) = ack.sign(&self.config.signer) {
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
        Ok(ReceiveOutcome::Acked {
            ack: Box::new(ack),
            evictions,
        })
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

    /// Stage data for a topic this node does not hold, and promote the staged
    /// history of the source once it makes this node and the source members.
    fn bootstrap_unknown(
        &self,
        source_peer_id: PeerId,
        data: &SyncData,
        forward: crate::oplog::ReceiveEffects<'_>,
    ) -> Result<Bootstrap> {
        let storage = self.storage();
        let topic_id = data.topic_id;
        if storage.topic_state(&topic_id)?.is_some() {
            return Ok(Bootstrap::Active(BTreeSet::new()));
        }
        let now_ms = now_millis()?;
        storage.expire_bootstrap(now_ms.saturating_sub(MAX_STAGED_IDLE_MS))?;
        // Data that already completes the proof is promoted without a staging write.
        let mut history = storage.staged_bootstrap_ops(&source_peer_id, &topic_id)?;
        // An op at a staged position with another id comes from a replaced
        // branch of the source; that branch's staging is dropped, not mixed in.
        let slots = history
            .iter()
            .map(|op| ((op.signed.body.actor_id, op.signed.body.actor_seq), op.id))
            .collect::<BTreeMap<_, _>>();
        if data.ops.iter().any(|op| {
            slots
                .get(&(op.signed.body.actor_id, op.signed.body.actor_seq))
                .is_some_and(|staged| *staged != op.id)
        }) {
            storage.discard_bootstrap(&source_peer_id, &topic_id)?;
            history.clear();
        }
        let known = history.iter().map(|op| op.id).collect::<BTreeSet<_>>();
        history.extend(
            data.ops
                .iter()
                .filter(|op| !known.contains(&op.id))
                .cloned(),
        );
        let batch =
            match self
                .oplog
                .bootstrap_batch(self.peer_id(), source_peer_id, history, Some(forward))
            {
                Ok(Some(batch)) => batch,
                Ok(None) => {
                    return match storage.stage_bootstrap_ops(
                        source_peer_id,
                        topic_id,
                        data.ops.clone(),
                        now_ms,
                    ) {
                        Err(Error::AdmissionConflict) => self.bootstrap_raced(topic_id),
                        staged => Ok(Bootstrap::Staged(staged?)),
                    };
                }
                Err(error) => {
                    // Invalid signed history never becomes valid; drop the session.
                    if !is_backend_failure(&error) {
                        storage.discard_bootstrap(&source_peer_id, &topic_id)?;
                    }
                    return Err(error);
                }
            };
        let promoted = batch.entries.iter().map(|(op, _)| op.id).collect();
        match storage.promote_bootstrap(batch) {
            Ok(()) => Ok(Bootstrap::Active(promoted)),
            Err(Error::AdmissionConflict) => self.bootstrap_raced(topic_id),
            Err(error) => Err(error),
        }
    }

    /// A staging write refused because the topic became active meanwhile. The
    /// normal receive then decides between branches; it must never admit an
    /// unknown topic without staging, so a conflict on a missing topic fails.
    fn bootstrap_raced(&self, topic_id: TopicId) -> Result<Bootstrap> {
        if self.storage().topic_state(&topic_id)?.is_none() {
            return Err(Error::AdmissionConflict);
        }
        Ok(Bootstrap::Active(BTreeSet::new()))
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

    /// Forwarding work a received batch commits with its ops: one coalesced
    /// clock target per selected peer other than the source and this node.
    /// Storage skips a peer whose certified ack already covers the target.
    fn forward_effects(
        &self,
        source: Option<PeerId>,
        entries: &[(Op, OpMeta)],
        state: &TopicState,
    ) -> Result<AdmissionEffects> {
        let mut target_clock = ActorClock::new();
        for (_, meta) in entries {
            target_clock.observe(meta.actor_id, meta.actor_seq);
        }
        Ok(AdmissionEffects {
            sync_obligations: self
                .sync_peers(state.topic_id, state)
                .into_iter()
                .filter(|peer_id| Some(*peer_id) != source && *peer_id != self.peer_id())
                .map(|peer_id| SyncObligation::clock(peer_id, state.topic_id, target_clock.clone()))
                .collect(),
        })
    }

    /// Status bookkeeping for peers owed forwarded work on `topic_id`. Its
    /// failure must not fail the receive that already committed the work.
    fn note_forwarded(&self, source_peer_id: PeerId, topic_id: TopicId) {
        let result = (|| -> Result<()> {
            let Some(state) = self.storage().topic_state(&topic_id)? else {
                return Ok(());
            };
            for peer_id in self.sync_peers(topic_id, &state) {
                if peer_id != source_peer_id
                    && peer_id != self.peer_id()
                    && self.storage().has_sync_obligations(&peer_id, &topic_id)?
                {
                    self.record_replication_scheduled(peer_id, topic_id)?;
                }
            }
            Ok(())
        })();
        if let Err(error) = result {
            tracing::warn!(%topic_id, %error, "forward replication bookkeeping failed");
        }
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
            sync_obligations: self
                .sync_peers(topic_id, state)
                .into_iter()
                .map(|peer_id| SyncObligation::clock(peer_id, topic_id, target_clock.clone()))
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
                for peer_id in self.sync_peers(topic_id, &state) {
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
        let pending = self.storage().sync_obligation_count(&peer_id, &topic_id)?;
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

    /// Record one attempt's reachability, once per attempt however many topics
    /// it served. Only reachability failures demote a peer: a refused exchange
    /// says nothing about other topics. Returns whether selection changed.
    #[cfg(feature = "iroh")]
    pub(crate) fn note_peer_outcome<'a>(
        &self,
        peer_id: PeerId,
        results: impl IntoIterator<Item = std::result::Result<(), &'a std::io::Error>>,
    ) -> bool {
        let mut reached = false;
        let mut unreachable = false;
        for result in results {
            match result {
                Ok(()) => reached = true,
                Err(error) => unreachable |= is_unreachable(error),
            }
        }
        if reached {
            self.peer_health.record_success(&peer_id)
        } else if unreachable {
            self.peer_health.record_failure(peer_id)
        } else {
            false
        }
    }

    /// Record how attempt `(epoch, sequence)` ended. The state follows the outcome,
    /// so a partial pull stays `Behind`; Complete and Advanced count as successes,
    /// Blocked and Failed as failures. Peer health is left to the caller.
    #[cfg(any(feature = "iroh", test))]
    pub(crate) fn record_attempt_result(
        &self,
        peer_id: PeerId,
        topic_id: TopicId,
        attempt: (u64, u64),
        outcome: &crate::AttemptOutcome,
    ) -> Result<SyncPeerStatus> {
        let attempt_ms = now_millis()?;
        let pending = self.storage().sync_obligation_count(&peer_id, &topic_id)?;
        let (state, error) = match outcome {
            crate::AttemptOutcome::Complete => (SyncPeerState::Healthy, None),
            crate::AttemptOutcome::Advanced => (SyncPeerState::Behind, None),
            crate::AttemptOutcome::Blocked(reason) => (SyncPeerState::Behind, Some(reason.clone())),
            crate::AttemptOutcome::Failed(reason) => (SyncPeerState::Failed, Some(reason.clone())),
        };
        let advanced = matches!(
            outcome,
            crate::AttemptOutcome::Complete | crate::AttemptOutcome::Advanced
        );
        self.storage().update_sync_status(
            &peer_id,
            &topic_id,
            &SyncStatusUpdate {
                successful_attempts: u64::from(advanced),
                failed_attempts: u64::from(!advanced),
                state: SyncStateUpdate::Set(state),
                pending_obligations: Some(pending),
                last_attempt_ms: Some(attempt_ms),
                last_success_ms: advanced.then_some(attempt_ms),
                last_error: Some(error),
                attempt: Some(attempt),
                ..SyncStatusUpdate::default()
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn record_sync_result(
        &self,
        peer_id: PeerId,
        topic_id: TopicId,
        result: std::result::Result<(), &std::io::Error>,
    ) -> Result<()> {
        let attempt_ms = now_millis()?;
        let pending = self.storage().sync_obligation_count(&peer_id, &topic_id)?;
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
        let view = storage
            .topic_view(&topic_id, None)?
            .ok_or(Error::TopicNotFound)?;
        let state = view.state;
        let local_peer = self.peer_id();
        let mut clock = view.clock;
        for peer in &state.members {
            if *peer == local_peer {
                continue;
            }
            // Only evidence certified for this branch says what a peer holds.
            match storage.peer_ack(peer, &topic_id)? {
                Some(ack) if ack.genesis == Some(state.genesis) => {
                    clock = clock.intersect(&ack.clock)
                }
                _ => return Ok(ActorClock::new()),
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

/// Whether `error` is a failure of the store rather than of the data.
fn is_backend_failure(error: &Error) -> bool {
    #[cfg(feature = "fjall")]
    if matches!(error, Error::Fjall(_)) {
        return true;
    }
    matches!(
        error,
        Error::Storage(_) | Error::AdmissionConflict | Error::Encode(_) | Error::Decode(_)
    )
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
