// SPDX-License-Identifier: MIT OR Apache-2.0
//! High-level node, topic, publishing, and sync facade APIs.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

mod bootstrap;
mod builder;
#[cfg(feature = "iroh")]
mod network;
mod peers;
mod topic;

#[cfg(test)]
pub(crate) use peers::{PEER_FAILURE_LIMIT, select_sync_peers};
use peers::{PeerHealthStore, select_sync_targets};
pub use topic::{RawTopic, Topic};

use crate::ActorClock;
use crate::history::{DagQuery, HistoryCursor, HistoryOrder, ordered};
use crate::oplog::{Oplog, subset_entries_in, topological_subset_entries};
use crate::reducer::{EventRecord, HistoryEntry, HistoryPage};
use crate::storage::{AdmissionEffects, OpMeta, StagedTopic, SyncObligation, TopicState};
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

const SHARED_OVERLAP: usize = 2;

/// What receiving sync data did.
#[derive(Clone, Debug)]
pub enum ReceiveOutcome {
    /// The data reached the active topic; the signed ack speaks for it.
    Acked {
        ack: Box<SyncAck>,
        evictions: Vec<TopicEviction>,
    },
    /// The topic is not held here and the history staged from this source does
    /// not prove membership yet. This is no ack and certifies nothing.
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
    bootstraps: Arc<bootstrap::Bootstraps>,
    #[cfg(feature = "iroh")]
    net: Option<Arc<crate::net::IrohNet<S>>>,
}

pub struct IrokleBuilder<S = MemoryStorage> {
    storage: S,
    config: NodeConfig,
    signer_explicit: bool,
    write_concern_explicit: bool,
    #[cfg(feature = "iroh")]
    iroh: network::IrohSettings,
}

impl<S: Storage> Irokle<S> {
    pub fn with_storage(storage: S, config: NodeConfig) -> Result<Self> {
        let oplog = Oplog::with_storage(storage);
        oplog.reconcile_pending_ops()?;
        let sync = SyncEngine::new(oplog.clone(), config.signer.peer_id());
        let node = Self {
            oplog,
            sync,
            peer_whitelist: Arc::new(RwLock::new(config.peer_whitelist.clone())),
            peer_health: Arc::new(PeerHealthStore::default()),
            bootstraps: Arc::default(),
            config,
            #[cfg(feature = "iroh")]
            net: None,
        };
        // A bootstrap proven or begun before a restart becomes the topic now.
        node.resume_bootstraps()?;
        Ok(node)
    }

    pub fn storage(&self) -> &S {
        self.oplog.storage()
    }

    /// Select sync targets for `topic_id` from one replication-policy and peer-health view.
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

    /// Drop buffered ops that waited for their dependencies longer than
    /// [`crate::storage::MAX_PENDING_IDLE_MS`], with the ops that wait on them, and
    /// return how many went. Admitted history is untouched; the Iroh sweep calls this.
    pub fn expire_pending(&self) -> Result<usize> {
        let expired = self
            .storage()
            .expire_pending(now_millis()?, crate::storage::MAX_PENDING_IDLE_MS)?;
        if expired > 0 {
            tracing::info!(
                expired,
                "expired buffered ops whose dependencies never arrived"
            );
        }
        Ok(expired)
    }

    /// Discard unreachable ops of `topic_id` and rebuild it from the remaining heads.
    /// Replaced-genesis descendants stay unresolved; returned payloads belong to the embedder.
    pub fn quarantine_orphans(&self, topic_id: TopicId) -> Result<Option<TopicEviction>> {
        self.oplog.quarantine_orphans(&topic_id)
    }

    /// Return durable evictions awaiting acknowledgement. The transaction writes each record
    /// with discarded payloads, so restart recovery survives a lost in-memory delivery.
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

    /// Quarantine orphaned ops in every local topic and return committed evictions.
    /// Per-topic failures leave other topics running; only enumeration failure is global.
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

    /// Plans one bounded response page for `request` that fits `budget`; see
    /// [`crate::sync::SyncEngine::response_page`]. A transport repeats request and page until the
    /// page reports no more, as `tests/paging.rs` shows.
    pub fn response_page(
        &self,
        peer_id: PeerId,
        request: &SyncRequest,
        budget: crate::sync::PageBudget,
    ) -> Result<crate::sync::PlannedPage> {
        self.sync.response_page(peer_id, request, budget)
    }

    /// Serve with the authenticated peer's current branch or staging summary.
    pub fn response_with(
        &self,
        peer_id: PeerId,
        request: &crate::sync::SyncRequest,
        budget: crate::sync::PageBudget,
        summary: &SyncSummary,
    ) -> Result<crate::sync::PlannedPage> {
        self.sync.response_with(peer_id, request, budget, summary)
    }

    pub fn plan_sync_data(&self, peer_id: PeerId, remote: &SyncSummary) -> Result<SyncData> {
        self.sync.plan_data(peer_id, remote)
    }

    pub fn plan_sync_request(&self, peer_id: PeerId, remote: &SyncSummary) -> Result<SyncRequest> {
        self.sync.plan_request(peer_id, remote)
    }

    /// Continue one peer/topic/branch/session; reset knowledge when that scope changes.
    pub fn plan_request_with(
        &self,
        peer_id: PeerId,
        remote: &SyncSummary,
        knowledge: &crate::sync::RequestKnowledge,
    ) -> Result<SyncRequest> {
        self.sync.plan_request_with(peer_id, remote, knowledge)
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

    /// Alias for [`Self::receive_sync_data_from`] with an explicit name for callers that
    /// handle genesis tie-break evictions. The embedder re-emits discarded payloads under
    /// the winning genesis; irokle does not re-emit them.
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
        let promoted = match self.bootstrap_unknown(source_peer_id, &data, &verified)? {
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
        for topic_id in forwarded.borrow().iter() {
            self.recheck_topic(*topic_id, "forwarded replication wake failed");
        }
        for topic_id in forwarded.borrow().iter() {
            self.note_forwarded(source_peer_id, *topic_id);
        }
        let (mut ack, evictions) = match received {
            Ok(received) => received,
            Err(error) => {
                if let Error::ReceiveCommitted { ack, .. } = &error {
                    self.resync_committed(source_peer_id, ack.topic_id);
                }
                return Err(error);
            }
        };
        ack.accepted
            .extend(promoted.intersection(&verified).copied());
        if let Err(source) = ack.sign(&self.config.signer) {
            self.resync_committed(source_peer_id, ack.topic_id);
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
        self.schedule_resync(peer_id, topic_id);
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
        if matches!(concern, WriteConcern::AsyncReplication) && !self.network_attached() {
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

    /// Record how attempt `attempt` ended. Only its first completion, `first`, counts; a repeat
    /// returns the stored status. Complete and Advanced are successes, and Blocked, ReopenRequired
    /// and Failed are failures; a partial pull stays `Behind`. Peer health is left to the caller.
    #[cfg(any(feature = "iroh", test))]
    pub(crate) fn record_attempt_result(
        &self,
        peer_id: PeerId,
        topic_id: TopicId,
        attempt: (u64, u64),
        outcome: &crate::AttemptOutcome,
        first: bool,
    ) -> Result<SyncPeerStatus> {
        if !first {
            return self.storage().update_sync_status(
                &peer_id,
                &topic_id,
                &SyncStatusUpdate::default(),
            );
        }
        let attempt_ms = now_millis()?;
        let pending = self.storage().sync_obligation_count(&peer_id, &topic_id)?;
        let (state, error) = match outcome {
            crate::AttemptOutcome::Complete => (SyncPeerState::Healthy, None),
            crate::AttemptOutcome::Advanced => (SyncPeerState::Behind, None),
            crate::AttemptOutcome::Blocked(reason)
            | crate::AttemptOutcome::ReopenRequired(reason) => {
                (SyncPeerState::Behind, Some(reason.clone()))
            }
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
        self.topic_entries(topic_id, order)?
            .into_iter()
            .map(HistoryEntry::into_record)
            .collect()
    }

    pub(crate) fn topic_entries<E: Event>(
        &self,
        topic_id: TopicId,
        order: HistoryOrder,
    ) -> Result<Vec<HistoryEntry<E>>> {
        let storage = self.oplog.storage();
        let ids = storage.list_op_ids(&topic_id)?;
        let entries = topological_subset_entries(storage, &ids)?;
        if entries.len() != ids.len() || !self.oplog.history_whole(&topic_id)? {
            return Err(incomplete_history());
        }
        Ok(ordered(decode_entries(entries), order))
    }

    pub(crate) fn history_after_cursor<E: Event>(
        &self,
        topic_id: TopicId,
        cursor: &HistoryCursor,
        order: HistoryOrder,
    ) -> Result<Vec<EventRecord<E>>> {
        let records = self
            .history_page::<E>(topic_id, cursor, None)?
            .entries
            .into_iter()
            .map(HistoryEntry::into_record)
            .collect::<Result<Vec<_>>>()?;
        Ok(ordered(records, order))
    }

    /// Events after `cursor`, at most `limit` ops, from one snapshot. Only the
    /// actor ranges past the cursor are read, so the work follows the unread ops,
    /// not the whole history. The returned cursor covers exactly the page.
    pub(crate) fn history_page<E: Event>(
        &self,
        topic_id: TopicId,
        cursor: &HistoryCursor,
        limit: Option<usize>,
    ) -> Result<HistoryPage<E>> {
        if !self.oplog.history_whole(&topic_id)? {
            return Err(incomplete_history());
        }
        let limit = limit.unwrap_or(usize::MAX).max(1);
        let entries = self.oplog.storage().read_snapshot(|read| {
            let view = read
                .topic_view(&topic_id, None)?
                .ok_or(Error::TopicNotFound)?;
            if view.state.genesis != cursor.genesis {
                return Err(Error::StaleIncarnation);
            }
            let mut candidates = Vec::new();
            for (actor, seq) in view.clock.iter() {
                let after = cursor.clock.get(actor);
                if *seq <= after {
                    continue;
                }
                for (_, id) in read.actor_range(&topic_id, actor, after, limit)? {
                    let position = read.get_position(&id)?.ok_or_else(incomplete_history)?;
                    candidates.push((position.generation, id, position.deps));
                }
            }
            // An op joins once every dependency is covered by the cursor or joined
            // before it; dependencies have smaller generations, so they come first.
            candidates.sort_by_key(|(generation, id, _)| (*generation, *id));
            let mut page = BTreeSet::new();
            for (_, id, deps) in candidates {
                let mut joins = true;
                for dep in &deps {
                    if page.contains(dep) {
                        continue;
                    }
                    let header = read.get_header(dep)?.ok_or_else(incomplete_history)?;
                    if cursor.clock.get(&header.actor_id) < header.actor_seq {
                        joins = false;
                        break;
                    }
                }
                if joins {
                    page.insert(id);
                }
            }
            let mut entries = subset_entries_in(read, &page)?;
            if entries.len() != page.len() {
                return Err(incomplete_history());
            }
            entries.truncate(limit);
            Ok(entries)
        })?;
        // A topological prefix holds each actor's ops contiguously, so the clock
        // of its last ops covers exactly the page.
        let mut clock = cursor.clock.clone();
        for (_, meta) in &entries {
            clock.observe(meta.actor_id, meta.actor_seq);
        }
        Ok(HistoryPage {
            entries: decode_entries(entries),
            cursor: HistoryCursor {
                genesis: cursor.genesis,
                clock,
            },
        })
    }

    pub(crate) fn topic_dag(&self, topic_id: TopicId, query: DagQuery<OpId>) -> Result<Vec<Op>> {
        topic::dag_ops(self.oplog.storage(), topic_id, query)
    }

    pub(crate) fn topic_heads(&self, topic_id: TopicId) -> Result<BTreeSet<OpId>> {
        self.oplog.storage().heads(&topic_id)
    }

    pub(crate) fn topic_history_cursor(&self, topic_id: TopicId) -> Result<HistoryCursor> {
        let view = self
            .oplog
            .storage()
            .topic_view(&topic_id, None)?
            .ok_or(Error::TopicNotFound)?;
        Ok(HistoryCursor {
            genesis: view.state.genesis,
            clock: view.clock,
        })
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

/// Without Iroh no transport is attached, so the network hooks do nothing.
#[cfg(not(feature = "iroh"))]
impl<S: Storage> Irokle<S> {
    fn wake_async_replication(
        &self,
        _topic_id: TopicId,
        _op_id: OpId,
        _write_concern: &WriteConcern,
        _wake_failed_message: &'static str,
    ) {
    }

    fn recheck_topic(&self, _topic_id: TopicId, _message: &'static str) {}

    fn resync_committed(&self, _peer_id: PeerId, _topic_id: TopicId) {}

    fn schedule_resync(&self, _peer_id: PeerId, _topic_id: TopicId) {}

    fn network_attached(&self) -> bool {
        false
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
fn now_millis() -> Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| Error::Storage(format!("system time before unix epoch: {err}")))?
        .as_millis();
    millis
        .try_into()
        .map_err(|_| Error::Storage("system time does not fit in u64 milliseconds".into()))
}

fn incomplete_history() -> Error {
    Error::Storage("incomplete topic history".into())
}

/// The events of `entries` in their order, each decoded on its own.
fn decode_entries<E: Event>(entries: Vec<(Op, crate::storage::OpMeta)>) -> Vec<HistoryEntry<E>> {
    let mut decoded = Vec::new();
    for (op, meta) in entries {
        let crate::TopicPayload::Event(envelope) = &op.signed.body.payload else {
            continue;
        };
        let meta = crate::reducer::OpMeta {
            op_id: op.id,
            actor_id: meta.actor_id,
            actor_seq: meta.actor_seq,
            observed_clock: meta.observed_clock,
        };
        decoded.push(match envelope.decode_event::<E>() {
            Ok(event) => HistoryEntry::Event(EventRecord { event, meta }),
            Err(error) => HistoryEntry::Undecodable { meta, error },
        });
    }
    decoded
}
