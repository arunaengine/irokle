pub(crate) use std::collections::BTreeSet;
pub(crate) use std::sync::{Arc, Barrier};
pub(crate) use std::thread;

pub(crate) use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub(crate) use crate::{
    ActorClock, ActorId, Ed25519Signer, Error, Event, EventEnvelope, Irokle, MemoryStorage,
    NodeConfig, Op, OpBody, OpId, PeerId, ReplicationPolicy, Signer, Storage, TopicConfig,
    TopicControl, TopicGenesis, TopicId, TopicPayload, WriteConcern, actor_id_for, history, net,
    node, oplog, sync,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Note {
    pub(crate) text: String,
}

impl Event for Note {
    const TYPE_ID: &'static str = "test.note";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Other;

impl Event for Other {
    const TYPE_ID: &'static str = "test.other";
}

/// Durable corruption a test can apply to a real store, so a regression starts
/// from records that are actually inconsistent rather than from a wrapper that
/// lies on reads.
pub(crate) trait Corrupt: Storage {
    fn drop_op_record(&self, id: &OpId);
    fn drop_meta_record(&self, id: &OpId);
}

/// Which record halves a test erases. `Both` leaves the topic, actor and child
/// indexes pointing at an op with no records at all, the shape an admitted
/// descendant with a lost dependency has.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Damage {
    Op,
    Meta,
    Both,
}

pub(crate) fn damage_op<S: Corrupt>(storage: &S, id: &OpId, damage: Damage) {
    match damage {
        Damage::Op => storage.drop_op_record(id),
        Damage::Meta => storage.drop_meta_record(id),
        Damage::Both => {
            storage.drop_op_record(id);
            storage.drop_meta_record(id);
        }
    }
    assert!(!storage.dep_resolvable(id).unwrap());
}

impl Corrupt for MemoryStorage {
    fn drop_op_record(&self, id: &OpId) {
        MemoryStorage::drop_op_record(self, id);
    }
    fn drop_meta_record(&self, id: &OpId) {
        MemoryStorage::drop_meta_record(self, id);
    }
}

#[cfg(feature = "fjall")]
impl Corrupt for crate::storage::FjallStorage {
    fn drop_op_record(&self, id: &OpId) {
        crate::storage::FjallStorage::drop_op_record(self, id);
    }
    fn drop_meta_record(&self, id: &OpId) {
        crate::storage::FjallStorage::drop_meta_record(self, id);
    }
}

/// Storage wrapper that simulates the stale reads of a concurrent admission:
/// `get_op`/`actor_index` report "unknown" exactly once for ops in the
/// one-shot sets, so a duplicate slips past the batch dedup check and reaches
/// seq validation while the actor tip already covers it. Ops in
/// `mid_commit_ops` stay invisible to `get_op` permanently, modelling a
/// commit whose actor index/tip keys are visible before the op record. Writes
/// for a topic in `failed_writes` are rejected, standing in for a storage fault
/// that only affects one topic.
#[derive(Clone)]
pub(crate) struct StaleReadStorage {
    pub(crate) inner: MemoryStorage,
    pub(crate) hidden_ops: Arc<std::sync::Mutex<BTreeSet<OpId>>>,
    pub(crate) hidden_index: Arc<std::sync::Mutex<BTreeSet<OpId>>>,
    pub(crate) mid_commit_ops: Arc<std::sync::Mutex<BTreeSet<OpId>>>,
    pub(crate) failed_writes: Arc<std::sync::Mutex<BTreeSet<TopicId>>>,
}

impl StaleReadStorage {
    pub(crate) fn new(inner: MemoryStorage) -> Self {
        Self {
            inner,
            hidden_ops: Arc::default(),
            hidden_index: Arc::default(),
            mid_commit_ops: Arc::default(),
            failed_writes: Arc::default(),
        }
    }

    #[cfg(feature = "iroh")]
    pub(crate) fn fail_writes(&self, topic_id: TopicId) {
        self.failed_writes.lock().unwrap().insert(topic_id);
    }
}

impl Storage for StaleReadStorage {
    fn put_admitted_batch(&self, batch: crate::storage::AdmittedBatch) -> Result<(), Error> {
        if self.failed_writes.lock().unwrap().contains(&batch.topic_id) {
            return Err(Error::Storage("injected admission write failure".into()));
        }
        self.inner.put_admitted_batch(batch)
    }
    fn get_op(&self, id: &OpId) -> Result<Option<Op>, Error> {
        if self.mid_commit_ops.lock().unwrap().contains(id) {
            return Ok(None);
        }
        if self.hidden_ops.lock().unwrap().remove(id) {
            return Ok(None);
        }
        self.inner.get_op(id)
    }
    fn get_meta(&self, id: &OpId) -> Result<Option<crate::storage::OpMeta>, Error> {
        self.inner.get_meta(id)
    }
    fn list_ops(&self, topic_id: &TopicId) -> Result<Vec<Op>, Error> {
        self.inner.list_ops(topic_id)
    }
    fn list_op_ids(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>, Error> {
        self.inner.list_op_ids(topic_id)
    }
    fn heads(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>, Error> {
        self.inner.heads(topic_id)
    }
    fn children(&self, op_id: &OpId) -> Result<BTreeSet<OpId>, Error> {
        self.inner.children(op_id)
    }
    fn actor_tip(
        &self,
        topic_id: &TopicId,
        actor_id: &ActorId,
    ) -> Result<Option<(u64, OpId)>, Error> {
        self.inner.actor_tip(topic_id, actor_id)
    }
    fn actor_index(
        &self,
        topic_id: &TopicId,
        actor_id: &ActorId,
        seq: u64,
    ) -> Result<Option<OpId>, Error> {
        let existing = self.inner.actor_index(topic_id, actor_id, seq)?;
        if let Some(id) = existing
            && self.hidden_index.lock().unwrap().remove(&id)
        {
            return Ok(None);
        }
        Ok(existing)
    }
    fn actor_clock(&self, topic_id: &TopicId) -> Result<ActorClock, Error> {
        self.inner.actor_clock(topic_id)
    }
    fn topic_fingerprint(&self, topic_id: &TopicId) -> Result<[u8; 32], Error> {
        self.inner.topic_fingerprint(topic_id)
    }
    fn max_generation(&self, topic_id: &TopicId) -> Result<u64, Error> {
        self.inner.max_generation(topic_id)
    }
    fn topic_state(&self, topic_id: &TopicId) -> Result<Option<crate::storage::TopicState>, Error> {
        self.inner.topic_state(topic_id)
    }
    fn list_topics(&self) -> Result<Vec<crate::TopicInfo>, Error> {
        self.inner.list_topics()
    }
    fn put_pending_op(
        &self,
        source_peer: PeerId,
        op: Op,
        meta: crate::storage::OpMeta,
    ) -> Result<(), Error> {
        self.inner.put_pending_op(source_peer, op, meta)
    }
    fn pending_waiters(&self, dep_id: &OpId) -> Result<Vec<(PeerId, Op)>, Error> {
        self.inner.pending_waiters(dep_id)
    }
    fn ready_pending_ops(&self) -> Result<Vec<(PeerId, Op)>, Error> {
        self.inner.ready_pending_ops()
    }
    fn pending_missing_deps(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>, Error> {
        self.inner.pending_missing_deps(topic_id)
    }
    fn remove_pending_op(&self, op_id: &OpId) -> Result<(), Error> {
        self.inner.remove_pending_op(op_id)
    }
    fn peer_ack(
        &self,
        peer_id: &PeerId,
        topic_id: &TopicId,
    ) -> Result<Option<crate::storage::PeerAck>, Error> {
        self.inner.peer_ack(peer_id, topic_id)
    }
    fn peer_acks(&self, topic_id: &TopicId) -> Result<Vec<crate::storage::PeerAck>, Error> {
        self.inner.peer_acks(topic_id)
    }
    fn put_sync_obligation(&self, obligation: crate::storage::SyncObligation) -> Result<(), Error> {
        self.inner.put_sync_obligation(obligation)
    }
    fn all_sync_obligations(&self) -> Result<Vec<crate::storage::SyncObligation>, Error> {
        self.inner.all_sync_obligations()
    }
    fn apply_peer_ack(&self, ack: crate::storage::PeerAck) -> Result<usize, Error> {
        self.inner.apply_peer_ack(ack)
    }
    fn sync_obligations(
        &self,
        peer_id: &PeerId,
        topic_id: &TopicId,
    ) -> Result<Vec<crate::storage::SyncObligation>, Error> {
        self.inner.sync_obligations(peer_id, topic_id)
    }
    fn put_sync_status(&self, status: crate::storage::SyncPeerStatus) -> Result<(), Error> {
        self.inner.put_sync_status(status)
    }
    fn sync_statuses(
        &self,
        topic_id: &TopicId,
    ) -> Result<Vec<crate::storage::SyncPeerStatus>, Error> {
        self.inner.sync_statuses(topic_id)
    }
    fn clear_peer_sync_state(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<usize, Error> {
        self.inner.clear_peer_sync_state(peer_id, topic_id)
    }
    fn reset_topic(&self, topic_id: &TopicId) -> Result<usize, Error> {
        self.inner.reset_topic(topic_id)
    }
    fn reset_topic_and_admit(
        &self,
        topic_id: &TopicId,
        expected_topic_state: &crate::storage::TopicState,
        batch: crate::storage::AdmittedBatch,
    ) -> Result<usize, Error> {
        self.inner
            .reset_topic_and_admit(topic_id, expected_topic_state, batch)
    }
    fn purge_pending_waiters(&self, dep_id: &OpId) -> Result<usize, Error> {
        self.inner.purge_pending_waiters(dep_id)
    }
}

/// A genesis plus one event for `topic_id`, authored in `storage` by `seed`'s
/// signer with `peers` as the other initial members. Two sides built for the
/// same topic id fork it, which is what genesis tie-break resolution decides.
pub(crate) fn forked_side<S: Storage>(
    storage: S,
    topic_id: TopicId,
    seed: u8,
    peers: impl IntoIterator<Item = PeerId>,
    text: &str,
) -> (oplog::Oplog<S>, Ed25519Signer, Op, Op) {
    let signer = Ed25519Signer::from_bytes(&[seed; 32]);
    let log = oplog::Oplog::with_storage(storage);
    let actor = actor_id_for(topic_id, signer.peer_id());
    let genesis = TopicGenesis {
        event_type_id: Note::TYPE_ID.into(),
        initial_peers: peers.into_iter().collect(),
        replication_policy: ReplicationPolicy::default(),
    };
    let genesis_op = log
        .create_topic_genesis(topic_id, actor, genesis, &signer)
        .unwrap();
    let event_op = log
        .create_event_op(
            topic_id,
            actor,
            EventEnvelope::encode_event(&Note { text: text.into() }).unwrap(),
            &signer,
        )
        .unwrap();
    (log, signer, genesis_op, event_op)
}

/// A source node holding a three-op chain that `holder_peer` may sync with.
pub(crate) fn chain_source(seed: u8, holder_peer: PeerId) -> (Irokle, TopicId, Vec<Op>) {
    let source = node(seed);
    let topic = source
        .create_topic::<Note>(TopicConfig {
            initial_peers: [holder_peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    topic.publish(Note { text: "one".into() }).unwrap();
    topic.publish(Note { text: "two".into() }).unwrap();
    let ops = oplog::topological(source.storage(), &topic.id()).unwrap();
    (source, topic.id(), ops)
}

/// A three-op chain seeded into `storage` with the middle op really damaged.
pub(crate) fn holed_store<S: Corrupt>(
    storage: &S,
    seed: u8,
    damage: Damage,
) -> (Irokle, TopicId, Vec<Op>) {
    let holder_peer = Ed25519Signer::from_bytes(&[seed.wrapping_add(1); 32]).peer_id();
    let (source, topic_id, ops) = chain_source(seed, holder_peer);
    oplog::Oplog::with_storage(storage.clone())
        .receive_ops(ops.clone())
        .unwrap();
    damage_op(storage, &ops[1].id, damage);
    (source, topic_id, ops)
}

pub(crate) fn node(seed: u8) -> Irokle {
    Irokle::new(NodeConfig {
        signer: Ed25519Signer::from_bytes(&[seed; 32]),
        default_write_concern: WriteConcern::Local,
        ..NodeConfig::default()
    })
    .unwrap()
}
