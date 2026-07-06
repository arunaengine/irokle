// SPDX-License-Identifier: MIT OR Apache-2.0
//! Operation-log admission, DAG validation, and topic-state materialization.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use crate::storage::{
    AdmissionEffects, AdmittedBatch, MAX_PENDING_MISSING_DEPS, MemoryStorage, OpMeta, Storage,
    TopicState,
};
use crate::{
    ActorId, Error, EventEnvelope, Op, OpBody, OpId, PeerId, Result, SignedOp, Signer,
    TopicControl, TopicGenesis, TopicId, TopicPayload, actor_id_for,
};

mod helpers;
mod topology;

use helpers::{
    apply_control_to_state, checked_next, ensure_event_type, heads_after, is_local_admission_race,
    is_semantic_rejection, materialize_topic_state, next_actor_position, pending_meta_for,
};
use topology::topological_ops;
pub use topology::{topological, topological_subset};

const MAX_ADMISSION_RETRIES: usize = 64;

enum OpAdmission {
    Admit,
    Duplicate,
}

/// A single op removed from the losing side of a genesis collision, carrying
/// enough for the application to re-emit it under the winning genesis.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvictedOp {
    pub op_id: OpId,
    pub actor_id: ActorId,
    pub author: PeerId,
    pub actor_seq: u64,
    pub payload: TopicPayload,
}

/// Reports that a topic's local chain was discarded in favour of a foreign
/// genesis with a smaller op id. `evicted` holds the reset chain's non-genesis
/// payloads ordered by `(actor_id, actor_seq)`; re-emission is the embedder's
/// responsibility.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicEviction {
    pub topic_id: TopicId,
    pub losing_genesis: OpId,
    pub winning_genesis: OpId,
    pub evicted: Vec<EvictedOp>,
}

/// Outcome of an admission pass: the accepted op ids plus any topic evictions
/// produced by genesis tie-break resolution.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Admitted {
    pub accepted: BTreeSet<OpId>,
    pub evictions: Vec<TopicEviction>,
}

fn is_structural_genesis(op: &Op) -> bool {
    let body = &op.signed.body;
    matches!(body.payload, TopicPayload::Genesis(_))
        && body.actor_seq == 1
        && body.actor_prev.is_none()
        && body.deps.is_empty()
}

#[derive(Clone)]
pub struct Oplog<S = MemoryStorage> {
    storage: S,
    // Serializes genesis tie-break resolution so two concurrent admissions
    // cannot both reset the same topic; shared across clones via `Arc`.
    resolution_lock: Arc<Mutex<()>>,
}

impl Default for Oplog<MemoryStorage> {
    fn default() -> Self {
        Self::new()
    }
}

impl Oplog<MemoryStorage> {
    pub fn new() -> Self {
        Self {
            storage: MemoryStorage::new(),
            resolution_lock: Arc::new(Mutex::new(())),
        }
    }
}

impl<S: Storage> Oplog<S> {
    pub fn with_storage(storage: S) -> Self {
        Self {
            storage,
            resolution_lock: Arc::new(Mutex::new(())),
        }
    }
    pub fn storage(&self) -> &S {
        &self.storage
    }

    pub fn create_topic_genesis(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        genesis: TopicGenesis,
        signer: &impl Signer,
    ) -> Result<Op> {
        self.create_topic_genesis_with_effects(topic_id, actor_id, genesis, signer, |_, _, _| {
            Ok(AdmissionEffects::default())
        })
    }

    pub(crate) fn create_topic_genesis_with_effects<F>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        genesis: TopicGenesis,
        signer: &impl Signer,
        effects: F,
    ) -> Result<Op>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        let mut peers = genesis.initial_peers.clone();
        peers.insert(signer.peer_id());
        let genesis = TopicGenesis {
            initial_peers: peers,
            ..genesis
        };
        self.create_and_admit_local_op_with_effects(
            topic_id,
            actor_id,
            TopicPayload::Genesis(genesis),
            signer,
            effects,
        )
    }

    /// Create a topic genesis op plus its first event op and admit both in a
    /// single storage transaction. The event op chains off the genesis
    /// (actor_seq 2, actor_prev/deps = genesis op). Returns `(genesis, event)`.
    /// Fails with [`Error::InvalidGenesis`] if the topic already exists, same
    /// as [`Self::create_topic_genesis`].
    pub fn create_topic_genesis_with_event(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        genesis: TopicGenesis,
        event: EventEnvelope,
        signer: &impl Signer,
    ) -> Result<(Op, Op)> {
        self.create_topic_genesis_with_event_with_effects(
            topic_id,
            actor_id,
            genesis,
            event,
            signer,
            |_, _, _| Ok(AdmissionEffects::default()),
        )
    }

    pub(crate) fn create_topic_genesis_with_event_with_effects<F>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        genesis: TopicGenesis,
        event: EventEnvelope,
        signer: &impl Signer,
        effects: F,
    ) -> Result<(Op, Op)>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        let mut peers = genesis.initial_peers.clone();
        peers.insert(signer.peer_id());
        let genesis = TopicGenesis {
            initial_peers: peers,
            ..genesis
        };
        for _ in 0..MAX_ADMISSION_RETRIES {
            match self.try_create_and_admit_genesis_with_event(
                topic_id,
                actor_id,
                genesis.clone(),
                event.clone(),
                signer,
                &effects,
            ) {
                Err(err) if is_local_admission_race(&err) => continue,
                result => return result,
            }
        }
        Err(Error::AdmissionConflict)
    }

    pub fn create_event_op(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        event: EventEnvelope,
        signer: &impl Signer,
    ) -> Result<Op> {
        self.create_event_op_with_effects(topic_id, actor_id, event, signer, |_, _, _| {
            Ok(AdmissionEffects::default())
        })
    }

    pub(crate) fn create_event_op_with_effects<F>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        event: EventEnvelope,
        signer: &impl Signer,
        effects: F,
    ) -> Result<Op>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        self.create_and_admit_local_op_with_effects(
            topic_id,
            actor_id,
            TopicPayload::Event(event),
            signer,
            effects,
        )
    }

    pub fn create_control_op(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        control: TopicControl,
        signer: &impl Signer,
    ) -> Result<Op> {
        self.create_control_op_with_effects(topic_id, actor_id, control, signer, |_, _, _| {
            Ok(AdmissionEffects::default())
        })
    }

    pub(crate) fn create_control_op_with_effects<F>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        control: TopicControl,
        signer: &impl Signer,
        effects: F,
    ) -> Result<Op>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        self.create_and_admit_local_op_with_effects(
            topic_id,
            actor_id,
            TopicPayload::Control(control),
            signer,
            effects,
        )
    }

    pub fn receive_op(&self, op: Op) -> Result<()> {
        self.receive_ops(vec![op]).map(|_| ())
    }

    pub fn receive_ops(&self, ops: Vec<Op>) -> Result<BTreeSet<crate::OpId>> {
        self.receive_ops_from_peer(None, ops)
    }

    pub fn receive_ops_from_peer(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
    ) -> Result<BTreeSet<crate::OpId>> {
        Ok(self
            .receive_ops_admission(source_peer, ops, &BTreeSet::new())?
            .accepted)
    }

    /// Like [`Self::receive_ops_from_peer`], but also returns any topic
    /// evictions produced by genesis tie-break resolution.
    pub fn receive_ops_from_peer_evicting(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
    ) -> Result<Admitted> {
        self.receive_ops_admission(source_peer, ops, &BTreeSet::new())
    }

    /// Like [`Self::receive_ops_from_peer_evicting`], but skips signature
    /// verification for ops whose id is in `verified`. The caller must have run
    /// [`Op::validate`] on those exact ops; op ids are content-addressed over
    /// the signed envelope, so a verified id proves the signature.
    pub(crate) fn receive_ops_from_peer_preverified(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
        verified: &BTreeSet<crate::OpId>,
    ) -> Result<Admitted> {
        self.receive_ops_admission(source_peer, ops, verified)
    }

    pub fn reconcile_pending_ops(&self) -> Result<BTreeSet<crate::OpId>> {
        Ok(self
            .receive_ops_admission(None, Vec::new(), &BTreeSet::new())?
            .accepted)
    }

    pub fn receive_signed_op(&self, signed: SignedOp) -> Result<Op> {
        let op = Op::new(signed)?;
        self.receive_op(op.clone())?;
        Ok(op)
    }

    fn receive_ops_admission(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
        verified: &BTreeSet<crate::OpId>,
    ) -> Result<Admitted> {
        let mut accepted = BTreeSet::new();
        let mut evictions = Vec::new();
        let mut queue = VecDeque::new();
        let mut queued_pending = BTreeSet::new();
        if !ops.is_empty() {
            queue.push_back((source_peer, ops, false));
        }
        self.enqueue_ready_pending_ops(&mut queue, &mut queued_pending)?;

        while let Some((batch_source_peer, ops, from_pending)) = queue.pop_front() {
            let pending_op_ids = if from_pending {
                ops.iter().map(|op| op.id).collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            for op_id in &pending_op_ids {
                queued_pending.remove(op_id);
            }
            // Pending ops re-queued from storage are not in `verified`; they
            // get re-verified during admission like before.
            let (batch_accepted, batch_eviction) =
                match self.admit_ops_batch_retry(batch_source_peer, ops, verified) {
                    Ok(outcome) => outcome,
                    Err(err) if from_pending && is_semantic_rejection(&err) => {
                        for op_id in pending_op_ids {
                            self.storage.remove_pending_op(&op_id)?;
                        }
                        continue;
                    }
                    Err(err) => return Err(err),
                };
            if let Some(eviction) = batch_eviction {
                evictions.push(eviction);
            }
            for op_id in &batch_accepted {
                self.enqueue_pending_ops(
                    &mut queue,
                    &mut queued_pending,
                    self.storage.pending_waiters(op_id)?,
                );
            }
            self.enqueue_ready_pending_ops(&mut queue, &mut queued_pending)?;
            accepted.extend(batch_accepted);
        }

        Ok(Admitted {
            accepted,
            evictions,
        })
    }

    fn enqueue_ready_pending_ops(
        &self,
        queue: &mut VecDeque<(Option<PeerId>, Vec<Op>, bool)>,
        queued_pending: &mut BTreeSet<crate::OpId>,
    ) -> Result<()> {
        let pending = self.storage.ready_pending_ops()?;
        self.enqueue_pending_ops(queue, queued_pending, pending);
        Ok(())
    }

    fn enqueue_pending_ops(
        &self,
        queue: &mut VecDeque<(Option<PeerId>, Vec<Op>, bool)>,
        queued_pending: &mut BTreeSet<crate::OpId>,
        pending: Vec<(PeerId, Op)>,
    ) {
        for (source_peer, op) in pending {
            if queued_pending.insert(op.id) {
                queue.push_back((Some(source_peer), vec![op], true));
            }
        }
    }

    fn admit_ops_batch_retry(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
        verified: &BTreeSet<crate::OpId>,
    ) -> Result<(BTreeSet<crate::OpId>, Option<TopicEviction>)> {
        // A batch bearing a genesis may collide with an existing topic. Hold
        // the resolution lock across both the tie-break and the admission that
        // consumes it, so a concurrent admission cannot reset the topic in
        // between. Non-genesis batches (the common path) never lock.
        let _guard = if ops.iter().any(is_structural_genesis) {
            Some(
                self.resolution_lock
                    .lock()
                    .map_err(|_| Error::Storage("resolution lock poisoned".into()))?,
            )
        } else {
            None
        };
        let (ops, eviction) = if _guard.is_some() {
            self.resolve_genesis_collision(ops, verified)?
        } else {
            (ops, None)
        };
        for _ in 0..MAX_ADMISSION_RETRIES {
            match self.admit_ops_batch(source_peer, ops.clone(), verified) {
                Err(Error::AdmissionConflict) => continue,
                Ok(accepted) => return Ok((accepted, eviction)),
                Err(err) => return Err(err),
            }
        }
        Err(Error::AdmissionConflict)
    }

    /// Resolve a genesis tie-break for a batch that carries a structurally
    /// valid genesis. The caller holds `resolution_lock`, so the reads and the
    /// reset here are single-flight per oplog. Returns the ops to admit
    /// (unchanged when the incoming genesis wins or there is no collision; with
    /// a losing foreign genesis filtered out when the local one wins) plus any
    /// eviction produced when the local topic is reset.
    fn resolve_genesis_collision(
        &self,
        ops: Vec<Op>,
        verified: &BTreeSet<crate::OpId>,
    ) -> Result<(Vec<Op>, Option<TopicEviction>)> {
        let Some(genesis) = ops.iter().find(|op| is_structural_genesis(op)).cloned() else {
            return Ok((ops, None));
        };
        let topic_id = genesis.signed.body.topic_id;
        let Some(state) = self.storage.topic_state(&topic_id)? else {
            // Fresh topic: normal admission accepts the genesis.
            return Ok((ops, None));
        };
        if state.genesis == genesis.id {
            // Same node re-sending its genesis: normal dedup handles it.
            return Ok((ops, None));
        }
        // Only a signature-valid genesis may win the tie-break.
        if !verified.contains(&genesis.id) {
            genesis.validate()?;
        }
        // Op ids are content-addressed 32-byte blake3 digests; the derived
        // `Ord` is lexicographic over those bytes, so both nodes pick the same
        // winner with no coordination.
        if genesis.id < state.genesis {
            let eviction = self.evict_and_reset_topic(topic_id, &state, genesis.id)?;
            tracing::warn!(
                %topic_id,
                losing_genesis = %state.genesis,
                winning_genesis = %genesis.id,
                evicted = eviction.evicted.len(),
                "genesis collision resolved: reset local topic for smaller winning genesis"
            );
            Ok((ops, Some(eviction)))
        } else {
            tracing::warn!(
                %topic_id,
                local_genesis = %state.genesis,
                foreign_genesis = %genesis.id,
                evicted = 0,
                "genesis collision resolved: kept local genesis, rejected larger foreign genesis"
            );
            let filtered = ops.into_iter().filter(|op| op.id != genesis.id).collect();
            Ok((filtered, None))
        }
    }

    /// Extract the local topic chain's non-genesis payloads (ordered by actor,
    /// then sequence) and then wipe every local record for the topic so the
    /// winning genesis can be admitted from a clean slate.
    fn evict_and_reset_topic(
        &self,
        topic_id: TopicId,
        local_state: &TopicState,
        winning_genesis: OpId,
    ) -> Result<TopicEviction> {
        let mut metas = Vec::new();
        for op_id in self.storage.list_op_ids(&topic_id)? {
            if let Some(meta) = self.storage.get_meta(&op_id)? {
                metas.push(meta);
            }
        }
        metas.sort_by_key(|meta| (meta.actor_id, meta.actor_seq));
        let mut evicted = Vec::new();
        for meta in metas {
            if meta.id == local_state.genesis {
                continue;
            }
            let op = self.storage.get_op(&meta.id)?.ok_or_else(|| {
                Error::Storage(format!("missing op during eviction: {}", meta.id))
            })?;
            evicted.push(EvictedOp {
                op_id: meta.id,
                actor_id: meta.actor_id,
                author: meta.author,
                actor_seq: meta.actor_seq,
                payload: op.signed.body.payload.clone(),
            });
        }
        self.storage.reset_topic(&topic_id)?;
        Ok(TopicEviction {
            topic_id,
            losing_genesis: local_state.genesis,
            winning_genesis,
            evicted,
        })
    }

    fn admit_ops_batch(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
        verified: &BTreeSet<crate::OpId>,
    ) -> Result<BTreeSet<crate::OpId>> {
        let ops = topological_ops(ops)?;
        let mut accepted = BTreeSet::new();
        let Some(topic_id) = ops.first().map(|op| op.signed.body.topic_id) else {
            return Ok(accepted);
        };
        if ops.iter().any(|op| op.signed.body.topic_id != topic_id) {
            return Err(Error::TopicMismatch);
        }

        let expected_heads = self.storage.heads(&topic_id)?;
        let expected_state = self.storage.topic_state(&topic_id)?;
        let mut heads = expected_heads.clone();
        let mut state = expected_state.clone();
        let mut topic_state_changed = false;
        let mut overlay_ops = BTreeMap::new();
        let mut overlay_meta = BTreeMap::new();
        let mut overlay_tips = BTreeMap::new();
        let mut overlay_index = BTreeMap::new();
        let mut entries = Vec::new();
        let mut pending = Vec::new();

        for op in ops {
            if !verified.contains(&op.id) {
                op.validate()?;
            }
            if self.storage.get_op(&op.id)?.is_some() {
                continue;
            }

            let missing_deps = self.missing_deps_projected(&op, &overlay_ops)?;
            if !missing_deps.is_empty() {
                match self.validate_pending_op_projected(
                    &op,
                    &missing_deps,
                    &overlay_meta,
                    &overlay_tips,
                    &overlay_index,
                    state.as_ref(),
                )? {
                    OpAdmission::Duplicate => {}
                    OpAdmission::Admit => pending.push((op, missing_deps)),
                }
                continue;
            }

            if let OpAdmission::Duplicate = self.validate_op_projected(
                &op,
                &overlay_ops,
                &overlay_meta,
                &overlay_tips,
                &overlay_index,
                &heads,
                state.as_ref(),
            )? {
                continue;
            }
            let meta = self.meta_for_projected(&op, &overlay_meta)?;
            heads = heads_after(&heads, &op);
            match &op.signed.body.payload {
                TopicPayload::Genesis(genesis) => {
                    state = Some(TopicState {
                        topic_id,
                        event_type_id: genesis.event_type_id.clone(),
                        genesis: op.id,
                        heads: heads.clone(),
                        members: genesis.initial_peers.clone(),
                        replication_policy: genesis.replication_policy.clone(),
                        membership_controls: BTreeMap::new(),
                        replication_policy_control: None,
                    });
                    topic_state_changed = true;
                }
                TopicPayload::Event(_) => {
                    if let Some(state) = state.as_mut() {
                        state.heads = heads.clone();
                    }
                }
                TopicPayload::Control(control) => {
                    let state = state.as_mut().ok_or(Error::TopicNotFound)?;
                    state.heads = heads.clone();
                    apply_control_to_state(state, &op, control);
                    topic_state_changed = true;
                }
            }

            overlay_index.insert((topic_id, meta.actor_id, meta.actor_seq), op.id);
            overlay_tips.insert((topic_id, meta.actor_id), (meta.actor_seq, op.id));
            overlay_meta.insert(op.id, meta.clone());
            overlay_ops.insert(op.id, op.clone());
            accepted.insert(op.id);
            entries.push((op, meta));
        }

        for (op, missing_deps) in pending {
            let source_peer = source_peer.unwrap_or(op.signed.body.author);
            self.storage.put_pending_op(
                source_peer,
                op.clone(),
                pending_meta_for(&op, missing_deps),
            )?;
        }

        if !entries.is_empty() {
            self.storage.put_admitted_batch(AdmittedBatch {
                topic_id,
                expected_heads,
                expected_topic_state: expected_state,
                entries,
                heads,
                topic_state: topic_state_changed.then(|| state.clone()).flatten(),
                effects: AdmissionEffects::default(),
            })?;
        }

        Ok(accepted)
    }

    pub fn observed_clock(&self, topic_id: &TopicId) -> Result<crate::ActorClock> {
        self.storage.actor_clock(topic_id)
    }

    fn create_and_admit_local_op_with_effects<F>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        payload: TopicPayload,
        signer: &impl Signer,
        effects: F,
    ) -> Result<Op>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        for _ in 0..MAX_ADMISSION_RETRIES {
            match self.try_create_and_admit_local_op(
                topic_id,
                actor_id,
                payload.clone(),
                signer,
                &effects,
            ) {
                Err(err) if is_local_admission_race(&err) => continue,
                result => return result,
            }
        }
        Err(Error::AdmissionConflict)
    }

    fn try_create_and_admit_local_op<F>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        payload: TopicPayload,
        signer: &impl Signer,
        effects: &F,
    ) -> Result<Op>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        if !matches!(payload, TopicPayload::Genesis(_)) {
            self.ensure_member(&topic_id, signer.peer_id())?;
        }
        let expected_heads = self.storage.heads(&topic_id)?;
        let expected_state = self.storage.topic_state(&topic_id)?;
        let op = self.next_local_op(topic_id, actor_id, expected_heads.clone(), payload, signer)?;
        op.validate()?;
        self.validate_op(&op)?;
        let meta = self.meta_for(&op)?;
        self.commit_admission(op.clone(), meta, expected_heads, expected_state, effects)?;
        Ok(op)
    }

    fn try_create_and_admit_genesis_with_event<F>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        genesis: TopicGenesis,
        event: EventEnvelope,
        signer: &impl Signer,
        effects: &F,
    ) -> Result<(Op, Op)>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        let expected_heads = self.storage.heads(&topic_id)?;
        let expected_state = self.storage.topic_state(&topic_id)?;
        let genesis_op = self.next_local_op(
            topic_id,
            actor_id,
            expected_heads.clone(),
            TopicPayload::Genesis(genesis),
            signer,
        )?;
        genesis_op.validate()?;
        self.validate_op(&genesis_op)?;
        let genesis_meta = self.meta_for(&genesis_op)?;

        let event_op = Op::sign(
            OpBody {
                topic_id,
                author: signer.peer_id(),
                actor_id,
                actor_seq: checked_next(genesis_meta.actor_seq)?,
                actor_prev: Some(genesis_op.id),
                deps: [genesis_op.id].into(),
                generation: checked_next(genesis_meta.generation)?,
                payload: TopicPayload::Event(event),
            },
            signer,
        )?;
        event_op.validate()?;

        let genesis_heads = heads_after(&expected_heads, &genesis_op);
        let mut state = self
            .topic_state_after(&genesis_op, genesis_heads.clone(), expected_state.clone())?
            .ok_or(Error::TopicNotFound)?;
        let overlay_ops = BTreeMap::from([(genesis_op.id, genesis_op.clone())]);
        let overlay_meta = BTreeMap::from([(genesis_op.id, genesis_meta.clone())]);
        let overlay_tips = BTreeMap::from([(
            (topic_id, actor_id),
            (genesis_meta.actor_seq, genesis_op.id),
        )]);
        let overlay_index =
            BTreeMap::from([((topic_id, actor_id, genesis_meta.actor_seq), genesis_op.id)]);
        if let OpAdmission::Duplicate = self.validate_op_projected(
            &event_op,
            &overlay_ops,
            &overlay_meta,
            &overlay_tips,
            &overlay_index,
            &genesis_heads,
            Some(&state),
        )? {
            return Err(Error::AdmissionConflict);
        }
        let event_meta = self.meta_for_projected(&event_op, &overlay_meta)?;

        let mut admission_effects = effects(&genesis_op, &genesis_meta, &state)?;
        let heads = heads_after(&genesis_heads, &event_op);
        state.heads = heads.clone();
        admission_effects
            .sync_obligations
            .extend(effects(&event_op, &event_meta, &state)?.sync_obligations);
        self.storage.put_admitted_batch(AdmittedBatch {
            topic_id,
            expected_heads,
            expected_topic_state: expected_state,
            entries: vec![
                (genesis_op.clone(), genesis_meta),
                (event_op.clone(), event_meta),
            ],
            heads,
            topic_state: Some(state),
            effects: admission_effects,
        })?;
        Ok((genesis_op, event_op))
    }

    fn next_local_op(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        mut deps: BTreeSet<crate::OpId>,
        payload: TopicPayload,
        signer: &impl Signer,
    ) -> Result<Op> {
        let tip = self.storage.actor_tip(&topic_id, &actor_id)?;
        let (actor_seq, actor_prev) = match tip {
            Some((seq, id)) => (
                seq.checked_add(1)
                    .ok_or_else(|| Error::Storage("actor sequence overflow".into()))?,
                Some(id),
            ),
            None => (1, None),
        };
        if let Some(prev) = actor_prev {
            deps.insert(prev);
        }
        let generation = if deps.is_empty() {
            0
        } else {
            self.storage
                .max_generation(&topic_id)?
                .checked_add(1)
                .ok_or(Error::InvalidOpId)?
        };
        Op::sign(
            OpBody {
                topic_id,
                author: signer.peer_id(),
                actor_id,
                actor_seq,
                actor_prev,
                deps,
                generation,
                payload,
            },
            signer,
        )
    }

    fn validate_op(&self, op: &Op) -> Result<()> {
        let body = &op.signed.body;
        if body.actor_id != actor_id_for(body.topic_id, body.author) {
            return Err(Error::ActorAuthorMismatch);
        }
        for dep in &body.deps {
            if self.storage.get_op(dep)?.is_none() {
                return Err(Error::MissingDependency(*dep));
            }
        }
        match &body.payload {
            TopicPayload::Genesis(_) => {
                if body.actor_seq != 1
                    || body.actor_prev.is_some()
                    || !body.deps.is_empty()
                    || self.storage.topic_state(&body.topic_id)?.is_some()
                {
                    return Err(Error::InvalidGenesis);
                }
            }
            TopicPayload::Event(envelope) => {
                let current = self
                    .storage
                    .topic_state(&body.topic_id)?
                    .ok_or(Error::TopicNotFound)?;
                ensure_event_type(&current.event_type_id, &envelope.type_id)?;
                let author_is_member = if body.deps == current.heads {
                    current.members.contains(&body.author)
                } else {
                    self.topic_state_for_deps(&body.topic_id, &body.deps)?
                        .members
                        .contains(&body.author)
                };
                if !author_is_member {
                    return Err(Error::NotTopicMember);
                }
            }
            TopicPayload::Control(_) => {
                let current = self
                    .storage
                    .topic_state(&body.topic_id)?
                    .ok_or(Error::TopicNotFound)?;
                let author_is_member = if body.deps == current.heads {
                    current.members.contains(&body.author)
                } else {
                    self.topic_state_for_deps(&body.topic_id, &body.deps)?
                        .members
                        .contains(&body.author)
                };
                if !author_is_member {
                    return Err(Error::NotTopicMember);
                }
            }
        }
        if let Some(existing) =
            self.storage
                .actor_index(&body.topic_id, &body.actor_id, body.actor_seq)?
            && existing != op.id
        {
            return Err(Error::ActorFork);
        }
        let expected = self.storage.actor_tip(&body.topic_id, &body.actor_id)?;
        let (expected_seq, expected_prev) = next_actor_position(expected)?;
        if body.actor_seq != expected_seq {
            return Err(Error::ActorSeqGap {
                expected: expected_seq,
                actual: body.actor_seq,
            });
        }
        if body.actor_prev != expected_prev {
            return Err(Error::ActorPrevMismatch);
        }
        let mut generation = 0;
        for id in &body.deps {
            let meta = self
                .storage
                .get_meta(id)?
                .ok_or(Error::MissingDependency(*id))?;
            generation = generation.max(checked_next(meta.generation)?);
        }
        if body.generation != generation {
            return Err(Error::InvalidOpId);
        }
        Ok(())
    }

    fn meta_for(&self, op: &Op) -> Result<OpMeta> {
        let body = &op.signed.body;
        let observed_clock = self.observed_clock_for_deps(&body.topic_id, &body.deps)?;
        Ok(OpMeta {
            id: op.id,
            topic_id: body.topic_id,
            author: body.author,
            actor_id: body.actor_id,
            actor_seq: body.actor_seq,
            actor_prev: body.actor_prev,
            deps: body.deps.clone(),
            generation: body.generation,
            observed_clock,
            ready: true,
            missing_deps: BTreeSet::new(),
        })
    }

    fn meta_for_projected(
        &self,
        op: &Op,
        overlay_meta: &BTreeMap<crate::OpId, OpMeta>,
    ) -> Result<OpMeta> {
        let body = &op.signed.body;
        let mut observed_clock = crate::ActorClock::new();
        for dep in &body.deps {
            let meta = self.meta_projected(dep, overlay_meta)?;
            if meta.topic_id != body.topic_id {
                return Err(Error::TopicMismatch);
            }
            observed_clock.merge(&meta.observed_clock);
            observed_clock.observe(meta.actor_id, meta.actor_seq);
        }
        Ok(OpMeta {
            id: op.id,
            topic_id: body.topic_id,
            author: body.author,
            actor_id: body.actor_id,
            actor_seq: body.actor_seq,
            actor_prev: body.actor_prev,
            deps: body.deps.clone(),
            generation: body.generation,
            observed_clock,
            ready: true,
            missing_deps: BTreeSet::new(),
        })
    }

    fn meta_projected(
        &self,
        id: &crate::OpId,
        overlay_meta: &BTreeMap<crate::OpId, OpMeta>,
    ) -> Result<OpMeta> {
        overlay_meta.get(id).cloned().map(Ok).unwrap_or_else(|| {
            self.storage
                .get_meta(id)?
                .ok_or(Error::MissingDependency(*id))
        })
    }

    fn op_projected(
        &self,
        id: &crate::OpId,
        overlay_ops: &BTreeMap<crate::OpId, Op>,
    ) -> Result<Op> {
        overlay_ops.get(id).cloned().map(Ok).unwrap_or_else(|| {
            self.storage
                .get_op(id)?
                .ok_or(Error::MissingDependency(*id))
        })
    }

    fn missing_deps_projected(
        &self,
        op: &Op,
        overlay_ops: &BTreeMap<crate::OpId, Op>,
    ) -> Result<BTreeSet<crate::OpId>> {
        let mut missing = BTreeSet::new();
        for dep in &op.signed.body.deps {
            if !overlay_ops.contains_key(dep) && self.storage.get_op(dep)?.is_none() {
                missing.insert(*dep);
            }
        }
        Ok(missing)
    }

    fn validate_pending_op_projected(
        &self,
        op: &Op,
        missing_deps: &BTreeSet<crate::OpId>,
        overlay_meta: &BTreeMap<crate::OpId, OpMeta>,
        overlay_tips: &BTreeMap<(TopicId, ActorId), (u64, crate::OpId)>,
        overlay_index: &BTreeMap<(TopicId, ActorId, u64), crate::OpId>,
        state: Option<&TopicState>,
    ) -> Result<OpAdmission> {
        let body = &op.signed.body;
        if missing_deps.len() > MAX_PENDING_MISSING_DEPS {
            return Err(Error::Storage(
                "pending op has too many missing deps".into(),
            ));
        }
        if body.actor_id != actor_id_for(body.topic_id, body.author) {
            return Err(Error::ActorAuthorMismatch);
        }
        if body.actor_seq == 0 {
            return Err(Error::ActorSeqGap {
                expected: 1,
                actual: 0,
            });
        }
        if let Some(existing) = self
            .storage
            .actor_index(&body.topic_id, &body.actor_id, body.actor_seq)?
            .or_else(|| {
                overlay_index
                    .get(&(body.topic_id, body.actor_id, body.actor_seq))
                    .copied()
            })
        {
            if existing != op.id {
                return Err(Error::ActorFork);
            }
            if self.is_admitted_duplicate(op)? {
                return Ok(OpAdmission::Duplicate);
            }
        }
        match &body.payload {
            TopicPayload::Genesis(_) => {
                if body.actor_seq != 1
                    || body.actor_prev.is_some()
                    || !body.deps.is_empty()
                    || state.is_some()
                {
                    return Err(Error::InvalidGenesis);
                }
            }
            TopicPayload::Event(envelope) => {
                if body.deps.is_empty() || body.generation == 0 {
                    return Err(Error::InvalidOpId);
                }
                // When we already know the topic, only buffer pending ops from
                // known members. This stops non-members from consuming
                // per-source pending quota by submitting structurally-valid
                // ops that would be rejected at admission time anyway.
                if let Some(state) = state {
                    ensure_event_type(&state.event_type_id, &envelope.type_id)?;
                    if !state.members.contains(&body.author) {
                        return Err(Error::NotTopicMember);
                    }
                }
            }
            TopicPayload::Control(_) => {
                if body.deps.is_empty() || body.generation == 0 {
                    return Err(Error::InvalidOpId);
                }
                if let Some(state) = state
                    && !state.members.contains(&body.author)
                {
                    return Err(Error::NotTopicMember);
                }
            }
        }
        match (body.actor_seq, body.actor_prev) {
            (1, Some(_)) => return Err(Error::ActorPrevMismatch),
            (2.., None) => return Err(Error::ActorPrevMismatch),
            _ => {}
        }
        if let Some(prev) = body.actor_prev {
            if !body.deps.contains(&prev) {
                return Err(Error::ActorPrevMismatch);
            }
            if !missing_deps.contains(&prev) {
                let prev_meta = self.meta_projected(&prev, overlay_meta)?;
                if prev_meta.topic_id != body.topic_id || prev_meta.actor_id != body.actor_id {
                    return Err(Error::ActorPrevMismatch);
                }
                if checked_next(prev_meta.actor_seq)? != body.actor_seq {
                    return Err(Error::ActorSeqGap {
                        expected: checked_next(prev_meta.actor_seq)?,
                        actual: body.actor_seq,
                    });
                }
            }
        }
        let expected = match overlay_tips.get(&(body.topic_id, body.actor_id)).copied() {
            Some(tip) => Some(tip),
            None => self.storage.actor_tip(&body.topic_id, &body.actor_id)?,
        };
        if let Some((tip_seq, tip_id)) = expected {
            let next_seq = checked_next(tip_seq)?;
            if body.actor_seq <= tip_seq {
                if self.is_admitted_duplicate(op)? {
                    return Ok(OpAdmission::Duplicate);
                }
                return Err(Error::ActorSeqGap {
                    expected: next_seq,
                    actual: body.actor_seq,
                });
            }
            if body.actor_prev == Some(tip_id) && body.actor_seq != next_seq {
                return Err(Error::ActorSeqGap {
                    expected: next_seq,
                    actual: body.actor_seq,
                });
            }
        }
        for dep in &body.deps {
            if missing_deps.contains(dep) {
                continue;
            }
            let meta = self.meta_projected(dep, overlay_meta)?;
            if meta.topic_id != body.topic_id {
                return Err(Error::TopicMismatch);
            }
            if meta.generation >= body.generation {
                return Err(Error::InvalidOpId);
            }
        }
        Ok(OpAdmission::Admit)
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_op_projected(
        &self,
        op: &Op,
        overlay_ops: &BTreeMap<crate::OpId, Op>,
        overlay_meta: &BTreeMap<crate::OpId, OpMeta>,
        overlay_tips: &BTreeMap<(TopicId, ActorId), (u64, crate::OpId)>,
        overlay_index: &BTreeMap<(TopicId, ActorId, u64), crate::OpId>,
        heads: &BTreeSet<crate::OpId>,
        state: Option<&TopicState>,
    ) -> Result<OpAdmission> {
        let body = &op.signed.body;
        if body.actor_id != actor_id_for(body.topic_id, body.author) {
            return Err(Error::ActorAuthorMismatch);
        }
        if let Some(existing) = self
            .storage
            .actor_index(&body.topic_id, &body.actor_id, body.actor_seq)?
            .or_else(|| {
                overlay_index
                    .get(&(body.topic_id, body.actor_id, body.actor_seq))
                    .copied()
            })
        {
            if existing != op.id {
                return Err(Error::ActorFork);
            }
            if self.is_admitted_duplicate(op)? {
                return Ok(OpAdmission::Duplicate);
            }
        }
        match &body.payload {
            TopicPayload::Genesis(_) => {
                if body.actor_seq != 1
                    || body.actor_prev.is_some()
                    || !body.deps.is_empty()
                    || state.is_some()
                {
                    return Err(Error::InvalidGenesis);
                }
            }
            TopicPayload::Event(envelope) => {
                let state = state.ok_or(Error::TopicNotFound)?;
                ensure_event_type(&state.event_type_id, &envelope.type_id)?;
                let author_is_member = if body.deps == *heads {
                    state.members.contains(&body.author)
                } else {
                    self.projected_state_for_deps(
                        &body.topic_id,
                        &body.deps,
                        overlay_ops,
                        overlay_meta,
                    )?
                    .members
                    .contains(&body.author)
                };
                if !author_is_member {
                    return Err(Error::NotTopicMember);
                }
            }
            TopicPayload::Control(_) => {
                let state = state.ok_or(Error::TopicNotFound)?;
                let author_is_member = if body.deps == *heads {
                    state.members.contains(&body.author)
                } else {
                    self.projected_state_for_deps(
                        &body.topic_id,
                        &body.deps,
                        overlay_ops,
                        overlay_meta,
                    )?
                    .members
                    .contains(&body.author)
                };
                if !author_is_member {
                    return Err(Error::NotTopicMember);
                }
            }
        }
        let expected = match overlay_tips.get(&(body.topic_id, body.actor_id)).copied() {
            Some(tip) => Some(tip),
            None => self.storage.actor_tip(&body.topic_id, &body.actor_id)?,
        };
        let (expected_seq, expected_prev) = next_actor_position(expected)?;
        if body.actor_seq != expected_seq {
            if body.actor_seq < expected_seq && self.is_admitted_duplicate(op)? {
                return Ok(OpAdmission::Duplicate);
            }
            return Err(Error::ActorSeqGap {
                expected: expected_seq,
                actual: body.actor_seq,
            });
        }
        if body.actor_prev != expected_prev {
            return Err(Error::ActorPrevMismatch);
        }
        let mut generation = 0;
        for id in &body.deps {
            let meta = self.meta_projected(id, overlay_meta)?;
            generation = generation.max(checked_next(meta.generation)?);
        }
        if body.generation != generation {
            return Err(Error::InvalidOpId);
        }
        Ok(OpAdmission::Admit)
    }

    /// Re-reads storage after a tip/seq mismatch: a concurrent admission may
    /// have committed this exact op between the batch dedup check and the
    /// validation reads. Such ops are duplicates, not gaps or forks. Op ids
    /// are content-addressed, so an actor-index entry mapping the op's seq to
    /// its exact id also proves admission even while the op record itself is
    /// not yet visible mid-commit.
    fn is_admitted_duplicate(&self, op: &Op) -> Result<bool> {
        if self.storage.get_op(&op.id)?.is_some() {
            return Ok(true);
        }
        let body = &op.signed.body;
        Ok(self
            .storage
            .actor_index(&body.topic_id, &body.actor_id, body.actor_seq)?
            == Some(op.id))
    }

    fn projected_state_for_deps(
        &self,
        topic_id: &TopicId,
        deps: &BTreeSet<crate::OpId>,
        overlay_ops: &BTreeMap<crate::OpId, Op>,
        overlay_meta: &BTreeMap<crate::OpId, OpMeta>,
    ) -> Result<TopicState> {
        let mut reachable = BTreeMap::new();
        let mut stack = deps.iter().copied().collect::<Vec<_>>();
        while let Some(id) = stack.pop() {
            if reachable.contains_key(&id) {
                continue;
            }
            let meta = self.meta_projected(&id, overlay_meta)?;
            if meta.topic_id != *topic_id {
                return Err(Error::TopicMismatch);
            }
            let op = self.op_projected(&id, overlay_ops)?;
            stack.extend(meta.deps.iter().copied());
            reachable.insert(id, op);
        }

        materialize_topic_state(reachable.into_values().collect(), BTreeSet::new())
    }

    fn observed_clock_for_deps(
        &self,
        topic_id: &TopicId,
        deps: &BTreeSet<crate::OpId>,
    ) -> Result<crate::ActorClock> {
        let mut observed_clock = crate::ActorClock::new();
        for id in deps {
            let meta = self
                .storage
                .get_meta(id)?
                .ok_or(Error::MissingDependency(*id))?;
            if meta.topic_id != *topic_id {
                return Err(Error::TopicMismatch);
            }
            observed_clock.merge(&meta.observed_clock);
            observed_clock.observe(meta.actor_id, meta.actor_seq);
        }

        Ok(observed_clock)
    }

    fn topic_state_after(
        &self,
        op: &Op,
        heads: BTreeSet<crate::OpId>,
        base_state: Option<TopicState>,
    ) -> Result<Option<TopicState>> {
        let body = &op.signed.body;
        match &body.payload {
            TopicPayload::Genesis(genesis) => Ok(Some(TopicState {
                topic_id: body.topic_id,
                event_type_id: genesis.event_type_id.clone(),
                genesis: op.id,
                heads,
                members: genesis.initial_peers.clone(),
                replication_policy: genesis.replication_policy.clone(),
                membership_controls: BTreeMap::new(),
                replication_policy_control: None,
            })),
            TopicPayload::Event(_) => Ok(None),
            TopicPayload::Control(control) => {
                let mut state = base_state.ok_or(Error::TopicNotFound)?;
                state.heads = heads;
                apply_control_to_state(&mut state, op, control);
                Ok(Some(state))
            }
        }
    }

    fn commit_admission<F>(
        &self,
        op: Op,
        meta: OpMeta,
        expected_heads: BTreeSet<crate::OpId>,
        expected_state: Option<TopicState>,
        effects: &F,
    ) -> Result<()>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        let heads = heads_after(&expected_heads, &op);
        let topic_state = self.topic_state_after(&op, heads.clone(), expected_state.clone())?;
        let effective_state = topic_state
            .as_ref()
            .or(expected_state.as_ref())
            .ok_or(Error::TopicNotFound)?;
        let effects = effects(&op, &meta, effective_state)?;
        self.storage.put_admitted_batch(AdmittedBatch {
            topic_id: op.signed.body.topic_id,
            expected_heads,
            expected_topic_state: expected_state,
            entries: vec![(op, meta)],
            heads,
            topic_state,
            effects,
        })
    }

    fn topic_state_for_deps(
        &self,
        topic_id: &TopicId,
        deps: &BTreeSet<crate::OpId>,
    ) -> Result<TopicState> {
        let mut reachable = BTreeMap::new();
        let mut stack = deps.iter().copied().collect::<Vec<_>>();
        while let Some(id) = stack.pop() {
            if reachable.contains_key(&id) {
                continue;
            }
            let op = self
                .storage
                .get_op(&id)?
                .ok_or(Error::MissingDependency(id))?;
            let body = &op.signed.body;
            if body.topic_id != *topic_id {
                return Err(Error::TopicMismatch);
            }
            stack.extend(body.deps.iter().copied());
            reachable.insert(id, op);
        }

        materialize_topic_state(reachable.into_values().collect(), BTreeSet::new())
    }

    fn ensure_member(&self, topic_id: &TopicId, peer: crate::PeerId) -> Result<()> {
        let state = self
            .storage
            .topic_state(topic_id)?
            .ok_or(Error::TopicNotFound)?;
        if state.members.contains(&peer) {
            Ok(())
        } else {
            Err(Error::NotTopicMember)
        }
    }
}
