// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use crate::storage::{AdmissionEffects, OpMeta, TopicState};
use crate::{
    ActorId, Error, EventEnvelope, Op, OpBody, OpId, Result, Signer, TopicControl, TopicGenesis,
    TopicId, TopicPayload, actor_id_for,
};

use crate::oplog::admission::{
    checked_next, ensure_event_type, is_admission_race, next_actor_position,
};
use crate::oplog::membership::{apply_control, materialize_topic_state};
use crate::oplog::{
    AdmittedBatch, BatchOverlay, MAX_ADMISSION_RETRIES, OpAdmission, Oplog, conflict_pause,
};

impl<S: crate::oplog::Storage> Oplog<S> {
    pub fn create_topic_genesis(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        genesis: TopicGenesis,
        signer: &impl Signer,
    ) -> Result<Op> {
        self.create_topic_effects(topic_id, actor_id, genesis, signer, |_, _, _| {
            Ok(AdmissionEffects::default())
        })
    }

    pub(crate) fn create_topic_effects<F>(
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
        self.create_local_effects(
            topic_id,
            actor_id,
            TopicPayload::Genesis(genesis),
            signer,
            effects,
        )
        .map(|(op, _)| op)
    }

    /// Create a topic and its first event, chained off the genesis, as one atomic admission.
    /// Returns `(genesis, event)`; fails with [`crate::Error::InvalidGenesis`] if the topic exists.
    pub fn create_topic_genesis_with_event(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        genesis: TopicGenesis,
        event: EventEnvelope,
        signer: &impl Signer,
    ) -> Result<(Op, Op)> {
        self.create_genesis_effects(topic_id, actor_id, genesis, event, signer, |_, _, _| {
            Ok(AdmissionEffects::default())
        })
        .map(|((genesis, _), (event, _))| (genesis, event))
    }

    pub(crate) fn create_genesis_effects<F>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        genesis: TopicGenesis,
        event: EventEnvelope,
        signer: &impl Signer,
        effects: F,
    ) -> Result<((Op, OpMeta), (Op, OpMeta))>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        let mut peers = genesis.initial_peers.clone();
        peers.insert(signer.peer_id());
        let genesis = TopicGenesis {
            initial_peers: peers,
            ..genesis
        };
        let mut signed = None;
        // Held across retries, so a slow received batch of the topic is not voided.
        let _turn = self.turns.take(topic_id)?;
        for attempt in 0..MAX_ADMISSION_RETRIES {
            conflict_pause(attempt);
            match self.try_genesis_effects(
                (topic_id, actor_id),
                (genesis.clone(), event.clone()),
                signer,
                &mut signed,
                &effects,
            ) {
                Err(err) if is_admission_race(&err) => continue,
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
        self.create_event_effects(topic_id, actor_id, event, signer, |_, _, _| {
            Ok(AdmissionEffects::default())
        })
        .map(|(op, _)| op)
    }

    pub(crate) fn create_event_effects<F>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        event: EventEnvelope,
        signer: &impl Signer,
        effects: F,
    ) -> Result<(Op, OpMeta)>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        self.create_local_effects(
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
        self.create_control_effects(topic_id, actor_id, control, signer, |_, _, _| {
            Ok(AdmissionEffects::default())
        })
    }

    pub(crate) fn create_control_effects<F>(
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
        self.create_local_effects(
            topic_id,
            actor_id,
            TopicPayload::Control(control),
            signer,
            effects,
        )
        .map(|(op, _)| op)
    }

    fn create_local_effects<F>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        payload: TopicPayload,
        signer: &impl Signer,
        effects: F,
    ) -> Result<(Op, OpMeta)>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        let mut signed = None;
        // Held across retries, so a slow received batch of the topic is not voided.
        let _turn = self.turns.take(topic_id)?;
        for attempt in 0..MAX_ADMISSION_RETRIES {
            conflict_pause(attempt);
            match self.try_local_effects(
                topic_id,
                actor_id,
                payload.clone(),
                signer,
                &mut signed,
                &effects,
            ) {
                Err(err) if is_admission_race(&err) => continue,
                result => return result,
            }
        }
        Err(Error::AdmissionConflict)
    }

    fn try_local_effects<F>(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        payload: TopicPayload,
        signer: &impl Signer,
        signed: &mut Option<Op>,
        effects: &F,
    ) -> Result<(Op, OpMeta)>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        if !matches!(payload, TopicPayload::Genesis(_)) {
            self.ensure_member(&topic_id, signer.peer_id())?;
            if self.storage.actor_tip(&topic_id, &actor_id)?.is_none()
                && self
                    .storage
                    .read_snapshot(|read| read.actor_count(&topic_id))?
                    >= crate::sync::MAX_TOPIC_ACTORS
            {
                return Err(Error::TopicFull);
            }
        }
        let expected_heads = self.storage.heads(&topic_id)?;
        let expected_state = self.storage.topic_state(&topic_id)?;
        let op = self.next_local_op(
            topic_id,
            actor_id,
            expected_heads.clone(),
            payload,
            signer,
            signed.take(),
        )?;
        let committed = (|| {
            op.validate_frame()?;
            self.validate_op(&op)?;
            let meta = self.meta_for(&op)?;
            self.commit_admission(
                op.clone(),
                meta.clone(),
                expected_heads,
                expected_state,
                effects,
            )?;
            Ok(meta)
        })();
        match committed {
            Ok(meta) => Ok((op, meta)),
            // A failed attempt keeps the signed op for the retry to reuse.
            Err(error) => {
                *signed = Some(op);
                Err(error)
            }
        }
    }

    fn try_genesis_effects<F>(
        &self,
        (topic_id, actor_id): (TopicId, ActorId),
        (genesis, event): (TopicGenesis, EventEnvelope),
        signer: &impl Signer,
        signed: &mut Option<(Op, Op)>,
        effects: &F,
    ) -> Result<((Op, OpMeta), (Op, OpMeta))>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        let expected_heads = self.storage.heads(&topic_id)?;
        let expected_state = self.storage.topic_state(&topic_id)?;
        // A retry whose bodies did not change reuses the ops signed before.
        let (previous_genesis, previous_event) = signed.take().unzip();
        let genesis_op = self.next_local_op(
            topic_id,
            actor_id,
            expected_heads.clone(),
            TopicPayload::Genesis(genesis),
            signer,
            previous_genesis,
        )?;
        let genesis_meta = self.meta_for(&genesis_op)?;
        let event_body = OpBody {
            topic_id,
            author: signer.peer_id(),
            actor_id,
            actor_seq: checked_next(genesis_meta.actor_seq)?,
            actor_prev: Some(genesis_op.id),
            deps: [genesis_op.id].into(),
            generation: checked_next(genesis_meta.generation)?,
            payload: TopicPayload::Event(event),
        };
        let event_op = match previous_event.filter(|op| op.signed.body == event_body) {
            Some(op) => op,
            None => {
                let op = Op::sign(event_body, signer)?;
                op.validate()?;
                op
            }
        };
        let admitted = self.admit_genesis_pair(
            (genesis_op.clone(), genesis_meta),
            event_op.clone(),
            (expected_heads, expected_state),
            effects,
        );
        if admitted.is_err() {
            *signed = Some((genesis_op, event_op));
        }
        admitted
    }

    /// Checks and commits a signed genesis and its first event in one batch.
    fn admit_genesis_pair<F>(
        &self,
        (genesis_op, genesis_meta): (Op, OpMeta),
        event_op: Op,
        (expected_heads, expected_state): (BTreeSet<OpId>, Option<TopicState>),
        effects: &F,
    ) -> Result<((Op, OpMeta), (Op, OpMeta))>
    where
        F: Fn(&Op, &OpMeta, &TopicState) -> Result<AdmissionEffects>,
    {
        let topic_id = genesis_op.signed.body.topic_id;
        let actor_id = genesis_op.signed.body.actor_id;
        genesis_op.validate_frame()?;
        self.validate_op(&genesis_op)?;
        event_op.validate_frame()?;

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
            &BatchOverlay {
                ops: &overlay_ops,
                meta: &overlay_meta,
                tips: &overlay_tips,
                index: &overlay_index,
                reset: false,
            },
            &genesis_heads,
            Some(&state),
            &mut BTreeMap::new(),
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
                (genesis_op.clone(), genesis_meta.clone()),
                (event_op.clone(), event_meta.clone()),
            ],
            heads,
            topic_state: Some(state),
            effects: admission_effects,
        })?;
        Ok(((genesis_op, genesis_meta), (event_op, event_meta)))
    }

    fn next_local_op(
        &self,
        topic_id: TopicId,
        actor_id: ActorId,
        mut deps: BTreeSet<crate::OpId>,
        payload: TopicPayload,
        signer: &impl Signer,
        previous: Option<Op>,
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
        let body = OpBody {
            topic_id,
            author: signer.peer_id(),
            actor_id,
            actor_seq,
            actor_prev,
            deps,
            generation,
            payload,
        };
        // A retry whose body did not change reuses the op signed before.
        if let Some(op) = previous.filter(|op| op.signed.body == body) {
            return Ok(op);
        }
        let op = Op::sign(body, signer)?;
        op.validate()?;
        Ok(op)
    }

    fn validate_op(&self, op: &Op) -> Result<()> {
        let body = &op.signed.body;
        if body.actor_id != actor_id_for(body.topic_id, body.author) {
            return Err(Error::ActorAuthorMismatch);
        }
        if let TopicPayload::Genesis(genesis) = &body.payload
            && (genesis.event_type_id.len() > crate::sync::MAX_TYPE_BYTES
                || self.storage.topic_state(&body.topic_id)?.is_some())
        {
            return Err(Error::InvalidGenesis);
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
        if let Some(prev) = body.actor_prev
            && !body.deps.contains(&prev)
        {
            return Err(Error::ActorPrevMismatch);
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
                    self.state_for_deps(&body.topic_id, &body.deps)?
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
                    self.state_for_deps(&body.topic_id, &body.deps)?
                        .members
                        .contains(&body.author)
                };
                if !author_is_member {
                    return Err(Error::NotTopicMember);
                }
            }
        }
        let mut generation = 0;
        for id in &body.deps {
            let meta = self
                .storage
                .get_position(id)?
                .ok_or(Error::MissingDependency(*id))?;
            generation = generation.max(checked_next(meta.generation)?);
        }
        if body.generation != generation {
            return Err(Error::GenerationMismatch {
                expected: generation,
                actual: body.generation,
            });
        }
        Ok(())
    }

    fn state_for_deps(
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

    fn meta_for(&self, op: &Op) -> Result<OpMeta> {
        let body = &op.signed.body;
        let observed_clock = self.clock_from_deps(&body.topic_id, &body.deps)?;
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

    fn clock_from_deps(
        &self,
        topic_id: &TopicId,
        deps: &BTreeSet<crate::OpId>,
    ) -> Result<crate::ActorClock> {
        let mut observed_clock = crate::ActorClock::new();
        for id in deps {
            let (meta, clock) = self
                .storage
                .get_observation(id)?
                .ok_or(Error::MissingDependency(*id))?;
            if meta.topic_id != *topic_id {
                return Err(Error::TopicMismatch);
            }
            observed_clock.merge(&clock);
            observed_clock.observe(meta.actor_id, meta.actor_seq);
        }

        Ok(observed_clock)
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
                apply_control(&mut state, op, control);
                Ok(Some(state))
            }
        }
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

fn heads_after(current: &BTreeSet<crate::OpId>, op: &Op) -> BTreeSet<crate::OpId> {
    let mut heads = current.clone();
    for dep in &op.signed.body.deps {
        heads.remove(dep);
    }
    heads.insert(op.id);
    heads
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ed25519_dalek::Signature;

    use crate::oplog::Oplog;
    use crate::{
        Ed25519Signer, Error, EventEnvelope, Op, OpBody, PeerId, ReplicationPolicy, Result, Signer,
        TopicControl, TopicGenesis, TopicId, TopicPayload, actor_id_for,
    };

    fn note(byte: u8) -> EventEnvelope {
        EventEnvelope {
            type_id: "test.note".into(),
            payload: vec![byte].into(),
        }
    }

    /// A genesis of `topic` by `signer` without dependencies, signed directly.
    fn genesis_body(topic: TopicId, signer: &Ed25519Signer, type_id: String) -> OpBody {
        OpBody {
            topic_id: topic,
            author: signer.peer_id(),
            actor_id: actor_id_for(topic, signer.peer_id()),
            actor_seq: 1,
            actor_prev: None,
            deps: [].into(),
            generation: 0,
            payload: TopicPayload::Genesis(TopicGenesis::new(type_id, [signer.peer_id()])),
        }
    }

    /// The first control of `writer` in `topic`, depending only on `dep`.
    fn writer_control(topic: TopicId, writer: &Ed25519Signer, dep: &Op) -> Op {
        let policy = ReplicationPolicy::all().with_max_sync_peers(7);
        let body = OpBody {
            topic_id: topic,
            author: writer.peer_id(),
            actor_id: actor_id_for(topic, writer.peer_id()),
            actor_seq: 1,
            actor_prev: None,
            deps: [dep.id].into(),
            generation: dep.signed.body.generation + 1,
            payload: TopicPayload::Control(TopicControl::SetReplicationPolicy { policy }),
        };
        Op::sign(body, writer).unwrap()
    }

    struct CountingSigner {
        inner: Ed25519Signer,
        signed: AtomicUsize,
    }

    impl Signer for CountingSigner {
        fn peer_id(&self) -> PeerId {
            self.inner.peer_id()
        }

        fn sign(&self, message: &[u8]) -> Result<Signature> {
            self.signed.fetch_add(1, Ordering::SeqCst);
            self.inner.sign(message)
        }
    }

    /// A second op at a held actor position is a fork; the holder itself is not.
    #[test]
    fn rejects_actor_fork() {
        let owner = Ed25519Signer::from_bytes(&[81; 32]);
        let topic = TopicId::hash(b"rejects-actor-fork");
        let actor = actor_id_for(topic, owner.peer_id());
        let log = Oplog::new();
        let genesis = TopicGenesis::new("test.note", []);
        log.create_topic_genesis(topic, actor, genesis, &owner)
            .unwrap();
        let first = log.create_event_op(topic, actor, note(0), &owner).unwrap();
        let mut body = first.signed.body.clone();
        body.payload = TopicPayload::Event(note(1));
        let fork = Op::sign(body, &owner).unwrap();
        assert!(matches!(log.validate_op(&fork), Err(Error::ActorFork)));
        assert!(matches!(
            log.validate_op(&first),
            Err(Error::ActorSeqGap {
                expected: 3,
                actual: 2
            })
        ));
    }

    /// A first-position genesis that depends on an existing op of another topic
    /// is still not a genesis.
    #[test]
    fn rejects_genesis_deps() {
        let owner = Ed25519Signer::from_bytes(&[82; 32]);
        let other = TopicId::hash(b"rejects-genesis-deps-other");
        let topic = TopicId::hash(b"rejects-genesis-deps");
        let log = Oplog::new();
        let genesis = TopicGenesis::new("test.note", []);
        let anchor = log
            .create_topic_genesis(other, actor_id_for(other, owner.peer_id()), genesis, &owner)
            .unwrap();
        let mut body = genesis_body(topic, &owner, "test.note".into());
        body.deps = [anchor.id].into();
        body.generation = 1;
        let op = Op::sign(body, &owner).unwrap();
        assert!(matches!(log.validate_op(&op), Err(Error::InvalidGenesis)));
    }

    #[test]
    fn type_length_boundary() {
        let owner = Ed25519Signer::from_bytes(&[83; 32]);
        let topic = TopicId::hash(b"type-length-boundary");
        let log = Oplog::new();
        let longest = "x".repeat(crate::sync::MAX_TYPE_BYTES);
        let op = Op::sign(genesis_body(topic, &owner, longest), &owner).unwrap();
        log.validate_op(&op).unwrap();
        let too_long = "x".repeat(crate::sync::MAX_TYPE_BYTES + 1);
        let op = Op::sign(genesis_body(topic, &owner, too_long), &owner).unwrap();
        assert!(matches!(log.validate_op(&op), Err(Error::InvalidGenesis)));
    }

    /// A control on stale deps is checked against the members at those deps,
    /// not against the members at the current heads.
    #[test]
    fn control_uses_deps() {
        let owner = Ed25519Signer::from_bytes(&[84; 32]);
        let writer = Ed25519Signer::from_bytes(&[85; 32]);
        let peer = writer.peer_id();
        for (seed, initial, control, admitted) in [
            (
                &b"control-added-later"[..],
                vec![],
                TopicControl::AddPeer { peer },
                false,
            ),
            (
                b"control-removed-later",
                vec![peer],
                TopicControl::RemovePeer { peer },
                true,
            ),
        ] {
            let topic = TopicId::hash(seed);
            let actor = actor_id_for(topic, owner.peer_id());
            let log = Oplog::new();
            let genesis = TopicGenesis::new("test.note", initial);
            let genesis = log
                .create_topic_genesis(topic, actor, genesis, &owner)
                .unwrap();
            log.create_control_op(topic, actor, control, &owner)
                .unwrap();
            let result = log.validate_op(&writer_control(topic, &writer, &genesis));
            if admitted {
                result.unwrap();
            } else {
                assert!(matches!(result, Err(Error::NotTopicMember)));
            }
        }
    }

    /// A non-member is refused before its signer is asked to sign anything.
    #[test]
    fn refuses_before_signing() {
        let owner = Ed25519Signer::from_bytes(&[86; 32]);
        let stranger = CountingSigner {
            inner: Ed25519Signer::from_bytes(&[87; 32]),
            signed: AtomicUsize::new(0),
        };
        let topic = TopicId::hash(b"refuses-before-signing");
        let log = Oplog::new();
        let genesis = TopicGenesis::new("test.note", []);
        log.create_topic_genesis(topic, actor_id_for(topic, owner.peer_id()), genesis, &owner)
            .unwrap();
        let actor = actor_id_for(topic, stranger.peer_id());
        let result = log.create_event_op(topic, actor, note(0), &stranger);
        assert!(matches!(result, Err(Error::NotTopicMember)));
        assert_eq!(stranger.signed.load(Ordering::SeqCst), 0);
    }
}
