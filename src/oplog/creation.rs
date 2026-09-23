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
        if matches!(body.payload, TopicPayload::Genesis(_))
            && self.storage.topic_state(&body.topic_id)?.is_some()
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
