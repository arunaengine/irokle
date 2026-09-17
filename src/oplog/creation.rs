// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use crate::storage::{AdmissionEffects, OpMeta, TopicState};
use crate::{
    ActorId, Error, EventEnvelope, Op, OpBody, OpId, Result, Signer, TopicControl, TopicGenesis,
    TopicId, TopicPayload,
};

use super::admission::{checked_next, heads_after, is_local_race};
use super::{
    AdmittedBatch, BatchOverlay, MAX_ADMISSION_RETRIES, OpAdmission, Oplog, conflict_pause,
};

impl<S: super::Storage> Oplog<S> {
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

    /// Create a topic and its first event as one atomic admission.
    ///
    #[doc = include_str!("contracts/create_genesis_event.md")]
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
                Err(err) if is_local_race(&err) => continue,
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
                Err(err) if is_local_race(&err) => continue,
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
            #[cfg(feature = "iroh")]
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
        #[cfg(feature = "iroh")]
        genesis_op.validate_frame()?;
        self.validate_op(&genesis_op)?;
        #[cfg(feature = "iroh")]
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
}
