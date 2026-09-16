// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::storage::{AdmissionEffects, OpMeta, TopicState};
use crate::{
    ActorId, Error, EventEnvelope, Op, Result, Signer, TopicControl, TopicGenesis, TopicId,
    TopicPayload,
};

use super::admission::is_admission_race;
use super::{MAX_ADMISSION_RETRIES, Oplog, conflict_pause};

pub(crate) fn is_structural_genesis(op: &Op) -> bool {
    let body = &op.signed.body;
    matches!(body.payload, TopicPayload::Genesis(_))
        && body.actor_seq == 1
        && body.actor_prev.is_none()
        && body.deps.is_empty()
}


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
}
