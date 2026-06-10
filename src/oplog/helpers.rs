// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use crate::storage::{ControlKey, OpMeta, TopicState};
use crate::{Error, Op, Result, TopicControl, TopicPayload};

pub(super) fn materialize_topic_state(
    ops: Vec<Op>,
    heads: BTreeSet<crate::OpId>,
) -> Result<TopicState> {
    let mut genesis = None;
    let mut member_controls = BTreeMap::new();
    let mut replication_policy_control = None;

    for op in ops {
        let body = &op.signed.body;
        match &body.payload {
            TopicPayload::Genesis(topic_genesis) => {
                genesis = Some((op.id, body.topic_id, topic_genesis.clone()));
            }
            TopicPayload::Control(TopicControl::AddPeer { peer }) => {
                set_membership_control(&mut member_controls, *peer, control_key(&op), true);
            }
            TopicPayload::Control(TopicControl::RemovePeer { peer }) => {
                set_membership_control(&mut member_controls, *peer, control_key(&op), false);
            }
            TopicPayload::Control(TopicControl::SetReplicationPolicy { policy }) => {
                let key = control_key(&op);
                if replication_policy_control
                    .as_ref()
                    .is_none_or(|(current_key, _)| key > *current_key)
                {
                    replication_policy_control = Some((key, policy.clone()));
                }
            }
            TopicPayload::Event(_) => {}
        }
    }

    let (genesis_id, topic_id, topic_genesis) = genesis.ok_or(Error::TopicNotFound)?;
    let mut members = topic_genesis.initial_peers.clone();
    for (peer, (_, is_member)) in &member_controls {
        if *is_member {
            members.insert(*peer);
        } else {
            members.remove(peer);
        }
    }

    let replication_policy = replication_policy_control
        .as_ref()
        .map(|(_, policy)| policy.clone())
        .unwrap_or(topic_genesis.replication_policy);

    Ok(TopicState {
        topic_id,
        event_type_id: topic_genesis.event_type_id,
        genesis: genesis_id,
        heads,
        members,
        replication_policy,
        membership_controls: member_controls,
        replication_policy_control,
    })
}

pub(super) fn next_actor_position(
    tip: Option<(u64, crate::OpId)>,
) -> Result<(u64, Option<crate::OpId>)> {
    match tip {
        Some((seq, id)) => Ok((checked_next(seq)?, Some(id))),
        None => Ok((1, None)),
    }
}

pub(super) fn checked_next(value: u64) -> Result<u64> {
    value.checked_add(1).ok_or(Error::InvalidOpId)
}

pub(super) fn ensure_event_type(expected: &str, actual: &str) -> Result<()> {
    if expected == actual {
        Ok(())
    } else {
        Err(Error::EventTypeMismatch {
            expected: expected.to_owned(),
            actual: actual.to_owned(),
        })
    }
}

pub(super) fn is_semantic_rejection(err: &Error) -> bool {
    matches!(
        err,
        Error::InvalidSignature
            | Error::InvalidPublicKey
            | Error::InvalidOpId
            | Error::WrongSigner
            | Error::EventTypeMismatch { .. }
            | Error::TopicNotFound
            | Error::NotTopicMember
            | Error::ActorSeqGap { .. }
            | Error::ActorPrevMismatch
            | Error::ActorFork
            | Error::ActorAuthorMismatch
            | Error::TopicMismatch
            | Error::MissingDependency(_)
            | Error::InvalidGenesis
            | Error::Decode(_)
    )
}

pub(super) fn is_local_admission_race(err: &Error) -> bool {
    // InvalidOpId covers a concurrent admission advancing max_generation
    // between the heads read and op validation; a retry re-reads fresh state.
    matches!(
        err,
        Error::AdmissionConflict
            | Error::ActorSeqGap { .. }
            | Error::ActorPrevMismatch
            | Error::ActorFork
            | Error::InvalidOpId
    )
}

pub(super) fn heads_after(current: &BTreeSet<crate::OpId>, op: &Op) -> BTreeSet<crate::OpId> {
    let mut heads = current.clone();
    for dep in &op.signed.body.deps {
        heads.remove(dep);
    }
    heads.insert(op.id);
    heads
}

pub(super) fn pending_meta_for(op: &Op, missing_deps: BTreeSet<crate::OpId>) -> OpMeta {
    let body = &op.signed.body;
    OpMeta {
        id: op.id,
        topic_id: body.topic_id,
        author: body.author,
        actor_id: body.actor_id,
        actor_seq: body.actor_seq,
        actor_prev: body.actor_prev,
        deps: body.deps.clone(),
        generation: body.generation,
        observed_clock: crate::ActorClock::new(),
        ready: false,
        missing_deps,
    }
}

pub(super) fn set_membership_control(
    controls: &mut BTreeMap<crate::PeerId, (ControlKey, bool)>,
    peer: crate::PeerId,
    key: ControlKey,
    is_member: bool,
) -> bool {
    if controls
        .get(&peer)
        .is_none_or(|(current_key, _)| key > *current_key)
    {
        controls.insert(peer, (key, is_member));
        true
    } else {
        false
    }
}

pub(super) fn apply_control_to_state(state: &mut TopicState, op: &Op, control: &TopicControl) {
    match control {
        TopicControl::AddPeer { peer } => {
            if set_membership_control(&mut state.membership_controls, *peer, control_key(op), true)
            {
                state.members.insert(*peer);
            }
        }
        TopicControl::RemovePeer { peer } => {
            if set_membership_control(
                &mut state.membership_controls,
                *peer,
                control_key(op),
                false,
            ) {
                state.members.remove(peer);
            }
        }
        TopicControl::SetReplicationPolicy { policy } => {
            let key = control_key(op);
            if state
                .replication_policy_control
                .as_ref()
                .is_none_or(|(current_key, _)| key > *current_key)
            {
                state.replication_policy_control = Some((key, policy.clone()));
                state.replication_policy = policy.clone();
            }
        }
    }
}

pub(super) fn control_key(op: &Op) -> ControlKey {
    let body = &op.signed.body;
    ControlKey {
        generation: body.generation,
        actor_id: body.actor_id,
        actor_seq: body.actor_seq,
        op_id: op.id,
    }
}
