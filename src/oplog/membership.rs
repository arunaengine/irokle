// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::storage::{ControlKey, TopicState};
use crate::{Error, Op, Result, TopicControl, TopicPayload};

pub(super) fn materialize_topic_state(
    ops: Vec<Op>,
    heads: BTreeSet<crate::OpId>,
) -> Result<TopicState> {
    let mut genesis = None;
    let mut topic_id = None;
    let mut member_controls = BTreeMap::new();
    let mut replication_policy_control = None;

    for op in ops {
        let body = &op.signed.body;
        if topic_id.is_some_and(|topic_id| topic_id != body.topic_id) {
            return Err(Error::TopicMismatch);
        }
        topic_id.get_or_insert(body.topic_id);
        match &body.payload {
            TopicPayload::Genesis(topic_genesis) => {
                if genesis
                    .as_ref()
                    .is_some_and(|(genesis_id, _, _)| *genesis_id != op.id)
                {
                    return Err(Error::InvalidGenesis);
                }
                genesis.get_or_insert((op.id, body.topic_id, topic_genesis.clone()));
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

pub(super) fn apply_control(state: &mut TopicState, op: &Op, control: &TopicControl) {
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

pub(super) fn merge_states(
    deps: &BTreeSet<crate::OpId>,
    projections: &BTreeMap<crate::OpId, Arc<TopicState>>,
) -> Result<Arc<TopicState>> {
    let mut merged: Option<Arc<TopicState>> = None;
    for id in deps {
        let incoming = projections.get(id).ok_or(Error::MissingDependency(*id))?;
        let Some(current) = merged.as_mut() else {
            merged = Some(incoming.clone());
            continue;
        };
        if Arc::ptr_eq(current, incoming) {
            continue;
        }
        if current.genesis != incoming.genesis {
            return Err(Error::InvalidGenesis);
        }
        let current = Arc::make_mut(current);
        for (peer, (key, member)) in &incoming.membership_controls {
            if set_membership_control(&mut current.membership_controls, *peer, *key, *member) {
                if *member {
                    current.members.insert(*peer);
                } else {
                    current.members.remove(peer);
                }
            }
        }
        if let Some((key, policy)) = &incoming.replication_policy_control
            && current
                .replication_policy_control
                .as_ref()
                .is_none_or(|(current_key, _)| key > current_key)
        {
            current.replication_policy_control = Some((*key, policy.clone()));
            current.replication_policy = policy.clone();
        }
    }
    merged.ok_or(Error::TopicNotFound)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::oplog::Oplog;
    use crate::oplog::membership::{control_key, materialize_topic_state};
    use crate::{
        Ed25519Signer, Error, EventEnvelope, ReplicationPolicy, Signer, TopicControl, TopicGenesis,
        TopicId, actor_id_for,
    };

    #[test]
    fn rejects_mixed_topics() {
        let owner = Ed25519Signer::from_bytes(&[91; 32]);
        let mut ops = Vec::new();
        for seed in [&b"mixed-topics-first"[..], b"mixed-topics-second"] {
            let topic = TopicId::hash(seed);
            let actor = actor_id_for(topic, owner.peer_id());
            let log = Oplog::new();
            let genesis = log
                .create_topic_genesis(topic, actor, TopicGenesis::new("test.note", []), &owner)
                .unwrap();
            let envelope = EventEnvelope {
                type_id: "test.note".into(),
                payload: vec![0].into(),
            };
            let event = log.create_event_op(topic, actor, envelope, &owner).unwrap();
            ops.push((genesis, event));
        }
        let mixed = vec![ops[0].0.clone(), ops[1].1.clone()];
        assert!(matches!(
            materialize_topic_state(mixed, BTreeSet::new()),
            Err(Error::TopicMismatch)
        ));
        let same = vec![ops[1].0.clone(), ops[1].1.clone()];
        materialize_topic_state(same, BTreeSet::new()).unwrap();
    }

    /// The policy control with the greatest control key wins in every input order.
    #[test]
    fn latest_policy_wins() {
        let owner = Ed25519Signer::from_bytes(&[92; 32]);
        let topic = TopicId::hash(b"latest-policy-wins");
        let actor = actor_id_for(topic, owner.peer_id());
        let log = Oplog::new();
        let genesis = log
            .create_topic_genesis(topic, actor, TopicGenesis::new("test.note", []), &owner)
            .unwrap();
        let controls = (1..=3)
            .map(|peers| {
                let policy = ReplicationPolicy::all().with_max_sync_peers(peers);
                let control = TopicControl::SetReplicationPolicy { policy };
                log.create_control_op(topic, actor, control, &owner)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let latest = ReplicationPolicy::all().with_max_sync_peers(3);
        for order in [[0, 1, 2], [2, 1, 0], [1, 2, 0], [2, 0, 1]] {
            let mut ops = vec![genesis.clone()];
            ops.extend(order.map(|index| controls[index].clone()));
            let state = materialize_topic_state(ops, BTreeSet::new()).unwrap();
            assert_eq!(state.replication_policy, latest, "order {order:?}");
            let (key, _) = state.replication_policy_control.unwrap();
            assert_eq!(key, control_key(&controls[2]), "order {order:?}");
        }
    }
}
