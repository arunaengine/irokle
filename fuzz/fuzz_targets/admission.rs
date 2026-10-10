// SPDX-License-Identifier: MIT OR Apache-2.0
//! Hostile and malformed operations offered to one in-memory node.

#![no_main]

use std::collections::{BTreeMap, BTreeSet};

use arbitrary::Arbitrary;
use bytes::Bytes;
use irokle::sync::{SyncData, SyncMessage};
use irokle::{
    Ed25519Signer, Error, Event, EventEnvelope, Irokle, Op, OpBody, OpId, Signer, Storage,
    TopicControl, TopicGenesis, TopicId, TopicPayload, actor_id_for,
};
use libfuzzer_sys::fuzz_target;

const MAX_ARRIVALS: usize = 8;

#[derive(Clone, irokle::Event, serde::Deserialize, serde::Serialize)]
#[irokle(type_id = "fuzz.note")]
struct Note(u8);

#[derive(Arbitrary, Debug)]
enum Arrival {
    /// Wire bytes: sync data if they decode as such, otherwise a bare op.
    Raw(Vec<u8>),
    Forged(Forged),
}

#[derive(Arbitrary, Debug)]
struct Forged {
    signer: u8,
    foreign: bool,
    actor: u8,
    seq: u8,
    prev: Option<u8>,
    deps: Vec<u8>,
    generation: u8,
    payload: Payload,
    tamper: Tamper,
    source: Option<u8>,
}

#[derive(Arbitrary, Debug)]
enum Payload {
    Event { typed: bool, bytes: Vec<u8> },
    Add(u8),
    Remove(u8),
    Genesis,
}

#[derive(Arbitrary, Debug, PartialEq)]
enum Tamper {
    Intact,
    Id,
    Body,
}

fuzz_target!(|arrivals: Vec<Arrival>| {
    let signers = [1, 2, 3].map(|seed| Ed25519Signer::from_bytes(&[seed; 32]));
    let topic = TopicId::from_bytes([7; 32]);
    let node = Irokle::builder()
        .with_signer(signers[0].clone())
        .build()
        .expect("node builds");
    let peers = [signers[0].peer_id(), signers[1].peer_id()];
    node.oplog()
        .create_topic_genesis(
            topic,
            actor_id_for(topic, peers[0]),
            TopicGenesis::new(Note::TYPE_ID, peers),
            &signers[0],
        )
        .expect("genesis admits");
    let handle = node.open_topic::<Note>(topic).expect("topic opens");
    for value in 0..2 {
        handle.publish(Note(value)).expect("publish admits");
    }
    let mut known: Vec<OpId> = node
        .storage()
        .list_op_ids(&topic)
        .unwrap()
        .into_iter()
        .collect();
    for arrival in arrivals.into_iter().take(MAX_ARRIVALS) {
        let before = node.storage().list_op_ids(&topic).unwrap();
        let (result, forged) = match arrival {
            Arrival::Raw(bytes) => (offer_raw(&node, &signers, &bytes), None),
            Arrival::Forged(forged) => {
                let op = forge(&forged, &signers, topic, &known);
                known.push(op.id);
                let data = SyncData {
                    topic_id: topic,
                    ops: vec![op.clone()],
                };
                let result = match forged.source {
                    Some(index) => {
                        let source = signers[usize::from(index) % signers.len()].peer_id();
                        node.receive_sync_outcome(source, data).map(drop)
                    }
                    None => node.oplog().receive_op(op.clone()),
                };
                (result, Some((forged, op)))
            }
        };
        let after = node.storage().list_op_ids(&topic).unwrap();
        if let Err(error) = &result
            && !matches!(error, Error::ReceiveCommitted { .. })
        {
            assert_eq!(before, after, "rejected arrival changed the topic: {error}");
        }
        if let Some((forged, op)) = forged
            && (forged.tamper != Tamper::Intact || forged.foreign)
        {
            assert!(!after.contains(&op.id), "invalid op admitted: {op:?}");
        }
        check_state(&node, topic);
    }
    let member = node
        .storage()
        .topic_state(&topic)
        .unwrap()
        .is_some_and(|state| state.members.contains(&peers[0]));
    if member {
        let record = handle.publish(Note(9)).expect("a member still publishes");
        let ids = node.storage().list_op_ids(&topic).unwrap();
        assert!(ids.contains(&record.meta.op_id));
        check_state(&node, topic);
    }
});

fn offer_raw(node: &Irokle, signers: &[Ed25519Signer; 3], bytes: &[u8]) -> irokle::Result<()> {
    if let Ok(SyncMessage::Data(data)) = irokle::net::decode_sync_message(bytes) {
        return node
            .receive_sync_outcome(signers[1].peer_id(), data)
            .map(drop);
    }
    match postcard::from_bytes::<Op>(bytes) {
        Ok(op) => node.oplog().receive_op(op),
        Err(_) => Ok(()),
    }
}

/// A signed op whose fields come from the input and point into `known` where possible.
fn forge(forged: &Forged, signers: &[Ed25519Signer; 3], topic: TopicId, known: &[OpId]) -> Op {
    let signer = &signers[usize::from(forged.signer) % signers.len()];
    let topic_id = if forged.foreign {
        TopicId::from_bytes([8; 32])
    } else {
        topic
    };
    let actor_id = match forged.actor % 3 {
        0 => actor_id_for(topic_id, signer.peer_id()),
        1 => actor_id_for(topic_id, signers[0].peer_id()),
        _ => irokle::ActorId::from_bytes([forged.actor; 32]),
    };
    let pick = |index: u8| {
        known
            .get(usize::from(index))
            .copied()
            .unwrap_or(OpId::from_bytes([index; 32]))
    };
    let actor_prev = forged.prev.map(pick);
    let mut deps: BTreeSet<OpId> = forged
        .deps
        .iter()
        .take(4)
        .map(|&index| pick(index))
        .collect();
    deps.extend(actor_prev);
    let peer = |index: u8| signers[usize::from(index) % signers.len()].peer_id();
    let payload = match &forged.payload {
        Payload::Event { typed, bytes } => TopicPayload::Event(EventEnvelope {
            type_id: if *typed { Note::TYPE_ID } else { "fuzz.other" }.to_owned(),
            payload: Bytes::copy_from_slice(&bytes[..bytes.len().min(64)]),
        }),
        Payload::Add(index) => TopicPayload::Control(TopicControl::AddPeer { peer: peer(*index) }),
        Payload::Remove(index) => {
            TopicPayload::Control(TopicControl::RemovePeer { peer: peer(*index) })
        }
        Payload::Genesis => TopicPayload::Genesis(TopicGenesis::new(
            Note::TYPE_ID,
            [signer.peer_id(), signers[0].peer_id()],
        )),
    };
    let body = OpBody {
        topic_id,
        author: signer.peer_id(),
        actor_id,
        actor_seq: u64::from(forged.seq),
        actor_prev,
        deps,
        generation: u64::from(forged.generation),
        payload,
    };
    let mut op = Op::sign(body, signer).expect("the author signs");
    match forged.tamper {
        Tamper::Intact => {}
        Tamper::Id => op.id = OpId::from_bytes([forged.seq; 32]),
        Tamper::Body => {
            op.signed.body.generation ^= 1;
            op.id = Op::derive_id(&op.signed).expect("id derives");
        }
    }
    op
}

/// Admitted ops are valid, causally complete, and agree with heads and the actor clock.
fn check_state(node: &Irokle, topic: TopicId) {
    let storage = node.storage();
    let ops = storage.list_ops(&topic).unwrap();
    let ids: BTreeSet<OpId> = ops.iter().map(|op| op.id).collect();
    let mut parents = BTreeSet::new();
    let mut tips = BTreeMap::new();
    for op in &ops {
        op.validate().expect("admitted op validates");
        let body = &op.signed.body;
        assert_eq!(body.topic_id, topic);
        assert!(body.deps.is_subset(&ids), "admitted op misses a dependency");
        parents.extend(body.deps.iter().copied());
        let tip = tips.entry(body.actor_id).or_insert(0);
        *tip = body.actor_seq.max(*tip);
    }
    let heads: BTreeSet<OpId> = ids.difference(&parents).copied().collect();
    assert_eq!(storage.heads(&topic).unwrap(), heads);
    if let Some(state) = storage.topic_state(&topic).unwrap() {
        assert_eq!(state.heads, heads);
        assert!(ids.contains(&state.genesis));
    }
    let clock = storage.actor_clock(&topic).unwrap();
    let clocked: BTreeMap<_, _> = clock.iter().map(|(actor, seq)| (*actor, *seq)).collect();
    assert_eq!(clocked, tips);
    // Buffered ops may wait for missing ids; admitted history has no holes.
    let waiting = storage.pending_missing_deps(&topic).unwrap();
    assert!(node.topic_unresolved(topic).unwrap().is_subset(&waiting));
}
