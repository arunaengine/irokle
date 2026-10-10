// SPDX-License-Identifier: MIT OR Apache-2.0
//! Replicas that write concurrently and receive every op in different orders converge.

#![no_main]

use std::collections::BTreeSet;

use arbitrary::{Arbitrary, Unstructured};
use irokle::sync::SyncData;
use irokle::{
    Ed25519Signer, Event, Irokle, Op, OpId, PeerId, Signer, Storage, Topic, TopicGenesis, TopicId,
    actor_id_for,
};
use libfuzzer_sys::fuzz_target;

const REPLICAS: usize = 3;
const MAX_ACTIONS: usize = 32;
const MAX_BATCH: usize = 8;

#[derive(Clone, irokle::Event, serde::Deserialize, serde::Serialize)]
#[irokle(type_id = "fuzz.note")]
struct Note(u8);

#[derive(Debug)]
struct Input {
    /// Seeds for each replica's final delivery order.
    orders: [u8; REPLICAS],
    actions: Vec<Action>,
}

impl<'a> Arbitrary<'a> for Input {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let orders = u.arbitrary()?;
        // Every remaining byte feeds actions, so even short inputs write and deliver.
        let mut actions = Vec::new();
        while !u.is_empty() && actions.len() < MAX_ACTIONS {
            actions.push(u.arbitrary()?);
        }
        Ok(Self { orders, actions })
    }
}

#[derive(Arbitrary, Debug)]
enum Action {
    Publish {
        replica: u8,
        count: u8,
    },
    /// Peers index the replicas first, then two outsiders.
    AddPeer {
        replica: u8,
        peer: u8,
    },
    RemovePeer {
        replica: u8,
        peer: u8,
    },
    Deliver {
        replica: u8,
        index: u8,
    },
    Batch {
        replica: u8,
        start: u8,
        count: u8,
        reverse: bool,
    },
    Sync {
        from: u8,
        to: u8,
    },
}

struct Replica {
    node: Irokle,
    topic: Topic<Note>,
}

fuzz_target!(|input: Input| {
    let topic_id = TopicId::from_bytes([7; 32]);
    let signers: Vec<Ed25519Signer> = (1..=REPLICAS as u8)
        .map(|seed| Ed25519Signer::from_bytes(&[seed; 32]))
        .collect();
    let peers: Vec<PeerId> = signers
        .iter()
        .map(Ed25519Signer::peer_id)
        .chain([PeerId::from_bytes([200; 32]), PeerId::from_bytes([201; 32])])
        .collect();
    let replicas = start(&signers, topic_id);
    let mut pool: Vec<(usize, Op)> = Vec::new();
    let mut seen = BTreeSet::new();
    collect(&replicas[0], 0, &mut pool, &mut seen);
    let mut removed = false;
    for action in input.actions {
        let pick = |index: u8| usize::from(index) % REPLICAS;
        match action {
            Action::Publish { replica, count } => {
                for value in 0..=count % 3 {
                    let _ = replicas[pick(replica)].topic.publish(Note(value));
                }
                collect(
                    &replicas[pick(replica)],
                    pick(replica),
                    &mut pool,
                    &mut seen,
                );
            }
            Action::AddPeer { replica, peer } => {
                let peer = peers[usize::from(peer) % peers.len()];
                let _ = replicas[pick(replica)].topic.add_peer(peer);
                collect(
                    &replicas[pick(replica)],
                    pick(replica),
                    &mut pool,
                    &mut seen,
                );
            }
            Action::RemovePeer { replica, peer } => {
                let index = usize::from(peer) % peers.len();
                let done = replicas[pick(replica)].topic.remove_peer(peers[index]);
                removed |= done.is_ok() && index < REPLICAS;
                collect(
                    &replicas[pick(replica)],
                    pick(replica),
                    &mut pool,
                    &mut seen,
                );
            }
            Action::Deliver { replica, index } => {
                let (author, op) = &pool[usize::from(index) % pool.len()];
                deliver(&replicas, *author, pick(replica), vec![op.clone()]);
            }
            Action::Batch {
                replica,
                start,
                count,
                reverse,
            } => {
                let mut batch: Vec<&(usize, Op)> = pool
                    .iter()
                    .cycle()
                    .skip(usize::from(start) % pool.len())
                    .take((usize::from(count) % MAX_BATCH + 1).min(pool.len()))
                    .collect();
                if reverse {
                    batch.reverse();
                }
                let author = batch[0].0;
                let ops = batch.iter().map(|(_, op)| op.clone()).collect();
                deliver(&replicas, author, pick(replica), ops);
            }
            Action::Sync { from, to } => {
                let (from, to) = (&replicas[pick(from)], &replicas[pick(to)]);
                if let Ok(summary) = to.node.sync_summary(topic_id)
                    && let Ok(data) = from.node.plan_sync_data(to.node.peer_id(), &summary)
                {
                    let _ = to.node.receive_sync_outcome(from.node.peer_id(), data);
                }
            }
        }
    }
    for (index, seed) in input.orders.into_iter().enumerate() {
        for position in shuffled(pool.len(), u64::from(seed)) {
            let (author, op) = &pool[position];
            deliver(&replicas, *author, index, vec![op.clone()]);
        }
    }
    check_converged(&replicas, topic_id, &pool, removed);
});

/// Replica 0 writes a fixed genesis; the others receive it from replica 0.
fn start(signers: &[Ed25519Signer], topic_id: TopicId) -> Vec<Replica> {
    let nodes: Vec<Irokle> = signers
        .iter()
        .map(|signer| {
            Irokle::builder()
                .with_signer(signer.clone())
                .build()
                .expect("node builds")
        })
        .collect();
    let creator = nodes[0].peer_id();
    let genesis = nodes[0]
        .oplog()
        .create_topic_genesis(
            topic_id,
            actor_id_for(topic_id, creator),
            TopicGenesis::new(Note::TYPE_ID, nodes.iter().map(Irokle::peer_id)),
            &signers[0],
        )
        .expect("genesis admits");
    for node in &nodes[1..] {
        let data = SyncData {
            topic_id,
            ops: vec![genesis.clone()],
        };
        node.receive_sync_outcome(creator, data)
            .expect("a member accepts the genesis");
    }
    nodes
        .into_iter()
        .map(|node| Replica {
            topic: node.open_topic::<Note>(topic_id).expect("topic opens"),
            node,
        })
        .collect()
}

/// Add the replica's not yet pooled ops, which it authored itself.
fn collect(
    replica: &Replica,
    index: usize,
    pool: &mut Vec<(usize, Op)>,
    seen: &mut BTreeSet<OpId>,
) {
    let history = replica
        .node
        .raw_topic(replica.topic.id())
        .unwrap()
        .history()
        .unwrap();
    for op in history {
        if seen.insert(op.id) {
            pool.push((index, op));
        }
    }
}

/// Offer ops to a replica as sync data from their author; failures are allowed here.
fn deliver(replicas: &[Replica], author: usize, target: usize, ops: Vec<Op>) {
    if author == target {
        return;
    }
    let data = SyncData {
        topic_id: replicas[target].topic.id(),
        ops,
    };
    let source = replicas[author].node.peer_id();
    let _ = replicas[target].node.receive_sync_outcome(source, data);
}

/// A seeded permutation of `0..len`.
fn shuffled(len: usize, mut seed: u64) -> Vec<usize> {
    let mut order: Vec<usize> = (0..len).collect();
    for index in (1..len).rev() {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        order.swap(index, (seed >> 33) as usize % (index + 1));
    }
    order
}

/// Replicas that still count themselves members hold the same topic, with nothing pending.
fn check_converged(replicas: &[Replica], topic_id: TopicId, pool: &[(usize, Op)], removed: bool) {
    let mut members = Vec::new();
    for (index, replica) in replicas.iter().enumerate() {
        let storage = replica.node.storage();
        assert!(
            storage.pending_missing_deps(&topic_id).unwrap().is_empty(),
            "replica {index} waits"
        );
        let state = storage.topic_state(&topic_id).unwrap();
        if state.is_some_and(|state| state.members.contains(&replica.node.peer_id())) {
            members.push(index);
        }
    }
    if !removed {
        assert_eq!(members.len(), REPLICAS, "no replica was removed");
    }
    let view = |index: usize| {
        let node = &replicas[index].node;
        let storage = node.storage();
        let state = storage
            .topic_state(&topic_id)
            .unwrap()
            .expect("a member holds the topic");
        let history: Vec<OpId> = node
            .raw_topic(topic_id)
            .unwrap()
            .history()
            .unwrap()
            .iter()
            .map(|op| op.id)
            .collect();
        (
            storage.list_op_ids(&topic_id).unwrap(),
            state.heads,
            state.members,
            state.replication_policy,
            storage.actor_clock(&topic_id).unwrap(),
            storage.topic_fingerprint(&topic_id).unwrap(),
            history,
            node.topic_unresolved(topic_id).unwrap(),
        )
    };
    let Some(&first) = members.first() else {
        return;
    };
    let expected = view(first);
    assert!(expected.7.is_empty(), "replica {first} has unresolved ops");
    for &index in &members[1..] {
        assert_eq!(
            view(index),
            expected,
            "replica {index} diverges from replica {first}"
        );
    }
    if !removed {
        for (_, op) in pool {
            assert!(
                expected.0.contains(&op.id),
                "an honest op was never admitted"
            );
        }
    }
}
