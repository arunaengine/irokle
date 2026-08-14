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
