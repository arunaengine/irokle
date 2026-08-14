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

pub(crate) fn node(seed: u8) -> Irokle {
    Irokle::new(NodeConfig {
        signer: Ed25519Signer::from_bytes(&[seed; 32]),
        default_write_concern: WriteConcern::Local,
        ..NodeConfig::default()
    })
    .unwrap()
}
