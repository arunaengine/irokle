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

pub(crate) fn node(seed: u8) -> Irokle {
    Irokle::new(NodeConfig {
        signer: Ed25519Signer::from_bytes(&[seed; 32]),
        default_write_concern: WriteConcern::Local,
        ..NodeConfig::default()
    })
    .unwrap()
}
