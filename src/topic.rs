use crate::{event::EventEnvelope, ids::PeerId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationPolicy {
    pub selected_peers: BTreeSet<PeerId>,
}

impl ReplicationPolicy {
    pub fn all() -> Self {
        Self {
            selected_peers: BTreeSet::new(),
        }
    }
}

impl Default for ReplicationPolicy {
    fn default() -> Self {
        Self::all()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicConfig {
    pub initial_peers: BTreeSet<PeerId>,
    pub replication_policy: ReplicationPolicy,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            initial_peers: BTreeSet::new(),
            replication_policy: ReplicationPolicy::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicInfo {
    pub topic_id: crate::TopicId,
    pub event_type_id: String,
    pub genesis: crate::OpId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerInfo {
    pub peer_id: PeerId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicGenesis {
    pub event_type_id: String,
    pub initial_peers: BTreeSet<PeerId>,
    pub replication_policy: ReplicationPolicy,
}

impl TopicGenesis {
    pub fn new(event_type_id: impl Into<String>, peers: impl IntoIterator<Item = PeerId>) -> Self {
        Self {
            event_type_id: event_type_id.into(),
            initial_peers: peers.into_iter().collect(),
            replication_policy: ReplicationPolicy::default(),
        }
    }

    pub fn allows(&self, peer: &PeerId) -> bool {
        self.initial_peers.contains(peer)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopicControl {
    AddPeer { peer: PeerId },
    RemovePeer { peer: PeerId },
    SetReplicationPolicy { policy: ReplicationPolicy },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopicPayload {
    Genesis(TopicGenesis),
    Event(EventEnvelope),
    Control(TopicControl),
}
