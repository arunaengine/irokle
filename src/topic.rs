// SPDX-License-Identifier: MIT OR Apache-2.0
//! Topic configuration, replication policy, membership, and payload types.

use crate::{event::EventEnvelope, ids::PeerId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const DEFAULT_MAX_SYNC_PEERS: usize = 8;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationPolicy {
    /// Empty means all current topic members are eligible, still capped by `max_sync_peers`.
    pub selected_peers: BTreeSet<PeerId>,
    #[serde(default = "default_max_sync_peers")]
    pub max_sync_peers: usize,
}

impl ReplicationPolicy {
    pub fn all() -> Self {
        Self {
            selected_peers: BTreeSet::new(),
            max_sync_peers: DEFAULT_MAX_SYNC_PEERS,
        }
    }

    pub fn selected(peers: impl IntoIterator<Item = PeerId>) -> Self {
        Self {
            selected_peers: peers.into_iter().collect(),
            max_sync_peers: DEFAULT_MAX_SYNC_PEERS,
        }
    }

    pub fn with_max_sync_peers(mut self, max_sync_peers: usize) -> Self {
        self.max_sync_peers = max_sync_peers;
        self
    }
}

impl Default for ReplicationPolicy {
    fn default() -> Self {
        Self::all()
    }
}

fn default_max_sync_peers() -> usize {
    DEFAULT_MAX_SYNC_PEERS
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicConfig {
    pub initial_peers: BTreeSet<PeerId>,
    pub replication_policy: ReplicationPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicInfo {
    pub topic_id: crate::TopicId,
    pub event_type_id: String,
    pub genesis: crate::OpId,
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
