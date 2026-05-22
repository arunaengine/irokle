// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeSet, VecDeque};
use std::marker::PhantomData;

use crate::history::{DagQuery, HistoryOrder, limited};
use crate::oplog::{Oplog, topological, topological_subset};
use crate::reducer::EventRecord;
use crate::storage::{MemoryStorage, Storage};
use crate::{ActorId, Error, Event, Op, OpId, PeerId, Result, TopicControl, TopicId};

use super::{Irokle, PublishOptions};

#[derive(Clone)]
pub struct Topic<E: Event, S: Storage = MemoryStorage> {
    node: Irokle<S>,
    topic_id: TopicId,
    actor_id: ActorId,
    _event: PhantomData<E>,
}

impl<E: Event, S: Storage> Topic<E, S> {
    pub(super) fn new(node: Irokle<S>, topic_id: TopicId, actor_id: ActorId) -> Self {
        Self {
            node,
            topic_id,
            actor_id,
            _event: PhantomData,
        }
    }

    pub fn id(&self) -> TopicId {
        self.topic_id
    }

    pub fn publish(&self, event: E) -> Result<EventRecord<E>> {
        self.publish_with(
            event,
            PublishOptions {
                write_concern: self.node.config.default_write_concern.clone(),
            },
        )
    }

    pub fn publish_with(&self, event: E, options: PublishOptions) -> Result<EventRecord<E>> {
        self.node
            .publish_event(self.topic_id, self.actor_id, event, options)
    }

    pub fn add_peer(&self, peer: PeerId) -> Result<()> {
        self.node
            .publish_control(self.topic_id, self.actor_id, TopicControl::AddPeer { peer })
    }

    pub fn remove_peer(&self, peer: PeerId) -> Result<()> {
        self.node.publish_control(
            self.topic_id,
            self.actor_id,
            TopicControl::RemovePeer { peer },
        )
    }

    pub fn leave(&self) -> Result<()> {
        self.node.reject_topic(self.topic_id)
    }

    pub fn set_replication_policy(&self, policy: crate::ReplicationPolicy) -> Result<()> {
        self.node.publish_control(
            self.topic_id,
            self.actor_id,
            TopicControl::SetReplicationPolicy { policy },
        )
    }

    pub fn history(&self, order: HistoryOrder) -> Result<Vec<EventRecord<E>>> {
        self.node.topic_history(self.topic_id, order)
    }

    pub fn dag(&self, query: DagQuery<OpId>) -> Result<Vec<Op>> {
        self.node.topic_dag(self.topic_id, query)
    }

    pub fn heads(&self) -> Result<BTreeSet<OpId>> {
        self.node.topic_heads(self.topic_id)
    }

    pub fn peer_reached_op(&self, peer_id: PeerId, op_id: OpId) -> Result<bool> {
        self.node.peer_reached_op(peer_id, op_id)
    }

    pub fn peers_reached_op(&self, op_id: OpId) -> Result<Vec<PeerId>> {
        self.node.peers_reached_op(op_id)
    }

    #[cfg(feature = "iroh")]
    pub async fn sync_now(&self) -> std::io::Result<()> {
        self.node.sync_topic_now(self.topic_id).await
    }
}

#[derive(Clone)]
pub struct RawTopic<S: Storage = MemoryStorage> {
    pub(super) oplog: Oplog<S>,
    pub(super) topic_id: TopicId,
}

impl<S: Storage> RawTopic<S> {
    pub fn id(&self) -> TopicId {
        self.topic_id
    }

    pub fn history(&self) -> Result<Vec<Op>> {
        topological(self.oplog.storage(), &self.topic_id)
    }

    pub fn dag(&self, query: DagQuery<OpId>) -> Result<Vec<Op>> {
        dag_ops(self.oplog.storage(), self.topic_id, query)
    }

    pub fn heads(&self) -> Result<BTreeSet<OpId>> {
        self.oplog.storage().heads(&self.topic_id)
    }

    pub fn peer_reached_op(&self, peer_id: PeerId, op_id: OpId) -> Result<bool> {
        self.oplog.storage().peer_reached_op(&peer_id, &op_id)
    }

    pub fn peers_reached_op(&self, op_id: OpId) -> Result<Vec<PeerId>> {
        self.oplog.storage().peers_reached_op(&op_id)
    }
}

pub(super) fn dag_ops<S: Storage>(
    storage: &S,
    topic_id: TopicId,
    query: DagQuery<OpId>,
) -> Result<Vec<Op>> {
    if query.order == HistoryOrder::NewestFirst || !query.heads.is_empty() {
        let starts = if query.heads.is_empty() {
            storage.heads(&topic_id)?.into_iter().collect::<Vec<_>>()
        } else {
            query.heads
        };
        let mut seen = BTreeSet::new();
        let mut queue = starts
            .into_iter()
            .map(|head| (head, true))
            .collect::<VecDeque<_>>();
        let mut ids = Vec::new();
        while let Some((id, is_head)) = queue.pop_front() {
            if !seen.insert(id) {
                continue;
            }
            let meta = storage
                .get_meta(&id)?
                .ok_or_else(|| Error::Storage(format!("missing op meta for {id}")))?;
            if meta.topic_id != topic_id {
                return Err(Error::TopicMismatch);
            }
            if query.include_heads || !is_head {
                ids.push(id);
                if query.limit.is_some_and(|limit| ids.len() >= limit) {
                    break;
                }
            }
            for dep in meta.deps {
                queue.push_back((dep, false));
            }
        }
        if query.order == HistoryOrder::OldestFirst {
            let subset = ids.into_iter().collect::<BTreeSet<_>>();
            topological_subset(storage, &subset)
        } else {
            ids.into_iter()
                .map(|id| {
                    storage
                        .get_op(&id)?
                        .ok_or_else(|| Error::Storage(format!("missing op {id}")))
                })
                .collect()
        }
    } else {
        Ok(limited(topological(storage, &topic_id)?, query.limit))
    }
}
