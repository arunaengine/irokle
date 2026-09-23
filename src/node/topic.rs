// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeSet, VecDeque};
use std::marker::PhantomData;

use crate::history::{DagQuery, HistoryCursor, HistoryOrder, limited, ordered};
use crate::oplog::{Oplog, topological, topological_ids};
use crate::reducer::EventRecord;
use crate::storage::{MemoryStorage, Storage};
use crate::{ActorClock, ActorId, Error, Event, Op, OpId, PeerId, Result, TopicControl, TopicId};

use crate::node::{Irokle, PublishOptions};

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

    /// Events not covered by `cursor`. A cursor from a replaced genesis fails with
    /// [`Error::StaleIncarnation`]; the caller then rebuilds from [`Self::history`].
    pub fn history_after(
        &self,
        cursor: &HistoryCursor,
        order: HistoryOrder,
    ) -> Result<Vec<EventRecord<E>>> {
        self.node.history_after_cursor(self.topic_id, cursor, order)
    }

    /// The current branch and actor clock, read together, for a later [`Self::history_after`].
    pub fn history_cursor(&self) -> Result<HistoryCursor> {
        self.node.topic_history_cursor(self.topic_id)
    }

    pub fn dag(&self, query: DagQuery<OpId>) -> Result<Vec<Op>> {
        self.node.topic_dag(self.topic_id, query)
    }

    pub fn heads(&self) -> Result<BTreeSet<OpId>> {
        self.node.topic_heads(self.topic_id)
    }

    pub fn actor_clock(&self) -> Result<ActorClock> {
        self.node.topic_actor_clock(self.topic_id)
    }

    pub fn observed_clock(&self) -> Result<ActorClock> {
        self.node.topic_observed_clock(self.topic_id)
    }

    pub fn peer_reached_op(&self, peer_id: PeerId, op_id: OpId) -> Result<bool> {
        if self
            .node
            .storage()
            .get_position(&op_id)?
            .is_some_and(|meta| meta.topic_id != self.topic_id)
        {
            return Err(Error::TopicMismatch);
        }
        self.node.peer_reached_op(peer_id, op_id)
    }

    pub fn peers_reached_op(&self, op_id: OpId) -> Result<Vec<PeerId>> {
        if self
            .node
            .storage()
            .get_position(&op_id)?
            .is_some_and(|meta| meta.topic_id != self.topic_id)
        {
            return Err(Error::TopicMismatch);
        }
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
        if self
            .oplog
            .storage()
            .get_position(&op_id)?
            .is_some_and(|meta| meta.topic_id != self.topic_id)
        {
            return Err(Error::TopicMismatch);
        }
        self.oplog.storage().peer_reached_op(&peer_id, &op_id)
    }

    pub fn peers_reached_op(&self, op_id: OpId) -> Result<Vec<PeerId>> {
        if self
            .oplog
            .storage()
            .get_position(&op_id)?
            .is_some_and(|meta| meta.topic_id != self.topic_id)
        {
            return Err(Error::TopicMismatch);
        }
        self.oplog.storage().peers_reached_op(&op_id)
    }
}

pub(super) fn dag_ops<S: Storage>(
    storage: &S,
    topic_id: TopicId,
    query: DagQuery<OpId>,
) -> Result<Vec<Op>> {
    if query.limit == Some(0) {
        return Ok(Vec::new());
    }
    let mut excluded = BTreeSet::new();
    let ids = if query.order == HistoryOrder::NewestFirst
        || !query.heads.is_empty()
        || !query.include_heads
    {
        let starts = if query.heads.is_empty() {
            storage.heads(&topic_id)?.into_iter().collect::<Vec<_>>()
        } else {
            query.heads
        };
        excluded = if query.include_heads {
            BTreeSet::new()
        } else {
            starts.iter().copied().collect()
        };
        let mut seen = BTreeSet::new();
        let mut queue = starts.into_iter().collect::<VecDeque<_>>();
        while let Some(id) = queue.pop_front() {
            if !seen.insert(id) {
                continue;
            }
            // A dependency still awaiting repair simply ends this branch of the
            // walk; the rest of the DAG stays queryable.
            let Some(meta) = storage.get_position(&id)? else {
                continue;
            };
            if meta.topic_id != topic_id {
                return Err(Error::TopicMismatch);
            }
            for dep in meta.deps {
                queue.push_back(dep);
            }
        }
        // The walk runs unbounded: `query.limit` counts usable results, so
        // applying it here would let blocked ids spend the caller's budget and
        // return a short page over history that is still reachable.
        seen
    } else {
        storage.list_op_ids(&topic_id)?
    };
    let mut ids = topological_ids(storage, &ids)?;
    ids.retain(|id| !excluded.contains(id));
    let ids = limited(ordered(ids, query.order), query.limit);
    ids.into_iter()
        .map(|id| {
            storage
                .get_op(&id)?
                .ok_or_else(|| Error::Storage(format!("missing op {id}")))
        })
        .collect()
}
