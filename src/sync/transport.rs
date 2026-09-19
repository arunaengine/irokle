// SPDX-License-Identifier: MIT OR Apache-2.0
//! The sync engine calls that only the Iroh transport makes.

use crate::storage::{SnapshotRead, Storage};
use crate::sync::request::request_ranges;
use crate::sync::{
    ActorRangeHint, ActorWindow, PageBudget, PlannedPage, RequestKnowledge, SyncEngine,
    SyncRequest, SyncSummary,
};
use crate::{ActorClock, OpId, PeerId, Result, TopicId};
use std::sync::{Arc, Mutex};

impl<S: Storage> SyncEngine<S> {
    pub(crate) fn bound_genesis(mut self, topic: TopicId, genesis: OpId) -> Self {
        self.oplog = self.oplog.bound_genesis(topic, genesis);
        self
    }

    pub(crate) fn session_plan(&self) -> Self {
        let mut engine = self.clone();
        engine.continuations = Arc::new(Mutex::new(self.continuations().fork()));
        engine
    }

    pub(crate) fn plan_idle(&self) -> bool {
        self.continuations().idle()
    }

    pub(crate) fn release_plan(&self, peer: PeerId, topic: TopicId) {
        self.continuations().release((peer, topic));
    }

    /// The ranges and window of a request from `local` toward `remote` that
    /// continues from `knowledge` within this engine's item limit.
    pub(crate) fn request_ranges(
        &self,
        local: &ActorClock,
        remote: &ActorClock,
        knowledge: &RequestKnowledge,
    ) -> (Vec<ActorRangeHint>, ActorWindow) {
        request_ranges(local, remote, self.request_items, knowledge)
    }

    /// [`Self::summary`] read from a snapshot the caller already holds, where the
    /// integrity scan takes at most one step; an unfinished scan digests as incomplete.
    pub(crate) fn summary_in(
        &self,
        read: &dyn SnapshotRead,
        topic_id: TopicId,
    ) -> Result<SyncSummary> {
        let Some(view) = read.topic_view(&topic_id, None)? else {
            return Self::unknown_summary(topic_id);
        };
        let integrity = self.oplog.integrity_in(read, &view)?;
        Self::summary_for(view, &integrity)
    }

    /// [`Self::response_page`] over a snapshot the caller already holds.
    pub(crate) fn response_in(
        &self,
        read: &dyn SnapshotRead,
        peer_id: PeerId,
        request: &SyncRequest,
        budget: PageBudget,
    ) -> Result<PlannedPage> {
        self.response_known(read, peer_id, request, budget, None)
    }
}
