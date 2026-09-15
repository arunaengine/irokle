// SPDX-License-Identifier: MIT OR Apache-2.0
//! Page plans kept across work slices. A plan that ends its slice before it can
//! send anything keeps its frontier here, and the same peer's next request for
//! the same positions goes on from it instead of walking the same prefix again.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::storage::RequestView;
use crate::{ActorClock, ActorId, Error, OpId, PeerId, Result, TopicId};

use super::plan::Frontier;
use super::{ActorWindow, SyncRequest};

/// Plans one engine keeps at once.
pub(crate) const MAX_CONTINUATIONS: usize = 16;
/// Estimated bytes one kept plan may hold.
pub(super) const MAX_CONTINUATION_BYTES: usize = 4 * 1024 * 1024;
/// A kept plan nobody resumed for this long gives its place to a new one.
const CONTINUATION_IDLE: Duration = Duration::from_secs(60);

/// A kept plan: the frontier, the clocks it planned against, and the branch,
/// window and named positions of the request it answers.
pub(super) struct Continuation {
    genesis: OpId,
    epoch: u64,
    window: ActorWindow,
    named: BTreeMap<ActorId, u64>,
    pub(super) local: ActorClock,
    pub(super) goal: ActorClock,
    pub(super) frontier: Frontier,
    used: Instant,
}

impl Continuation {
    pub(super) fn new(
        (view, request): (&RequestView, &SyncRequest),
        local: ActorClock,
        goal: ActorClock,
        frontier: Frontier,
    ) -> Self {
        Self {
            genesis: view.genesis,
            epoch: view.epoch,
            window: request.window.clone(),
            named: named(request),
            local,
            goal,
            frontier,
            used: Instant::now(),
        }
    }

    /// Whether `request` on `view` may go on from this plan: the same branch,
    /// destructive epoch and window; every actor the plan's request named named
    /// again at the same position; and any actor named since starting where the
    /// plan takes the requester to be. Appends that grew the goal are later work.
    fn resumes(&self, view: &RequestView, request: &SyncRequest) -> bool {
        let named = named(request);
        self.genesis == view.genesis
            && self.epoch == view.epoch
            && self.window == request.window
            && self
                .named
                .iter()
                .all(|(actor_id, from)| named.get(actor_id) == Some(from))
            && named.iter().all(|(actor_id, from)| {
                self.named.contains_key(actor_id) || self.frontier.covered(actor_id) == *from
            })
    }

    /// A conservative estimate of the bytes this plan holds.
    pub(super) fn bytes(&self) -> usize {
        let clock = |clock: &ActorClock| clock.iter().count() * 48;
        let window = self
            .window
            .behind
            .as_ref()
            .map_or(0, |behind| behind.bits.len());
        clock(&self.local)
            + clock(&self.goal)
            + self.named.len() * 48
            + window
            + self.frontier.bytes()
    }
}

/// The kept plans of one engine, keyed by peer and topic.
pub(super) struct Continuations {
    entries: BTreeMap<(PeerId, TopicId), Continuation>,
    capacity: usize,
}

impl Continuations {
    #[cfg(feature = "iroh")]
    pub(super) fn release(&mut self, key: (PeerId, TopicId)) {
        self.entries.remove(&key);
    }

    pub(super) fn new(capacity: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            capacity,
        }
    }

    /// The plan kept for the requesting peer's topic, when `request` on `view`
    /// may go on from it. A kept plan the request does not continue is dropped.
    pub(super) fn take(
        &mut self,
        key: (PeerId, TopicId),
        view: &RequestView,
        request: &SyncRequest,
    ) -> Option<Continuation> {
        let kept = self.entries.remove(&key)?;
        kept.resumes(view, request).then_some(kept)
    }

    /// Keep `continuation` for `key`, making room from plans idle too long.
    /// Refused with a capacity error when it is too large or every place holds
    /// a plan still in use, since an empty slice that cannot be kept would
    /// repeat forever.
    pub(super) fn keep(
        &mut self,
        key: (PeerId, TopicId),
        continuation: Continuation,
    ) -> Result<()> {
        let bytes = continuation.bytes();
        if bytes > MAX_CONTINUATION_BYTES {
            return Err(Error::SyncCapacity(format!(
                "retained plan needs {bytes} bytes, limit {MAX_CONTINUATION_BYTES}"
            )));
        }
        self.entries
            .retain(|_, kept| kept.used.elapsed() < CONTINUATION_IDLE);
        if !self.entries.contains_key(&key) && self.entries.len() >= self.capacity {
            return Err(Error::SyncCapacity(format!(
                "all {} retained plan slots are occupied",
                self.capacity
            )));
        }
        self.entries.insert(key, continuation);
        Ok(())
    }

    /// Estimated bytes all kept plans hold.
    #[cfg(test)]
    pub(super) fn bytes(&self) -> usize {
        self.entries.values().map(Continuation::bytes).sum()
    }
}

/// The requester's position of every actor `request` names.
fn named(request: &SyncRequest) -> BTreeMap<ActorId, u64> {
    request
        .actor_range_hints
        .iter()
        .map(|hint| (hint.actor_id, hint.from_exclusive))
        .collect()
}
