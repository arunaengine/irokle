// SPDX-License-Identifier: MIT OR Apache-2.0
//! Captured goals retained across slices and confirmed data pages. A changed
//! branch, staging session or unconfirmed prefix starts a fresh traversal.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::storage::RequestView;
use crate::{ActorClock, ActorId, Error, OpId, PeerId, Result, TopicId};

use super::plan::Frontier;
use super::{ActorWindow, SyncRequest, SyncSummary};

/// Plans one engine keeps at once.
pub(crate) const MAX_CONTINUATIONS: usize = 16;
/// Estimated bytes one kept plan may hold.
pub(super) const MAX_CONTINUATION_BYTES: usize = 4 * 1024 * 1024;
/// Reservations for captured immutable clocks, separate from traversal workspace.
const MAX_CLOCK_BYTES: usize = 128 * 1024 * 1024;
const CLOCK_POOL_BYTES: usize = 256 * 1024 * 1024;
/// A kept plan nobody resumed for this long gives its place to a new one.
const CONTINUATION_IDLE: Duration = Duration::from_secs(60);

/// A captured goal and its frontier, bound to branch and peer staging session.
pub(super) struct Continuation {
    genesis: OpId,
    epoch: u64,
    window: ActorWindow,
    named: Vec<(ActorId, u64)>,
    peer: Option<(bool, Option<u64>)>,
    pub(super) local: ActorClock,
    pub(super) goal: ActorClock,
    pub(super) frontier: Frontier,
    pub(super) clocks: ClockClaim,
    used: Instant,
}

impl Continuation {
    pub(super) fn new(
        (view, request): (&RequestView, &SyncRequest),
        local: ActorClock,
        goal: ActorClock,
        frontier: Frontier,
        clocks: ClockClaim,
        summary: Option<&SyncSummary>,
    ) -> Self {
        Self {
            genesis: view.genesis,
            epoch: view.epoch,
            window: request.window.clone(),
            named: named(request).into_iter().collect(),
            peer: summary.map(|summary| peer_scope(summary, view.genesis).0),
            local,
            goal,
            frontier,
            clocks,
            used: Instant::now(),
        }
    }

    /// Resume only with the same branch/session and confirmed positions.
    /// Without a full summary, require the original window and named positions.
    fn resumes(
        &self,
        view: &RequestView,
        request: &SyncRequest,
        summary: Option<&SyncSummary>,
    ) -> bool {
        if self.genesis != view.genesis || self.epoch != view.epoch {
            return false;
        }
        if let Some(peer) = self.peer {
            return summary.is_some_and(|summary| {
                let (current, clock) = peer_scope(summary, view.genesis);
                let empty = ActorClock::new();
                peer == current
                    && clock.unwrap_or(&empty).dominates(self.frontier.clock())
                    && request
                        .actor_range_hints
                        .iter()
                        .all(|hint| hint.from_exclusive >= self.frontier.covered(&hint.actor_id))
            });
        }
        let named = named(request);
        self.genesis == view.genesis
            && self.epoch == view.epoch
            && self.window == request.window
            && self
                .named
                .iter()
                .all(|(actor_id, from)| named.get(actor_id) == Some(from))
            && named.iter().all(|(actor_id, from)| {
                self.named
                    .binary_search_by_key(actor_id, |(actor, _)| *actor)
                    .is_ok()
                    || self.frontier.covered(actor_id) == *from
            })
    }

    /// A conservative estimate of the bytes this plan holds.
    pub(super) fn bytes(&self) -> usize {
        let window = self
            .window
            .behind
            .as_ref()
            .map_or(0, |behind| behind.bits.len());
        self.named.capacity() * size_of::<(ActorId, u64)>() + window + self.frontier.bytes()
    }
}

/// A reservation follows the plan through a started job and any kept frontier.
pub(super) struct ClockClaim {
    pool: Arc<AtomicUsize>,
    bytes: usize,
}

impl ClockClaim {
    pub(super) fn grow(&mut self, entries: usize) -> Result<()> {
        self.grow_roots([entries; 4])
    }

    pub(super) fn grow_roots(&mut self, entries: [usize; 4]) -> Result<()> {
        let bytes = entries.into_iter().fold(0_usize, |bytes, entries| {
            bytes.saturating_add(ActorClock::allocation_bound(entries))
        });
        if bytes > MAX_CLOCK_BYTES {
            return Err(Error::SyncCapacity(format!(
                "captured clocks need {bytes} reserved bytes, limit {MAX_CLOCK_BYTES}"
            )));
        }
        let added = bytes.saturating_sub(self.bytes);
        self.pool
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                held.checked_add(added)
                    .filter(|sum| *sum <= CLOCK_POOL_BYTES)
            })
            .map_err(|_| Error::SyncCapacity("captured clock pool is occupied".into()))?;
        self.bytes += added;
        Ok(())
    }
}

impl Drop for ClockClaim {
    fn drop(&mut self) {
        self.pool.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// The kept plans of one engine, keyed by peer and topic.
pub(super) struct Continuations {
    entries: BTreeMap<(PeerId, TopicId), Continuation>,
    capacity: usize,
    clocks: Arc<AtomicUsize>,
    records: Arc<AtomicUsize>,
}

impl Continuations {
    #[cfg(feature = "iroh")]
    pub(super) fn fork(&self) -> Self {
        Self {
            entries: BTreeMap::new(),
            capacity: 1,
            clocks: Arc::clone(&self.clocks),
            records: Arc::clone(&self.records),
        }
    }

    #[cfg(feature = "iroh")]
    pub(super) fn idle(&self) -> bool {
        self.entries.is_empty()
    }

    #[cfg(feature = "iroh")]
    pub(super) fn release(&mut self, key: (PeerId, TopicId)) {
        self.entries.remove(&key);
    }

    pub(super) fn new(capacity: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            capacity,
            clocks: Arc::default(),
            records: Arc::default(),
        }
    }

    pub(super) fn reserve_clocks(&self, entries: usize) -> Result<ClockClaim> {
        let mut claim = ClockClaim {
            pool: Arc::clone(&self.clocks),
            bytes: 0,
        };
        claim.grow(entries)?;
        Ok(claim)
    }

    pub(super) fn records(&self) -> super::records::Records {
        super::records::Records::new(Arc::clone(&self.records))
    }

    /// The plan kept for the requesting peer's topic, when `request` on `view`
    /// may go on from it. A kept plan the request does not continue is dropped.
    pub(super) fn take(
        &mut self,
        key: (PeerId, TopicId),
        view: &RequestView,
        request: &SyncRequest,
        summary: Option<&SyncSummary>,
    ) -> Option<Continuation> {
        let kept = self.entries.remove(&key)?;
        kept.resumes(view, request, summary).then_some(kept)
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
        self.entries
            .values()
            .map(Continuation::bytes)
            .sum::<usize>()
            + self.clocks.load(Ordering::Acquire)
            + self.records.load(Ordering::Acquire)
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

fn peer_scope(summary: &SyncSummary, genesis: OpId) -> ((bool, Option<u64>), Option<&ActorClock>) {
    if summary.genesis == Some(genesis) {
        return ((true, None), Some(&summary.actor_clock));
    }
    match summary
        .staged
        .as_ref()
        .filter(|staged| staged.genesis == genesis)
    {
        Some(staged) => ((false, Some(staged.session)), Some(&staged.clock)),
        None => ((false, None), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_claims_release() {
        let plans = Continuations::new(MAX_CONTINUATIONS);
        let mut claims = Vec::new();
        while let Ok(claim) = plans.reserve_clocks(65_536) {
            claims.push(claim);
            assert!(claims.len() < 16);
        }
        assert!(!claims.is_empty());
        assert!(plans.clocks.load(Ordering::Acquire) <= CLOCK_POOL_BYTES);
        let held = plans.clocks.load(Ordering::Acquire);
        assert!(plans.reserve_clocks(usize::MAX).is_err());
        assert_eq!(plans.clocks.load(Ordering::Acquire), held);
        claims.pop();
        claims.push(plans.reserve_clocks(65_536).unwrap());
        drop(claims);
        assert_eq!(plans.clocks.load(Ordering::Acquire), 0);
        let panicked = std::panic::catch_unwind(|| {
            let mut claim = plans.reserve_clocks(1).unwrap();
            claim.grow(257).unwrap();
            panic!("started job failed");
        });
        assert!(panicked.is_err());
        assert_eq!(plans.clocks.load(Ordering::Acquire), 0);
    }
}
