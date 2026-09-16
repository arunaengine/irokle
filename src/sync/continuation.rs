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
use super::slice::MAX_CONTINUATION_BYTES;
use super::{ActorWindow, SyncRequest, SyncSummary};

/// Plans one engine keeps at once.
pub(crate) const MAX_CONTINUATIONS: usize = 16;
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
    pub(super) clocks: Arc<ClockClaim>,
    used: Instant,
}

impl Continuation {
    pub(super) fn new(
        (view, request): (&RequestView, &SyncRequest),
        local: ActorClock,
        goal: ActorClock,
        frontier: Frontier,
        clocks: Arc<ClockClaim>,
        summary: Option<&SyncSummary>,
    ) -> Self {
        Self {
            genesis: view.genesis,
            epoch: view.epoch,
            window: if summary.is_none() {
                request.window.clone()
            } else {
                ActorWindow::default()
            },
            named: if summary.is_none() {
                named(request).into_iter().collect()
            } else {
                Vec::new()
            },
            peer: summary.map(|summary| peer_scope(summary, view.genesis).0),
            local,
            goal,
            frontier,
            clocks,
            used: Instant::now(),
        }
    }

    /// Resume only with the same branch/session and confirmed positions.
    /// Without a full summary, require the original window and confirmed named prefixes.
    fn resumes(
        &mut self,
        view: &RequestView,
        request: &SyncRequest,
        summary: Option<&SyncSummary>,
    ) -> bool {
        if self.genesis != view.genesis
            || self.epoch != view.epoch
            || self.peer.is_some() != summary.is_some()
        {
            return false;
        }
        let empty = ActorClock::new();
        let floor = self.frontier.offer_floor();
        let (confirmed, full, held) = if let Some(peer) = self.peer {
            let Some(summary) = summary else {
                return false;
            };
            let (current, clock) = peer_scope(summary, view.genesis);
            let clock = clock.unwrap_or(&empty);
            let confirms = |required: &ActorClock| {
                peer == current
                    && clock.dominates(required)
                    && request
                        .actor_range_hints
                        .iter()
                        .all(|hint| hint.from_exclusive >= required.get(&hint.actor_id))
            };
            (
                confirms(floor),
                confirms(self.frontier.clock()),
                Some(clock),
            )
        } else {
            let named = named(request);
            let confirms = |required: &ActorClock| {
                self.named.iter().all(|(actor, from)| {
                    named.get(actor).map_or_else(
                        || self.window.holds(actor),
                        |current| current >= from && *current >= required.get(actor),
                    )
                })
            };
            let compatible = self.window == request.window
                && named.iter().all(|(actor, from)| {
                    self.named
                        .binary_search_by_key(actor, |(actor, _)| *actor)
                        .is_ok()
                        || self.frontier.covered(actor) == *from
                        || (!self.window.holds(actor) && *from <= self.local.get(actor))
                });
            let full = confirms(self.frontier.clock())
                && request
                    .actor_range_hints
                    .iter()
                    .all(|hint| hint.from_exclusive >= self.frontier.covered(&hint.actor_id));
            (compatible && confirms(floor), full, None)
        };
        if !confirmed
            || !self
                .frontier
                .repair
                .as_mut()
                .is_none_or(|repair| repair.confirms(request))
        {
            return false;
        }
        if self.peer.is_none() {
            let named = named(request);
            for (actor, _) in &self.named {
                if !named.contains_key(actor) && self.window.holds(actor) {
                    self.frontier.discover(*actor, self.local.get(actor));
                }
            }
            for hint in &request.actor_range_hints {
                if !self.window.holds(&hint.actor_id)
                    && self
                        .named
                        .binary_search_by_key(&hint.actor_id, |(actor, _)| *actor)
                        .is_err()
                {
                    self.frontier.discover(hint.actor_id, hint.from_exclusive);
                }
            }
        }
        let full = full
            && !self
                .frontier
                .repair
                .as_ref()
                .is_some_and(super::repair::Repair::offered);
        self.frontier.confirm_offer(request, held, full);
        true
    }

    /// A conservative estimate of the bytes this plan holds.
    pub(super) fn bytes(&self) -> usize {
        let window = self.window.behind.as_ref().map_or(0, |behind| {
            super::space::vector_bytes::<u8>(behind.bits.capacity())
        });
        super::space::vector_bytes::<(ActorId, u64)>(self.named.capacity())
            + window
            + self.frontier.bytes()
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
        if !super::space::reserve_bytes(&self.pool, added, CLOCK_POOL_BYTES) {
            return Err(Error::SyncCapacity(
                "captured clock pool is occupied".into(),
            ));
        }
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
    records: Arc<super::records::RecordPool>,
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
        if self.entries.is_empty() {
            self.entries = BTreeMap::new();
        }
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

    pub(super) fn captured(
        &self,
        key: (PeerId, TopicId),
    ) -> Option<(ActorClock, usize, Arc<ClockClaim>)> {
        self.entries.get(&key).map(|kept| {
            let clocks = kept
                .local
                .len()
                .saturating_add(kept.goal.len())
                .saturating_add(kept.frontier.clock().len())
                .saturating_add(kept.frontier.offer_floor().len());
            let work = clocks
                .saturating_mul(2)
                .saturating_add(kept.named.len().saturating_mul(8))
                .saturating_add(kept.frontier.offer_count().saturating_mul(8));
            (kept.local.clone(), work, Arc::clone(&kept.clocks))
        })
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
        let mut kept = self.entries.remove(&key)?;
        if self.entries.is_empty() {
            self.entries = BTreeMap::new();
        }
        kept.resumes(view, request, summary).then_some(kept)
    }

    /// Idle plans expire; a data-producing plan may yield its slot to another goal.
    /// Empty advancing prefixes stay protected, or retries could repeat forever.
    /// Fair progress requires finite goals and service for admitted requests.
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
            let evictable = self
                .entries
                .iter()
                .filter(|(_, kept)| kept.frontier.evictable)
                .min_by_key(|(_, kept)| kept.used)
                .map(|(key, _)| *key);
            if let Some(evictable) = evictable {
                self.entries.remove(&evictable);
            } else {
                return Err(Error::SyncCapacity(format!(
                    "all {} plan slots protect unfinished slices; resume them or retry after a data page",
                    self.capacity
                )));
            }
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
            + if self.entries.is_empty() {
                0
            } else {
                super::space::tree_bytes::<(PeerId, TopicId), Continuation>(self.entries.len())
            }
            + self.clocks.load(Ordering::Acquire)
            + self.records.bytes()
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
    fn captured_lease_survives() {
        let mut plans = Continuations::new(1);
        let mut clock = ActorClock::new();
        clock.observe(ActorId::from_bytes([1; 32]), 1);
        let mut allocation = None;
        clock
            .visit_allocations(|node| {
                allocation = Some(node);
                Ok(false)
            })
            .unwrap();
        let allocation = allocation.unwrap();
        let key = (PeerId::from_bytes([2; 32]), TopicId::from_bytes([3; 32]));
        let view = RequestView {
            genesis: OpId::from_bytes([4; 32]),
            epoch: 1,
            member: true,
            clock: clock.clone(),
        };
        let request = SyncRequest {
            topic_id: key.1,
            known: Default::default(),
            wants: Default::default(),
            actor_range_hints: Vec::new(),
            genesis: Some(view.genesis),
            credit: Default::default(),
            window: Default::default(),
        };
        let frontier = Frontier::new(
            &clock,
            &super::super::ActorScope::whole(),
            1,
            plans.records(),
        );
        let claim = Arc::new(plans.reserve_clocks(1).unwrap());
        plans
            .keep(
                key,
                Continuation::new(
                    (&view, &request),
                    clock.clone(),
                    clock.clone(),
                    frontier,
                    claim,
                    None,
                ),
            )
            .unwrap();
        let pool = Arc::clone(&plans.clocks);
        let held = pool.load(Ordering::Acquire);
        assert!(held > 0);
        drop((view, clock));
        let plans = Arc::new(std::sync::Mutex::new(plans));
        let captured = plans.lock().unwrap().captured(key).unwrap();
        let writer = Arc::clone(&plans);
        std::thread::spawn(move || writer.lock().unwrap().entries.clear())
            .join()
            .unwrap();
        assert!(
            allocation.alive(),
            "the caller still owns its captured root"
        );
        assert_eq!(pool.load(Ordering::Acquire), held);
        let (root, _, lease) = captured;
        drop(root);
        assert!(!allocation.alive());
        assert_eq!(pool.load(Ordering::Acquire), held);
        drop(lease);
        assert_eq!(pool.load(Ordering::Acquire), 0);
    }

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
