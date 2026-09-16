// SPDX-License-Identifier: MIT OR Apache-2.0
//! Bounded forward page planning over one read context: actor ranges merged by
//! generation, with waiting heads suspended outside the active set.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::clock::ClockCursor;
use crate::storage::{DependencyCursor, OpHeader, SnapshotRead, Storage};
use crate::{ActorClock, ActorId, Error, Op, OpId, Result, TopicId};

use super::request::need;
use super::{
    ActorScope, MAX_PAGE_BYTES, MAX_PAGE_MISSING, PageBudget, PlannedPage, RangeHead, SyncEngine,
};

/// Storage reads one page plan makes before it ends its work slice.
pub(super) const MAX_PAGE_VISITS: usize = 65_536;

/// Work page plans performed: storage reads, dependency edges examined, slices
/// that ended on their read budget, and plans resumed from a kept frontier.
#[derive(Debug, Default)]
pub(crate) struct PageWork {
    visits: AtomicU64,
    actors: AtomicU64,
    tips: AtomicU64,
    edges: AtomicU64,
    ended: AtomicU64,
    resumed: AtomicU64,
    decoded: AtomicU64,
    preparation: AtomicU64,
    auth_reads: AtomicU64,
    captured: AtomicU64,
}

/// A copy of [`PageWork`] with the bytes kept plans hold at one moment.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PageWorkSnapshot {
    pub(crate) visits: u64,
    pub(crate) actors: u64,
    pub(crate) tips: u64,
    pub(crate) edges: u64,
    pub(crate) ended: u64,
    pub(crate) resumed: u64,
    /// Encoded upper-bound bytes admitted for decoding, including failed loads.
    pub(crate) decoded: u64,
    pub(crate) preparation: u64,
    pub(crate) auth_reads: u64,
    pub(crate) captured: u64,
    pub(crate) kept_bytes: u64,
}

impl PageWork {
    pub(super) fn tip(&self) {
        self.tips.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn resumed(&self) {
        self.resumed.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self, kept_bytes: usize) -> PageWorkSnapshot {
        PageWorkSnapshot {
            visits: self.visits.load(Ordering::Relaxed),
            actors: self.actors.load(Ordering::Relaxed),
            tips: self.tips.load(Ordering::Relaxed),
            edges: self.edges.load(Ordering::Relaxed),
            ended: self.ended.load(Ordering::Relaxed),
            resumed: self.resumed.load(Ordering::Relaxed),
            decoded: self.decoded.load(Ordering::Relaxed),
            preparation: self.preparation.load(Ordering::Relaxed),
            auth_reads: self.auth_reads.load(Ordering::Relaxed),
            captured: self.captured.load(Ordering::Relaxed),
            kept_bytes: kept_bytes as u64,
        }
    }
}

pub(super) struct Slice {
    work: std::sync::Arc<PageWork>,
    limit: usize,
    visits: usize,
    scanned: usize,
    edges: usize,
    workspace: usize,
    decoded: usize,
    decode_limit: usize,
    preparation: usize,
    input_units: usize,
    prep_bytes: usize,
    auth_reads: usize,
    captured: usize,
}

impl Slice {
    pub(super) fn new(
        work: std::sync::Arc<PageWork>,
        limit: usize,
        workspace: usize,
    ) -> Result<Self> {
        let mut slice = Self {
            work,
            limit,
            visits: 0,
            scanned: 0,
            edges: 0,
            workspace: 0,
            decoded: 0,
            decode_limit: MAX_PAGE_BYTES,
            preparation: 0,
            input_units: 0,
            prep_bytes: 0,
            auth_reads: 0,
            captured: 0,
        };
        slice.reserve(workspace)?;
        Ok(slice)
    }

    pub(super) fn read(&mut self) -> bool {
        if self.visits + self.scanned >= self.limit {
            return false;
        }
        self.visits += 1;
        self.work.visits.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub(super) fn actor(&mut self) -> bool {
        if self.visits + self.scanned >= self.limit {
            return false;
        }
        self.scanned += 1;
        self.work.actors.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub(super) fn edge(&mut self) -> bool {
        if self.edges >= self.limit {
            return false;
        }
        self.edges += 1;
        self.work.edges.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub(super) fn exhausted(&self) -> bool {
        self.visits + self.scanned >= self.limit || self.edges >= self.limit
    }

    pub(super) fn reserve(&mut self, bytes: usize) -> Result<()> {
        let required = self.workspace.saturating_add(bytes);
        if required > super::continuation::MAX_CONTINUATION_BYTES {
            return Err(Error::SyncCapacity(format!(
                "planner workspace needs {required} bytes; reduce the request's wants or actor window"
            )));
        }
        self.workspace = required;
        Ok(())
    }

    pub(super) fn prepare(&mut self, units: usize, bytes: usize) -> Result<()> {
        let units = self.preparation.saturating_add(units);
        let bytes = self.prep_bytes.saturating_add(bytes);
        if units > 16 * MAX_PAGE_VISITS || bytes > 128 * 1024 * 1024 {
            return Err(Error::SyncCapacity(
                "request preparation exceeds its entry or memory envelope; reduce the actor window or wants".into(),
            ));
        }
        self.work
            .preparation
            .fetch_add((units - self.preparation) as u64, Ordering::Relaxed);
        self.preparation = units;
        self.prep_bytes = bytes;
        Ok(())
    }

    pub(super) fn prepare_input(&mut self, units: usize, bytes: usize) -> Result<()> {
        let units = self.input_units.saturating_add(units);
        let bytes = self.prep_bytes.saturating_add(bytes);
        let limit = 16 * super::MAX_REQUEST_ITEMS
            + 8 * (super::MAX_PAGE_OPS + MAX_PAGE_MISSING);
        if units > limit || bytes > 128 * 1024 * 1024 {
            return Err(Error::SyncCapacity(
                "request input exceeds its work or memory limit; reduce wants, hints or filter bytes".into(),
            ));
        }
        self.work.preparation.fetch_add((units - self.input_units) as u64, Ordering::Relaxed);
        self.input_units = units;
        self.prep_bytes = bytes;
        Ok(())
    }

    pub(super) fn auth_read(&mut self) -> Result<()> {
        if self.auth_reads >= 16 {
            return Err(Error::SyncCapacity(
                "request authorization exceeds its metadata envelope".into(),
            ));
        }
        self.auth_reads += 1;
        self.work.auth_reads.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn capture(&mut self, entries: usize, raw: usize, bytes: usize) -> Result<()> {
        if raw > MAX_PAGE_BYTES.saturating_sub(self.captured) {
            return Err(Error::SyncCapacity(
                "request snapshot exceeds its encoded metadata envelope".into(),
            ));
        }
        self.prepare(entries, bytes)?;
        self.captured += raw;
        self.work.captured.fetch_add(raw as u64, Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn decode(&mut self, bytes: usize) -> Result<bool> {
        if bytes > self.decode_limit {
            return Err(Error::SyncCapacity(format!(
                "operation decoding needs {bytes} bytes, slice capacity {}",
                self.decode_limit,
            )));
        }
        if bytes > self.decode_limit - self.decoded {
            return Ok(false);
        }
        self.decoded += bytes;
        self.work.decoded.fetch_add(bytes as u64, Ordering::Relaxed);
        Ok(true)
    }

    #[cfg(test)]
    pub(super) fn with_decode_limit(mut self, limit: usize) -> Self {
        self.decode_limit = limit;
        self
    }
}

/// A planned page, the actors whose positions it needed by the lowest
/// generation needing each, and the frontier of a slice that ended empty.
pub(super) type PlannedSlice = (PlannedPage, BTreeMap<ActorId, u64>, Option<Frontier>);

#[derive(Default)]
struct DependencyScan {
    after: Option<OpId>,
    needed: BTreeMap<ActorId, (u64, u64)>,
    checked: Option<ActorId>,
    revision: u64,
    ancestry: Vec<Ancestor>,
}

struct Ancestor {
    id: OpId,
    actor: ActorId,
    seq: u64,
    cursor: DependencyCursor,
    pending: Option<Dependency>,
}

struct Dependency {
    id: OpId,
    header: Option<OpHeader>,
}

/// How far one actor of a page plan got.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActorState {
    /// Its next head is in the active set or waiting for a free slot.
    Active,
    /// Its next head waits for another actor's position.
    Suspended,
    /// It reached this limit; a dependency may still raise it.
    Reached(u64),
    /// A missing record or an unsent want stopped it.
    Blocked,
}

/// Retained traversal state, with a separate reservation for operation records.
pub(super) struct Frontier {
    pub(super) repair: Option<super::repair::Repair>,
    pub(super) evictable: bool,
    revision: u64,
    offered: Vec<Offered>,
    offer_positions: BTreeSet<ActorId>,
    offer_missing: BTreeSet<OpId>,
    offer_floor: Option<ActorClock>,
    replay: usize,
    pub(super) replaying: bool,
    fresh: bool,
    pending: VecDeque<HeadScan>,
    updates: VecDeque<Update>,
    active: BinaryHeap<Reverse<RangeHead>>,
    selecting: Option<BinaryHeap<RangeHead>>,
    remainder: bool,
    deferred: ClockCursor,
    suspended: BTreeMap<ActorId, Vec<(u64, RangeHead)>>,
    resumable: VecDeque<RangeHead>,
    states: BTreeMap<ActorId, ActorState>,
    covered: ActorClock,
    blocked: BTreeSet<OpId>,
    missing: BTreeSet<OpId>,
    positions: BTreeMap<ActorId, u64>,
    checked: BTreeMap<OpId, DependencyScan>,
    pub(super) records: super::records::Records,
}

#[derive(Clone, Copy)]
struct HeadScan {
    actor: ActorId,
    after: u64,
    limit: u64,
    next: Option<(u64, OpId)>,
}

#[derive(Clone, Copy)]
enum Update {
    Wake {
        actor: ActorId,
        seq: u64,
        index: usize,
    },
    Raise {
        actor: ActorId,
        limit: u64,
        index: usize,
        raised: Option<u64>,
    },
    Finish {
        actor: ActorId,
        raised: Option<u64>,
        index: usize,
    },
    Stop(ActorId),
}

#[derive(Clone, Copy)]
struct Offered {
    id: OpId,
    actor: ActorId,
    seq: u64,
    held: bool,
}

impl Frontier {
    pub(super) fn new(
        peer: &ActorClock,
        scope: &ActorScope<'_>,
        window: usize,
        records: super::records::Records,
    ) -> Self {
        Self {
            repair: None,
            evictable: false,
            revision: 0,
            offered: Vec::new(),
            offer_positions: BTreeSet::new(),
            offer_missing: BTreeSet::new(),
            offer_floor: None,
            replay: 0,
            replaying: false,
            fresh: true,
            pending: VecDeque::new(),
            updates: VecDeque::new(),
            active: BinaryHeap::new(),
            selecting: (scope.informed() && scope.named.len() > window).then(BinaryHeap::new),
            remainder: false,
            deferred: ClockCursor::default(),
            suspended: BTreeMap::new(),
            resumable: VecDeque::new(),
            states: BTreeMap::new(),
            covered: peer.clone(),
            blocked: BTreeSet::new(),
            missing: BTreeSet::new(),
            positions: BTreeMap::new(),
            checked: BTreeMap::new(),
            records,
        }
    }

    pub(super) fn discover(&mut self, actor: ActorId, seq: u64) {
        self.covered.set(actor, seq);
        if let Some(floor) = &mut self.offer_floor {
            floor.set(actor, seq);
        }
        self.positions.remove(&actor);
        self.revision = self.revision.wrapping_add(1);
    }

    pub(super) fn offer_count(&self) -> usize {
        self.offered.len()
    }

    pub(super) fn scan_entries(&self) -> usize {
        self.suspended.len().saturating_add(self.checked.len())
    }

    pub(super) fn offer_floor(&self) -> &ActorClock {
        self.offer_floor.as_ref().unwrap_or(&self.covered)
    }

    pub(super) fn capture_offer(&mut self, page: &PlannedPage, floor: ActorClock) -> Result<()> {
        let ops = &page.ops;
        if self.replaying || ops.is_empty() {
            return Ok(());
        }
        let required = self
            .bytes()
            .saturating_add(super::space::vector_bytes::<Offered>(ops.len()))
            .saturating_add(super::space::tree_bytes::<ActorId, ()>(
                page.positions.len(),
            ))
            .saturating_add(super::space::tree_bytes::<OpId, ()>(page.missing.len()));
        if required > super::continuation::MAX_CONTINUATION_BYTES {
            return Err(Error::SyncCapacity(
                "offered page identifiers exceed retained workspace".into(),
            ));
        }
        self.offered = ops
            .iter()
            .map(|op| Offered {
                id: op.id,
                actor: op.signed.body.actor_id,
                seq: op.signed.body.actor_seq,
                held: false,
            })
            .collect();
        let mut proof = self.covered.clone();
        let mut first = BTreeMap::<ActorId, u64>::new();
        for op in ops {
            let body = &op.signed.body;
            first
                .entry(body.actor_id)
                .and_modify(|seq| *seq = (*seq).min(body.actor_seq))
                .or_insert(body.actor_seq);
        }
        for (actor, seq) in first {
            proof.set(actor, floor.get(&actor).max(seq.saturating_sub(1)));
        }
        self.offer_floor = Some(proof);
        self.offer_positions = page.positions.clone();
        self.offer_missing = page.missing.clone();
        self.replay = 0;
        Ok(())
    }

    pub(super) fn confirm_offer(
        &mut self,
        request: &super::SyncRequest,
        held: Option<&ActorClock>,
        full: bool,
    ) {
        if full || self.offered.is_empty() {
            self.clear_offer();
            return;
        }
        let mut named = self
            .offered
            .iter()
            .map(|offer| (offer.actor, None::<u64>))
            .collect::<BTreeMap<_, _>>();
        for hint in &request.actor_range_hints {
            if let Some(known) = named.get_mut(&hint.actor_id) {
                *known =
                    Some(known.map_or(hint.from_exclusive, |known| known.min(hint.from_exclusive)));
            }
        }
        let mut first = None;
        for (index, offer) in self.offered.iter_mut().enumerate() {
            let prefix = named.get(&offer.actor).copied().flatten();
            let confirmed = match held {
                Some(clock) => {
                    clock.get(&offer.actor) >= offer.seq
                        && prefix.is_none_or(|prefix| prefix >= offer.seq)
                }
                None => prefix.map_or_else(
                    || request.window.holds(&offer.actor),
                    |prefix| prefix >= offer.seq,
                ),
            };
            offer.held = confirmed && !request.wants.contains(&offer.id);
            if !offer.held && first.is_none() {
                first = Some(index);
            }
        }
        if let Some(first) = first {
            self.replay = first;
            self.replaying = true;
        } else {
            self.clear_offer();
        }
    }

    fn clear_offer(&mut self) {
        self.offered = Vec::new();
        self.offer_positions = BTreeSet::new();
        self.offer_missing = BTreeSet::new();
        self.offer_floor = None;
        self.replay = 0;
        self.replaying = false;
    }

    pub(super) fn advancing(&self) -> bool {
        self.fresh
            || self.replaying
            || !self.updates.is_empty()
            || !self.pending.is_empty()
            || !self.active.is_empty()
            || !self.resumable.is_empty()
            || self.selecting.is_some()
            || !self.deferred.is_empty()
    }

    pub(super) fn admit(&mut self, ops: &[Op]) {
        for op in ops {
            let body = &op.signed.body;
            if self.covered.get(&body.actor_id).checked_add(1) == Some(body.actor_seq) {
                self.covered.observe(body.actor_id, body.actor_seq);
                self.revision = self.revision.wrapping_add(1);
            }
        }
    }

    pub(super) fn confirm(&mut self, request: &super::SyncRequest, local: &ActorClock) {
        for hint in &request.actor_range_hints {
            let known = hint.from_exclusive.min(local.get(&hint.actor_id));
            if self.positions.remove(&hint.actor_id).is_some() {
                self.revision = self.revision.wrapping_add(1);
            }
            if known > self.covered.get(&hint.actor_id) {
                self.covered.observe(hint.actor_id, known);
                self.revision = self.revision.wrapping_add(1);
            }
        }
    }
    pub(super) fn clock(&self) -> &ActorClock {
        &self.covered
    }

    /// The position the plan takes the peer to hold of `actor_id`.
    pub(super) fn covered(&self, actor_id: &ActorId) -> u64 {
        self.covered.get(actor_id)
    }

    /// A conservative estimate of the bytes the frontier holds.
    pub(super) fn bytes(&self) -> usize {
        use super::space::{tree_bytes, vector_bytes};
        let suspended = self
            .suspended
            .values()
            .map(|waiting| vector_bytes::<(u64, RangeHead)>(waiting.capacity()))
            .sum::<usize>();
        self.repair.as_ref().map_or(0, super::repair::Repair::bytes)
            + vector_bytes::<Offered>(self.offered.capacity())
            + tree_bytes::<ActorId, ()>(self.offer_positions.len())
            + tree_bytes::<OpId, ()>(self.offer_missing.len())
            + vector_bytes::<HeadScan>(self.pending.capacity())
            + vector_bytes::<Update>(self.updates.capacity())
            + vector_bytes::<RangeHead>(self.active.capacity())
            + self
                .selecting
                .as_ref()
                .map_or(0, |heap| vector_bytes::<RangeHead>(heap.capacity()))
            + suspended
            + tree_bytes::<ActorId, Vec<(u64, RangeHead)>>(self.suspended.len())
            + tree_bytes::<OpId, ()>(self.blocked.len())
            + tree_bytes::<OpId, ()>(self.missing.len())
            + self.deferred.bytes()
            + 32
            + tree_bytes::<ActorId, u64>(self.positions.len())
            + vector_bytes::<RangeHead>(self.resumable.capacity())
            + tree_bytes::<ActorId, ActorState>(self.states.len())
            + tree_bytes::<OpId, DependencyScan>(self.checked.len())
            + self
                .checked
                .values()
                .map(|scan| {
                    tree_bytes::<ActorId, (u64, u64)>(scan.needed.len())
                        + vector_bytes::<Ancestor>(scan.ancestry.capacity())
                })
                .sum::<usize>()
            + 4096
    }
}

/// What an op still needs before it can be sent.
enum Wait {
    Yield,
    Ready,
    Blocked,
    Position(ActorId, u64),
}

/// One bounded page plan over a snapshot. At most `window` actors are active.
/// An op waiting for another actor's position is suspended outside that set
/// and resumes once the position is sent, so waiters never fill the set and
/// every actor behind gets a slot as others finish. A page that holds more
/// but carries nothing names a missing record or an operation that is too
/// large, so a caller never repeats an identical empty page silently.
struct Pager<'a> {
    revision: u64,
    repair: Option<super::repair::Repair>,
    pending: VecDeque<HeadScan>,
    updates: VecDeque<Update>,
    slice: Slice,
    read: &'a dyn SnapshotRead,
    topic_id: &'a TopicId,
    local: &'a ActorClock,
    goal: Option<&'a ActorClock>,
    /// Which peer positions the request gave, and ids already sent before this plan.
    scope: &'a ActorScope<'a>,
    sent: &'a BTreeSet<OpId>,
    window: usize,
    /// Positions one page result names at most.
    position_limit: usize,
    work: &'a PageWork,
    ended: bool,
    active: BinaryHeap<Reverse<RangeHead>>,
    selecting: Option<BinaryHeap<RangeHead>>,
    remainder: bool,
    /// Actors behind the goal not activated yet, in clock order.
    deferred: ClockCursor,
    /// Suspended heads by the actor they wait for, with the position needed.
    suspended: BTreeMap<ActorId, Vec<(u64, RangeHead)>>,
    /// Suspended heads whose position was sent, waiting for a free slot.
    resumable: VecDeque<RangeHead>,
    states: BTreeMap<ActorId, ActorState>,
    covered: ActorClock,
    blocked: BTreeSet<OpId>,
    missing: BTreeSet<OpId>,
    /// Actors the request left unknown, with the lowest generation needing each.
    positions: BTreeMap<ActorId, u64>,
    more: bool,
    checked: BTreeMap<OpId, DependencyScan>,
    records: super::records::Records,
}

impl Pager<'_> {
    /// The highest position of `actor_id` the goal asks for.
    fn limit(&self, actor_id: &ActorId) -> u64 {
        let local_seq = self.local.get(actor_id);
        self.goal
            .map_or(local_seq, |goal| goal.get(actor_id).min(local_seq))
    }

    /// Count one storage read of this slice.
    fn visit(&mut self) -> bool {
        self.slice.read()
    }

    fn exhausted(&self) -> bool {
        self.slice.exhausted()
    }

    /// Plan one slice. A fresh plan starts from the actors behind; a resumed one
    /// goes on from its frontier. Returns the actors whose positions the page
    /// needed, by the lowest generation needing each, and the frontier when the
    /// slice ended on its read budget before sending anything.
    fn plan(mut self, budget: PageBudget, fresh: bool) -> Result<PlannedSlice> {
        if fresh {
            // A zero allowance reads nothing; whether the goal holds more is
            // known from the clocks alone.
            if budget.ops == 0 || budget.bytes == 0 {
                let page = PlannedPage {
                    more: self
                        .local
                        .iter()
                        .any(|(actor, _)| self.limit(actor) > self.covered.get(actor)),
                    ..PlannedPage::default()
                };
                return Ok((page, BTreeMap::new(), None));
            }
            self.deferred = if !self.scope.informed() && !self.scope.named.is_empty() {
                self.local.selected(&self.scope.named).cursor()
            } else {
                self.local.cursor()
            };
        }
        let mut ops = Vec::new();
        let mut too_large = None;
        let mut bytes = 0_usize;
        loop {
            // A slice out of reads, or a resumed plan given no allowance, ends
            // here and keeps what it holds.
            if ops.len() >= budget.ops || bytes >= budget.bytes || self.exhausted() {
                self.ended = true;
                break;
            }
            self.fill()?;
            if self.ended || self.selecting.is_some() || self.exhausted() {
                self.ended = true;
                break;
            }
            let Some(Reverse(head)) = self.active.pop() else {
                break;
            };
            let (_, actor_id, seq, id, limit) = head;
            if !self.records.contains(&id) {
                self.visit();
            }
            let Some(record) = self.records.take(self.read, &id)? else {
                self.missing.insert(id);
                self.block(actor_id, id);
                continue;
            };
            match self.wait_for(&record.op)? {
                Wait::Yield => {
                    self.records.keep(record);
                    self.active.push(Reverse(head));
                    self.ended = true;
                    break;
                }
                Wait::Ready => {}
                Wait::Blocked => {
                    self.block(actor_id, id);
                    continue;
                }
                Wait::Position(dep_actor, dep_seq) => {
                    self.records.keep(record);
                    self.suspend(head, dep_actor, dep_seq)?;
                    continue;
                }
            }
            let op = record.into_op();
            let size = postcard::experimental::serialized_size(&op)?;
            if size > MAX_PAGE_BYTES {
                return Err(Error::Storage("operation exceeds sync page budget".into()));
            }
            if ops.len() >= budget.ops || bytes + size > budget.bytes {
                if ops.is_empty() && size > budget.bytes {
                    too_large = Some(id);
                }
                self.active.push(Reverse(head));
                self.ended = true;
                self.more = true;
                break;
            }
            bytes += size;
            self.covered.observe(actor_id, seq);
            self.blocked.remove(&id);
            ops.push(op);
            self.wake(actor_id, seq);
            if seq < limit {
                self.activate(actor_id, seq, limit)?;
            } else {
                self.reach(actor_id, limit)?;
            }
        }
        let more = self.more
            || self.remainder
            || self.selecting.is_some()
            || !self.active.is_empty()
            || !self.resumable.is_empty()
            || self.suspended.values().any(|waiting| !waiting.is_empty())
            || !self.deferred.is_empty();
        self.ended &= more;
        let (mut missing, positions) = (self.missing.clone(), self.positions.clone());
        while missing.len() > MAX_PAGE_MISSING {
            missing.pop_last();
        }
        let frontier = (self.ended
            && (ops.is_empty() || (self.scope.informed() && !self.remainder))
            && too_large.is_none())
        .then(|| {
            self.work.ended.fetch_add(1, Ordering::Relaxed);
            Frontier {
                repair: None,
                evictable: false,
                revision: self.revision,
                offered: Vec::new(),
                offer_positions: BTreeSet::new(),
                offer_missing: BTreeSet::new(),
                offer_floor: None,
                replay: 0,
                replaying: false,
                fresh: false,
                pending: self.pending,
                updates: self.updates,
                active: self.active,
                selecting: self.selecting,
                remainder: self.remainder,
                deferred: self.deferred,
                suspended: self.suspended,
                resumable: self.resumable,
                states: self.states,
                covered: self.covered,
                blocked: self.blocked,
                missing: self.missing,
                positions: self.positions,
                checked: self.checked,
                records: self.records,
            }
        });
        let page = PlannedPage {
            ops,
            more,
            missing,
            too_large,
            positions: BTreeSet::new(),
            continued: false,
        };
        Ok((page, positions, frontier))
    }

    /// Fill free slots: resumed heads first, then deferred actors in order.
    fn fill(&mut self) -> Result<()> {
        if let Some(selected) = self.selecting.take() {
            self.select(selected)?;
            if self.selecting.is_some() {
                return Ok(());
            }
        }
        while self.active.len() < self.window && !self.exhausted() {
            if let Some(head) = self.resumable.pop_front() {
                let (_, actor_id, _, _, _) = head;
                self.active.push(Reverse(head));
                self.states.insert(actor_id, ActorState::Active);
                continue;
            }
            let Some((actor_id, _)) = self.deferred.next() else {
                return Ok(());
            };
            self.scan();
            let limit = self.limit(&actor_id);
            if self.states.contains_key(&actor_id) || limit <= self.covered.get(&actor_id) {
                continue;
            }
            self.activate(actor_id, self.covered.get(&actor_id), limit)?;
        }
        Ok(())
    }

    /// Queue the op after `after` on `actor_id`, up to `limit`; callers keep
    /// `after < limit`. A gap in the index stops the actor and names the
    /// record the next indexed op follows.
    fn activate(&mut self, actor_id: ActorId, after: u64, limit: u64) -> Result<()> {
        if let Some(head) = self.head(actor_id, after, limit)? {
            self.active.push(Reverse(head));
            self.states.insert(actor_id, ActorState::Active);
        }
        Ok(())
    }

    fn head(&mut self, actor_id: ActorId, after: u64, limit: u64) -> Result<Option<RangeHead>> {
        self.visit();
        let next = self
            .read
            .actor_range(self.topic_id, &actor_id, after, 1)?
            .pop();
        // The clock is ahead of the index, so no id names the lost position.
        let Some((seq, id)) = next else {
            self.stop(actor_id);
            return Ok(None);
        };
        self.visit();
        let meta = self.read.get_header(&id)?;
        if seq != after + 1 || meta.is_none() {
            match meta.filter(|_| seq != after + 1) {
                Some(meta) => self.missing.extend(meta.actor_prev),
                None => {
                    self.missing.insert(id);
                }
            }
            self.stop(actor_id);
            return Ok(None);
        }
        Ok(meta.map(|meta| (meta.generation, actor_id, seq, id, limit)))
    }

    fn scan(&mut self) {
        self.slice.scanned += 1;
        self.work.actors.fetch_add(1, Ordering::Relaxed);
    }

    /// Retain only the oldest heads while the bounded inventory cursor advances.
    fn select(&mut self, mut selected: BinaryHeap<RangeHead>) -> Result<()> {
        while !self.exhausted() {
            let Some((actor, _)) = self.deferred.next() else {
                for head in selected {
                    self.states.insert(head.1, ActorState::Active);
                    self.active.push(Reverse(head));
                }
                // Finish inventory before spending a fresh slice on its selected page.
                self.ended = self.slice.scanned > 0;
                return Ok(());
            };
            self.scan();
            let (after, limit) = (self.covered.get(&actor), self.limit(&actor));
            if after >= limit {
                continue;
            }
            if let Some(head) = self.head(actor, after, limit)? {
                if selected.len() < self.window {
                    selected.push(head);
                } else {
                    self.remainder = true;
                    if selected.peek().is_some_and(|last| head < *last) {
                        selected.pop();
                        selected.push(head);
                    }
                }
            }
        }
        self.selecting = Some(selected);
        Ok(())
    }

    /// What `meta` still waits for: an unsent or blocked dependency, a missing
    /// record, positions of actors the request did not describe, all named at
    /// once, or a position of another actor the peer does not hold yet.
    fn wait_for(&mut self, op: &Op) -> Result<Wait> {
        let mut unknown = false;
        let mut waits = None;
        let after = self.checked.get(&op.id).and_then(|scan| scan.after);
        let start = after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        for dep in op
            .signed
            .body
            .deps
            .range((start, std::ops::Bound::Unbounded))
        {
            if self.scope.informed() && self.exhausted() {
                return Ok(Wait::Yield);
            }
            self.work.edges.fetch_add(1, Ordering::Relaxed);
            if self.blocked.contains(dep) {
                return Ok(Wait::Blocked);
            }
            if self.sent.contains(dep) {
                continue;
            }
            self.visit();
            let position = self
                .read
                .get_header(dep)?
                .map(|meta| (meta.actor_id, meta.actor_seq, meta.generation));
            let Some((dep_actor, dep_seq, dep_generation)) = position else {
                self.missing.insert(*dep);
                return Ok(Wait::Blocked);
            };
            if let Some(known) = self.scope.known_prefix(&dep_actor)
                && known > 0
            {
                self.covered.observe(dep_actor, known.min(dep_seq));
            }
            if self.scope.holds_prefix(&dep_actor, dep_seq) {
                self.covered.observe(dep_actor, dep_seq);
                self.checked.entry(op.id).or_default().after = Some(*dep);
                continue;
            }
            // Omitted from the request is not held: the requester names it next.
            if self.scope.unknown(&dep_actor) {
                need(&mut self.positions, dep_actor, dep_generation);
                self.unknown_ancestors(*dep)?;
                unknown = true;
            } else if waits.is_none() && self.covered.get(&dep_actor) < dep_seq {
                if self.scope.informed() {
                    return Ok(Wait::Position(dep_actor, dep_seq));
                }
                waits = Some(Wait::Position(dep_actor, dep_seq));
            } else if self.scope.informed() {
                self.checked.entry(op.id).or_default().after = Some(*dep);
            }
        }
        self.checked.remove(&op.id);
        Ok(match (unknown, waits) {
            (true, _) => Wait::Blocked,
            (false, Some(waits)) => waits,
            (false, None) => Wait::Ready,
        })
    }

    /// Name the unknown actors of `id`'s ancestry too, up to the page's
    /// position limit, so one result names a run of a dependency chain instead
    /// of one link per request. A walk stops at actors the request describes
    /// and at actors already named, so shared ancestry is walked once.
    fn unknown_ancestors(&mut self, id: OpId) -> Result<()> {
        let mut queue = vec![id];
        let mut found = 0;
        while let Some(next) = queue.pop() {
            if found >= self.position_limit || self.exhausted() {
                return Ok(());
            }
            self.visit();
            let Some(meta) = self.read.get_position(&next)? else {
                continue;
            };
            for dep in &meta.deps {
                self.work.edges.fetch_add(1, Ordering::Relaxed);
                self.visit();
                let Some(dep_meta) = self.read.get_header(dep)? else {
                    continue;
                };
                if !self.scope.unknown(&dep_meta.actor_id)
                    || self
                        .scope
                        .holds_prefix(&dep_meta.actor_id, dep_meta.actor_seq)
                    || self.positions.contains_key(&dep_meta.actor_id)
                {
                    continue;
                }
                need(&mut self.positions, dep_meta.actor_id, dep_meta.generation);
                found += 1;
                queue.push(*dep);
            }
        }
        Ok(())
    }

    /// Park `head` until `dep_actor` reaches `dep_seq`, activating that actor
    /// when it has no head yet. A dependency beyond the goal is ancestry the
    /// goal needs, so the dependency actor's limit rises to cover it.
    fn suspend(&mut self, head: RangeHead, dep_actor: ActorId, dep_seq: u64) -> Result<()> {
        let (_, actor_id, _, id, _) = head;
        match self.states.get(&dep_actor).copied() {
            Some(ActorState::Blocked) => {
                self.block(actor_id, id);
                return Ok(());
            }
            Some(ActorState::Active | ActorState::Suspended) => {}
            Some(ActorState::Reached(limit)) => {
                if limit >= dep_seq
                    || (!self.scope.informed() && dep_seq > self.local.get(&dep_actor))
                {
                    self.block(actor_id, id);
                    return Ok(());
                }
                self.activate(dep_actor, limit, dep_seq)?;
            }
            None => {
                let after = self.covered.get(&dep_actor);
                self.activate(dep_actor, after, dep_seq.max(self.limit(&dep_actor)))?;
            }
        }
        if matches!(self.states.get(&dep_actor), Some(ActorState::Blocked)) {
            self.block(actor_id, id);
            return Ok(());
        }
        self.states.insert(actor_id, ActorState::Suspended);
        self.suspended
            .entry(dep_actor)
            .or_default()
            .push((dep_seq, head));
        Ok(())
    }

    /// `actor_id` sent `seq`: every head waiting for that position may resume.
    fn wake(&mut self, actor_id: ActorId, seq: u64) {
        let Some(waiting) = self.suspended.get_mut(&actor_id) else {
            return;
        };
        let (ready, rest) = std::mem::take(waiting)
            .into_iter()
            .partition::<Vec<_>, _>(|(needed, _)| *needed <= seq);
        *waiting = rest;
        self.resumable
            .extend(ready.into_iter().map(|(_, head)| head));
    }

    /// `actor_id` sent everything up to `limit`. Heads still waiting on it need
    /// a later position: the actor continues once up to the highest of them,
    /// and a waiter no stored position can satisfy is blocked.
    fn reach(&mut self, actor_id: ActorId, limit: u64) -> Result<()> {
        self.states.insert(actor_id, ActorState::Reached(limit));
        let waiting = self.suspended.remove(&actor_id).unwrap_or_default();
        let local_seq = self.local.get(&actor_id);
        let raised = waiting
            .iter()
            .map(|(needed, _)| *needed)
            .filter(|needed| *needed > limit && (self.scope.informed() || *needed <= local_seq))
            .max();
        if let Some(raised) = raised {
            self.activate(actor_id, limit, raised)?;
        }
        let continues = matches!(self.states.get(&actor_id), Some(ActorState::Active));
        for (needed, head) in waiting {
            if continues && raised.is_some_and(|raised| needed <= raised) {
                self.suspended
                    .entry(actor_id)
                    .or_default()
                    .push((needed, head));
            } else {
                self.block(head.1, head.3);
            }
        }
        if self.scope.informed()
            && matches!(self.states.get(&actor_id), Some(ActorState::Reached(_)))
        {
            self.states.remove(&actor_id);
        }
        Ok(())
    }

    /// Stop `actor_id` at `id`, and every head waiting on it.
    fn block(&mut self, actor_id: ActorId, id: OpId) {
        self.blocked.insert(id);
        self.stop(actor_id);
    }

    /// Stop `actor_id` and every head suspended on it, transitively.
    fn stop(&mut self, actor_id: ActorId) {
        self.states.insert(actor_id, ActorState::Blocked);
        self.more = true;
        let mut stopped = vec![actor_id];
        while let Some(stopped_actor) = stopped.pop() {
            for (_, (_, waiter, _, id, _)) in
                self.suspended.remove(&stopped_actor).unwrap_or_default()
            {
                self.blocked.insert(id);
                self.states.insert(waiter, ActorState::Blocked);
                stopped.push(waiter);
            }
        }
    }
}

impl<S: Storage> SyncEngine<S> {
    /// The next causal page for a peer at `peer`, merging forward actor ranges by
    /// generation, so dependencies come first. Work grows with the page and the
    /// actors behind, never with history the peer holds, and one slice reads at
    /// most the engine's visit budget. See [`Pager`].
    pub(super) fn plan_page(
        &self,
        read: &dyn SnapshotRead,
        topic_id: &TopicId,
        (local, peer, goal): (&ActorClock, &ActorClock, Option<&ActorClock>),
        (scope, sent): (&ActorScope<'_>, &BTreeSet<OpId>),
        excluded: &BTreeSet<OpId>,
        budget: PageBudget,
    ) -> Result<PlannedSlice> {
        let pager = Pager {
            revision: 0,
            repair: None,
            pending: VecDeque::new(),
            updates: VecDeque::new(),
            slice: Slice::new(std::sync::Arc::clone(&self.work), self.page_visits, 0)?,
            read,
            topic_id,
            local,
            goal,
            scope,
            sent,
            window: self.page_actors,
            position_limit: self.page_positions,
            work: &self.work,
            ended: false,
            active: BinaryHeap::new(),
            selecting: (scope.informed() && scope.named.len() > self.page_actors)
                .then(BinaryHeap::new),
            remainder: false,
            deferred: ClockCursor::default(),
            suspended: BTreeMap::new(),
            resumable: VecDeque::new(),
            states: BTreeMap::new(),
            covered: peer.clone(),
            blocked: excluded.clone(),
            missing: BTreeSet::new(),
            positions: BTreeMap::new(),
            more: false,
            checked: BTreeMap::new(),
            records: self.continuations().records(),
        };
        pager.plan(budget, true)
    }

    /// One more slice of the kept plan `frontier`, against the clocks it
    /// planned on and a request with the same scope.
    pub(super) fn resume_page(
        &self,
        read: &dyn SnapshotRead,
        topic_id: &TopicId,
        (local, goal, frontier): (&ActorClock, &ActorClock, Frontier),
        scope: &ActorScope<'_>,
        budget: PageBudget,
    ) -> Result<PlannedSlice> {
        const SENT: BTreeSet<OpId> = BTreeSet::new();
        let pager = Pager {
            revision: frontier.revision,
            repair: frontier.repair,
            pending: frontier.pending,
            updates: frontier.updates,
            slice: Slice::new(std::sync::Arc::clone(&self.work), self.page_visits, 0)?,
            read,
            topic_id,
            local,
            goal: Some(goal),
            scope,
            sent: &SENT,
            window: self.page_actors,
            position_limit: self.page_positions,
            work: &self.work,
            ended: false,
            active: frontier.active,
            selecting: frontier.selecting,
            remainder: frontier.remainder,
            deferred: frontier.deferred,
            suspended: frontier.suspended,
            resumable: frontier.resumable,
            states: frontier.states,
            covered: frontier.covered,
            blocked: frontier.blocked,
            missing: frontier.missing,
            positions: frontier.positions,
            more: false,
            checked: frontier.checked,
            records: frontier.records,
        };
        pager.plan(budget, false)
    }
}
