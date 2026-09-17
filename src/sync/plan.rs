// SPDX-License-Identifier: MIT OR Apache-2.0
//! Bounded forward page planning over one read context: actor ranges merged by
//! generation, with waiting heads suspended outside the active set.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};

use crate::clock::ClockCursor;
use crate::storage::{DependencyCursor, OpHeader, SnapshotRead, Storage};
use crate::{ActorClock, ActorId, Error, Op, OpId, Result, TopicId};

use super::request::need;
use super::{
    ActorScope, MAX_PAGE_BYTES, MAX_PAGE_MISSING, PageBudget, PlannedPage, RangeHead, SyncEngine,
};

use super::slice::{PageWork, Slice};

/// A planned page, the actors whose positions it needed by the lowest
/// generation needing each, and the frontier of a slice that stopped with work left.
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
        if required > super::slice::MAX_CONTINUATION_BYTES {
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
    Unknown,
    Ready,
    Blocked,
    Position(ActorId, u64),
}

/// Bounded page plan over a snapshot with at most `window` active actors. A head that waits for
/// another actor's position is suspended outside that set, so waiting heads never fill it.
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

    /// Plan one slice, fresh from the actors behind or resumed from its frontier. Return needed
    /// positions by lowest generation, and the frontier whenever work is left: also after a full
    /// page or with repair pending, but not after an oversized op or data from a partial window.
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
            if ops.len() >= budget.ops || bytes >= budget.bytes || self.slice.exhausted() {
                self.ended = true;
                break;
            }
            self.settle()?;
            if self.ended {
                break;
            }
            self.fill()?;
            if self.ended || self.selecting.is_some() || self.slice.exhausted() {
                self.ended = true;
                break;
            }
            let Some(Reverse(head)) = self.active.pop() else {
                break;
            };
            let (_, actor_id, seq, id, limit) = head;
            if let Some(known) = self.scope.known_prefix(&actor_id) {
                self.observe(actor_id, known.min(limit));
            }
            let held = self.covered.get(&actor_id);
            if held >= seq {
                if !self.slice.charge_actor() {
                    self.active.push(Reverse(head));
                    self.ended = true;
                    break;
                }
                self.wake(actor_id, held);
                if held < limit {
                    self.activate(actor_id, held, limit)?;
                } else {
                    self.reach(actor_id, limit)?;
                }
                continue;
            }
            if !self.records.contains(&id) && !self.slice.charge_read() {
                self.active.push(Reverse(head));
                self.ended = true;
                break;
            }
            let record = match self.records.take(self.read, &id, &mut self.slice) {
                Ok(Some(record)) => record,
                Ok(None) => {
                    self.missing.insert(id);
                    self.block(actor_id, id);
                    continue;
                }
                Err(super::records::LoadError::Yield) => {
                    self.active.push(Reverse(head));
                    self.ended = true;
                    break;
                }
                Err(super::records::LoadError::Failed(error)) => return Err(error),
            };
            match self.wait_for(&record.op)? {
                Wait::Yield | Wait::Unknown => {
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
            let size = postcard::experimental::serialized_size(&record.op)?;
            if size > MAX_PAGE_BYTES {
                return Err(Error::Storage("operation exceeds sync page budget".into()));
            }
            if ops.len() >= budget.ops || bytes + size > budget.bytes {
                if ops.is_empty() && size > budget.bytes {
                    too_large = Some(id);
                }
                self.active.push(Reverse(head));
                self.records.keep(record);
                self.ended = true;
                self.more = true;
                break;
            }
            bytes += size;
            self.observe(actor_id, seq);
            self.blocked.remove(&id);
            ops.push(record.into_op());
            self.wake(actor_id, seq);
            if seq < limit {
                self.activate(actor_id, seq, limit)?;
            } else {
                self.reach(actor_id, limit)?;
            }
        }
        if let Some(repair) = &mut self.repair {
            repair.admit(&ops, &mut self.slice)?;
        }
        let more = self.more
            || self.remainder
            || self.selecting.is_some()
            || !self.active.is_empty()
            || !self.pending.is_empty()
            || self
                .repair
                .as_ref()
                .is_some_and(super::repair::Repair::pending)
            || !self.resumable.is_empty()
            || self.suspended.values().any(|waiting| !waiting.is_empty())
            || !self.deferred.is_empty();
        self.ended &= more;
        let (mut missing, positions) = (self.missing.clone(), self.positions.clone());
        while missing.len() > MAX_PAGE_MISSING {
            missing.pop_last();
        }
        let frontier = ((self.ended
            || self
                .repair
                .as_ref()
                .is_some_and(super::repair::Repair::pending))
            && (ops.is_empty() || !self.remainder)
            && too_large.is_none())
        .then(|| {
            self.work.ended();
            Frontier {
                repair: self.repair,
                evictable: !ops.is_empty(),
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

    fn fill(&mut self) -> Result<()> {
        if let Some(selected) = self.selecting.take() {
            self.select(selected)?;
            if self.selecting.is_some() || self.ended {
                return Ok(());
            }
        }
        while self.active.len() < self.window && !self.slice.exhausted() {
            if let Some(scan) = self.pending.pop_front() {
                if let Some(head) = self.head(scan)? {
                    self.active.push(Reverse(head));
                }
                if self.ended {
                    return Ok(());
                }
                continue;
            }
            if let Some(head) = self.resumable.pop_front() {
                if !self.slice.charge_actor() {
                    self.resumable.push_front(head);
                    break;
                }
                self.active.push(Reverse(head));
                self.states.insert(head.1, ActorState::Active);
                continue;
            }
            if self.deferred.is_empty() || !self.slice.charge_actor() {
                break;
            }
            let Some((actor, _)) = self.deferred.next() else {
                break;
            };
            let limit = self.limit(&actor);
            if self.states.contains_key(&actor) || limit <= self.covered.get(&actor) {
                continue;
            }
            self.activate(actor, self.covered.get(&actor), limit)?;
        }
        Ok(())
    }

    fn activate(&mut self, actor: ActorId, after: u64, limit: u64) -> Result<()> {
        if !self.states.contains_key(&actor) {
            // One actor may own a head, a suspended entry, a cursor and its state.
            self.slice.reserve(768)?;
        }
        self.pending.push_back(HeadScan {
            actor,
            after,
            limit,
            next: None,
        });
        self.states.insert(actor, ActorState::Active);
        Ok(())
    }

    fn head(&mut self, mut scan: HeadScan) -> Result<Option<RangeHead>> {
        if scan.next.is_none() {
            if !self.slice.charge_read() {
                self.pending.push_front(scan);
                self.ended = true;
                return Ok(None);
            }
            scan.next = self
                .read
                .actor_range(self.topic_id, &scan.actor, scan.after, 1)?
                .pop();
            if scan.next.is_none() {
                self.stop(scan.actor);
                return Ok(None);
            }
        }
        if !self.slice.charge_read() {
            self.pending.push_front(scan);
            self.ended = true;
            return Ok(None);
        }
        let Some((seq, id)) = scan.next else {
            return Ok(None);
        };
        let meta = self.read.get_header(&id)?;
        if seq != scan.after + 1 || meta.is_none() {
            self.slice.reserve(256)?;
            match meta.filter(|_| seq != scan.after + 1) {
                Some(meta) => self.missing.extend(meta.actor_prev),
                None => {
                    self.missing.insert(id);
                }
            }
            self.stop(scan.actor);
            return Ok(None);
        }
        Ok(meta.map(|meta| (meta.generation, scan.actor, seq, id, scan.limit)))
    }

    fn select(&mut self, mut selected: BinaryHeap<RangeHead>) -> Result<()> {
        loop {
            if self.pending.is_empty() && self.deferred.is_empty() {
                while !selected.is_empty() {
                    if !self.slice.charge_actor() {
                        self.selecting = Some(selected);
                        return Ok(());
                    }
                    if let Some(head) = selected.pop() {
                        self.slice.reserve(768)?;
                        self.states.insert(head.1, ActorState::Active);
                        self.active.push(Reverse(head));
                    }
                }
                self.ended = true;
                return Ok(());
            }
            if self.slice.exhausted() {
                self.selecting = Some(selected);
                return Ok(());
            }
            if let Some(scan) = self.pending.pop_front() {
                if let Some(head) = self.head(scan)? {
                    if selected.len() < self.window {
                        self.slice.reserve(2 * size_of::<RangeHead>())?;
                        selected.push(head);
                    } else {
                        self.remainder = true;
                        if selected.peek().is_some_and(|last| head < *last) {
                            selected.pop();
                            selected.push(head);
                        }
                    }
                }
                if self.ended {
                    self.selecting = Some(selected);
                    return Ok(());
                }
                continue;
            }
            if !self.slice.charge_actor() {
                self.selecting = Some(selected);
                return Ok(());
            }
            let Some((actor, _)) = self.deferred.next() else {
                continue;
            };
            let (after, limit) = (self.covered.get(&actor), self.limit(&actor));
            if after < limit {
                self.pending.push_back(HeadScan {
                    actor,
                    after,
                    limit,
                    next: None,
                });
            }
        }
    }

    fn wait_for(&mut self, op: &Op) -> Result<Wait> {
        let mut scan = self.checked.remove(&op.id).unwrap_or_default();
        if scan.revision != self.revision {
            scan.checked = None;
            scan.revision = self.revision;
        }
        loop {
            let start = scan
                .checked
                .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
            let next = scan
                .needed
                .range((start, std::ops::Bound::Unbounded))
                .next()
                .map(|(actor, needed)| (*actor, *needed));
            let Some((actor, (seq, generation))) = next else {
                break;
            };
            if !self.slice.charge_actor() {
                self.checked.insert(op.id, scan);
                return Ok(Wait::Yield);
            }
            if let Some(known) = self.scope.known_prefix(&actor) {
                self.observe(actor, known.min(seq));
            }
            if self.scope.unknown(&actor) {
                if !self.need_position(actor, generation)? {
                    self.checked.insert(op.id, scan);
                    return Ok(Wait::Unknown);
                }
            } else if self.scope.holds_prefix(&actor, seq) || self.covered.get(&actor) >= seq {
                scan.needed.remove(&actor);
            } else {
                self.checked.insert(op.id, scan);
                return Ok(Wait::Position(actor, seq));
            }
            scan.checked = Some(actor);
        }
        if !self.unknown_ancestors(&mut scan)? {
            self.checked.insert(op.id, scan);
            return Ok(Wait::Yield);
        }
        let start = scan
            .after
            .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        for dep in op
            .signed
            .body
            .deps
            .range((start, std::ops::Bound::Unbounded))
        {
            if self.slice.exhausted() || !self.slice.charge_edge() {
                self.checked.insert(op.id, scan);
                return Ok(Wait::Yield);
            }
            if self.blocked.contains(dep)
                || self
                    .repair
                    .as_ref()
                    .is_some_and(|repair| repair.contains(dep))
            {
                return Ok(Wait::Blocked);
            }
            if self.sent.contains(dep) {
                scan.after = Some(*dep);
                continue;
            }
            if !self.slice.charge_read() {
                self.checked.insert(op.id, scan);
                return Ok(Wait::Yield);
            }
            let Some(meta) = self.read.get_header(dep)? else {
                self.slice.reserve(256)?;
                self.missing.insert(*dep);
                return Ok(Wait::Blocked);
            };
            if let Some(known) = self.scope.known_prefix(&meta.actor_id)
                && known > 0
            {
                self.observe(meta.actor_id, known.min(meta.actor_seq));
            }
            if self.scope.holds_prefix(&meta.actor_id, meta.actor_seq) {
                self.observe(meta.actor_id, meta.actor_seq);
            } else if self.scope.unknown(&meta.actor_id) {
                if !self.demand(&mut scan, &meta)? {
                    self.checked.insert(op.id, scan);
                    return Ok(Wait::Unknown);
                }
                self.slice.reserve(2 * size_of::<Ancestor>())?;
                scan.ancestry.push(Ancestor {
                    id: *dep,
                    actor: meta.actor_id,
                    seq: meta.actor_seq,
                    cursor: DependencyCursor::default(),
                    pending: None,
                });
                scan.after = Some(*dep);
                if !self.unknown_ancestors(&mut scan)? {
                    self.checked.insert(op.id, scan);
                    return Ok(Wait::Yield);
                }
            } else if self.covered.get(&meta.actor_id) < meta.actor_seq {
                let waiting = Wait::Position(meta.actor_id, meta.actor_seq);
                self.checked.insert(op.id, scan);
                return Ok(waiting);
            }
            scan.after = Some(*dep);
        }
        if scan.needed.is_empty() {
            Ok(Wait::Ready)
        } else {
            self.checked.insert(op.id, scan);
            Ok(Wait::Unknown)
        }
    }

    fn observe(&mut self, actor: ActorId, seq: u64) {
        if seq > self.covered.get(&actor) {
            self.covered.observe(actor, seq);
            self.revision = self.revision.wrapping_add(1);
        }
    }

    fn need_position(&mut self, actor: ActorId, generation: u64) -> Result<bool> {
        if !self.positions.contains_key(&actor) {
            if self.positions.len() >= self.position_limit {
                return Ok(false);
            }
            self.slice.reserve(256)?;
        }
        need(&mut self.positions, actor, generation);
        Ok(true)
    }

    fn demand(&mut self, scan: &mut DependencyScan, meta: &OpHeader) -> Result<bool> {
        if !scan.needed.contains_key(&meta.actor_id) {
            if scan.needed.len() >= self.position_limit {
                return Ok(false);
            }
            self.slice.reserve(256)?;
        }
        if !self.need_position(meta.actor_id, meta.generation)? {
            return Ok(false);
        }
        scan.needed
            .entry(meta.actor_id)
            .and_modify(|needed| {
                needed.0 = needed.0.max(meta.actor_seq);
                needed.1 = needed.1.min(meta.generation);
            })
            .or_insert((meta.actor_seq, meta.generation));
        Ok(true)
    }

    fn unknown_ancestors(&mut self, scan: &mut DependencyScan) -> Result<bool> {
        while let Some(mut frame) = scan.ancestry.pop() {
            if !self.scope.unknown(&frame.actor)
                && (self.scope.holds_prefix(&frame.actor, frame.seq)
                    || self.covered.get(&frame.actor) >= frame.seq)
            {
                if !self.slice.charge_actor() {
                    scan.ancestry.push(frame);
                    return Ok(false);
                }
                continue;
            }
            if frame.pending.is_none() {
                if !self.slice.charge_read() {
                    scan.ancestry.push(frame);
                    return Ok(false);
                }
                let next = self.read.dependency_ids(&frame.id, frame.cursor, 1)?;
                let Some(mut next) = next else {
                    self.slice.reserve(256)?;
                    self.missing.insert(frame.id);
                    continue;
                };
                let Some(id) = next.pop() else { continue };
                frame.pending = Some(Dependency { id, header: None });
            }
            let Some(mut dependency) = frame.pending.take() else {
                continue;
            };
            if dependency.header.is_none() {
                if self.slice.exhausted() || !self.slice.charge_edge() || !self.slice.charge_read()
                {
                    frame.pending = Some(dependency);
                    scan.ancestry.push(frame);
                    return Ok(false);
                }
                dependency.header = self.read.get_header(&dependency.id)?;
            }
            let mut descend = false;
            if let Some(meta) = &dependency.header
                && self.scope.unknown(&meta.actor_id)
                && scan
                    .needed
                    .get(&meta.actor_id)
                    .is_none_or(|needed| needed.0 < meta.actor_seq)
            {
                if !self.demand(scan, meta)? {
                    frame.pending = Some(dependency);
                    scan.ancestry.push(frame);
                    return Ok(false);
                }
                descend = true;
            }
            frame.cursor.offset = frame
                .cursor
                .offset
                .checked_add(1)
                .ok_or_else(|| Error::SyncCapacity("dependency cursor overflow".into()))?;
            frame.cursor.after = Some(dependency.id);
            scan.ancestry.push(frame);
            if descend && let Some(meta) = dependency.header {
                self.slice.reserve(2 * size_of::<Ancestor>())?;
                scan.ancestry.push(Ancestor {
                    id: dependency.id,
                    actor: meta.actor_id,
                    seq: meta.actor_seq,
                    cursor: DependencyCursor::default(),
                    pending: None,
                });
            }
        }
        Ok(true)
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

    fn wake(&mut self, actor: ActorId, seq: u64) {
        if self.suspended.contains_key(&actor) {
            self.updates.push_back(Update::Wake {
                actor,
                seq,
                index: 0,
            });
        }
    }

    fn reach(&mut self, actor: ActorId, limit: u64) -> Result<()> {
        self.states.insert(actor, ActorState::Reached(limit));
        if self.suspended.contains_key(&actor) {
            self.updates.push_back(Update::Raise {
                actor,
                limit,
                index: 0,
                raised: None,
            });
        } else if self.scope.informed() {
            self.states.remove(&actor);
        }
        Ok(())
    }

    fn block(&mut self, actor: ActorId, id: OpId) {
        self.blocked.insert(id);
        self.stop(actor);
    }

    fn stop(&mut self, actor: ActorId) {
        self.states.insert(actor, ActorState::Blocked);
        self.more = true;
        if self.suspended.contains_key(&actor) {
            self.updates.push_back(Update::Stop(actor));
        }
    }

    fn settle(&mut self) -> Result<()> {
        while let Some(update) = self.updates.pop_front() {
            match update {
                Update::Wake { actor, seq, index } => {
                    let next = self
                        .suspended
                        .get(&actor)
                        .and_then(|waiting| waiting.get(index))
                        .copied();
                    let Some((needed, head)) = next else { continue };
                    if !self.slice.charge_actor() {
                        self.updates.push_front(update);
                        self.ended = true;
                        return Ok(());
                    }
                    let index = if needed <= seq {
                        if let Some(waiting) = self.suspended.get_mut(&actor) {
                            waiting.swap_remove(index);
                            if waiting.is_empty() {
                                self.suspended.remove(&actor);
                            }
                        }
                        self.resumable.push_back(head);
                        index
                    } else {
                        index + 1
                    };
                    self.updates.push_front(Update::Wake { actor, seq, index });
                }
                Update::Raise {
                    actor,
                    limit,
                    index,
                    mut raised,
                } => {
                    let next = self
                        .suspended
                        .get(&actor)
                        .and_then(|waiting| waiting.get(index))
                        .copied();
                    if let Some((needed, _)) = next {
                        if !self.slice.charge_actor() {
                            self.updates.push_front(update);
                            self.ended = true;
                            return Ok(());
                        }
                        if needed > limit
                            && (self.scope.informed() || needed <= self.local.get(&actor))
                        {
                            raised = Some(raised.map_or(needed, |raised| raised.max(needed)));
                        }
                        self.updates.push_front(Update::Raise {
                            actor,
                            limit,
                            index: index + 1,
                            raised,
                        });
                    } else {
                        if let Some(raised) = raised {
                            self.activate(actor, limit, raised)?;
                        }
                        self.updates.push_front(Update::Finish {
                            actor,
                            raised,
                            index: 0,
                        });
                    }
                }
                Update::Finish {
                    actor,
                    raised,
                    index,
                } => {
                    let next = self
                        .suspended
                        .get(&actor)
                        .and_then(|waiting| waiting.get(index))
                        .copied();
                    let Some((needed, head)) = next else {
                        if self.scope.informed()
                            && matches!(self.states.get(&actor), Some(ActorState::Reached(_)))
                        {
                            self.states.remove(&actor);
                        }
                        continue;
                    };
                    if !self.slice.charge_actor() {
                        self.updates.push_front(update);
                        self.ended = true;
                        return Ok(());
                    }
                    let index = if raised.is_some_and(|raised| needed <= raised) {
                        index + 1
                    } else {
                        if let Some(waiting) = self.suspended.get_mut(&actor) {
                            waiting.swap_remove(index);
                            if waiting.is_empty() {
                                self.suspended.remove(&actor);
                            }
                        }
                        self.block(head.1, head.3);
                        index
                    };
                    self.updates.push_front(Update::Finish {
                        actor,
                        raised,
                        index,
                    });
                }
                Update::Stop(actor) => {
                    let next = self
                        .suspended
                        .get(&actor)
                        .and_then(|waiting| waiting.last())
                        .copied();
                    let Some((_, head)) = next else { continue };
                    if !self.slice.charge_actor() {
                        self.updates.push_front(update);
                        self.ended = true;
                        return Ok(());
                    }
                    if let Some(waiting) = self.suspended.get_mut(&actor) {
                        waiting.pop();
                        if waiting.is_empty() {
                            self.suspended.remove(&actor);
                        }
                    }
                    self.block(head.1, head.3);
                    self.updates.push_front(Update::Stop(actor));
                }
            }
        }
        Ok(())
    }
}

impl<S: Storage> SyncEngine<S> {
    /// Next causal page for `peer`, merging forward ranges by generation so dependencies come
    /// first. Work grows with the page and actors behind, not history the peer holds; one slice
    /// reads at most the engine's visit budget. See [`Pager`].
    pub(super) fn plan_page(
        &self,
        read: &dyn SnapshotRead,
        topic_id: &TopicId,
        (local, peer, goal): (&ActorClock, &ActorClock, Option<&ActorClock>),
        (scope, sent): (&ActorScope<'_>, &BTreeSet<OpId>),
        excluded: &BTreeSet<OpId>,
        budget: PageBudget,
    ) -> Result<PlannedSlice> {
        let mut frontier = Frontier::new(
            peer,
            scope,
            self.page_actors,
            self.continuations().records(),
        );
        frontier.blocked = excluded.clone();
        let slice = Slice::new(std::sync::Arc::clone(&self.work), self.page_visits, 0)?;
        self.pager(
            read,
            topic_id,
            (local, goal, frontier),
            (scope, sent, self.page_positions),
            slice,
        )
        .plan(budget, true)
    }

    /// One slice of `frontier`, new from `Frontier::new` or kept from an earlier request with the
    /// same scope, against the local and goal clocks the plan captured.
    pub(super) fn resume_page(
        &self,
        read: &dyn SnapshotRead,
        topic_id: &TopicId,
        (local, goal, frontier): (&ActorClock, &ActorClock, Frontier),
        (scope, position_limit): (&ActorScope<'_>, usize),
        budget: PageBudget,
        slice: Slice,
    ) -> Result<PlannedSlice> {
        const SENT: BTreeSet<OpId> = BTreeSet::new();
        let fresh = frontier.fresh;
        self.pager(
            read,
            topic_id,
            (local, Some(goal), frontier),
            (scope, &SENT, position_limit),
            slice,
        )
        .plan(budget, fresh)
    }

    /// A pager that continues `frontier` in `slice` against the local and goal clocks.
    fn pager<'a>(
        &'a self,
        read: &'a dyn SnapshotRead,
        topic_id: &'a TopicId,
        (local, goal, frontier): (&'a ActorClock, Option<&'a ActorClock>, Frontier),
        (scope, sent, position_limit): (&'a ActorScope<'a>, &'a BTreeSet<OpId>, usize),
        slice: Slice,
    ) -> Pager<'a> {
        Pager {
            revision: frontier.revision,
            repair: frontier.repair,
            pending: frontier.pending,
            updates: frontier.updates,
            slice,
            read,
            topic_id,
            local,
            goal,
            scope,
            sent,
            window: self.page_actors,
            position_limit,
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
        }
    }

    pub(super) fn replay_page(
        &self,
        read: &dyn SnapshotRead,
        topic: &TopicId,
        frontier: &mut Frontier,
        budget: PageBudget,
        slice: &mut Slice,
    ) -> Result<PlannedPage> {
        slice.charge_preparation(
            frontier.offer_positions.len() + frontier.offer_missing.len(),
            super::space::tree_bytes::<ActorId, u64>(frontier.offer_positions.len())
                + super::space::tree_bytes::<OpId, ()>(frontier.offer_missing.len()),
        )?;
        let mut page = PlannedPage {
            more: true,
            missing: frontier.offer_missing.clone(),
            positions: frontier.offer_positions.clone(),
            ..PlannedPage::default()
        };
        let mut bytes = 0;
        while let Some(offer) = frontier.offered.get(frontier.replay).copied() {
            if page.ops.len() >= budget.ops || bytes >= budget.bytes {
                break;
            }
            if offer.held {
                if !slice.charge_actor() {
                    break;
                }
                if let Some(floor) = &mut frontier.offer_floor {
                    floor.observe(offer.actor, offer.seq);
                }
                frontier.replay += 1;
                continue;
            }
            if !frontier.records.contains(&offer.id) && !slice.charge_read() {
                break;
            }
            let record = match frontier.records.take(read, &offer.id, slice) {
                Ok(Some(record)) => record,
                Ok(None) => {
                    if page.missing.len() >= MAX_PAGE_MISSING && !page.missing.contains(&offer.id) {
                        return Err(Error::SyncCapacity(
                            "offered page missing records exceed result capacity".into(),
                        ));
                    }
                    page.missing.insert(offer.id);
                    break;
                }
                Err(super::records::LoadError::Yield) => break,
                Err(super::records::LoadError::Failed(error)) => return Err(error),
            };
            let body = &record.op.signed.body;
            if record.op.id != offer.id
                || body.actor_id != offer.actor
                || body.actor_seq != offer.seq
            {
                return Err(Error::InvalidOpId);
            }
            if body.topic_id != *topic {
                return Err(Error::TopicMismatch);
            }
            let size = postcard::experimental::serialized_size(&record.op)?;
            if size > budget.bytes - bytes {
                if page.ops.is_empty() {
                    page.too_large = Some(offer.id);
                }
                frontier.records.keep(record);
                break;
            }
            bytes += size;
            page.ops.push(record.into_op());
            frontier.replay += 1;
        }
        if frontier.replay == frontier.offered.len() {
            frontier.clear_offer();
        }
        page.continued = frontier.replaying
            && page.ops.is_empty()
            && page.missing.is_empty()
            && page.too_large.is_none();
        frontier.evictable = !page.ops.is_empty();
        if page.continued {
            self.work.ended();
        }
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oplog::Oplog;
    use crate::sync::{ActorRangeHint, SyncRequest};
    use crate::tests::support::*;

    fn wide_slices<S: Storage>(storage: S, unknown: bool) {
        let owner = Ed25519Signer::from_bytes(&[201; 32]);
        let peer = Ed25519Signer::from_bytes(&[202; 32]).peer_id();
        let writers = (10..34)
            .map(|seed| Ed25519Signer::from_bytes(&[seed; 32]))
            .collect::<Vec<_>>();
        let topic = TopicId::hash(b"wide-dependency-slices");
        let actor = actor_id_for(topic, owner.peer_id());
        let log = Oplog::with_storage(storage);
        let members = writers
            .iter()
            .map(Signer::peer_id)
            .chain([owner.peer_id(), peer]);
        let genesis = log
            .create_topic_genesis(
                topic,
                actor,
                TopicGenesis::new(Note::TYPE_ID, members),
                &owner,
            )
            .unwrap();
        let mut prefix = vec![genesis.clone()];
        for writer in &writers {
            let op = Op::sign(
                OpBody {
                    topic_id: topic,
                    author: writer.peer_id(),
                    actor_id: actor_id_for(topic, writer.peer_id()),
                    actor_seq: 1,
                    actor_prev: None,
                    deps: [genesis.id].into(),
                    generation: 1,
                    payload: TopicPayload::Event(
                        EventEnvelope::encode_event(&Note {
                            text: "root".into(),
                        })
                        .unwrap(),
                    ),
                },
                writer,
            )
            .unwrap();
            log.receive_ops(vec![op.clone()]).unwrap();
            prefix.push(op);
        }
        let join = log
            .create_event_op(
                topic,
                actor,
                EventEnvelope::encode_event(&Note {
                    text: "join".into(),
                })
                .unwrap(),
                &owner,
            )
            .unwrap();
        assert!(join.signed.body.deps.len() > 4);
        if unknown {
            let writer = &writers[0];
            let actor = actor_id_for(topic, writer.peer_id());
            let tip = log
                .create_event_op(
                    topic,
                    actor,
                    EventEnvelope::encode_event(&Note { text: "tip".into() }).unwrap(),
                    writer,
                )
                .unwrap();
            let responder = SyncEngine::new(log, owner.peer_id())
                .with_page_visits(4, 1)
                .with_page_positions(2);
            let receiver = Oplog::new();
            receiver.receive_ops(prefix.clone()).unwrap();
            let mut held = prefix.iter().map(|op| op.id).collect::<BTreeSet<_>>();
            let mut request = SyncRequest {
                topic_id: topic,
                known: BTreeSet::new(),
                wants: BTreeSet::new(),
                actor_range_hints: vec![ActorRangeHint {
                    actor_id: actor,
                    from_exclusive: 1,
                    to_inclusive: 2,
                }],
                genesis: Some(genesis.id),
                credit: Default::default(),
                window: crate::sync::ActorWindow {
                    after: Some(actor),
                    through: Some(actor),
                    behind: None,
                },
            };
            let goal = responder.summary(topic).unwrap().actor_clock;
            let mut complete = false;
            for _ in 0..512 {
                let before = responder.page_work();
                let page = responder
                    .response_page(
                        peer,
                        &request,
                        PageBudget {
                            ops: 1,
                            bytes: MAX_PAGE_BYTES,
                        },
                    )
                    .unwrap();
                let after = responder.page_work();
                assert!(after.visits - before.visits + after.actors - before.actors <= 4);
                assert!(after.edges - before.edges <= 4);
                assert!(page.positions.len() <= 2);
                assert!(page.missing.is_empty());
                assert!(!page.ops.is_empty() || page.continued || !page.positions.is_empty());
                for op in &page.ops {
                    assert!(op.signed.body.deps.is_subset(&held));
                    assert!(held.insert(op.id));
                }
                receiver.receive_ops(page.ops).unwrap();
                let clock = receiver.storage().actor_clock(&topic).unwrap();
                for actor in page.positions {
                    if !request
                        .actor_range_hints
                        .iter()
                        .any(|hint| hint.actor_id == actor)
                    {
                        request.actor_range_hints.push(ActorRangeHint {
                            actor_id: actor,
                            from_exclusive: clock.get(&actor),
                            to_inclusive: goal.get(&actor),
                        });
                    }
                }
                for hint in &mut request.actor_range_hints {
                    hint.from_exclusive = clock.get(&hint.actor_id);
                }
                if !page.more {
                    complete = true;
                    break;
                }
            }
            assert!(complete, "unknown ancestry failed to complete");
            assert!(held.contains(&join.id) && held.contains(&tip.id));
            assert_eq!(responder.page_work().kept_bytes, 0);
            return;
        }
        for informed in [false, true] {
            let responder = SyncEngine::new(log.clone(), owner.peer_id()).with_page_visits(4, 1);
            let receiver = Oplog::new();
            receiver.receive_ops(prefix.clone()).unwrap();
            let held = SyncEngine::new(receiver.clone(), peer)
                .summary(topic)
                .unwrap();
            let request = SyncRequest {
                topic_id: topic,
                known: BTreeSet::new(),
                wants: BTreeSet::new(),
                actor_range_hints: vec![ActorRangeHint {
                    actor_id: actor,
                    from_exclusive: 1,
                    to_inclusive: 2,
                }],
                genesis: Some(genesis.id),
                credit: Default::default(),
                window: Default::default(),
            };
            let mut complete = false;
            for _ in 0..32 {
                let before = responder.page_work();
                let budget = PageBudget {
                    ops: 1,
                    bytes: MAX_PAGE_BYTES,
                };
                let page = if informed {
                    responder.response_with(peer, &request, budget, &held)
                } else {
                    responder.response_page(peer, &request, budget)
                }
                .unwrap();
                let after = responder.page_work();
                assert!(after.edges - before.edges <= 4);
                assert!(after.visits - before.visits <= 4);
                assert!(
                    after.kept_bytes <= super::super::slice::MAX_CONTINUATION_BYTES as u64 + 65536
                );
                if page.ops.is_empty() {
                    assert!(page.more && page.continued);
                    continue;
                }
                assert_eq!(page.ops, vec![join.clone()]);
                assert!(!page.more && !page.continued);
                receiver.receive_ops(page.ops).unwrap();
                complete = true;
                break;
            }
            assert!(complete);
            assert!(responder.page_work().resumed > 0);
        }
    }

    #[test]
    fn memory_unknown_slices() {
        wide_slices(MemoryStorage::new(), true);
    }

    #[cfg(feature = "fjall")]
    #[test]
    fn fjall_unknown_slices() {
        let directory = tempfile::tempdir().unwrap();
        wide_slices(
            crate::storage::FjallStorage::open(directory.path()).unwrap(),
            true,
        );
    }

    #[test]
    fn memory_wide_slices() {
        wide_slices(MemoryStorage::new(), false);
    }

    #[cfg(feature = "fjall")]
    #[test]
    fn fjall_wide_slices() {
        let directory = tempfile::tempdir().unwrap();
        wide_slices(
            crate::storage::FjallStorage::open(directory.path()).unwrap(),
            false,
        );
    }
}
