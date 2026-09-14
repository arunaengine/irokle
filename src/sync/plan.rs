// SPDX-License-Identifier: MIT OR Apache-2.0
//! Bounded forward page planning over one read context: actor ranges merged by
//! generation, with waiting heads suspended outside the active set.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};

use crate::storage::{SnapshotRead, Storage};
use crate::{ActorClock, ActorId, Error, OpId, Result, TopicId};

use super::{
    ActorScope, MAX_PAGE_BYTES, MAX_PAGE_MISSING, PageBudget, PlannedPage, RangeHead, SyncEngine,
};

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

/// What an op still needs before it can be sent.
enum Wait {
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
    read: &'a dyn SnapshotRead,
    topic_id: &'a TopicId,
    local: &'a ActorClock,
    goal: Option<&'a ActorClock>,
    /// Which peer positions the request gave, and ids already sent before this plan.
    scope: &'a ActorScope<'a>,
    sent: &'a BTreeSet<OpId>,
    window: usize,
    active: BinaryHeap<Reverse<RangeHead>>,
    metas: BTreeMap<OpId, crate::storage::OpMeta>,
    /// Actors behind the goal not activated yet, in clock order.
    deferred: VecDeque<ActorId>,
    /// Suspended heads by the actor they wait for, with the position needed.
    suspended: BTreeMap<ActorId, Vec<(u64, RangeHead)>>,
    /// Suspended heads whose position was sent, waiting for a free slot.
    resumable: VecDeque<RangeHead>,
    states: BTreeMap<ActorId, ActorState>,
    covered: ActorClock,
    blocked: BTreeSet<OpId>,
    missing: BTreeSet<OpId>,
    positions: BTreeSet<ActorId>,
    more: bool,
}

impl Pager<'_> {
    /// The highest position of `actor_id` the goal asks for.
    fn limit(&self, actor_id: &ActorId) -> u64 {
        let local_seq = self.local.get(actor_id);
        self.goal
            .map_or(local_seq, |goal| goal.get(actor_id).min(local_seq))
    }

    fn plan(mut self, budget: PageBudget) -> Result<PlannedPage> {
        let behind = self
            .local
            .iter()
            .filter(|(actor_id, _)| self.limit(actor_id) > self.covered.get(actor_id))
            .map(|(actor_id, _)| *actor_id)
            .collect::<VecDeque<_>>();
        // A zero allowance reads nothing; whether the goal holds more is known
        // from the clocks alone.
        if budget.ops == 0 || budget.bytes == 0 {
            return Ok(PlannedPage {
                more: !behind.is_empty(),
                ..PlannedPage::default()
            });
        }
        self.deferred = behind;
        let mut ops = Vec::new();
        let mut too_large = None;
        let mut bytes = 0_usize;
        loop {
            self.fill()?;
            let Some(Reverse(head)) = self.active.pop() else {
                break;
            };
            let (_, actor_id, seq, id, limit) = head;
            let meta = self.metas.remove(&id).ok_or(Error::MissingDependency(id))?;
            match self.wait_for(&meta)? {
                Wait::Ready => {}
                Wait::Blocked => {
                    self.block(actor_id, id);
                    continue;
                }
                Wait::Position(dep_actor, dep_seq) => {
                    self.suspend(head, dep_actor, dep_seq)?;
                    continue;
                }
            }
            let Some(op) = self.read.get_op(&id)? else {
                self.missing.insert(id);
                self.block(actor_id, id);
                continue;
            };
            let size = postcard::experimental::serialized_size(&op)?;
            if size > MAX_PAGE_BYTES {
                return Err(Error::Storage("operation exceeds sync page budget".into()));
            }
            if ops.len() >= budget.ops || bytes + size > budget.bytes {
                if ops.is_empty() && size > budget.bytes {
                    too_large = Some(id);
                }
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
            || !self.active.is_empty()
            || !self.resumable.is_empty()
            || self.suspended.values().any(|waiting| !waiting.is_empty())
            || self
                .deferred
                .iter()
                .any(|actor_id| !self.states.contains_key(actor_id));
        let (mut missing, mut positions) = (self.missing, self.positions);
        while missing.len() > MAX_PAGE_MISSING {
            missing.pop_last();
        }
        while positions.len() > MAX_PAGE_MISSING {
            positions.pop_last();
        }
        Ok(PlannedPage {
            ops,
            more,
            missing,
            too_large,
            positions,
        })
    }

    /// Fill free slots: resumed heads first, then deferred actors in order.
    fn fill(&mut self) -> Result<()> {
        while self.active.len() < self.window {
            if let Some(head) = self.resumable.pop_front() {
                let (_, actor_id, _, id, _) = head;
                let Some(meta) = self.read.get_meta(&id)? else {
                    self.missing.insert(id);
                    self.block(actor_id, id);
                    continue;
                };
                self.metas.insert(id, meta);
                self.active.push(Reverse(head));
                self.states.insert(actor_id, ActorState::Active);
                continue;
            }
            let Some(actor_id) = self.deferred.pop_front() else {
                return Ok(());
            };
            if self.states.contains_key(&actor_id) {
                continue;
            }
            let limit = self.limit(&actor_id);
            self.activate(actor_id, self.covered.get(&actor_id), limit)?;
        }
        Ok(())
    }

    /// Queue the op after `after` on `actor_id`, up to `limit`; callers keep
    /// `after < limit`. A gap in the index stops the actor and names the
    /// record the next indexed op follows.
    fn activate(&mut self, actor_id: ActorId, after: u64, limit: u64) -> Result<()> {
        let next = self
            .read
            .actor_range(self.topic_id, &actor_id, after, 1)?
            .pop();
        // The clock is ahead of the index, so no id names the lost position.
        let Some((seq, id)) = next else {
            self.stop(actor_id);
            return Ok(());
        };
        let meta = self.read.get_meta(&id)?;
        if seq != after + 1 || meta.is_none() {
            match meta.filter(|_| seq != after + 1) {
                Some(meta) => self.missing.extend(meta.actor_prev),
                None => {
                    self.missing.insert(id);
                }
            }
            self.stop(actor_id);
            return Ok(());
        }
        if let Some(meta) = meta {
            self.active
                .push(Reverse((meta.generation, actor_id, seq, id, limit)));
            self.metas.insert(id, meta);
            self.states.insert(actor_id, ActorState::Active);
        }
        Ok(())
    }

    /// What `meta` still waits for: an unsent or blocked dependency, a missing
    /// record, positions of actors the request did not describe, all named at
    /// once, or a position of another actor the peer does not hold yet.
    fn wait_for(&mut self, meta: &crate::storage::OpMeta) -> Result<Wait> {
        let mut unknown = false;
        let mut waits = None;
        for dep in &meta.deps {
            if self.blocked.contains(dep) {
                return Ok(Wait::Blocked);
            }
            if self.sent.contains(dep) {
                continue;
            }
            let position = match self.metas.get(dep) {
                Some(dep_meta) => Some((dep_meta.actor_id, dep_meta.actor_seq)),
                None => self
                    .read
                    .get_meta(dep)?
                    .map(|dep_meta| (dep_meta.actor_id, dep_meta.actor_seq)),
            };
            let Some((dep_actor, dep_seq)) = position else {
                self.missing.insert(*dep);
                return Ok(Wait::Blocked);
            };
            // Omitted from the request is not held: the requester names it next.
            if self.scope.unknown(&dep_actor) {
                self.positions.insert(dep_actor);
                unknown = true;
            } else if waits.is_none() && self.covered.get(&dep_actor) < dep_seq {
                waits = Some(Wait::Position(dep_actor, dep_seq));
            }
        }
        Ok(match (unknown, waits) {
            (true, _) => Wait::Blocked,
            (false, Some(waits)) => waits,
            (false, None) => Wait::Ready,
        })
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
                if limit >= dep_seq || dep_seq > self.local.get(&dep_actor) {
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
            .filter(|needed| *needed > limit && *needed <= local_seq)
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
    /// actors behind, never with history the peer holds. See [`Pager`].
    pub(crate) fn plan_page(
        &self,
        read: &dyn SnapshotRead,
        topic_id: &TopicId,
        (local, peer, goal): (&ActorClock, &ActorClock, Option<&ActorClock>),
        (scope, sent): (&ActorScope<'_>, &BTreeSet<OpId>),
        excluded: &BTreeSet<OpId>,
        budget: PageBudget,
    ) -> Result<PlannedPage> {
        let pager = Pager {
            read,
            topic_id,
            local,
            goal,
            scope,
            sent,
            window: self.page_actors,
            active: BinaryHeap::new(),
            metas: BTreeMap::new(),
            deferred: VecDeque::new(),
            suspended: BTreeMap::new(),
            resumable: VecDeque::new(),
            states: BTreeMap::new(),
            covered: peer.clone(),
            blocked: excluded.clone(),
            missing: BTreeSet::new(),
            positions: BTreeSet::new(),
            more: false,
        };
        pager.plan(budget)
    }
}
