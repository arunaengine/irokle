// SPDX-License-Identifier: MIT OR Apache-2.0
//! Transport-neutral sync messages, planning, acknowledgements, and reports.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use smallvec::SmallVec;

use crate::oplog::{Integrity, Oplog, ReceiveEffects, TopicEviction, subset_in};
use crate::storage::{SnapshotCharge, SnapshotRead, Storage, TopicView, topic_fingerprint_for};
use crate::{ActorClock, ActorId, Error, Op, OpId, PeerId, Result, TopicId, canonical_bytes};

mod continuation;
mod evidence;
mod plan;
mod records;
mod repair;
mod request;
mod slice;
mod space;
#[cfg(feature = "iroh")]
mod transport;
mod types;

#[cfg(test)]
pub(crate) use continuation::MAX_CONTINUATIONS;
use continuation::{Continuation, Continuations};
pub use request::RequestKnowledge;
use request::{ActorScope, request_ranges};
#[cfg(test)]
pub(crate) use slice::PageWorkSnapshot;
use slice::{MAX_PAGE_VISITS, PageWork};
pub use types::{
    ActorFilter, ActorRangeHint, ActorWindow, PageBudget, SyncAck, SyncCredit, SyncData,
    SyncFailure, SyncFailureCode, SyncFingerprint, SyncMessage, SyncOpen, SyncPage, SyncPlan,
    SyncReceipt, SyncReport, SyncRequest, SyncSummary,
};

const ACK_SIGNING_DOMAIN: &[u8] = b"irokle/sync-ack/2";

/// Wire contract this build speaks. Version 2 bounds the actors a request describes by a
/// window and names the positions a page needed. Older peers are refused before any message.
pub const SYNC_PROTOCOL: &str = "irokle/sync/2";

/// Actors a topic may hold before a member's first write is refused, so its summary
/// still fits one sync frame. Admission does not refuse: that would make the
/// accepted set depend on arrival order.
pub const MAX_TOPIC_ACTORS: usize = 100_000;

/// Maximum sequences a single `ActorRangeHint` may span. Hints built by `request_ranges` stay
/// within it, and every response clamps peer hints to it, so a malicious peer cannot make us
/// walk unbounded sequence ranges.
pub const MAX_ACTOR_RANGE_HINT_SPAN: u64 = 65_536;
/// Wants and range hints one request may carry together.
const MAX_REQUEST_ITEMS: usize = 65_536;
/// Bytes of the filter of actors behind that a request leaves out; past it the
/// request sends none and those actors stay unknown.
pub const MAX_ACTOR_FILTER_BYTES: usize = 1024 * 1024;
pub(crate) use self::{
    MAX_ACTOR_FILTER_BYTES as MAX_FILTER_BYTES, MAX_ACTOR_RANGE_HINT_SPAN as MAX_RANGE_SPAN,
};
const MAX_PAGE_OPS: usize = 4096;
pub(crate) const MAX_PAGE_BYTES: usize = 32 * 1024 * 1024;
/// Actors one page plan keeps active range heads for; the rest wait for a free
/// slot in the same page or for a later page.
const MAX_PAGE_ACTORS: usize = 4096;
/// Records one page names as missing, at most.
pub const MAX_PAGE_MISSING: usize = 256;
/// Raw and workspace bytes, with one authorization read, that every response charges for its
/// snapshot, also when a caller opened it. Fjall's two record envelopes stop 1024 bytes short
/// of the capture limit, which leaves room for this charge and the epoch record.
const SNAPSHOT_OPEN_BYTES: usize = 512;

/// A queued range position: generation, actor, sequence, id and range limit.
type RangeHead = (u64, ActorId, u64, OpId, u64);

/// One planned page and whether the goal still holds more after it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PlannedPage {
    /// A causal prefix: every op's dependencies precede it or the peer holds them.
    pub ops: Vec<Op>,
    /// Whether this store holds more of the goal than the page carries.
    pub more: bool,
    /// Records the goal depends on that this store does not hold, at most
    /// [`MAX_PAGE_MISSING`]. No later page from this store carries them.
    pub missing: BTreeSet<OpId>,
    /// An operation that alone exceeds the page's byte budget.
    pub too_large: Option<OpId>,
    /// Actors the request did not describe whose positions the page needed,
    /// at most [`MAX_PAGE_MISSING`]. Their dependents wait for a request naming them.
    pub positions: BTreeSet<ActorId>,
    /// The page ended its work slice before it could send anything and this
    /// store kept its plan: the same request goes on from it.
    pub continued: bool,
}

#[derive(Clone)]
pub struct SyncEngine<S> {
    oplog: Oplog<S>,
    peer_id: PeerId,
    /// Active actors of one page plan; tests scale it down.
    page_actors: usize,
    /// Wants and hints a request may carry; tests scale it down.
    request_items: usize,
    /// Storage reads of one page slice; tests scale it down.
    page_visits: usize,
    /// Positions one page result names; tests scale it down.
    page_positions: usize,
    /// Plans kept across slices, shared by the engine's clones.
    continuations: Arc<Mutex<Continuations>>,
    work: Arc<PageWork>,
}

/// Which operation bodies a negotiation materializes into `SyncPlan::send`.
#[derive(Clone, Copy)]
enum SendSet {
    Closure,
    Page(PageBudget),
}

impl<S: Storage> SyncEngine<S> {
    pub fn new(oplog: Oplog<S>, peer_id: PeerId) -> Self {
        Self {
            oplog,
            peer_id,
            page_actors: MAX_PAGE_ACTORS,
            request_items: MAX_REQUEST_ITEMS,
            page_visits: MAX_PAGE_VISITS,
            page_positions: MAX_PAGE_MISSING,
            continuations: Arc::new(Mutex::new(Continuations::new(
                continuation::MAX_CONTINUATIONS,
            ))),
            work: Arc::default(),
        }
    }

    fn continuations(&self) -> MutexGuard<'_, Continuations> {
        // Kept plans are only a shortcut; a poisoned store still holds valid ones.
        self.continuations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Work page plans of this engine and its clones performed so far.
    #[cfg(test)]
    pub(crate) fn page_work(&self) -> PageWorkSnapshot {
        self.work.snapshot(self.continuations().bytes())
    }

    /// The same engine ending a page slice after `visits` storage reads and
    /// keeping at most `kept` plans.
    #[cfg(test)]
    pub(crate) fn with_page_visits(mut self, visits: usize, kept: usize) -> Self {
        self.page_visits = visits.max(1);
        self.continuations = Arc::new(Mutex::new(Continuations::new(kept)));
        self
    }

    /// The same engine planning pages with at most `actors` active actors.
    #[cfg(test)]
    pub(crate) fn with_page_actors(mut self, actors: usize) -> Self {
        self.page_actors = actors.max(1);
        self
    }

    /// The same engine naming at most `positions` needed positions per page.
    #[cfg(test)]
    pub(crate) fn with_page_positions(mut self, positions: usize) -> Self {
        self.page_positions = positions.max(1);
        self
    }

    /// The same engine building and accepting requests of at most `items` wants and hints.
    #[cfg(test)]
    pub(crate) fn with_request_items(mut self, items: usize) -> Self {
        // One hint names a needed position, the other an actor behind.
        self.request_items = items.max(2);
        self
    }
    pub fn open(topic_id: TopicId, peer_id: PeerId, event_type_id: Option<String>) -> SyncOpen {
        SyncOpen {
            protocol: SYNC_PROTOCOL.into(),
            topic_id,
            peer_id,
            event_type_id,
        }
    }

    /// The topic as one snapshot reads it. Heads, clock, tips and the digest
    /// all describe the same commit, so no part can come from a replaced branch.
    /// The integrity scan finishes first, one bounded step per snapshot.
    pub fn summary(&self, topic_id: TopicId) -> Result<SyncSummary> {
        match self.oplog.inspect(&topic_id)? {
            Some((view, integrity)) => Self::summary_for(view, &integrity),
            None => Self::unknown_summary(topic_id),
        }
    }

    /// The summary of `view`, digested with its topic's `integrity`.
    pub(crate) fn summary_for(view: TopicView, integrity: &Integrity) -> Result<SyncSummary> {
        let fingerprint = digest_for(&view, integrity)?;
        let actor_tips = view
            .tips
            .iter()
            .filter(|(actor_id, (seq, _))| view.clock.get(actor_id) == *seq)
            .map(|(actor_id, tip)| (*actor_id, *tip))
            .collect();
        Ok(SyncSummary {
            topic_id: view.state.topic_id,
            genesis: Some(view.state.genesis),
            event_type_id: Some(view.state.event_type_id),
            fingerprint,
            heads: view.state.heads,
            actor_clock: view.clock,
            actor_tips,
            staged: None,
        })
    }

    fn unknown_summary(topic_id: TopicId) -> Result<SyncSummary> {
        Ok(SyncSummary {
            topic_id,
            event_type_id: None,
            genesis: None,
            fingerprint: topic_fingerprint_for(&BTreeSet::new(), &ActorClock::new())?,
            heads: BTreeSet::new(),
            actor_clock: ActorClock::new(),
            actor_tips: BTreeMap::new(),
            staged: None,
        })
    }

    pub fn fingerprint(&self, topic_id: TopicId) -> Result<SyncFingerprint> {
        let fingerprint = match self.oplog.inspect(&topic_id)? {
            Some((view, integrity)) => digest_for(&view, &integrity)?,
            None => topic_fingerprint_for(&BTreeSet::new(), &ActorClock::new())?,
        };
        Ok(SyncFingerprint {
            topic_id,
            fingerprint,
        })
    }

    /// The whole missing closure against `remote`, unbounded: an export of
    /// history, not a sync step. Sync uses [`Self::negotiate_page`].
    pub fn negotiate(&self, peer_id: PeerId, remote: &SyncSummary) -> Result<SyncPlan> {
        self.oplog.storage().read_snapshot(|read| {
            let knowledge = RequestKnowledge::default();
            let closure = SendSet::Closure;
            let (plan, _) =
                self.negotiate_inner(read, peer_id, remote, closure, &knowledge, &mut false)?;
            Ok(plan)
        })
    }

    /// Plan a causal push page within `budget` and a request for what the peer holds beyond
    /// this node, plus whether the push holds more. Zero budget reads no operation; another
    /// genesis is planned as a branch, never by comparing positions across branches.
    pub fn negotiate_page(
        &self,
        peer_id: PeerId,
        remote: &SyncSummary,
        budget: PageBudget,
    ) -> Result<(SyncPlan, bool)> {
        let knowledge = RequestKnowledge::default();
        self.oplog
            .storage()
            .read_snapshot(|read| self.negotiate_in(read, peer_id, remote, budget, &knowledge))
    }

    /// [`Self::negotiate_page`] over a snapshot the caller already holds, with
    /// the request continuing from `knowledge`.
    pub(crate) fn negotiate_in(
        &self,
        read: &dyn SnapshotRead,
        peer_id: PeerId,
        remote: &SyncSummary,
        budget: PageBudget,
        knowledge: &RequestKnowledge,
    ) -> Result<(SyncPlan, bool)> {
        let mut more = false;
        let send_set = SendSet::Page(budget);
        let (plan, _) =
            self.negotiate_inner(read, peer_id, remote, send_set, knowledge, &mut more)?;
        Ok((plan, more))
    }

    /// Authorization, branch choice and selection all read `read`, so a plan
    /// never pairs a membership verdict with records of a later commit. Also
    /// returns the local genesis the plan read, if the topic is held.
    fn negotiate_inner(
        &self,
        read: &dyn SnapshotRead,
        peer_id: PeerId,
        remote: &SyncSummary,
        send_set: SendSet,
        knowledge: &RequestKnowledge,
        more: &mut bool,
    ) -> Result<(SyncPlan, Option<OpId>)> {
        // An unknown topic's remote heads are unauthenticated, so they never become
        // `need`. Bootstrap stages pages the inviter pushes or the transport pulls
        // with range hints, which the responder clamps and serves to members only.
        let Some(view) = read.topic_view(&remote.topic_id, None)? else {
            let plan = SyncPlan {
                topic_id: remote.topic_id,
                common: BTreeSet::new(),
                have: BTreeSet::new(),
                send: Vec::new(),
                need: BTreeSet::new(),
                actor_range_hints: Vec::new(),
                window: ActorWindow::default(),
            };
            return Ok((plan, None));
        };
        let genesis = Some(view.state.genesis);
        if !view.state.members.contains(&peer_id) {
            let plan = SyncPlan {
                topic_id: remote.topic_id,
                common: BTreeSet::new(),
                have: view.state.heads,
                send: Vec::new(),
                need: BTreeSet::new(),
                actor_range_hints: Vec::new(),
                window: ActorWindow::default(),
            };
            return Ok((plan, genesis));
        }
        if let Some(remote_type_id) = &remote.event_type_id
            && *remote_type_id != view.state.event_type_id
        {
            return Err(Error::EventTypeMismatch {
                expected: view.state.event_type_id,
                actual: remote_type_id.clone(),
            });
        }

        let local_heads = view.state.heads.clone();
        // The push page and the request's reads below share one slice.
        let mut slice = slice::Slice::new(Arc::clone(&self.work), self.page_visits, 0)?;
        // Another genesis is another sequence namespace, however equal the actor
        // positions look. The smaller genesis wins: its holder offers its branch
        // from the start and the other side asks for that branch from the start.
        if let Some(remote_genesis) = remote.genesis
            && remote_genesis != view.state.genesis
        {
            let empty = ActorClock::new();
            let mut plan = SyncPlan {
                topic_id: remote.topic_id,
                common: BTreeSet::new(),
                have: local_heads,
                send: Vec::new(),
                need: BTreeSet::new(),
                actor_range_hints: Vec::new(),
                window: ActorWindow::default(),
            };
            *more = false;
            if remote_genesis < view.state.genesis {
                (plan.actor_range_hints, plan.window) =
                    request_ranges(&empty, &remote.actor_clock, self.request_items, knowledge);
                return Ok((plan, genesis));
            }
            plan.send = match send_set {
                SendSet::Closure => self.missing_closure_in(
                    read,
                    &SyncSummary {
                        topic_id: remote.topic_id,
                        event_type_id: None,
                        genesis: None,
                        fingerprint: [0; 32],
                        heads: BTreeSet::new(),
                        actor_clock: empty,
                        actor_tips: BTreeMap::new(),
                        staged: None,
                    },
                    &view.state.heads,
                )?,
                SendSet::Page(budget) => {
                    let (page, _, _) = self.plan_page(
                        read,
                        &remote.topic_id,
                        (&view.clock, &empty, None),
                        (&ActorScope::whole(), &BTreeSet::new()),
                        &BTreeSet::new(),
                        (budget, &mut slice),
                    )?;
                    *more = page.more;
                    page.ops
                }
            };
            return Ok((plan, genesis));
        }
        // A hole moves neither heads nor the clock, so a matching fingerprint does
        // not prove we are whole; keep negotiating until a complete scan finds none.
        let integrity = self.oplog.integrity_in(read, &view)?;
        let unresolved = integrity.unresolved(&view);
        if integrity.certifies(&view) && view.fingerprint == remote.fingerprint {
            let plan = SyncPlan {
                topic_id: remote.topic_id,
                common: local_heads.clone(),
                have: local_heads,
                send: Vec::new(),
                need: BTreeSet::new(),
                actor_range_hints: Vec::new(),
                window: ActorWindow::default(),
            };
            return Ok((plan, genesis));
        }

        // A page plan needs no walk from the heads: the peer's clock says what
        // it holds, and holes come from the integrity check above.
        let (common, dangling) = match send_set {
            SendSet::Closure => Self::survey_in(read, remote, &view.state.heads)?,
            SendSet::Page(_) => Default::default(),
        };
        let send = match send_set {
            SendSet::Closure => self.missing_closure_in(read, remote, &view.state.heads)?,
            SendSet::Page(budget) => {
                let (page, _, _) = self.plan_page(
                    read,
                    &remote.topic_id,
                    (&view.clock, &remote.actor_clock, None),
                    (&ActorScope::whole(), &BTreeSet::new()),
                    &BTreeSet::new(),
                    (budget, &mut slice),
                )?;
                *more = page.more;
                page.ops
            }
        };
        let mut need = BTreeSet::new();
        for id in &remote.heads {
            // Heads past the slice hide no work: one ahead of this clock is reached by ranges,
            // and one at or behind it is held, a hole the scan above found, or a fork.
            if !slice.charge_read() {
                break;
            }
            if !read.dep_resolvable(id)? {
                need.insert(*id);
            }
        }
        // Request every referenced but unresolved ID: dangling edges, partial records,
        // and holes blocking buffered ops. Ordinary sync can then repair dependencies
        // instead of deferring their dependents forever.
        let repair = dangling
            .into_iter()
            .chain(unresolved.keys().copied())
            .collect::<BTreeSet<_>>();
        if !repair.is_empty() {
            tracing::debug!(
                topic_id = %remote.topic_id,
                dangling = repair.len(),
                "requesting unresolved dependencies from peer"
            );
            need.extend(repair);
        }
        // A page request reaches a remote tip ahead of this clock through its
        // actor's ranges, over as many pages as the gap needs; only a head at or
        // behind the local position is a hole and stays an explicit want.
        if matches!(send_set, SendSet::Page(_)) {
            for (actor_id, (seq, tip)) in &remote.actor_tips {
                if need.is_empty() {
                    break;
                }
                self.work.tip();
                if view.clock.get(actor_id) < *seq {
                    need.remove(tip);
                }
            }
        }
        // Leave room for repair state, needed positions and one forward hint.
        // Unselected roots remain unresolved and follow once this window resolves.
        let behind = remote
            .actor_clock
            .iter()
            .any(|(actor_id, seq)| *seq > view.clock.get(actor_id));
        let reserved = if behind { 1 + knowledge.positions() } else { 0 };
        let limit = self
            .request_items
            .saturating_sub(reserved)
            .min(repair::Repair::root_limit());
        let need = if need.len() > limit {
            // Known ancestors must precede descendants across repair windows. The hole scan
            // read every stored position's generation, so ordering reads no headers.
            let mut ordered = BTreeSet::new();
            for id in need {
                let generation = unresolved.get(&id).copied().flatten();
                ordered.insert((generation.is_none(), generation, id));
                if ordered.len() > limit {
                    ordered.pop_last();
                }
            }
            ordered.into_iter().map(|(_, _, id)| id).collect()
        } else {
            need
        };
        let items = self.request_items - need.len();
        let (actor_range_hints, window) =
            request_ranges(&view.clock, &remote.actor_clock, items, knowledge);
        let plan = SyncPlan {
            topic_id: remote.topic_id,
            common,
            have: local_heads,
            send,
            need,
            actor_range_hints,
            window,
        };
        Ok((plan, genesis))
    }

    pub fn find_common_ancestors(&self, remote: &SyncSummary) -> Result<BTreeSet<OpId>> {
        self.oplog.storage().read_snapshot(|read| {
            let heads = read
                .topic_view(&remote.topic_id, None)?
                .map(|view| view.state.heads)
                .unwrap_or_default();
            Ok(Self::survey_in(read, remote, &heads)?.0)
        })
    }

    /// Walk local heads to the remote frontier, reporting common ancestors and ids found
    /// stored incompletely. The second set feeds anti-entropy repair; collecting it here costs
    /// no extra traversal.
    fn survey_in(
        read: &dyn SnapshotRead,
        remote: &SyncSummary,
        heads: &BTreeSet<OpId>,
    ) -> Result<(BTreeSet<OpId>, BTreeSet<OpId>)> {
        let mut common = BTreeSet::new();
        let mut dangling = BTreeSet::new();
        let mut queue: VecDeque<_> = heads.iter().copied().collect();
        let mut seen = BTreeSet::new();

        while let Some(id) = queue.pop_front() {
            if !seen.insert(id) {
                continue;
            }
            let Some(meta) = read.get_position(&id)? else {
                dangling.insert(id);
                continue;
            };
            if meta.topic_id != remote.topic_id {
                continue;
            }
            // Metadata alone lets the walk continue but cannot be served, so the
            // op record is requested while the traversal still uses the meta.
            if read.get_op(&id)?.is_none() {
                dangling.insert(id);
            }
            if remote_contains(remote, &id, &meta) {
                common.insert(id);
                continue;
            }
            queue.extend(meta.deps.iter().copied());
        }

        Ok((common, dangling))
    }

    pub fn missing_closure(&self, remote: &SyncSummary) -> Result<Vec<Op>> {
        self.oplog.storage().read_snapshot(|read| {
            let heads = read
                .topic_view(&remote.topic_id, None)?
                .map(|view| view.state.heads)
                .unwrap_or_default();
            self.missing_closure_in(read, remote, &heads)
        })
    }

    fn missing_closure_in(
        &self,
        read: &dyn SnapshotRead,
        remote: &SyncSummary,
        heads: &BTreeSet<OpId>,
    ) -> Result<Vec<Op>> {
        let mut missing = BTreeSet::new();
        let mut stack: SmallVec<[OpId; 8]> = heads.iter().copied().collect();
        while let Some(id) = stack.pop() {
            if missing.contains(&id) {
                continue;
            }
            let Some(meta) = read.get_position(&id)? else {
                continue;
            };
            if meta.topic_id != remote.topic_id || remote_contains(remote, &id, &meta) {
                continue;
            }
            missing.insert(id);
            stack.extend(meta.deps.iter().copied());
        }
        subset_in(read, &missing)
    }

    pub fn plan_data(&self, peer_id: PeerId, remote: &SyncSummary) -> Result<SyncData> {
        let ops = self.negotiate(peer_id, remote)?.send;
        Ok(SyncData {
            topic_id: remote.topic_id,
            ops,
        })
    }

    /// The request for what `remote` holds beyond this node: explicit wants for
    /// holes and ranges for positions ahead, with a credit sized to them. It
    /// walks no local history. The request names the branch it plans on.
    pub fn plan_request(&self, peer_id: PeerId, remote: &SyncSummary) -> Result<SyncRequest> {
        self.oplog
            .storage()
            .read_snapshot(|read| self.request_in(read, peer_id, remote, &Default::default()))
    }

    /// [`Self::plan_request`] continuing from what earlier page results left in
    /// `knowledge`.
    pub fn plan_request_with(
        &self,
        peer_id: PeerId,
        remote: &SyncSummary,
        knowledge: &RequestKnowledge,
    ) -> Result<SyncRequest> {
        self.oplog
            .storage()
            .read_snapshot(|read| self.request_in(read, peer_id, remote, knowledge))
    }

    /// [`Self::plan_request`] over a snapshot the caller already holds; the
    /// branch it names is the one that snapshot planned on.
    pub(crate) fn request_in(
        &self,
        read: &dyn SnapshotRead,
        peer_id: PeerId,
        remote: &SyncSummary,
        knowledge: &RequestKnowledge,
    ) -> Result<SyncRequest> {
        let no_push = SendSet::Page(PageBudget { ops: 0, bytes: 0 });
        let (plan, genesis) =
            self.negotiate_inner(read, peer_id, remote, no_push, knowledge, &mut false)?;
        let genesis = genesis.map(|genesis| request_genesis(genesis, remote.genesis));
        Ok(page_request(plan, genesis))
    }

    /// One page of `request` within its credit: [`Self::response_page`]
    /// without the page result. Use that method to learn whether more remains.
    pub fn plan_response_data(&self, peer_id: PeerId, request: &SyncRequest) -> Result<SyncData> {
        let page = self.response_page(peer_id, request, PageBudget::from_credit(request.credit))?;
        Ok(SyncData {
            topic_id: request.topic_id,
            ops: page.ops,
        })
    }

    /// Serve one causal page of `request` within the smaller of `budget` and request credit:
    /// explicit wants, then ranges, while refusing another genesis. `more` means the goal holds
    /// more and the requester asks again; one snapshot supplies membership and every record.
    pub fn response_page(
        &self,
        peer_id: PeerId,
        request: &SyncRequest,
        budget: PageBudget,
    ) -> Result<PlannedPage> {
        self.oplog
            .storage()
            .read_snapshot(|read| self.response_known(read, peer_id, request, budget, None))
    }

    /// Serve using the authenticated peer's current summary, never as an ACK.
    pub fn response_with(
        &self,
        peer_id: PeerId,
        request: &SyncRequest,
        budget: PageBudget,
        summary: &SyncSummary,
    ) -> Result<PlannedPage> {
        self.oplog.storage().read_snapshot(|read| {
            self.response_known(read, peer_id, request, budget, Some(summary))
        })
    }

    fn response_known(
        &self,
        read: &dyn SnapshotRead,
        peer_id: PeerId,
        request: &SyncRequest,
        budget: PageBudget,
        summary: Option<&SyncSummary>,
    ) -> Result<PlannedPage> {
        // Every local root drops before this active reservation, including on errors.
        let mut clocks = None;
        // Every entry point admits its snapshot here, whether it opened it or a caller did.
        let mut slice = slice::Slice::new(Arc::clone(&self.work), self.page_visits, 0)?;
        slice.charge_authorization()?;
        slice.charge_capture(0, SNAPSHOT_OPEN_BYTES, SNAPSHOT_OPEN_BYTES)?;
        let empty = PlannedPage::default();
        let filter = request.window.behind.as_ref();
        if request
            .actor_range_hints
            .len()
            .saturating_add(request.wants.len())
            > self.request_items
            || filter.is_some_and(|filter| filter.bits.len() > MAX_FILTER_BYTES)
        {
            return Err(Error::SyncCapacity(
                "reduce the request's wants, actor hints or filter".into(),
            ));
        }
        let hints = request.actor_range_hints.len();
        let wanted = request.wants.len();
        let input_work = hints
            .saturating_mul(16)
            .saturating_add(wanted.saturating_mul(8))
            .saturating_add(8 * (MAX_PAGE_OPS + MAX_PAGE_MISSING));
        let input_bytes = space::tree_bytes::<ActorId, u64>(hints)
            .saturating_mul(5)
            .saturating_add(space::tree_bytes::<ActorId, ()>(hints))
            .saturating_add(space::tree_bytes::<ActorId, Option<u64>>(MAX_PAGE_OPS))
            .saturating_add(filter.map_or(0, |filter| 2 * filter.bits.len()));
        slice.charge_input(
            input_work,
            input_bytes,
            16 * MAX_REQUEST_ITEMS + 8 * (MAX_PAGE_OPS + MAX_PAGE_MISSING),
        )?;
        let scope = ActorScope::new(&request.actor_range_hints, &request.window);
        let view = read.sync_identity(&request.topic_id, &peer_id, &mut |charge| {
            reserve_snapshot(&mut slice, charge)
        })?;
        let Some(mut view) = view else {
            return Ok(empty);
        };
        if !view.member {
            return Ok(empty);
        }
        if request
            .genesis
            .is_some_and(|genesis| genesis != view.genesis)
        {
            return Err(Error::StaleIncarnation);
        }
        // The requester's credit binds every caller, not only one transport.
        let credit = PageBudget::from_credit(request.credit);
        let budget = PageBudget {
            ops: budget.ops.min(credit.ops),
            bytes: budget.bytes.min(credit.bytes),
        };
        let key = (peer_id, request.topic_id);
        let mut kept = {
            let mut plans = self.continuations();
            if let Some((captured, work, claim)) = plans.captured(key) {
                clocks = Some(claim);
                slice.charge_preparation(work, 0)?;
                view.clock = captured;
            }
            plans.take(key, &view, request, summary)
        };
        if kept.is_none() {
            view.clock = ActorClock::new();
            drop(clocks.take());
            let mut claim = self.continuations().reserve_clocks(0)?;
            let clock = read.sync_clock(
                &request.topic_id,
                summary.map(|_| &scope.named),
                &mut |charge| {
                    if let SnapshotCharge::Clock { entries, .. } = charge {
                        claim.grow_roots(if summary.is_some() {
                            [hints, hints, entries, entries]
                        } else {
                            [entries; 4]
                        })?;
                        slice.charge_preparation(entries.saturating_mul(4), 0)?;
                    }
                    reserve_snapshot(&mut slice, charge)
                },
            )?;
            clocks = Some(Arc::new(claim));
            view.clock = clock;
        }
        if budget.ops == 0 || budget.bytes == 0 {
            let more = !request.wants.is_empty() || forward_remaining(request, &view.clock, &[]);
            if let Some(kept) = kept {
                self.continuations().keep(key, kept)?;
            }
            return Ok(PlannedPage {
                more,
                ..PlannedPage::default()
            });
        }
        let local = &view.clock;
        if let Some(kept) = &mut kept {
            kept.frontier.confirm(request, local);
        }
        let (mut peer_clock, mut goal) = match &kept {
            Some(kept) => (kept.frontier.clock().clone(), kept.goal.clone()),
            None => (local.clone(), local.clone()),
        };
        if kept.is_none() {
            for hint in &request.actor_range_hints {
                if let Some((from, to)) = clamp_actor_range(hint, local.get(&hint.actor_id)) {
                    peer_clock.set(hint.actor_id, from);
                    goal.set(hint.actor_id, to);
                }
            }
        }
        let absent = ActorClock::new();
        if summary.is_some_and(|summary| {
            summary
                .staged
                .as_ref()
                .is_some_and(|staged| staged.topic_id != summary.topic_id)
        }) {
            return Err(Error::TopicMismatch);
        }
        let held = match summary {
            Some(summary) if summary.topic_id != request.topic_id => {
                return Err(Error::TopicMismatch);
            }
            Some(summary) if summary.genesis == Some(view.genesis) => Some(&summary.actor_clock),
            Some(summary) => Some(
                summary
                    .staged
                    .as_ref()
                    .filter(|staged| {
                        staged.topic_id == request.topic_id && staged.genesis == view.genesis
                    })
                    .map_or(&absent, |staged| &staged.clock),
            ),
            None => None,
        };
        let scope = scope.with_held(held);
        if summary.is_some() && kept.is_none() {
            peer_clock = held
                .map(|held| held.selected(&scope.named))
                .unwrap_or_default();
            for hint in &request.actor_range_hints {
                peer_clock.set(
                    hint.actor_id,
                    hint.from_exclusive.min(local.get(&hint.actor_id)),
                );
            }
        }
        let (captured, goal, mut frontier) = match kept {
            Some(kept) => {
                self.work.resumed();
                clocks = Some(Arc::clone(&kept.clocks));
                (kept.local, kept.goal, kept.frontier)
            }
            None => (
                local.clone(),
                goal,
                plan::Frontier::new(
                    &peer_clock,
                    &scope,
                    self.page_actors,
                    self.continuations().records(),
                ),
            ),
        };
        let claim = clocks
            .as_ref()
            .ok_or_else(|| Error::Storage("missing clock reservation".into()))?;
        frontier.evictable = false;
        slice.charge_preparation(frontier.scan_entries(), 0)?;
        slice.reserve(frontier.bytes())?;
        let floor = frontier.offer_floor().clone();
        let mut page = PlannedPage::default();
        if frontier.replaying {
            page = self.replay_page(read, &request.topic_id, &mut frontier, budget, &mut slice)?;
            if frontier.replaying {
                let continuation = Continuation::new(
                    (&view, request),
                    captured,
                    goal,
                    frontier,
                    Arc::clone(claim),
                    summary,
                );
                self.continuations().keep(key, continuation)?;
                return Ok(page);
            }
        }
        let replayed = page.ops.iter().try_fold(0_usize, |bytes, op| {
            postcard::experimental::serialized_size(op).map(|size| bytes.saturating_add(size))
        })?;
        let repair_budget = PageBudget {
            ops: budget.ops.saturating_sub(page.ops.len()),
            bytes: budget.bytes.saturating_sub(replayed),
        };
        if frontier.repair.is_none() && !request.wants.is_empty() {
            frontier.repair = Some(repair::Repair::new(&request.wants, &mut slice)?);
        }
        let named = request.actor_range_hints.len().saturating_sub(1).max(1);
        let position_limit = self.page_positions.min(named);
        let repair = match &mut frontier.repair {
            Some(repair) => repair.step(
                repair::RepairView {
                    read,
                    topic_id: &request.topic_id,
                    peer: &peer_clock,
                    scope: &scope,
                },
                repair_budget,
                position_limit,
                &mut slice,
                &mut frontier.records,
            )?,
            None => repair::RepairPage::default(),
        };
        frontier.admit(&repair.ops);
        let used = repair.ops.iter().try_fold(replayed, |bytes, op| {
            postcard::experimental::serialized_size(op).map(|size| bytes.saturating_add(size))
        })?;
        let rest = PageBudget {
            ops: budget.ops.saturating_sub(page.ops.len() + repair.ops.len()),
            bytes: budget.bytes.saturating_sub(used),
        };
        page.ops.extend(repair.ops);
        page.more = frontier
            .repair
            .as_ref()
            .is_some_and(repair::Repair::pending);
        page.missing.extend(repair.missing);
        page.too_large = repair.too_large;
        let mut needed = page
            .positions
            .iter()
            .map(|actor| (*actor, 0))
            .collect::<BTreeMap<_, _>>();
        for (actor, generation) in repair.positions {
            request::need(&mut needed, actor, generation);
        }
        page.positions = request::deepest(&needed, position_limit);
        page.continued = false;
        if repair.continued || rest.ops == 0 || rest.bytes == 0 || page.too_large.is_some() {
            page.more |= forward_remaining(request, &goal, &page.ops);
            if page.more && page.too_large.is_none() {
                frontier.evictable = !page.ops.is_empty();
                frontier.capture_offer(&page, floor)?;
                let continuation = Continuation::new(
                    (&view, request),
                    captured,
                    goal,
                    frontier,
                    Arc::clone(claim),
                    summary,
                );
                self.continuations().keep(key, continuation)?;
                page.continued =
                    repair.continued && page.ops.is_empty() && page.positions.is_empty();
            }
            return Ok(page);
        }
        let (mut planned, positions, mut frontier) = self.resume_page(
            read,
            &request.topic_id,
            (&captured, &goal, frontier),
            // Find more positions than the page names, so it names the deepest.
            (&scope, self.page_positions),
            rest,
            &mut slice,
        )?;
        if frontier.as_ref().is_some_and(|frontier| {
            !frontier.replaying
                && frontier.clock().dominates(&goal)
                && !frontier
                    .repair
                    .as_ref()
                    .is_some_and(repair::Repair::pending)
                && positions.is_empty()
        }) {
            frontier = None;
            planned.more = false;
        }
        page.ops.extend(planned.ops);
        page.more = planned.more || forward_remaining(request, &goal, &page.ops);
        page.missing.extend(planned.missing);
        for (actor, generation) in positions {
            request::need(&mut needed, actor, generation);
        }
        page.positions = request::deepest(&needed, position_limit);
        page.too_large = page.too_large.or(planned.too_large);
        while page.missing.len() > MAX_PAGE_MISSING {
            page.missing.pop_last();
        }
        if let Some(mut frontier) = frontier
            && page.more
            && page.too_large.is_none()
        {
            frontier.evictable = !page.ops.is_empty();
            frontier.capture_offer(&page, floor)?;
            page.continued =
                page.ops.is_empty() && page.positions.is_empty() && frontier.advancing();
            let continuation = Continuation::new(
                (&view, request),
                captured,
                goal,
                frontier,
                Arc::clone(claim),
                summary,
            );
            self.continuations().keep(key, continuation)?;
        }

        Ok(page)
    }

    pub fn receive_data(
        &self,
        source_peer_id: PeerId,
        ack_peer_id: PeerId,
        data: SyncData,
    ) -> Result<(SyncAck, Vec<TopicEviction>)> {
        self.receive_data_preverified(source_peer_id, ack_peer_id, data, &BTreeSet::new(), None)
    }

    /// Like [`Self::receive_data`], but skips signature verification for ops whose
    /// id is in `verified` (the caller already ran [`Op::validate`] on those exact
    /// ops).
    pub(crate) fn receive_data_preverified(
        &self,
        source_peer_id: PeerId,
        ack_peer_id: PeerId,
        data: SyncData,
        verified: &BTreeSet<OpId>,
        effects: Option<ReceiveEffects<'_>>,
    ) -> Result<(SyncAck, Vec<TopicEviction>)> {
        let mut data_op_ids = BTreeSet::new();
        for op in &data.ops {
            if op.signed.body.topic_id != data.topic_id {
                return Err(Error::TopicMismatch);
            }
            data_op_ids.insert(op.id);
        }
        let (admitted, failure) =
            match self
                .oplog
                .receive_preverified(Some(source_peer_id), data.ops, verified, effects)
            {
                Ok(admitted) => (admitted, None),
                Err(Error::AdmissionCommitted { admitted, source }) => (*admitted, Some(source)),
                Err(error) => return Err(error),
            };
        // Admission also flushes buffered ops of other topics; an ack speaks
        // for its own topic only, and its reader files obligations under it.
        let mut ack = SyncAck {
            topic_id: data.topic_id,
            peer_id: ack_peer_id,
            genesis: None,
            accepted: admitted
                .accepted
                .iter()
                .copied()
                .filter(|id| data_op_ids.contains(id))
                .collect(),
            heads: BTreeSet::new(),
            clock: ActorClock::new(),
            signature: None,
        };
        let frontier = (|| {
            for op_id in &admitted.accepted {
                if !ack.accepted.contains(op_id)
                    && self
                        .oplog
                        .storage()
                        .get_position(op_id)?
                        .is_some_and(|meta| meta.topic_id == data.topic_id)
                {
                    ack.accepted.insert(*op_id);
                }
            }
            self.ack_frontier(&data.topic_id)
        })();
        if let Some(source) = failure {
            return Err(Error::ReceiveCommitted {
                ack: Box::new(ack),
                evictions: admitted.evictions,
                source,
            });
        }
        match frontier {
            Ok((state, heads, clock)) => {
                ack.genesis = Some(state.genesis);
                ack.heads = heads;
                ack.clock = clock;
                Ok((ack, admitted.evictions))
            }
            Err(source) => Err(Error::ReceiveCommitted {
                ack: Box::new(ack),
                evictions: admitted.evictions,
                source: Box::new(source),
            }),
        }
    }
}

/// Digest heads, clock and unresolved ids so incomplete and whole topics differ; a
/// scan not yet complete digests the holes it found. Identically damaged topics can
/// still match, so callers reject the fingerprint fast path unless theirs is whole.
pub(crate) fn digest_for(view: &TopicView, integrity: &Integrity) -> Result<[u8; 32]> {
    if integrity.certifies(view) {
        return Ok(view.fingerprint);
    }
    let unresolved = integrity
        .unresolved(view)
        .into_keys()
        .collect::<BTreeSet<_>>();
    Ok(*blake3::hash(&canonical_bytes(&(view.fingerprint, &unresolved))?).as_bytes())
}

fn reserve_snapshot(slice: &mut slice::Slice, charge: SnapshotCharge) -> Result<()> {
    match charge {
        SnapshotCharge::Read { bytes } => {
            slice.charge_authorization()?;
            slice.charge_capture(0, bytes, 0)
        }
        SnapshotCharge::Members(entries) => slice.charge_preparation(entries, 0),
        SnapshotCharge::State { entries, workspace } => {
            slice.charge_preparation(entries, workspace)
        }
        SnapshotCharge::Clock { entries, workspace } => slice.charge_capture(entries, 0, workspace),
    }
}

fn forward_remaining(request: &SyncRequest, local: &ActorClock, ops: &[Op]) -> bool {
    let mut sent = BTreeMap::<ActorId, u64>::new();
    for hint in &request.actor_range_hints {
        sent.entry(hint.actor_id)
            .and_modify(|seq| *seq = (*seq).min(hint.from_exclusive))
            .or_insert(hint.from_exclusive);
    }
    for op in ops {
        let body = &op.signed.body;
        if let Some(seq) = sent.get_mut(&body.actor_id)
            && seq.checked_add(1) == Some(body.actor_seq)
        {
            *seq = body.actor_seq;
        }
    }
    request.actor_range_hints.iter().any(|hint| {
        hint.to_inclusive.min(local.get(&hint.actor_id))
            > hint
                .from_exclusive
                .max(sent.get(&hint.actor_id).copied().unwrap_or(0))
    })
}

/// The request for the ids and ranges of `plan`, on branch `genesis`, with a
/// credit sized to what it asks for.
pub(crate) fn page_request(plan: SyncPlan, genesis: Option<OpId>) -> SyncRequest {
    let requested = plan.need.len() as u64
        + plan
            .actor_range_hints
            .iter()
            .map(|hint| hint.to_inclusive.saturating_sub(hint.from_exclusive))
            .sum::<u64>();
    let mut credit = SyncCredit::default();
    credit.ops = credit
        .ops
        .min(u32::try_from(requested).unwrap_or(u32::MAX))
        .max(1);
    SyncRequest {
        topic_id: plan.topic_id,
        known: plan.common,
        wants: plan.need,
        actor_range_hints: plan.actor_range_hints,
        genesis,
        credit,
        window: plan.window,
    }
}

/// The branch a request made against a peer on `remote` names: the smaller
/// genesis wins, so a local branch that loses asks for the peer's.
pub(crate) fn request_genesis(local: OpId, remote: Option<OpId>) -> OpId {
    remote.filter(|remote| *remote < local).unwrap_or(local)
}

/// Clamp a peer `ActorRangeHint`: `from_exclusive <= to_inclusive <= local_seq`, with
/// span capped at `MAX_RANGE_SPAN`; empty or reversed ranges still name the peer's position.
/// Return `None` when the peer holds everything local.
fn clamp_actor_range(hint: &ActorRangeHint, local_seq: u64) -> Option<(u64, u64)> {
    if hint.from_exclusive >= local_seq {
        return None;
    }
    let upper = hint.to_inclusive.min(local_seq).max(hint.from_exclusive);
    let span = upper - hint.from_exclusive;
    let span = span.min(MAX_RANGE_SPAN);
    let to_inclusive = hint.from_exclusive.checked_add(span)?;
    Some((hint.from_exclusive, to_inclusive))
}

fn remote_contains(remote: &SyncSummary, id: &OpId, meta: &crate::storage::OpPosition) -> bool {
    meta.topic_id == remote.topic_id
        && (remote.heads.contains(id)
            || remote.actor_tips.get(&meta.actor_id) == Some(&(meta.actor_seq, *id))
            || remote.actor_clock.get(&meta.actor_id) >= meta.actor_seq)
}

#[cfg(test)]
mod tests;
