// SPDX-License-Identifier: MIT OR Apache-2.0
//! Transport-neutral sync messages, planning, acknowledgements, and reports.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use smallvec::SmallVec;

use crate::oplog::{Oplog, ReceiveEffects, TopicEviction, subset_in};
use crate::storage::{SnapshotRead, Storage, TopicView, topic_fingerprint_for};
use crate::{ActorClock, ActorId, Error, Op, OpId, PeerId, Result, TopicId, canonical_bytes};

mod evidence;
mod plan;
mod types;

use plan::{ActorState, Pager, Wait};

pub use types::{
    ActorRangeHint, PageBudget, SyncAck, SyncCredit, SyncData, SyncFailure, SyncFailureCode,
    SyncFingerprint, SyncMessage, SyncOpen, SyncPage, SyncPlan, SyncReceipt, SyncReport,
    SyncRequest, SyncSummary,
};

const SYNC_ACK_SIGNING_DOMAIN: &[u8] = b"irokle/sync-ack/2";

/// Wire contract this build speaks. Version 3 added receive credits, page results
/// and branch names; version 4 names missing records in page results and accepts
/// zero-span position hints. Older peers are refused before any message.
pub const SYNC_PROTOCOL: &str = "irokle/sync/4";

/// Maximum number of sequences a single ActorRangeHint may span. Caps both the
/// hint a peer can construct via `actor_ranges` and the work
/// `plan_response_data` is willing to do for a peer-supplied hint, so a
/// malicious peer cannot push us into walking unbounded sequence ranges.
pub const MAX_ACTOR_RANGE_HINT_SPAN: u64 = 65_536;
const MAX_REQUEST_ITEMS: usize = 65_536;
const MAX_PAGE_OPS: usize = 4096;
const MAX_PAGE_BYTES: usize = 32 * 1024 * 1024;
/// Actors one page plan keeps active range heads for; the rest wait for a free
/// slot in the same page or for a later page.
const MAX_PAGE_ACTORS: usize = 4096;
/// Records one page names as missing, at most.
pub const MAX_PAGE_MISSING: usize = 256;

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
}

#[derive(Clone)]
pub struct SyncEngine<S> {
    oplog: Oplog<S>,
    peer_id: PeerId,
    /// Active actors of one page plan; tests scale it down.
    page_actors: usize,
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
        }
    }

    /// The same engine planning pages with at most `actors` active actors.
    #[cfg(test)]
    pub(crate) fn with_page_actors(mut self, actors: usize) -> Self {
        self.page_actors = actors.max(1);
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
    pub fn summary(&self, topic_id: TopicId) -> Result<SyncSummary> {
        self.oplog
            .storage()
            .read_snapshot(|read| self.summary_in(read, topic_id))
    }

    /// [`Self::summary`] read from a snapshot the caller already holds.
    pub(crate) fn summary_in(
        &self,
        read: &dyn SnapshotRead,
        topic_id: TopicId,
    ) -> Result<SyncSummary> {
        let Some(view) = read.topic_view(&topic_id, None)? else {
            return Ok(SyncSummary {
                topic_id,
                event_type_id: None,
                genesis: None,
                fingerprint: topic_fingerprint_for(&BTreeSet::new(), &ActorClock::new())?,
                heads: BTreeSet::new(),
                actor_clock: ActorClock::new(),
                actor_tips: BTreeMap::new(),
                staged: None,
            });
        };
        let fingerprint = self.digest_in(read, &view)?;
        let actor_tips = view
            .tips
            .iter()
            .filter(|(actor_id, (seq, _))| view.clock.get(actor_id) == *seq)
            .map(|(actor_id, tip)| (*actor_id, *tip))
            .collect();
        Ok(SyncSummary {
            topic_id,
            genesis: Some(view.state.genesis),
            event_type_id: Some(view.state.event_type_id),
            fingerprint,
            heads: view.state.heads,
            actor_clock: view.clock,
            actor_tips,
            staged: None,
        })
    }

    pub fn fingerprint(&self, topic_id: TopicId) -> Result<SyncFingerprint> {
        let fingerprint = self.oplog.storage().read_snapshot(|read| {
            match read.topic_view(&topic_id, None)? {
                Some(view) => self.digest_in(read, &view),
                None => topic_fingerprint_for(&BTreeSet::new(), &ActorClock::new()),
            }
        })?;
        Ok(SyncFingerprint {
            topic_id,
            fingerprint,
        })
    }

    /// The digest a peer compares its own against. Stored heads and clock say
    /// nothing about whether the records behind them exist, so the topic's
    /// unresolved ids are folded in: a node holding a hole never looks equal to
    /// a whole one, which is exactly what the matched-fingerprint fast path
    /// assumes. Callers must still refuse that fast path while their own topic
    /// is incomplete, since two identically damaged stores do match.
    pub(crate) fn digest_in(&self, read: &dyn SnapshotRead, view: &TopicView) -> Result<[u8; 32]> {
        let unresolved = self.oplog.unresolved_in(read, view)?;
        if unresolved.is_empty() {
            return Ok(view.fingerprint);
        }
        Ok(*blake3::hash(&canonical_bytes(&(view.fingerprint, &unresolved))?).as_bytes())
    }

    /// The whole missing closure against `remote`, unbounded: an export of
    /// history, not a sync step. Sync uses [`Self::negotiate_page`].
    pub fn negotiate(&self, peer_id: PeerId, remote: &SyncSummary) -> Result<SyncPlan> {
        self.oplog.storage().read_snapshot(|read| {
            self.negotiate_inner(read, peer_id, remote, SendSet::Closure, &mut false)
        })
    }

    /// Plan a causal push page within `budget` and the request for what the
    /// peer holds beyond this node, and whether the push holds more. A zero
    /// budget reads no operation; another genesis is planned as a branch,
    /// never by comparing positions across branches.
    pub fn negotiate_page(
        &self,
        peer_id: PeerId,
        remote: &SyncSummary,
        budget: PageBudget,
    ) -> Result<(SyncPlan, bool)> {
        self.oplog
            .storage()
            .read_snapshot(|read| self.negotiate_in(read, peer_id, remote, budget))
    }

    /// [`Self::negotiate_page`] over a snapshot the caller already holds.
    pub(crate) fn negotiate_in(
        &self,
        read: &dyn SnapshotRead,
        peer_id: PeerId,
        remote: &SyncSummary,
        budget: PageBudget,
    ) -> Result<(SyncPlan, bool)> {
        let mut more = false;
        let plan = self.negotiate_inner(read, peer_id, remote, SendSet::Page(budget), &mut more)?;
        Ok((plan, more))
    }

    /// Authorization, branch choice and selection all read `read`, so a plan
    /// never pairs a membership verdict with records of a later commit.
    fn negotiate_inner(
        &self,
        read: &dyn SnapshotRead,
        peer_id: PeerId,
        remote: &SyncSummary,
        send_set: SendSet,
        more: &mut bool,
    ) -> Result<SyncPlan> {
        // An unknown topic's remote heads are unauthenticated, so they never become
        // `need`. Bootstrap stages pages the inviter pushes or the transport pulls
        // with range hints, which the responder clamps and serves to members only.
        let Some(view) = read.topic_view(&remote.topic_id, None)? else {
            return Ok(SyncPlan {
                topic_id: remote.topic_id,
                common: BTreeSet::new(),
                have: BTreeSet::new(),
                send: Vec::new(),
                need: BTreeSet::new(),
                actor_range_hints: Vec::new(),
            });
        };
        if !view.state.members.contains(&peer_id) {
            return Ok(SyncPlan {
                topic_id: remote.topic_id,
                common: BTreeSet::new(),
                have: view.state.heads,
                send: Vec::new(),
                need: BTreeSet::new(),
                actor_range_hints: Vec::new(),
            });
        }
        if let Some(remote_event_type_id) = &remote.event_type_id
            && *remote_event_type_id != view.state.event_type_id
        {
            return Err(Error::EventTypeMismatch {
                expected: view.state.event_type_id,
                actual: remote_event_type_id.clone(),
            });
        }

        let local_heads = view.state.heads.clone();
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
            };
            *more = false;
            if remote_genesis < view.state.genesis {
                plan.actor_range_hints = actor_ranges(&empty, &remote.actor_clock, 0);
                return Ok(plan);
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
                    let page = self.plan_page(
                        read,
                        &remote.topic_id,
                        &view.clock,
                        &empty,
                        None,
                        &BTreeSet::new(),
                        budget,
                    )?;
                    *more = page.more;
                    page.ops
                }
            };
            return Ok(plan);
        }
        // A hole moves neither heads nor the clock, so a matching fingerprint
        // does not prove we are whole; keep negotiating until it is repaired.
        let unresolved = self.oplog.unresolved_in(read, &view)?;
        if unresolved.is_empty() && view.fingerprint == remote.fingerprint {
            return Ok(SyncPlan {
                topic_id: remote.topic_id,
                common: local_heads.clone(),
                have: local_heads,
                send: Vec::new(),
                need: BTreeSet::new(),
                actor_range_hints: Vec::new(),
            });
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
                let page = self.plan_page(
                    read,
                    &remote.topic_id,
                    &view.clock,
                    &remote.actor_clock,
                    None,
                    &BTreeSet::new(),
                    budget,
                )?;
                *more = page.more;
                page.ops
            }
        };
        let mut need = BTreeSet::new();
        for id in &remote.heads {
            if !read.dep_resolvable(id)? {
                need.insert(*id);
            }
        }
        // Anti-entropy: an id we reference but cannot resolve - a head the walk
        // could not follow, an admitted record that is half stored, or a hole a
        // buffered op waits on - is requested from this peer like any other
        // missing op, so a store already holding a dangling edge heals over
        // normal sync instead of deferring its dependents forever.
        let repair = dangling
            .into_iter()
            .chain(unresolved)
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
            need.retain(|id| {
                !remote
                    .actor_tips
                    .iter()
                    .any(|(actor_id, (seq, tip))| tip == id && view.clock.get(actor_id) < *seq)
            });
        }
        // A request past the item limit is refused whole, so the wants are cut
        // to what one request may carry and the rest follow once these resolve.
        let need = need
            .into_iter()
            .take(MAX_REQUEST_ITEMS)
            .collect::<BTreeSet<_>>();
        let actor_range_hints = actor_ranges(&view.clock, &remote.actor_clock, need.len());
        Ok(SyncPlan {
            topic_id: remote.topic_id,
            common,
            have: local_heads,
            send,
            need,
            actor_range_hints,
        })
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

    /// Walk local heads down to the frontier the remote already has, reporting
    /// the common ancestors found there and every id the walk found stored
    /// incompletely. The second set is what anti-entropy repair asks the peer
    /// for; collecting it here costs no extra traversal.
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
            let Some(meta) = read.get_meta(&id)? else {
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
            if remote_contains(remote, &meta) {
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
            let Some(meta) = read.get_meta(&id)? else {
                continue;
            };
            if meta.topic_id != remote.topic_id || remote_contains(remote, &meta) {
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
            .read_snapshot(|read| self.request_in(read, peer_id, remote))
    }

    /// [`Self::plan_request`] over a snapshot the caller already holds; the
    /// branch it names is the one that snapshot planned on.
    pub(crate) fn request_in(
        &self,
        read: &dyn SnapshotRead,
        peer_id: PeerId,
        remote: &SyncSummary,
    ) -> Result<SyncRequest> {
        let no_push = PageBudget { ops: 0, bytes: 0 };
        let (plan, _) = self.negotiate_in(read, peer_id, remote, no_push)?;
        let genesis = read
            .topic_view(&plan.topic_id, None)?
            .map(|view| request_genesis(view.state.genesis, remote.genesis));
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

    /// Serve one causal page of `request` within `budget` and the request's own
    /// credit, whichever is smaller: its explicit wants, then its ranges,
    /// refusing a request planned on another genesis. `more` says the requested
    /// goal holds more; the requester asks again. The membership check and
    /// every record come from one snapshot.
    pub fn response_page(
        &self,
        peer_id: PeerId,
        request: &SyncRequest,
        budget: PageBudget,
    ) -> Result<PlannedPage> {
        self.oplog
            .storage()
            .read_snapshot(|read| self.response_in(read, peer_id, request, budget))
    }

    /// [`Self::response_page`] over a snapshot the caller already holds.
    pub(crate) fn response_in(
        &self,
        read: &dyn SnapshotRead,
        peer_id: PeerId,
        request: &SyncRequest,
        budget: PageBudget,
    ) -> Result<PlannedPage> {
        let empty = PlannedPage::default();
        let Some(view) = read.topic_view(&request.topic_id, None)? else {
            return Ok(empty);
        };
        if !view.state.members.contains(&peer_id) {
            return Ok(empty);
        }
        if request
            .genesis
            .is_some_and(|genesis| genesis != view.state.genesis)
        {
            return Err(Error::StaleIncarnation);
        }
        if request.actor_range_hints.len() > MAX_REQUEST_ITEMS
            || request.wants.len() > MAX_REQUEST_ITEMS
        {
            return Err(Error::Storage("sync request exceeds work budget".into()));
        }
        let asks = !request.wants.is_empty() || !request.actor_range_hints.is_empty();
        // The requester's credit binds every caller, not only one transport.
        let credit = PageBudget::from_credit(request.credit);
        let budget = PageBudget {
            ops: budget.ops.min(credit.ops),
            bytes: budget.bytes.min(credit.bytes),
        };
        if budget.ops == 0 || budget.bytes == 0 {
            return Ok(PlannedPage {
                more: asks,
                ..PlannedPage::default()
            });
        }
        let local = &view.clock;
        let mut peer_clock = local.clone();
        let mut goal = local.clone();
        for hint in &request.actor_range_hints {
            if let Some((from, to)) = clamp_actor_range_hint(hint, local.get(&hint.actor_id)) {
                peer_clock.set(hint.actor_id, from);
                goal.set(hint.actor_id, to);
            }
        }
        let repair =
            Self::plan_repair(read, &request.topic_id, &request.wants, &peer_clock, budget)?;
        let mut used = 0;
        for op in &repair.ops {
            used += postcard::experimental::serialized_size(op)?;
            let body = &op.signed.body;
            if peer_clock.get(&body.actor_id) + 1 == body.actor_seq {
                peer_clock.set(body.actor_id, body.actor_seq);
            }
        }
        let rest = PageBudget {
            ops: budget.ops.saturating_sub(repair.ops.len()),
            bytes: budget.bytes.saturating_sub(used),
        };
        let mut page = PlannedPage {
            ops: repair.ops,
            more: !repair.unsent.is_empty(),
            missing: repair.missing,
            too_large: repair.too_large,
        };
        if rest.ops == 0 || rest.bytes == 0 || page.too_large.is_some() {
            return Ok(page);
        }
        // Wants this page could not carry keep their dependents out of the
        // forward ranges, which still serve every independent actor.
        let planned = self.plan_page(
            read,
            &request.topic_id,
            local,
            &peer_clock,
            Some(&goal),
            &repair.unsent,
            rest,
        )?;
        let forwarded = planned.ops.iter().map(|op| op.id).collect::<BTreeSet<_>>();
        page.more = planned.more || !repair.unsent.is_subset(&forwarded);
        page.ops.extend(planned.ops);
        page.missing.extend(planned.missing);
        page.too_large = page.too_large.or(planned.too_large);
        while page.missing.len() > MAX_PAGE_MISSING {
            page.missing.pop_last();
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
        let (admitted, failure) = match self.oplog.receive_ops_from_peer_preverified(
            Some(source_peer_id),
            data.ops,
            verified,
            effects,
        ) {
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
                        .get_meta(op_id)?
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

    /// Requested repair ids this store holds, oldest generation first. An id
    /// whose dependency is neither held by the peer nor sent before it waits,
    /// and so do its dependents: its ancestors come from the forward ranges.
    fn plan_repair(
        read: &dyn SnapshotRead,
        topic_id: &TopicId,
        wants: &BTreeSet<OpId>,
        peer: &ActorClock,
        budget: PageBudget,
    ) -> Result<RepairPage> {
        let mut page = RepairPage::default();
        let mut ordered = Vec::with_capacity(wants.len());
        for id in wants {
            match read.get_meta(id)? {
                Some(meta) if meta.topic_id == *topic_id => {
                    ordered.push((meta.generation, *id, meta.deps))
                }
                Some(_) => {}
                None => {
                    page.missing.insert(*id);
                }
            }
        }
        // Generations order ancestors first, whatever order the ids sort in.
        ordered.sort_unstable_by_key(|(generation, id, _)| (*generation, *id));
        let mut sent = BTreeSet::new();
        let mut bytes = 0;
        for (index, (_, id, deps)) in ordered.iter().enumerate() {
            let mut ready = true;
            for dep in deps {
                if sent.contains(dep) {
                    continue;
                }
                if wants.contains(dep) {
                    ready = false;
                    break;
                }
                let covered = read
                    .get_meta(dep)?
                    .is_some_and(|meta| peer.get(&meta.actor_id) >= meta.actor_seq);
                if !covered {
                    ready = false;
                    break;
                }
            }
            if !ready {
                page.unsent.insert(*id);
                continue;
            }
            let Some(op) = read.get_op(id)? else {
                page.missing.insert(*id);
                continue;
            };
            let size = postcard::experimental::serialized_size(&op)?;
            if page.ops.len() >= budget.ops || bytes + size > budget.bytes {
                if page.ops.is_empty() && size > budget.bytes {
                    page.too_large = Some(*id);
                }
                page.unsent
                    .extend(ordered[index..].iter().map(|(_, id, _)| *id));
                break;
            }
            bytes += size;
            sent.insert(*id);
            page.ops.push(op);
        }
        Ok(page)
    }
}

/// What the repair part of a page carried and left.
#[derive(Default)]
struct RepairPage {
    ops: Vec<Op>,
    /// Held wants not carried, whose dependents must wait.
    unsent: BTreeSet<OpId>,
    missing: BTreeSet<OpId>,
    too_large: Option<OpId>,
}

impl Pager<'_> {
    /// What `meta` still waits for: an unsent or blocked dependency, a missing
    /// record, or a position of another actor the peer does not hold yet.
    fn wait_for(&mut self, meta: &crate::storage::OpMeta) -> Result<Wait> {
        for dep in &meta.deps {
            if self.blocked.contains(dep) {
                return Ok(Wait::Blocked);
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
            if self.covered.get(&dep_actor) < dep_seq {
                return Ok(Wait::Position(dep_actor, dep_seq));
            }
        }
        Ok(Wait::Ready)
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

/// Ranges from `local` up to `remote` for every actor `remote` is ahead on,
/// spanning at most `MAX_ACTOR_RANGE_HINT_SPAN` positions beside `wants` ids.
/// Actors past the span still get a zero-span hint naming their position, so a
/// responder never takes an actor this node is behind on as held.
fn actor_ranges(local: &ActorClock, remote: &ActorClock, wants: usize) -> Vec<ActorRangeHint> {
    let mut remaining = MAX_ACTOR_RANGE_HINT_SPAN.saturating_sub(wants as u64);
    remote
        .iter()
        .filter_map(|(actor_id, remote_seq)| {
            let local_seq = local.get(actor_id);
            if *remote_seq <= local_seq {
                return None;
            }
            let to_inclusive = remote_seq
                .saturating_sub(local_seq)
                .min(remaining)
                .saturating_add(local_seq);
            remaining -= to_inclusive - local_seq;
            Some(ActorRangeHint {
                actor_id: *actor_id,
                from_exclusive: local_seq,
                to_inclusive,
            })
        })
        .take(MAX_REQUEST_ITEMS.saturating_sub(wants))
        .collect()
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
    }
}

/// The branch a request made against a peer on `remote` names: the smaller
/// genesis wins, so a local branch that loses asks for the peer's.
pub(crate) fn request_genesis(local: OpId, remote: Option<OpId>) -> OpId {
    remote.filter(|remote| *remote < local).unwrap_or(local)
}

/// Clamp a peer-supplied `ActorRangeHint` against our local knowledge so that
/// `from_exclusive <= to_inclusive`, `to_inclusive <= local_seq` (we only walk
/// sequences we actually have), and the resulting span never exceeds
/// `MAX_ACTOR_RANGE_HINT_SPAN`. An empty or reversed range still names the
/// peer's position. Returns `None` when the peer holds everything local.
fn clamp_actor_range_hint(hint: &ActorRangeHint, local_seq: u64) -> Option<(u64, u64)> {
    if hint.from_exclusive >= local_seq {
        return None;
    }
    let upper = hint.to_inclusive.min(local_seq).max(hint.from_exclusive);
    let span = upper - hint.from_exclusive;
    let span = span.min(MAX_ACTOR_RANGE_HINT_SPAN);
    let to_inclusive = hint.from_exclusive.checked_add(span)?;
    Some((hint.from_exclusive, to_inclusive))
}

fn remote_contains(remote: &SyncSummary, meta: &crate::storage::OpMeta) -> bool {
    meta.topic_id == remote.topic_id
        && (remote.heads.contains(&meta.id)
            || remote.actor_tips.get(&meta.actor_id) == Some(&(meta.actor_seq, meta.id))
            || remote.actor_clock.get(&meta.actor_id) >= meta.actor_seq)
}
