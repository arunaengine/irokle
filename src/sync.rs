// SPDX-License-Identifier: MIT OR Apache-2.0
//! Transport-neutral sync messages, planning, acknowledgements, and reports.

#[cfg(feature = "iroh")]
use std::cmp::Reverse;
#[cfg(feature = "iroh")]
use std::collections::BinaryHeap;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use ed25519_dalek::Signature;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::oplog::{Oplog, ReceiveEffects, TopicEviction, topological_subset};
use crate::storage::{PeerAck, Storage, SyncObligation, TopicState, TopicView};
use crate::{
    ActorClock, ActorId, Error, Op, OpId, PeerId, Result, Signer, TopicId, actor_id_for,
    canonical_bytes, verify,
};

const SYNC_ACK_SIGNING_DOMAIN: &[u8] = b"irokle/sync-ack/2";

/// Wire contract this build speaks. Version 3 adds receive credits, page results
/// and branch names to summaries and requests; older peers are refused by the
/// transport before any message is exchanged.
pub const SYNC_PROTOCOL: &str = "irokle/sync/3";

/// Maximum number of sequences a single ActorRangeHint may span. Caps both the
/// hint a peer can construct via `needed_actor_ranges` and the work
/// `plan_response_data` is willing to do for a peer-supplied hint, so a
/// malicious peer cannot push us into walking unbounded sequence ranges.
pub const MAX_ACTOR_RANGE_HINT_SPAN: u64 = 65_536;
const MAX_REQUEST_ITEMS: usize = 65_536;
const MAX_PAGE_OPS: usize = 4096;
const MAX_PAGE_BYTES: usize = 32 * 1024 * 1024;
#[cfg(feature = "iroh")]
/// Actors one page plan keeps range heads for; the rest wait for a later page.
const MAX_PAGE_ACTORS: usize = 4096;

#[cfg(feature = "iroh")]
/// A queued range position: generation, actor, sequence, id and range limit.
type RangeHead = (u64, ActorId, u64, OpId, u64);

#[cfg(feature = "iroh")]
/// One planned page and whether the goal still holds more after it.
pub(crate) struct PlannedPage {
    pub(crate) ops: Vec<Op>,
    pub(crate) more: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncOpen {
    pub protocol: String,
    pub topic_id: TopicId,
    pub peer_id: PeerId,
    #[serde(default)]
    pub event_type_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncSummary {
    pub topic_id: TopicId,
    #[serde(default)]
    pub event_type_id: Option<String>,
    /// Genesis of the branch this summary describes, if the topic is known.
    pub genesis: Option<OpId>,
    pub fingerprint: [u8; 32],
    pub heads: BTreeSet<OpId>,
    pub actor_clock: ActorClock,
    pub actor_tips: BTreeMap<ActorId, (u64, OpId)>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncFingerprint {
    pub topic_id: TopicId,
    pub fingerprint: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActorRangeHint {
    pub actor_id: ActorId,
    pub from_exclusive: u64,
    pub to_inclusive: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncPlan {
    pub topic_id: TopicId,
    pub common: BTreeSet<OpId>,
    pub have: BTreeSet<OpId>,
    pub send: Vec<Op>,
    pub need: BTreeSet<OpId>,
    pub actor_range_hints: Vec<ActorRangeHint>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncRequest {
    pub topic_id: TopicId,
    pub known: BTreeSet<OpId>,
    pub wants: BTreeSet<OpId>,
    pub actor_range_hints: Vec<ActorRangeHint>,
    /// Branch the requester plans against; a responder on another branch refuses.
    pub genesis: Option<OpId>,
    pub credit: SyncCredit,
}

/// What a requester is willing to receive for one page.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncCredit {
    pub ops: u32,
    pub bytes: u64,
}

impl Default for SyncCredit {
    fn default() -> Self {
        Self {
            ops: MAX_PAGE_OPS as u32,
            bytes: MAX_PAGE_BYTES as u64,
        }
    }
}

/// Ends the data a responder served for one topic's request. `more` means the
/// requested goal holds more than this page; the requester asks again.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncPage {
    pub topic_id: TopicId,
    pub more: bool,
}

/// Staged progress of data for a topic the receiver does not hold yet. It is
/// never an ack: it certifies nothing and clears no obligation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncReceipt {
    pub topic_id: TopicId,
    pub clock: ActorClock,
}

#[cfg(feature = "iroh")]
/// Bounds of one planned page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PageBudget {
    pub(crate) ops: usize,
    pub(crate) bytes: usize,
}

#[cfg(feature = "iroh")]
impl PageBudget {
    /// The budget a credit allows, never above the page limits.
    pub(crate) fn from_credit(credit: SyncCredit) -> Self {
        Self {
            ops: (credit.ops as usize).min(MAX_PAGE_OPS),
            bytes: usize::try_from(credit.bytes)
                .unwrap_or(usize::MAX)
                .min(MAX_PAGE_BYTES),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncData {
    pub topic_id: TopicId,
    pub ops: Vec<Op>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncAck {
    pub topic_id: TopicId,
    pub peer_id: PeerId,
    /// Genesis of the incarnation this acknowledgement certifies. Signed, so a
    /// proof cannot be moved to a branch that replaced the one it was made on.
    /// `None` is an acknowledgement from a peer that predates this contract, or
    /// one whose sender could not read a coherent view; it certifies nothing.
    #[serde(default)]
    pub genesis: Option<OpId>,
    pub accepted: BTreeSet<OpId>,
    pub heads: BTreeSet<OpId>,
    pub clock: ActorClock,
    #[serde(default)]
    pub signature: Option<Signature>,
}

#[derive(Serialize)]
struct SyncAckToSign<'a> {
    topic_id: TopicId,
    peer_id: PeerId,
    genesis: &'a Option<OpId>,
    accepted: &'a BTreeSet<OpId>,
    heads: &'a BTreeSet<OpId>,
    clock: &'a ActorClock,
}

impl SyncAck {
    pub fn sign(&mut self, signer: &impl Signer) -> Result<()> {
        if signer.peer_id() != self.peer_id {
            return Err(Error::WrongSigner);
        }
        let bytes = self.signing_bytes()?;
        self.signature = Some(signer.sign(&bytes)?);
        Ok(())
    }

    pub fn verify_signature(&self) -> Result<()> {
        let signature = self.signature.as_ref().ok_or(Error::MissingSignature)?;
        verify(self.peer_id, &self.signing_bytes()?, signature)
    }

    fn signing_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = SYNC_ACK_SIGNING_DOMAIN.to_vec();
        bytes.extend_from_slice(&canonical_bytes(&SyncAckToSign {
            topic_id: self.topic_id,
            peer_id: self.peer_id,
            genesis: &self.genesis,
            accepted: &self.accepted,
            heads: &self.heads,
            clock: &self.clock,
        })?);
        Ok(bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncReport {
    pub topic_id: TopicId,
    pub peer_id: PeerId,
    pub obligations: Vec<SyncObligation>,
}

/// Which part of a topic exchange failed. Bounded on purpose: it names the
/// message the responder could not handle, never the underlying error text.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SyncFailureCode {
    Open,
    Fingerprint,
    Summary,
    Request,
    Data,
    Ack,
}

/// The terminal result of one topic's exchange when it could not be completed.
/// Without it a responder that swallowed a per-topic error is indistinguishable
/// from one that had nothing to say, and the requester marks the topic clean.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncFailure {
    pub topic_id: TopicId,
    pub code: SyncFailureCode,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SyncMessage {
    Open(SyncOpen),
    Fingerprint(SyncFingerprint),
    Summary(SyncSummary),
    Request(SyncRequest),
    Data(SyncData),
    Ack(SyncAck),
    Failure(SyncFailure),
    Page(SyncPage),
    Receipt(SyncReceipt),
}

#[derive(Clone)]
pub struct SyncEngine<S> {
    oplog: Oplog<S>,
    peer_id: PeerId,
}

/// Which operation bodies a negotiation materializes into `SyncPlan::send`.
#[derive(Clone, Copy)]
enum SendSet {
    Closure,
    #[cfg(feature = "iroh")]
    Page(PageBudget),
    /// Nothing: the caller only needs the plan's id sets.
    Empty,
}

impl<S: Storage> SyncEngine<S> {
    pub fn new(oplog: Oplog<S>, peer_id: PeerId) -> Self {
        Self { oplog, peer_id }
    }
    pub fn open(topic_id: TopicId, peer_id: PeerId, event_type_id: Option<String>) -> SyncOpen {
        SyncOpen {
            protocol: SYNC_PROTOCOL.into(),
            topic_id,
            peer_id,
            event_type_id,
        }
    }

    /// The topic as one view reads it. Heads, clock, tips and the digest all
    /// describe the same commit, so no part can come from a replaced branch.
    pub fn summary(&self, topic_id: TopicId) -> Result<SyncSummary> {
        let Some(view) = self.oplog.storage().topic_view(&topic_id, None)? else {
            return Ok(SyncSummary {
                topic_id,
                event_type_id: None,
                genesis: None,
                fingerprint: self.oplog.storage().topic_fingerprint(&topic_id)?,
                heads: BTreeSet::new(),
                actor_clock: ActorClock::new(),
                actor_tips: BTreeMap::new(),
            });
        };
        let fingerprint = self.view_digest(&view)?;
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
        })
    }

    pub fn fingerprint(&self, topic_id: TopicId) -> Result<SyncFingerprint> {
        let fingerprint = match self.oplog.storage().topic_view(&topic_id, None)? {
            Some(view) => self.view_digest(&view)?,
            None => self.oplog.storage().topic_fingerprint(&topic_id)?,
        };
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
    fn view_digest(&self, view: &TopicView) -> Result<[u8; 32]> {
        let (unresolved, _) = self.oplog.view_unresolved(view)?;
        if unresolved.is_empty() {
            return Ok(view.fingerprint);
        }
        Ok(*blake3::hash(&canonical_bytes(&(view.fingerprint, &unresolved))?).as_bytes())
    }

    pub fn negotiate(&self, peer_id: PeerId, remote: &SyncSummary) -> Result<SyncPlan> {
        self.negotiate_inner(peer_id, remote, SendSet::Closure, &mut false)
    }

    /// Plan a causal push page within `budget`, and whether more remains.
    #[cfg(feature = "iroh")]
    pub(crate) fn negotiate_page(
        &self,
        peer_id: PeerId,
        remote: &SyncSummary,
        budget: PageBudget,
    ) -> Result<(SyncPlan, bool)> {
        let mut more = false;
        let plan = self.negotiate_inner(peer_id, remote, SendSet::Page(budget), &mut more)?;
        Ok((plan, more))
    }

    /// Negotiate the id sets only. The returned plan's `send` is always empty.
    fn negotiate_request(&self, peer_id: PeerId, remote: &SyncSummary) -> Result<SyncPlan> {
        self.negotiate_inner(peer_id, remote, SendSet::Empty, &mut false)
    }

    fn negotiate_inner(
        &self,
        peer_id: PeerId,
        remote: &SyncSummary,
        send_set: SendSet,
        more: &mut bool,
    ) -> Result<SyncPlan> {
        // An unknown topic's remote heads are unauthenticated, so they never become
        // `need`. Bootstrap stages pages the inviter pushes or the transport pulls
        // with range hints, which the responder clamps and serves to members only.
        let Some(state) = self.oplog.storage().topic_state(&remote.topic_id)? else {
            return Ok(SyncPlan {
                topic_id: remote.topic_id,
                common: BTreeSet::new(),
                have: BTreeSet::new(),
                send: Vec::new(),
                need: BTreeSet::new(),
                actor_range_hints: Vec::new(),
            });
        };
        if !state.members.contains(&peer_id) {
            return Ok(SyncPlan {
                topic_id: remote.topic_id,
                common: BTreeSet::new(),
                have: state.heads,
                send: Vec::new(),
                need: BTreeSet::new(),
                actor_range_hints: Vec::new(),
            });
        }
        if let Some(remote_event_type_id) = &remote.event_type_id
            && *remote_event_type_id != state.event_type_id
        {
            return Err(Error::EventTypeMismatch {
                expected: state.event_type_id,
                actual: remote_event_type_id.clone(),
            });
        }

        let view = self
            .oplog
            .storage()
            .topic_view(&remote.topic_id, None)?
            .ok_or(Error::TopicNotFound)?;
        let local_heads = view.state.heads.clone();
        // A hole moves neither heads nor the clock, so a matching fingerprint
        // does not prove we are whole; keep negotiating until it is repaired.
        let (unresolved, _) = self.oplog.view_unresolved(&view)?;
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
            SendSet::Closure | SendSet::Empty => self.survey_local(remote)?,
            #[cfg(feature = "iroh")]
            SendSet::Page(_) => Default::default(),
        };
        let send = match send_set {
            SendSet::Closure => self.missing_closure(remote)?,
            #[cfg(feature = "iroh")]
            SendSet::Page(budget) => {
                let page = self.plan_page(
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
            SendSet::Empty => {
                *more = false;
                Vec::new()
            }
        };
        let mut need = BTreeSet::new();
        for id in &remote.heads {
            if !self.oplog.storage().dep_resolvable(id)? {
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
        #[cfg(feature = "iroh")]
        if matches!(send_set, SendSet::Page(_)) {
            need.retain(|id| {
                !remote
                    .actor_tips
                    .iter()
                    .any(|(actor_id, (seq, tip))| tip == id && view.clock.get(actor_id) < *seq)
            });
        }
        let actor_range_hints = self.needed_actor_ranges(remote, need.len())?;
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
        Ok(self.survey_local(remote)?.0)
    }

    /// Walk local heads down to the frontier the remote already has, reporting
    /// the common ancestors found there and every id the walk found stored
    /// incompletely. The second set is what anti-entropy repair asks the peer
    /// for; collecting it here costs no extra traversal.
    fn survey_local(&self, remote: &SyncSummary) -> Result<(BTreeSet<OpId>, BTreeSet<OpId>)> {
        let mut common = BTreeSet::new();
        let mut dangling = BTreeSet::new();
        let mut queue: VecDeque<_> = self
            .oplog
            .storage()
            .heads(&remote.topic_id)?
            .into_iter()
            .collect();
        let mut seen = BTreeSet::new();

        while let Some(id) = queue.pop_front() {
            if !seen.insert(id) {
                continue;
            }
            let Some(meta) = self.oplog.storage().get_meta(&id)? else {
                dangling.insert(id);
                continue;
            };
            if meta.topic_id != remote.topic_id {
                continue;
            }
            // Metadata alone lets the walk continue but cannot be served, so the
            // op record is requested while the traversal still uses the meta.
            if self.oplog.storage().get_op(&id)?.is_none() {
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
        let mut missing = BTreeSet::new();
        let mut stack: SmallVec<[OpId; 8]> = self
            .oplog
            .storage()
            .heads(&remote.topic_id)?
            .into_iter()
            .collect();
        while let Some(id) = stack.pop() {
            if missing.contains(&id) {
                continue;
            }
            let Some(meta) = self.oplog.storage().get_meta(&id)? else {
                continue;
            };
            if meta.topic_id != remote.topic_id || remote_contains(remote, &meta) {
                continue;
            }
            missing.insert(id);
            stack.extend(meta.deps.iter().copied());
        }
        topological_subset(self.oplog.storage(), &missing)
    }

    pub fn plan_data(&self, peer_id: PeerId, remote: &SyncSummary) -> Result<SyncData> {
        let ops = self.negotiate(peer_id, remote)?.send;
        Ok(SyncData {
            topic_id: remote.topic_id,
            ops,
        })
    }

    pub fn plan_request(&self, peer_id: PeerId, remote: &SyncSummary) -> Result<SyncRequest> {
        let plan = self.negotiate_request(peer_id, remote)?;
        let genesis = self
            .oplog
            .storage()
            .topic_state(&plan.topic_id)?
            .map(|state| state.genesis);
        Ok(SyncRequest {
            topic_id: plan.topic_id,
            known: plan.common,
            wants: plan.need,
            actor_range_hints: plan.actor_range_hints,
            genesis,
            credit: SyncCredit::default(),
        })
    }

    pub fn plan_response_data(&self, peer_id: PeerId, request: &SyncRequest) -> Result<SyncData> {
        self.response_inner(peer_id, request)
    }

    /// Return a causal prefix; request the remainder after acknowledging this page.
    #[cfg(feature = "iroh")]
    pub(crate) fn response_page(
        &self,
        peer_id: PeerId,
        request: &SyncRequest,
        budget: PageBudget,
    ) -> Result<PlannedPage> {
        let empty = PlannedPage {
            ops: Vec::new(),
            more: false,
        };
        let Some(view) = self.oplog.storage().topic_view(&request.topic_id, None)? else {
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
        if budget.ops == 0 || budget.bytes == 0 {
            return Ok(PlannedPage {
                ops: Vec::new(),
                more: asks,
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
        let (mut ops, repair_more) =
            self.plan_repair(&request.topic_id, &request.wants, &peer_clock, budget)?;
        // Wants this page could not carry keep their dependents out of the
        // forward ranges, which still serve every independent actor.
        let unsent = if repair_more {
            let sent = ops.iter().map(|op| op.id).collect::<BTreeSet<_>>();
            request.wants.difference(&sent).copied().collect()
        } else {
            BTreeSet::new()
        };
        let mut used = 0;
        for op in &ops {
            used += postcard::experimental::serialized_size(op)?;
            let body = &op.signed.body;
            if peer_clock.get(&body.actor_id) + 1 == body.actor_seq {
                peer_clock.set(body.actor_id, body.actor_seq);
            }
        }
        let rest = PageBudget {
            ops: budget.ops.saturating_sub(ops.len()),
            bytes: budget.bytes.saturating_sub(used),
        };
        if rest.ops == 0 || rest.bytes == 0 {
            return Ok(PlannedPage { ops, more: true });
        }
        let planned = self.plan_page(
            &request.topic_id,
            local,
            &peer_clock,
            Some(&goal),
            &unsent,
            rest,
        )?;
        ops.extend(planned.ops);
        Ok(PlannedPage {
            ops,
            more: repair_more || planned.more,
        })
    }

    fn response_inner(&self, peer_id: PeerId, request: &SyncRequest) -> Result<SyncData> {
        let Some(state) = self.oplog.storage().topic_state(&request.topic_id)? else {
            return Ok(SyncData {
                topic_id: request.topic_id,
                ops: Vec::new(),
            });
        };
        if !state.members.contains(&peer_id) {
            return Ok(SyncData {
                topic_id: request.topic_id,
                ops: Vec::new(),
            });
        }

        if request.actor_range_hints.len() > MAX_REQUEST_ITEMS
            || request.wants.len() > MAX_REQUEST_ITEMS
            || request.known.len() > MAX_REQUEST_ITEMS
        {
            return Err(Error::Storage("sync request exceeds work budget".into()));
        }
        let mut wanted = request.wants.clone();
        let local_clock = self.oplog.storage().actor_clock(&request.topic_id)?;
        let mut probes = request.wants.len() as u64;
        for hint in &request.actor_range_hints {
            if let Some((from, to)) = clamp_actor_range_hint(hint, local_clock.get(&hint.actor_id))
            {
                probes = probes.saturating_add(to - from);
                if probes > MAX_ACTOR_RANGE_HINT_SPAN {
                    return Err(Error::Storage("sync request exceeds work budget".into()));
                }
            }
        }
        let mut visited = BTreeSet::new();
        for hint in &request.actor_range_hints {
            let Some((from_exclusive, to_inclusive)) =
                clamp_actor_range_hint(hint, local_clock.get(&hint.actor_id))
            else {
                continue;
            };
            for seq in (from_exclusive + 1)..=to_inclusive {
                if !visited.insert((hint.actor_id, seq)) {
                    continue;
                }
                if let Some(op_id) =
                    self.oplog
                        .storage()
                        .actor_index(&request.topic_id, &hint.actor_id, seq)?
                {
                    wanted.insert(op_id);
                }
            }
        }
        let ops = {
            let wanted_closure =
                self.closure_excluding(&request.topic_id, wanted, &request.known)?;
            topological_subset(self.oplog.storage(), &wanted_closure)?
        };
        Ok(SyncData {
            topic_id: request.topic_id,
            ops,
        })
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

    /// Heads and clock an ack may certify, read from one view with their genesis. A topic
    /// holding an unresolvable id certifies nothing until repair completes, so the source
    /// keeps its obligation and this node stays visibly behind.
    fn ack_frontier(&self, topic_id: &TopicId) -> Result<(TopicState, BTreeSet<OpId>, ActorClock)> {
        let (view, whole) = self
            .oplog
            .whole_view(topic_id)?
            .ok_or(Error::TopicNotFound)?;
        if !whole {
            tracing::debug!(%topic_id, "withholding ack frontier for an incomplete topic");
            return Ok((view.state, BTreeSet::new(), ActorClock::new()));
        }
        let heads = view.state.heads.clone();
        Ok((view.state, heads, view.clock))
    }

    pub fn apply_ack(&self, ack: &SyncAck) -> Result<()> {
        ack.verify_signature()?;
        self.validate_ack(ack)?;
        // Storage repeats the identity and membership checks in the writing
        // transaction, so a reset or removal in between refuses the commit.
        self.oplog
            .storage()
            .apply_peer_ack(Self::peer_ack_for(ack))?;
        Ok(())
    }

    /// Apply many acks with the storage writes batched into one operation.
    /// Each ack is verified and validated individually so a bad ack does not
    /// block the others. Returns one result per input ack, in order.
    pub fn apply_acks(&self, acks: &[SyncAck]) -> Vec<Result<()>> {
        let mut results = Vec::with_capacity(acks.len());
        let mut validated = Vec::new();
        let mut peer_acks = Vec::new();
        for (index, ack) in acks.iter().enumerate() {
            match ack.verify_signature().and_then(|()| self.validate_ack(ack)) {
                Ok(()) => {
                    validated.push(index);
                    peer_acks.push(Self::peer_ack_for(ack));
                    results.push(Ok(()));
                }
                Err(err) => results.push(Err(err)),
            }
        }
        if peer_acks.is_empty() {
            return results;
        }
        // One uncertifiable record is reported against its own ack; only a
        // backend failure covering the whole batch fails the rest.
        match self.oplog.storage().apply_peer_acks(peer_acks) {
            Ok(applied) => {
                for (index, outcome) in validated.into_iter().zip(applied) {
                    if let Err(err) = outcome {
                        results[index] = Err(err);
                    }
                }
            }
            Err(err) => {
                let message = err.to_string();
                for index in validated {
                    results[index] = Err(Error::Storage(message.clone()));
                }
            }
        }
        results
    }

    pub fn record_peer_synced(&self, peer_id: PeerId, topic_id: TopicId) -> Result<()> {
        let (state, heads, clock) = self.ack_frontier(&topic_id)?;
        if !state.members.contains(&peer_id) {
            return Err(Error::NotTopicMember);
        }
        let peer_ack = PeerAck {
            peer_id,
            topic_id,
            genesis: Some(state.genesis),
            heads,
            clock,
        };
        self.oplog.storage().apply_peer_ack(peer_ack)?;
        Ok(())
    }

    /// Record that `peer_id` matched this topic's fingerprint. The compared
    /// fingerprint and the certified frontier come from the same view, so a
    /// reset after the comparison cannot swap in the replacement branch.
    #[cfg(feature = "iroh")]
    pub(crate) fn record_fingerprint(
        &self,
        peer_id: PeerId,
        topic_id: TopicId,
        fingerprint: [u8; 32],
    ) -> Result<bool> {
        let (state, heads, clock) = self.ack_frontier(&topic_id)?;
        if !state.members.contains(&peer_id) {
            return Err(Error::NotTopicMember);
        }
        if crate::storage::topic_fingerprint_for(&heads, &clock)? != fingerprint {
            return Ok(false);
        }
        self.oplog.storage().apply_peer_ack(PeerAck {
            peer_id,
            topic_id,
            genesis: Some(state.genesis),
            heads,
            clock,
        })?;
        Ok(true)
    }

    fn validate_ack(&self, ack: &SyncAck) -> Result<()> {
        let view = self
            .oplog
            .storage()
            .topic_view(&ack.topic_id, None)?
            .ok_or(Error::TopicNotFound)?;
        let state = &view.state;
        match ack.genesis {
            Some(genesis) if genesis == state.genesis => {}
            Some(_) => return Err(Error::StaleIncarnation),
            None => {
                return Err(Error::InvalidSyncAck(
                    "acknowledgement does not name the topic incarnation it certifies".into(),
                ));
            }
        }
        if !state.members.contains(&ack.peer_id) {
            return Err(Error::NotTopicMember);
        }

        // Only our own actor is locally bounded: we author all of its ops, so
        // no peer can hold more. Other actors reach a peer through a third
        // member before they reach us, so their entries stay unchecked.
        let local_actor = actor_id_for(ack.topic_id, self.peer_id);
        let local_seq = view.clock.get(&local_actor);
        let claimed_seq = ack.clock.get(&local_actor);
        if claimed_seq > local_seq {
            return Err(Error::InvalidSyncAck(format!(
                "clock for actor {local_actor} claims seq {claimed_seq}, local seq is {local_seq}"
            )));
        }

        for op_id in ack.accepted.iter().chain(ack.heads.iter()) {
            // History we have not learned yet makes no locally checkable claim.
            let Some(meta) = self.oplog.storage().get_meta(op_id)? else {
                continue;
            };
            if meta.topic_id != ack.topic_id {
                return Err(Error::TopicMismatch);
            }
            if ack.heads.contains(op_id) && ack.clock.get(&meta.actor_id) < meta.actor_seq {
                return Err(Error::InvalidSyncAck(format!(
                    "head {op_id} is not represented by ack clock"
                )));
            }
        }
        Ok(())
    }

    /// The stored record for a validated acknowledgement. One constructor means
    /// no evidence path can forget the incarnation the proof was signed for.
    fn peer_ack_for(ack: &SyncAck) -> PeerAck {
        PeerAck {
            peer_id: ack.peer_id,
            topic_id: ack.topic_id,
            genesis: ack.genesis,
            heads: ack.heads.clone(),
            clock: ack.clock.clone(),
        }
    }

    pub fn put_obligation(
        &self,
        peer_id: PeerId,
        topic_id: TopicId,
        op_ids: BTreeSet<OpId>,
    ) -> Result<()> {
        if op_ids.is_empty() {
            return Ok(());
        }
        // Ids are resolved against this branch, so the writes are conditioned on it.
        let genesis = self
            .oplog
            .storage()
            .topic_state(&topic_id)?
            .map(|state| state.genesis);
        let mut resolved = BTreeSet::new();
        let mut unresolved = BTreeSet::new();
        let mut target_clock = ActorClock::new();
        for op_id in &op_ids {
            if let Some(meta) = self.oplog.storage().get_meta(op_id)?
                && meta.topic_id == topic_id
            {
                target_clock.observe(meta.actor_id, meta.actor_seq);
                resolved.insert(*op_id);
            } else {
                unresolved.insert(*op_id);
            }
        }
        if !resolved.is_empty() {
            self.oplog.storage().put_sync_obligation(
                SyncObligation::clock(peer_id, topic_id, target_clock),
                genesis,
            )?;
        }
        // An id with no trustworthy actor position becomes an explicit repair
        // want, so the positions that did resolve still coalesce by clock.
        if !unresolved.is_empty() {
            self.oplog.storage().put_sync_obligation(
                SyncObligation::repair(peer_id, topic_id, unresolved),
                genesis,
            )?;
        }
        Ok(())
    }

    pub fn report(&self, peer_id: PeerId, topic_id: TopicId) -> Result<SyncReport> {
        Ok(SyncReport {
            topic_id,
            peer_id,
            obligations: self.oplog.storage().sync_obligations(&peer_id, &topic_id)?,
        })
    }

    fn needed_actor_ranges(
        &self,
        remote: &SyncSummary,
        wants: usize,
    ) -> Result<Vec<ActorRangeHint>> {
        let local_clock = self.oplog.storage().actor_clock(&remote.topic_id)?;
        let mut remaining = MAX_ACTOR_RANGE_HINT_SPAN.saturating_sub(wants as u64);
        Ok(remote
            .actor_clock
            .iter()
            .filter_map(|(actor_id, remote_seq)| {
                let local_seq = local_clock.get(actor_id);
                if *remote_seq <= local_seq || remaining == 0 {
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
            .collect())
    }

    #[cfg(feature = "iroh")]
    /// The next causal page for a peer at `peer`, merging forward actor ranges by generation
    /// (one past the highest dependency), so dependencies come first. Work grows with the
    /// page and the number of actors behind, never with history the peer holds.
    pub(crate) fn plan_page(
        &self,
        topic_id: &TopicId,
        local: &ActorClock,
        peer: &ActorClock,
        goal: Option<&ActorClock>,
        excluded: &BTreeSet<OpId>,
        budget: PageBudget,
    ) -> Result<PlannedPage> {
        let storage = self.oplog.storage();
        let limit_of = |actor_id: &ActorId, local_seq: u64| {
            goal.map_or(local_seq, |goal| goal.get(actor_id).min(local_seq))
        };
        // A zero allowance reads nothing; whether the goal holds more is known
        // from the clocks alone.
        if budget.ops == 0 || budget.bytes == 0 {
            return Ok(PlannedPage {
                ops: Vec::new(),
                more: local
                    .iter()
                    .any(|(actor_id, seq)| limit_of(actor_id, *seq) > peer.get(actor_id)),
            });
        }
        let mut heads = BinaryHeap::new();
        let mut metas = BTreeMap::new();
        let mut more = false;
        let mut deferred = BTreeSet::new();
        for (actor_id, local_seq) in local.iter() {
            let limit = limit_of(actor_id, *local_seq);
            let after = peer.get(actor_id);
            if limit <= after {
                continue;
            }
            if heads.len() >= MAX_PAGE_ACTORS {
                more = true;
                deferred.insert(*actor_id);
                continue;
            }
            more |=
                !self.push_range_head(topic_id, *actor_id, after, limit, &mut heads, &mut metas)?;
        }
        let mut covered = peer.clone();
        let mut blocked = excluded.clone();
        let mut ops = Vec::new();
        let mut bytes = 0_usize;
        while let Some(Reverse((generation, actor_id, seq, id, limit))) = heads.pop() {
            let meta = metas.remove(&id).ok_or(Error::MissingDependency(id))?;
            let mut waits = None;
            for dep in &meta.deps {
                let dep_position = match metas.get(dep) {
                    Some(dep_meta) => Some((dep_meta.actor_id, dep_meta.actor_seq)),
                    None => storage
                        .get_meta(dep)?
                        .map(|dep_meta| (dep_meta.actor_id, dep_meta.actor_seq)),
                };
                if blocked.contains(dep)
                    || dep_position.is_none_or(|(actor, dep_seq)| covered.get(&actor) < dep_seq)
                {
                    waits = Some(dep_position);
                    break;
                }
            }
            // A dependency on an actor left outside the window joins the page
            // once, within twice the window; its ops have lower generations, so
            // they pop before this op is tried again.
            if let Some(Some((dep_actor, _))) = waits
                && heads.len() < 2 * MAX_PAGE_ACTORS
                && deferred.remove(&dep_actor)
            {
                let dep_limit = limit_of(&dep_actor, local.get(&dep_actor));
                more |= !self.push_range_head(
                    topic_id,
                    dep_actor,
                    peer.get(&dep_actor),
                    dep_limit,
                    &mut heads,
                    &mut metas,
                )?;
                heads.push(Reverse((generation, actor_id, seq, id, limit)));
                metas.insert(id, meta);
                continue;
            }
            // A dependency behind a hole, an unsent repair or a blocked op stops
            // this actor only; independent actors keep filling the page.
            let Some(op) = waits
                .is_none()
                .then(|| storage.get_op(&id))
                .transpose()?
                .flatten()
            else {
                more = true;
                blocked.insert(id);
                continue;
            };
            let size = postcard::experimental::serialized_size(&op)?;
            if size > MAX_PAGE_BYTES {
                return Err(Error::Storage("operation exceeds sync page budget".into()));
            }
            if ops.len() >= budget.ops || bytes + size > budget.bytes {
                more = true;
                break;
            }
            bytes += size;
            covered.observe(actor_id, seq);
            ops.push(op);
            if seq < limit {
                more |= !self
                    .push_range_head(topic_id, actor_id, seq, limit, &mut heads, &mut metas)?;
            }
        }
        Ok(PlannedPage {
            ops,
            more: more || !heads.is_empty(),
        })
    }

    #[cfg(feature = "iroh")]
    /// Queue the op after `after` on `actor_id`, up to `limit`. Returns false
    /// when the index skips a position: the actor stops at that hole.
    fn push_range_head(
        &self,
        topic_id: &TopicId,
        actor_id: ActorId,
        after: u64,
        limit: u64,
        heads: &mut BinaryHeap<Reverse<RangeHead>>,
        metas: &mut BTreeMap<OpId, crate::storage::OpMeta>,
    ) -> Result<bool> {
        let storage = self.oplog.storage();
        let Some((seq, id)) = storage.actor_range(topic_id, &actor_id, after, 1)?.pop() else {
            return Ok(false);
        };
        if seq != after + 1 || seq > limit {
            return Ok(seq > limit);
        }
        let Some(meta) = storage.get_meta(&id)? else {
            return Ok(false);
        };
        heads.push(Reverse((meta.generation, actor_id, seq, id, limit)));
        metas.insert(id, meta);
        Ok(true)
    }

    #[cfg(feature = "iroh")]
    /// Requested repair ids and the ancestry the peer's clock does not cover,
    /// oldest first, bounded by the page.
    fn plan_repair(
        &self,
        topic_id: &TopicId,
        wants: &BTreeSet<OpId>,
        peer: &ActorClock,
        budget: PageBudget,
    ) -> Result<(Vec<Op>, bool)> {
        let storage = self.oplog.storage();
        let mut closure = BTreeSet::new();
        let mut covered = BTreeSet::new();
        let mut stack = wants.iter().copied().collect::<Vec<_>>();
        let mut more = false;
        while let Some(id) = stack.pop() {
            if closure.contains(&id) || covered.contains(&id) {
                continue;
            }
            if closure.len() >= MAX_PAGE_OPS {
                more = true;
                continue;
            }
            let Some(meta) = storage.get_meta(&id)? else {
                continue;
            };
            if meta.topic_id != *topic_id {
                continue;
            }
            // An explicit want overrides what the clock implies; ancestry does not.
            if !wants.contains(&id) && peer.get(&meta.actor_id) >= meta.actor_seq {
                covered.insert(id);
                continue;
            }
            closure.insert(id);
            stack.extend(meta.deps.iter().copied());
        }
        let mut ordered = topological_subset(storage, &closure)?;
        // Wants unconnected inside the closure still depend on each other through
        // covered ops, so a cut page must keep the oldest generations first.
        ordered.sort_by_key(|op| op.signed.body.generation);
        let mut ops = Vec::new();
        let mut sent = BTreeSet::new();
        let mut bytes = 0;
        for op in ordered {
            // A walk cut short leaves ancestors unsent; an op above them waits.
            let deps = &op.signed.body.deps;
            if !deps
                .iter()
                .all(|dep| sent.contains(dep) || covered.contains(dep))
            {
                more = true;
                continue;
            }
            let size = postcard::experimental::serialized_size(&op)?;
            if ops.len() >= budget.ops || bytes + size > budget.bytes {
                more = true;
                break;
            }
            bytes += size;
            sent.insert(op.id);
            ops.push(op);
        }
        Ok((ops, more))
    }

    fn closure_excluding(
        &self,
        topic_id: &TopicId,
        wants: BTreeSet<OpId>,
        known: &BTreeSet<OpId>,
    ) -> Result<BTreeSet<OpId>> {
        let mut out = BTreeSet::new();
        let mut queue: VecDeque<_> = wants.into_iter().collect();
        while let Some(id) = queue.pop_front() {
            if known.contains(&id) {
                continue;
            }
            if !out.insert(id) {
                continue;
            }
            if out.len() > MAX_REQUEST_ITEMS {
                return Err(Error::Storage(
                    "sync request closure exceeds work budget".into(),
                ));
            }
            // A peer may want an id we never had, or one a topic reset removed.
            // Serving what we do have keeps the exchange alive; the wanted id
            // stays in the peer's request set for a peer that holds it.
            let Some(meta) = self.oplog.storage().get_meta(&id)? else {
                out.remove(&id);
                continue;
            };
            if meta.topic_id != *topic_id {
                out.remove(&id);
                continue;
            }
            queue.extend(meta.deps.iter().copied());
        }
        Ok(out)
    }
}

/// Clamp a peer-supplied `ActorRangeHint` against our local knowledge so that
/// `from_exclusive < to_inclusive`, `to_inclusive <= local_seq` (we only walk
/// sequences we actually have), and the resulting span never exceeds
/// `MAX_ACTOR_RANGE_HINT_SPAN`. Returns `None` if the range is empty,
/// reversed, or otherwise unsalvageable.
fn clamp_actor_range_hint(hint: &ActorRangeHint, local_seq: u64) -> Option<(u64, u64)> {
    if hint.from_exclusive >= local_seq {
        return None;
    }
    let upper = hint.to_inclusive.min(local_seq);
    if upper <= hint.from_exclusive {
        return None;
    }
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
