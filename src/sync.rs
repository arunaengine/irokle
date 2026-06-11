// SPDX-License-Identifier: MIT OR Apache-2.0
//! Transport-neutral sync messages, planning, acknowledgements, and reports.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use ed25519_dalek::Signature;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::oplog::{Oplog, topological_subset};
use crate::storage::{PeerAck, Storage, SyncObligation};
use crate::{
    ActorClock, ActorId, Error, Op, OpId, PeerId, Result, Signer, TopicId, canonical_bytes, verify,
};

const SYNC_ACK_SIGNING_DOMAIN: &[u8] = b"irokle/sync-ack/1";

/// Maximum number of sequences a single ActorRangeHint may span. Caps both the
/// hint a peer can construct via `needed_actor_ranges` and the work
/// `plan_response_data` is willing to do for a peer-supplied hint, so a
/// malicious peer cannot push us into walking unbounded sequence ranges.
pub const MAX_ACTOR_RANGE_HINT_SPAN: u64 = 65_536;

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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SyncMessage {
    Open(SyncOpen),
    Fingerprint(SyncFingerprint),
    Summary(SyncSummary),
    Request(SyncRequest),
    Data(SyncData),
    Ack(SyncAck),
}

#[derive(Clone)]
pub struct SyncEngine<S> {
    oplog: Oplog<S>,
}

impl<S: Storage> SyncEngine<S> {
    pub fn new(oplog: Oplog<S>) -> Self {
        Self { oplog }
    }
    pub fn open(topic_id: TopicId, peer_id: PeerId, event_type_id: Option<String>) -> SyncOpen {
        SyncOpen {
            protocol: "irokle/sync/1".into(),
            topic_id,
            peer_id,
            event_type_id,
        }
    }

    pub fn summary(&self, topic_id: TopicId) -> Result<SyncSummary> {
        let event_type_id = self
            .oplog
            .storage()
            .topic_state(&topic_id)?
            .map(|state| state.event_type_id);
        Ok(SyncSummary {
            topic_id,
            event_type_id,
            fingerprint: self.oplog.storage().topic_fingerprint(&topic_id)?,
            heads: self.oplog.storage().heads(&topic_id)?,
            actor_clock: self.oplog.storage().actor_clock(&topic_id)?,
            actor_tips: self.actor_tips(topic_id)?,
        })
    }

    pub fn fingerprint(&self, topic_id: TopicId) -> Result<SyncFingerprint> {
        Ok(SyncFingerprint {
            topic_id,
            fingerprint: self.oplog.storage().topic_fingerprint(&topic_id)?,
        })
    }

    pub fn negotiate(&self, peer_id: PeerId, remote: &SyncSummary) -> Result<SyncPlan> {
        // If we don't know this topic locally, the remote's heads are
        // unauthenticated claims. We must not surface them as `need`
        // because that would let a peer inflate our request set for a topic
        // we can't even validate. Bootstrap for a new member happens when the
        // inviter pushes the genesis (and reachable history) via SyncData;
        // until then we have nothing to negotiate.
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

        let local_heads = self.oplog.storage().heads(&remote.topic_id)?;
        if self.oplog.storage().topic_fingerprint(&remote.topic_id)? == remote.fingerprint {
            return Ok(SyncPlan {
                topic_id: remote.topic_id,
                common: local_heads.clone(),
                have: local_heads,
                send: Vec::new(),
                need: BTreeSet::new(),
                actor_range_hints: Vec::new(),
            });
        }

        let common = self.find_common_ancestors(remote)?;
        let send = self.missing_closure(remote)?;
        let mut need = BTreeSet::new();
        for id in &remote.heads {
            if self.oplog.storage().get_meta(id)?.is_none() {
                need.insert(*id);
            }
        }
        Ok(SyncPlan {
            topic_id: remote.topic_id,
            common,
            have: local_heads,
            send,
            need,
            actor_range_hints: self.needed_actor_ranges(remote.topic_id, remote)?,
        })
    }

    pub fn find_common_ancestors(&self, remote: &SyncSummary) -> Result<BTreeSet<OpId>> {
        let mut common = BTreeSet::new();
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
                continue;
            };
            if meta.topic_id != remote.topic_id {
                continue;
            }
            if remote_contains(remote, &meta) {
                common.insert(id);
                continue;
            }
            queue.extend(meta.deps.iter().copied());
        }

        Ok(common)
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
        let plan = self.negotiate(peer_id, remote)?;
        Ok(SyncRequest {
            topic_id: plan.topic_id,
            known: plan.common,
            wants: plan.need,
            actor_range_hints: plan.actor_range_hints,
        })
    }

    pub fn plan_response_data(&self, peer_id: PeerId, request: &SyncRequest) -> Result<SyncData> {
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

        let mut wanted = request.wants.clone();
        let local_clock = self.oplog.storage().actor_clock(&request.topic_id)?;
        for hint in &request.actor_range_hints {
            let Some((from_exclusive, to_inclusive)) =
                clamp_actor_range_hint(hint, local_clock.get(&hint.actor_id))
            else {
                continue;
            };
            for seq in (from_exclusive + 1)..=to_inclusive {
                if let Some(op_id) =
                    self.oplog
                        .storage()
                        .actor_index(&request.topic_id, &hint.actor_id, seq)?
                {
                    wanted.insert(op_id);
                }
            }
        }
        let wanted_closure = self.closure_excluding(&request.topic_id, wanted, &request.known)?;
        let ops = topological_subset(self.oplog.storage(), &wanted_closure)?;
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
    ) -> Result<SyncAck> {
        self.receive_data_preverified(source_peer_id, ack_peer_id, data, &BTreeSet::new())
    }

    /// Like [`Self::receive_data`], but skips signature verification for ops
    /// whose id is in `verified` (the caller already ran [`Op::validate`] on
    /// those exact ops).
    pub(crate) fn receive_data_preverified(
        &self,
        source_peer_id: PeerId,
        ack_peer_id: PeerId,
        data: SyncData,
        verified: &BTreeSet<OpId>,
    ) -> Result<SyncAck> {
        for op in &data.ops {
            if op.signed.body.topic_id != data.topic_id {
                return Err(Error::TopicMismatch);
            }
        }
        let accepted =
            self.oplog
                .receive_ops_from_peer_preverified(Some(source_peer_id), data.ops, verified)?;
        Ok(SyncAck {
            topic_id: data.topic_id,
            peer_id: ack_peer_id,
            accepted,
            heads: self.oplog.storage().heads(&data.topic_id)?,
            clock: self.oplog.storage().actor_clock(&data.topic_id)?,
            signature: None,
        })
    }

    pub fn apply_ack(&self, ack: &SyncAck) -> Result<()> {
        ack.verify_signature()?;
        self.validate_ack(ack)?;
        let peer_ack = PeerAck {
            peer_id: ack.peer_id,
            topic_id: ack.topic_id,
            heads: ack.heads.clone(),
            clock: ack.clock.clone(),
        };
        self.oplog.storage().apply_peer_ack(peer_ack)?;
        Ok(())
    }

    pub fn record_peer_synced(&self, peer_id: PeerId, topic_id: TopicId) -> Result<()> {
        let state = self
            .oplog
            .storage()
            .topic_state(&topic_id)?
            .ok_or(Error::TopicNotFound)?;
        if !state.members.contains(&peer_id) {
            return Err(Error::NotTopicMember);
        }
        let peer_ack = PeerAck {
            peer_id,
            topic_id,
            heads: self.oplog.storage().heads(&topic_id)?,
            clock: self.oplog.storage().actor_clock(&topic_id)?,
        };
        self.oplog.storage().apply_peer_ack(peer_ack)?;
        Ok(())
    }

    fn validate_ack(&self, ack: &SyncAck) -> Result<()> {
        let state = self
            .oplog
            .storage()
            .topic_state(&ack.topic_id)?
            .ok_or(Error::TopicNotFound)?;
        if !state.members.contains(&ack.peer_id) {
            return Err(Error::NotTopicMember);
        }

        let local_clock = self.oplog.storage().actor_clock(&ack.topic_id)?;
        for (actor_id, seq) in ack.clock.iter() {
            let local_seq = local_clock.get(actor_id);
            if *seq > local_seq {
                return Err(Error::InvalidSyncAck(format!(
                    "clock for actor {actor_id} claims seq {seq}, local seq is {local_seq}"
                )));
            }
        }

        for op_id in ack.accepted.iter().chain(ack.heads.iter()) {
            let Some(meta) = self.oplog.storage().get_meta(op_id)? else {
                return Err(Error::InvalidSyncAck(format!(
                    "ack references unknown op {op_id}"
                )));
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

    pub fn put_obligation(
        &self,
        peer_id: PeerId,
        topic_id: TopicId,
        op_ids: BTreeSet<OpId>,
    ) -> Result<()> {
        let mut target_clock = ActorClock::new();
        for op_id in &op_ids {
            if let Some(meta) = self.oplog.storage().get_meta(op_id)?
                && meta.topic_id == topic_id
            {
                target_clock.observe(meta.actor_id, meta.actor_seq);
            }
        }
        self.oplog.storage().put_sync_obligation(SyncObligation {
            peer_id,
            topic_id,
            op_ids,
            target_clock,
        })
    }

    pub fn report(&self, peer_id: PeerId, topic_id: TopicId) -> Result<SyncReport> {
        Ok(SyncReport {
            topic_id,
            peer_id,
            obligations: self.oplog.storage().sync_obligations(&peer_id, &topic_id)?,
        })
    }

    fn actor_tips(&self, topic_id: TopicId) -> Result<BTreeMap<ActorId, (u64, OpId)>> {
        let mut tips = BTreeMap::new();
        for (actor_id, seq) in self.oplog.storage().actor_clock(&topic_id)?.iter() {
            if let Some((tip_seq, tip_id)) = self.oplog.storage().actor_tip(&topic_id, actor_id)?
                && tip_seq == *seq
            {
                tips.insert(*actor_id, (tip_seq, tip_id));
            }
        }
        Ok(tips)
    }

    fn needed_actor_ranges(
        &self,
        topic_id: TopicId,
        remote: &SyncSummary,
    ) -> Result<Vec<ActorRangeHint>> {
        let local_clock = self.oplog.storage().actor_clock(&topic_id)?;
        Ok(remote
            .actor_clock
            .iter()
            .filter_map(|(actor_id, remote_seq)| {
                let local_seq = local_clock.get(actor_id);
                if *remote_seq <= local_seq {
                    return None;
                }
                let to_inclusive = remote_seq
                    .saturating_sub(local_seq)
                    .min(MAX_ACTOR_RANGE_HINT_SPAN)
                    .saturating_add(local_seq);
                Some(ActorRangeHint {
                    actor_id: *actor_id,
                    from_exclusive: local_seq,
                    to_inclusive,
                })
            })
            .collect())
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
            let Some(meta) = self.oplog.storage().get_meta(&id)? else {
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
