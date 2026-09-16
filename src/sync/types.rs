// SPDX-License-Identifier: MIT OR Apache-2.0
//! Sync wire messages, page results, credits and budgets.

use std::collections::{BTreeMap, BTreeSet};

use ed25519_dalek::Signature;
use serde::{Deserialize, Serialize};

use crate::storage::SyncObligation;
use crate::{
    ActorClock, ActorId, Error, Op, OpId, PeerId, Result, Signer, TopicId, canonical_bytes, verify,
};

use super::{ACK_SIGNING_DOMAIN, MAX_PAGE_BYTES, MAX_PAGE_OPS};

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
    /// What the summary's author staged from the peer it answers, when it does
    /// not hold the topic: the branch, session and contiguous clock to continue.
    pub staged: Option<SyncReceipt>,
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

/// Actors fully described by request hints. From `after` (exclusive) through `through`
/// (inclusive), actors the requester is behind on are named and others held; outside, `behind`
/// excludes held actors and leaves others unknown. `None` leaves an end open; default holds all.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActorWindow {
    pub after: Option<ActorId>,
    pub through: Option<ActorId>,
    /// The actors outside the interval the requester is behind on, when their
    /// filter fits [`MAX_FILTER_BYTES`](super::MAX_FILTER_BYTES).
    pub behind: Option<ActorFilter>,
}

impl ActorWindow {
    /// Whether `actor_id` lies inside the id interval.
    pub fn contains(&self, actor_id: &ActorId) -> bool {
        self.after.is_none_or(|after| *actor_id > after)
            && self.through.is_none_or(|through| *actor_id <= through)
    }

    /// Whether the requester holds `actor_id` when no hint names it.
    pub fn holds(&self, actor_id: &ActorId) -> bool {
        self.contains(actor_id)
            || self
                .behind
                .as_ref()
                .is_some_and(|behind| !behind.contains(actor_id))
    }
}

/// A Bloom filter of actor ids: it contains every inserted actor and may
/// contain others. Actor ids are hashes, so their bytes choose the bits.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActorFilter {
    pub bits: Vec<u8>,
}

impl ActorFilter {
    /// Bits of each actor, from separate four-byte slices of its id.
    const PROBES: usize = 7;

    /// A filter of `actors` at ten bits each, or `None` past `max_bytes`.
    pub fn new(actors: &[ActorId], max_bytes: usize) -> Option<Self> {
        let bytes = actors.len().checked_mul(10)?.div_ceil(8).max(1);
        if bytes > max_bytes {
            return None;
        }
        Some(Self::sized(actors, bytes))
    }

    /// A filter of `actors` in `bytes` bytes, at least one.
    pub(crate) fn sized(actors: &[ActorId], bytes: usize) -> Self {
        let bytes = bytes.max(1);
        let mut bits = vec![0; bytes];
        for actor_id in actors {
            for bit in Self::probes(bytes * 8, actor_id) {
                bits[bit / 8] |= 1 << (bit % 8);
            }
        }
        Self { bits }
    }

    pub fn contains(&self, actor_id: &ActorId) -> bool {
        !self.bits.is_empty()
            && Self::probes(self.bits.len() * 8, actor_id)
                .all(|bit| self.bits[bit / 8] & (1 << (bit % 8)) != 0)
    }

    fn probes(bits: usize, actor_id: &ActorId) -> impl Iterator<Item = usize> + use<> {
        let id = *actor_id.as_bytes();
        (0..Self::PROBES).map(move |probe| {
            let slice = [
                id[probe * 4],
                id[probe * 4 + 1],
                id[probe * 4 + 2],
                id[probe * 4 + 3],
            ];
            u32::from_le_bytes(slice) as usize % bits
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncPlan {
    pub topic_id: TopicId,
    pub common: BTreeSet<OpId>,
    pub have: BTreeSet<OpId>,
    pub send: Vec<Op>,
    pub need: BTreeSet<OpId>,
    pub actor_range_hints: Vec<ActorRangeHint>,
    /// The actors `actor_range_hints` describe, see [`ActorWindow`].
    pub window: ActorWindow,
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
    /// The actors `actor_range_hints` describe, see [`ActorWindow`].
    pub window: ActorWindow,
}

/// What a requester is willing to receive for one page: a number of operations
/// and their postcard-serialized bytes. Framing is not counted; a transport
/// fits the page to its own wire limits on top of this credit.
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncPage {
    pub topic_id: TopicId,
    pub more: bool,
    /// Records the requested goal depends on that the responder does not hold.
    pub missing: BTreeSet<OpId>,
    /// Actors outside the request's window whose positions the page needed:
    /// the requester names them in its next request.
    pub positions: BTreeSet<ActorId>,
    /// The responder ended its work slice before it could send anything and
    /// kept its plan: the same request goes on from it.
    pub continued: bool,
}

/// Staged data progress for a topic the receiver does not hold, never an acknowledgement.
/// It certifies no state and clears no obligation, and names its branch and staging session so
/// replaced or expired receipts cannot count as current progress.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncReceipt {
    pub topic_id: TopicId,
    pub genesis: OpId,
    pub session: u64,
    pub clock: ActorClock,
}

/// Bounds of one planned page, in the units of [`SyncCredit`]: operations and
/// their serialized bytes. A transport adds its own framing on top and must fit
/// the page to its limits; `net::sync_data_page` sizes the wire frames exactly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PageBudget {
    pub ops: usize,
    pub bytes: usize,
}

impl PageBudget {
    /// The budget a credit allows, never above the page limits.
    pub fn from_credit(credit: SyncCredit) -> Self {
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
    /// Genesis of the certified incarnation. Because this is signed, a proof cannot move to a
    /// replaced branch. `None` is from a pre-contract peer or incoherent sender and certifies
    /// nothing.
    #[serde(default)]
    pub genesis: Option<OpId>,
    pub accepted: BTreeSet<OpId>,
    pub heads: BTreeSet<OpId>,
    pub clock: ActorClock,
    #[serde(default)]
    pub signature: Option<Signature>,
}

#[derive(Serialize)]
struct AckPayload<'a> {
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
        let mut bytes = ACK_SIGNING_DOMAIN.to_vec();
        bytes.extend_from_slice(&canonical_bytes(&AckPayload {
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
