// SPDX-License-Identifier: MIT OR Apache-2.0
//! Sync wire messages, page results, credits and budgets.

use std::collections::{BTreeMap, BTreeSet};

use ed25519_dalek::Signature;
use serde::{Deserialize, Serialize};

use crate::storage::SyncObligation;
use crate::{
    ActorClock, ActorId, Error, Op, OpId, PeerId, Result, Signer, TopicId, canonical_bytes, verify,
};

use super::{MAX_PAGE_BYTES, MAX_PAGE_OPS, SYNC_ACK_SIGNING_DOMAIN};

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
}

/// Staged progress of data for a topic the receiver does not hold yet. It is
/// never an ack: it certifies nothing and clears no obligation. It names the
/// branch and staging session it describes, so a receipt of a replaced or
/// expired staging is not read as progress of the current one.
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
