// SPDX-License-Identifier: MIT OR Apache-2.0
//! One served stream: messages read in order, authorized against the open
//! topic, retained until the whole request is read, then one bounded reply.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use crate::net::frame::MAX_SYNC_DATA_OPS_PER_MESSAGE;
use crate::sync::SyncMessage;
use crate::{PeerId, Storage};

use super::budget::{ByteBudget, Charge, OwnedClass, Pool};
use super::{
    IROKLE_SYNC_ALPN, SharedNet, StreamLimits, invalid_data, message_topic_id,
    peer_id_from_endpoint_id, peer_may_open_topic,
};

/// The failure a sync message reports when its handling must be contained to
/// one topic instead of aborting the stream. Every message that names a topic
/// and does real work belongs here; the session validates the protocol and the
/// peer binding before this point, so those failures still indict the peer.
fn per_topic_failure_scope(message: &SyncMessage) -> Option<crate::sync::SyncFailure> {
    let (topic_id, code) = match message {
        SyncMessage::Open(open) => (open.topic_id, crate::sync::SyncFailureCode::Open),
        SyncMessage::Fingerprint(fingerprint) => (
            fingerprint.topic_id,
            crate::sync::SyncFailureCode::Fingerprint,
        ),
        SyncMessage::Summary(summary) => (summary.topic_id, crate::sync::SyncFailureCode::Summary),
        SyncMessage::Request(request) => (request.topic_id, crate::sync::SyncFailureCode::Request),
        SyncMessage::Data(data) => (data.topic_id, crate::sync::SyncFailureCode::Data),
        SyncMessage::Ack(_)
        | SyncMessage::Failure(_)
        | SyncMessage::Page(_)
        | SyncMessage::Receipt(_) => return None,
    };
    Some(crate::sync::SyncFailure { topic_id, code })
}

pub(super) struct SyncSession {
    authenticated_peer_id: PeerId,
    pub(super) remote_peer_id: Option<PeerId>,
    open_topic_id: Option<crate::TopicId>,
    open_allowed: bool,
    pub(super) acks: Vec<crate::sync::SyncAck>,
    pub(super) controls: Vec<SyncMessage>,
    /// One ack per topic that received data, covering every message of it.
    pub(super) replies: BTreeMap<crate::TopicId, crate::sync::SyncAck>,
    /// Newest staging receipt per topic that has no ack in this stream.
    pub(super) receipts: BTreeMap<crate::TopicId, crate::sync::SyncReceipt>,
    /// Requests to serve once the whole stream is read, latest per topic.
    pub(super) requests: BTreeMap<crate::TopicId, crate::sync::SyncRequest>,
    /// Bytes every message kept above may hold, never decreased.
    retained: usize,
    pub(super) charge: Option<Charge>,
}

impl SyncSession {
    pub(super) fn new(peer: iroh::EndpointId) -> Self {
        Self {
            authenticated_peer_id: peer_id_from_endpoint_id(peer),
            remote_peer_id: None,
            open_topic_id: None,
            open_allowed: false,
            acks: Vec::new(),
            controls: Vec::new(),
            replies: BTreeMap::new(),
            receipts: BTreeMap::new(),
            requests: BTreeMap::new(),
            retained: 0,
            charge: None,
        }
    }

    pub(super) fn retain(&mut self, message: &SyncMessage) -> io::Result<()> {
        let held = ByteBudget::held_bound(crate::net::framed_message_len(message)?);
        self.retained = self.retained.saturating_add(held);
        Ok(())
    }

    /// Charge what the session retains. Growth never waits, since the stream
    /// already holds session bytes; a full pool fails the stream instead.
    pub(super) fn hold(&mut self, budget: &Arc<ByteBudget>) -> io::Result<()> {
        let held = self.charge.as_ref().map_or(0, Charge::bytes);
        if self.retained <= held {
            return Ok(());
        }
        let added = budget.try_take(Pool::Session, self.retained - held, OwnedClass::Session)?;
        match &mut self.charge {
            Some(charge) => charge.merge(added),
            None => self.charge = Some(added),
        }
        Ok(())
    }

    /// Bytes the pages of every request may hold, at most their credits.
    pub(super) fn pages_bound(&self, limits: StreamLimits) -> usize {
        self.requests.values().fold(0, |bytes: usize, request| {
            let budget = crate::sync::PageBudget::from_credit(request.credit);
            let page = ByteBudget::page_bound(budget.bytes.min(limits.bytes), budget.ops);
            bytes.saturating_add(page)
        })
    }

    pub(super) fn handle<S: Storage>(
        &mut self,
        net: &SharedNet<S>,
        message: SyncMessage,
    ) -> io::Result<()> {
        if let SyncMessage::Open(open) = &message {
            if open.protocol.as_bytes() != IROKLE_SYNC_ALPN {
                return Err(invalid_data("unsupported sync protocol"));
            }
            if open.peer_id != self.authenticated_peer_id {
                return Err(invalid_data(
                    "sync open peer_id does not match iroh endpoint id",
                ));
            }
            self.remote_peer_id = Some(open.peer_id);
            self.open_topic_id = Some(open.topic_id);
            self.open_allowed = false;
            let allowed = match net.node.storage().topic_state(&open.topic_id) {
                Ok(state) => state.is_none_or(|state| peer_may_open_topic(&state, open.peer_id)),
                Err(error) => {
                    tracing::warn!(topic_id = %open.topic_id, %error, "failed to authorize sync topic");
                    false
                }
            };
            // Deny silently, like the non-member path in handle_message, rather
            // than replying with a failure code.
            if !allowed {
                return Ok(());
            }
            self.open_allowed = true;
        } else {
            if self.remote_peer_id.is_none() {
                return Err(invalid_data(
                    "sync message requires a preceding SyncOpen with peer_id",
                ));
            }
            if let Some(topic_id) = message_topic_id(&message)
                && self.open_topic_id != Some(topic_id)
            {
                return Err(invalid_data(
                    "sync message topic does not match SyncOpen topic",
                ));
            }
        }

        if !self.open_allowed {
            let failure = SyncMessage::Failure(crate::sync::SyncFailure {
                topic_id: message_topic_id(&message)
                    .ok_or_else(|| invalid_data("sync message requires a topic"))?,
                code: crate::sync::SyncFailureCode::Open,
            });
            self.retain(&failure)?;
            self.controls.push(failure);
            return Ok(());
        }
        if matches!(message, SyncMessage::Ack(_) | SyncMessage::Request(_)) {
            self.retain(&message)?;
        }

        match message {
            SyncMessage::Data(data) if data.ops.len() > MAX_SYNC_DATA_OPS_PER_MESSAGE => {
                Err(invalid_data("sync data has too many operations"))
            }
            SyncMessage::Ack(ack) => {
                self.acks.push(ack);
                Ok(())
            }
            SyncMessage::Request(request) => {
                self.requests.insert(request.topic_id, request);
                Ok(())
            }
            SyncMessage::Page(_) | SyncMessage::Receipt(_) => Err(invalid_data(
                "sync page and receipt are response-only messages",
            )),
            message => {
                // A data-plane failure fails only its topic, explicitly, so the
                // other topics batched into the stream keep their replies.
                // Framing and authentication failures above stay fatal.
                let failure = per_topic_failure_scope(&message);
                match net.handle_message(message, self.remote_peer_id) {
                    Ok(responses) => {
                        for response in responses {
                            self.keep_reply(net, response)?;
                        }
                        Ok(())
                    }
                    Err(error) => {
                        let failure = failure.ok_or(error)?;
                        tracing::warn!(topic_id = %failure.topic_id, "failing one sync topic");
                        let failure = SyncMessage::Failure(failure);
                        self.retain(&failure)?;
                        self.controls.push(failure);
                        Ok(())
                    }
                }
            }
        }
    }
}
