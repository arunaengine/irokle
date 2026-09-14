// SPDX-License-Identifier: MIT OR Apache-2.0
//! One served stream: messages read in order, authorized against the open
//! topic, retained until the whole request is read, then one bounded reply.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Arc;

use crate::net::frame::MAX_SYNC_DATA_OPS_PER_MESSAGE;
use crate::sync::SyncMessage;
use crate::{PeerId, Storage};

use super::budget::{ByteBudget, Charge, OwnedClass, Pool};
use super::exchange::reply_fits;
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
    remote_peer_id: Option<PeerId>,
    open_topic_id: Option<crate::TopicId>,
    open_allowed: bool,
    acks: Vec<crate::sync::SyncAck>,
    controls: Vec<SyncMessage>,
    /// One ack per topic that received data, covering every message of it.
    replies: BTreeMap<crate::TopicId, crate::sync::SyncAck>,
    /// Newest staging receipt per topic that has no ack in this stream.
    receipts: BTreeMap<crate::TopicId, crate::sync::SyncReceipt>,
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

    fn retain(&mut self, message: &SyncMessage) -> io::Result<()> {
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

    /// Queue one reply, folding acks and receipts of a topic into the newest.
    fn keep_reply<S: Storage>(&mut self, net: &SharedNet<S>, reply: SyncMessage) -> io::Result<()> {
        self.retain(&reply)?;
        let mut ack = match reply {
            SyncMessage::Ack(ack) => ack,
            SyncMessage::Receipt(receipt) => {
                self.receipts.insert(receipt.topic_id, receipt);
                return Ok(());
            }
            reply => {
                self.controls.push(reply);
                return Ok(());
            }
        };
        // Promotion supersedes the staging this stream reported before.
        self.receipts.remove(&ack.topic_id);
        if let Some(earlier) = self.replies.remove(&ack.topic_id) {
            ack.accepted.extend(earlier.accepted);
            ack.sign(net.node.signer()).map_err(invalid_data)?;
        }
        self.replies.insert(ack.topic_id, ack);
        Ok(())
    }

    /// Apply the stream's acks independently, then reply: every control first,
    /// then one bounded page per request, sharing what is left of the stream
    /// budget among the requests still to serve. Pages hold at most `granted`
    /// bytes; the bytes they hold are returned with the reply.
    pub(super) fn finish<S: Storage>(
        &mut self,
        net: &SharedNet<S>,
        granted: usize,
    ) -> io::Result<(Vec<SyncMessage>, usize)> {
        let mut responses = std::mem::take(&mut self.controls);
        responses.extend(self.apply_acks(net)?);
        responses.extend(
            std::mem::take(&mut self.replies)
                .into_values()
                .map(SyncMessage::Ack),
        );
        responses.extend(
            std::mem::take(&mut self.receipts)
                .into_values()
                .map(SyncMessage::Receipt),
        );
        let requests = std::mem::take(&mut self.requests);
        let Some(peer_id) = self.remote_peer_id else {
            return Ok((responses, 0));
        };
        let page_len = crate::net::framed_message_len(&SyncMessage::Page(crate::sync::SyncPage {
            topic_id: crate::TopicId::default(),
            more: false,
            missing: BTreeSet::new(),
            positions: BTreeSet::new(),
            continued: false,
        }))?;
        let mut bytes = requests.len() * page_len;
        for response in &responses {
            bytes += crate::net::framed_message_len(response)?;
        }
        let mut messages = responses.len() + requests.len();
        let limits = net.limits;
        if bytes > limits.bytes || messages > limits.messages {
            return Err(invalid_data("sync reply controls exceed the stream budget"));
        }
        // Data is sized in framed wire bytes against what the controls and one
        // page result per request left. A request whose next op does not fit
        // its share is served again from what every other request left over.
        let mut left = requests.len();
        let mut held = 0_usize;
        let mut queue = requests.into_iter().collect::<Vec<_>>();
        let mut deferred = Vec::new();
        for pass in [false, true] {
            if pass {
                left = deferred.len();
                queue = std::mem::take(&mut deferred);
            }
            for (topic_id, request) in std::mem::take(&mut queue) {
                let share_bytes = (limits.bytes - bytes) / left;
                let share_messages = (limits.messages - messages) / left;
                left -= 1;
                let mut budget = crate::sync::PageBudget::from_credit(request.credit);
                budget.bytes = budget.bytes.min(share_bytes);
                budget.ops = budget
                    .ops
                    .min(share_messages.saturating_mul(MAX_SYNC_DATA_OPS_PER_MESSAGE));
                // What is left of the output grant bounds decoded pages too.
                let grant_left = granted.saturating_sub(held);
                budget.ops = budget.ops.min(grant_left / (2 * size_of::<crate::Op>()));
                let ops_bytes = ByteBudget::page_bound(0, budget.ops);
                budget.bytes = budget
                    .bytes
                    .min(grant_left.saturating_sub(ops_bytes) / super::budget::DECODED_FACTOR);
                let page = match net.node.response_page(peer_id, &request, budget) {
                    Ok(page) => page,
                    Err(error) => {
                        tracing::warn!(%topic_id, %error, "failing one sync request");
                        responses.push(SyncMessage::Failure(crate::sync::SyncFailure {
                            topic_id,
                            code: crate::sync::SyncFailureCode::Request,
                        }));
                        continue;
                    }
                };
                let data =
                    crate::net::sync_data_page(topic_id, page.ops, share_messages, share_bytes)?;
                let more = page.more || data.cut;
                if !pass
                    && data.messages.is_empty()
                    && more
                    && page.missing.is_empty()
                    && page.positions.is_empty()
                    && !page.continued
                {
                    deferred.push((topic_id, request));
                    continue;
                }
                bytes += data.bytes;
                messages += data.messages.len();
                let ops = data.messages.iter().fold(0, |ops, message| match message {
                    SyncMessage::Data(data) => ops + data.ops.len(),
                    _ => ops,
                });
                held = held.saturating_add(ByteBudget::page_bound(data.bytes, ops));
                responses.extend(data.messages);
                // Missing ids are advisory: they take only bytes no share needs.
                // Positions go last, since the next request depends on them.
                let mut result = crate::sync::SyncPage {
                    topic_id,
                    more,
                    missing: page.missing,
                    positions: page.positions,
                    continued: page.continued,
                };
                let mut extra =
                    crate::net::framed_message_len(&SyncMessage::Page(result.clone()))? - page_len;
                while extra > limits.bytes - bytes {
                    if result.missing.pop_last().is_none() {
                        result.positions.pop_last();
                    }
                    extra = crate::net::framed_message_len(&SyncMessage::Page(result.clone()))?
                        - page_len;
                }
                bytes += extra;
                held = held.saturating_add(ByteBudget::page_bound(page_len + extra, 0));
                responses.push(SyncMessage::Page(result));
            }
        }
        reply_fits(&responses, limits)?;
        Ok((responses, held))
    }

    /// One rejected ack, a stale clock after a reset or one bound to another
    /// peer, must not discard the others. Each rejection names its own topic.
    fn apply_acks<S: Storage>(&mut self, net: &SharedNet<S>) -> io::Result<Vec<SyncMessage>> {
        let acks = std::mem::take(&mut self.acks);
        if acks.is_empty() {
            return Ok(Vec::new());
        }
        let peer_id = self
            .remote_peer_id
            .ok_or_else(|| invalid_data("sync ack requires a preceding SyncOpen with peer_id"))?;
        let mut responses = Vec::new();
        let mut bound = Vec::new();
        for ack in acks {
            if ack.peer_id == peer_id {
                bound.push(ack);
                continue;
            }
            let topic_id = ack.topic_id;
            tracing::warn!(%topic_id, "dropping sync ack bound to another peer");
            responses.push(SyncMessage::Failure(crate::sync::SyncFailure {
                topic_id,
                code: crate::sync::SyncFailureCode::Ack,
            }));
        }
        for (ack, result) in bound.iter().zip(net.node.apply_sync_acks(&bound)) {
            match result {
                Ok(()) => {
                    net.receipt_log().clear(&(peer_id, ack.topic_id));
                    net.reconsider_target(peer_id, ack.topic_id);
                }
                Err(error) => {
                    let topic_id = ack.topic_id;
                    tracing::warn!(%topic_id, %error, "skipping rejected sync ack");
                    responses.push(SyncMessage::Failure(crate::sync::SyncFailure {
                        topic_id,
                        code: crate::sync::SyncFailureCode::Ack,
                    }));
                }
            }
        }
        Ok(responses)
    }
}
