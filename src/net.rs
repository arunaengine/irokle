// SPDX-License-Identifier: MIT OR Apache-2.0
//! Iroh-backed sync framing, connection handling, and bounded resync loops.

#![allow(unexpected_cfgs)]

use std::io;

#[cfg(feature = "iroh")]
use std::collections::BTreeSet;
#[cfg(feature = "iroh")]
use std::collections::HashMap;
#[cfg(feature = "iroh")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "iroh")]
use std::sync::{Arc, Mutex};
#[cfg(feature = "iroh")]
use std::time::Duration;

use crate::sync::SyncMessage;
#[cfg(feature = "iroh")]
use crate::{Irokle, MemoryStorage, PeerId, Storage};
#[cfg(feature = "iroh")]
use smallvec::{SmallVec, smallvec};

const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;
#[cfg(feature = "iroh")]
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(feature = "iroh")]
const SYNC_IO_TIMEOUT: Duration = Duration::from_secs(30);
pub const IROKLE_SYNC_ALPN: &[u8] = b"irokle/sync/1";

pub fn encode_sync_message(message: &SyncMessage) -> io::Result<Vec<u8>> {
    postcard::to_allocvec(message).map_err(invalid_data)
}

pub fn decode_sync_message(bytes: &[u8]) -> io::Result<SyncMessage> {
    postcard::from_bytes(bytes).map_err(invalid_data)
}

pub fn encode_frame(payload: &[u8]) -> io::Result<Vec<u8>> {
    if payload.len() > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "sync frame exceeds maximum length",
        ));
    }
    let len = payload.len() as u32;
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

pub fn decode_frame(input: &[u8]) -> io::Result<Option<(Vec<u8>, usize)>> {
    if input.len() < 4 {
        return Ok(None);
    }

    let len = u32::from_be_bytes([input[0], input[1], input[2], input[3]]) as usize;
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "sync frame exceeds maximum length",
        ));
    }

    let end = 4 + len;
    if input.len() < end {
        return Ok(None);
    }

    Ok(Some((input[4..end].to_vec(), end)))
}

pub fn encode_frames<'a>(payloads: impl IntoIterator<Item = &'a [u8]>) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    for payload in payloads {
        out.extend_from_slice(&encode_frame(payload)?);
    }
    Ok(out)
}

pub fn decode_frames(mut input: &[u8]) -> io::Result<Vec<Vec<u8>>> {
    let mut frames = Vec::new();
    while !input.is_empty() {
        let Some((frame, consumed)) = decode_frame(input)? else {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete sync frame",
            ));
        };
        frames.push(frame);
        input = &input[consumed..];
    }
    Ok(frames)
}

#[cfg(feature = "iroh")]
#[derive(Clone)]
struct ConnectionPool {
    endpoint: iroh::Endpoint,
    connections: Arc<Mutex<HashMap<iroh::EndpointId, iroh::endpoint::Connection>>>,
}

#[cfg(feature = "iroh")]
impl ConnectionPool {
    fn new(endpoint: iroh::Endpoint) -> Self {
        Self {
            endpoint,
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn endpoint(&self) -> &iroh::Endpoint {
        &self.endpoint
    }

    fn insert(&self, connection: iroh::endpoint::Connection) -> io::Result<iroh::EndpointId> {
        let peer = connection.remote_id();
        self.connections
            .lock()
            .map_err(|_| io::Error::other("connection pool lock poisoned"))?
            .insert(peer, connection);
        Ok(peer)
    }

    fn get(&self, peer: &iroh::EndpointId) -> io::Result<Option<iroh::endpoint::Connection>> {
        Ok(self
            .connections
            .lock()
            .map_err(|_| io::Error::other("connection pool lock poisoned"))?
            .get(peer)
            .filter(|connection| connection.close_reason().is_none())
            .cloned())
    }

    async fn get_or_connect(
        &self,
        peer: iroh::EndpointAddr,
    ) -> io::Result<iroh::endpoint::Connection> {
        if let Some(connection) = self.get(&peer.id)? {
            return Ok(connection);
        }
        let connection = tokio::time::timeout(
            CONNECT_TIMEOUT,
            self.endpoint.connect(peer, IROKLE_SYNC_ALPN),
        )
        .await
        .map_err(|_| timed_out("iroh connect timed out"))?
        .map_err(other)?;
        self.insert(connection.clone())?;
        Ok(connection)
    }
}

#[cfg(feature = "iroh")]
pub struct IrohNet<S: Storage = MemoryStorage> {
    pool: ConnectionPool,
    node: Irokle<S>,
    resync_started: Arc<AtomicBool>,
}

#[cfg(feature = "iroh")]
impl<S: Storage> IrohNet<S> {
    pub fn new(endpoint: iroh::Endpoint, node: Irokle<S>) -> io::Result<Self> {
        Self::new_with_alpns(endpoint, node, Vec::new())
    }

    pub fn new_with_alpns(
        endpoint: iroh::Endpoint,
        node: Irokle<S>,
        alpns: Vec<Vec<u8>>,
    ) -> io::Result<Self> {
        let endpoint_peer = peer_id_from_endpoint_id(endpoint.id());
        if endpoint_peer != node.peer_id() {
            return Err(invalid_data("iroh endpoint id does not match node signer"));
        }
        let alpns = extend_alpns(alpns);
        if !alpns.is_empty() {
            endpoint.set_alpns(alpns);
        }
        Ok(Self {
            pool: ConnectionPool::new(endpoint),
            node,
            resync_started: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn node(&self) -> &Irokle<S> {
        &self.node
    }

    pub fn endpoint(&self) -> &iroh::Endpoint {
        self.pool.endpoint()
    }

    pub async fn sync_peer_now(&self, peer_id: PeerId, topic_id: crate::TopicId) -> io::Result<()> {
        let addr = peer_id_to_endpoint_addr(peer_id)?;
        self.sync_now(addr, topic_id).await
    }

    pub async fn sync_endpoint_now(
        &self,
        endpoint_id: iroh::EndpointId,
        topic_id: crate::TopicId,
    ) -> io::Result<()> {
        self.sync_now(iroh::EndpointAddr::from(endpoint_id), topic_id)
            .await
    }

    pub fn start_accept_loop(self: &Arc<Self>) -> io::Result<()> {
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::other("iroh auto accept requires a Tokio runtime"))?;
        let net = Arc::clone(self);
        handle.spawn(async move {
            loop {
                let Some(incoming) = net.endpoint().accept().await else {
                    break;
                };
                match incoming.await.map_err(other) {
                    Ok(connection) => {
                        let peer = connection.remote_id();
                        let connection_net = Arc::clone(&net);
                        tokio::spawn(async move {
                            connection_net.handle_connection(peer, connection).await;
                        });
                    }
                    Err(error) => {
                        tracing::warn!(%error, "failed to accept iroh connection");
                        continue;
                    }
                }
            }
        });
        Ok(())
    }

    pub fn start_resync_loop(self: &Arc<Self>, interval: Duration) -> io::Result<()> {
        if self.resync_started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::other("iroh resync requires a Tokio runtime"))?;
        let net = Arc::clone(self);
        handle.spawn(async move {
            let mut tick = tokio::time::interval(interval);
            loop {
                tick.tick().await;
                if net.endpoint().is_closed() {
                    break;
                }
                let obligations = match net.node.storage().all_sync_obligations() {
                    Ok(obligations) => obligations,
                    Err(error) => {
                        tracing::warn!(%error, "failed to read sync obligations");
                        continue;
                    }
                };
                let targets = obligations
                    .into_iter()
                    .map(|obligation| (obligation.peer_id, obligation.topic_id))
                    .collect::<BTreeSet<_>>();
                for (peer_id, topic_id) in targets {
                    if let Err(error) = net.sync_peer_now(peer_id, topic_id).await {
                        tracing::warn!(%peer_id, %topic_id, %error, "failed to resync peer");
                    }
                }
            }
        });
        Ok(())
    }

    async fn handle_connection(
        self: Arc<Self>,
        peer: iroh::EndpointId,
        connection: iroh::endpoint::Connection,
    ) {
        loop {
            let (send, recv) = match connection.accept_bi().await {
                Ok(streams) => streams,
                Err(error) => {
                    tracing::debug!(%peer, %error, "iroh connection stopped accepting streams");
                    break;
                }
            };
            if let Err(error) = self.handle_stream(peer, recv, send).await {
                tracing::warn!(%peer, %error, "failed to handle iroh sync stream");
            }
        }
    }

    pub async fn sync_with(
        &self,
        peer: iroh::EndpointAddr,
        messages: &[SyncMessage],
    ) -> io::Result<Vec<SyncMessage>> {
        let payloads = messages
            .iter()
            .map(encode_sync_message)
            .collect::<io::Result<Vec<_>>>()?;
        let body = encode_frames(payloads.iter().map(Vec::as_slice))?;
        let connection = self.pool.get_or_connect(peer).await?;
        let (mut send, mut recv) = tokio::time::timeout(SYNC_IO_TIMEOUT, connection.open_bi())
            .await
            .map_err(|_| timed_out("sync stream open timed out"))?
            .map_err(other)?;
        tokio::time::timeout(SYNC_IO_TIMEOUT, send.write_all(&body))
            .await
            .map_err(|_| timed_out("sync write timed out"))?
            .map_err(other)?;
        send.finish().map_err(other)?;
        read_sync_messages(&mut recv).await
    }

    pub async fn sync_now(
        &self,
        peer: iroh::EndpointAddr,
        topic_id: crate::TopicId,
    ) -> io::Result<()> {
        let remote_peer_id = peer_id_from_endpoint_id(peer.id);
        let result = self.sync_now_inner(peer, topic_id).await;
        let record_result = match &result {
            Ok(()) => Ok(()),
            Err(error) => Err(error),
        };
        let _ = self
            .node
            .record_sync_result(remote_peer_id, topic_id, record_result);
        result
    }

    async fn sync_now_inner(
        &self,
        peer: iroh::EndpointAddr,
        topic_id: crate::TopicId,
    ) -> io::Result<()> {
        let remote_peer_id = peer_id_from_endpoint_id(peer.id);
        let local_fingerprint = self.node.sync_fingerprint(topic_id).map_err(invalid_data)?;
        let responses = self
            .sync_with(
                peer.clone(),
                &[
                    SyncMessage::Open(self.node.sync_open(topic_id)),
                    SyncMessage::Fingerprint(local_fingerprint.clone()),
                ],
            )
            .await?;
        let mut summary = None;
        for response in responses {
            match response {
                SyncMessage::Fingerprint(remote)
                    if remote.topic_id == topic_id
                        && remote.fingerprint == local_fingerprint.fingerprint =>
                {
                    self.node
                        .record_peer_synced(remote_peer_id, topic_id)
                        .map_err(invalid_data)?;
                    return Ok(());
                }
                SyncMessage::Summary(remote_summary) if remote_summary.topic_id == topic_id => {
                    summary = Some(remote_summary)
                }
                other => {
                    return Err(invalid_data(format!(
                        "unexpected sync response {}",
                        _message_type_name(&other)
                    )));
                }
            }
        }
        let summary = summary.ok_or_else(|| invalid_data("peer did not return a sync summary"))?;
        let plan = self
            .node
            .negotiate_sync(remote_peer_id, &summary)
            .map_err(invalid_data)?;
        let data = crate::sync::SyncData {
            topic_id: plan.topic_id,
            ops: plan.send,
        };
        let request = crate::sync::SyncRequest {
            topic_id: plan.topic_id,
            known: plan.common,
            wants: plan.need,
            actor_range_hints: plan.actor_range_hints,
        };

        let mut messages: SmallVec<[SyncMessage; 3]> =
            smallvec![SyncMessage::Open(self.node.sync_open(topic_id))];
        if !data.ops.is_empty() {
            messages.push(SyncMessage::Data(data));
        }
        if !request.wants.is_empty() || !request.actor_range_hints.is_empty() {
            messages.push(SyncMessage::Request(request));
        }
        if messages.len() == 1 {
            return Ok(());
        }

        let responses = self.sync_with(peer.clone(), &messages).await?;
        let mut followup: SmallVec<[SyncMessage; 2]> =
            smallvec![SyncMessage::Open(self.node.sync_open(topic_id))];
        for response in responses {
            match response {
                SyncMessage::Ack(ack) => {
                    if ack.peer_id != remote_peer_id || ack.topic_id != topic_id {
                        return Err(invalid_data("sync ack does not match remote peer/topic"));
                    }
                    self.node.apply_sync_ack(&ack).map_err(invalid_data)?;
                }
                SyncMessage::Summary(summary) if summary.topic_id == topic_id => {}
                SyncMessage::Data(data) => {
                    let ack = self
                        .node
                        .receive_sync_data_from(remote_peer_id, data)
                        .map_err(invalid_data)?;
                    followup.push(SyncMessage::Ack(ack));
                }
                other => {
                    return Err(invalid_data(format!(
                        "unexpected sync response {}",
                        _message_type_name(&other)
                    )));
                }
            }
        }
        if followup.len() > 1 {
            let responses = self.sync_with(peer, &followup).await?;
            for response in responses {
                match response {
                    SyncMessage::Summary(summary) if summary.topic_id == topic_id => {}
                    other => {
                        return Err(invalid_data(format!(
                            "unexpected sync ack response {}",
                            _message_type_name(&other)
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn accept_one(&self) -> io::Result<Option<iroh::EndpointId>> {
        let Some(incoming) = self.endpoint().accept().await else {
            return Ok(None);
        };
        let connection = incoming.await.map_err(other)?;
        let peer = connection.remote_id();
        let (send, recv) = connection.accept_bi().await.map_err(other)?;
        self.handle_stream(peer, recv, send).await?;
        Ok(Some(peer))
    }

    pub async fn handle_stream(
        &self,
        peer: iroh::EndpointId,
        mut recv: iroh::endpoint::RecvStream,
        mut send: iroh::endpoint::SendStream,
    ) -> io::Result<()> {
        let messages = read_sync_messages(&mut recv).await?;
        let responses = self
            .handle_messages(peer, messages)?
            .iter()
            .map(encode_sync_message)
            .collect::<io::Result<Vec<_>>>()?;
        let out = encode_frames(responses.iter().map(Vec::as_slice))?;
        send.write_all(&out).await.map_err(other)?;
        send.finish().map_err(other)?;
        Ok(())
    }

    pub fn handle_messages(
        &self,
        peer: iroh::EndpointId,
        messages: Vec<SyncMessage>,
    ) -> io::Result<Vec<SyncMessage>> {
        let authenticated_peer_id = peer_id_from_endpoint_id(peer);
        let mut remote_peer_id = None;
        let mut open_topic_id = None;
        let mut responses = Vec::new();
        for message in messages {
            if let SyncMessage::Open(open) = &message {
                if open.protocol.as_bytes() != IROKLE_SYNC_ALPN {
                    return Err(invalid_data("unsupported sync protocol"));
                }
                if open.peer_id != authenticated_peer_id {
                    return Err(invalid_data(
                        "sync open peer_id does not match iroh endpoint id",
                    ));
                }
                remote_peer_id = Some(open.peer_id);
                open_topic_id = Some(open.topic_id);
            } else if let Some(topic_id) = message_topic_id(&message)
                && open_topic_id != Some(topic_id)
            {
                return Err(invalid_data(
                    "sync message topic does not match SyncOpen topic",
                ));
            }
            responses.extend(self.handle_message(message, remote_peer_id)?);
        }
        Ok(responses)
    }

    fn handle_message(
        &self,
        message: SyncMessage,
        remote_peer_id: Option<PeerId>,
    ) -> io::Result<Vec<SyncMessage>> {
        match message {
            SyncMessage::Open(open) => {
                let peer_id = remote_peer_id
                    .ok_or_else(|| invalid_data("sync open requires authenticated peer context"))?;
                if let Some(state) = self
                    .node
                    .storage()
                    .topic_state(&open.topic_id)
                    .map_err(invalid_data)?
                    && !state.members.contains(&peer_id)
                {
                    return Ok(Vec::new());
                }
                // Unknown topics return an empty local summary so an inviter can
                // bootstrap a new member by pushing the signed genesis/history.
                self.node
                    .sync_summary(open.topic_id)
                    .map(SyncMessage::Summary)
                    .map(|message| vec![message])
                    .map_err(invalid_data)
            }
            SyncMessage::Fingerprint(fingerprint) => {
                let peer_id = remote_peer_id.ok_or_else(|| {
                    invalid_data("sync fingerprint requires a preceding SyncOpen with peer_id")
                })?;
                let Some(state) = self
                    .node
                    .storage()
                    .topic_state(&fingerprint.topic_id)
                    .map_err(invalid_data)?
                else {
                    return Ok(Vec::new());
                };
                if !state.members.contains(&peer_id) {
                    return Ok(Vec::new());
                }
                let local = self
                    .node
                    .sync_fingerprint(fingerprint.topic_id)
                    .map_err(invalid_data)?;
                if local.fingerprint == fingerprint.fingerprint {
                    self.node
                        .record_peer_synced(peer_id, fingerprint.topic_id)
                        .map_err(invalid_data)?;
                    Ok(vec![SyncMessage::Fingerprint(local)])
                } else {
                    self.node
                        .sync_summary(fingerprint.topic_id)
                        .map(SyncMessage::Summary)
                        .map(|message| vec![message])
                        .map_err(invalid_data)
                }
            }
            SyncMessage::Summary(summary) => {
                let peer_id = remote_peer_id.ok_or_else(|| {
                    invalid_data("sync summary requires a preceding SyncOpen with peer_id")
                })?;
                let plan = self
                    .node
                    .negotiate_sync(peer_id, &summary)
                    .map_err(invalid_data)?;
                let mut responses = Vec::new();
                if !plan.send.is_empty() {
                    responses.push(SyncMessage::Data(crate::sync::SyncData {
                        topic_id: plan.topic_id,
                        ops: plan.send,
                    }));
                }
                if !plan.need.is_empty() || !plan.actor_range_hints.is_empty() {
                    responses.push(SyncMessage::Request(crate::sync::SyncRequest {
                        topic_id: plan.topic_id,
                        known: plan.common,
                        wants: plan.need,
                        actor_range_hints: plan.actor_range_hints,
                    }));
                }
                Ok(responses)
            }
            SyncMessage::Request(request) => {
                let peer_id = remote_peer_id.ok_or_else(|| {
                    invalid_data("sync request requires a preceding SyncOpen with peer_id")
                })?;
                self.node
                    .plan_sync_response_data(peer_id, &request)
                    .map(SyncMessage::Data)
                    .map(|message| vec![message])
                    .map_err(invalid_data)
            }
            SyncMessage::Data(data) => {
                let source_peer = remote_peer_id.ok_or_else(|| {
                    invalid_data("sync data requires a preceding SyncOpen with peer_id")
                })?;
                self.node
                    .ensure_iroh_peer_whitelisted(source_peer, &data)
                    .map_err(invalid_data)?;
                self.node
                    .receive_sync_data_from(source_peer, data)
                    .map(SyncMessage::Ack)
                    .map(|message| vec![message])
                    .map_err(invalid_data)
            }
            SyncMessage::Ack(ack) => {
                let peer_id = remote_peer_id.ok_or_else(|| {
                    invalid_data("sync ack requires a preceding SyncOpen with peer_id")
                })?;
                if ack.peer_id != peer_id {
                    return Err(invalid_data(
                        "sync ack peer_id does not match SyncOpen peer_id",
                    ));
                }
                self.node
                    .apply_sync_ack(&ack)
                    .map(|()| Vec::new())
                    .map_err(invalid_data)
            }
        }
    }
}

#[cfg(feature = "iroh")]
async fn read_sync_messages(recv: &mut iroh::endpoint::RecvStream) -> io::Result<Vec<SyncMessage>> {
    let mut messages = Vec::new();
    while let Some(frame) = read_next_frame(recv).await? {
        let frame_index = messages.len();
        let frame_len = frame.len();
        messages.push(decode_sync_message(&frame).map_err(|err| {
            invalid_data(format!(
                "invalid sync message frame {frame_index} ({frame_len} bytes): {err}"
            ))
        })?);
    }
    Ok(messages)
}

#[cfg(feature = "iroh")]
async fn read_next_frame(recv: &mut iroh::endpoint::RecvStream) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0_u8; 4];
    let Some(first_read) = read_some_with_timeout(recv, &mut len_buf[..1]).await? else {
        return Ok(None);
    };
    if first_read == 0 {
        return Ok(None);
    }

    let mut read = first_read;
    while read < len_buf.len() {
        let Some(n) = read_some_with_timeout(recv, &mut len_buf[read..]).await? else {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete sync frame length",
            ));
        };
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete sync frame length",
            ));
        }
        read += n;
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "sync frame exceeds maximum length",
        ));
    }
    let mut payload = vec![0_u8; len];
    if len > 0 {
        tokio::time::timeout(SYNC_IO_TIMEOUT, recv.read_exact(&mut payload))
            .await
            .map_err(|_| timed_out("sync read timed out"))?
            .map_err(other)?;
    }
    Ok(Some(payload))
}

#[cfg(feature = "iroh")]
async fn read_some_with_timeout(
    recv: &mut iroh::endpoint::RecvStream,
    buf: &mut [u8],
) -> io::Result<Option<usize>> {
    tokio::time::timeout(SYNC_IO_TIMEOUT, recv.read(buf))
        .await
        .map_err(|_| timed_out("sync read timed out"))?
        .map_err(other)
}

#[cfg(feature = "iroh")]
fn peer_id_from_endpoint_id(peer: iroh::EndpointId) -> PeerId {
    PeerId::from_bytes(*peer.as_bytes())
}

#[cfg(feature = "iroh")]
fn peer_id_to_endpoint_addr(peer_id: PeerId) -> io::Result<iroh::EndpointAddr> {
    Ok(iroh::EndpointAddr::from(
        iroh::EndpointId::from_bytes(peer_id.as_bytes()).map_err(invalid_data)?,
    ))
}

#[cfg(feature = "iroh")]
fn extend_alpns(mut alpns: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    let irokle = IROKLE_SYNC_ALPN.to_vec();
    if !alpns.contains(&irokle) {
        alpns.push(irokle);
    }
    alpns
}

#[cfg(feature = "iroh")]
fn message_topic_id(message: &SyncMessage) -> Option<crate::TopicId> {
    match message {
        SyncMessage::Open(open) => Some(open.topic_id),
        SyncMessage::Fingerprint(fingerprint) => Some(fingerprint.topic_id),
        SyncMessage::Summary(summary) => Some(summary.topic_id),
        SyncMessage::Request(request) => Some(request.topic_id),
        SyncMessage::Data(data) => Some(data.topic_id),
        SyncMessage::Ack(ack) => Some(ack.topic_id),
    }
}

pub fn _message_type_name(message: &SyncMessage) -> &'static str {
    match message {
        SyncMessage::Open(_) => "open",
        SyncMessage::Fingerprint(_) => "fingerprint",
        SyncMessage::Summary(_) => "summary",
        SyncMessage::Request(_) => "request",
        SyncMessage::Data(_) => "data",
        SyncMessage::Ack(_) => "ack",
    }
}

fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[cfg(feature = "iroh")]
fn timed_out(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, message)
}

#[cfg(feature = "iroh")]
fn other(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}
