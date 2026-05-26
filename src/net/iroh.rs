// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeSet, HashMap};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

use smallvec::{SmallVec, smallvec};

use crate::node::select_sync_peers;
use crate::sync::SyncMessage;
use crate::{Irokle, MemoryStorage, PeerId, Storage};

use super::frame::MAX_FRAME_LEN;
use super::{
    _message_type_name, IROKLE_SYNC_ALPN, decode_sync_message, encode_frame, encode_sync_message,
    invalid_data, sync_data_messages,
};

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_SYNC_IO_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_RESYNC_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IrohRuntimeConfig {
    pub connect_timeout: Duration,
    pub sync_io_timeout: Duration,
    pub resync_interval: Duration,
}

impl Default for IrohRuntimeConfig {
    fn default() -> Self {
        Self {
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            sync_io_timeout: DEFAULT_SYNC_IO_TIMEOUT,
            resync_interval: DEFAULT_RESYNC_INTERVAL,
        }
    }
}

#[derive(Clone)]
struct ConnectionPool {
    endpoint: iroh::Endpoint,
    connections: Arc<RwLock<HashMap<iroh::EndpointId, iroh::endpoint::Connection>>>,
}

impl ConnectionPool {
    fn new(endpoint: iroh::Endpoint) -> Self {
        Self {
            endpoint,
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn endpoint(&self) -> &iroh::Endpoint {
        &self.endpoint
    }

    fn insert(&self, connection: iroh::endpoint::Connection) -> io::Result<iroh::EndpointId> {
        let peer = connection.remote_id();
        self.connections
            .write()
            .map_err(|_| io::Error::other("connection pool write lock poisoned"))?
            .insert(peer, connection);
        Ok(peer)
    }

    fn get(&self, peer: &iroh::EndpointId) -> io::Result<Option<iroh::endpoint::Connection>> {
        Ok(self
            .connections
            .read()
            .map_err(|_| io::Error::other("connection pool read lock poisoned"))?
            .get(peer)
            .filter(|connection| connection.close_reason().is_none())
            .cloned())
    }

    async fn get_or_connect(
        &self,
        peer: iroh::EndpointAddr,
        connect_timeout: Duration,
    ) -> io::Result<iroh::endpoint::Connection> {
        if let Some(connection) = self.get(&peer.id)? {
            return Ok(connection);
        }
        let connection = tokio::time::timeout(
            connect_timeout,
            self.endpoint.connect(peer, IROKLE_SYNC_ALPN),
        )
        .await
        .map_err(|_| timed_out("iroh connect timed out"))?
        .map_err(other)?;
        self.insert(connection.clone())?;
        Ok(connection)
    }
}

pub struct IrohNet<S: Storage = MemoryStorage> {
    pool: ConnectionPool,
    node: Irokle<S>,
    runtime: IrohRuntimeConfig,
    accept_started: AtomicBool,
    resync_started: AtomicBool,
    shutdown: tokio::sync::watch::Sender<bool>,
}

impl<S: Storage> IrohNet<S> {
    pub fn new(endpoint: iroh::Endpoint, node: Irokle<S>) -> io::Result<Self> {
        Self::new_with_alpns(endpoint, node, Vec::new())
    }

    pub fn new_with_config(
        endpoint: iroh::Endpoint,
        node: Irokle<S>,
        runtime: IrohRuntimeConfig,
    ) -> io::Result<Self> {
        Self::new_with_alpns_and_config(endpoint, node, Vec::new(), runtime)
    }

    pub fn new_with_alpns(
        endpoint: iroh::Endpoint,
        node: Irokle<S>,
        alpns: Vec<Vec<u8>>,
    ) -> io::Result<Self> {
        Self::new_with_alpns_and_config(endpoint, node, alpns, IrohRuntimeConfig::default())
    }

    pub fn new_with_alpns_and_config(
        endpoint: iroh::Endpoint,
        node: Irokle<S>,
        alpns: Vec<Vec<u8>>,
        runtime: IrohRuntimeConfig,
    ) -> io::Result<Self> {
        let endpoint_peer = peer_id_from_endpoint_id(endpoint.id());
        if endpoint_peer != node.peer_id() {
            return Err(invalid_data("iroh endpoint id does not match node signer"));
        }
        let alpns = extend_alpns(alpns);
        if !alpns.is_empty() {
            endpoint.set_alpns(alpns);
        }
        let (shutdown, _) = tokio::sync::watch::channel(false);
        Ok(Self {
            pool: ConnectionPool::new(endpoint),
            node,
            runtime,
            accept_started: AtomicBool::new(false),
            resync_started: AtomicBool::new(false),
            shutdown,
        })
    }

    pub fn node(&self) -> &Irokle<S> {
        &self.node
    }

    pub fn endpoint(&self) -> &iroh::Endpoint {
        self.pool.endpoint()
    }

    pub fn runtime_config(&self) -> IrohRuntimeConfig {
        self.runtime
    }

    pub async fn shutdown(&self) {
        let _ = self.shutdown.send(true);
        self.endpoint().close().await;
    }

    fn is_shutdown(&self) -> bool {
        *self.shutdown.borrow()
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
        let _ = self.spawn_accept_loop()?;
        Ok(())
    }

    pub fn spawn_accept_loop(self: &Arc<Self>) -> io::Result<Option<tokio::task::JoinHandle<()>>> {
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::other("iroh auto accept requires a Tokio runtime"))?;
        if self.accept_started.swap(true, Ordering::SeqCst) {
            return Ok(None);
        }
        let net = Arc::downgrade(self);
        let endpoint = self.endpoint().clone();
        let mut shutdown = self.shutdown.subscribe();
        Ok(Some(handle.spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let Some(current) = net.upgrade() else {
                    break;
                };
                if current.is_shutdown() || endpoint.is_closed() {
                    break;
                }
                drop(current);

                let incoming = tokio::select! {
                    Some(result) = connections.join_next(), if !connections.is_empty() => {
                        if let Err(error) = result {
                            tracing::warn!(%error, "iroh connection task failed");
                        }
                        continue;
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                        continue;
                    }
                    incoming = endpoint.accept() => incoming,
                };
                let Some(incoming) = incoming else {
                    break;
                };
                let Some(current) = net.upgrade() else {
                    break;
                };
                if current.is_shutdown() {
                    break;
                }
                drop(current);
                let accepted = tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                        continue;
                    }
                    accepted = incoming => accepted.map_err(other),
                };
                match accepted {
                    Ok(connection) => {
                        let peer = connection.remote_id();
                        let connection_net = Weak::clone(&net);
                        let connection_shutdown = shutdown.clone();
                        connections.spawn(async move {
                            handle_connection(
                                connection_net,
                                connection_shutdown,
                                peer,
                                connection,
                            )
                            .await;
                        });
                    }
                    Err(error) => {
                        tracing::warn!(%error, "failed to accept iroh connection");
                        continue;
                    }
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        })))
    }

    pub fn start_resync_loop(self: &Arc<Self>, interval: Duration) -> io::Result<()> {
        let _ = self.spawn_resync_loop(interval)?;
        Ok(())
    }

    pub fn start_configured_resync_loop(self: &Arc<Self>) -> io::Result<()> {
        self.start_resync_loop(self.runtime.resync_interval)
    }

    pub fn spawn_resync_loop(
        self: &Arc<Self>,
        interval: Duration,
    ) -> io::Result<Option<tokio::task::JoinHandle<()>>> {
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::other("iroh resync requires a Tokio runtime"))?;
        if self.resync_started.swap(true, Ordering::SeqCst) {
            return Ok(None);
        }
        let net = Arc::downgrade(self);
        let mut shutdown = self.shutdown.subscribe();
        Ok(Some(handle.spawn(async move {
            let mut tick = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                        continue;
                    }
                    _ = tick.tick() => {}
                }
                let Some(current) = net.upgrade() else {
                    break;
                };
                if current.is_shutdown() || current.endpoint().is_closed() {
                    break;
                }
                let targets = match current.resync_targets() {
                    Ok(targets) => targets,
                    Err(error) => {
                        tracing::warn!(%error, "failed to build resync targets");
                        continue;
                    }
                };
                drop(current);
                for (peer_id, topic_id) in targets {
                    let Some(current) = net.upgrade() else {
                        break;
                    };
                    if current.is_shutdown() {
                        break;
                    }
                    let result = tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                break;
                            }
                            continue;
                        }
                        result = current.sync_peer_now(peer_id, topic_id) => result,
                    };
                    if let Err(error) = result {
                        tracing::warn!(%peer_id, %topic_id, %error, "failed to resync peer");
                    }
                }
            }
        })))
    }

    pub async fn sync_with(
        &self,
        peer: iroh::EndpointAddr,
        messages: &[SyncMessage],
    ) -> io::Result<Vec<SyncMessage>> {
        let connection = self
            .pool
            .get_or_connect(peer, self.runtime.connect_timeout)
            .await?;
        let (mut send, mut recv) =
            tokio::time::timeout(self.runtime.sync_io_timeout, connection.open_bi())
                .await
                .map_err(|_| timed_out("sync stream open timed out"))?
                .map_err(other)?;
        write_sync_messages(&mut send, messages, self.runtime.sync_io_timeout).await?;
        read_sync_messages(&mut recv, self.runtime.sync_io_timeout).await
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
        let request = crate::sync::SyncRequest {
            topic_id: plan.topic_id,
            known: plan.common,
            wants: plan.need,
            actor_range_hints: plan.actor_range_hints,
        };

        let mut messages: SmallVec<[SyncMessage; 3]> =
            smallvec![SyncMessage::Open(self.node.sync_open(topic_id))];
        messages.extend(sync_data_messages(plan.topic_id, plan.send));
        if !request.wants.is_empty() || !request.actor_range_hints.is_empty() {
            messages.push(SyncMessage::Request(request));
        }
        if messages.len() == 1 {
            return Ok(());
        }

        let responses = self.sync_with(peer.clone(), &messages).await?;
        let mut followup: SmallVec<[SyncMessage; 2]> =
            smallvec![SyncMessage::Open(self.node.sync_open(topic_id))];
        let mut acks = Vec::new();
        for response in responses {
            match response {
                SyncMessage::Ack(ack) => {
                    if ack.peer_id != remote_peer_id || ack.topic_id != topic_id {
                        return Err(invalid_data("sync ack does not match remote peer/topic"));
                    }
                    acks.push(ack);
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
        for ack in acks {
            self.node.apply_sync_ack(&ack).map_err(invalid_data)?;
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
        let messages = read_sync_messages(&mut recv, self.runtime.sync_io_timeout).await?;
        let responses = self
            .handle_messages(peer, messages)?
            .into_iter()
            .collect::<Vec<_>>();
        write_sync_messages(&mut send, &responses, self.runtime.sync_io_timeout).await?;
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
        let mut acks = Vec::new();
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
            if let SyncMessage::Ack(ack) = message {
                if remote_peer_id.is_none() {
                    return Err(invalid_data(
                        "sync ack requires a preceding SyncOpen with peer_id",
                    ));
                }
                acks.push(ack);
                continue;
            }
            responses.extend(self.handle_message(message, remote_peer_id)?);
        }
        for ack in acks {
            responses.extend(self.handle_message(SyncMessage::Ack(ack), remote_peer_id)?);
        }
        Ok(responses)
    }

    fn resync_targets(&self) -> io::Result<BTreeSet<(PeerId, crate::TopicId)>> {
        let mut targets = self
            .node
            .storage()
            .all_sync_obligations()
            .map_err(invalid_data)?
            .into_iter()
            .map(|obligation| (obligation.peer_id, obligation.topic_id))
            .collect::<BTreeSet<_>>();
        for topic in self.node.storage().list_topics().map_err(invalid_data)? {
            let Some(state) = self
                .node
                .storage()
                .topic_state(&topic.topic_id)
                .map_err(invalid_data)?
            else {
                continue;
            };
            if !state.members.contains(&self.node.peer_id()) {
                continue;
            }
            targets.extend(
                select_sync_peers(topic.topic_id, self.node.peer_id(), &state)
                    .into_iter()
                    .map(|peer_id| (peer_id, topic.topic_id)),
            );
        }
        Ok(targets)
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
                    responses.extend(sync_data_messages(plan.topic_id, plan.send));
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
                let data = self
                    .node
                    .plan_sync_response_data(peer_id, &request)
                    .map_err(invalid_data)?;
                Ok(sync_data_messages(data.topic_id, data.ops))
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

async fn handle_connection<S: Storage>(
    net: Weak<IrohNet<S>>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    peer: iroh::EndpointId,
    connection: iroh::endpoint::Connection,
) {
    loop {
        let streams = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
            streams = connection.accept_bi() => streams,
        };
        let (send, recv) = match streams {
            Ok(streams) => streams,
            Err(error) => {
                tracing::debug!(%peer, %error, "iroh connection stopped accepting streams");
                break;
            }
        };
        let Some(current) = net.upgrade() else {
            break;
        };
        if current.is_shutdown() {
            break;
        }
        if let Err(error) = current.handle_stream(peer, recv, send).await {
            tracing::warn!(%peer, %error, "failed to handle iroh sync stream");
        }
    }
}

impl<S: Storage> Drop for IrohNet<S> {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

async fn read_sync_messages(
    recv: &mut iroh::endpoint::RecvStream,
    sync_io_timeout: Duration,
) -> io::Result<Vec<SyncMessage>> {
    let mut messages = Vec::new();
    while let Some(frame) = read_next_frame(recv, sync_io_timeout).await? {
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

async fn write_sync_messages(
    send: &mut iroh::endpoint::SendStream,
    messages: &[SyncMessage],
    sync_io_timeout: Duration,
) -> io::Result<()> {
    for message in messages {
        let payload = encode_sync_message(message)?;
        let frame = encode_frame(&payload)?;
        tokio::time::timeout(sync_io_timeout, send.write_all(&frame))
            .await
            .map_err(|_| timed_out("sync write timed out"))?
            .map_err(other)?;
    }
    send.finish().map_err(other)
}

async fn read_next_frame(
    recv: &mut iroh::endpoint::RecvStream,
    sync_io_timeout: Duration,
) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0_u8; 4];
    let Some(first_read) = read_some_with_timeout(recv, &mut len_buf[..1], sync_io_timeout).await?
    else {
        return Ok(None);
    };
    if first_read == 0 {
        return Ok(None);
    }

    let mut read = first_read;
    while read < len_buf.len() {
        let Some(n) = read_some_with_timeout(recv, &mut len_buf[read..], sync_io_timeout).await?
        else {
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
        tokio::time::timeout(sync_io_timeout, recv.read_exact(&mut payload))
            .await
            .map_err(|_| timed_out("sync read timed out"))?
            .map_err(other)?;
    }
    Ok(Some(payload))
}

async fn read_some_with_timeout(
    recv: &mut iroh::endpoint::RecvStream,
    buf: &mut [u8],
    sync_io_timeout: Duration,
) -> io::Result<Option<usize>> {
    tokio::time::timeout(sync_io_timeout, recv.read(buf))
        .await
        .map_err(|_| timed_out("sync read timed out"))?
        .map_err(other)
}

fn peer_id_from_endpoint_id(peer: iroh::EndpointId) -> PeerId {
    PeerId::from_bytes(*peer.as_bytes())
}

fn peer_id_to_endpoint_addr(peer_id: PeerId) -> io::Result<iroh::EndpointAddr> {
    Ok(iroh::EndpointAddr::from(
        iroh::EndpointId::from_bytes(peer_id.as_bytes()).map_err(invalid_data)?,
    ))
}

fn extend_alpns(mut alpns: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    let irokle = IROKLE_SYNC_ALPN.to_vec();
    if !alpns.contains(&irokle) {
        alpns.push(irokle);
    }
    alpns
}

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

fn timed_out(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, message)
}

fn other(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}
