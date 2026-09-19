// SPDX-License-Identifier: MIT OR Apache-2.0
//! The node's Iroh transport: the builder's settings and the node it attaches
//! to an endpoint.

use crate::node::{Irokle, IrokleBuilder, ReceiveOutcome, WriteConcern};
use crate::storage::Storage;
use crate::sync::{SyncData, SyncEngine};
use crate::{Ed25519Signer, Error, OpId, PeerId, Result, TopicEviction, TopicId};
use std::collections::BTreeSet;
use std::sync::Arc;

/// Topics `sync_topic_now` synchronizes at once.
const SYNC_TOPIC_CONCURRENCY: usize = 8;

/// The Iroh transport settings of an [`IrokleBuilder`].
pub(crate) struct IrohSettings {
    pub(super) endpoint: Option<iroh::Endpoint>,
    alpns: Vec<Vec<u8>>,
    auto_accept: bool,
    runtime: crate::net::IrohRuntimeConfig,
    eviction_sink: Option<tokio::sync::mpsc::UnboundedSender<TopicEviction>>,
}

impl Default for IrohSettings {
    fn default() -> Self {
        Self {
            endpoint: None,
            alpns: Vec::new(),
            auto_accept: true,
            runtime: crate::net::IrohRuntimeConfig::default(),
            eviction_sink: None,
        }
    }
}

impl<S: Storage> IrokleBuilder<S> {
    pub fn with_iroh_runtime_config(mut self, runtime: crate::net::IrohRuntimeConfig) -> Self {
        self.iroh.runtime = runtime;
        self
    }

    /// Forward genesis tie-break evictions produced by the builder-managed Iroh
    /// transport to `sink`.
    pub fn with_eviction_sink(
        mut self,
        sink: tokio::sync::mpsc::UnboundedSender<crate::TopicEviction>,
    ) -> Self {
        self.iroh.eviction_sink = Some(sink);
        self
    }

    pub fn with_iroh_secret_key(mut self, secret_key: &iroh::SecretKey) -> Self {
        self.config.signer = Ed25519Signer::from_iroh_secret_key(secret_key);
        self.signer_explicit = true;
        self
    }

    pub fn with_net(mut self, endpoint: iroh::Endpoint) -> Self {
        if !self.signer_explicit {
            self.config.signer = Ed25519Signer::from_iroh_secret_key(endpoint.secret_key());
        }
        if !self.write_concern_explicit {
            self.config.default_write_concern = WriteConcern::AsyncReplication;
        }
        self.iroh.endpoint = Some(endpoint);
        self.iroh.auto_accept = true;
        self
    }

    pub fn with_alpn(mut self, alpn: impl AsRef<[u8]>) -> Self {
        let alpn = alpn.as_ref().to_vec();
        if !self.iroh.alpns.contains(&alpn) {
            self.iroh.alpns.push(alpn);
        }
        self
    }

    pub fn with_alpns<I, A>(mut self, alpns: I) -> Self
    where
        I: IntoIterator<Item = A>,
        A: AsRef<[u8]>,
    {
        for alpn in alpns {
            let alpn = alpn.as_ref().to_vec();
            if !self.iroh.alpns.contains(&alpn) {
                self.iroh.alpns.push(alpn);
            }
        }
        self
    }

    pub fn without_auto_accept(mut self) -> Self {
        self.iroh.auto_accept = false;
        self
    }

    /// Build the node and attach it to the configured endpoint, refusing an
    /// auto-accepting endpoint shared with other protocols before any effect.
    pub(super) fn build_attached(self) -> Result<Irokle<S>> {
        let settings = self.iroh;
        let Some(endpoint) = settings.endpoint else {
            return Irokle::with_storage(self.storage, self.config);
        };
        if settings.auto_accept
            && settings
                .alpns
                .iter()
                .any(|alpn| alpn.as_slice() != crate::net::IROKLE_SYNC_ALPN)
        {
            return Err(Error::Storage(
                "iroh auto accept requires a dedicated endpoint; call without_auto_accept after with_net and route connections manually".into(),
            ));
        }
        let node = Irokle::with_storage(self.storage, self.config)?;
        let net = std::sync::Arc::new(
            crate::net::IrohNet::new_with_alpns_config_and_sink(
                endpoint,
                node.clone(),
                settings.alpns,
                settings.runtime,
                settings.eviction_sink,
            )
            .map_err(|err| Error::Storage(format!("failed to configure iroh: {err}")))?,
        );
        if settings.auto_accept {
            net.start_accept_loop().map_err(|err| {
                Error::Storage(format!("failed to start iroh accept loop: {err}"))
            })?;
        }
        net.start_configured_resync_loop()
            .map_err(|err| Error::Storage(format!("failed to start iroh resync loop: {err}")))?;
        Ok(node.with_net(net))
    }
}

impl<S: Storage> Irokle<S> {
    pub(crate) fn with_net(mut self, net: Arc<crate::net::IrohNet<S>>) -> Self {
        self.net = Some(net);
        self
    }

    pub fn endpoint(&self) -> Option<&iroh::Endpoint> {
        self.net.as_ref().map(|net| net.endpoint())
    }

    pub fn iroh_runtime_config(&self) -> Option<crate::net::IrohRuntimeConfig> {
        self.net.as_ref().map(|net| net.runtime_config())
    }

    pub async fn shutdown_iroh(&self) {
        if let Some(net) = &self.net {
            net.shutdown().await;
        }
    }

    pub fn start_accept_loop(&self) -> std::io::Result<()> {
        self.net
            .as_ref()
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotConnected, "iroh is not configured")
            })?
            .start_accept_loop()
    }

    pub async fn accept_one(&self) -> std::io::Result<Option<iroh::EndpointId>> {
        self.net
            .as_ref()
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotConnected, "iroh is not configured")
            })?
            .accept_one()
            .await
    }

    pub async fn sync_now(&self, peer_id: PeerId, topic_id: TopicId) -> std::io::Result<()> {
        self.net
            .as_ref()
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotConnected, "iroh is not configured")
            })?
            .sync_peer_now(peer_id, topic_id)
            .await
    }

    pub async fn sync_addr_now(
        &self,
        addr: iroh::EndpointAddr,
        topic_id: TopicId,
    ) -> std::io::Result<()> {
        self.net
            .as_ref()
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotConnected, "iroh is not configured")
            })?
            .sync_now(addr, topic_id)
            .await
    }

    pub async fn sync_endpoint_now(
        &self,
        endpoint_id: iroh::EndpointId,
        topic_id: TopicId,
    ) -> std::io::Result<()> {
        self.net
            .as_ref()
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotConnected, "iroh is not configured")
            })?
            .sync_endpoint_now(endpoint_id, topic_id)
            .await
    }

    pub async fn sync_topic_now(&self, topic_id: TopicId) -> std::io::Result<()> {
        let net = self.net.as_ref().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "iroh is not configured")
        })?;
        let state = self
            .storage()
            .topic_state(&topic_id)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()))?
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "topic not found"))?;
        let peers = self.sync_peers(topic_id, &state);
        let mut syncs = tokio::task::JoinSet::new();
        let mut first_error = None;
        for peer in peers {
            while syncs.len() >= SYNC_TOPIC_CONCURRENCY {
                if let Some(result) = syncs.join_next().await {
                    record_sync_join(result, &mut first_error);
                }
            }
            let net = Arc::clone(net);
            syncs.spawn(async move { (peer, net.sync_peer_now(peer, topic_id).await) });
        }
        while let Some(result) = syncs.join_next().await {
            record_sync_join(result, &mut first_error);
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    /// The sync engine, for transport planners that read one snapshot.
    pub(crate) fn sync_engine(&self) -> &SyncEngine<S> {
        &self.sync
    }

    /// The same node building and accepting requests of at most `items` wants and hints.
    #[cfg(test)]
    pub(crate) fn with_request_items(mut self, items: usize) -> Self {
        self.sync = self.sync.with_request_items(items);
        self
    }

    /// The same node ending a page slice after `visits` storage reads.
    #[cfg(test)]
    pub(crate) fn with_page_visits(mut self, visits: usize) -> Self {
        self.sync = self.sync.with_page_visits(visits, 16);
        self
    }

    /// What is known of `view`'s topic integrity within `read`, which `view` came from.
    pub(crate) fn integrity_in(
        &self,
        read: &dyn crate::storage::SnapshotRead,
        view: &crate::storage::TopicView,
    ) -> Result<crate::oplog::Integrity> {
        self.oplog.integrity_in(read, view)
    }

    /// Ids `view`'s topic cannot resolve, with any hole scan recorded under the
    /// view's own branch and epoch.
    pub(crate) fn view_unresolved(
        &self,
        view: &crate::storage::TopicView,
    ) -> Result<BTreeSet<OpId>> {
        self.oplog.view_unresolved(view)
    }

    /// The same node reading at most `reads` ids and edges per integrity scan step.
    #[cfg(test)]
    pub(crate) fn set_step_reads(&self, reads: usize) {
        self.oplog.set_step_reads(reads);
    }

    pub(crate) fn receive_bound(
        &self,
        peer: PeerId,
        data: SyncData,
        genesis: Option<OpId>,
    ) -> Result<ReceiveOutcome> {
        let Some(genesis) = genesis else {
            return self.receive_sync_outcome(peer, data);
        };
        let mut node = self.clone();
        node.oplog = node.oplog.bound_genesis(data.topic_id, genesis);
        node.sync = node.sync.bound_genesis(data.topic_id, genesis);
        node.receive_sync_outcome(peer, data)
    }

    pub(crate) fn record_fingerprint(
        &self,
        peer_id: PeerId,
        topic_id: TopicId,
        fingerprint: [u8; 32],
    ) -> Result<bool> {
        self.sync.record_fingerprint(peer_id, topic_id, fingerprint)
    }

    pub(crate) fn ensure_peer_allowed(
        &self,
        source_peer_id: PeerId,
        data: &SyncData,
    ) -> Result<()> {
        if self.storage().topic_state(&data.topic_id)?.is_some() {
            return Ok(());
        }
        let peer_allowed = {
            let peer_whitelist = self
                .peer_whitelist
                .read()
                .map_err(|_| Error::Storage("peer whitelist read lock poisoned".into()))?;
            match &*peer_whitelist {
                Some(peer_whitelist) => peer_whitelist.contains(&source_peer_id),
                None => true,
            }
        };
        if !peer_allowed {
            return Err(Error::PeerNotWhitelisted(source_peer_id));
        }
        Ok(())
    }

    /// Ask the transport to recheck `topic_id`; `message` reports a failed request.
    pub(super) fn recheck_topic(&self, topic_id: TopicId, message: &'static str) {
        if let Some(net) = &self.net
            && let Err(error) = net.schedule_topic_recheck(topic_id)
        {
            tracing::warn!(%topic_id, %error, "{}", message);
        }
    }

    /// Resync `peer_id` and recheck `topic_id` after a receive whose data committed.
    pub(super) fn resync_committed(&self, peer_id: PeerId, topic_id: TopicId) {
        if let Some(net) = &self.net {
            net.schedule_resync(peer_id, topic_id);
            if let Err(error) = net.schedule_topic_recheck(topic_id) {
                tracing::warn!(%topic_id, %error, "committed receive recheck failed");
            }
        }
    }

    pub(super) fn schedule_resync(&self, peer_id: PeerId, topic_id: TopicId) {
        if let Some(net) = &self.net {
            net.schedule_resync(peer_id, topic_id);
        }
    }

    pub(super) fn network_attached(&self) -> bool {
        self.net.is_some()
    }

    pub(super) fn wake_async_replication(
        &self,
        topic_id: TopicId,
        _op_id: OpId,
        write_concern: &WriteConcern,
        wake_failed_message: &'static str,
    ) {
        let Some(net) = &self.net else {
            return;
        };
        let result = (|| -> Result<()> {
            let state = self
                .storage()
                .topic_state(&topic_id)?
                .ok_or(Error::TopicNotFound)?;
            if matches!(write_concern, WriteConcern::AsyncReplication) {
                for peer_id in self.sync_peers(topic_id, &state) {
                    self.record_replication_scheduled(peer_id, topic_id)?;
                }
            }
            Ok(())
        })();
        if let Err(error) = result {
            tracing::warn!(%topic_id, %error, "committed replication bookkeeping failed");
        }
        if let Err(error) = net.schedule_topic_recheck(topic_id) {
            tracing::warn!(%topic_id, %error, "{}", wake_failed_message);
        }
    }

    /// Record one attempt's reachability, however many topics it served. Reaching the peer
    /// clears its failures; otherwise an unreachable result, as classed by the transport,
    /// adds one. Returns whether selection changed.
    pub(crate) fn note_peer_outcome(
        &self,
        peer_id: PeerId,
        reached: bool,
        unreachable: bool,
    ) -> bool {
        if reached {
            self.peer_health.record_success(&peer_id)
        } else if unreachable {
            self.peer_health.record_failure(peer_id)
        } else {
            false
        }
    }
}

fn record_sync_join(
    result: std::result::Result<(PeerId, std::io::Result<()>), tokio::task::JoinError>,
    first_error: &mut Option<std::io::Error>,
) {
    match result {
        Ok((_, Ok(()))) => {}
        Ok((_, Err(error))) => {
            if first_error.is_none() {
                *first_error = Some(error);
            }
        }
        Err(error) => {
            if first_error.is_none() {
                *first_error = Some(std::io::Error::other(error.to_string()));
            }
        }
    }
}
