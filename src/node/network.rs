// SPDX-License-Identifier: MIT OR Apache-2.0
//! The node's Iroh transport: the builder's settings and the node it attaches
//! to an endpoint.

use crate::node::{Irokle, IrokleBuilder, WriteConcern};
use crate::storage::Storage;
use crate::{Ed25519Signer, Error, Result, TopicEviction};

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
