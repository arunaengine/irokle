// SPDX-License-Identifier: MIT OR Apache-2.0

#[cfg(feature = "iroh")]
use crate::Error;
use crate::storage::{MemoryStorage, Storage};
use crate::{Ed25519Signer, PeerId, Result};

use super::{Irokle, IrokleBuilder, NodeConfig, WriteConcern};

impl Irokle<MemoryStorage> {
    pub fn builder() -> IrokleBuilder<MemoryStorage> {
        IrokleBuilder {
            storage: MemoryStorage::new(),
            config: NodeConfig::default(),
            signer_explicit: false,
            write_concern_explicit: false,
            #[cfg(feature = "iroh")]
            endpoint: None,
            #[cfg(feature = "iroh")]
            alpns: Vec::new(),
            #[cfg(feature = "iroh")]
            auto_accept: true,
            #[cfg(feature = "iroh")]
            iroh_runtime: crate::net::IrohRuntimeConfig::default(),
        }
    }

    pub fn new(config: NodeConfig) -> Result<Self> {
        Self::with_storage(MemoryStorage::new(), config)
    }

    pub fn in_memory() -> Result<Self> {
        Self::new(NodeConfig::default())
    }
}

impl<S: Storage> IrokleBuilder<S> {
    pub fn with_storage<T: Storage>(self, storage: T) -> IrokleBuilder<T> {
        IrokleBuilder {
            storage,
            config: self.config,
            signer_explicit: self.signer_explicit,
            write_concern_explicit: self.write_concern_explicit,
            #[cfg(feature = "iroh")]
            endpoint: self.endpoint,
            #[cfg(feature = "iroh")]
            alpns: self.alpns,
            #[cfg(feature = "iroh")]
            auto_accept: self.auto_accept,
            #[cfg(feature = "iroh")]
            iroh_runtime: self.iroh_runtime,
        }
    }

    pub fn with_config(mut self, config: NodeConfig) -> Self {
        self.config = config;
        self.signer_explicit = true;
        self.write_concern_explicit = true;
        self
    }

    pub fn with_signer(mut self, signer: Ed25519Signer) -> Self {
        self.config.signer = signer;
        self.signer_explicit = true;
        self
    }

    pub fn with_write_concern(mut self, write_concern: WriteConcern) -> Self {
        self.config.default_write_concern = write_concern;
        self.write_concern_explicit = true;
        self
    }

    pub fn with_peer_whitelist<I>(mut self, peer_ids: I) -> Self
    where
        I: IntoIterator<Item = PeerId>,
    {
        self.config.peer_whitelist = Some(peer_ids.into_iter().collect());
        self
    }

    pub fn without_peer_whitelist(mut self) -> Self {
        self.config.peer_whitelist = None;
        self
    }

    #[cfg(feature = "iroh")]
    pub fn with_iroh_runtime_config(mut self, runtime: crate::net::IrohRuntimeConfig) -> Self {
        self.iroh_runtime = runtime;
        self
    }

    #[cfg(feature = "iroh")]
    pub fn with_iroh_secret_key(mut self, secret_key: &iroh::SecretKey) -> Self {
        self.config.signer = Ed25519Signer::from_iroh_secret_key(secret_key);
        self.signer_explicit = true;
        self
    }

    #[cfg(feature = "iroh")]
    pub fn with_net(mut self, endpoint: iroh::Endpoint) -> Self {
        if !self.signer_explicit {
            self.config.signer = Ed25519Signer::from_iroh_secret_key(endpoint.secret_key());
        }
        if !self.write_concern_explicit {
            self.config.default_write_concern = WriteConcern::AsyncReplication;
        }
        self.endpoint = Some(endpoint);
        self.auto_accept = true;
        self
    }

    #[cfg(feature = "iroh")]
    pub fn with_alpn(mut self, alpn: impl AsRef<[u8]>) -> Self {
        let alpn = alpn.as_ref().to_vec();
        if !self.alpns.contains(&alpn) {
            self.alpns.push(alpn);
        }
        self
    }

    #[cfg(feature = "iroh")]
    pub fn with_alpns<I, A>(mut self, alpns: I) -> Self
    where
        I: IntoIterator<Item = A>,
        A: AsRef<[u8]>,
    {
        for alpn in alpns {
            let alpn = alpn.as_ref().to_vec();
            if !self.alpns.contains(&alpn) {
                self.alpns.push(alpn);
            }
        }
        self
    }

    #[cfg(feature = "iroh")]
    pub fn without_auto_accept(mut self) -> Self {
        self.auto_accept = false;
        self
    }

    #[cfg(feature = "fjall")]
    pub fn with_fjall_path(
        self,
        path: impl AsRef<std::path::Path>,
    ) -> Result<IrokleBuilder<crate::FjallStorage>> {
        Ok(IrokleBuilder {
            storage: crate::FjallStorage::open(path)?,
            config: self.config,
            signer_explicit: self.signer_explicit,
            write_concern_explicit: self.write_concern_explicit,
            #[cfg(feature = "iroh")]
            endpoint: self.endpoint,
            #[cfg(feature = "iroh")]
            alpns: self.alpns,
            #[cfg(feature = "iroh")]
            auto_accept: self.auto_accept,
            #[cfg(feature = "iroh")]
            iroh_runtime: self.iroh_runtime,
        })
    }

    #[cfg(feature = "fjall")]
    pub fn with_fjall_database(
        self,
        db: fjall::OptimisticTxDatabase,
    ) -> Result<IrokleBuilder<crate::FjallStorage>> {
        Ok(IrokleBuilder {
            storage: crate::FjallStorage::from_database(db)?,
            config: self.config,
            signer_explicit: self.signer_explicit,
            write_concern_explicit: self.write_concern_explicit,
            #[cfg(feature = "iroh")]
            endpoint: self.endpoint,
            #[cfg(feature = "iroh")]
            alpns: self.alpns,
            #[cfg(feature = "iroh")]
            auto_accept: self.auto_accept,
            #[cfg(feature = "iroh")]
            iroh_runtime: self.iroh_runtime,
        })
    }

    pub fn build(self) -> Result<Irokle<S>> {
        #[cfg(feature = "iroh")]
        if let Some(endpoint) = self.endpoint {
            let node = Irokle::with_storage(self.storage, self.config)?;
            let net = std::sync::Arc::new(
                crate::net::IrohNet::new_with_alpns_and_config(
                    endpoint,
                    node.clone(),
                    self.alpns,
                    self.iroh_runtime,
                )
                .map_err(|err| Error::Storage(format!("failed to configure iroh: {err}")))?,
            );
            if self.auto_accept {
                net.start_accept_loop().map_err(|err| {
                    Error::Storage(format!("failed to start iroh accept loop: {err}"))
                })?;
            }
            net.start_configured_resync_loop().map_err(|err| {
                Error::Storage(format!("failed to start iroh resync loop: {err}"))
            })?;
            return Ok(node.with_net(net));
        }

        let node = Irokle::with_storage(self.storage, self.config)?;
        Ok(node)
    }
}
