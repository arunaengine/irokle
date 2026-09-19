// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::storage::{MemoryStorage, Storage};
use crate::{Ed25519Signer, PeerId, Result};

use crate::node::{Irokle, IrokleBuilder, NodeConfig, WriteConcern};

impl Irokle<MemoryStorage> {
    pub fn builder() -> IrokleBuilder<MemoryStorage> {
        IrokleBuilder {
            storage: MemoryStorage::new(),
            config: NodeConfig::default(),
            signer_explicit: false,
            write_concern_explicit: false,
            #[cfg(feature = "iroh")]
            iroh: Default::default(),
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
            iroh: self.iroh,
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

    #[cfg(feature = "fjall")]
    pub fn with_fjall_path(
        self,
        path: impl AsRef<std::path::Path>,
    ) -> Result<IrokleBuilder<crate::FjallStorage>> {
        self.with_fjall_path_and_persist_mode(path, fjall::PersistMode::SyncAll)
    }

    #[cfg(feature = "fjall")]
    /// Use Fjall storage with an explicit transaction persist mode.
    ///
    /// `with_fjall_path` uses `SyncAll`; `Buffer` lets the caller set the durability boundary.
    pub fn with_fjall_path_and_persist_mode(
        self,
        path: impl AsRef<std::path::Path>,
        persist_mode: fjall::PersistMode,
    ) -> Result<IrokleBuilder<crate::FjallStorage>> {
        let storage = crate::FjallStorage::open_with_persist_mode(path, persist_mode)?;
        Ok(self.with_storage(storage))
    }

    #[cfg(feature = "fjall")]
    pub fn with_fjall_database(
        self,
        db: fjall::OptimisticTxDatabase,
    ) -> Result<IrokleBuilder<crate::FjallStorage>> {
        self.with_fjall_database_and_persist_mode(db, fjall::PersistMode::SyncAll)
    }

    #[cfg(feature = "fjall")]
    /// Use an existing Fjall database with an explicit transaction persist mode.
    pub fn with_fjall_database_and_persist_mode(
        self,
        db: fjall::OptimisticTxDatabase,
        persist_mode: fjall::PersistMode,
    ) -> Result<IrokleBuilder<crate::FjallStorage>> {
        let storage = crate::FjallStorage::from_database_with_persist_mode(db, persist_mode)?;
        Ok(self.with_storage(storage))
    }

    pub fn build(self) -> Result<Irokle<S>> {
        #[cfg(feature = "iroh")]
        if self.iroh.endpoint.is_some() {
            return self.build_attached();
        }
        Irokle::with_storage(self.storage, self.config)
    }
}
