// SPDX-License-Identifier: MIT OR Apache-2.0
//! Pooled connections of a net, one per peer, and one dial at a time per peer.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;

use super::{IROKLE_SYNC_ALPN, other, timed_out};

#[derive(Clone)]
pub(super) struct ConnectionPool {
    endpoint: iroh::Endpoint,
    connections: Arc<RwLock<HashMap<iroh::EndpointId, iroh::endpoint::Connection>>>,
    dialing: Arc<Mutex<HashMap<iroh::EndpointId, Weak<tokio::sync::Mutex<()>>>>>,
}

impl ConnectionPool {
    pub(super) fn new(endpoint: iroh::Endpoint) -> Self {
        Self {
            endpoint,
            connections: Arc::new(RwLock::new(HashMap::new())),
            dialing: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(super) fn endpoint(&self) -> &iroh::Endpoint {
        &self.endpoint
    }

    pub(super) fn insert(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> io::Result<iroh::EndpointId> {
        let peer = connection.remote_id();
        self.connections
            .write()
            .map_err(|_| io::Error::other("connection pool write lock poisoned"))?
            .insert(peer, connection);
        Ok(peer)
    }

    pub(super) fn remove(&self, connection: &iroh::endpoint::Connection) -> io::Result<()> {
        let mut connections = self
            .connections
            .write()
            .map_err(|_| io::Error::other("connection pool write lock poisoned"))?;
        let peer = connection.remote_id();
        if connections
            .get(&peer)
            .is_some_and(|pooled| pooled.stable_id() == connection.stable_id())
        {
            connections.remove(&peer);
        }
        Ok(())
    }

    pub(super) fn get(
        &self,
        peer: &iroh::EndpointId,
    ) -> io::Result<Option<iroh::endpoint::Connection>> {
        let mut connections = self
            .connections
            .write()
            .map_err(|_| io::Error::other("connection pool write lock poisoned"))?;
        match connections.get(peer) {
            Some(connection) if connection.close_reason().is_none() => Ok(Some(connection.clone())),
            Some(_) => {
                connections.remove(peer);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    pub(super) async fn get_or_connect(
        &self,
        peer: iroh::EndpointAddr,
        connect_timeout: Duration,
    ) -> io::Result<iroh::endpoint::Connection> {
        if let Some(connection) = self.get(&peer.id)? {
            return Ok(connection);
        }
        let dialing = {
            let mut pending = self
                .dialing
                .lock()
                .map_err(|_| io::Error::other("connection dial lock poisoned"))?;
            pending.retain(|_, lock| lock.strong_count() > 0);
            match pending.get(&peer.id).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(tokio::sync::Mutex::new(()));
                    pending.insert(peer.id, Arc::downgrade(&lock));
                    lock
                }
            }
        };
        let _dialing = tokio::time::timeout(connect_timeout, dialing.lock())
            .await
            .map_err(|_| timed_out("connection pool wait timed out"))?;
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
