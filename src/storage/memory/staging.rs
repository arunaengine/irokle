// SPDX-License-Identifier: MIT OR Apache-2.0
//! The provisional namespace registry of `MemoryStorage` and the checked lock
//! every store operation takes: a namespace view reads and writes only while
//! the registry still holds its session.

use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::{Error, PeerId, Result, TopicId};

use super::super::{ProvisionalTopic, StagingLimits, StagingQuota};
use super::{MemoryInner, MemoryStorage};

/// Registered provisional namespaces with their records. Lock order: this
/// registry, then the active records, then a namespace's records.
#[derive(Default)]
pub(super) struct Staging {
    pub(super) namespaces: BTreeMap<(PeerId, TopicId), Namespace>,
    pub(super) sessions: u64,
}

pub(super) type Namespace = (ProvisionalTopic, Arc<Mutex<MemoryInner>>);

/// A store's records, locked after a view's registry entry was checked. A
/// change through a view updates that entry's bytes and revision on release.
pub(super) struct Locked<'a> {
    inner: MutexGuard<'a, MemoryInner>,
    staging: Option<(MutexGuard<'a, Staging>, (PeerId, TopicId))>,
    changed: bool,
}

impl Deref for Locked<'_> {
    type Target = MemoryInner;

    fn deref(&self) -> &MemoryInner {
        &self.inner
    }
}

impl DerefMut for Locked<'_> {
    fn deref_mut(&mut self) -> &mut MemoryInner {
        self.changed = true;
        &mut self.inner
    }
}

impl Drop for Locked<'_> {
    fn drop(&mut self) {
        if let (true, Some((staging, key))) = (self.changed, &mut self.staging)
            && let Some((provisional, _)) = staging.namespaces.get_mut(key)
        {
            provisional.revision = provisional.revision.saturating_add(1);
            provisional.bytes = self.inner.admitted_bytes + self.inner.pending_usage.bytes;
        }
    }
}

impl Locked<'_> {
    /// What the other namespaces leave a view; `None` for the main store.
    pub(super) fn quota(&self, limits: &StagingLimits) -> Option<StagingQuota> {
        let (staging, key) = self.staging.as_ref()?;
        let (mut others, mut source) = (0_u64, 0_u64);
        for (other, (provisional, _)) in &staging.namespaces {
            if other != key {
                others = others.saturating_add(provisional.bytes);
                if other.0 == key.0 {
                    source = source.saturating_add(provisional.bytes);
                }
            }
        }
        Some(StagingQuota::new(limits, others, source))
    }
}

impl MemoryStorage {
    /// The records of this store. A view first checks, under the registry
    /// lock it keeps, that its session is still registered.
    pub(super) fn lock(&self) -> Result<Locked<'_>> {
        let staging = match self.namespace {
            Some((source, topic_id, session)) => {
                let staging = self.staging()?;
                let current = staging
                    .namespaces
                    .get(&(source, topic_id))
                    .is_some_and(|(provisional, _)| provisional.session == session);
                if !current {
                    return Err(Error::StaleIncarnation);
                }
                Some((staging, (source, topic_id)))
            }
            None => None,
        };
        Ok(Locked {
            inner: self.inner.lock()?,
            staging,
            changed: false,
        })
    }

    pub(super) fn staging(&self) -> Result<MutexGuard<'_, Staging>> {
        self.staging
            .lock()
            .map_err(|_| Error::Storage("staging lock poisoned".into()))
    }

    /// Refuse a registry operation on a namespace view.
    pub(super) fn main_store(&self) -> Result<()> {
        match self.namespace {
            Some(_) => Err(Error::StaleIncarnation),
            None => Ok(()),
        }
    }
}
