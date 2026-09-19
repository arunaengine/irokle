// SPDX-License-Identifier: MIT OR Apache-2.0
//! Provisional namespace registry and checked locks for `MemoryStorage`.
//! Namespace views validate their session before reads and writes.

use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::{ActorId, Error, OpId, PeerId, Result, TopicId};

use crate::storage::memory::{
    MemoryInner, MemoryStorage, MetadataPlan, ObligationKind, put_obligation_locked,
    topic_state_locked,
};
use crate::storage::{
    AdmissionEffects, ProvisionalTopic, StagingLimits, StagingQuota, TopicState, ack_covers,
    check_namespaces, merged_obligation,
};

/// Registered provisional namespaces with their records. Lock order: this
/// registry, then the active records, then a namespace's records.
#[derive(Default)]
pub(super) struct Staging {
    namespaces: BTreeMap<(PeerId, TopicId), Namespace>,
    sessions: u64,
}

type Namespace = (ProvisionalTopic, Arc<Mutex<MemoryInner>>);

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

impl MemoryStorage {
    pub(super) fn read_namespaces(&self) -> Result<Vec<ProvisionalTopic>> {
        Ok(self
            .staging()?
            .namespaces
            .values()
            .map(|(provisional, _)| provisional.clone())
            .collect())
    }

    pub(super) fn open_namespace(
        &self,
        source: PeerId,
        topic_id: TopicId,
        genesis: OpId,
        now_ms: u64,
    ) -> Result<ProvisionalTopic> {
        self.main_store()?;
        let mut staging = self.staging()?;
        if self.lock()?.topics.contains_key(&topic_id) {
            return Err(Error::AdmissionConflict);
        }
        if let Some((provisional, _)) = staging.namespaces.get(&(source, topic_id)) {
            return Ok(provisional.clone());
        }
        let from_source = staging
            .namespaces
            .keys()
            .filter(|(peer, _)| *peer == source)
            .count();
        check_namespaces(&self.limits, staging.namespaces.len(), from_source)?;
        let budget = Arc::clone(&self.lock()?.budget);
        let namespace_charge = Some(Arc::new(
            budget.reserve(crate::storage::memory::MemoryDomain::Metadata, 4096)?,
        ));
        staging.sessions += 1;
        let provisional = ProvisionalTopic {
            source,
            topic_id,
            genesis,
            session: staging.sessions,
            updated_ms: now_ms,
            activating: false,
            revision: 0,
            bytes: 0,
        };
        staging.namespaces.insert(
            (source, topic_id),
            (
                provisional.clone(),
                Arc::new(Mutex::new(MemoryInner {
                    budget,
                    namespace_charge,
                    ..Default::default()
                })),
            ),
        );
        Ok(provisional)
    }

    pub(super) fn namespace_store(&self, provisional: &ProvisionalTopic) -> Result<Option<Self>> {
        self.main_store()?;
        Ok(self
            .staging()?
            .namespaces
            .get(&(provisional.source, provisional.topic_id))
            .filter(|(current, _)| current.session == provisional.session)
            .map(|(_, records)| MemoryStorage {
                inner: Arc::clone(records),
                counters: Arc::clone(&self.counters),
                limits: self.limits,
                staging: Arc::clone(&self.staging),
                namespace: Some((
                    provisional.source,
                    provisional.topic_id,
                    provisional.session,
                )),
            }))
    }

    pub(super) fn touch_namespace(
        &self,
        provisional: &ProvisionalTopic,
        now_ms: u64,
    ) -> Result<()> {
        self.main_store()?;
        if let Some((current, _)) = self
            .staging()?
            .namespaces
            .get_mut(&(provisional.source, provisional.topic_id))
            .filter(|(current, _)| current.session == provisional.session)
        {
            current.updated_ms = current.updated_ms.max(now_ms);
        }
        Ok(())
    }

    pub(super) fn activate_namespace(
        &self,
        provisional: &ProvisionalTopic,
        expected: &TopicState,
        effects: AdmissionEffects,
    ) -> Result<()> {
        self.main_store()?;
        let topic_id = provisional.topic_id;
        if effects
            .sync_obligations
            .iter()
            .any(|obligation| obligation.topic_id != topic_id)
        {
            return Err(Error::TopicMismatch);
        }
        // Registry, active and staged records stay locked together, so no view
        // writes and no reader sees part of the history.
        let mut staging = self.staging()?;
        let mut inner = self.lock()?;
        if inner.topics.contains_key(&topic_id) {
            return Err(Error::AdmissionConflict);
        }
        if staging
            .namespaces
            .get(&(provisional.source, topic_id))
            .is_none_or(|(current, _)| current.session != provisional.session)
        {
            return Err(Error::StaleIncarnation);
        }
        let records = staging
            .namespaces
            .iter()
            .filter(|((_, topic), _)| *topic == topic_id)
            .map(|(key, (_, records))| (*key, Arc::clone(records)))
            .collect::<Vec<_>>();
        let mut locked = records
            .iter()
            .map(|(_, records)| records.lock())
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let position = records
            .iter()
            .position(|(key, _)| *key == (provisional.source, topic_id))
            .ok_or(Error::StaleIncarnation)?;
        let staged = &locked[position];
        if topic_state_locked(staged, &topic_id).as_ref() != Some(expected) {
            return Err(Error::AdmissionConflict);
        }
        let copy_bytes = staged
            .charges
            .values()
            .map(|charge| charge.operation.bytes + charge.metadata.bytes)
            .sum::<u64>()
            + staged
                .metadata
                .values()
                .map(|charge| charge.bytes)
                .sum::<u64>()
            + staged
                .namespace_charge
                .as_ref()
                .map_or(0, |charge| charge.bytes);
        let _copy = inner
            .budget
            .reserve(crate::storage::memory::MemoryDomain::Activation, copy_bytes)?;
        let _effects = crate::storage::memory::metadata::merge_workspace(
            &inner,
            effects.sync_obligations.iter(),
        )?;
        let mut changes = BTreeMap::new();
        for obligation in effects.sync_obligations {
            let ack = inner.peer_acks.get(&(obligation.peer_id, topic_id));
            if obligation.is_empty() || ack_covers(ack, Some(expected.genesis), &obligation) {
                continue;
            }
            let kind = ObligationKind::of(&obligation);
            let key = (obligation.peer_id, kind);
            let existing = changes.get(&key).cloned().or_else(|| {
                inner
                    .obligations
                    .get(&(topic_id, obligation.peer_id))
                    .and_then(|records| records.get(&kind))
                    .cloned()
            });
            let merged = merged_obligation(existing, &obligation)?;
            changes.insert(key, merged);
        }
        let mut reservation = MetadataPlan::new(&inner)?;
        for obligation in changes.values() {
            reservation.obligation(&inner, obligation)?;
        }
        reservation.commit(&mut inner);
        copy_topic_locked(staged, &mut inner, &topic_id);
        inner.topics.insert(topic_id, expected.clone());
        for merged in changes.into_values() {
            put_obligation_locked(&mut inner, merged);
        }
        for records in &mut locked {
            **records = MemoryInner {
                budget: Arc::clone(&records.budget),
                ..Default::default()
            };
        }
        drop(locked);
        for (key, _) in records {
            staging.namespaces.remove(&key);
        }
        Ok(())
    }

    pub(super) fn discard_namespace(&self, provisional: &ProvisionalTopic) -> Result<bool> {
        self.main_store()?;
        let mut staging = self.staging()?;
        let key = (provisional.source, provisional.topic_id);
        let current = staging
            .namespaces
            .get(&key)
            .is_some_and(|(current, _)| current == provisional && !current.activating);
        if current {
            if let Some((_, records)) = staging.namespaces.get(&key) {
                let mut records = records.lock()?;
                *records = MemoryInner {
                    budget: Arc::clone(&records.budget),
                    ..Default::default()
                };
            }
            staging.namespaces.remove(&key);
        }
        Ok(current)
    }
}

/// Copy every record of `topic_id` from a namespace store into `inner`.
fn copy_topic_locked(staged: &MemoryInner, inner: &mut MemoryInner, topic_id: &TopicId) {
    let key = crate::storage::memory::MetadataKey::Topic(*topic_id);
    if let Some(charge) = staged.metadata.get(&key) {
        inner.metadata.insert(key, Arc::clone(charge));
    }
    let ids = staged.topic_ops.get(topic_id).cloned().unwrap_or_default();
    for id in &ids {
        if let (Some(op), Some(meta)) = (staged.ops.get(id), staged.meta.get(id)) {
            if let Some(charge) = staged.charges.get(id) {
                inner.charges.insert(*id, charge.clone());
            }
            for dep in &meta.deps {
                inner.children.entry(*dep).or_default().insert(*id);
            }
            inner.ops.insert(*id, op.clone());
            inner.meta.insert(*id, meta.clone());
        }
    }
    inner.topic_ops.insert(*topic_id, ids);
    let first = ActorId::from_bytes([0; 32]);
    let last = ActorId::from_bytes([0xff; 32]);
    for (key, id) in staged
        .actor_by_seq
        .range((*topic_id, first, 0)..=(*topic_id, last, u64::MAX))
    {
        inner.actor_by_seq.insert(*key, *id);
    }
    for (key, tip) in staged
        .actor_tip
        .range((*topic_id, first)..=(*topic_id, last))
    {
        inner.actor_tip.insert(*key, *tip);
    }
    if let Some(heads) = staged.heads.get(topic_id) {
        inner.heads.insert(*topic_id, heads.clone());
    }
    if let Some(clock) = staged.actor_clock.get(topic_id) {
        inner.actor_clock.insert(*topic_id, clock.clone());
    }
    if let Some(fingerprint) = staged.topic_fingerprint.get(topic_id) {
        inner.topic_fingerprint.insert(*topic_id, *fingerprint);
    }
    if let Some(generation) = staged.max_generation.get(topic_id) {
        inner.max_generation.insert(*topic_id, *generation);
    }
}
