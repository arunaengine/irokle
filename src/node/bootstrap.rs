// SPDX-License-Identifier: MIT OR Apache-2.0
//! Bootstrap of topics this node does not hold: each source stages its history
//! in a provisional namespace, admitted by the normal oplog, and the namespace
//! becomes the topic once that history makes this node and the source members.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use crate::oplog::is_structural_genesis;
use crate::storage::{
    AdmissionEffects, MAX_STAGED_IDLE_MS, ProvisionalTopic, StagedTopic, SyncObligation, TopicState,
};
use crate::sync::SyncData;
use crate::{ActorClock, Error, OpId, PeerId, Result, Storage, TopicId};

use super::{Bootstrap, Irokle, now_millis};

/// Owner locks per topic and byte reservations of admissions in flight. The
/// registry lock is never held while storage work runs.
#[derive(Default)]
pub(crate) struct Bootstraps {
    owners: Mutex<BTreeMap<TopicId, Weak<Mutex<()>>>>,
    reserved: Mutex<Reserved>,
}

#[derive(Default)]
struct Reserved {
    total: u64,
    by_source: BTreeMap<PeerId, u64>,
}

/// Bytes an admission in flight holds against the staging limits until dropped.
struct Reservation<'a> {
    bootstraps: &'a Bootstraps,
    source: PeerId,
    bytes: u64,
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        let mut reserved = self.bootstraps.reserved();
        reserved.total = reserved.total.saturating_sub(self.bytes);
        if let Some(bytes) = reserved.by_source.get_mut(&self.source) {
            *bytes = bytes.saturating_sub(self.bytes);
            if *bytes == 0 {
                reserved.by_source.remove(&self.source);
            }
        }
    }
}

impl Bootstraps {
    /// The owner lock of `topic_id`, shared by every bootstrap step of it.
    fn owner(&self, topic_id: TopicId) -> Arc<Mutex<()>> {
        let mut owners = self
            .owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        owners.retain(|_, owner| owner.strong_count() > 0);
        if let Some(owner) = owners.get(&topic_id).and_then(Weak::upgrade) {
            return owner;
        }
        let owner = Arc::new(Mutex::new(()));
        owners.insert(topic_id, Arc::downgrade(&owner));
        owner
    }

    fn reserved(&self) -> MutexGuard<'_, Reserved> {
        // Counters only, so a poisoned lock is still consistent.
        self.reserved
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Reserve `bytes` for `source` against the total and per-source limits,
    /// counting what the namespaces hold and what other admissions reserved.
    fn reserve<S: Storage>(
        &self,
        storage: &S,
        source: PeerId,
        bytes: u64,
    ) -> Result<Reservation<'_>> {
        let limits = storage.staging_limits();
        let (mut total, mut from_source) = (0, 0);
        for provisional in storage.provisional_topics()? {
            let held = match storage.provisional_store(&provisional)? {
                Some(store) => store.stored_bytes()?,
                None => 0,
            };
            total += held;
            if provisional.source == source {
                from_source += held;
            }
        }
        let mut reserved = self.reserved();
        let source_reserved = reserved.by_source.get(&source).copied().unwrap_or_default();
        if total + reserved.total + bytes > limits.total_bytes {
            return Err(Error::StagingCapacity(
                "bootstrap staging byte budget is full".into(),
            ));
        }
        if from_source + source_reserved + bytes > limits.source_bytes {
            return Err(Error::StagingCapacity(
                "bootstrap staging byte quota exceeded for source".into(),
            ));
        }
        reserved.total += bytes;
        *reserved.by_source.entry(source).or_default() += bytes;
        Ok(Reservation {
            bootstraps: self,
            source,
            bytes,
        })
    }
}

impl<S: Storage> Irokle<S> {
    /// Stage data for a topic this node does not hold in the namespace of its
    /// source, and activate the namespace once its history makes this node and
    /// the source members. A fragment of a smaller genesis replaces the
    /// namespace; one of a larger genesis is refused as stale.
    pub(super) fn bootstrap_unknown(
        &self,
        source: PeerId,
        data: &SyncData,
        verified: &BTreeSet<OpId>,
    ) -> Result<Bootstrap> {
        let storage = self.storage();
        let topic_id = data.topic_id;
        if storage.topic_state(&topic_id)?.is_some() {
            return Ok(Bootstrap::Active(BTreeSet::new()));
        }
        let owner = self.bootstraps.owner(topic_id);
        let _owned = owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if storage.topic_state(&topic_id)?.is_some() {
            return Ok(Bootstrap::Active(BTreeSet::new()));
        }
        let now_ms = now_millis()?;
        self.expire_bootstraps(now_ms)?;
        let fragment = data
            .ops
            .iter()
            .find(|op| is_structural_genesis(op))
            .map(|op| op.id);
        let current = self.provisional_of(source, topic_id)?;
        let provisional = match (current, fragment) {
            (Some(current), _) if current.activating => {
                return self.activate_proven(&current, current.source);
            }
            (Some(current), Some(genesis)) if genesis < current.genesis => {
                storage.discard_provisional(&current)?;
                storage.open_provisional(source, topic_id, genesis, now_ms)?
            }
            (Some(current), Some(genesis)) if genesis > current.genesis => {
                return Err(Error::StaleIncarnation);
            }
            (Some(current), _) => current,
            (None, Some(genesis)) => storage.open_provisional(source, topic_id, genesis, now_ms)?,
            // Nothing anchors the fragment to a branch yet; a later pull asks again.
            (None, None) => return Ok(Bootstrap::Staged(StagedTopic::default())),
        };
        let store = storage
            .provisional_store(&provisional)?
            .ok_or(Error::StaleIncarnation)?;
        let mut bytes = 0;
        for op in &data.ops {
            bytes += crate::storage::pending_op_bytes(op)? as u64;
        }
        let reservation = self.bootstraps.reserve(storage, source, bytes)?;
        let admitted = self
            .oplog
            .sharing_membership(store)
            .receive_ops_from_peer_preverified(Some(source), data.ops.clone(), verified, None);
        drop(reservation);
        storage.touch_provisional(&provisional, now_ms)?;
        // The ack of an activating fragment names its ops the topic now holds.
        let outcome = match self.activate_proven(&provisional, source) {
            Ok(Bootstrap::Active(_)) => {
                let mut held = BTreeSet::new();
                for id in verified {
                    if storage.dep_resolvable(id)? {
                        held.insert(*id);
                    }
                }
                Ok(Bootstrap::Active(held))
            }
            other => other,
        };
        match admitted {
            Ok(_) => outcome,
            // What committed still counts; the refused rest fails this data.
            Err(Error::AdmissionCommitted { source, .. }) => {
                outcome?;
                Err(*source)
            }
            Err(error) => {
                outcome?;
                Err(error)
            }
        }
    }

    /// Activate every namespace whose activation began or whose history
    /// already proves membership, as a restart after the last write requires.
    pub(super) fn resume_bootstraps(&self) -> Result<()> {
        for provisional in self.storage().provisional_topics()? {
            let owner = self.bootstraps.owner(provisional.topic_id);
            let _owned = owner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Err(error) = self.activate_proven(&provisional, provisional.source) {
                tracing::warn!(
                    topic_id = %provisional.topic_id,
                    %error,
                    "leaving a provisional bootstrap for a later attempt"
                );
            }
        }
        Ok(())
    }

    /// Finish the bootstrap of `topic_id` from `source` when its staged history
    /// already proves membership. Returns whether the topic is active.
    pub fn finish_bootstrap(&self, source: PeerId, topic_id: TopicId) -> Result<bool> {
        let owner = self.bootstraps.owner(topic_id);
        let _owned = owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.storage().topic_state(&topic_id)?.is_some() {
            return Ok(true);
        }
        let Some(provisional) = self.provisional_of(source, topic_id)? else {
            return Ok(false);
        };
        Ok(matches!(
            self.activate_proven(&provisional, source)?,
            Bootstrap::Active(_)
        ))
    }

    /// What `source` staged here for `topic_id`: its branch, session, contiguous
    /// clock and bytes. `None` when nothing is staged.
    pub fn staged_topic(&self, source: PeerId, topic_id: TopicId) -> Result<Option<StagedTopic>> {
        let Some(provisional) = self.provisional_of(source, topic_id)? else {
            return Ok(None);
        };
        let Some(store) = self.storage().provisional_store(&provisional)? else {
            return Ok(None);
        };
        Ok(Some(staged_of(&provisional, &store)?))
    }

    fn provisional_of(
        &self,
        source: PeerId,
        topic_id: TopicId,
    ) -> Result<Option<ProvisionalTopic>> {
        Ok(self
            .storage()
            .provisional_topics()?
            .into_iter()
            .find(|provisional| provisional.source == source && provisional.topic_id == topic_id))
    }

    /// Activate `provisional` when its state holds this node and `source`, or
    /// when its activation already began; otherwise report what is staged.
    fn activate_proven(&self, provisional: &ProvisionalTopic, source: PeerId) -> Result<Bootstrap> {
        let storage = self.storage();
        let Some(store) = storage.provisional_store(provisional)? else {
            return Err(Error::StaleIncarnation);
        };
        let Some(view) = store.topic_view(&provisional.topic_id, None)? else {
            return Ok(Bootstrap::Staged(staged_of(provisional, &store)?));
        };
        let members = &view.state.members;
        let proven = members.contains(&self.peer_id()) && members.contains(&source);
        if !proven && !provisional.activating {
            return Ok(Bootstrap::Staged(staged_of(provisional, &store)?));
        }
        let effects = self.activation_effects(source, &view.state, &view.clock);
        match storage.activate_provisional(provisional, &view.state, effects) {
            Ok(()) => Ok(Bootstrap::Active(BTreeSet::new())),
            Err(Error::AdmissionConflict)
                if storage.topic_state(&provisional.topic_id)?.is_some() =>
            {
                Ok(Bootstrap::Active(BTreeSet::new()))
            }
            Err(error) => Err(error),
        }
    }

    /// Forwarding work an activation commits: the whole staged clock for every
    /// selected peer other than the source and this node.
    fn activation_effects(
        &self,
        source: PeerId,
        state: &TopicState,
        clock: &ActorClock,
    ) -> AdmissionEffects {
        AdmissionEffects {
            sync_obligations: self
                .sync_peers(state.topic_id, state)
                .into_iter()
                .filter(|peer_id| *peer_id != source && *peer_id != self.peer_id())
                .map(|peer_id| SyncObligation::clock(peer_id, state.topic_id, clock.clone()))
                .collect(),
        }
    }

    /// End namespaces with no write for `MAX_STAGED_IDLE_MS`, unless activating.
    fn expire_bootstraps(&self, now_ms: u64) -> Result<()> {
        let storage = self.storage();
        for provisional in storage.provisional_topics()? {
            if !provisional.activating
                && provisional.updated_ms < now_ms.saturating_sub(MAX_STAGED_IDLE_MS)
            {
                storage.discard_provisional(&provisional)?;
            }
        }
        Ok(())
    }
}

/// The receipt of a namespace: branch, session, contiguous clock and bytes.
fn staged_of<S: Storage>(provisional: &ProvisionalTopic, store: &S) -> Result<StagedTopic> {
    let clock = store.actor_clock(&provisional.topic_id)?;
    Ok(StagedTopic {
        genesis: Some(provisional.genesis),
        session: provisional.session,
        clock,
        bytes: store.stored_bytes()?,
    })
}
