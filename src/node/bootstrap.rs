// SPDX-License-Identifier: MIT OR Apache-2.0
//! Bootstrap unknown topics through provisional storage and activate them once
//! their staged history proves membership.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};

use crate::oplog::is_structural_genesis;
use crate::storage::{
    AdmissionEffects, MAX_STAGED_IDLE_MS as STAGED_IDLE_MS, ProvisionalTopic, StagedTopic,
    SyncObligation, TopicState,
};
use crate::sync::SyncData;
use crate::{ActorClock, Error, OpId, PeerId, Result, Storage, TopicId, TopicPayload};

use super::{Bootstrap, Irokle, now_millis};

/// Owner locks per topic. They only spare redundant work between callers of
/// one node: the store checks every namespace write and activation itself.
/// The registry lock is never held while storage work runs.
#[derive(Default)]
pub(crate) struct Bootstraps {
    owners: Mutex<BTreeMap<TopicId, Weak<Mutex<()>>>>,
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
}

/// Refuse unless `incoming` fits after removing weaker same-topic stages; refusal evicts nothing.
/// Remove those stages weakest first. The storage commit rechecks concurrent quota changes.
fn make_room<S: Storage>(storage: &S, current: &ProvisionalTopic, incoming: u64) -> Result<()> {
    let limits = storage.staging_limits();
    let held = storage.provisional_topics()?;
    let sum = |bytes: &mut dyn Iterator<Item = u64>| bytes.fold(0_u64, u64::saturating_add);
    let from_source = sum(&mut held
        .iter()
        .filter(|provisional| provisional.source == current.source)
        .map(|provisional| provisional.bytes));
    if from_source.saturating_add(incoming) > limits.source_bytes {
        return Err(Error::StagingCapacity(
            "bootstrap staging byte quota exceeded for source".into(),
        ));
    }
    let rank = |provisional: &ProvisionalTopic| (provisional.bytes, provisional.source);
    let own = held
        .iter()
        .find(|provisional| {
            provisional.topic_id == current.topic_id && provisional.source == current.source
        })
        .map_or((0, current.source), rank);
    let mut losers = held
        .iter()
        .filter(|provisional| {
            provisional.topic_id == current.topic_id
                && provisional.source != current.source
                && !provisional.activating
                && rank(provisional) < own
        })
        .collect::<Vec<_>>();
    losers.sort_by_key(|provisional| rank(provisional));
    let total = sum(&mut held.iter().map(|provisional| provisional.bytes));
    let sizes = losers.iter().map(|provisional| provisional.bytes);
    let Some(count) = reclaim_count(total, incoming, limits.total_bytes, sizes) else {
        return Err(Error::StagingCapacity(
            "bootstrap staging byte budget is full".into(),
        ));
    };
    for loser in losers.into_iter().take(count) {
        // A loser that changed since it was read is left alone; the store's
        // own check then decides whether the incoming bytes fit.
        storage.discard_provisional(loser)?;
    }
    Ok(())
}

/// How many of `losers`, taken in order, must go before `incoming` bytes fit
/// beside `total` within `limit`: zero when they already fit, `None` when even
/// all of them would not make room.
fn reclaim_count(
    total: u64,
    incoming: u64,
    limit: u64,
    losers: impl IntoIterator<Item = u64>,
) -> Option<usize> {
    let fits = |total: u64| {
        total
            .checked_add(incoming)
            .is_some_and(|after| after <= limit)
    };
    let mut remaining = total;
    for (count, bytes) in std::iter::once(0).chain(losers).enumerate() {
        remaining = remaining.saturating_sub(bytes);
        if fits(remaining) {
            return Some(count);
        }
    }
    None
}

impl<S: Storage> Irokle<S> {
    /// Stage an unknown topic until history proves membership of both source and this node.
    /// Smaller genesis replaces staging; larger genesis is refused as stale.
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
        let genesis = data.ops.iter().find(|op| is_structural_genesis(op));
        let fragment = genesis.map(|op| op.id);
        // A genesis that already names this node and the source proves nothing
        // more when staged: with no staging of the topic, admit it directly.
        if let Some(TopicPayload::Genesis(created)) = genesis.map(|op| &op.signed.body.payload)
            && created.initial_peers.contains(&self.peer_id())
            && created.initial_peers.contains(&source)
            && !storage
                .provisional_topics()?
                .iter()
                .any(|provisional| provisional.topic_id == topic_id)
        {
            return Ok(Bootstrap::Active(BTreeSet::new()));
        }
        let current = self.provisional_of(source, topic_id)?;
        let provisional = match (current, fragment) {
            (Some(current), _) if current.activating => {
                return self.activate_proven(&current, current.source);
            }
            (Some(current), Some(genesis)) if genesis < current.genesis => {
                // A staging that changed since it was read is not replaced blindly.
                if !storage.discard_provisional(&current)? {
                    return Err(Error::AdmissionConflict);
                }
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
        let mut bytes = 0_u64;
        for op in &data.ops {
            bytes = bytes.saturating_add(crate::storage::pending_op_bytes(op)? as u64);
        }
        make_room(storage, &provisional, bytes)?;
        let admitted = self.oplog.sharing_membership(store).receive_preverified(
            Some(source),
            data.ops.clone(),
            verified,
            None,
        );
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
                if let Some(current) = self
                    .provisional_of(provisional.source, provisional.topic_id)?
                    .filter(|current| {
                        current.session == provisional.session
                            && current.genesis == provisional.genesis
                    })
                    && !storage.discard_provisional(&current)?
                {
                    return Err(Error::AdmissionConflict);
                }
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

    /// End namespaces with no touch for the staging idle limit, unless
    /// activating. Each ends only while it is exactly as this scan read it, so
    /// a write or touch in between keeps it.
    pub(crate) fn expire_bootstraps(&self, now_ms: u64) -> Result<()> {
        let storage = self.storage();
        for provisional in storage.provisional_topics()? {
            if !provisional.activating
                && provisional.updated_ms < now_ms.saturating_sub(STAGED_IDLE_MS)
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

#[cfg(test)]
mod tests {
    use super::reclaim_count;

    /// Weaker stagings are discarded only as far as the incoming bytes need,
    /// and not at all when discarding every one would not make room.
    #[test]
    fn reclaim_counts() {
        // Limit 100 with 50 held by the incoming staging and 5 and 35 by weaker ones.
        assert_eq!(reclaim_count(90, 10, 100, [5, 35]), Some(0));
        assert_eq!(reclaim_count(90, 15, 100, [5, 35]), Some(1));
        assert_eq!(reclaim_count(90, 30, 100, [5, 35]), Some(2));
        assert_eq!(reclaim_count(90, 51, 100, [5, 35]), None);
        assert_eq!(reclaim_count(90, 40, 100, [35, 5]), Some(1));
        assert_eq!(reclaim_count(u64::MAX, 1, u64::MAX, [0]), None);
    }
}
