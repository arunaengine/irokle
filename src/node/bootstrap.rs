// SPDX-License-Identifier: MIT OR Apache-2.0
//! Bootstrap of topics this node does not hold: each source stages its history
//! in a provisional namespace, admitted by the normal oplog, and the namespace
//! becomes the topic once that history makes this node and the source members.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};

use crate::storage::{AdmissionEffects, ProvisionalTopic, StagedTopic, SyncObligation, TopicState};
use crate::{ActorClock, Error, PeerId, Result, Storage, TopicId};

use super::{Bootstrap, Irokle};

/// Owner locks per topic. The registry lock is never held while storage work runs.
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

impl<S: Storage> Irokle<S> {
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
}

/// The receipt of a namespace: branch, session, contiguous clock and bytes.
fn staged_of<S: Storage>(provisional: &ProvisionalTopic, store: &S) -> Result<StagedTopic> {
    let clock = store.actor_clock(&provisional.topic_id)?;
    Ok(StagedTopic {
        genesis: Some(provisional.genesis),
        session: provisional.session,
        clock,
        ops: 0,
        bytes: store.stored_bytes()?,
    })
}
