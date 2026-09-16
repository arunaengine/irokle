// SPDX-License-Identifier: MIT OR Apache-2.0
//! Evidence of what peers hold: signed acknowledgements, fingerprint matches,
//! forwarding obligations and reports.

use std::collections::BTreeSet;

use crate::storage::{PeerAck, SnapshotRead, Storage, SyncObligation, TopicState};
use crate::{ActorClock, Error, OpId, PeerId, Result, TopicId, actor_id_for};

use super::{SyncAck, SyncEngine, SyncReport};

impl<S: Storage> SyncEngine<S> {
    /// Heads and clock an ack may certify, read from one view with their genesis. A topic
    /// holding an unresolvable id certifies nothing until repair completes, so the source
    /// keeps its obligation and this node stays visibly behind.
    pub(super) fn ack_frontier(
        &self,
        topic_id: &TopicId,
    ) -> Result<(TopicState, BTreeSet<OpId>, ActorClock)> {
        let (view, whole) = self
            .oplog
            .whole_view(topic_id)?
            .ok_or(Error::TopicNotFound)?;
        if !whole {
            tracing::debug!(%topic_id, "withholding ack frontier for an incomplete topic");
            return Ok((view.state, BTreeSet::new(), ActorClock::new()));
        }
        let heads = view.state.heads.clone();
        Ok((view.state, heads, view.clock))
    }

    pub fn apply_ack(&self, ack: &SyncAck) -> Result<()> {
        ack.verify_signature()?;
        self.validate_ack(ack)?;
        // Storage repeats the identity and membership checks in the writing
        // transaction, so a reset or removal in between refuses the commit.
        self.oplog
            .storage()
            .apply_peer_ack(Self::peer_ack_for(ack))?;
        Ok(())
    }

    /// Apply many acks with the storage writes batched into one operation.
    /// Each ack is verified and validated individually so a bad ack does not
    /// block the others. Returns one result per input ack, in order.
    pub fn apply_acks(&self, acks: &[SyncAck]) -> Vec<Result<()>> {
        let mut results = Vec::with_capacity(acks.len());
        let mut validated = Vec::new();
        let mut peer_acks = Vec::new();
        for (index, ack) in acks.iter().enumerate() {
            match ack.verify_signature().and_then(|()| self.validate_ack(ack)) {
                Ok(()) => {
                    validated.push(index);
                    peer_acks.push(Self::peer_ack_for(ack));
                    results.push(Ok(()));
                }
                Err(err) => results.push(Err(err)),
            }
        }
        if peer_acks.is_empty() {
            return results;
        }
        // One uncertifiable record is reported against its own ack; only a
        // backend failure covering the whole batch fails the rest.
        match self.oplog.storage().apply_peer_acks(peer_acks) {
            Ok(applied) => {
                for (index, outcome) in validated.into_iter().zip(applied) {
                    if let Err(err) = outcome {
                        results[index] = Err(err);
                    }
                }
            }
            Err(err) => {
                let source = std::sync::Arc::new(err);
                for index in validated {
                    results[index] = Err(Error::Shared(std::sync::Arc::clone(&source)));
                }
            }
        }
        results
    }

    pub fn record_peer_synced(&self, peer_id: PeerId, topic_id: TopicId) -> Result<()> {
        let (state, heads, clock) = self.ack_frontier(&topic_id)?;
        if !state.members.contains(&peer_id) {
            return Err(Error::NotTopicMember);
        }
        let peer_ack = PeerAck {
            peer_id,
            topic_id,
            genesis: Some(state.genesis),
            heads,
            clock,
        };
        self.oplog.storage().apply_peer_ack(peer_ack)?;
        Ok(())
    }

    /// Record that `peer_id` matched this topic's fingerprint. The compared
    /// fingerprint and the certified frontier come from the same view, so a
    /// reset after the comparison cannot swap in the replacement branch.
    #[cfg(feature = "iroh")]
    pub(crate) fn record_fingerprint(
        &self,
        peer_id: PeerId,
        topic_id: TopicId,
        fingerprint: [u8; 32],
    ) -> Result<bool> {
        let (state, heads, clock) = self.ack_frontier(&topic_id)?;
        if !state.members.contains(&peer_id) {
            return Err(Error::NotTopicMember);
        }
        if crate::storage::topic_fingerprint_for(&heads, &clock)? != fingerprint {
            return Ok(false);
        }
        self.oplog.storage().apply_peer_ack(PeerAck {
            peer_id,
            topic_id,
            genesis: Some(state.genesis),
            heads,
            clock,
        })?;
        Ok(true)
    }

    fn validate_ack(&self, ack: &SyncAck) -> Result<()> {
        self.oplog
            .storage()
            .read_snapshot(|read| self.validate_ack_in(read, ack))
    }

    fn validate_ack_in(&self, read: &dyn SnapshotRead, ack: &SyncAck) -> Result<()> {
        let view = read
            .topic_view(&ack.topic_id, None)?
            .ok_or(Error::TopicNotFound)?;
        let state = &view.state;
        match ack.genesis {
            Some(genesis) if genesis == state.genesis => {}
            Some(_) => return Err(Error::StaleIncarnation),
            None => {
                return Err(Error::InvalidSyncAck(
                    "acknowledgement does not name the topic incarnation it certifies".into(),
                ));
            }
        }
        if !state.members.contains(&ack.peer_id) {
            return Err(Error::NotTopicMember);
        }

        // Only our own actor is locally bounded: we author all of its ops, so
        // no peer can hold more. Other actors reach a peer through a third
        // member before they reach us, so their entries stay unchecked.
        let local_actor = actor_id_for(ack.topic_id, self.peer_id);
        let local_seq = view.clock.get(&local_actor);
        let claimed_seq = ack.clock.get(&local_actor);
        if claimed_seq > local_seq {
            return Err(Error::InvalidSyncAck(format!(
                "clock for actor {local_actor} claims seq {claimed_seq}, local seq is {local_seq}"
            )));
        }

        for op_id in ack.accepted.iter().chain(ack.heads.iter()) {
            // History we have not learned yet makes no locally checkable claim.
            let Some(meta) = read.get_position(op_id)? else {
                continue;
            };
            if meta.topic_id != ack.topic_id {
                return Err(Error::TopicMismatch);
            }
            if ack.heads.contains(op_id) && ack.clock.get(&meta.actor_id) < meta.actor_seq {
                return Err(Error::InvalidSyncAck(format!(
                    "head {op_id} is not represented by ack clock"
                )));
            }
        }
        Ok(())
    }

    /// The stored record for a validated acknowledgement. One constructor means
    /// no evidence path can forget the incarnation the proof was signed for.
    fn peer_ack_for(ack: &SyncAck) -> PeerAck {
        PeerAck {
            peer_id: ack.peer_id,
            topic_id: ack.topic_id,
            genesis: ack.genesis,
            heads: ack.heads.clone(),
            clock: ack.clock.clone(),
        }
    }

    pub fn put_obligation(
        &self,
        peer_id: PeerId,
        topic_id: TopicId,
        op_ids: BTreeSet<OpId>,
    ) -> Result<()> {
        if op_ids.is_empty() {
            return Ok(());
        }
        // Ids are resolved against this branch, so the writes are conditioned on it.
        let genesis = self
            .oplog
            .storage()
            .topic_state(&topic_id)?
            .map(|state| state.genesis);
        let mut resolved = BTreeSet::new();
        let mut unresolved = BTreeSet::new();
        let mut target_clock = ActorClock::new();
        for op_id in &op_ids {
            if let Some(meta) = self.oplog.storage().get_position(op_id)?
                && meta.topic_id == topic_id
            {
                target_clock.observe(meta.actor_id, meta.actor_seq);
                resolved.insert(*op_id);
            } else {
                unresolved.insert(*op_id);
            }
        }
        if !resolved.is_empty() {
            self.oplog.storage().put_sync_obligation(
                SyncObligation::clock(peer_id, topic_id, target_clock),
                genesis,
            )?;
        }
        // An id with no trustworthy actor position becomes an explicit repair
        // want, so the positions that did resolve still coalesce by clock.
        if !unresolved.is_empty() {
            self.oplog.storage().put_sync_obligation(
                SyncObligation::repair(peer_id, topic_id, unresolved),
                genesis,
            )?;
        }
        Ok(())
    }

    pub fn report(&self, peer_id: PeerId, topic_id: TopicId) -> Result<SyncReport> {
        Ok(SyncReport {
            topic_id,
            peer_id,
            obligations: self.oplog.storage().sync_obligations(&peer_id, &topic_id)?,
        })
    }
}
