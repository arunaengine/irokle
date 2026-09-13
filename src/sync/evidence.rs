// SPDX-License-Identifier: MIT OR Apache-2.0
//! Evidence of what peers hold: signed acknowledgements, fingerprint matches,
//! forwarding obligations and reports.

use std::collections::BTreeSet;

use crate::storage::{PeerAck, Storage, TopicState};
use crate::{ActorClock, Error, OpId, PeerId, Result, TopicId};

use super::{SyncAck, SyncEngine};

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
                let message = err.to_string();
                for index in validated {
                    results[index] = Err(Error::Storage(message.clone()));
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
}
