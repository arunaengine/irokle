// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::storage::TopicState;
use crate::{Op, OpId, Result, TopicId, TopicPayload};

use crate::oplog::{GenesisResolution, Oplog, ResetPlan, TopicEviction};

pub(crate) fn is_structural_genesis(op: &Op) -> bool {
    let body = &op.signed.body;
    matches!(body.payload, TopicPayload::Genesis(_))
        && body.actor_seq == 1
        && body.actor_prev.is_none()
        && body.deps.is_empty()
}

impl<S: crate::oplog::Storage> Oplog<S> {
    /// Resolve a valid genesis collision, returning the ops to admit, an optional reset
    /// plan when the local topic loses, and an optional rejected genesis to purge.
    pub(super) fn resolve_genesis_collision(
        &self,
        ops: Vec<Op>,
        verified: &BTreeSet<crate::OpId>,
    ) -> Result<GenesisResolution> {
        let Some(genesis) = ops.iter().find(|op| is_structural_genesis(op)).cloned() else {
            return Ok((ops, None, None));
        };
        let topic_id = genesis.signed.body.topic_id;
        let Some(state) = self.storage.topic_state(&topic_id)? else {
            // Fresh topic: normal admission accepts the genesis.
            return Ok((ops, None, None));
        };
        if state.genesis == genesis.id {
            // Same node re-sending its genesis: normal dedup handles it.
            return Ok((ops, None, None));
        }
        // Only a signature-valid genesis may win the tie-break.
        if !verified.contains(&genesis.id) {
            genesis.validate()?;
        }
        // Op ids are content-addressed 32-byte blake3 digests; the derived
        // `Ord` is lexicographic over those bytes, so both nodes pick the same
        // winner with no coordination.
        if genesis.id < state.genesis {
            // Genesis ids are grindable, so a smaller genesis resets only when it
            // names the same initial peers, its author among them. Every genesis of
            // a chain shares that set, so replicas agree whatever the arrival order.
            if !self.same_peers(&state.genesis, &genesis)? {
                tracing::warn!(
                    %topic_id,
                    local_genesis = %state.genesis,
                    foreign_genesis = %genesis.id,
                    author = %genesis.signed.body.author,
                    "rejected genesis collision naming other initial peers"
                );
                let filtered = without_descendants(ops, genesis.id);
                return Ok((filtered, None, Some(genesis.id)));
            }
            let eviction = self.extract_eviction(topic_id, &state, genesis.id)?;
            tracing::warn!(
                %topic_id,
                losing_genesis = %state.genesis,
                winning_genesis = %genesis.id,
                evicted = eviction.evicted.len(),
                "genesis collision resolved: reset local topic for smaller winning genesis"
            );
            Ok((
                ops,
                Some(ResetPlan {
                    expected_state: state,
                    eviction,
                }),
                None,
            ))
        } else {
            tracing::warn!(
                %topic_id,
                local_genesis = %state.genesis,
                foreign_genesis = %genesis.id,
                evicted = 0,
                "genesis collision resolved: kept local genesis, rejected larger foreign genesis"
            );
            let filtered = without_descendants(ops, genesis.id);
            Ok((filtered, None, Some(genesis.id)))
        }
    }

    /// Whether `candidate` names the initial peers of the genesis `genesis_id`,
    /// with its own author among them. An unreadable genesis matches nothing.
    fn same_peers(&self, genesis_id: &OpId, candidate: &Op) -> Result<bool> {
        let peers = |op: &Op| match &op.signed.body.payload {
            TopicPayload::Genesis(genesis) => Some(genesis.initial_peers.clone()),
            TopicPayload::Event(_) | TopicPayload::Control(_) => None,
        };
        let Some(current) = self.storage.get_op(genesis_id)?.as_ref().and_then(peers) else {
            return Ok(false);
        };
        Ok(peers(candidate)
            .is_some_and(|named| named == current && named.contains(&candidate.signed.body.author)))
    }

    /// Collect local non-genesis payloads by actor and sequence for re-emission.
    /// Reset and winner writes commit together in `reset_topic_and_admit`; reads
    /// stay outside that transaction.
    fn extract_eviction(
        &self,
        topic_id: TopicId,
        local_state: &TopicState,
        winning_genesis: OpId,
    ) -> Result<TopicEviction> {
        let mut discarded = self.storage.list_op_ids(&topic_id)?;
        discarded.remove(&local_state.genesis);
        Ok(TopicEviction {
            topic_id,
            losing_genesis: local_state.genesis,
            winning_genesis,
            evicted: self.evicted_ops(&discarded)?,
        })
    }

    /// Return payloads for `ids` ordered by actor and sequence for re-emission.
    /// Missing metadata or payloads are logged and skipped, so one damaged record
    /// does not strand the topic.
    pub(super) fn evicted_ops(&self, ids: &BTreeSet<OpId>) -> Result<Vec<crate::oplog::EvictedOp>> {
        let mut metas = Vec::new();
        for id in ids {
            match self.storage.get_meta(id)? {
                Some(meta) => metas.push(meta),
                None => tracing::warn!(%id, "discarded op has no metadata to re-emit"),
            }
        }
        metas.sort_by_key(|meta| (meta.actor_id, meta.actor_seq));
        let mut evicted = Vec::new();
        for meta in metas {
            let Some(op) = self.storage.get_op(&meta.id)? else {
                tracing::warn!(id = %meta.id, "discarded op has no record to re-emit");
                continue;
            };
            evicted.push(crate::oplog::EvictedOp {
                op_id: meta.id,
                actor_id: meta.actor_id,
                author: meta.author,
                actor_seq: meta.actor_seq,
                payload: op.signed.body.payload.clone(),
            });
        }
        Ok(evicted)
    }
}

fn without_descendants(ops: Vec<Op>, rejected: OpId) -> Vec<Op> {
    let mut children = BTreeMap::<OpId, Vec<OpId>>::new();
    for op in &ops {
        for dep in &op.signed.body.deps {
            children.entry(*dep).or_default().push(op.id);
        }
    }
    let mut rejected_ids = BTreeSet::from([rejected]);
    let mut pending = VecDeque::from([rejected]);
    while let Some(id) = pending.pop_front() {
        for child in children.remove(&id).unwrap_or_default() {
            if rejected_ids.insert(child) {
                pending.push_back(child);
            }
        }
    }
    ops.into_iter()
        .filter(|op| !rejected_ids.contains(&op.id))
        .collect()
}
