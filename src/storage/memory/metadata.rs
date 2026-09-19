//! Reservations for records that can exist independently of operations.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::storage::memory::budget::{Charge, NodePlan};
use crate::storage::memory::{MemoryDomain, MemoryInner, ObligationKind};
use crate::storage::{ObligationTarget, PeerAck, SyncObligation, SyncPeerStatus};
use crate::{ActorClock, EvictionKey, PeerId, Result, TopicId};

pub(super) fn obligation_bytes(obligation: &SyncObligation) -> u64 {
    4096 + match &obligation.target {
        ObligationTarget::Clock(clock) => ActorClock::allocation_bound(clock.len()) as u64,
        ObligationTarget::Repair(ids) => ids.len() as u64 * 256,
    }
}

pub(super) fn merge_workspace<'a>(
    inner: &MemoryInner,
    obligations: impl Iterator<Item = &'a SyncObligation>,
) -> Result<Charge> {
    let bytes = obligations.fold(0_u64, |bytes, incoming| {
        let old = inner
            .obligations
            .get(&(incoming.topic_id, incoming.peer_id))
            .and_then(|records| records.get(&ObligationKind::of(incoming)));
        bytes
            .saturating_add(obligation_bytes(incoming))
            .saturating_add(old.map_or(0, obligation_bytes))
    });
    inner.budget.reserve(MemoryDomain::Workspace, bytes)
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum MetadataKey {
    Topic(TopicId),
    Rejected(TopicId),
    Ack(TopicId, PeerId),
    Obligation(TopicId, PeerId, ObligationKind),
    Status(TopicId, PeerId),
    Eviction(EvictionKey),
}

impl MetadataKey {
    pub(super) fn peer(self, topic: TopicId, peer: PeerId) -> bool {
        matches!(self, Self::Ack(t, p) | Self::Obligation(t, p, _) | Self::Status(t, p)
            if t == topic && p == peer)
    }

    pub(super) fn reset(self, topic: TopicId) -> bool {
        matches!(self, Self::Rejected(t) | Self::Ack(t, _) | Self::Obligation(t, _, _)
            | Self::Status(t, _) if t == topic)
    }
}

pub(super) struct MetadataPlan {
    charges: BTreeMap<MetadataKey, Arc<Charge>>,
    nodes: NodePlan,
    domain: MemoryDomain,
}

impl MetadataPlan {
    pub(super) fn new(inner: &MemoryInner) -> Result<Self> {
        Self::build(inner, MemoryDomain::Metadata)
    }

    pub(super) fn control(inner: &MemoryInner) -> Result<Self> {
        Self::build(inner, MemoryDomain::Control)
    }

    fn build(inner: &MemoryInner, domain: MemoryDomain) -> Result<Self> {
        Ok(Self {
            charges: BTreeMap::new(),
            nodes: inner.budget.nodes_in(if domain == MemoryDomain::Control {
                domain
            } else {
                MemoryDomain::SharedNodes
            })?,
            domain,
        })
    }

    pub(super) fn reserve(
        &mut self,
        inner: &MemoryInner,
        key: MetadataKey,
        bytes: u64,
    ) -> Result<()> {
        let charge = match inner.metadata.get(&key) {
            Some(charge) if charge.bytes >= bytes => Arc::clone(charge),
            _ => Arc::new(inner.budget.reserve(self.domain, bytes)?),
        };
        self.charges.insert(key, charge);
        Ok(())
    }

    pub(super) fn obligation(
        &mut self,
        inner: &MemoryInner,
        obligation: &SyncObligation,
    ) -> Result<()> {
        if obligation.is_empty() {
            return Ok(());
        }
        let bytes = match &obligation.target {
            ObligationTarget::Clock(clock) => {
                self.nodes.add(clock)?;
                4096
            }
            ObligationTarget::Repair(ids) => 4096 + ids.len() as u64 * 256,
        };
        self.reserve(
            inner,
            MetadataKey::Obligation(
                obligation.topic_id,
                obligation.peer_id,
                ObligationKind::of(obligation),
            ),
            bytes,
        )
    }

    pub(super) fn ack(&mut self, inner: &MemoryInner, ack: &PeerAck) -> Result<()> {
        self.nodes.add(&ack.clock)?;
        self.reserve(
            inner,
            MetadataKey::Ack(ack.topic_id, ack.peer_id),
            4096 + ack.heads.len() as u64 * 256,
        )
    }

    pub(super) fn status(&mut self, inner: &MemoryInner, status: &SyncPeerStatus) -> Result<()> {
        self.reserve(
            inner,
            MetadataKey::Status(status.topic_id, status.peer_id),
            4096 + status.last_error.as_ref().map_or(0, String::capacity) as u64
                + status.recent_attempts.capacity() as u64 * 16,
        )
    }

    pub(super) fn commit(self, inner: &mut MemoryInner) {
        self.nodes.commit();
        inner.metadata.extend(self.charges);
    }
}
