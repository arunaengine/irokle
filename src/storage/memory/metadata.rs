//! Reservations for records that can exist independently of operations.

use super::budget::NodePlan;
use super::*;

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
}

impl MetadataPlan {
    pub(super) fn new(inner: &MemoryInner) -> Result<Self> {
        Ok(Self {
            charges: BTreeMap::new(),
            nodes: inner.budget.nodes()?,
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
            _ => Arc::new(inner.budget.reserve(MemoryDomain::Metadata, bytes)?),
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
