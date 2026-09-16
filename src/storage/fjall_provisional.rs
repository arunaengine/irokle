// SPDX-License-Identifier: MIT OR Apache-2.0
//! Provisional bootstrap namespaces in Fjall. Each namespace takes one slot of
//! a fixed pool of keyspaces, which is cleared before another session reuses it.
//!
//! The registry in the main keyspace owns every namespace, in four phases:
//! - staging: `bn<source><topic>` names the session and `bs<slot>` gives it the
//!   slot. A view reads while `bn` names its session and writes, checked in the
//!   writing transaction, while that session is not activating.
//! - activating: `bn` is frozen and `ba<topic>` claims the topic's one
//!   activation for the session. Copies into the active records stay hidden
//!   from every read until publication; each copy checks the claim.
//! - published: one transaction installs the topic, drops the claim and ends
//!   every namespace of the topic.
//! - clearing: `bs` names the ended session; every delete and the release of
//!   the slot check that session.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{ActorClock, Error, OpId, PeerId, Result, TopicId};

use super::fjall::{FjallStorage, Hook};
use super::{
    AdmissionEffects, PeerAck, ProvisionalTopic, StagingLimits, StagingQuota, TopicState,
    ack_covers, check_namespaces,
};

type Tx = super::pressure::Transaction;
type Records = fjall::OptimisticTxKeyspace;

/// Namespace record, `bn<source><topic>`. No other key begins with `b`.
const NAMESPACE: &[u8] = b"bn";
/// Slot owner, `bs<slot>` with a big-endian slot number.
pub(super) const SLOT: &[u8] = b"bs";
/// The durable session counter.
const SESSIONS: &[u8] = b"bc";
/// Activation claim, `ba<topic>` holding the claiming session.
pub(super) const ACTIVATING: &[u8] = b"ba";
/// `bq<slot>`: the bytes a clearing slot still holds and the source they came
/// from, charged against the staging limits until the slot is released.
pub(super) const CLEARING: &[u8] = b"bq";
/// Serialized bytes of ops admitted into a namespace keyspace.
pub(super) const ADMITTED_BYTES: &[u8] = b"nb";
/// Records one copy or clearing transaction moves.
const CHUNK: usize = 4096;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NamespaceRecord {
    provisional: ProvisionalTopic,
    slot: u32,
}

/// Schema 5 layout of a namespace record, without revision and bytes.
#[derive(Deserialize)]
struct LegacyNamespaceRecord {
    source: PeerId,
    topic_id: TopicId,
    genesis: OpId,
    session: u64,
    updated_ms: u64,
    activating: bool,
    slot: u32,
}

/// The charge a clearing slot carries over from its ended session.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(super) struct ClearingCharge {
    pub(super) source: PeerId,
    pub(super) bytes: u64,
}

pub(super) fn clearing_key(slot: u32) -> Vec<u8> {
    [CLEARING, &slot.to_be_bytes()].concat()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct SlotRecord {
    pub(super) source: PeerId,
    topic_id: TopicId,
    session: u64,
    /// The session ended; the keyspace is emptied before the slot is reused.
    pub(super) clearing: bool,
}

/// The capability of a namespace view: the registry record it stages under.
#[derive(Clone)]
pub(super) struct Fence {
    registry: Records,
    key: Vec<u8>,
    session: u64,
}

fn namespace_key(source: &PeerId, topic_id: &TopicId) -> Vec<u8> {
    [NAMESPACE, source.as_ref(), topic_id.as_ref()].concat()
}

fn slot_key(slot: u32) -> Vec<u8> {
    [SLOT, &slot.to_be_bytes()].concat()
}

/// Whether a namespace key holds a record an active topic keeps: an op, its
/// metadata, a clock node, a child edge, an actor index or tip, or the topic's
/// op index.
fn copied_record(key: &[u8]) -> bool {
    matches!(
        (key.get(..1), key.get(..2), key.len()),
        (Some(b"o" | b"m"), _, 33)
            | (_, Some(b"ch" | b"at" | b"to" | b"cn"), 66)
            | (_, Some(b"as"), 74)
    )
}

impl FjallStorage {
    fn tx_namespaces(tx: &impl fjall::Readable, records: &Records) -> Result<Vec<NamespaceRecord>> {
        let mut out = Vec::new();
        for item in fjall::Readable::prefix(tx, records, NAMESPACE) {
            out.push(postcard::from_bytes(item.value()?.as_ref())?);
        }
        Ok(out)
    }

    /// The keyspace of `slot`.
    pub(super) fn slot_records(&self, slot: u32) -> Result<Records> {
        Ok(self.db.keyspace(
            &format!("bootstrap-{slot}"),
            fjall::KeyspaceCreateOptions::default,
        )?)
    }

    fn current_record(&self, provisional: &ProvisionalTopic) -> Result<Option<NamespaceRecord>> {
        let record: Option<NamespaceRecord> =
            self.get(namespace_key(&provisional.source, &provisional.topic_id))?;
        Ok(record.filter(|record| record.provisional.session == provisional.session))
    }

    /// The registry record of `fence`, while its session is registered.
    fn fenced(tx: &impl fjall::Readable, fence: &Fence) -> Result<NamespaceRecord> {
        Self::tx_get::<NamespaceRecord>(tx, &fence.registry, fence.key.as_slice())?
            .filter(|record| record.provisional.session == fence.session)
            .ok_or(Error::StaleIncarnation)
    }

    /// Refuse a view's read once its session ended.
    pub(super) fn check_fence(tx: &impl fjall::Readable, fence: &Fence) -> Result<()> {
        Self::fenced(tx, fence).map(drop)
    }

    /// Refuse a view's write unless its session still stages. Read in the
    /// writing transaction, so an end or activation committed meanwhile makes
    /// the write conflict instead of landing after it.
    pub(super) fn tx_fence_write(tx: &Tx, fence: &Fence) -> Result<()> {
        if Self::fenced(tx, fence)?.provisional.activating {
            return Err(Error::StaleIncarnation);
        }
        Ok(())
    }

    /// Record a view's write in its registry record: the bytes its keyspace
    /// holds now and a new revision.
    pub(super) fn tx_fence_commit(tx: &mut Tx, fence: &Fence, records: &Records) -> Result<()> {
        let mut record = Self::fenced(tx, fence)?;
        let admitted: u64 = Self::tx_get(tx, records, ADMITTED_BYTES)?.unwrap_or_default();
        record.provisional.bytes = admitted + Self::tx_pending_bytes(tx, records)?;
        record.provisional.revision = record.provisional.revision.saturating_add(1);
        Self::tx_put(tx, &fence.registry, fence.key.as_slice(), &record)
    }

    /// What every other namespace leaves the view of `fence`, read in its
    /// writing transaction so a concurrent charge makes one of them conflict.
    pub(super) fn tx_staging_quota(
        tx: &Tx,
        fence: &Fence,
        limits: &StagingLimits,
    ) -> Result<StagingQuota> {
        let own = Self::fenced(tx, fence)?.provisional;
        let (mut others, mut source) = (0_u64, 0_u64);
        for record in Self::tx_namespaces(tx, &fence.registry)? {
            if record.provisional.session == own.session {
                continue;
            }
            others = others.saturating_add(record.provisional.bytes);
            if record.provisional.source == own.source {
                source = source.saturating_add(record.provisional.bytes);
            }
        }
        // Bytes of ended sessions stay charged until their slot is emptied.
        for item in fjall::Readable::prefix(tx, &fence.registry, CLEARING) {
            let charge: ClearingCharge = postcard::from_bytes(item.value()?.as_ref())?;
            others = others.saturating_add(charge.bytes);
            if charge.source == own.source {
                source = source.saturating_add(charge.bytes);
            }
        }
        Ok(StagingQuota::new(limits, others, source))
    }

    /// Empty every slot whose session ended, one bounded transaction at a time,
    /// then release it. Every delete and the release check in their own
    /// transaction that the slot still clears that session, so a repeated or
    /// late pass never touches a slot another session took.
    fn reclaim_slots(&self) -> Result<()> {
        let mut clearing = Vec::new();
        for item in fjall::Readable::prefix(&self.db.read_tx(), &self.records, SLOT) {
            let (key, value) = item.into_inner()?;
            let record: SlotRecord = postcard::from_bytes(value.as_ref())?;
            if record.clearing {
                clearing.push((key.to_vec(), record.session));
            }
        }
        for (key, session) in clearing {
            let slot = key
                .get(SLOT.len()..)
                .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
                .map(u32::from_be_bytes)
                .ok_or_else(|| Error::Storage("corrupt fjall bootstrap slot key".into()))?;
            let store = self.slot_records(slot)?;
            let clears = |tx: &Tx| -> Result<bool> {
                Ok(
                    Self::tx_get::<SlotRecord>(tx, &self.records, key.as_slice())?
                        .is_some_and(|record| record.clearing && record.session == session),
                )
            };
            let mut chunk = CHUNK;
            let owned = loop {
                self.hook(Hook::DeleteChunk)?;
                let removed = self.transaction(|tx| {
                    if !clears(tx)? {
                        return Ok(None);
                    }
                    let mut keys = Vec::new();
                    for item in fjall::Readable::iter(tx, &store).take(chunk) {
                        keys.push(item.key()?.to_vec());
                    }
                    for key in &keys {
                        tx.remove(&store, key.clone())?;
                    }
                    Ok(Some(keys.len()))
                });
                let removed = match removed {
                    Err(Error::StorageBuffer { .. }) if chunk > 1 => {
                        chunk = chunk.div_ceil(2);
                        continue;
                    }
                    result => result?,
                };
                match removed {
                    None => break false,
                    Some(removed) if removed < chunk => break true,
                    Some(_) => {}
                }
            };
            if !owned {
                continue;
            }
            self.hook(Hook::ReleaseSlot)?;
            self.transaction(|tx| {
                if clears(tx)? && fjall::Readable::iter(tx, &store).next().is_none() {
                    tx.remove(&self.records, key.clone())?;
                    tx.remove(&self.records, clearing_key(slot))?;
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    /// Every slot in use: whether it clears, the bytes its keyspace counts
    /// and the charge a clearing slot carries.
    #[cfg(test)]
    pub(crate) fn slot_bytes(&self) -> Result<Vec<(bool, u64, Option<u64>)>> {
        let tx = self.db.read_tx();
        let mut out = Vec::new();
        for item in fjall::Readable::prefix(&tx, &self.records, SLOT) {
            let (key, value) = item.into_inner()?;
            let record: SlotRecord = postcard::from_bytes(value.as_ref())?;
            let slot = u32::from_be_bytes(key[SLOT.len()..].try_into().unwrap());
            let store = self.slot_records(slot)?;
            let admitted: u64 = Self::tx_get(&tx, &store, ADMITTED_BYTES)?.unwrap_or_default();
            let counted = admitted + Self::tx_pending_bytes(&tx, &store)?;
            let charge = Self::tx_get::<ClearingCharge>(&tx, &self.records, clearing_key(slot))?;
            out.push((record.clearing, counted, charge.map(|charge| charge.bytes)));
        }
        Ok(out)
    }

    pub(super) fn read_provisional_topics(&self) -> Result<Vec<ProvisionalTopic>> {
        Ok(Self::tx_namespaces(&self.db.read_tx(), &self.records)?
            .into_iter()
            .map(|record| record.provisional)
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
        match self.reclaim_slots() {
            Ok(()) | Err(Error::AdmissionConflict | Error::StorageBuffer { .. }) => {}
            Err(error) => return Err(error),
        }
        let limits = self.limits;
        self.transaction(|tx| {
            if fjall::Readable::contains_key(tx, &self.records, Self::key_id(b"ts", &topic_id))? {
                return Err(Error::AdmissionConflict);
            }
            let key = namespace_key(&source, &topic_id);
            if let Some(record) =
                Self::tx_get::<NamespaceRecord>(tx, &self.records, key.as_slice())?
            {
                return Ok(record.provisional);
            }
            let namespaces = Self::tx_namespaces(tx, &self.records)?;
            let from_source = namespaces
                .iter()
                .filter(|record| record.provisional.source == source)
                .count();
            check_namespaces(&limits, namespaces.len(), from_source)?;
            let mut used = BTreeSet::new();
            for item in fjall::Readable::prefix(tx, &self.records, SLOT) {
                let key = item.key()?;
                if let Some(slot) = key
                    .get(SLOT.len()..)
                    .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
                {
                    used.insert(u32::from_be_bytes(slot));
                }
            }
            let slot = (0..u32::try_from(limits.namespaces).unwrap_or(u32::MAX))
                .find(|slot| !used.contains(slot))
                .ok_or_else(|| {
                    Error::StagingCapacity("bootstrap namespace slots are clearing".into())
                })?;
            let session = Self::tx_get::<u64>(tx, &self.records, SESSIONS)?
                .unwrap_or_default()
                .checked_add(1)
                .ok_or_else(|| Error::Storage("bootstrap session overflow".into()))?;
            let provisional = ProvisionalTopic {
                source,
                topic_id,
                genesis,
                session,
                updated_ms: now_ms,
                activating: false,
                revision: 0,
                bytes: 0,
            };
            Self::tx_put(tx, &self.records, SESSIONS, &session)?;
            Self::tx_put(
                tx,
                &self.records,
                key,
                &NamespaceRecord {
                    provisional: provisional.clone(),
                    slot,
                },
            )?;
            Self::tx_put(
                tx,
                &self.records,
                slot_key(slot),
                &SlotRecord {
                    source,
                    topic_id,
                    session,
                    clearing: false,
                },
            )?;
            Ok(provisional)
        })
    }

    pub(super) fn namespace_store(&self, provisional: &ProvisionalTopic) -> Result<Option<Self>> {
        self.main_store()?;
        let Some(record) = self.current_record(provisional)? else {
            return Ok(None);
        };
        let fence = Fence {
            registry: self.records.clone(),
            key: namespace_key(&provisional.source, &provisional.topic_id),
            session: provisional.session,
        };
        Ok(Some(
            self.namespace_view(self.slot_records(record.slot)?, fence),
        ))
    }

    pub(super) fn touch_namespace(
        &self,
        provisional: &ProvisionalTopic,
        now_ms: u64,
    ) -> Result<()> {
        self.main_store()?;
        let key = namespace_key(&provisional.source, &provisional.topic_id);
        self.transaction(|tx| {
            if let Some(mut record) =
                Self::tx_get::<NamespaceRecord>(tx, &self.records, key.as_slice())?
                && record.provisional.session == provisional.session
                && record.provisional.updated_ms < now_ms
            {
                record.provisional.updated_ms = now_ms;
                Self::tx_put(tx, &self.records, key.as_slice(), &record)?;
            }
            Ok(())
        })
    }

    /// The topic state a namespace keyspace holds, with its heads.
    fn tx_namespace_state(
        tx: &impl fjall::Readable,
        records: &Records,
        topic_id: &TopicId,
    ) -> Result<Option<TopicState>> {
        let heads: BTreeSet<OpId> =
            Self::tx_get(tx, records, Self::key_id(b"h", topic_id))?.unwrap_or_default();
        Ok(
            Self::tx_get::<TopicState>(tx, records, Self::key_id(b"ts", topic_id))?.map(
                |mut state| {
                    state.heads = heads;
                    state
                },
            ),
        )
    }

    /// End a namespace's session in `tx`: its record goes and its slot clears,
    /// keeping the namespace's bytes charged until the slot is released.
    fn tx_end_namespace(tx: &mut Tx, records: &Records, record: &NamespaceRecord) -> Result<()> {
        let provisional = &record.provisional;
        tx.remove(
            records,
            namespace_key(&provisional.source, &provisional.topic_id),
        )?;
        Self::tx_put(
            tx,
            records,
            clearing_key(record.slot),
            &ClearingCharge {
                source: provisional.source,
                bytes: provisional.bytes,
            },
        )?;
        Self::tx_put(
            tx,
            records,
            slot_key(record.slot),
            &SlotRecord {
                source: provisional.source,
                topic_id: provisional.topic_id,
                session: provisional.session,
                clearing: true,
            },
        )
    }

    pub(super) fn discard_namespace(&self, provisional: &ProvisionalTopic) -> Result<bool> {
        self.main_store()?;
        let key = namespace_key(&provisional.source, &provisional.topic_id);
        let ended = self.transaction(|tx| {
            let Some(record) = Self::tx_get::<NamespaceRecord>(tx, &self.records, key.as_slice())?
            else {
                return Ok(false);
            };
            if record.provisional != *provisional || record.provisional.activating {
                return Ok(false);
            }
            Self::tx_end_namespace(tx, &self.records, &record)?;
            Ok(true)
        })?;
        if ended {
            self.reclaim_slots()?;
        }
        Ok(ended)
    }

    pub(super) fn activate_namespace(
        &self,
        provisional: &ProvisionalTopic,
        expected: &TopicState,
        effects: &AdmissionEffects,
    ) -> Result<()> {
        self.main_store()?;
        if effects
            .sync_obligations
            .iter()
            .any(|obligation| obligation.topic_id != provisional.topic_id)
        {
            return Err(Error::TopicMismatch);
        }
        let store = self.claim_activation(provisional, expected)?;
        self.hook(Hook::Claimed)?;
        self.copy_namespace(provisional, &store)?;
        self.hook(Hook::Publish)?;
        self.finish_activation(provisional, &store, expected, effects)?;
        self.reclaim_slots()
    }

    /// Stop an activation after its copies, as a crash there would.
    #[cfg(test)]
    pub(crate) fn interrupt_activation(&self, provisional: &ProvisionalTopic) {
        let record = self.current_record(provisional).unwrap().unwrap();
        let records = self.slot_records(record.slot).unwrap();
        let state = Self::tx_namespace_state(&self.db.read_tx(), &records, &provisional.topic_id)
            .unwrap()
            .unwrap();
        let store = self.claim_activation(provisional, &state).unwrap();
        self.copy_namespace(provisional, &store).unwrap();
    }

    /// Refuse unless `session` holds the activation claim of an inactive topic.
    fn tx_claimed(tx: &Tx, records: &Records, topic_id: &TopicId, session: u64) -> Result<()> {
        if fjall::Readable::contains_key(tx, records, Self::key_id(b"ts", topic_id))?
            || Self::tx_get::<u64>(tx, records, Self::key_id(ACTIVATING, topic_id))?
                != Some(session)
        {
            return Err(Error::AdmissionConflict);
        }
        Ok(())
    }

    /// Claim the topic's one activation for the session of `provisional` and
    /// freeze its namespace at `expected`, in one transaction. The same session
    /// may claim again; another session's claim or an active topic refuses.
    /// Returns the namespace keyspace.
    fn claim_activation(
        &self,
        provisional: &ProvisionalTopic,
        expected: &TopicState,
    ) -> Result<Records> {
        let topic_id = provisional.topic_id;
        let key = namespace_key(&provisional.source, &topic_id);
        let claim = Self::key_id(ACTIVATING, &topic_id);
        let slot = self.transaction(|tx| {
            let mut record = Self::tx_get::<NamespaceRecord>(tx, &self.records, key.as_slice())?
                .filter(|record| record.provisional.session == provisional.session)
                .ok_or(Error::StaleIncarnation)?;
            if fjall::Readable::contains_key(tx, &self.records, Self::key_id(b"ts", &topic_id))? {
                return Err(Error::AdmissionConflict);
            }
            let claimed = Self::tx_get::<u64>(tx, &self.records, claim.as_slice())?;
            if claimed.is_some_and(|session| session != provisional.session) {
                return Err(Error::AdmissionConflict);
            }
            // The state is read here, so a write the view commits meanwhile
            // conflicts with the freeze instead of following it.
            let store = self.slot_records(record.slot)?;
            if Self::tx_namespace_state(tx, &store, &topic_id)?.as_ref() != Some(expected) {
                return Err(Error::AdmissionConflict);
            }
            if !record.provisional.activating || claimed.is_none() {
                record.provisional.activating = true;
                record.provisional.revision = record.provisional.revision.saturating_add(1);
                Self::tx_put(tx, &self.records, key.as_slice(), &record)?;
                Self::tx_put(tx, &self.records, claim.as_slice(), &provisional.session)?;
            }
            Ok(record.slot)
        })?;
        self.slot_records(slot)
    }

    /// Copy the frozen namespace into the active records in bounded
    /// transactions, each under the activation claim. Copies stay invisible
    /// until the state record names the topic.
    fn copy_namespace(&self, provisional: &ProvisionalTopic, store: &Records) -> Result<()> {
        let topic_id = provisional.topic_id;
        let clocks = crate::clock::ClockCache::default();
        let mut after: Option<Vec<u8>> = None;
        let mut chunk = CHUNK;
        loop {
            self.hook(Hook::CopyChunk)?;
            // The claim freezes this namespace; validation needs no write-conflict reads.
            let read = self.db.read_tx();
            let copied = self.transaction_bulk(|tx| {
                tx.activation();
                Self::tx_claimed(tx, &self.records, &topic_id, provisional.session)?;
                let mut seen = 0;
                let mut last = None;
                let start = after
                    .clone()
                    .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
                for item in fjall::Readable::range::<Vec<u8>, _>(
                    tx,
                    store,
                    (start, std::ops::Bound::Unbounded),
                )
                .take(chunk)
                {
                    let (key, value) = item.into_inner()?;
                    seen += 1;
                    if copied_record(&key) {
                        if key.len() == 1 + OpId::LEN && key.starts_with(b"m") {
                            Self::validate_meta(&read, store, &value, &clocks)?;
                        }
                        tx.insert(&self.records, &key, value)?;
                    }
                    last = Some(key.to_vec());
                }
                Ok((seen, last))
            });
            let (seen, last) = match copied {
                Err(Error::StorageBuffer { .. }) if chunk > 1 => {
                    chunk = chunk.div_ceil(2);
                    continue;
                }
                result => result?,
            };
            after = last.or(after);
            if seen < chunk {
                return Ok(());
            }
        }
    }

    /// The one transaction that makes the copied history the active topic.
    fn finish_activation(
        &self,
        provisional: &ProvisionalTopic,
        store: &Records,
        expected: &TopicState,
        effects: &AdmissionEffects,
    ) -> Result<()> {
        let topic_id = provisional.topic_id;
        self.transaction(|tx| {
            Self::tx_claimed(tx, &self.records, &topic_id, provisional.session)?;
            if Self::tx_namespace_state(tx, store, &topic_id)?.as_ref() != Some(expected) {
                return Err(Error::AdmissionConflict);
            }
            let heads: BTreeSet<OpId> =
                Self::tx_get(tx, store, Self::key_id(b"h", &topic_id))?.unwrap_or_default();
            let clock: ActorClock =
                Self::tx_get(tx, store, Self::key_id(b"ac", &topic_id))?.unwrap_or_default();
            let fingerprint: Option<[u8; 32]> =
                Self::tx_get(tx, store, Self::key_id(b"fp", &topic_id))?;
            let generation: u64 =
                Self::tx_get(tx, store, Self::key_id(b"mg", &topic_id))?.unwrap_or_default();
            tx.remove(&self.records, Self::key_id(ACTIVATING, &topic_id))?;
            Self::tx_put(tx, &self.records, Self::key_id(b"ts", &topic_id), expected)?;
            Self::tx_put(tx, &self.records, Self::key_id(b"h", &topic_id), &heads)?;
            Self::tx_put(tx, &self.records, Self::key_id(b"ac", &topic_id), &clock)?;
            if let Some(fingerprint) = fingerprint {
                Self::tx_put(
                    tx,
                    &self.records,
                    Self::key_id(b"fp", &topic_id),
                    &fingerprint,
                )?;
            }
            Self::tx_put(
                tx,
                &self.records,
                Self::key_id(b"mg", &topic_id),
                &generation,
            )?;
            for obligation in &effects.sync_obligations {
                let ack: Option<PeerAck> = Self::tx_get(
                    tx,
                    &self.records,
                    Self::ack_key(&topic_id, &obligation.peer_id),
                )?;
                if !ack_covers(ack.as_ref(), Some(expected.genesis), obligation) {
                    Self::tx_put_obligation(tx, &self.records, obligation)?;
                }
            }
            for other in Self::tx_namespaces(tx, &self.records)? {
                if other.provisional.topic_id == topic_id {
                    Self::tx_end_namespace(tx, &self.records, &other)?;
                }
            }
            Ok(())
        })
    }

    /// Rewrite schema 5 namespace records in `tx` with a first revision and
    /// the bytes their keyspace's counters hold.
    pub(super) fn tx_migrate_namespaces(&self, tx: &mut Tx) -> Result<()> {
        let mut legacy = Vec::new();
        for item in fjall::Readable::prefix(tx, &self.records, NAMESPACE) {
            let (key, value) = item.into_inner()?;
            legacy.push((
                key.to_vec(),
                postcard::from_bytes::<LegacyNamespaceRecord>(value.as_ref())?,
            ));
        }
        for (key, record) in legacy {
            let store = self.slot_records(record.slot)?;
            let admitted: u64 = Self::tx_get(tx, &store, ADMITTED_BYTES)?.unwrap_or_default();
            let bytes = admitted + Self::tx_pending_bytes(tx, &store)?;
            let provisional = ProvisionalTopic {
                source: record.source,
                topic_id: record.topic_id,
                genesis: record.genesis,
                session: record.session,
                updated_ms: record.updated_ms,
                activating: record.activating,
                revision: 0,
                bytes,
            };
            Self::tx_put(
                tx,
                &self.records,
                key,
                &NamespaceRecord {
                    provisional,
                    slot: record.slot,
                },
            )?;
        }
        Ok(())
    }
}
