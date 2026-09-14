// SPDX-License-Identifier: MIT OR Apache-2.0
//! Provisional bootstrap namespaces in Fjall. Each namespace takes one slot of
//! a fixed pool of keyspaces, which is cleared before another session reuses it.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{ActorClock, Error, OpId, PeerId, Result, TopicId};

use super::fjall::FjallStorage;
use super::{
    AdmissionEffects, PeerAck, ProvisionalTopic, TopicState, ack_covers, check_namespaces,
};

type Tx = fjall::OptimisticWriteTx;
type Records = fjall::OptimisticTxKeyspace;

/// Namespace record, `bn<source><topic>`. No other key begins with `b`.
const NAMESPACE: &[u8] = b"bn";
/// Slot owner, `bs<slot>` with a big-endian slot number.
const SLOT: &[u8] = b"bs";
/// The durable session counter.
const SESSIONS: &[u8] = b"bc";
/// Activation in progress, `ba<topic>`: copies may sit in the active records.
pub(super) const ACTIVATING: &[u8] = b"ba";
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SlotRecord {
    source: PeerId,
    topic_id: TopicId,
    session: u64,
    /// The session ended; the keyspace is emptied before the slot is reused.
    clearing: bool,
}

fn namespace_key(source: &PeerId, topic_id: &TopicId) -> Vec<u8> {
    [NAMESPACE, source.as_ref(), topic_id.as_ref()].concat()
}

fn slot_key(slot: u32) -> Vec<u8> {
    [SLOT, &slot.to_be_bytes()].concat()
}

/// Whether a namespace key holds a record an active topic keeps: an op, its
/// metadata, a child edge, an actor index or tip, or the topic's op index.
fn copied_record(key: &[u8]) -> bool {
    matches!(
        (key.get(..1), key.get(..2), key.len()),
        (Some(b"o" | b"m"), _, 33) | (_, Some(b"ch" | b"at" | b"to"), 66) | (_, Some(b"as"), 74)
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

    /// The keyspace store of `slot`, applying the namespace byte limit.
    fn slot_store(&self, slot: u32) -> Result<Self> {
        let records = self.db.keyspace(
            &format!("bootstrap-{slot}"),
            fjall::KeyspaceCreateOptions::default,
        )?;
        Ok(self.namespace_view(records))
    }

    fn current_record(&self, provisional: &ProvisionalTopic) -> Result<Option<NamespaceRecord>> {
        let record: Option<NamespaceRecord> =
            self.get(namespace_key(&provisional.source, &provisional.topic_id))?;
        Ok(record.filter(|record| record.provisional.session == provisional.session))
    }

    /// Empty every slot whose session ended, one bounded transaction at a time.
    fn reclaim_slots(&self) -> Result<()> {
        let mut clearing = Vec::new();
        for item in fjall::Readable::prefix(&self.db.read_tx(), &self.records, SLOT) {
            let (key, value) = item.into_inner()?;
            let record: SlotRecord = postcard::from_bytes(value.as_ref())?;
            if record.clearing {
                clearing.push(key.to_vec());
            }
        }
        for key in clearing {
            let slot = key
                .get(SLOT.len()..)
                .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
                .map(u32::from_be_bytes)
                .ok_or_else(|| Error::Storage("corrupt fjall bootstrap slot key".into()))?;
            let store = self.slot_store(slot)?;
            loop {
                let removed = store.transaction(|tx| {
                    let mut keys = Vec::new();
                    for item in fjall::Readable::iter(tx, &store.records).take(CHUNK) {
                        keys.push(item.key()?.to_vec());
                    }
                    for key in &keys {
                        tx.remove(&store.records, key.clone());
                    }
                    Ok(keys.len())
                })?;
                if removed < CHUNK {
                    break;
                }
            }
            self.transaction(|tx| {
                let record: Option<SlotRecord> = Self::tx_get(tx, &self.records, key.as_slice())?;
                if record.is_some_and(|record| record.clearing) {
                    tx.remove(&self.records, key.clone());
                }
                Ok(())
            })?;
        }
        Ok(())
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
        self.reclaim_slots()?;
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
        self.current_record(provisional)?
            .map(|record| self.slot_store(record.slot))
            .transpose()
    }

    pub(super) fn touch_namespace(
        &self,
        provisional: &ProvisionalTopic,
        now_ms: u64,
    ) -> Result<()> {
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

    /// End a namespace's session in `tx`: its record goes and its slot clears.
    fn tx_end_namespace(tx: &mut Tx, records: &Records, record: &NamespaceRecord) -> Result<()> {
        let provisional = &record.provisional;
        tx.remove(
            records,
            namespace_key(&provisional.source, &provisional.topic_id),
        );
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
        if effects
            .sync_obligations
            .iter()
            .any(|obligation| obligation.topic_id != provisional.topic_id)
        {
            return Err(Error::TopicMismatch);
        }
        let store = self.copy_namespace(provisional, expected)?;
        self.finish_activation(provisional, &store, expected, effects)?;
        self.reclaim_slots()
    }

    /// Stop an activation after its copies, as a crash there would.
    #[cfg(test)]
    pub(crate) fn interrupt_activation(&self, provisional: &ProvisionalTopic) {
        let store = self.namespace_store(provisional).unwrap().unwrap();
        let state =
            Self::tx_namespace_state(&self.db.read_tx(), &store.records, &provisional.topic_id)
                .unwrap()
                .unwrap();
        self.copy_namespace(provisional, &state).unwrap();
    }

    /// Mark the namespace activating and copy its records into the active
    /// records in bounded transactions. Returns the namespace store.
    fn copy_namespace(
        &self,
        provisional: &ProvisionalTopic,
        expected: &TopicState,
    ) -> Result<Self> {
        let topic_id = provisional.topic_id;
        let key = namespace_key(&provisional.source, &topic_id);
        let record = self.transaction(|tx| {
            let record = Self::tx_get::<NamespaceRecord>(tx, &self.records, key.as_slice())?
                .filter(|record| record.provisional.session == provisional.session)
                .ok_or(Error::StaleIncarnation)?;
            if fjall::Readable::contains_key(tx, &self.records, Self::key_id(b"ts", &topic_id))? {
                return Err(Error::AdmissionConflict);
            }
            // Checked before any copy, so a stale expectation leaves nothing behind.
            let store = self.slot_store(record.slot)?;
            if Self::tx_namespace_state(tx, &store.records, &topic_id)?.as_ref() != Some(expected) {
                return Err(Error::AdmissionConflict);
            }
            if !record.provisional.activating {
                let mut marked = record.clone();
                marked.provisional.activating = true;
                Self::tx_put(tx, &self.records, key.as_slice(), &marked)?;
                Self::tx_put(
                    tx,
                    &self.records,
                    Self::key_id(ACTIVATING, &topic_id),
                    &provisional.session,
                )?;
            }
            Ok(record)
        })?;
        let store = self.slot_store(record.slot)?;
        // Copies stay invisible until the state record below names the topic.
        let mut after: Option<Vec<u8>> = None;
        loop {
            let (copied, last) = self.transaction(|tx| {
                let mut copied = 0;
                let mut last = None;
                let start = after
                    .clone()
                    .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
                for item in fjall::Readable::range::<Vec<u8>, _>(
                    tx,
                    &store.records,
                    (start, std::ops::Bound::Unbounded),
                )
                .take(CHUNK)
                {
                    let (key, value) = item.into_inner()?;
                    copied += 1;
                    if copied_record(&key) {
                        tx.insert(&self.records, key.to_vec(), value);
                    }
                    last = Some(key.to_vec());
                }
                Ok((copied, last))
            })?;
            after = last.or(after);
            if copied < CHUNK {
                return Ok(store);
            }
        }
    }

    /// The one transaction that makes the copied history the active topic.
    fn finish_activation(
        &self,
        provisional: &ProvisionalTopic,
        store: &Self,
        expected: &TopicState,
        effects: &AdmissionEffects,
    ) -> Result<()> {
        let topic_id = provisional.topic_id;
        let key = namespace_key(&provisional.source, &topic_id);
        self.transaction(|tx| {
            let current = Self::tx_get::<NamespaceRecord>(tx, &self.records, key.as_slice())?
                .filter(|current| current.provisional.session == provisional.session)
                .ok_or(Error::StaleIncarnation)?;
            if fjall::Readable::contains_key(tx, &self.records, Self::key_id(b"ts", &topic_id))? {
                return Err(Error::AdmissionConflict);
            }
            if Self::tx_namespace_state(tx, &store.records, &topic_id)?.as_ref() != Some(expected) {
                return Err(Error::AdmissionConflict);
            }
            let heads: BTreeSet<OpId> =
                Self::tx_get(tx, &store.records, Self::key_id(b"h", &topic_id))?
                    .unwrap_or_default();
            let clock: ActorClock =
                Self::tx_get(tx, &store.records, Self::key_id(b"ac", &topic_id))?
                    .unwrap_or_default();
            let fingerprint: Option<[u8; 32]> =
                Self::tx_get(tx, &store.records, Self::key_id(b"fp", &topic_id))?;
            let generation: u64 = Self::tx_get(tx, &store.records, Self::key_id(b"mg", &topic_id))?
                .unwrap_or_default();
            tx.remove(&self.records, Self::key_id(ACTIVATING, &topic_id));
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
            debug_assert!(current.provisional.activating);
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
            let store = self.slot_store(record.slot)?.records;
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
