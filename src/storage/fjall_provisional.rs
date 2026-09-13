// SPDX-License-Identifier: MIT OR Apache-2.0
//! Provisional bootstrap namespaces in Fjall. Each namespace takes one slot of
//! a fixed pool of keyspaces, which is cleared before another session reuses it.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{Error, OpId, PeerId, Result, TopicId};

use super::fjall::FjallStorage;
use super::{ProvisionalTopic, check_namespaces};

type Tx = fjall::OptimisticWriteTx;
type Records = fjall::OptimisticTxKeyspace;

/// Namespace record, `bn<source><topic>`. No other key begins with `b`.
const NAMESPACE: &[u8] = b"bn";
/// Slot owner, `bs<slot>` with a big-endian slot number.
const SLOT: &[u8] = b"bs";
/// The durable session counter.
const SESSIONS: &[u8] = b"bc";
/// Serialized bytes of ops admitted into a namespace keyspace.
pub(super) const ADMITTED_BYTES: &[u8] = b"nb";
/// Records one copy or clearing transaction moves.
const CHUNK: usize = 4096;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NamespaceRecord {
    provisional: ProvisionalTopic,
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
            if record.provisional.session != provisional.session || record.provisional.activating {
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
}
