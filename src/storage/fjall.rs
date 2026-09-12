// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{
    ActorClock, ActorId, Error, EvictionKey, Op, OpId, PeerId, Result, TopicEviction, TopicId,
    TopicInfo,
};

use super::{
    AckCommit, AdmittedBatch, CounterSnapshot, MAX_PENDING_EVICTIONS, ObligationTarget, OpMeta,
    PeerAck, StagedTopic, Storage, StorageCounters, SyncObligation, SyncPeerStatus,
    SyncStatusUpdate, TopicState, TopicView, ack_commit, ack_covers, ack_reached_op,
    apply_status_update, branch_matches, ensure_deps_resolvable, journalled_eviction,
    merged_obligation, merged_peer_ack, new_peer_status, peer_departed, pending_op_bytes,
    settled_obligation, stored_ack_dominates, topic_fingerprint_for, validate_batch,
    validate_heads,
};

#[cfg(feature = "fjall")]
#[derive(Clone)]
pub struct FjallStorage {
    pub(super) db: fjall::OptimisticTxDatabase,
    pub(super) records: fjall::OptimisticTxKeyspace,
    persist_mode: fjall::PersistMode,
    pub(super) counters: std::sync::Arc<StorageCounters>,
    /// Key a test rewrites before every single-attempt commit, forcing a conflict.
    #[cfg(test)]
    conflict_key: std::sync::Arc<std::sync::Mutex<Option<Vec<u8>>>>,
}

#[cfg(feature = "fjall")]
const FJALL_SCHEMA_VERSION: u32 = 4;
/// Eviction journal records. No other keyspace begins with `e`, so this is the
/// whole prefix: unlike `ob`, it cannot be shadowed by a single-letter prefix.
#[cfg(feature = "fjall")]
const EVICTION_PREFIX: &[u8] = b"ev";
#[cfg(feature = "fjall")]
const SEALED_TOPIC_PREFIX: &[u8] = b"se";
#[cfg(feature = "fjall")]
const FJALL_SCHEMA_VERSION_KEY: &[u8] = b"sv";
/// Stored acknowledgements, keyed on `ak<topic><peer>` since schema 3. No
/// other key starts with `ak`, so this is the whole prefix.
#[cfg(feature = "fjall")]
const PEER_ACK_PREFIX: &[u8] = b"ak";
/// Sync obligations, keyed on `ob<topic><peer><kind>` since schema 3. An op
/// key `o<id>` with an id starting with `b` shares the prefix, so bare scans
/// check the key length.
#[cfg(feature = "fjall")]
const OBLIGATION_PREFIX: &[u8] = b"ob";
#[cfg(feature = "fjall")]
const OBLIGATION_KEY_LEN: usize = 2 + TopicId::LEN + PeerId::LEN + 1;
/// Buffered pending payload bytes, in total and per authenticated source.
#[cfg(feature = "fjall")]
const PENDING_BYTES_KEY: &[u8] = b"pb";
#[cfg(feature = "fjall")]
const PENDING_SOURCE_BYTES_PREFIX: &[u8] = b"pq";
/// Pending payload records, keyed on `po<op id>`.
#[cfg(feature = "fjall")]
const PENDING_OP_PREFIX: &[u8] = b"po";
/// Destructive data epoch per topic, keyed on `ep<topic id>`. A reset keeps
/// and advances it.
#[cfg(feature = "fjall")]
const TOPIC_EPOCH_PREFIX: &[u8] = b"ep";
/// The durable attempt epoch, one `u64` under exactly this key.
#[cfg(feature = "fjall")]
const ATTEMPT_EPOCH_KEY: &[u8] = b"ae";

/// Schema 1 layout of a stored acknowledgement, which did not name the branch
/// it certified. Kept only to read those records during the upgrade; postcard
/// is not self-describing, so the old bytes need the old field order.
#[cfg(feature = "fjall")]
#[derive(Deserialize)]
struct LegacyPeerAck {
    peer_id: PeerId,
    topic_id: TopicId,
    heads: BTreeSet<OpId>,
    clock: ActorClock,
}

/// Sync status layout before attempt identities. Postcard is not
/// self-describing, so such records decode only with this shape.
#[cfg(feature = "fjall")]
#[derive(Deserialize)]
struct LegacyPeerStatus {
    peer_id: PeerId,
    topic_id: TopicId,
    state: super::SyncPeerState,
    pending_obligations: usize,
    failed_attempts: u64,
    successful_attempts: u64,
    last_attempt_ms: Option<u64>,
    last_success_ms: Option<u64>,
    last_error: Option<String>,
}

/// Decode a status record in the current or the earlier layout.
#[cfg(feature = "fjall")]
fn decode_status(bytes: &[u8]) -> Result<SyncPeerStatus> {
    if let Ok(status) = postcard::from_bytes(bytes) {
        return Ok(status);
    }
    let legacy: LegacyPeerStatus = postcard::from_bytes(bytes)?;
    Ok(SyncPeerStatus {
        peer_id: legacy.peer_id,
        topic_id: legacy.topic_id,
        state: legacy.state,
        pending_obligations: legacy.pending_obligations,
        failed_attempts: legacy.failed_attempts,
        successful_attempts: legacy.successful_attempts,
        last_attempt_ms: legacy.last_attempt_ms,
        last_success_ms: legacy.last_success_ms,
        last_error: legacy.last_error,
        latest_attempt: None,
        recent_attempts: Vec::new(),
    })
}

/// Schema 1 and 2 layout of a sync obligation, which kept resolved and
/// unresolved wants in one shape told apart only by empty fields.
#[cfg(feature = "fjall")]
#[derive(Deserialize)]
struct LegacyObligation {
    peer_id: PeerId,
    topic_id: TopicId,
    op_ids: BTreeSet<OpId>,
    target_clock: ActorClock,
}

#[cfg(feature = "fjall")]
impl FjallStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_persist_mode(path, fjall::PersistMode::SyncAll)
    }

    /// Open Fjall storage with an explicit transaction persist mode.
    ///
    /// `SyncAll` preserves the historical fully durable behavior. `Buffer`
    /// avoids a foreground fsync on every Irokle transaction and is useful when
    /// callers provide their own durability boundary.
    pub fn open_with_persist_mode(
        path: impl AsRef<Path>,
        persist_mode: fjall::PersistMode,
    ) -> Result<Self> {
        let db = fjall::OptimisticTxDatabase::builder(path).open()?;
        Self::from_database_with_persist_mode(db, persist_mode)
    }

    pub fn from_database(db: fjall::OptimisticTxDatabase) -> Result<Self> {
        Self::from_database_with_persist_mode(db, fjall::PersistMode::SyncAll)
    }

    /// Build Fjall storage from an existing database with an explicit
    /// transaction persist mode.
    pub fn from_database_with_persist_mode(
        db: fjall::OptimisticTxDatabase,
        persist_mode: fjall::PersistMode,
    ) -> Result<Self> {
        let storage = Self {
            records: db.keyspace("records", fjall::KeyspaceCreateOptions::default)?,
            db,
            persist_mode,
            counters: Default::default(),
            #[cfg(test)]
            conflict_key: Default::default(),
        };
        storage.ensure_schema_version()?;
        Ok(storage)
    }

    /// Work this store and its clones performed so far.
    pub fn counters(&self) -> CounterSnapshot {
        self.counters.snapshot()
    }

    /// Flush buffered transactions with the requested durability.
    pub fn persist(&self, persist_mode: fjall::PersistMode) -> Result<()> {
        self.db.persist(persist_mode)?;
        Ok(())
    }

    fn ensure_schema_version(&self) -> Result<()> {
        match self.get::<u32>(FJALL_SCHEMA_VERSION_KEY)? {
            Some(FJALL_SCHEMA_VERSION) => Ok(()),
            Some(1) => {
                self.migrate_to_schema_two()?;
                self.migrate_to_schema_three()?;
                self.migrate_to_schema_four()
            }
            Some(2) => {
                self.migrate_to_schema_three()?;
                self.migrate_to_schema_four()
            }
            Some(3) => self.migrate_to_schema_four(),
            Some(version) => Err(Error::Storage(format!(
                "unsupported fjall schema version {version}"
            ))),
            None => self.put(FJALL_SCHEMA_VERSION_KEY, &FJALL_SCHEMA_VERSION),
        }
    }

    /// Upgrade a schema 1 database. Two things change:
    ///
    /// Acknowledgements move into the current layout, which names the
    /// incarnation each one certifies. Their branch was never recorded, so they
    /// migrate as uncertified: the records and their clocks are preserved, but
    /// they prove nothing until the peer acknowledges the current branch.
    ///
    /// Buffered pending payloads gain byte counters, seeded by measuring the
    /// records already stored so the budgets describe the whole pool rather
    /// than only what arrives afterwards.
    ///
    /// One transaction carries every rewrite and the version bump, so an
    /// interrupted upgrade reopens at schema 1 and retries from the start.
    fn migrate_to_schema_two(&self) -> Result<()> {
        self.transaction(|tx| {
            // A concurrent facade may have finished the upgrade already.
            if Self::tx_get::<u32>(tx, &self.records, FJALL_SCHEMA_VERSION_KEY)? != Some(1) {
                return Ok(());
            }
            let mut migrated = Vec::new();
            for item in fjall::Readable::prefix(tx, &self.records, PEER_ACK_PREFIX) {
                let (key, value) = item.into_inner()?;
                let legacy: LegacyPeerAck = postcard::from_bytes(value.as_ref())?;
                migrated.push((
                    key.to_vec(),
                    PeerAck {
                        peer_id: legacy.peer_id,
                        topic_id: legacy.topic_id,
                        genesis: None,
                        heads: legacy.heads,
                        clock: legacy.clock,
                    },
                ));
            }
            for (key, ack) in &migrated {
                Self::tx_put(tx, &self.records, key, ack)?;
            }

            let mut total_bytes = 0_u64;
            let mut source_bytes: BTreeMap<PeerId, u64> = BTreeMap::new();
            for item in fjall::Readable::prefix(tx, &self.records, PENDING_OP_PREFIX) {
                let (key, value) = item.into_inner()?;
                if key.len() != PENDING_OP_PREFIX.len() + OpId::LEN {
                    continue;
                }
                let (source_peer, op, _) =
                    postcard::from_bytes::<(PeerId, Op, OpMeta)>(value.as_ref())?;
                let bytes = pending_op_bytes(&op)? as u64;
                total_bytes = total_bytes.saturating_add(bytes);
                *source_bytes.entry(source_peer).or_default() += bytes;
            }
            if total_bytes > 0 {
                Self::tx_put(tx, &self.records, PENDING_BYTES_KEY, &total_bytes)?;
            }
            for (source_peer, bytes) in source_bytes {
                Self::tx_put(
                    tx,
                    &self.records,
                    [PENDING_SOURCE_BYTES_PREFIX, source_peer.as_ref()].concat(),
                    &bytes,
                )?;
            }

            Self::tx_put(tx, &self.records, FJALL_SCHEMA_VERSION_KEY, &2_u32)?;
            Ok(())
        })
    }

    /// Upgrade schema 2 in one transaction that rechecks the version. Acks and
    /// obligations move to topic-first keys; a legacy clock becomes a clock
    /// target, ids alone a repair want, and a record with neither is dropped.
    fn migrate_to_schema_three(&self) -> Result<()> {
        self.transaction(|tx| {
            if Self::tx_get::<u32>(tx, &self.records, FJALL_SCHEMA_VERSION_KEY)? != Some(2) {
                return Ok(());
            }
            let mut acks = Vec::new();
            for item in fjall::Readable::prefix(tx, &self.records, PEER_ACK_PREFIX) {
                let (key, value) = item.into_inner()?;
                acks.push((
                    key.to_vec(),
                    postcard::from_bytes::<PeerAck>(value.as_ref())?,
                ));
            }
            for (key, ack) in acks {
                tx.remove(&self.records, key);
                Self::tx_put(
                    tx,
                    &self.records,
                    Self::ack_key(&ack.topic_id, &ack.peer_id),
                    &ack,
                )?;
            }

            let mut obligations = BTreeMap::<Vec<u8>, SyncObligation>::new();
            let mut legacy_keys = Vec::new();
            for item in fjall::Readable::prefix(tx, &self.records, OBLIGATION_PREFIX) {
                let (key, value) = item.into_inner()?;
                if Self::is_op_record_key(key.as_ref()) {
                    continue;
                }
                legacy_keys.push(key.to_vec());
                let legacy: LegacyObligation = postcard::from_bytes(value.as_ref())?;
                let obligation = if !legacy.target_clock.is_empty() {
                    SyncObligation::clock(legacy.peer_id, legacy.topic_id, legacy.target_clock)
                } else if !legacy.op_ids.is_empty() {
                    SyncObligation::repair(legacy.peer_id, legacy.topic_id, legacy.op_ids)
                } else {
                    continue;
                };
                // Legacy wants are kept whole even past the repair limit.
                let key = Self::obligation_key(&obligation);
                let merged = match (obligations.remove(&key), &obligation.target) {
                    (Some(mut stored), ObligationTarget::Repair(ids)) => {
                        if let ObligationTarget::Repair(stored_ids) = &mut stored.target {
                            stored_ids.extend(ids.iter().copied());
                        }
                        stored
                    }
                    (stored, _) => merged_obligation(stored, &obligation)?,
                };
                obligations.insert(key, merged);
            }
            for key in legacy_keys {
                tx.remove(&self.records, key);
            }
            for (key, obligation) in obligations {
                Self::tx_put(tx, &self.records, key, &obligation)?;
            }

            let mut total_bytes = 0_u64;
            let mut source_bytes: BTreeMap<PeerId, u64> = BTreeMap::new();
            for item in fjall::Readable::prefix(tx, &self.records, PENDING_OP_PREFIX) {
                let (key, value) = item.into_inner()?;
                if key.len() != PENDING_OP_PREFIX.len() + OpId::LEN {
                    continue;
                }
                let (source_peer, op, _) =
                    postcard::from_bytes::<(PeerId, Op, OpMeta)>(value.as_ref())?;
                let bytes = pending_op_bytes(&op)? as u64;
                total_bytes += bytes;
                *source_bytes.entry(source_peer).or_default() += bytes;
            }
            Self::tx_remove_prefix(tx, &self.records, PENDING_SOURCE_BYTES_PREFIX)?;
            tx.remove(&self.records, PENDING_BYTES_KEY.to_vec());
            if total_bytes > 0 {
                Self::tx_put(tx, &self.records, PENDING_BYTES_KEY, &total_bytes)?;
            }
            for (source_peer, bytes) in source_bytes {
                Self::tx_put(
                    tx,
                    &self.records,
                    [PENDING_SOURCE_BYTES_PREFIX, source_peer.as_ref()].concat(),
                    &bytes,
                )?;
            }

            Self::tx_put(tx, &self.records, FJALL_SCHEMA_VERSION_KEY, &3_u32)?;
            Ok(())
        })
    }

    /// Upgrade schema 3 in one transaction that rechecks the version: each
    /// buffered op splits into a small record and its payload, with topic,
    /// waiter and ready indexes and usage counters rebuilt from the records.
    fn migrate_to_schema_four(&self) -> Result<()> {
        self.transaction(|tx| {
            if Self::tx_get::<u32>(tx, &self.records, FJALL_SCHEMA_VERSION_KEY)? != Some(3) {
                return Ok(());
            }
            let mut legacy = Vec::new();
            for item in fjall::Readable::prefix(tx, &self.records, PENDING_OP_PREFIX) {
                let (key, value) = item.into_inner()?;
                if key.len() == PENDING_OP_PREFIX.len() + OpId::LEN {
                    legacy.push(postcard::from_bytes::<(PeerId, Op, OpMeta)>(
                        value.as_ref(),
                    )?);
                }
            }
            for prefix in [
                PENDING_OP_PREFIX,
                b"pn",
                PENDING_BYTES_KEY,
                PENDING_SOURCE_BYTES_PREFIX,
                b"ps",
                b"pw",
                b"wn",
            ] {
                Self::tx_remove_prefix(tx, &self.records, prefix)?;
            }
            for (source_peer, op, meta) in legacy {
                let mut missing = BTreeSet::new();
                for dep in meta.missing_deps {
                    if !fjall::Readable::contains_key(tx, &self.records, Self::key_id(b"o", &dep))?
                        || !fjall::Readable::contains_key(
                            tx,
                            &self.records,
                            Self::key_id(b"m", &dep),
                        )?
                    {
                        missing.insert(dep);
                    }
                }
                Self::tx_import_pending(tx, &self.records, source_peer, &op, missing)?;
            }
            Self::tx_put(
                tx,
                &self.records,
                FJALL_SCHEMA_VERSION_KEY,
                &FJALL_SCHEMA_VERSION,
            )?;
            Ok(())
        })
    }

    fn transaction<R>(
        &self,
        mut f: impl FnMut(&mut fjall::OptimisticWriteTx) -> Result<R>,
    ) -> Result<R> {
        for _ in 0..64 {
            self.counters.count_attempt();
            let mut tx = self.db.write_tx()?.durability(Some(self.persist_mode));
            let result = f(&mut tx)?;
            match tx.commit()? {
                Ok(()) => return Ok(result),
                Err(_) => continue,
            }
        }
        Err(Error::AdmissionConflict)
    }

    /// One attempt that reports a commit conflict as `AdmissionConflict`, for
    /// admission writes whose caller owns the whole retry budget.
    fn transaction_once<R>(
        &self,
        f: impl FnOnce(&mut fjall::OptimisticWriteTx) -> Result<R>,
    ) -> Result<R> {
        self.counters.count_attempt();
        let mut tx = self.db.write_tx()?.durability(Some(self.persist_mode));
        let result = f(&mut tx)?;
        #[cfg(test)]
        self.race_commit()?;
        match tx.commit()? {
            Ok(()) => Ok(result),
            Err(_) => Err(Error::AdmissionConflict),
        }
    }

    /// Make every later single-attempt transaction that reads `topic_id`'s
    /// heads lose its commit to a concurrent rewrite of them.
    #[cfg(test)]
    pub(crate) fn race_heads(&self, topic_id: &TopicId) {
        *self.conflict_key.lock().unwrap() = Some(Self::key_id(b"h", topic_id));
    }

    #[cfg(test)]
    fn race_commit(&self) -> Result<()> {
        let Some(key) = self.conflict_key.lock().unwrap().clone() else {
            return Ok(());
        };
        let mut tx = self.db.write_tx()?;
        if let Some(value) = fjall::Readable::get(&tx, &self.records, key.as_slice())? {
            tx.insert(&self.records, key, value);
        }
        tx.commit()?
            .map_err(|_| Error::Storage("racing commit conflicted".into()))
    }

    pub(super) fn key_id(prefix: &[u8], id: &impl AsRef<[u8]>) -> Vec<u8> {
        [prefix, id.as_ref()].concat()
    }

    // An op id beginning with `b` makes its `o<id>` key match the `ob` scan prefix.
    fn is_op_record_key(key: &[u8]) -> bool {
        key.len() == b"o".len() + OpId::LEN && key.starts_with(b"o")
    }

    fn put<T: Serialize>(&self, key: impl AsRef<[u8]>, value: &T) -> Result<()> {
        let key = key.as_ref().to_vec();
        let value = postcard::to_allocvec(value)?;
        self.transaction(|tx| {
            tx.insert(&self.records, key.clone(), value.clone());
            Ok(())
        })
    }

    pub(super) fn tx_put<T: Serialize>(
        tx: &mut fjall::OptimisticWriteTx,
        records: &fjall::OptimisticTxKeyspace,
        key: impl AsRef<[u8]>,
        value: &T,
    ) -> Result<()> {
        tx.insert(
            records,
            key.as_ref().to_vec(),
            postcard::to_allocvec(value)?,
        );
        Ok(())
    }

    pub(super) fn tx_get<T: for<'de> Deserialize<'de>>(
        tx: &impl fjall::Readable,
        records: &fjall::OptimisticTxKeyspace,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<T>> {
        Ok(fjall::Readable::get(tx, records, key.as_ref())?
            .map(|v| postcard::from_bytes(v.as_ref()))
            .transpose()?)
    }

    /// Outstanding journal records, counted no further than the cap the caller
    /// compares against.
    fn tx_eviction_count(
        tx: &fjall::OptimisticWriteTx,
        records: &fjall::OptimisticTxKeyspace,
    ) -> usize {
        fjall::Readable::prefix(tx, records, EVICTION_PREFIX)
            .take(MAX_PENDING_EVICTIONS)
            .count()
    }

    fn ack_key(topic_id: &TopicId, peer_id: &PeerId) -> Vec<u8> {
        [PEER_ACK_PREFIX, topic_id.as_ref(), peer_id.as_ref()].concat()
    }

    /// Obligations of one peer on one topic share this prefix.
    fn obligation_prefix(topic_id: &TopicId, peer_id: &PeerId) -> Vec<u8> {
        [OBLIGATION_PREFIX, topic_id.as_ref(), peer_id.as_ref()].concat()
    }

    /// One key per kind: an ordinary target coalesces into the clock record
    /// and explicit repair wants into the repair record.
    fn obligation_key(obligation: &SyncObligation) -> Vec<u8> {
        let kind = match obligation.target {
            ObligationTarget::Clock(_) => b'c',
            ObligationTarget::Repair(_) => b'r',
        };
        let mut key = Self::obligation_prefix(&obligation.topic_id, &obligation.peer_id);
        key.push(kind);
        key
    }

    /// Delete every key under `prefix` and report how many there were.
    pub(super) fn tx_remove_prefix(
        tx: &mut fjall::OptimisticWriteTx,
        records: &fjall::OptimisticTxKeyspace,
        prefix: &[u8],
    ) -> Result<usize> {
        let mut keys = Vec::new();
        for item in fjall::Readable::prefix(tx, records, prefix) {
            keys.push(item.key()?.to_vec());
        }
        let removed = keys.len();
        for key in keys {
            tx.remove(records, key);
        }
        Ok(removed)
    }

    fn tx_put_obligation(
        tx: &mut fjall::OptimisticWriteTx,
        records: &fjall::OptimisticTxKeyspace,
        obligation: &SyncObligation,
    ) -> Result<()> {
        if obligation.is_empty() {
            return Ok(());
        }
        let key = Self::obligation_key(obligation);
        let existing = Self::tx_get::<SyncObligation>(tx, records, key.as_slice())?;
        let merged = merged_obligation(existing.clone(), obligation)?;
        if existing.as_ref() == Some(&merged) {
            return Ok(());
        }
        Self::tx_put(tx, records, key, &merged)
    }

    fn get<T: for<'de> Deserialize<'de>>(&self, key: impl AsRef<[u8]>) -> Result<Option<T>> {
        Ok(self
            .records
            .get(key)?
            .map(|v| postcard::from_bytes(v.as_ref()))
            .transpose()?)
    }

    /// How `ack` may commit against the topic as this transaction sees it. The
    /// outer result is a backend failure; the inner one is the per-ack verdict,
    /// which a batch records without abandoning the other acks.
    fn tx_ack_commit(
        tx: &mut fjall::OptimisticWriteTx,
        records: &fjall::OptimisticTxKeyspace,
        ack: &PeerAck,
    ) -> Result<Result<AckCommit>> {
        let state: Option<TopicState> =
            Self::tx_get(tx, records, Self::key_id(b"ts", &ack.topic_id))?;
        Ok(ack_commit(state.as_ref(), ack))
    }

    fn tx_apply_peer_ack(
        tx: &mut fjall::OptimisticWriteTx,
        records: &fjall::OptimisticTxKeyspace,
        ack: &PeerAck,
        commit: AckCommit,
    ) -> Result<usize> {
        let ack_key = Self::ack_key(&ack.topic_id, &ack.peer_id);
        let effective_ack = match Self::tx_get::<PeerAck>(tx, records, ack_key.as_slice())? {
            Some(existing) if stored_ack_dominates(&existing, ack) => existing,
            Some(existing) => {
                let merged = merged_peer_ack(&existing, ack);
                Self::tx_put(tx, records, ack_key, &merged)?;
                merged
            }
            None => {
                Self::tx_put(tx, records, ack_key, ack)?;
                ack.clone()
            }
        };
        if commit == AckCommit::Retain {
            return Ok(0);
        }
        clear_satisfied_tx(tx, records, &effective_ack)
    }

    fn op_id_from_key(key: &[u8], offset: usize) -> Result<OpId> {
        let bytes = key
            .get(offset..offset + OpId::LEN)
            .ok_or_else(|| Error::Storage("corrupt fjall op id index key".into()))?;
        let mut out = [0_u8; OpId::LEN];
        out.copy_from_slice(bytes);
        Ok(OpId::from_bytes(out))
    }

    fn tx_admit_batch(
        &self,
        tx: &mut fjall::OptimisticWriteTx,
        batch: &AdmittedBatch,
    ) -> Result<()> {
        validate_batch(batch)?;
        let AdmittedBatch {
            topic_id,
            expected_heads,
            expected_topic_state,
            entries,
            heads,
            topic_state,
            effects,
        } = batch;
        let topic_id = *topic_id;
        {
            let current_heads: BTreeSet<OpId> =
                Self::tx_get(tx, &self.records, Self::key_id(b"h", &topic_id))?.unwrap_or_default();
            if current_heads != *expected_heads {
                return Err(Error::AdmissionConflict);
            }
            let current_topic_state: Option<TopicState> =
                Self::tx_get::<TopicState>(tx, &self.records, Self::key_id(b"ts", &topic_id))?.map(
                    |mut state| {
                        state.heads = current_heads.clone();
                        state
                    },
                );
            if current_topic_state.as_ref() != expected_topic_state.as_ref() {
                return Err(Error::AdmissionConflict);
            }

            let mut actor_tips = BTreeMap::new();
            let mut new_entries = Vec::new();
            let mut accounted_entries = BTreeSet::new();
            for (op, meta) in entries {
                if meta.topic_id != topic_id {
                    return Err(Error::TopicMismatch);
                }
                let has_op =
                    match Self::tx_get::<Op>(tx, &self.records, Self::key_id(b"o", &op.id))? {
                        Some(existing) if existing != *op => {
                            return Err(Error::Storage("op id collision with different op".into()));
                        }
                        Some(_) => true,
                        None => false,
                    };
                let has_meta =
                    Self::tx_get::<OpMeta>(tx, &self.records, Self::key_id(b"m", &op.id))?
                        .is_some();
                if has_op && has_meta {
                    accounted_entries.insert(op.id);
                    continue;
                }
                let indexed = Self::tx_get::<OpId>(
                    tx,
                    &self.records,
                    [
                        b"as".as_slice(),
                        meta.topic_id.as_ref(),
                        meta.actor_id.as_ref(),
                        &meta.actor_seq.to_be_bytes(),
                    ]
                    .concat(),
                )?;
                if let Some(existing) = indexed
                    && existing != op.id
                {
                    return Err(Error::ActorFork);
                }
                let has_children = fjall::Readable::prefix(
                    tx,
                    &self.records,
                    [b"ch".as_slice(), op.id.as_ref()].concat(),
                )
                .next()
                .is_some();
                // Refilling an id the chain already accounts for is not an
                // append: its actor position is already recorded, so the checks
                // below cannot apply.
                if has_op || has_meta || indexed == Some(op.id) || has_children {
                    accounted_entries.insert(op.id);
                    new_entries.push((op.clone(), meta.clone()));
                    continue;
                }
                let tip =
                    actor_tips
                        .get(&(meta.topic_id, meta.actor_id))
                        .copied()
                        .or(Self::tx_get::<(u64, OpId)>(
                            tx,
                            &self.records,
                            [
                                b"at".as_slice(),
                                meta.topic_id.as_ref(),
                                meta.actor_id.as_ref(),
                            ]
                            .concat(),
                        )?);
                match tip {
                    Some((seq, id)) => {
                        let expected = seq.checked_add(1).ok_or(Error::InvalidOpId)?;
                        if meta.actor_seq != expected {
                            return Err(Error::ActorSeqGap {
                                expected,
                                actual: meta.actor_seq,
                            });
                        }
                        if meta.actor_prev != Some(id) {
                            return Err(Error::ActorPrevMismatch);
                        }
                    }
                    None => {
                        if meta.actor_seq != 1 {
                            return Err(Error::ActorSeqGap {
                                expected: 1,
                                actual: meta.actor_seq,
                            });
                        }
                        if meta.actor_prev.is_some() {
                            return Err(Error::ActorPrevMismatch);
                        }
                    }
                }
                actor_tips.insert((meta.topic_id, meta.actor_id), (meta.actor_seq, op.id));
                new_entries.push((op.clone(), meta.clone()));
            }

            validate_heads(batch, |meta| Ok(accounted_entries.contains(&meta.id)))?;
            ensure_deps_resolvable(&new_entries, |dep| {
                Ok(
                    Self::tx_get::<Op>(tx, &self.records, Self::key_id(b"o", dep))?.is_some()
                        && Self::tx_get::<OpMeta>(tx, &self.records, Self::key_id(b"m", dep))?
                            .is_some(),
                )
            })?;

            let mut clock: ActorClock =
                Self::tx_get(tx, &self.records, Self::key_id(b"ac", &topic_id))?
                    .unwrap_or_default();
            let mut max_generation: u64 =
                Self::tx_get(tx, &self.records, Self::key_id(b"mg", &topic_id))?
                    .unwrap_or_default();
            for (op, meta) in new_entries {
                Self::tx_put(tx, &self.records, Self::key_id(b"o", &op.id), &op)?;
                Self::tx_put(tx, &self.records, Self::key_id(b"m", &meta.id), &meta)?;
                Self::tx_put(
                    tx,
                    &self.records,
                    [b"to".as_slice(), meta.topic_id.as_ref(), op.id.as_ref()].concat(),
                    &(),
                )?;
                for dep in &meta.deps {
                    Self::tx_put(
                        tx,
                        &self.records,
                        [b"ch".as_slice(), dep.as_ref(), op.id.as_ref()].concat(),
                        &(),
                    )?;
                }
                Self::tx_put(
                    tx,
                    &self.records,
                    [
                        b"as".as_slice(),
                        meta.topic_id.as_ref(),
                        meta.actor_id.as_ref(),
                        &meta.actor_seq.to_be_bytes(),
                    ]
                    .concat(),
                    &op.id,
                )?;
                // A refilled op sits behind the tip, so the tip only advances.
                let tip_key = [
                    b"at".as_slice(),
                    meta.topic_id.as_ref(),
                    meta.actor_id.as_ref(),
                ]
                .concat();
                let stored_tip: Option<(u64, OpId)> =
                    Self::tx_get(tx, &self.records, tip_key.as_slice())?;
                if stored_tip.is_none_or(|(seq, _)| seq < meta.actor_seq) {
                    Self::tx_put(
                        tx,
                        &self.records,
                        tip_key.as_slice(),
                        &(meta.actor_seq, op.id),
                    )?;
                }
                // Admission drops the buffered copy through the same funnel as
                // every other removal, so the pending counts and byte budgets
                // are released in exactly one place.
                Self::tx_remove_pending_op(tx, &self.records, &op.id)?;
                Self::tx_settle_waiters(tx, &self.records, &op.id)?;
                clock.observe(meta.actor_id, meta.actor_seq);
                max_generation = max_generation.max(meta.generation);
            }
            Self::tx_put(tx, &self.records, Self::key_id(b"ac", &topic_id), &clock)?;
            Self::tx_put(tx, &self.records, Self::key_id(b"h", &topic_id), heads)?;
            Self::tx_put(
                tx,
                &self.records,
                Self::key_id(b"fp", &topic_id),
                &topic_fingerprint_for(heads, &clock)?,
            )?;
            Self::tx_put(
                tx,
                &self.records,
                Self::key_id(b"mg", &topic_id),
                &max_generation,
            )?;
            if let Some(state) = topic_state {
                Self::tx_put(
                    tx,
                    &self.records,
                    Self::key_id(b"ts", &state.topic_id),
                    state,
                )?;
            }
            let genesis = topic_state
                .as_ref()
                .or(expected_topic_state.as_ref())
                .map(|state| state.genesis);
            for obligation in &effects.sync_obligations {
                if obligation.topic_id != topic_id {
                    return Err(Error::TopicMismatch);
                }
                let ack: Option<PeerAck> = Self::tx_get(
                    tx,
                    &self.records,
                    Self::ack_key(&topic_id, &obligation.peer_id),
                )?;
                if !ack_covers(ack.as_ref(), genesis, obligation) {
                    Self::tx_put_obligation(tx, &self.records, obligation)?;
                }
            }
            if let (Some(previous), Some(state)) = (expected_topic_state, topic_state) {
                for peer in previous.members.difference(&state.members) {
                    Self::tx_remove_prefix(
                        tx,
                        &self.records,
                        &Self::obligation_prefix(&topic_id, peer),
                    )?;
                    tx.remove(
                        &self.records,
                        [b"ss".as_slice(), topic_id.as_ref(), peer.as_ref()].concat(),
                    );
                    tx.remove(&self.records, Self::ack_key(&topic_id, peer));
                }
            }
            Ok(())
        }
    }

    /// Erase a stored op record, leaving its metadata and indexes behind. Tests
    /// use this to build a store that is already durably inconsistent.
    #[cfg(test)]
    pub(crate) fn drop_op_record(&self, id: &OpId) {
        self.transaction(|tx| {
            tx.remove(&self.records, Self::key_id(b"o", id));
            Ok(())
        })
        .expect("fjall drop op record");
    }

    /// Erase a stored metadata record, leaving the op and indexes behind.
    #[cfg(test)]
    pub(crate) fn drop_meta_record(&self, id: &OpId) {
        self.transaction(|tx| {
            tx.remove(&self.records, Self::key_id(b"m", id));
            Ok(())
        })
        .expect("fjall drop meta record");
    }

    /// Store both records and the topic/child indexes while leaving heads and
    /// topic state alone, so the op is admitted yet reachable from no head.
    #[cfg(test)]
    pub(crate) fn orphan_op(&self, op: &Op, meta: &OpMeta) {
        self.transaction(|tx| {
            Self::tx_put(tx, &self.records, Self::key_id(b"o", &op.id), op)?;
            Self::tx_put(tx, &self.records, Self::key_id(b"m", &op.id), meta)?;
            Self::tx_put(
                tx,
                &self.records,
                [b"to".as_slice(), meta.topic_id.as_ref(), op.id.as_ref()].concat(),
                &(),
            )?;
            for dep in &meta.deps {
                Self::tx_put(
                    tx,
                    &self.records,
                    [b"ch".as_slice(), dep.as_ref(), op.id.as_ref()].concat(),
                    &(),
                )?;
            }
            Self::tx_put(
                tx,
                &self.records,
                [
                    b"as".as_slice(),
                    meta.topic_id.as_ref(),
                    meta.actor_id.as_ref(),
                    &meta.actor_seq.to_be_bytes(),
                ]
                .concat(),
                &op.id,
            )?;
            Self::tx_put(
                tx,
                &self.records,
                [
                    b"at".as_slice(),
                    meta.topic_id.as_ref(),
                    meta.actor_id.as_ref(),
                ]
                .concat(),
                &(meta.actor_seq, op.id),
            )?;
            Ok(())
        })
        .expect("fjall orphan op");
    }

    /// The topic as one read sees it, see [`Storage::topic_view`].
    fn read_topic_view(
        tx: &impl fjall::Readable,
        records: &fjall::OptimisticTxKeyspace,
        topic_id: &TopicId,
        peer_id: Option<&PeerId>,
    ) -> Result<Option<TopicView>> {
        let Some(mut state) =
            Self::tx_get::<TopicState>(tx, records, Self::key_id(b"ts", topic_id))?
        else {
            return Ok(None);
        };
        state.heads = Self::tx_get(tx, records, Self::key_id(b"h", topic_id))?.unwrap_or_default();
        let clock: ActorClock =
            Self::tx_get(tx, records, Self::key_id(b"ac", topic_id))?.unwrap_or_default();
        let mut tips = BTreeMap::new();
        let tip_prefix = [b"at".as_slice(), topic_id.as_ref()].concat();
        for item in fjall::Readable::prefix(tx, records, tip_prefix) {
            let (key, value) = item.into_inner()?;
            let actor = key
                .get(2 + TopicId::LEN..)
                .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
                .ok_or_else(|| Error::Storage("corrupt fjall actor tip key".into()))?;
            tips.insert(
                ActorId::from_bytes(actor),
                postcard::from_bytes(value.as_ref())?,
            );
        }
        let fingerprint = match Self::tx_get(tx, records, Self::key_id(b"fp", topic_id))? {
            Some(fingerprint) => fingerprint,
            None => topic_fingerprint_for(&state.heads, &clock)?,
        };
        let (ack, owed) = match peer_id {
            Some(peer_id) => (
                Self::tx_get(tx, records, Self::ack_key(topic_id, peer_id))?,
                fjall::Readable::prefix(tx, records, Self::obligation_prefix(topic_id, peer_id))
                    .next()
                    .is_some(),
            ),
            None => (None, false),
        };
        Ok(Some(TopicView {
            epoch: Self::tx_get(tx, records, Self::key_id(TOPIC_EPOCH_PREFIX, topic_id))?
                .unwrap_or_default(),
            pending_missing: Self::read_pending_missing(tx, records, topic_id)?,
            state,
            clock,
            tips,
            fingerprint,
            ack,
            owed,
        }))
    }

    /// Metadata of a stored op and the genesis of the branch holding it.
    fn read_op_branch(
        tx: &impl fjall::Readable,
        records: &fjall::OptimisticTxKeyspace,
        op_id: &OpId,
    ) -> Result<Option<(OpMeta, OpId)>> {
        let Some(meta) = Self::tx_get::<OpMeta>(tx, records, Self::key_id(b"m", op_id))? else {
            return Ok(None);
        };
        let state = Self::tx_get::<TopicState>(tx, records, Self::key_id(b"ts", &meta.topic_id))?;
        Ok(state.map(|state| (meta, state.genesis)))
    }

    fn tx_reset_topic(
        &self,
        tx: &mut fjall::OptimisticWriteTx,
        topic_id: &TopicId,
    ) -> Result<usize> {
        let epoch_key = Self::key_id(TOPIC_EPOCH_PREFIX, topic_id);
        let epoch: u64 = Self::tx_get(tx, &self.records, epoch_key.as_slice())?.unwrap_or_default();
        let next_epoch = epoch
            .checked_add(1)
            .ok_or_else(|| Error::Storage("topic data epoch overflow".into()))?;
        Self::tx_put(tx, &self.records, epoch_key, &next_epoch)?;
        // Topic op ids come from the `to` index; op records, meta, and the
        // children edges are keyed by op id, not by topic.
        let to_prefix = [b"to".as_slice(), topic_id.as_ref()].concat();
        let mut op_ids = Vec::new();
        for item in fjall::Readable::prefix(tx, &self.records, to_prefix) {
            let (key, _) = item.into_inner()?;
            op_ids.push(Self::op_id_from_key(key.as_ref(), 2 + TopicId::LEN)?);
        }
        let removed = op_ids.len();
        for op_id in &op_ids {
            // Edges pointing at this op go too, or a dependency the reset does
            // not reach keeps naming a child the topic no longer holds.
            if let Some(meta) =
                Self::tx_get::<OpMeta>(tx, &self.records, Self::key_id(b"m", op_id))?
            {
                for dep in &meta.deps {
                    tx.remove(
                        &self.records,
                        [b"ch".as_slice(), dep.as_ref(), op_id.as_ref()].concat(),
                    );
                }
            }
            tx.remove(&self.records, Self::key_id(b"o", op_id));
            tx.remove(&self.records, Self::key_id(b"m", op_id));
            let ch_prefix = [b"ch".as_slice(), op_id.as_ref()].concat();
            let mut ch_keys = Vec::new();
            for item in fjall::Readable::prefix(tx, &self.records, ch_prefix) {
                let (key, _) = item.into_inner()?;
                ch_keys.push(key.to_vec());
            }
            for key in ch_keys {
                tx.remove(&self.records, key);
            }
            tx.remove(
                &self.records,
                [b"to".as_slice(), topic_id.as_ref(), op_id.as_ref()].concat(),
            );
        }
        for prefix in [
            b"h".as_slice(),
            b"ac".as_slice(),
            b"fp".as_slice(),
            b"mg".as_slice(),
            b"ts".as_slice(),
        ] {
            tx.remove(&self.records, Self::key_id(prefix, topic_id));
        }
        // Actor index/tip and sync status share a `<prefix><topic>` layout.
        for prefix in [b"as".as_slice(), b"at".as_slice(), b"ss".as_slice()] {
            let scan = [prefix, topic_id.as_ref()].concat();
            let mut keys = Vec::new();
            for item in fjall::Readable::prefix(tx, &self.records, scan) {
                let (key, _) = item.into_inner()?;
                keys.push(key.to_vec());
            }
            for key in keys {
                tx.remove(&self.records, key);
            }
        }
        // Acks and obligations key topic first, so both are prefix deletes.
        Self::tx_remove_prefix(
            tx,
            &self.records,
            &[PEER_ACK_PREFIX, topic_id.as_ref()].concat(),
        )?;
        Self::tx_remove_prefix(
            tx,
            &self.records,
            &[OBLIGATION_PREFIX, topic_id.as_ref()].concat(),
        )?;
        Self::tx_reset_pending(tx, &self.records, topic_id)?;
        Ok(removed)
    }
}

#[cfg(feature = "fjall")]
impl Storage for FjallStorage {
    fn put_admitted_batch(&self, batch: AdmittedBatch) -> Result<()> {
        self.transaction_once(|tx| self.tx_admit_batch(tx, &batch))
    }

    fn reset_topic_and_admit(
        &self,
        topic_id: &TopicId,
        expected_topic_state: &TopicState,
        batch: AdmittedBatch,
        eviction: Option<&TopicEviction>,
    ) -> Result<usize> {
        self.transaction_once(|tx| {
            if Self::tx_get::<bool>(
                tx,
                &self.records,
                Self::key_id(SEALED_TOPIC_PREFIX, topic_id),
            )?
            .unwrap_or(false)
            {
                return Err(Error::TopicSealed);
            }
            let current_heads: BTreeSet<OpId> =
                Self::tx_get(tx, &self.records, Self::key_id(b"h", topic_id))?.unwrap_or_default();
            let current_topic_state: Option<TopicState> =
                Self::tx_get::<TopicState>(tx, &self.records, Self::key_id(b"ts", topic_id))?.map(
                    |mut state| {
                        state.heads = current_heads;
                        state
                    },
                );
            if current_topic_state.as_ref() != Some(expected_topic_state) {
                return Err(Error::AdmissionConflict);
            }
            let removed = self.tx_reset_topic(tx, topic_id)?;
            self.tx_admit_batch(tx, &batch)?;
            // The journal entry commits with the reset that made it the only
            // copy, so no crash point can leave the payloads unrecorded.
            if let Some((key, eviction)) = journalled_eviction(eviction) {
                let key = Self::key_id(EVICTION_PREFIX, &key);
                if Self::tx_get::<TopicEviction>(tx, &self.records, key.as_slice())?.is_none()
                    && Self::tx_eviction_count(tx, &self.records) >= MAX_PENDING_EVICTIONS
                {
                    return Err(Error::EvictionJournalFull);
                }
                Self::tx_put(tx, &self.records, key, eviction)?;
            }
            Ok(removed)
        })
    }

    fn seal_topic(&self, topic_id: &TopicId) -> Result<bool> {
        let key = Self::key_id(SEALED_TOPIC_PREFIX, topic_id);
        self.transaction(|tx| {
            if Self::tx_get::<bool>(tx, &self.records, key.as_slice())?.is_some() {
                return Ok(false);
            }
            Self::tx_put(tx, &self.records, key.clone(), &true)?;
            Ok(true)
        })
    }

    fn unseal_topic(&self, topic_id: &TopicId) -> Result<bool> {
        let key = Self::key_id(SEALED_TOPIC_PREFIX, topic_id);
        self.transaction(|tx| {
            if Self::tx_get::<bool>(tx, &self.records, key.as_slice())?.is_none() {
                return Ok(false);
            }
            tx.remove(&self.records, key.clone());
            Ok(true)
        })
    }

    fn pending_evictions(&self) -> Result<Vec<TopicEviction>> {
        let mut out = Vec::new();
        for item in self.records.inner().prefix(EVICTION_PREFIX) {
            let (_, value) = item.into_inner()?;
            out.push(postcard::from_bytes(value.as_ref())?);
        }
        Ok(out)
    }

    fn clear_eviction(&self, key: &EvictionKey) -> Result<()> {
        self.transaction(|tx| {
            tx.remove(&self.records, Self::key_id(EVICTION_PREFIX, key));
            Ok(())
        })
    }
    fn get_op(&self, id: &OpId) -> Result<Option<Op>> {
        self.counters.count_op();
        self.get(Self::key_id(b"o", id))
    }
    fn get_meta(&self, id: &OpId) -> Result<Option<OpMeta>> {
        self.counters.count_meta();
        self.get(Self::key_id(b"m", id))
    }
    fn dep_resolvable(&self, id: &OpId) -> Result<bool> {
        let read_tx = self.db.read_tx();
        Ok(
            fjall::Readable::get(&read_tx, &self.records, Self::key_id(b"o", id))?.is_some()
                && fjall::Readable::get(&read_tx, &self.records, Self::key_id(b"m", id))?.is_some(),
        )
    }
    fn list_ops(&self, topic_id: &TopicId) -> Result<Vec<Op>> {
        let read_tx = self.db.read_tx();
        let prefix = [b"to".as_slice(), topic_id.as_ref()].concat();
        let mut out = Vec::new();
        for item in fjall::Readable::prefix(&read_tx, &self.records, prefix) {
            let (key, _) = item.into_inner()?;
            let id = Self::op_id_from_key(key.as_ref(), 2 + TopicId::LEN)?;
            let value = fjall::Readable::get(&read_tx, &self.records, Self::key_id(b"o", &id))?
                .ok_or_else(|| Error::Storage(format!("missing op indexed for topic: {id}")))?;
            out.push(postcard::from_bytes(value.as_ref())?);
        }
        Ok(out)
    }
    fn list_op_ids(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>> {
        let prefix = [b"to".as_slice(), topic_id.as_ref()].concat();
        let mut out = BTreeSet::new();
        for item in self.records.inner().prefix(prefix) {
            let (key, _) = item.into_inner()?;
            out.insert(Self::op_id_from_key(key.as_ref(), 2 + TopicId::LEN)?);
        }
        Ok(out)
    }
    fn heads(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>> {
        Ok(self.get(Self::key_id(b"h", topic_id))?.unwrap_or_default())
    }
    fn children(&self, op_id: &OpId) -> Result<BTreeSet<OpId>> {
        let prefix = [b"ch".as_slice(), op_id.as_ref()].concat();
        let mut out = BTreeSet::new();
        for item in self.records.inner().prefix(prefix) {
            let (key, _) = item.into_inner()?;
            out.insert(Self::op_id_from_key(key.as_ref(), 2 + OpId::LEN)?);
        }
        Ok(out)
    }
    fn actor_tip(&self, topic_id: &TopicId, actor_id: &ActorId) -> Result<Option<(u64, OpId)>> {
        self.get([b"at".as_slice(), topic_id.as_ref(), actor_id.as_ref()].concat())
    }
    fn actor_index(
        &self,
        topic_id: &TopicId,
        actor_id: &ActorId,
        seq: u64,
    ) -> Result<Option<OpId>> {
        self.get(
            [
                b"as".as_slice(),
                topic_id.as_ref(),
                actor_id.as_ref(),
                &seq.to_be_bytes(),
            ]
            .concat(),
        )
    }
    fn actor_range(
        &self,
        topic_id: &TopicId,
        actor_id: &ActorId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, OpId)>> {
        let Some(start) = after.checked_add(1) else {
            return Ok(Vec::new());
        };
        let prefix = [b"as".as_slice(), topic_id.as_ref(), actor_id.as_ref()].concat();
        let from = [prefix.as_slice(), &start.to_be_bytes()].concat();
        let to = [prefix.as_slice(), &u64::MAX.to_be_bytes()].concat();
        let read_tx = self.db.read_tx();
        let mut out = Vec::new();
        for item in fjall::Readable::range(&read_tx, &self.records, from..=to).take(limit) {
            let (key, value) = item.into_inner()?;
            let seq = key
                .get(prefix.len()..)
                .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                .map(u64::from_be_bytes)
                .ok_or_else(|| Error::Storage("corrupt fjall actor index key".into()))?;
            out.push((seq, postcard::from_bytes(value.as_ref())?));
        }
        self.counters.count_index(out.len());
        Ok(out)
    }
    fn actor_clock(&self, topic_id: &TopicId) -> Result<ActorClock> {
        Ok(self.get(Self::key_id(b"ac", topic_id))?.unwrap_or_default())
    }
    fn topic_fingerprint(&self, topic_id: &TopicId) -> Result<[u8; 32]> {
        match self.get(Self::key_id(b"fp", topic_id))? {
            Some(fingerprint) => Ok(fingerprint),
            None => topic_fingerprint_for(&self.heads(topic_id)?, &self.actor_clock(topic_id)?),
        }
    }
    fn max_generation(&self, topic_id: &TopicId) -> Result<u64> {
        Ok(self.get(Self::key_id(b"mg", topic_id))?.unwrap_or_default())
    }
    fn topic_state(&self, topic_id: &TopicId) -> Result<Option<TopicState>> {
        let read_tx = self.db.read_tx();
        let Some(value) =
            fjall::Readable::get(&read_tx, &self.records, Self::key_id(b"ts", topic_id))?
        else {
            return Ok(None);
        };
        let mut state: TopicState = postcard::from_bytes(value.as_ref())?;
        state.heads = fjall::Readable::get(&read_tx, &self.records, Self::key_id(b"h", topic_id))?
            .map(|value| postcard::from_bytes(value.as_ref()))
            .transpose()?
            .unwrap_or_default();
        Ok(Some(state))
    }
    fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        // v0 keeps this simple: scan durable topic records instead of maintaining a second index.
        let mut out = Vec::new();
        for item in self.records.inner().prefix(b"ts") {
            let value = item.value()?;
            let s: TopicState = postcard::from_bytes(value.as_ref())?;
            out.push(TopicInfo {
                topic_id: s.topic_id,
                event_type_id: s.event_type_id,
                genesis: s.genesis,
            });
        }
        Ok(out)
    }
    fn topic_view(
        &self,
        topic_id: &TopicId,
        peer_id: Option<&PeerId>,
    ) -> Result<Option<TopicView>> {
        Self::read_topic_view(&self.db.read_tx(), &self.records, topic_id, peer_id)
    }
    fn peer_reached_op(&self, peer_id: &PeerId, op_id: &OpId) -> Result<bool> {
        let read_tx = self.db.read_tx();
        let Some((meta, genesis)) = Self::read_op_branch(&read_tx, &self.records, op_id)? else {
            return Ok(false);
        };
        let ack: Option<PeerAck> = Self::tx_get(
            &read_tx,
            &self.records,
            Self::ack_key(&meta.topic_id, peer_id),
        )?;
        Ok(ack.is_some_and(|ack| ack_reached_op(&ack, genesis, &meta)))
    }
    fn peers_reached_op(&self, op_id: &OpId) -> Result<Vec<PeerId>> {
        let read_tx = self.db.read_tx();
        let Some((meta, genesis)) = Self::read_op_branch(&read_tx, &self.records, op_id)? else {
            return Ok(Vec::new());
        };
        let mut peers = Vec::new();
        let prefix = [PEER_ACK_PREFIX, meta.topic_id.as_ref()].concat();
        for item in fjall::Readable::prefix(&read_tx, &self.records, prefix) {
            let value = item.value()?;
            let ack: PeerAck = postcard::from_bytes(value.as_ref())?;
            if ack_reached_op(&ack, genesis, &meta) {
                peers.push(ack.peer_id);
            }
        }
        peers.sort();
        Ok(peers)
    }
    fn put_pending_op(&self, source_peer: PeerId, op: Op, meta: OpMeta) -> Result<()> {
        let charge = Self::pending_charge(&op, &meta)?;
        self.transaction(|tx| {
            Self::tx_put_pending(tx, &self.records, source_peer, &op, &meta, charge)
        })
    }
    fn pending_waiters(&self, dep_id: &OpId) -> Result<Vec<(PeerId, Op)>> {
        self.read_pending_waiters(dep_id)
    }
    fn ready_pending_after(&self, after: Option<&OpId>, limit: usize) -> Result<Vec<(PeerId, Op)>> {
        self.read_ready_after(after, limit)
    }
    fn pending_missing_deps(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>> {
        Self::read_pending_missing(&self.db.read_tx(), &self.records, topic_id)
    }
    fn remove_pending_op(&self, op_id: &OpId) -> Result<()> {
        self.transaction(|tx| Self::tx_remove_pending_op(tx, &self.records, op_id))
    }
    fn purge_pending_waiters(&self, dep_id: &OpId) -> Result<usize> {
        self.transaction(|tx| Self::tx_purge_waiters(tx, &self.records, dep_id))
    }
    fn reject_pending_subtree(&self, op_id: &OpId) -> Result<usize> {
        // The markers, the root and its closure share one transaction, so no
        // reader sees the root gone while its waiters still hold quota.
        self.transaction(|tx| Self::tx_reject_subtree(tx, &self.records, op_id))
    }
    fn peer_ack(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<Option<PeerAck>> {
        self.get(Self::ack_key(topic_id, peer_id))
    }
    fn peer_acks(&self, topic_id: &TopicId) -> Result<Vec<PeerAck>> {
        let mut out = Vec::new();
        let read_tx = self.db.read_tx();
        let prefix = [PEER_ACK_PREFIX, topic_id.as_ref()].concat();
        for item in fjall::Readable::prefix(&read_tx, &self.records, prefix) {
            out.push(postcard::from_bytes(item.value()?.as_ref())?);
        }
        Ok(out)
    }
    fn put_sync_obligation(
        &self,
        obligation: SyncObligation,
        expected_genesis: Option<OpId>,
    ) -> Result<()> {
        self.transaction(|tx| {
            let state: Option<TopicState> =
                Self::tx_get(tx, &self.records, Self::key_id(b"ts", &obligation.topic_id))?;
            branch_matches(state.as_ref(), expected_genesis)?;
            Self::tx_put_obligation(tx, &self.records, &obligation)
        })
    }

    fn all_sync_obligations(&self) -> Result<Vec<SyncObligation>> {
        let mut out = Vec::new();
        let read_tx = self.db.read_tx();
        for item in fjall::Readable::prefix(&read_tx, &self.records, OBLIGATION_PREFIX) {
            let (key, value) = item.into_inner()?;
            if key.len() != OBLIGATION_KEY_LEN {
                continue;
            }
            out.push(postcard::from_bytes(value.as_ref())?);
        }
        self.counters.count_obligations(out.len());
        Ok(out)
    }

    fn apply_peer_ack(&self, ack: PeerAck) -> Result<usize> {
        self.transaction(|tx| {
            let commit = Self::tx_ack_commit(tx, &self.records, &ack)??;
            Self::tx_apply_peer_ack(tx, &self.records, &ack, commit)
        })
    }

    fn apply_peer_acks(&self, acks: Vec<PeerAck>) -> Result<Vec<Result<usize>>> {
        if acks.is_empty() {
            return Ok(Vec::new());
        }
        self.transaction(|tx| {
            let mut results = Vec::with_capacity(acks.len());
            for ack in &acks {
                match Self::tx_ack_commit(tx, &self.records, ack)? {
                    // A backend failure after this ack's writes were staged
                    // aborts the transaction, so none of the batch commits.
                    Ok(commit) => {
                        let cleared = Self::tx_apply_peer_ack(tx, &self.records, ack, commit)?;
                        results.push(Ok(cleared));
                    }
                    Err(rejected) => results.push(Err(rejected)),
                }
            }
            Ok(results)
        })
    }

    fn sync_obligations(
        &self,
        peer_id: &PeerId,
        topic_id: &TopicId,
    ) -> Result<Vec<SyncObligation>> {
        let prefix = Self::obligation_prefix(topic_id, peer_id);
        let mut out = Vec::new();
        let read_tx = self.db.read_tx();
        for item in fjall::Readable::prefix(&read_tx, &self.records, prefix) {
            let value = item.value()?;
            out.push(postcard::from_bytes(value.as_ref())?);
        }
        self.counters.count_obligations(out.len());
        Ok(out)
    }

    fn sync_obligation_count(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<usize> {
        let read_tx = self.db.read_tx();
        let mut count = 0;
        for target in [
            ObligationTarget::Clock(ActorClock::new()),
            ObligationTarget::Repair(BTreeSet::new()),
        ] {
            let key = Self::obligation_key(&SyncObligation {
                peer_id: *peer_id,
                topic_id: *topic_id,
                target,
            });
            count += usize::from(fjall::Readable::contains_key(&read_tx, &self.records, key)?);
        }
        Ok(count)
    }

    fn has_sync_obligations(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<bool> {
        let prefix = Self::obligation_prefix(topic_id, peer_id);
        let read_tx = self.db.read_tx();
        Ok(fjall::Readable::prefix(&read_tx, &self.records, prefix)
            .next()
            .is_some())
    }

    fn next_attempt_epoch(&self) -> Result<u64> {
        self.transaction(|tx| {
            let epoch = Self::tx_get::<u64>(tx, &self.records, ATTEMPT_EPOCH_KEY)?
                .unwrap_or_default()
                .checked_add(1)
                .ok_or_else(|| Error::Storage("attempt epoch overflow".into()))?;
            Self::tx_put(tx, &self.records, ATTEMPT_EPOCH_KEY, &epoch)?;
            Ok(epoch)
        })
    }

    fn put_sync_status(&self, status: SyncPeerStatus) -> Result<()> {
        self.put(
            [
                b"ss".as_slice(),
                status.topic_id.as_ref(),
                status.peer_id.as_ref(),
            ]
            .concat(),
            &status,
        )
    }

    fn update_sync_status(
        &self,
        peer_id: &PeerId,
        topic_id: &TopicId,
        update: &SyncStatusUpdate,
    ) -> Result<SyncPeerStatus> {
        let key = [b"ss".as_slice(), topic_id.as_ref(), peer_id.as_ref()].concat();
        self.transaction(|tx| {
            let mut status = fjall::Readable::get(tx, &self.records, key.as_slice())?
                .map(|bytes| decode_status(bytes.as_ref()))
                .transpose()?
                .unwrap_or_else(|| new_peer_status(*peer_id, *topic_id));
            // A rejected update leaves no record behind for a peer that had none.
            if apply_status_update(&mut status, update) {
                Self::tx_put(tx, &self.records, key.as_slice(), &status)?;
            }
            Ok(status)
        })
    }

    fn topic_obligation_counts(&self, topic_id: &TopicId) -> Result<BTreeMap<PeerId, usize>> {
        let mut counts = BTreeMap::new();
        let read_tx = self.db.read_tx();
        let prefix = [OBLIGATION_PREFIX, topic_id.as_ref()].concat();
        for item in fjall::Readable::prefix(&read_tx, &self.records, prefix) {
            let key = item.key()?;
            let peer = key
                .get(2 + TopicId::LEN..2 + TopicId::LEN + PeerId::LEN)
                .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
                .ok_or_else(|| Error::Storage("corrupt fjall obligation key".into()))?;
            *counts.entry(PeerId::from_bytes(peer)).or_default() += 1;
        }
        Ok(counts)
    }

    fn clear_peer_sync_state(
        &self,
        peer_id: &PeerId,
        topic_id: &TopicId,
        expected_genesis: Option<OpId>,
    ) -> Result<usize> {
        self.transaction(|tx| {
            let state: Option<TopicState> =
                Self::tx_get(tx, &self.records, Self::key_id(b"ts", topic_id))?;
            if !peer_departed(state.as_ref(), peer_id, expected_genesis) {
                return Ok(0);
            }
            let cleared = Self::tx_remove_prefix(
                tx,
                &self.records,
                &Self::obligation_prefix(topic_id, peer_id),
            )?;
            tx.remove(
                &self.records,
                [b"ss".as_slice(), topic_id.as_ref(), peer_id.as_ref()].concat(),
            );
            tx.remove(&self.records, Self::ack_key(topic_id, peer_id));
            Ok(cleared)
        })
    }

    fn sync_statuses(&self, topic_id: &TopicId) -> Result<Vec<SyncPeerStatus>> {
        let prefix = [b"ss".as_slice(), topic_id.as_ref()].concat();
        let mut out = Vec::new();
        let read_tx = self.db.read_tx();
        for item in fjall::Readable::prefix(&read_tx, &self.records, prefix) {
            let value = item.value()?;
            out.push(decode_status(value.as_ref())?);
        }
        Ok(out)
    }

    fn reset_topic(&self, topic_id: &TopicId) -> Result<usize> {
        self.transaction(|tx| self.tx_reset_topic(tx, topic_id))
    }

    fn stage_bootstrap_ops(
        &self,
        source: PeerId,
        topic_id: TopicId,
        ops: Vec<Op>,
        now_ms: u64,
    ) -> Result<StagedTopic> {
        let charges = Self::staged_charges(&topic_id, &ops)?;
        self.transaction(|tx| {
            Self::tx_stage_ops(
                tx,
                &self.records,
                (source, topic_id),
                &ops,
                &charges,
                now_ms,
            )
        })
    }

    fn staged_bootstrap_ops(&self, source: &PeerId, topic_id: &TopicId) -> Result<Vec<Op>> {
        self.read_staged_ops(source, topic_id)
    }

    fn promote_bootstrap(&self, batch: AdmittedBatch) -> Result<()> {
        self.transaction(|tx| {
            if fjall::Readable::contains_key(
                tx,
                &self.records,
                Self::key_id(b"ts", &batch.topic_id),
            )? {
                return Err(Error::AdmissionConflict);
            }
            self.tx_admit_batch(tx, &batch)?;
            Self::tx_discard_topic(tx, &self.records, &batch.topic_id)
        })
    }

    fn discard_bootstrap(&self, source: &PeerId, topic_id: &TopicId) -> Result<usize> {
        self.transaction(|tx| Self::tx_discard_session(tx, &self.records, source, topic_id))
    }

    fn expire_bootstrap(&self, older_than_ms: u64) -> Result<usize> {
        self.transaction(|tx| Self::tx_expire_sessions(tx, &self.records, older_than_ms))
    }
}

#[cfg(feature = "fjall")]
fn clear_satisfied_tx(
    tx: &mut fjall::OptimisticWriteTx,
    records: &fjall::OptimisticTxKeyspace,
    ack: &PeerAck,
) -> Result<usize> {
    let prefix = FjallStorage::obligation_prefix(&ack.topic_id, &ack.peer_id);
    let mut stored = Vec::new();
    for item in fjall::Readable::prefix(tx, records, prefix) {
        let (key, value) = item.into_inner()?;
        stored.push((
            key.to_vec(),
            postcard::from_bytes::<SyncObligation>(value.as_ref())?,
        ));
    }
    let mut cleared = 0;
    for (key, obligation) in stored {
        let rest = settled_obligation(&obligation, ack, |id| {
            FjallStorage::tx_get(tx, records, FjallStorage::key_id(b"m", id))
        })?;
        match rest {
            Some(rest) if rest == obligation => {}
            Some(rest) => FjallStorage::tx_put(tx, records, key, &rest)?,
            None => {
                tx.remove(records, key);
                cleared += 1;
            }
        }
    }
    Ok(cleared)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn obligation_scans_skip_op_collision() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path()).unwrap();
        let mut op_id = [0_u8; OpId::LEN];
        op_id[0] = b'b';
        storage
            .put(FjallStorage::key_id(b"o", &OpId::from_bytes(op_id)), &())
            .unwrap();

        let obligation = SyncObligation::repair(
            PeerId::hash(b"collision-peer"),
            TopicId::hash(b"collision-topic"),
            [OpId::hash(b"collision-op")].into(),
        );
        storage
            .put_sync_obligation(obligation.clone(), None)
            .unwrap();

        assert_eq!(
            storage.all_sync_obligations().unwrap(),
            vec![obligation.clone()]
        );
        assert_eq!(storage.reset_topic(&obligation.topic_id).unwrap(), 0);
        assert!(storage.all_sync_obligations().unwrap().is_empty());
    }
}
