// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{ActorClock, ActorId, Error, Op, OpId, PeerId, Result, TopicId, TopicInfo};

use super::{
    AdmittedBatch, MAX_PENDING_MISSING_DEPS, MAX_PENDING_OPS_PER_SOURCE, MAX_PENDING_OPS_TOTAL,
    MAX_PENDING_WAITERS_PER_DEP, OpMeta, PeerAck, Storage, SyncObligation, SyncPeerStatus,
    TopicState, stored_ack_dominates, sync_obligation_satisfied, topic_fingerprint_for,
};

#[cfg(feature = "fjall")]
#[derive(Clone)]
pub struct FjallStorage {
    db: fjall::OptimisticTxDatabase,
    records: fjall::OptimisticTxKeyspace,
    persist_mode: fjall::PersistMode,
}

#[cfg(feature = "fjall")]
const FJALL_SCHEMA_VERSION: u32 = 1;
#[cfg(feature = "fjall")]
const FJALL_SCHEMA_VERSION_KEY: &[u8] = b"sv";

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
        };
        storage.ensure_schema_version()?;
        Ok(storage)
    }

    fn ensure_schema_version(&self) -> Result<()> {
        match self.get::<u32>(FJALL_SCHEMA_VERSION_KEY)? {
            Some(FJALL_SCHEMA_VERSION) => Ok(()),
            Some(version) => Err(Error::Storage(format!(
                "unsupported fjall schema version {version}"
            ))),
            None => self.put(FJALL_SCHEMA_VERSION_KEY, &FJALL_SCHEMA_VERSION),
        }
    }

    fn transaction<R>(
        &self,
        mut f: impl FnMut(&mut fjall::OptimisticWriteTx) -> Result<R>,
    ) -> Result<R> {
        for _ in 0..64 {
            let mut tx = self.db.write_tx()?.durability(Some(self.persist_mode));
            let result = f(&mut tx)?;
            match tx.commit()? {
                Ok(()) => return Ok(result),
                Err(_) => continue,
            }
        }
        Err(Error::AdmissionConflict)
    }

    fn key_id(prefix: &[u8], id: &impl AsRef<[u8]>) -> Vec<u8> {
        [prefix, id.as_ref()].concat()
    }

    fn put<T: Serialize>(&self, key: impl AsRef<[u8]>, value: &T) -> Result<()> {
        let key = key.as_ref().to_vec();
        let value = postcard::to_allocvec(value)?;
        self.transaction(|tx| {
            tx.insert(&self.records, key.clone(), value.clone());
            Ok(())
        })
    }

    fn tx_put<T: Serialize>(
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

    fn tx_get<T: for<'de> Deserialize<'de>>(
        tx: &fjall::OptimisticWriteTx,
        records: &fjall::OptimisticTxKeyspace,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<T>> {
        Ok(fjall::Readable::get(tx, records, key.as_ref())?
            .map(|v| postcard::from_bytes(v.as_ref()))
            .transpose()?)
    }

    fn tx_remove_pending_op(
        tx: &mut fjall::OptimisticWriteTx,
        records: &fjall::OptimisticTxKeyspace,
        op_id: &OpId,
    ) -> Result<()> {
        let Some((source_peer, _, meta)) =
            Self::tx_get::<(PeerId, Op, OpMeta)>(tx, records, Self::key_id(b"po", op_id))?
        else {
            return Ok(());
        };
        tx.remove(records, Self::key_id(b"po", op_id));
        for dep in &meta.missing_deps {
            let waiter_key = [b"pw".as_slice(), dep.as_ref(), op_id.as_ref()].concat();
            tx.remove(records, waiter_key);
            let count_key = [b"wn".as_slice(), dep.as_ref()].concat();
            let count: u64 = Self::tx_get(tx, records, count_key.as_slice())?.unwrap_or_default();
            let next = count.saturating_sub(1);
            if next == 0 {
                tx.remove(records, count_key);
            } else {
                Self::tx_put(tx, records, count_key.as_slice(), &next)?;
            }
        }
        let total: u64 = Self::tx_get(tx, records, b"pn".as_slice())?.unwrap_or_default();
        let next_total = total.saturating_sub(1);
        if next_total == 0 {
            tx.remove(records, b"pn".as_slice());
        } else {
            Self::tx_put(tx, records, b"pn".as_slice(), &next_total)?;
        }
        let source_key = [b"ps".as_slice(), source_peer.as_ref()].concat();
        let source_count: u64 =
            Self::tx_get(tx, records, source_key.as_slice())?.unwrap_or_default();
        let next_source = source_count.saturating_sub(1);
        if next_source == 0 {
            tx.remove(records, source_key);
        } else {
            Self::tx_put(tx, records, source_key.as_slice(), &next_source)?;
        }
        Ok(())
    }

    fn sync_obligation_key(obligation: &SyncObligation) -> Vec<u8> {
        let digest =
            blake3::hash(&postcard::to_allocvec(&obligation.op_ids).expect("op ids serialize"));
        [
            b"ob".as_slice(),
            obligation.peer_id.as_ref(),
            obligation.topic_id.as_ref(),
            digest.as_bytes(),
        ]
        .concat()
    }
    fn get<T: for<'de> Deserialize<'de>>(&self, key: impl AsRef<[u8]>) -> Result<Option<T>> {
        Ok(self
            .records
            .get(key)?
            .map(|v| postcard::from_bytes(v.as_ref()))
            .transpose()?)
    }

    fn op_id_from_key(key: &[u8], offset: usize) -> Result<OpId> {
        let bytes = key
            .get(offset..offset + OpId::LEN)
            .ok_or_else(|| Error::Storage("corrupt fjall op id index key".into()))?;
        let mut out = [0_u8; OpId::LEN];
        out.copy_from_slice(bytes);
        Ok(OpId::from_bytes(out))
    }
}

#[cfg(feature = "fjall")]
impl Storage for FjallStorage {
    fn put_admitted_batch(&self, batch: AdmittedBatch) -> Result<()> {
        let AdmittedBatch {
            topic_id,
            expected_heads,
            expected_topic_state,
            entries,
            heads,
            topic_state,
            effects,
        } = batch;
        self.transaction(|tx| {
            let current_heads: BTreeSet<OpId> =
                Self::tx_get(tx, &self.records, Self::key_id(b"h", &topic_id))?.unwrap_or_default();
            if current_heads != expected_heads {
                return Err(Error::AdmissionConflict);
            }
            let current_topic_state: Option<TopicState> =
                Self::tx_get::<TopicState>(tx, &self.records, Self::key_id(b"ts", &topic_id))?.map(
                    |mut state| {
                        state.heads = current_heads.clone();
                        state
                    },
                );
            if current_topic_state != expected_topic_state {
                return Err(Error::AdmissionConflict);
            }

            let mut actor_tips = BTreeMap::new();
            let mut new_entries = Vec::new();
            for (op, meta) in &entries {
                if meta.topic_id != topic_id {
                    return Err(Error::TopicMismatch);
                }
                if let Some(existing) =
                    Self::tx_get::<Op>(tx, &self.records, Self::key_id(b"o", &op.id))?
                {
                    if existing != *op {
                        return Err(Error::Storage("op id collision with different op".into()));
                    }
                    continue;
                }
                if let Some(existing) = Self::tx_get::<OpId>(
                    tx,
                    &self.records,
                    [
                        b"as".as_slice(),
                        meta.topic_id.as_ref(),
                        meta.actor_id.as_ref(),
                        &meta.actor_seq.to_be_bytes(),
                    ]
                    .concat(),
                )? && existing != op.id
                {
                    return Err(Error::ActorFork);
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
                if let Some((source_peer, _, pending_meta)) = Self::tx_get::<(PeerId, Op, OpMeta)>(
                    tx,
                    &self.records,
                    Self::key_id(b"po", &op.id),
                )? {
                    tx.remove(&self.records, Self::key_id(b"po", &op.id));
                    for dep in &pending_meta.missing_deps {
                        tx.remove(
                            &self.records,
                            [b"pw".as_slice(), dep.as_ref(), op.id.as_ref()].concat(),
                        );
                        let count_key = [b"wn".as_slice(), dep.as_ref()].concat();
                        let count: u64 = Self::tx_get(tx, &self.records, count_key.as_slice())?
                            .unwrap_or_default();
                        let next = count.saturating_sub(1);
                        if next == 0 {
                            tx.remove(&self.records, count_key);
                        } else {
                            Self::tx_put(tx, &self.records, count_key.as_slice(), &next)?;
                        }
                    }
                    let total: u64 =
                        Self::tx_get(tx, &self.records, b"pn".as_slice())?.unwrap_or_default();
                    let next_total = total.saturating_sub(1);
                    if next_total == 0 {
                        tx.remove(&self.records, b"pn".as_slice());
                    } else {
                        Self::tx_put(tx, &self.records, b"pn".as_slice(), &next_total)?;
                    }
                    let source_key = [b"ps".as_slice(), source_peer.as_ref()].concat();
                    let source_count: u64 =
                        Self::tx_get(tx, &self.records, source_key.as_slice())?.unwrap_or_default();
                    let next_source = source_count.saturating_sub(1);
                    if next_source == 0 {
                        tx.remove(&self.records, source_key);
                    } else {
                        Self::tx_put(tx, &self.records, source_key.as_slice(), &next_source)?;
                    }
                }
                clock.observe(meta.actor_id, meta.actor_seq);
                max_generation = max_generation.max(meta.generation);
            }
            Self::tx_put(tx, &self.records, Self::key_id(b"ac", &topic_id), &clock)?;
            Self::tx_put(tx, &self.records, Self::key_id(b"h", &topic_id), &heads)?;
            Self::tx_put(
                tx,
                &self.records,
                Self::key_id(b"fp", &topic_id),
                &topic_fingerprint_for(&heads, &clock)?,
            )?;
            Self::tx_put(
                tx,
                &self.records,
                Self::key_id(b"mg", &topic_id),
                &max_generation,
            )?;
            if let Some(state) = &topic_state {
                Self::tx_put(
                    tx,
                    &self.records,
                    Self::key_id(b"ts", &state.topic_id),
                    state,
                )?;
            }
            for obligation in &effects.sync_obligations {
                Self::tx_put(
                    tx,
                    &self.records,
                    Self::sync_obligation_key(obligation),
                    obligation,
                )?;
            }
            Ok(())
        })
    }
    fn get_op(&self, id: &OpId) -> Result<Option<Op>> {
        self.get(Self::key_id(b"o", id))
    }
    fn get_meta(&self, id: &OpId) -> Result<Option<OpMeta>> {
        self.get(Self::key_id(b"m", id))
    }
    fn list_ops(&self, topic_id: &TopicId) -> Result<Vec<Op>> {
        self.list_op_ids(topic_id)?
            .iter()
            .map(|id| {
                self.get_op(id)?
                    .ok_or_else(|| Error::Storage(format!("missing op indexed for topic: {id}")))
            })
            .collect()
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
    fn actor_clock(&self, topic_id: &TopicId) -> Result<ActorClock> {
        Ok(self.get(Self::key_id(b"ac", topic_id))?.unwrap_or_default())
    }
    fn topic_fingerprint(&self, topic_id: &TopicId) -> Result<[u8; 32]> {
        Ok(self
            .get(Self::key_id(b"fp", topic_id))?
            .unwrap_or(topic_fingerprint_for(
                &self.heads(topic_id)?,
                &self.actor_clock(topic_id)?,
            )?))
    }
    fn max_generation(&self, topic_id: &TopicId) -> Result<u64> {
        Ok(self.get(Self::key_id(b"mg", topic_id))?.unwrap_or_default())
    }
    fn topic_state(&self, topic_id: &TopicId) -> Result<Option<TopicState>> {
        self.get::<TopicState>(Self::key_id(b"ts", topic_id))?
            .map(|mut state| {
                state.heads = self.heads(topic_id)?;
                Ok(state)
            })
            .transpose()
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
    fn put_pending_op(&self, source_peer: PeerId, op: Op, meta: OpMeta) -> Result<()> {
        self.transaction(|tx| {
            if Self::tx_get::<Op>(tx, &self.records, Self::key_id(b"o", &op.id))?.is_some() {
                return Ok(());
            }
            if let Some((_, existing, _)) = Self::tx_get::<(PeerId, Op, OpMeta)>(
                tx,
                &self.records,
                Self::key_id(b"po", &op.id),
            )? {
                if existing != op {
                    return Err(Error::Storage(
                        "pending op id collision with different op".into(),
                    ));
                }
                Self::tx_remove_pending_op(tx, &self.records, &op.id)?;
            }
            for dep in &meta.missing_deps {
                if Self::tx_get::<Op>(tx, &self.records, Self::key_id(b"o", dep))?.is_some() {
                    return Err(Error::AdmissionConflict);
                }
            }
            if meta.missing_deps.len() > MAX_PENDING_MISSING_DEPS {
                return Err(Error::Storage(
                    "pending op has too many missing deps".into(),
                ));
            }

            let total_pending: u64 =
                Self::tx_get(tx, &self.records, b"pn".as_slice())?.unwrap_or_default();
            if total_pending as usize >= MAX_PENDING_OPS_TOTAL {
                return Err(Error::Storage("pending op buffer is full".into()));
            }
            let source_key = [b"ps".as_slice(), source_peer.as_ref()].concat();
            let source_pending: u64 =
                Self::tx_get(tx, &self.records, source_key.as_slice())?.unwrap_or_default();
            if source_pending as usize >= MAX_PENDING_OPS_PER_SOURCE {
                return Err(Error::Storage("pending op source quota exceeded".into()));
            }
            let mut waiter_keys: Vec<(Vec<u8>, u64)> = Vec::with_capacity(meta.missing_deps.len());
            for dep in &meta.missing_deps {
                let key = [b"wn".as_slice(), dep.as_ref()].concat();
                let count: u64 =
                    Self::tx_get(tx, &self.records, key.as_slice())?.unwrap_or_default();
                if count as usize >= MAX_PENDING_WAITERS_PER_DEP {
                    return Err(Error::Storage("pending waiter quota exceeded".into()));
                }
                waiter_keys.push((key, count));
            }

            Self::tx_put(
                tx,
                &self.records,
                Self::key_id(b"po", &op.id),
                &(source_peer, op.clone(), meta.clone()),
            )?;
            for dep in &meta.missing_deps {
                Self::tx_put(
                    tx,
                    &self.records,
                    [b"pw".as_slice(), dep.as_ref(), op.id.as_ref()].concat(),
                    &(),
                )?;
            }
            Self::tx_put(tx, &self.records, b"pn".as_slice(), &(total_pending + 1))?;
            Self::tx_put(
                tx,
                &self.records,
                source_key.as_slice(),
                &(source_pending + 1),
            )?;
            for (key, count) in waiter_keys {
                Self::tx_put(tx, &self.records, key.as_slice(), &(count + 1))?;
            }
            Ok(())
        })
    }
    fn pending_waiters(&self, dep_id: &OpId) -> Result<Vec<(PeerId, Op)>> {
        let prefix = [b"pw".as_slice(), dep_id.as_ref()].concat();
        let mut out = Vec::new();
        let read_tx = self.db.read_tx();
        for item in fjall::Readable::prefix(&read_tx, &self.records, prefix) {
            let (key, _) = item.into_inner()?;
            let op_id = Self::op_id_from_key(key.as_ref(), 2 + OpId::LEN)?;
            if let Some((source, op, _)) =
                fjall::Readable::get(&read_tx, &self.records, Self::key_id(b"po", &op_id))?
                    .map(|value| postcard::from_bytes::<(PeerId, Op, OpMeta)>(value.as_ref()))
                    .transpose()?
            {
                out.push((source, op));
            }
        }
        Ok(out)
    }
    fn ready_pending_ops(&self) -> Result<Vec<(PeerId, Op)>> {
        let mut out = Vec::new();
        let read_tx = self.db.read_tx();
        for item in fjall::Readable::prefix(&read_tx, &self.records, b"po".as_slice()) {
            let value = item.value()?;
            let (source, op, meta): (PeerId, Op, OpMeta) = postcard::from_bytes(value.as_ref())?;
            let mut ready = true;
            for dep in &meta.missing_deps {
                if fjall::Readable::get(&read_tx, &self.records, Self::key_id(b"o", dep))?.is_none()
                {
                    ready = false;
                    break;
                }
            }
            if ready {
                out.push((source, op));
            }
        }
        Ok(out)
    }
    fn remove_pending_op(&self, op_id: &OpId) -> Result<()> {
        self.transaction(|tx| Self::tx_remove_pending_op(tx, &self.records, op_id))
    }
    fn peer_ack(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<Option<PeerAck>> {
        self.get([b"ak".as_slice(), peer_id.as_ref(), topic_id.as_ref()].concat())
    }
    fn peer_acks(&self, topic_id: &TopicId) -> Result<Vec<PeerAck>> {
        let mut out = Vec::new();
        let read_tx = self.db.read_tx();
        for item in fjall::Readable::prefix(&read_tx, &self.records, b"ak".as_slice()) {
            let value = item.value()?;
            let ack: PeerAck = postcard::from_bytes(value.as_ref())?;
            if ack.topic_id == *topic_id {
                out.push(ack);
            }
        }
        Ok(out)
    }
    fn put_sync_obligation(&self, obligation: SyncObligation) -> Result<()> {
        self.put(Self::sync_obligation_key(&obligation), &obligation)
    }

    fn all_sync_obligations(&self) -> Result<Vec<SyncObligation>> {
        let mut out = Vec::new();
        let read_tx = self.db.read_tx();
        for item in fjall::Readable::prefix(&read_tx, &self.records, b"ob".as_slice()) {
            let value = item.value()?;
            out.push(postcard::from_bytes(value.as_ref())?);
        }
        Ok(out)
    }

    fn apply_peer_ack(&self, ack: PeerAck) -> Result<usize> {
        self.transaction(|tx| {
            let ack_key = [
                b"ak".as_slice(),
                ack.peer_id.as_ref(),
                ack.topic_id.as_ref(),
            ]
            .concat();
            let effective_ack =
                match Self::tx_get::<PeerAck>(tx, &self.records, ack_key.as_slice())? {
                    Some(existing) if stored_ack_dominates(&existing, &ack) => existing,
                    _ => {
                        Self::tx_put(tx, &self.records, ack_key, &ack)?;
                        ack.clone()
                    }
                };
            clear_satisfied_tx(tx, &self.records, &effective_ack)
        })
    }

    fn sync_obligations(
        &self,
        peer_id: &PeerId,
        topic_id: &TopicId,
    ) -> Result<Vec<SyncObligation>> {
        let prefix = [b"ob".as_slice(), peer_id.as_ref(), topic_id.as_ref()].concat();
        let mut out = Vec::new();
        let read_tx = self.db.read_tx();
        for item in fjall::Readable::prefix(&read_tx, &self.records, prefix) {
            let value = item.value()?;
            out.push(postcard::from_bytes(value.as_ref())?);
        }
        Ok(out)
    }

    fn has_sync_obligations(&self, peer_id: &PeerId, topic_id: &TopicId) -> Result<bool> {
        let prefix = [b"ob".as_slice(), peer_id.as_ref(), topic_id.as_ref()].concat();
        let read_tx = self.db.read_tx();
        Ok(fjall::Readable::prefix(&read_tx, &self.records, prefix)
            .next()
            .is_some())
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

    fn sync_statuses(&self, topic_id: &TopicId) -> Result<Vec<SyncPeerStatus>> {
        let prefix = [b"ss".as_slice(), topic_id.as_ref()].concat();
        let mut out = Vec::new();
        let read_tx = self.db.read_tx();
        for item in fjall::Readable::prefix(&read_tx, &self.records, prefix) {
            let value = item.value()?;
            out.push(postcard::from_bytes(value.as_ref())?);
        }
        Ok(out)
    }
}

#[cfg(feature = "fjall")]
fn clear_satisfied_tx(
    tx: &mut fjall::OptimisticWriteTx,
    records: &fjall::OptimisticTxKeyspace,
    ack: &PeerAck,
) -> Result<usize> {
    let prefix = [
        b"ob".as_slice(),
        ack.peer_id.as_ref(),
        ack.topic_id.as_ref(),
    ]
    .concat();
    let mut keys = Vec::new();
    for item in fjall::Readable::prefix(tx, records, prefix) {
        let (key, value) = item.into_inner()?;
        let obligation: SyncObligation = postcard::from_bytes(value.as_ref())?;
        if sync_obligation_satisfied(&obligation, ack) {
            keys.push(key.to_vec());
        }
    }
    let cleared = keys.len();
    for key in keys {
        tx.remove(records, key);
    }
    Ok(cleared)
}
