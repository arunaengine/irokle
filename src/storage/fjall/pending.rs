// SPDX-License-Identifier: MIT OR Apache-2.0
//! Buffered ops in Fjall: small records, payloads, indexes and quotas.

use std::collections::BTreeSet;
use std::ops::Bound;

use crate::{Error, Op, OpId, PeerId, Result, TopicId};

use crate::storage::fjall::store::FjallStorage;
use crate::storage::{
    MAX_PENDING_MISSING_DEPS as MAX_MISSING_DEPS, MAX_PENDING_WAITERS_PER_DEP as MAX_WAITERS,
    MAX_REJECTED_PER_TOPIC as MAX_REJECTED, OpMeta, PendingRecord, PendingUsage,
    check_pending_quota, pending_op_bytes,
};

type Tx = crate::storage::pressure::Transaction;
type Records = fjall::OptimisticTxKeyspace;

/// Payload of a buffered op, `pp<op>`.
const PAYLOAD: &[u8] = b"pp";
/// Its record, `pm<op>`.
const RECORD: &[u8] = b"pm";
/// Ops per topic, `pt<topic><op>`.
const BY_TOPIC: &[u8] = b"pt";
/// Ops waiting on a dependency, `pw<dep><op>`.
const WAITER: &[u8] = b"pw";
/// Ops with nothing left to wait for, `pr<op>`. The next prefix is `ps`.
const READY: &[u8] = b"pr";
const READY_END: &[u8] = b"ps";
/// Waiter count per dependency, `wn<dep>`.
const WAITER_COUNT: &[u8] = b"wn";
/// Usage of the whole pool, per source `ps<source>` and per topic `pc<topic>`.
const TOTAL_USAGE: &[u8] = b"pu";
const SOURCE_USAGE: &[u8] = b"ps";
const TOPIC_USAGE: &[u8] = b"pc";
/// Rejected ids per topic: `rj<topic><op>` to a sequence, `rq<topic><seq>` to
/// the id in rejection order, and `rc<topic>` to the oldest and next sequence.
const REJECTED: &[u8] = b"rj";
const REJECTED_ORDER: &[u8] = b"rq";
const REJECTED_RANGE: &[u8] = b"rc";

fn key(parts: &[&[u8]]) -> Vec<u8> {
    parts.concat()
}

fn id_at(key: &[u8], offset: usize) -> Result<OpId> {
    key.get(offset..offset + OpId::LEN)
        .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
        .map(OpId::from_bytes)
        .ok_or_else(|| Error::Storage("corrupt fjall pending key".into()))
}

impl FjallStorage {
    pub(super) fn tx_pending_record(
        tx: &impl fjall::Readable,
        records: &Records,
        op_id: &OpId,
    ) -> Result<Option<PendingRecord>> {
        Self::tx_get(tx, records, key(&[RECORD, op_id.as_ref()]))
    }

    /// Serialized bytes of every buffered op.
    pub(super) fn tx_pending_bytes(tx: &impl fjall::Readable, records: &Records) -> Result<u64> {
        Ok(Self::tx_usage(tx, records, TOTAL_USAGE)?.bytes)
    }

    fn tx_usage(tx: &impl fjall::Readable, records: &Records, key: &[u8]) -> Result<PendingUsage> {
        Ok(Self::tx_get(tx, records, key)?.unwrap_or_default())
    }

    fn tx_put_usage(
        tx: &mut Tx,
        records: &Records,
        key: Vec<u8>,
        usage: PendingUsage,
    ) -> Result<()> {
        if usage == PendingUsage::default() {
            tx.remove(records, key)?;
            Ok(())
        } else {
            Self::tx_put(tx, records, key, &usage)
        }
    }

    fn tx_resolvable(tx: &impl fjall::Readable, records: &Records, id: &OpId) -> Result<bool> {
        Ok(
            fjall::Readable::contains_key(tx, records, Self::key_id(b"o", id))?
                && fjall::Readable::contains_key(tx, records, Self::key_id(b"m", id))?,
        )
    }

    fn tx_waiter_count(tx: &mut Tx, records: &Records, dep: &OpId, delta: i64) -> Result<u64> {
        let count_key = key(&[WAITER_COUNT, dep.as_ref()]);
        let count: u64 = Self::tx_get(tx, records, count_key.as_slice())?.unwrap_or_default();
        let next = count
            .checked_add_signed(delta)
            .ok_or_else(|| Error::Storage("pending waiter count does not match".into()))?;
        if next == 0 {
            tx.remove(records, count_key)?;
        } else {
            Self::tx_put(tx, records, count_key, &next)?;
        }
        Ok(count)
    }

    /// Buffer `op`, see [`crate::storage::Storage::put_pending_op`].
    pub(super) fn tx_put_pending(
        tx: &mut Tx,
        records: &Records,
        source_peer: PeerId,
        op: &Op,
        missing_deps: &BTreeSet<OpId>,
        charge: u64,
    ) -> Result<()> {
        let topic_id = op.signed.body.topic_id;
        // Only a completely stored op is already admitted; a half stored one
        // still has to buffer so its repair runs once its deps resolve.
        if Self::tx_resolvable(tx, records, &op.id)? {
            return Ok(());
        }
        for dep in missing_deps {
            if Self::tx_resolvable(tx, records, dep)? {
                return Err(Error::AdmissionConflict);
            }
        }
        for id in std::iter::once(&op.id).chain(missing_deps) {
            if fjall::Readable::contains_key(
                tx,
                records,
                key(&[REJECTED, topic_id.as_ref(), id.as_ref()]),
            )? {
                return Err(Error::RejectedOp(*id));
            }
        }
        let existing = Self::tx_pending_record(tx, records, &op.id)?;
        if let Some(record) = &existing {
            let payload: Option<Op> = Self::tx_get(tx, records, key(&[PAYLOAD, op.id.as_ref()]))?;
            if payload.as_ref() != Some(op) {
                return Err(Error::Storage(
                    "pending op id collision with different op".into(),
                ));
            }
            if record.missing == *missing_deps {
                return Ok(());
            }
        }
        let previous = existing
            .as_ref()
            .map(|record| record.missing.clone())
            .unwrap_or_default();
        for dep in missing_deps.difference(&previous) {
            if Self::tx_waiter_count(tx, records, dep, 1)? >= MAX_WAITERS as u64 {
                return Err(Error::Storage("pending waiter quota exceeded".into()));
            }
            Self::tx_put(
                tx,
                records,
                key(&[WAITER, dep.as_ref(), op.id.as_ref()]),
                &(),
            )?;
        }
        for dep in previous.difference(missing_deps) {
            tx.remove(records, key(&[WAITER, dep.as_ref(), op.id.as_ref()]))?;
            Self::tx_waiter_count(tx, records, dep, -1)?;
        }
        let ready_key = key(&[READY, op.id.as_ref()]);
        if missing_deps.is_empty() {
            Self::tx_put(tx, records, ready_key, &())?;
        } else {
            tx.remove(records, ready_key)?;
        }
        // A known op keeps its stored source and charge; only its waits move.
        let record = match existing {
            Some(record) => PendingRecord {
                missing: missing_deps.clone(),
                ..record
            },
            None => {
                let source_key = key(&[SOURCE_USAGE, source_peer.as_ref()]);
                let topic_key = key(&[TOPIC_USAGE, topic_id.as_ref()]);
                let total = Self::tx_usage(tx, records, TOTAL_USAGE)?;
                let source = Self::tx_usage(tx, records, &source_key)?;
                let topic = Self::tx_usage(tx, records, &topic_key)?;
                check_pending_quota(total, source, topic, charge)?;
                Self::tx_put_usage(tx, records, TOTAL_USAGE.to_vec(), total.charged(charge))?;
                Self::tx_put_usage(tx, records, source_key, source.charged(charge))?;
                Self::tx_put_usage(tx, records, topic_key, topic.charged(charge))?;
                Self::tx_put(
                    tx,
                    records,
                    key(&[BY_TOPIC, topic_id.as_ref(), op.id.as_ref()]),
                    &(),
                )?;
                Self::tx_put(tx, records, key(&[PAYLOAD, op.id.as_ref()]), op)?;
                PendingRecord {
                    source: source_peer,
                    topic_id,
                    missing: missing_deps.clone(),
                    charge,
                }
            }
        };
        Self::tx_put(tx, records, key(&[RECORD, op.id.as_ref()]), &record)
    }

    /// Move the buffered ops of `topic_id` from the namespace keyspace `store` into
    /// the active `records` in `tx`, each charged to its source again. Waits are
    /// read again from the active records, so an op whose dependencies arrived with
    /// the namespace becomes ready. A refused charge fails the whole transaction.
    pub(super) fn tx_move_pending(
        tx: &mut Tx,
        store: &Records,
        records: &Records,
        topic_id: &TopicId,
    ) -> Result<()> {
        let mut ids = Vec::new();
        for item in fjall::Readable::prefix(tx, store, key(&[BY_TOPIC, topic_id.as_ref()])) {
            ids.push(id_at(item.key()?.as_ref(), BY_TOPIC.len() + TopicId::LEN)?);
        }
        for op_id in ids {
            let Some(record) = Self::tx_pending_record(tx, store, &op_id)? else {
                continue;
            };
            let Some(op) = Self::tx_get::<Op>(tx, store, key(&[PAYLOAD, op_id.as_ref()]))? else {
                continue;
            };
            let mut missing = BTreeSet::new();
            for dep in &record.missing {
                if !Self::tx_resolvable(tx, records, dep)? {
                    missing.insert(*dep);
                }
            }
            match Self::tx_put_pending(tx, records, record.source, &op, &missing, record.charge) {
                // An op behind a rejected id can never be admitted here.
                Ok(()) | Err(Error::RejectedOp(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Drop a buffered op and refund its stored charge. Underflow is an error:
    /// the counters no longer describe the records.
    pub(super) fn tx_remove_pending(tx: &mut Tx, records: &Records, op_id: &OpId) -> Result<()> {
        let Some(record) = Self::tx_pending_record(tx, records, op_id)? else {
            return Ok(());
        };
        let source_key = key(&[SOURCE_USAGE, record.source.as_ref()]);
        let topic_key = key(&[TOPIC_USAGE, record.topic_id.as_ref()]);
        let total = Self::tx_usage(tx, records, TOTAL_USAGE)?.refunded(record.charge)?;
        let source = Self::tx_usage(tx, records, &source_key)?.refunded(record.charge)?;
        let topic = Self::tx_usage(tx, records, &topic_key)?.refunded(record.charge)?;
        Self::tx_put_usage(tx, records, TOTAL_USAGE.to_vec(), total)?;
        Self::tx_put_usage(tx, records, source_key, source)?;
        Self::tx_put_usage(tx, records, topic_key, topic)?;
        for dep in &record.missing {
            tx.remove(records, key(&[WAITER, dep.as_ref(), op_id.as_ref()]))?;
            Self::tx_waiter_count(tx, records, dep, -1)?;
        }
        tx.remove(
            records,
            key(&[BY_TOPIC, record.topic_id.as_ref(), op_id.as_ref()]),
        )?;
        tx.remove(records, key(&[READY, op_id.as_ref()]))?;
        tx.remove(records, key(&[RECORD, op_id.as_ref()]))?;
        tx.remove(records, key(&[PAYLOAD, op_id.as_ref()]))?;
        Ok(())
    }

    /// `admitted` resolved: its waiters stop waiting on it, and a waiter with
    /// nothing left to wait for joins the ready index.
    pub(super) fn tx_settle_waiters(tx: &mut Tx, records: &Records, admitted: &OpId) -> Result<()> {
        let prefix = key(&[WAITER, admitted.as_ref()]);
        let mut waiters = Vec::new();
        for item in fjall::Readable::prefix(tx, records, prefix) {
            let item_key = item.key()?;
            waiters.push((
                item_key.to_vec(),
                id_at(item_key.as_ref(), WAITER.len() + OpId::LEN)?,
            ));
        }
        if waiters.is_empty() {
            return Ok(());
        }
        tx.remove(records, key(&[WAITER_COUNT, admitted.as_ref()]))?;
        for (waiter_key, waiter) in waiters {
            tx.remove(records, waiter_key)?;
            let Some(mut record) = Self::tx_pending_record(tx, records, &waiter)? else {
                continue;
            };
            record.missing.remove(admitted);
            if record.missing.is_empty() {
                Self::tx_put(tx, records, key(&[READY, waiter.as_ref()]), &())?;
            }
            Self::tx_put(tx, records, key(&[RECORD, waiter.as_ref()]), &record)?;
        }
        Ok(())
    }

    /// Every buffered op that transitively waits on `dep_id`, excluding it.
    fn tx_waiter_closure(tx: &Tx, records: &Records, dep_id: &OpId) -> Result<BTreeSet<OpId>> {
        let mut frontier = vec![*dep_id];
        let mut seen = BTreeSet::new();
        while let Some(dep) = frontier.pop() {
            for item in fjall::Readable::prefix(tx, records, key(&[WAITER, dep.as_ref()])) {
                let waiter = id_at(item.key()?.as_ref(), WAITER.len() + OpId::LEN)?;
                if seen.insert(waiter) {
                    frontier.push(waiter);
                }
            }
        }
        seen.remove(dep_id);
        Ok(seen)
    }

    pub(super) fn tx_purge_waiters(tx: &mut Tx, records: &Records, dep_id: &OpId) -> Result<usize> {
        let closure = Self::tx_waiter_closure(tx, records, dep_id)?;
        for op_id in &closure {
            Self::tx_remove_pending(tx, records, op_id)?;
        }
        Ok(closure.len())
    }

    /// Drop `op_id` and what waits on it, remembering every dropped id as
    /// rejected for its topic, or nothing once `op_id` is no longer buffered.
    pub(super) fn tx_reject_subtree(tx: &mut Tx, records: &Records, op_id: &OpId) -> Result<usize> {
        let Some(record) = Self::tx_pending_record(tx, records, op_id)? else {
            return Ok(0);
        };
        let topic_id = record.topic_id;
        let mut subtree = Self::tx_waiter_closure(tx, records, op_id)?;
        subtree.insert(*op_id);
        let range_key = key(&[REJECTED_RANGE, topic_id.as_ref()]);
        let (mut oldest, mut next): (u64, u64) =
            Self::tx_get(tx, records, range_key.as_slice())?.unwrap_or_default();
        for id in &subtree {
            Self::tx_remove_pending(tx, records, id)?;
            let marker = key(&[REJECTED, topic_id.as_ref(), id.as_ref()]);
            if fjall::Readable::contains_key(tx, records, marker.as_slice())? {
                continue;
            }
            Self::tx_put(tx, records, marker, &next)?;
            Self::tx_put(
                tx,
                records,
                key(&[REJECTED_ORDER, topic_id.as_ref(), &next.to_be_bytes()]),
                id,
            )?;
            next += 1;
        }
        while next - oldest > MAX_REJECTED as u64 {
            let order_key = key(&[REJECTED_ORDER, topic_id.as_ref(), &oldest.to_be_bytes()]);
            if let Some(dropped) = Self::tx_get::<OpId>(tx, records, order_key.as_slice())? {
                tx.remove(
                    records,
                    key(&[REJECTED, topic_id.as_ref(), dropped.as_ref()]),
                )?;
            }
            tx.remove(records, order_key)?;
            oldest += 1;
        }
        Self::tx_put(tx, records, range_key, &(oldest, next))?;
        Ok(subtree.len())
    }

    /// Remove every buffered op and rejection marker of `topic_id`.
    pub(super) fn tx_reset_pending(
        tx: &mut Tx,
        records: &Records,
        topic_id: &TopicId,
    ) -> Result<()> {
        let mut ids = Vec::new();
        for item in fjall::Readable::prefix(tx, records, key(&[BY_TOPIC, topic_id.as_ref()])) {
            ids.push(id_at(item.key()?.as_ref(), BY_TOPIC.len() + TopicId::LEN)?);
        }
        for op_id in ids {
            Self::tx_remove_pending(tx, records, &op_id)?;
        }
        for prefix in [REJECTED, REJECTED_ORDER] {
            Self::tx_remove_prefix(tx, records, &key(&[prefix, topic_id.as_ref()]))?;
        }
        tx.remove(records, key(&[REJECTED_RANGE, topic_id.as_ref()]))?;
        Ok(())
    }

    /// Dependencies buffered ops of `topic_id` still wait for, read from the
    /// small records alone.
    pub(super) fn read_pending_missing(
        tx: &impl fjall::Readable,
        records: &Records,
        topic_id: &TopicId,
    ) -> Result<BTreeSet<OpId>> {
        let mut out = BTreeSet::new();
        for item in fjall::Readable::prefix(tx, records, key(&[BY_TOPIC, topic_id.as_ref()])) {
            let op_id = id_at(item.key()?.as_ref(), BY_TOPIC.len() + TopicId::LEN)?;
            if let Some(record) = Self::tx_pending_record(tx, records, &op_id)? {
                out.extend(record.missing);
            }
        }
        Ok(out)
    }

    fn read_pending_entry(
        tx: &impl fjall::Readable,
        records: &Records,
        op_id: &OpId,
    ) -> Result<Option<(PeerId, Op)>> {
        let Some(record) = Self::tx_pending_record(tx, records, op_id)? else {
            return Ok(None);
        };
        let payload: Option<Op> = Self::tx_get(tx, records, key(&[PAYLOAD, op_id.as_ref()]))?;
        Ok(payload.map(|op| (record.source, op)))
    }

    pub(super) fn read_pending_waiters(&self, dep_id: &OpId) -> Result<Vec<(PeerId, Op)>> {
        let read_tx = self.snapshot()?;
        let mut out = Vec::new();
        for item in
            fjall::Readable::prefix(&read_tx, &self.records, key(&[WAITER, dep_id.as_ref()]))
        {
            let op_id = id_at(item.key()?.as_ref(), WAITER.len() + OpId::LEN)?;
            out.extend(Self::read_pending_entry(&read_tx, &self.records, &op_id)?);
        }
        self.counters.count_payloads(out.len());
        Ok(out)
    }

    pub(super) fn read_ready_after(
        &self,
        after: Option<&OpId>,
        limit: usize,
    ) -> Result<Vec<(PeerId, Op)>> {
        let read_tx = self.snapshot()?;
        let start = match after {
            Some(after) => Bound::Excluded(key(&[READY, after.as_ref()])),
            None => Bound::Included(READY.to_vec()),
        };
        let mut out = Vec::new();
        for item in fjall::Readable::range(
            &read_tx,
            &self.records,
            (start, Bound::Excluded(READY_END.to_vec())),
        ) {
            if out.len() >= limit {
                break;
            }
            let op_id = id_at(item.key()?.as_ref(), READY.len())?;
            out.extend(Self::read_pending_entry(&read_tx, &self.records, &op_id)?);
        }
        self.counters.count_payloads(out.len());
        Ok(out)
    }

    /// Store a buffered op carried over by an upgrade. Nothing is refused:
    /// an old pool above today's limits keeps every record it holds.
    pub(super) fn tx_import_pending(
        tx: &mut Tx,
        records: &Records,
        source_peer: PeerId,
        op: &Op,
        missing: BTreeSet<OpId>,
    ) -> Result<()> {
        let charge = pending_op_bytes(op)? as u64;
        let topic_id = op.signed.body.topic_id;
        for dep in &missing {
            Self::tx_waiter_count(tx, records, dep, 1)?;
            Self::tx_put(
                tx,
                records,
                key(&[WAITER, dep.as_ref(), op.id.as_ref()]),
                &(),
            )?;
        }
        if missing.is_empty() {
            Self::tx_put(tx, records, key(&[READY, op.id.as_ref()]), &())?;
        }
        let source_key = key(&[SOURCE_USAGE, source_peer.as_ref()]);
        let topic_key = key(&[TOPIC_USAGE, topic_id.as_ref()]);
        let total = Self::tx_usage(tx, records, TOTAL_USAGE)?.charged(charge);
        let source = Self::tx_usage(tx, records, &source_key)?.charged(charge);
        let topic = Self::tx_usage(tx, records, &topic_key)?.charged(charge);
        Self::tx_put_usage(tx, records, TOTAL_USAGE.to_vec(), total)?;
        Self::tx_put_usage(tx, records, source_key, source)?;
        Self::tx_put_usage(tx, records, topic_key, topic)?;
        Self::tx_put(
            tx,
            records,
            key(&[BY_TOPIC, topic_id.as_ref(), op.id.as_ref()]),
            &(),
        )?;
        Self::tx_put(tx, records, key(&[PAYLOAD, op.id.as_ref()]), op)?;
        let record = PendingRecord {
            source: source_peer,
            topic_id,
            missing,
            charge,
        };
        Self::tx_put(tx, records, key(&[RECORD, op.id.as_ref()]), &record)
    }

    /// Stored pool usage in total and for `source_peer`, for tests.
    #[cfg(test)]
    pub(crate) fn pending_usage(&self, source_peer: &PeerId) -> (u64, u64, u64, u64) {
        let read_tx = self.snapshot().expect("fjall snapshot");
        let total = Self::tx_usage(&read_tx, &self.records, TOTAL_USAGE).unwrap();
        let source = Self::tx_usage(
            &read_tx,
            &self.records,
            &key(&[SOURCE_USAGE, source_peer.as_ref()]),
        )
        .unwrap();
        (total.ops, total.bytes, source.ops, source.bytes)
    }

    /// Check the parts of a pending insertion that need no transaction.
    pub(super) fn pending_charge(op: &Op, meta: &OpMeta) -> Result<u64> {
        if meta.topic_id != op.signed.body.topic_id || meta.id != op.id {
            return Err(Error::TopicMismatch);
        }
        if meta.missing_deps.len() > MAX_MISSING_DEPS {
            return Err(Error::Storage(
                "pending op has too many missing deps".into(),
            ));
        }
        Ok(pending_op_bytes(op)? as u64)
    }
}
