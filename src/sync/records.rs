//! Bounded immutable operation retention, separate from traversal workspace.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::storage::SnapshotRead;
use crate::{Error, Op, OpId, Result, TopicPayload};

const CACHE_BYTES: usize = 64 * 1024 * 1024;
const POOL_BYTES: usize = 256 * 1024 * 1024;

pub(super) struct Records {
    pool: Arc<AtomicUsize>,
    records: BTreeMap<OpId, Record>,
    bytes: usize,
}

pub(super) struct Record {
    pub(super) op: Op,
    claim: Claim,
    owned: bool,
}

struct Claim {
    pool: Arc<AtomicUsize>,
    bytes: usize,
}

fn charge(bytes: usize) -> usize {
    bytes
        .saturating_mul(3)
        .saturating_add(3 * size_of::<Op>() + 256)
}

impl Records {
    pub(super) fn new(pool: Arc<AtomicUsize>) -> Self {
        Self {
            pool,
            records: BTreeMap::new(),
            bytes: 0,
        }
    }

    pub(super) fn contains(&self, id: &OpId) -> bool {
        self.records.contains_key(id)
    }

    fn evict(&mut self) -> bool {
        let Some((_, record)) = self.records.pop_first() else {
            return false;
        };
        self.bytes -= record.claim.bytes;
        true
    }

    pub(super) fn take(&mut self, read: &dyn SnapshotRead, id: &OpId) -> Result<Option<Record>> {
        if let Some(record) = self.records.remove(id) {
            self.bytes -= record.claim.bytes;
            return Ok(Some(record));
        }
        let mut claim = None;
        let op = read.get_reserved_op(id, &mut |bytes| {
            if bytes > super::MAX_PAGE_BYTES {
                return Err(Error::SyncCapacity(
                    "operation exceeds page capacity".into(),
                ));
            }
            let bytes = charge(bytes);
            loop {
                if self
                    .pool
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                        held.checked_add(bytes).filter(|sum| *sum <= POOL_BYTES)
                    })
                    .is_ok()
                {
                    break;
                }
                if !self.evict() {
                    return Err(Error::SyncCapacity(
                        "operation retention pool is occupied".into(),
                    ));
                }
            }
            claim = Some(Claim {
                pool: Arc::clone(&self.pool),
                bytes,
            });
            Ok(())
        })?;
        let Some(op) = op else {
            return Ok(None);
        };
        let mut claim = claim.ok_or_else(|| Error::Storage("operation was not reserved".into()))?;
        let actual = charge(postcard::experimental::serialized_size(&op)?);
        if actual > claim.bytes {
            return Err(Error::SyncCapacity(
                "operation exceeded its reservation".into(),
            ));
        }
        claim.pool.fetch_sub(claim.bytes - actual, Ordering::AcqRel);
        claim.bytes = actual;
        Ok(Some(Record {
            op,
            claim,
            owned: false,
        }))
    }

    pub(super) fn keep(&mut self, mut record: Record) {
        if record.claim.bytes > CACHE_BYTES {
            return;
        }
        if let Some(old) = self.records.remove(&record.op.id) {
            self.bytes -= old.claim.bytes;
        }
        while self.bytes + record.claim.bytes > CACHE_BYTES && self.evict() {}
        if !record.owned {
            if let TopicPayload::Event(event) = &mut record.op.signed.body.payload {
                event.payload = bytes::Bytes::copy_from_slice(&event.payload);
            }
            record.owned = true;
        }
        self.bytes += record.claim.bytes;
        self.records.insert(record.op.id, record);
    }
}

impl Record {
    pub(super) fn into_op(self) -> Op {
        self.op
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        self.pool.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::support::*;

    fn stored_event() -> (MemoryStorage, OpId) {
        let store = MemoryStorage::new();
        let signer = Ed25519Signer::from_bytes(&[219; 32]);
        let topic = TopicId::hash(b"record-reservations");
        let actor = actor_id_for(topic, signer.peer_id());
        let log = oplog::Oplog::with_storage(store.clone());
        log.create_topic_genesis(
            topic,
            actor,
            TopicGenesis::new(Note::TYPE_ID, [signer.peer_id()]),
            &signer,
        )
        .unwrap();
        let op = log
            .create_event_op(
                topic,
                actor,
                EventEnvelope {
                    type_id: Note::TYPE_ID.into(),
                    payload: Bytes::from(vec![0; 1024 * 1024]).slice(..1),
                },
                &signer,
            )
            .unwrap();
        (store, op.id)
    }

    #[test]
    fn claims_release() {
        let (store, id) = stored_event();
        let pool = Arc::new(AtomicUsize::new(0));
        let mut records = Records::new(Arc::clone(&pool));
        let record = store
            .read_snapshot(|read| records.take(read, &id))
            .unwrap()
            .unwrap();
        let bytes = record.claim.bytes;
        assert_eq!(pool.load(Ordering::Acquire), bytes);
        let busy = Claim {
            pool: Arc::clone(&pool),
            bytes: POOL_BYTES - bytes,
        };
        pool.fetch_add(busy.bytes, Ordering::AcqRel);
        let mut other = Records::new(Arc::clone(&pool));
        assert!(matches!(
            store.read_snapshot(|read| other.take(read, &id)),
            Err(Error::SyncCapacity(_))
        ));
        assert_eq!(pool.load(Ordering::Acquire), POOL_BYTES);
        records.keep(record);
        drop(busy);
        let before = store.counters().op_reads;
        let record = store
            .read_snapshot(|read| records.take(read, &id))
            .unwrap()
            .unwrap();
        assert_eq!(
            store.counters().op_reads,
            before,
            "reuse the reserved operation"
        );
        assert_eq!(pool.load(Ordering::Acquire), bytes);
        assert!(
            std::panic::catch_unwind(move || {
                let _record = record;
                panic!("started planner job failed");
            })
            .is_err()
        );
        assert_eq!(pool.load(Ordering::Acquire), 0);
    }

    #[test]
    fn payload_owned() {
        let (store, id) = stored_event();
        let pool = Arc::new(AtomicUsize::new(0));
        let mut records = Records::new(Arc::clone(&pool));
        let original = store.get_op(&id).unwrap().unwrap();
        let record = store
            .read_snapshot(|read| records.take(read, &id))
            .unwrap()
            .unwrap();
        records.keep(record);
        let record = store
            .read_snapshot(|read| records.take(read, &id))
            .unwrap()
            .unwrap();
        let (TopicPayload::Event(before), TopicPayload::Event(after)) = (
            &original.signed.body.payload,
            &record.op.signed.body.payload,
        ) else {
            unreachable!()
        };
        assert_eq!(before.payload, after.payload);
        assert_ne!(before.payload.as_ptr(), after.payload.as_ptr());
        let copied = record.into_op();
        copied.validate().unwrap();
        assert_eq!(copied, original);
        assert_eq!(pool.load(Ordering::Acquire), 0);
    }
}
