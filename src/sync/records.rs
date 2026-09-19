//! Bounded immutable operation retention, separate from traversal workspace.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::storage::SnapshotRead;
use crate::{Error, Op, OpId, TopicPayload};

const CACHE_BYTES: usize = 64 * 1024 * 1024;
const POOL_BYTES: usize = 256 * 1024 * 1024;

#[derive(Default)]
pub(super) struct RecordPool {
    live: AtomicUsize,
    cached: AtomicUsize,
}

impl RecordPool {
    #[cfg(test)]
    pub(super) fn bytes(&self) -> usize {
        self.live.load(Ordering::Acquire)
    }
}

pub(super) struct Records {
    pool: Arc<RecordPool>,
    records: BTreeMap<OpId, Record>,
    bytes: usize,
}

pub(super) struct Record {
    pub(super) op: Op,
    claim: Claim,
    owned: bool,
}

#[derive(Debug)]
pub(super) enum LoadError {
    Yield,
    Failed(Error),
}

impl From<Error> for LoadError {
    fn from(error: Error) -> Self {
        Self::Failed(error)
    }
}

struct Claim {
    pool: Arc<RecordPool>,
    bytes: usize,
    cached: bool,
}

fn charge(bytes: usize) -> usize {
    bytes
        .saturating_mul(3)
        .saturating_add(3 * size_of::<Op>() + 256)
}

impl Records {
    pub(super) fn new(pool: Arc<RecordPool>) -> Self {
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

    pub(super) fn take(
        &mut self,
        read: &dyn SnapshotRead,
        id: &OpId,
        slice: &mut crate::sync::slice::Slice,
    ) -> std::result::Result<Option<Record>, LoadError> {
        if let Some(mut record) = self.records.remove(id) {
            self.bytes -= record.claim.bytes;
            record.claim.cached = false;
            self.pool
                .cached
                .fetch_sub(record.claim.bytes, Ordering::AcqRel);
            return Ok(Some(record));
        }
        let mut claim = None;
        let mut denied = None;
        let op = read.get_reserved_op(id, &mut |bytes| {
            if bytes > crate::sync::MAX_PAGE_BYTES {
                return Err(Error::SyncCapacity(
                    "operation exceeds page capacity".into(),
                ));
            }
            if !slice.charge_decode(bytes)? {
                let marker = Arc::new(Error::SyncCapacity(
                    "slice decode allowance exhausted".into(),
                ));
                denied = Some(Arc::clone(&marker));
                return Err(Error::Shared(marker));
            }
            let bytes = charge(bytes);
            loop {
                if crate::sync::space::reserve_bytes(&self.pool.live, bytes, POOL_BYTES) {
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
                cached: false,
            });
            Ok(())
        });
        let op = match op {
            Ok(op) => op,
            // Only our own admission refusal yields; backend errors retain their cause.
            Err(Error::Shared(source))
                if denied
                    .as_ref()
                    .is_some_and(|marker| Arc::ptr_eq(marker, &source)) =>
            {
                return Err(LoadError::Yield);
            }
            Err(error) => return Err(error.into()),
        };
        let Some(op) = op else {
            return Ok(None);
        };
        let mut claim = claim.ok_or_else(|| Error::Storage("operation was not reserved".into()))?;
        let actual = charge(postcard::experimental::serialized_size(&op).map_err(Error::from)?);
        if actual > claim.bytes {
            return Err(Error::SyncCapacity("operation exceeded its reservation".into()).into());
        }
        claim
            .pool
            .live
            .fetch_sub(claim.bytes - actual, Ordering::AcqRel);
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
        // Idle caches leave room for both bulk workers' largest decoded records.
        let cache_limit = POOL_BYTES - 2 * charge(crate::sync::MAX_PAGE_BYTES);
        if !crate::sync::space::reserve_bytes(&self.pool.cached, record.claim.bytes, cache_limit) {
            return;
        }
        record.claim.cached = true;
        record.own_payload();
        self.bytes += record.claim.bytes;
        self.records.insert(record.op.id, record);
    }
}

impl Record {
    fn own_payload(&mut self) {
        if !self.owned {
            if let TopicPayload::Event(event) = &mut self.op.signed.body.payload {
                event.payload = bytes::Bytes::copy_from_slice(&event.payload);
            }
            self.owned = true;
        }
    }

    pub(super) fn into_op(mut self) -> Op {
        self.own_payload();
        self.op
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        if self.cached {
            self.pool.cached.fetch_sub(self.bytes, Ordering::AcqRel);
        }
        self.pool.live.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use crate::sync::records::*;
    use crate::tests::support::*;

    fn read_record(
        records: &mut Records,
        read: &dyn SnapshotRead,
        id: &OpId,
    ) -> crate::Result<Option<Record>> {
        let mut slice = crate::sync::slice::Slice::new(Arc::default(), 1024, 0)?;
        match records.take(read, id, &mut slice) {
            Ok(record) => Ok(record),
            Err(LoadError::Failed(error)) => Err(error),
            Err(LoadError::Yield) => panic!("fresh record slice exhausted"),
        }
    }

    fn stored_event() -> (MemoryStorage, OpId) {
        stored_payload(Bytes::from(vec![0; 1024 * 1024]).slice(..1))
    }

    fn stored_payload(payload: Bytes) -> (MemoryStorage, OpId) {
        stored_in(MemoryStorage::new(), payload)
    }

    fn stored_in<S: Storage>(store: S, payload: Bytes) -> (S, OpId) {
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
                    payload,
                },
                &signer,
            )
            .unwrap();
        (store, op.id)
    }

    struct DecodeProbe<'a> {
        read: &'a dyn SnapshotRead,
        decoded: std::cell::Cell<usize>,
        failure: std::cell::RefCell<Option<Error>>,
    }

    impl SnapshotRead for DecodeProbe<'_> {
        fn get_reserved_op(
            &self,
            id: &OpId,
            reserve: &mut dyn FnMut(usize) -> crate::Result<()>,
        ) -> crate::Result<Option<Op>> {
            let result = self.read.get_reserved_op(id, reserve);
            if matches!(&result, Ok(Some(_))) {
                self.decoded.set(self.decoded.get() + 1);
            }
            match self.failure.borrow_mut().take() {
                Some(error) => Err(error),
                None => result,
            }
        }

        fn get_op(&self, id: &OpId) -> crate::Result<Option<Op>> {
            self.read.get_op(id)
        }

        fn get_meta(&self, id: &OpId) -> crate::Result<Option<crate::storage::OpMeta>> {
            self.read.get_meta(id)
        }

        fn topic_view(
            &self,
            topic: &TopicId,
            peer: Option<&PeerId>,
        ) -> crate::Result<Option<crate::storage::TopicView>> {
            self.read.topic_view(topic, peer)
        }

        fn dep_resolvable(&self, id: &OpId) -> crate::Result<bool> {
            self.read.dep_resolvable(id)
        }

        fn actor_range(
            &self,
            topic: &TopicId,
            actor: &ActorId,
            after: u64,
            limit: usize,
        ) -> crate::Result<Vec<(u64, OpId)>> {
            self.read.actor_range(topic, actor, after, limit)
        }

        fn list_op_ids(&self, topic: &TopicId) -> crate::Result<BTreeSet<OpId>> {
            self.read.list_op_ids(topic)
        }
    }

    fn decode_slices<S: Storage>(store: S) {
        let (store, id) = stored_in(store, Bytes::from(vec![7; 4096]));
        let op = store.get_op(&id).unwrap().unwrap();
        let bytes = postcard::experimental::serialized_size(&op).unwrap();
        let pool = Arc::new(RecordPool::default());
        let mut records = Records::new(Arc::clone(&pool));
        let work = Arc::new(crate::sync::slice::PageWork::default());
        let mut slice = crate::sync::slice::Slice::new(Arc::clone(&work), 16, 0)
            .unwrap()
            .with_decode_limit(bytes);
        store
            .read_snapshot(|read| {
                let probe = DecodeProbe {
                    read,
                    decoded: Default::default(),
                    failure: Default::default(),
                };
                let record = records.take(&probe, &id, &mut slice).unwrap().unwrap();
                assert_eq!(probe.decoded.get(), 1);
                records.keep(record);
                let cached = records.take(&probe, &id, &mut slice).unwrap().unwrap();
                assert_eq!(probe.decoded.get(), 1, "cached records require no decode");
                drop(cached);
                assert_eq!(pool.bytes(), 0);
                assert!(matches!(
                    records.take(&probe, &id, &mut slice),
                    Err(LoadError::Yield)
                ));
                assert_eq!(probe.decoded.get(), 1, "exhaustion must precede decoding");
                assert_eq!(pool.bytes(), 0);
                let mut resumed = crate::sync::slice::Slice::new(Arc::clone(&work), 16, 0)?
                    .with_decode_limit(bytes);
                let record = records.take(&probe, &id, &mut resumed).unwrap().unwrap();
                assert_eq!(probe.decoded.get(), 2);
                assert_eq!(record.op, op);
                drop(record);
                let mut small = crate::sync::slice::Slice::new(Arc::clone(&work), 16, 0)?
                    .with_decode_limit(bytes - 1);
                assert!(matches!(
                    records.take(&probe, &id, &mut small),
                    Err(LoadError::Failed(Error::SyncCapacity(_)))
                ));
                assert_eq!(probe.decoded.get(), 2);
                let failure = Arc::new(Error::SyncCapacity(
                    "slice decode allowance exhausted".into(),
                ));
                *probe.failure.borrow_mut() = Some(Error::Shared(Arc::clone(&failure)));
                match records.take(&probe, &id, &mut slice) {
                    Err(LoadError::Failed(Error::Shared(source))) => {
                        assert!(Arc::ptr_eq(&source, &failure))
                    }
                    _ => panic!("backend failure was mistaken for slice exhaustion"),
                }
                assert_eq!(probe.decoded.get(), 2);
                assert_eq!(pool.bytes(), 0);
                Ok(())
            })
            .unwrap();
        assert_eq!(work.snapshot(0).decoded, (2 * bytes) as u64);
    }

    #[test]
    fn decoding_is_sliced() {
        decode_slices(MemoryStorage::new());
    }

    #[cfg(feature = "fjall")]
    #[test]
    fn fjall_decoding_sliced() {
        let directory = tempfile::tempdir().unwrap();
        decode_slices(crate::FjallStorage::open(directory.path()).unwrap());
    }

    #[test]
    fn direct_payload_owned() {
        let (store, id) = stored_event();
        let original = store.get_op(&id).unwrap().unwrap();
        let pool = Arc::new(RecordPool::default());
        let mut records = Records::new(Arc::clone(&pool));
        let record = store
            .read_snapshot(|read| read_record(&mut records, read, &id))
            .unwrap()
            .unwrap();
        assert!(pool.bytes() > 0);
        let output = record.into_op();
        let (TopicPayload::Event(before), TopicPayload::Event(after)) =
            (&original.signed.body.payload, &output.signed.body.payload)
        else {
            unreachable!()
        };
        assert_eq!(before.payload, after.payload);
        assert_ne!(before.payload.as_ptr(), after.payload.as_ptr());
        assert_eq!(output, original);
        output.validate().unwrap();
        assert_eq!(pool.bytes(), 0);
    }

    #[test]
    fn idle_caches_progress() {
        let (store, id) = stored_payload(Bytes::from(vec![0; 2 * 1024 * 1024]));
        let pool = Arc::new(RecordPool::default());
        let mut goals = Vec::new();
        for n in 0..128 {
            let mut records = Records::new(Arc::clone(&pool));
            let record = store
                .read_snapshot(|read| read_record(&mut records, read, &id))
                .unwrap_or_else(|error| panic!("goal {n} starved by idle caches: {error}"))
                .unwrap();
            records.keep(record);
            goals.push(records);
        }
        let (large, id) = stored_payload(Bytes::from(vec![0; 16 * 1024 * 1024 - 4096]));
        let mut first = Records::new(Arc::clone(&pool));
        let mut second = Records::new(Arc::clone(&pool));
        let first = large
            .read_snapshot(|read| read_record(&mut first, read, &id))
            .unwrap()
            .unwrap();
        let second = large
            .read_snapshot(|read| read_record(&mut second, read, &id))
            .unwrap()
            .unwrap();
        assert!(pool.bytes() <= POOL_BYTES);
        drop((first, second));
        drop(goals);
        assert_eq!(pool.bytes(), 0);
    }

    #[test]
    fn claims_release() {
        let (store, id) = stored_event();
        let pool = Arc::new(RecordPool::default());
        let mut records = Records::new(Arc::clone(&pool));
        let record = store
            .read_snapshot(|read| read_record(&mut records, read, &id))
            .unwrap()
            .unwrap();
        let bytes = record.claim.bytes;
        assert_eq!(pool.bytes(), bytes);
        let busy = Claim {
            pool: Arc::clone(&pool),
            bytes: POOL_BYTES - bytes,
            cached: false,
        };
        pool.live.fetch_add(busy.bytes, Ordering::AcqRel);
        let mut other = Records::new(Arc::clone(&pool));
        assert!(matches!(
            store.read_snapshot(|read| read_record(&mut other, read, &id)),
            Err(Error::SyncCapacity(_))
        ));
        assert_eq!(pool.bytes(), POOL_BYTES);
        records.keep(record);
        drop(busy);
        let before = store.counters().op_reads;
        let record = store
            .read_snapshot(|read| read_record(&mut records, read, &id))
            .unwrap()
            .unwrap();
        assert_eq!(
            store.counters().op_reads,
            before,
            "reuse the reserved operation"
        );
        assert_eq!(pool.bytes(), bytes);
        assert!(
            std::panic::catch_unwind(move || {
                let _record = record;
                panic!("started planner job failed");
            })
            .is_err()
        );
        assert_eq!(pool.bytes(), 0);
    }

    #[test]
    fn payload_owned() {
        let (store, id) = stored_event();
        let pool = Arc::new(RecordPool::default());
        let mut records = Records::new(Arc::clone(&pool));
        let original = store.get_op(&id).unwrap().unwrap();
        let record = store
            .read_snapshot(|read| read_record(&mut records, read, &id))
            .unwrap()
            .unwrap();
        records.keep(record);
        let record = store
            .read_snapshot(|read| read_record(&mut records, read, &id))
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
        assert_eq!(pool.bytes(), 0);
    }
}
