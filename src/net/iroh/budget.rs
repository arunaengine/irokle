// SPDX-License-Identifier: MIT OR Apache-2.0
//! Bytes a net owns while it works. Every charge comes from a node-wide pool,
//! moves with the data it covers and is released when that data is dropped.
//!
//! Wire lengths are measured exactly. Decoded sizes are conservative upper
//! bounds: `DECODED_FACTOR` heap bytes per wire byte, plus the inline size of
//! each decoded message and operation. None of them is measured memory.
//!
//! A task never waits on a pool while it holds bytes of that pool, so charges
//! cannot wait on each other. Growth that would need such a wait fails instead.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::net::frame::{MAX_FRAME_LEN, MAX_SYNC_DATA_OPS_PER_MESSAGE};
use crate::sync::{PageBudget, SyncCredit, SyncMessage};

/// Frames up to this size of a kind other than data may use the control
/// pool, so control exchanges keep flowing while data fills its own pool.
pub(super) const CONTROL_FRAME_BYTES: usize = 64 * 1024;
const CONTROL_POOL_BYTES: usize = 16 * 1024 * 1024;
/// Bytes all served streams may retain between reading and replying.
pub(super) const SESSION_POOL_BYTES: usize = 128 * 1024 * 1024;
/// Heap bytes one wire byte may decode into. Keys of 32 bytes in B-tree nodes
/// at minimum fill, and vectors grown by doubling, stay below it.
pub(super) const DECODED_FACTOR: usize = 3;
/// The postcard variant index of `SyncMessage::Data`.
pub(super) const DATA_TAG: u8 = 4;
/// Buffer a writer needs to encode its largest message.
const ENCODE_BYTES: usize = MAX_FRAME_LEN + 4;

/// What charged bytes hold.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum OwnedClass {
    /// Inbound frames of served streams with their decoded messages, until
    /// the storage job handling them ends.
    Frames,
    /// Messages and replies a served stream retains until its reply is written.
    Session,
    /// Planned reply pages and encoding buffers, until written.
    Output,
    /// Responses of requester exchanges, until the caller drops them.
    Results,
}

/// Owned bytes by class, now and at their highest, and running storage jobs.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OwnedBytes {
    pub current: BTreeMap<OwnedClass, u64>,
    pub peak: BTreeMap<OwnedClass, u64>,
    /// Highest sum of all classes at one time.
    pub peak_total: u64,
    pub jobs: u64,
    pub peak_jobs: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Pool {
    Data,
    Control,
    Session,
    Results,
}

/// Node-wide byte pools and their counters.
pub(super) struct ByteBudget {
    data: Arc<Semaphore>,
    control: Arc<Semaphore>,
    session: Arc<Semaphore>,
    results: Arc<Semaphore>,
    capacity: usize,
    session_capacity: usize,
    counters: Mutex<OwnedBytes>,
}

/// Bytes charged to one class from one pool. Dropping it releases them.
pub(super) struct Charge {
    permit: OwnedSemaphorePermit,
    class: OwnedClass,
    budget: Arc<ByteBudget>,
}

/// One running storage job, counted until dropped.
pub(super) struct JobCount(Arc<ByteBudget>);

impl ByteBudget {
    /// Pools of `data_bytes` for data and results and `session_bytes` for
    /// sessions. The data pool always fits a largest frame and half of it a
    /// largest page, so no single charge can wait forever.
    pub(super) fn new(data_bytes: usize, session_bytes: usize) -> Arc<Self> {
        let page = PageBudget::from_credit(SyncCredit::default());
        let least = Self::frame_charge(MAX_FRAME_LEN, true)
            .max(2 * (Self::page_bound(MAX_FRAME_LEN, page.ops) + ENCODE_BYTES));
        let capacity = data_bytes.max(least);
        let session_capacity = session_bytes.max(Self::held_bound(MAX_FRAME_LEN));
        Arc::new(Self {
            data: Arc::new(Semaphore::new(capacity)),
            control: Arc::new(Semaphore::new(CONTROL_POOL_BYTES)),
            session: Arc::new(Semaphore::new(session_capacity)),
            results: Arc::new(Semaphore::new(capacity)),
            capacity,
            session_capacity,
            counters: Mutex::default(),
        })
    }

    /// Bytes a frame of `len` wire bytes is charged while raw and decoded
    /// copies coexist. A data frame may decode the most operations allowed.
    pub(super) fn frame_charge(len: usize, data: bool) -> usize {
        let ops = if data {
            MAX_SYNC_DATA_OPS_PER_MESSAGE
        } else {
            0
        };
        len.saturating_add(Self::decoded_bound(len, ops))
    }

    /// Bytes a decoded message of `len` wire bytes and `ops` operations holds.
    pub(super) fn decoded_bound(len: usize, ops: usize) -> usize {
        DECODED_FACTOR
            .saturating_mul(len)
            .saturating_add(ops.saturating_mul(size_of::<crate::Op>()))
            .saturating_add(2 * size_of::<SyncMessage>())
    }

    /// Bytes a planned page of `bytes` wire bytes and `ops` operations holds.
    pub(super) fn page_bound(bytes: usize, ops: usize) -> usize {
        DECODED_FACTOR
            .saturating_mul(bytes)
            .saturating_add(ops.saturating_mul(size_of::<crate::Op>()))
    }

    /// Bytes a retained message of `len` framed wire bytes holds.
    pub(super) fn held_bound(len: usize) -> usize {
        Self::decoded_bound(len, 0)
    }

    /// Bytes a served reply planning pages for `pages` needs: those pages and
    /// one encoding buffer, never above half the data pool.
    pub(super) fn output_bound(&self, pages: usize) -> usize {
        pages.saturating_add(ENCODE_BYTES).min(self.capacity / 2)
    }

    /// Page bytes of an output grant of `granted` bytes.
    pub(super) fn page_bytes(granted: usize) -> usize {
        granted.saturating_sub(ENCODE_BYTES)
    }

    fn semaphore(&self, pool: Pool) -> (&Arc<Semaphore>, usize) {
        match pool {
            Pool::Data => (&self.data, self.capacity),
            Pool::Control => (&self.control, CONTROL_POOL_BYTES),
            Pool::Session => (&self.session, self.session_capacity),
            Pool::Results => (&self.results, self.capacity),
        }
    }

    /// The pool an inbound or encoded message of `len` wire bytes uses.
    pub(super) fn frame_pool(len: usize, data: bool) -> Pool {
        if !data && len <= CONTROL_FRAME_BYTES {
            Pool::Control
        } else {
            Pool::Data
        }
    }

    fn counters(&self) -> MutexGuard<'_, OwnedBytes> {
        // Plain counters, so a poisoned lock is still consistent.
        self.counters.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn count(&self, class: OwnedClass, added: usize) {
        let mut counters = self.counters();
        let current = counters.current.entry(class).or_default();
        *current += added as u64;
        let now = *current;
        let peak = counters.peak.entry(class).or_default();
        *peak = (*peak).max(now);
        let total = counters.current.values().sum::<u64>();
        counters.peak_total = counters.peak_total.max(total);
    }

    fn uncount(&self, class: OwnedClass, removed: usize) {
        let mut counters = self.counters();
        let current = counters.current.entry(class).or_default();
        *current = current.saturating_sub(removed as u64);
    }

    /// Waits for `bytes` of `pool`. A caller must hold no bytes of that pool.
    pub(super) async fn wait(
        self: &Arc<Self>,
        pool: Pool,
        bytes: usize,
        class: OwnedClass,
    ) -> io::Result<Charge> {
        let (semaphore, capacity) = self.semaphore(pool);
        if bytes > capacity {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "sync message exceeds the net's byte budget",
            ));
        }
        let permits = u32::try_from(bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "byte charge overflow"))?;
        let permit = Arc::clone(semaphore)
            .acquire_many_owned(permits)
            .await
            .map_err(|_| closed())?;
        Ok(self.charged(permit, class))
    }

    /// Takes `bytes` of `pool` when they are free now.
    pub(super) fn try_take(
        self: &Arc<Self>,
        pool: Pool,
        bytes: usize,
        class: OwnedClass,
    ) -> io::Result<Charge> {
        let (semaphore, _) = self.semaphore(pool);
        let permits = u32::try_from(bytes).map_err(|_| full(pool))?;
        match Arc::clone(semaphore).try_acquire_many_owned(permits) {
            Ok(permit) => Ok(self.charged(permit, class)),
            Err(tokio::sync::TryAcquireError::Closed) => Err(closed()),
            Err(tokio::sync::TryAcquireError::NoPermits) => Err(full(pool)),
        }
    }

    fn charged(self: &Arc<Self>, permit: OwnedSemaphorePermit, class: OwnedClass) -> Charge {
        self.count(class, permit.num_permits());
        Charge {
            permit,
            class,
            budget: Arc::clone(self),
        }
    }

    /// Refuse every waiting and later charge. Held charges stay until dropped.
    pub(super) fn close(&self) {
        for semaphore in [&self.data, &self.control, &self.session, &self.results] {
            semaphore.close();
        }
    }

    pub(super) fn owned(&self) -> OwnedBytes {
        self.counters().clone()
    }

    pub(super) fn job(self: &Arc<Self>) -> JobCount {
        let mut counters = self.counters();
        counters.jobs += 1;
        counters.peak_jobs = counters.peak_jobs.max(counters.jobs);
        JobCount(Arc::clone(self))
    }

    /// Bytes of `pool` not charged now.
    #[cfg(test)]
    pub(super) fn available(&self, pool: Pool) -> usize {
        self.semaphore(pool).0.available_permits()
    }

    #[cfg(test)]
    pub(super) fn capacity(&self, pool: Pool) -> usize {
        self.semaphore(pool).1
    }
}

impl Charge {
    pub(super) fn bytes(&self) -> usize {
        self.permit.num_permits()
    }

    /// Release all but `bytes`.
    pub(super) fn shrink(&mut self, bytes: usize) {
        let excess = self.bytes().saturating_sub(bytes);
        if let Some(released) = self.permit.split(excess) {
            self.budget.uncount(self.class, excess);
            drop(released);
        }
    }

    /// Adds the bytes of `other`, which must come from the same pool.
    pub(super) fn merge(&mut self, mut other: Charge) {
        let bytes = other.bytes();
        self.budget.uncount(other.class, bytes);
        self.budget.count(self.class, bytes);
        if let Some(permit) = other.permit.split(bytes) {
            self.permit.merge(permit);
        }
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.budget.uncount(self.class, self.bytes());
    }
}

impl Drop for JobCount {
    fn drop(&mut self) {
        let mut counters = self.0.counters();
        counters.jobs = counters.jobs.saturating_sub(1);
    }
}

fn closed() -> io::Error {
    io::Error::other("irokle net byte budget is closed")
}

fn full(pool: Pool) -> io::Error {
    let message = match pool {
        Pool::Session => "sync stream retains more than the free session budget",
        Pool::Results => "sync response exceeds the free result budget",
        Pool::Data | Pool::Control => "sync message exceeds the free byte budget",
    };
    io::Error::new(io::ErrorKind::OutOfMemory, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_tag_matches() {
        let data = SyncMessage::Data(crate::sync::SyncData {
            topic_id: crate::TopicId::default(),
            ops: Vec::new(),
        });
        let encoded = crate::net::encode_sync_message(&data).unwrap();
        assert_eq!(encoded[0], DATA_TAG);
    }

    #[test]
    fn charges_move_once() {
        let budget = ByteBudget::new(0, 0);
        let capacity = budget.capacity(Pool::Data);
        let mut first = budget
            .try_take(Pool::Data, 100, OwnedClass::Frames)
            .unwrap();
        let second = budget.try_take(Pool::Data, 50, OwnedClass::Output).unwrap();
        first.merge(second);
        assert_eq!(first.bytes(), 150);
        first.shrink(40);
        assert_eq!(budget.available(Pool::Data), capacity - 40);
        let owned = budget.owned();
        assert_eq!(owned.current[&OwnedClass::Frames], 40);
        assert_eq!(owned.current[&OwnedClass::Output], 0);
        assert_eq!(owned.peak_total, 150);
        drop(first);
        assert_eq!(budget.available(Pool::Data), capacity);
        assert_eq!(budget.owned().current.values().sum::<u64>(), 0);
        assert!(
            budget
                .try_take(Pool::Data, capacity + 1, OwnedClass::Frames)
                .is_err()
        );
        budget.close();
        assert!(budget.try_take(Pool::Data, 1, OwnedClass::Frames).is_err());
    }
}
