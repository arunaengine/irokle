//! Shared reservations before Fjall allocates transaction buffers or writes records.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use crate::{Error, Result};

type Probe = dyn Fn(&Path) -> std::io::Result<u64> + Send + Sync;

/// The records a storage reservation covers. Recovery has a separate allowance.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum StorageDomain {
    Operations,
    Metadata,
    ClockNodes,
    Activation,
    Recovery,
}

/// Admission of transaction buffers and filesystem growth, shared by database facades.
#[derive(Clone)]
pub struct StoragePressure {
    pub minimum_free_bytes: u64,
    pub recovery_free_bytes: u64,
    pub buffer_bytes: u64,
    pub recovery_buffer_bytes: u64,
    probe: Arc<Probe>,
}

impl Default for StoragePressure {
    fn default() -> Self {
        Self {
            minimum_free_bytes: 64 * 1024 * 1024,
            recovery_free_bytes: 1024 * 1024,
            buffer_bytes: 512 * 1024 * 1024,
            recovery_buffer_bytes: 128 * 1024 * 1024,
            probe: Arc::new(|path| fs4::available_space(path)),
        }
    }
}

impl StoragePressure {
    /// The caller supplies current available space for its filesystem allocation.
    pub fn with_probe(
        mut self,
        probe: impl Fn(&Path) -> std::io::Result<u64> + Send + Sync + 'static,
    ) -> Self {
        self.probe = Arc::new(probe);
        self
    }
}

/// Reservations are upper bounds, separate from backend disk and cache measurements.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageUsage {
    pub reserved: BTreeMap<StorageDomain, u64>,
    pub peak_reserved: BTreeMap<StorageDomain, u64>,
    pub committed_bytes: BTreeMap<StorageDomain, u64>,
    pub filesystem_available: u64,
    pub database_bytes: u64,
    pub journal_bytes: u64,
    pub write_buffer_bytes: u64,
    pub clock_cache_bytes: u64,
    pub requires_reopen: bool,
}

struct State {
    policy: StoragePressure,
    usage: StorageUsage,
    buffers: [u64; 2],
    spent: u128,
}

pub(super) struct Pressure {
    path: PathBuf,
    state: Mutex<State>,
}

impl Pressure {
    pub(super) fn shared(path: PathBuf) -> Result<Arc<Self>> {
        type Registry = Mutex<BTreeMap<PathBuf, Weak<Pressure>>>;
        static REGISTRY: OnceLock<Registry> = OnceLock::new();
        let path = path.canonicalize().map_err(Error::StorageProbe)?;
        let mut registry = REGISTRY
            .get_or_init(Mutex::default)
            .lock()
            .map_err(|_| Error::Storage("storage reservation registry poisoned".into()))?;
        registry.retain(|_, value| value.strong_count() > 0);
        if let Some(shared) = registry.get(&path).and_then(Weak::upgrade) {
            return Ok(shared);
        }
        let shared = Arc::new(Self {
            path: path.clone(),
            state: Mutex::new(State {
                policy: StoragePressure::default(),
                usage: StorageUsage::default(),
                buffers: [0, 0],
                spent: 0,
            }),
        });
        registry.insert(path, Arc::downgrade(&shared));
        Ok(shared)
    }

    pub(super) fn configure(&self, policy: StoragePressure) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Storage("storage reservations poisoned".into()))?;
        if state.buffers != [0, 0] {
            return Err(Error::StoragePressure(
                "storage writes are still reserved".into(),
            ));
        }
        state.policy = policy;
        Ok(())
    }

    pub(super) fn usage(&self) -> Result<StorageUsage> {
        Ok(self
            .state
            .lock()
            .map_err(|_| Error::Storage("storage reservations poisoned".into()))?
            .usage
            .clone())
    }

    pub(super) fn uncertain(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .usage
            .requires_reopen = true;
    }

    pub(super) fn begin(
        self: &Arc<Self>,
        recovery: bool,
        sample: impl FnOnce() -> Result<(u64, u64)>,
    ) -> Result<Reservation> {
        let (policy, stamp) = {
            let state = self
                .state
                .lock()
                .map_err(|_| Error::Storage("storage reservations poisoned".into()))?;
            if state.usage.requires_reopen {
                return Err(Error::ReopenRequired(fjall::Error::Poisoned));
            }
            (state.policy.clone(), state.spent)
        };
        let (disk, buffered) = sample()?;
        let available = (policy.probe)(&self.path).map_err(Error::StorageProbe)?;
        let margin = if recovery {
            policy.recovery_free_bytes
        } else {
            policy
                .minimum_free_bytes
                .saturating_add(disk)
                .saturating_add(buffered)
        };
        let limit = if recovery {
            policy.recovery_buffer_bytes
        } else {
            policy.buffer_bytes
        };
        let mut reservation = Reservation {
            owner: Arc::clone(self),
            allowance: available.saturating_sub(margin),
            stamp,
            limit,
            recovery,
            charges: BTreeMap::new(),
            bytes: 0,
            committed: false,
        };
        self.state
            .lock()
            .map_err(|_| Error::Storage("storage reservations poisoned".into()))?
            .usage
            .filesystem_available = available;
        reservation.grow(StorageDomain::Metadata, 32 * 1024)?;
        Ok(reservation)
    }
}

pub(super) struct Reservation {
    owner: Arc<Pressure>,
    allowance: u64,
    stamp: u128,
    limit: u64,
    recovery: bool,
    charges: BTreeMap<StorageDomain, u64>,
    bytes: u64,
    committed: bool,
}

impl Reservation {
    pub(super) fn grow(&mut self, domain: StorageDomain, encoded: u64) -> Result<()> {
        let domain = if self.recovery {
            StorageDomain::Recovery
        } else {
            domain
        };
        let bytes = encoded
            .checked_mul(2)
            .ok_or_else(|| Error::StoragePressure("storage reservation overflow".into()))?;
        let needed = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| Error::StoragePressure("storage reservation overflow".into()))?;
        if needed > self.limit {
            return Err(Error::StorageBuffer {
                required: needed,
                limit: self.limit,
            });
        }
        let mut state = self
            .owner
            .state
            .lock()
            .map_err(|_| Error::Storage("storage reservations poisoned".into()))?;
        let lane = usize::from(self.recovery);
        let held = state.buffers[lane];
        if held.saturating_add(bytes) > self.limit {
            return Err(Error::StoragePressure(
                "storage transaction buffers are occupied".into(),
            ));
        }
        let reserved = state.buffers[0] as u128 + state.buffers[1] as u128;
        if reserved + bytes as u128 + state.spent.saturating_sub(self.stamp)
            > self.allowance as u128
        {
            return Err(Error::StoragePressure(
                "filesystem recovery headroom is reserved".into(),
            ));
        }
        state.buffers[lane] += bytes;
        let current = state.usage.reserved.entry(domain).or_default();
        *current += bytes;
        let now = *current;
        let peak = state.usage.peak_reserved.entry(domain).or_default();
        *peak = (*peak).max(now);
        *self.charges.entry(domain).or_default() += bytes;
        self.bytes = needed;
        Ok(())
    }

    pub(super) fn committed(&mut self) {
        self.committed = true;
    }

    pub(super) fn uncertain(&self) {
        self.owner.uncertain();
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut state = self
            .owner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.buffers[usize::from(self.recovery)] -= self.bytes;
        for (domain, bytes) in &self.charges {
            *state.usage.reserved.entry(*domain).or_default() -= bytes;
            if self.committed {
                *state.usage.committed_bytes.entry(*domain).or_default() += bytes / 2;
            }
        }
        if self.committed {
            state.spent += self.bytes as u128;
        }
    }
}

pub(super) struct Transaction {
    inner: fjall::OptimisticWriteTx,
    reservation: Reservation,
    domain: Option<StorageDomain>,
}

impl Transaction {
    pub(super) fn new(inner: fjall::OptimisticWriteTx, reservation: Reservation) -> Self {
        Self {
            inner,
            reservation,
            domain: None,
        }
    }

    pub(super) fn activation(&mut self) {
        self.domain = Some(StorageDomain::Activation);
    }

    fn charge(&mut self, key: &[u8], bytes: usize) -> Result<()> {
        let domain = self
            .domain
            .unwrap_or_else(|| match (key.get(..1), key.get(..2), key.len()) {
                (_, Some(b"cn"), 66) => StorageDomain::ClockNodes,
                (Some(b"o"), _, 33) | (_, Some(b"po"), 34) => StorageDomain::Operations,
                _ => StorageDomain::Metadata,
            });
        self.reservation.grow(
            domain,
            (key.len() as u64)
                .saturating_add(bytes as u64)
                .saturating_add(96),
        )
    }

    pub(super) fn put<T: serde::Serialize>(
        &mut self,
        records: &fjall::OptimisticTxKeyspace,
        key: impl AsRef<[u8]>,
        value: &T,
    ) -> Result<()> {
        let key = key.as_ref();
        let size = postcard::experimental::serialized_size(value)?;
        self.charge(key, size)?;
        self.inner
            .insert(records, key, postcard::to_allocvec(value)?);
        Ok(())
    }

    pub(super) fn insert(
        &mut self,
        records: &fjall::OptimisticTxKeyspace,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]> + Into<fjall::Slice>,
    ) -> Result<()> {
        self.charge(key.as_ref(), value.as_ref().len())?;
        self.inner.insert(records, key.as_ref(), value.into());
        Ok(())
    }

    pub(super) fn remove(
        &mut self,
        records: &fjall::OptimisticTxKeyspace,
        key: impl AsRef<[u8]>,
    ) -> Result<()> {
        self.charge(key.as_ref(), 0)?;
        self.inner.remove(records, key.as_ref());
        Ok(())
    }

    pub(super) fn commit(mut self) -> Result<std::result::Result<(), fjall::Conflict>> {
        match self.inner.commit() {
            Ok(result) => {
                if result.is_ok() {
                    self.reservation.committed();
                }
                Ok(result)
            }
            Err(error) => {
                self.reservation.uncertain();
                Err(Error::ReopenRequired(error))
            }
        }
    }
}

impl fjall::Readable for Transaction {
    fn get<K: AsRef<[u8]>>(
        &self,
        space: impl AsRef<fjall::Keyspace>,
        key: K,
    ) -> fjall::Result<Option<fjall::Slice>> {
        self.inner.get(space, key)
    }
    fn contains_key<K: AsRef<[u8]>>(
        &self,
        space: impl AsRef<fjall::Keyspace>,
        key: K,
    ) -> fjall::Result<bool> {
        self.inner.contains_key(space, key)
    }
    fn first_key_value(&self, space: impl AsRef<fjall::Keyspace>) -> Option<fjall::Guard> {
        self.inner.first_key_value(space)
    }
    fn last_key_value(&self, space: impl AsRef<fjall::Keyspace>) -> Option<fjall::Guard> {
        self.inner.last_key_value(space)
    }
    fn size_of<K: AsRef<[u8]>>(
        &self,
        space: impl AsRef<fjall::Keyspace>,
        key: K,
    ) -> fjall::Result<Option<u32>> {
        self.inner.size_of(space, key)
    }
    fn iter(&self, space: impl AsRef<fjall::Keyspace>) -> fjall::Iter {
        self.inner.iter(space)
    }
    fn range<K: AsRef<[u8]>, R: std::ops::RangeBounds<K>>(
        &self,
        space: impl AsRef<fjall::Keyspace>,
        range: R,
    ) -> fjall::Iter {
        self.inner.range(space, range)
    }
    fn prefix<K: AsRef<[u8]>>(&self, space: impl AsRef<fjall::Keyspace>, prefix: K) -> fjall::Iter {
        self.inner.prefix(space, prefix)
    }
}

#[cfg(test)]
#[path = "pressure_tests.rs"]
mod tests;
