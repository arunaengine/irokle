// SPDX-License-Identifier: MIT OR Apache-2.0
//! Shared work accounting and bounded slice admission for sync operations.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::{Error, Result};

use super::MAX_PAGE_BYTES;

/// Storage reads one page plan makes before it ends its work slice.
pub(super) const MAX_PAGE_VISITS: usize = 65_536;
/// Estimated bytes one kept plan may hold.
pub(super) const MAX_CONTINUATION_BYTES: usize = 4 * 1024 * 1024;
/// Estimated bytes one slice may plan with. Charges stay until the slice ends, and a chain
/// through twice the actor window activates every actor before it sends.
const MAX_WORKSPACE_BYTES: usize = 8 * 1024 * 1024;

/// Work sync planning performed: storage reads, actor items, remote tips and dependency edges;
/// slices that kept a frontier or replayed without data, and plans resumed; decoded bytes,
/// preparation units, authorization reads and captured raw bytes.
#[derive(Debug, Default)]
pub(crate) struct PageWork {
    visits: AtomicU64,
    actors: AtomicU64,
    tips: AtomicU64,
    edges: AtomicU64,
    ended: AtomicU64,
    resumed: AtomicU64,
    decoded: AtomicU64,
    preparation: AtomicU64,
    authorization_reads: AtomicU64,
    captured: AtomicU64,
}

/// A copy of [`PageWork`] with the bytes kept plans hold at one moment.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PageWorkSnapshot {
    pub(crate) visits: u64,
    pub(crate) actors: u64,
    pub(crate) tips: u64,
    pub(crate) edges: u64,
    pub(crate) ended: u64,
    pub(crate) resumed: u64,
    /// Encoded upper-bound bytes admitted for decoding, including failed loads.
    pub(crate) decoded: u64,
    pub(crate) preparation: u64,
    pub(crate) authorization_reads: u64,
    pub(crate) captured: u64,
    pub(crate) kept_bytes: u64,
}

impl PageWork {
    pub(super) fn tip(&self) {
        self.tips.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn resumed(&self) {
        self.resumed.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn ended(&self) {
        self.ended.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self, kept_bytes: usize) -> PageWorkSnapshot {
        PageWorkSnapshot {
            visits: self.visits.load(Ordering::Relaxed),
            actors: self.actors.load(Ordering::Relaxed),
            tips: self.tips.load(Ordering::Relaxed),
            edges: self.edges.load(Ordering::Relaxed),
            ended: self.ended.load(Ordering::Relaxed),
            resumed: self.resumed.load(Ordering::Relaxed),
            decoded: self.decoded.load(Ordering::Relaxed),
            preparation: self.preparation.load(Ordering::Relaxed),
            authorization_reads: self.authorization_reads.load(Ordering::Relaxed),
            captured: self.captured.load(Ordering::Relaxed),
            kept_bytes: kept_bytes as u64,
        }
    }
}

pub(super) struct Slice {
    work: std::sync::Arc<PageWork>,
    limit: usize,
    visits: usize,
    scanned: usize,
    edges: usize,
    workspace: usize,
    decoded: usize,
    decode_limit: usize,
    preparation: usize,
    input_units: usize,
    preparation_bytes: usize,
    authorization_reads: usize,
    captured: usize,
}

impl Slice {
    pub(super) fn new(
        work: std::sync::Arc<PageWork>,
        limit: usize,
        workspace: usize,
    ) -> Result<Self> {
        let mut slice = Self {
            work,
            limit,
            visits: 0,
            scanned: 0,
            edges: 0,
            workspace: 0,
            decoded: 0,
            decode_limit: MAX_PAGE_BYTES,
            preparation: 0,
            input_units: 0,
            preparation_bytes: 0,
            authorization_reads: 0,
            captured: 0,
        };
        slice.reserve(workspace)?;
        Ok(slice)
    }

    pub(super) fn read(&mut self) -> bool {
        if self.visits + self.scanned >= self.limit {
            return false;
        }
        self.visits += 1;
        self.work.visits.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub(super) fn actor(&mut self) -> bool {
        if self.visits + self.scanned >= self.limit {
            return false;
        }
        self.scanned += 1;
        self.work.actors.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub(super) fn edge(&mut self) -> bool {
        if self.edges >= self.limit {
            return false;
        }
        self.edges += 1;
        self.work.edges.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub(super) fn exhausted(&self) -> bool {
        self.visits + self.scanned >= self.limit || self.edges >= self.limit
    }

    pub(super) fn reserve(&mut self, bytes: usize) -> Result<()> {
        let required = self.workspace.saturating_add(bytes);
        if required > MAX_WORKSPACE_BYTES {
            return Err(Error::SyncCapacity(format!(
                "planner workspace needs {required} bytes; reduce the request's wants or actor window"
            )));
        }
        self.workspace = required;
        Ok(())
    }

    pub(super) fn prepare(&mut self, units: usize, bytes: usize) -> Result<()> {
        let units = self.preparation.saturating_add(units);
        let bytes = self.preparation_bytes.saturating_add(bytes);
        if units > 16 * MAX_PAGE_VISITS || bytes > 128 * 1024 * 1024 {
            return Err(Error::SyncCapacity(
                "request preparation exceeds its entry or memory envelope; reduce the actor window or wants".into(),
            ));
        }
        self.work
            .preparation
            .fetch_add((units - self.preparation) as u64, Ordering::Relaxed);
        self.preparation = units;
        self.preparation_bytes = bytes;
        Ok(())
    }

    pub(super) fn prepare_input(&mut self, units: usize, bytes: usize, limit: usize) -> Result<()> {
        let units = self.input_units.saturating_add(units);
        let bytes = self.preparation_bytes.saturating_add(bytes);
        if units > limit || bytes > 128 * 1024 * 1024 {
            return Err(Error::SyncCapacity(
                "request input exceeds its work or memory limit; reduce wants, hints or filter bytes".into(),
            ));
        }
        self.work
            .preparation
            .fetch_add((units - self.input_units) as u64, Ordering::Relaxed);
        self.input_units = units;
        self.preparation_bytes = bytes;
        Ok(())
    }

    pub(super) fn authorization_read(&mut self) -> Result<()> {
        if self.authorization_reads >= 16 {
            return Err(Error::SyncCapacity(
                "request authorization exceeds its metadata envelope".into(),
            ));
        }
        self.authorization_reads += 1;
        self.work
            .authorization_reads
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn capture(&mut self, entries: usize, raw: usize, bytes: usize) -> Result<()> {
        if raw > MAX_PAGE_BYTES.saturating_sub(self.captured) {
            return Err(Error::SyncCapacity(
                "request snapshot exceeds its encoded metadata envelope".into(),
            ));
        }
        self.prepare(entries, bytes)?;
        self.captured += raw;
        self.work.captured.fetch_add(raw as u64, Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn decode(&mut self, bytes: usize) -> Result<bool> {
        if bytes > self.decode_limit {
            return Err(Error::SyncCapacity(format!(
                "operation decoding needs {bytes} bytes, slice capacity {}",
                self.decode_limit,
            )));
        }
        if bytes > self.decode_limit - self.decoded {
            return Ok(false);
        }
        self.decoded += bytes;
        self.work.decoded.fetch_add(bytes as u64, Ordering::Relaxed);
        Ok(true)
    }

    #[cfg(test)]
    pub(super) fn with_decode_limit(mut self, limit: usize) -> Self {
        self.decode_limit = limit;
        self
    }
}
