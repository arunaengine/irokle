// SPDX-License-Identifier: MIT OR Apache-2.0
//! Task registration and lifecycle of a net: the tracker shutdown seals and
//! drains, the runtime configuration, and the latch that lets a loop restart.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crate::Storage;

use super::IrohNet;

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_SYNC_IO_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_RESYNC_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_RESYNC_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const DEFAULT_RESYNC_MAX_BACKOFF: Duration = Duration::from_secs(10 * 60);
const DEFAULT_FULL_SWEEP_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_FULL_SWEEP_TIME_OF_DAY: Duration = Duration::from_secs(3 * 60 * 60);

/// Result of [`IrohNet::shutdown_with_timeout`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownOutcome {
    /// Every task the net spawned has ended.
    Complete,
    /// Tasks were still running at the timeout; the net keeps owning them.
    Incomplete { running: usize },
}

/// Owns the tasks a net runs. Root work registers and shutdown seals under one
/// lock, so once shutdown has closed registration and seen zero tasks, no work
/// a caller starts later can run.
#[derive(Default)]
pub(super) struct TaskTracker {
    state: Mutex<TrackerState>,
    idle: tokio::sync::Notify,
}

#[derive(Default)]
struct TrackerState {
    running: usize,
    closed: bool,
}

impl TaskTracker {
    fn state(&self) -> std::sync::MutexGuard<'_, TrackerState> {
        // The state is two plain fields updated atomically, so a poisoned lock is still consistent.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Registers work started by a caller outside the net. Refused once
    /// shutdown has begun.
    pub(super) fn enter(self: &Arc<Self>) -> io::Result<TaskGuard> {
        let mut state = self.state();
        if state.closed {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "irokle net is shut down",
            ));
        }
        state.running += 1;
        Ok(TaskGuard(Arc::clone(self)))
    }

    /// Registers work an already registered task starts, which may still run
    /// while shutdown drains it.
    pub(super) fn track(self: &Arc<Self>) -> TaskGuard {
        self.state().running += 1;
        TaskGuard(Arc::clone(self))
    }

    pub(super) fn close(&self) {
        self.state().closed = true;
    }

    pub(super) fn is_closed(&self) -> bool {
        self.state().closed
    }

    pub(super) fn running(&self) -> usize {
        self.state().running
    }

    pub(super) async fn wait_idle(&self) {
        loop {
            let idle = self.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if self.running() == 0 {
                return;
            }
            idle.await;
        }
    }
}

/// Owned by a spawned task future, so the count drops when the task really ends.
pub(super) struct TaskGuard(Arc<TaskTracker>);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        let mut state = self.0.state();
        state.running -= 1;
        if state.running == 0 {
            drop(state);
            self.0.idle.notify_waiters();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IrohRuntimeConfig {
    pub connect_timeout: Duration,
    pub sync_io_timeout: Duration,
    pub resync_interval: Duration,
    pub resync_initial_backoff: Duration,
    pub resync_max_backoff: Duration,
    pub full_sweep_interval: Duration,
    pub full_sweep_time_of_day: Duration,
}

impl Default for IrohRuntimeConfig {
    fn default() -> Self {
        Self {
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            sync_io_timeout: DEFAULT_SYNC_IO_TIMEOUT,
            resync_interval: DEFAULT_RESYNC_INTERVAL,
            resync_initial_backoff: DEFAULT_RESYNC_INITIAL_BACKOFF,
            resync_max_backoff: DEFAULT_RESYNC_MAX_BACKOFF,
            full_sweep_interval: DEFAULT_FULL_SWEEP_INTERVAL,
            full_sweep_time_of_day: DEFAULT_FULL_SWEEP_TIME_OF_DAY,
        }
    }
}

/// Clears a loop's start latch when the loop task actually ends, including on
/// abort, so a replacement loop can be started.
pub(super) struct LoopGuard<S: Storage> {
    pub(super) net: Weak<IrohNet<S>>,
    pub(super) latch: fn(&IrohNet<S>) -> &AtomicBool,
}

impl<S: Storage> Drop for LoopGuard<S> {
    fn drop(&mut self) {
        if let Some(current) = self.net.upgrade() {
            (self.latch)(&current).store(false, Ordering::SeqCst);
        }
    }
}
