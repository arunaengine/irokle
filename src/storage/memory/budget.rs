//! Shared reservations for Memory storage records, clock nodes and copy work.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::clock::ClockAllocation;
use crate::{ActorClock, Error, Result};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum MemoryDomain {
    Operations,
    Metadata,
    SharedNodes,
    Workspace,
    Activation,
    Recovery,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryLimits {
    pub retained_bytes: u64,
    pub workspace_bytes: u64,
    pub activation_bytes: u64,
    pub recovery_bytes: u64,
}

impl Default for MemoryLimits {
    fn default() -> Self {
        Self {
            retained_bytes: 2 * 1024 * 1024 * 1024,
            workspace_bytes: 256 * 1024 * 1024,
            activation_bytes: 2 * 1024 * 1024 * 1024,
            recovery_bytes: 2 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MemoryUsage {
    pub reserved: BTreeMap<MemoryDomain, u64>,
    pub peak_reserved: BTreeMap<MemoryDomain, u64>,
    pub clock_allocations: usize,
}

struct State {
    limits: MemoryLimits,
    used: [u64; 6],
    peak: [u64; 6],
    nodes: BTreeMap<usize, ClockAllocation>,
    cursor: Option<usize>,
}

pub(super) struct Budget {
    state: Mutex<State>,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                limits: Default::default(),
                used: [0; 6],
                peak: [0; 6],
                nodes: BTreeMap::new(),
                cursor: None,
            }),
        }
    }
}

impl Budget {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn configure(&self, limits: MemoryLimits) -> Result<()> {
        self.sweep(usize::MAX);
        let mut state = self.lock();
        if state.used[..3].iter().sum::<u64>() > limits.retained_bytes
            || state.used[3..].iter().any(|bytes| *bytes > 0)
        {
            return Err(Error::Storage(
                "memory limits cannot replace live reservations".into(),
            ));
        }
        state.limits = limits;
        Ok(())
    }

    pub(super) fn reserve(self: &Arc<Self>, domain: MemoryDomain, bytes: u64) -> Result<Charge> {
        let mut charge = Charge {
            owner: Arc::clone(self),
            domain,
            bytes: 0,
        };
        if let Err(error) = charge.grow(bytes) {
            if !matches!(error, Error::MemoryPressure { .. }) {
                return Err(error);
            }
            self.sweep(1024);
            charge.grow(bytes)?;
        }
        Ok(charge)
    }

    pub(super) fn usage(&self) -> MemoryUsage {
        self.sweep(usize::MAX);
        let state = self.lock();
        let domains = [
            MemoryDomain::Operations,
            MemoryDomain::Metadata,
            MemoryDomain::SharedNodes,
            MemoryDomain::Workspace,
            MemoryDomain::Activation,
            MemoryDomain::Recovery,
        ];
        MemoryUsage {
            reserved: domains.into_iter().zip(state.used).collect(),
            peak_reserved: domains.into_iter().zip(state.peak).collect(),
            clock_allocations: state.nodes.len(),
        }
    }

    fn sweep(&self, limit: usize) {
        let mut state = self.lock();
        for _ in 0..state.nodes.len().min(limit) {
            let start = state
                .cursor
                .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
            let key = state
                .nodes
                .range((start, std::ops::Bound::Unbounded))
                .next()
                .or_else(|| state.nodes.first_key_value())
                .map(|(key, _)| *key);
            let Some(key) = key else {
                break;
            };
            state.cursor = Some(key);
            if state.nodes.get(&key).is_some_and(|node| !node.alive())
                && let Some(node) = state.nodes.remove(&key)
            {
                state.used[MemoryDomain::SharedNodes as usize] -= node.bytes as u64 + 128;
            }
        }
    }

    pub(super) fn nodes(self: &Arc<Self>) -> Result<NodePlan> {
        self.sweep(8);
        Ok(NodePlan {
            nodes: BTreeMap::new(),
            retained: self.reserve(MemoryDomain::SharedNodes, 0)?,
            workspace: self.reserve(MemoryDomain::Workspace, 4096)?,
        })
    }
}

pub(super) struct Charge {
    owner: Arc<Budget>,
    domain: MemoryDomain,
    pub(super) bytes: u64,
}

impl Charge {
    pub(super) fn grow(&mut self, bytes: u64) -> Result<()> {
        let mut state = self.owner.lock();
        let index = self.domain as usize;
        let (held, limit) = match self.domain {
            MemoryDomain::Operations | MemoryDomain::Metadata | MemoryDomain::SharedNodes => (
                state.used[..3].iter().sum::<u64>(),
                state.limits.retained_bytes,
            ),
            MemoryDomain::Workspace => (state.used[index], state.limits.workspace_bytes),
            MemoryDomain::Activation => (state.used[index], state.limits.activation_bytes),
            MemoryDomain::Recovery => (state.used[index], state.limits.recovery_bytes),
        };
        let required = held.checked_add(bytes).ok_or(Error::MemoryPressure {
            domain: self.domain,
            required: u64::MAX,
            limit,
        })?;
        if required > limit {
            return Err(Error::MemoryPressure {
                domain: self.domain,
                required,
                limit,
            });
        }
        state.used[index] += bytes;
        state.peak[index] = state.peak[index].max(state.used[index]);
        self.bytes += bytes;
        Ok(())
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.owner.lock().used[self.domain as usize] -= self.bytes;
    }
}

pub(super) struct NodePlan {
    nodes: BTreeMap<usize, ClockAllocation>,
    retained: Charge,
    workspace: Charge,
}

impl NodePlan {
    pub(super) fn add(&mut self, clock: &ActorClock) -> Result<()> {
        clock.visit_allocations(|node| {
            if self.nodes.contains_key(&node.address)
                || self.retained.owner.lock().nodes.contains_key(&node.address)
            {
                return Ok(false);
            }
            self.retained.grow(node.bytes as u64 + 128)?;
            self.workspace.grow(128)?;
            self.nodes.insert(node.address, node);
            Ok(true)
        })
    }

    pub(super) fn commit(mut self) {
        let mut state = self.retained.owner.lock();
        for (address, node) in std::mem::take(&mut self.nodes) {
            if let std::collections::btree_map::Entry::Vacant(entry) = state.nodes.entry(address) {
                self.retained.bytes -= node.bytes as u64 + 128;
                entry.insert(node);
            }
        }
    }
}
