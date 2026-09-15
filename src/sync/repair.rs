// SPDX-License-Identifier: MIT OR Apache-2.0
//! Retained explicit-hole repair: requested ids a store holds, served oldest
//! generation first once their dependencies are held or sent.

use std::collections::{BTreeMap, BTreeSet};

use crate::storage::{SnapshotRead, Storage};
use crate::{ActorClock, ActorId, Op, OpId, Result, TopicId};

use super::request::need;
use super::{ActorScope, PageBudget, SyncEngine};

impl<S: Storage> SyncEngine<S> {
    /// Requested repair ids this store holds, oldest generation first. An id
    /// whose dependency is neither held by the peer nor sent before it waits,
    /// and so do its dependents: its ancestors come from the forward ranges. A
    /// dependency on an actor `scope` leaves unknown names that actor instead.
    pub(super) fn plan_repair(
        read: &dyn SnapshotRead,
        topic_id: &TopicId,
        wants: &BTreeSet<OpId>,
        (peer, scope): (&ActorClock, &ActorScope<'_>),
        budget: PageBudget,
    ) -> Result<RepairPage> {
        let mut page = RepairPage::default();
        let mut ordered = Vec::with_capacity(wants.len());
        for id in wants {
            match read.get_position(id)? {
                Some(meta) if meta.topic_id == *topic_id => {
                    ordered.push((meta.generation, *id, meta.deps))
                }
                Some(_) => {}
                None => {
                    page.missing.insert(*id);
                }
            }
        }
        // Generations order ancestors first, whatever order the ids sort in.
        ordered.sort_unstable_by_key(|(generation, id, _)| (*generation, *id));
        let mut sent = BTreeSet::new();
        let mut bytes = 0;
        for (index, (_, id, deps)) in ordered.iter().enumerate() {
            let mut ready = true;
            for dep in deps {
                if sent.contains(dep) {
                    continue;
                }
                if wants.contains(dep) {
                    ready = false;
                    break;
                }
                match read.get_position(dep)? {
                    Some(meta) if scope.holds_prefix(&meta.actor_id, meta.actor_seq) => {}
                    // Every unknown actor of the want is named at once.
                    Some(meta) if scope.unknown(&meta.actor_id) => {
                        need(&mut page.positions, meta.actor_id, meta.generation);
                        ready = false;
                    }
                    Some(meta) if peer.get(&meta.actor_id) >= meta.actor_seq => {}
                    _ => {
                        ready = false;
                        break;
                    }
                }
            }
            if !ready {
                page.unsent.insert(*id);
                continue;
            }
            let Some(op) = read.get_op(id)? else {
                page.missing.insert(*id);
                continue;
            };
            let size = postcard::experimental::serialized_size(&op)?;
            if page.ops.len() >= budget.ops || bytes + size > budget.bytes {
                if page.ops.is_empty() && size > budget.bytes {
                    page.too_large = Some(*id);
                }
                page.unsent
                    .extend(ordered[index..].iter().map(|(_, id, _)| *id));
                break;
            }
            bytes += size;
            sent.insert(*id);
            page.ops.push(op);
        }
        Ok(page)
    }
}

/// What the repair part of a page carried and left.
#[derive(Default)]
pub(super) struct RepairPage {
    pub(super) ops: Vec<Op>,
    /// Held wants not carried, whose dependents must wait.
    pub(super) unsent: BTreeSet<OpId>,
    pub(super) missing: BTreeSet<OpId>,
    pub(super) too_large: Option<OpId>,
    /// Actors the request did not describe whose positions a want needed,
    /// with the lowest generation needing each.
    pub(super) positions: BTreeMap<ActorId, u64>,
}
