// SPDX-License-Identifier: MIT OR Apache-2.0
//! Explicit repair roots ordered by lightweight headers, with retained edge cursors.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

use crate::storage::{SnapshotRead, Storage};
use crate::{ActorClock, ActorId, Op, OpId, Result, TopicId};

use super::plan::Slice;
use super::request::need;
use super::SyncEngine;
use super::records::{LoadError, Records};
use super::space::tree_bytes;
use super::{ActorScope, MAX_PAGE_MISSING, PageBudget, SyncRequest};

#[derive(Clone, Copy, Default)]
struct Scan {
    after: Option<OpId>,
    waiting: Option<(ActorId, u64, u64)>,
}

pub(super) struct Repair {
    expected: BTreeSet<OpId>,
    remaining: BTreeSet<OpId>,
    emitted: BTreeSet<OpId>,
    missing: BTreeSet<OpId>,
    missing_after: Option<OpId>,
    ordered: BTreeSet<(u64, OpId)>,
    scans: BTreeMap<OpId, Scan>,
    prepared: bool,
    preparing: Option<OpId>,
    prepared_after: Option<OpId>,
    after: Option<(u64, OpId)>,
    selected: Option<(u64, OpId)>,
}

pub(super) struct RepairView<'a> {
    pub(super) read: &'a dyn SnapshotRead,
    pub(super) topic_id: &'a TopicId,
    pub(super) peer: &'a ActorClock,
    pub(super) scope: &'a ActorScope<'a>,
}

#[derive(Default)]
pub(super) struct RepairPage {
    pub(super) unsent: BTreeSet<OpId>,
    pub(super) ops: Vec<Op>,
    pub(super) missing: BTreeSet<OpId>,
    pub(super) too_large: Option<OpId>,
    pub(super) positions: BTreeMap<ActorId, u64>,
    pub(super) continued: bool,
}

impl Repair {
    pub(super) fn new(wants: &BTreeSet<OpId>, slice: &mut Slice) -> Result<Self> {
        // Input copying is admission work bounded by MAX_REQUEST_ITEMS, independent of reads.
        slice.reserve(
            2 * tree_bytes::<OpId, ()>(wants.len())
                + 2 * tree_bytes::<OpId, ()>(0)
                + tree_bytes::<(u64, OpId), ()>(0)
                + tree_bytes::<OpId, Scan>(0),
        )?;
        Ok(Self {
            expected: wants.clone(),
            remaining: wants.clone(),
            emitted: BTreeSet::new(),
            missing: BTreeSet::new(),
            missing_after: None,
            ordered: BTreeSet::new(),
            scans: BTreeMap::new(),
            prepared: false,
            preparing: None,
            prepared_after: None,
            after: None,
            selected: None,
        })
    }

    pub(super) fn step(
        &mut self,
        view: RepairView<'_>,
        budget: PageBudget,
        position_limit: usize,
        slice: &mut Slice,
        records: &mut Records,
    ) -> Result<RepairPage> {
        let mut page = RepairPage::default();
        if budget.ops == 0 || budget.bytes == 0 {
            return Ok(page);
        }
        if !self.prepare(&view, slice)? {
            page.continued = true;
            return Ok(page);
        }
        let mut bytes = 0;
        while page.ops.len() < budget.ops && bytes < budget.bytes {
            let (generation, id) = match self.selected {
                Some(root) => root,
                None => {
                    let start = self.after.map_or(Unbounded, Excluded);
                    let Some(&root) = self.ordered.range((start, Unbounded)).next() else {
                        self.after = None;
                        break;
                    };
                    if !slice.actor() {
                        page.continued = true;
                        break;
                    }
                    self.selected = Some(root);
                    root
                }
            };
            if !self.remaining.contains(&id) {
                self.advance(generation, id);
                continue;
            }
            let mut scan = self.scans.get(&id).copied().unwrap_or_default();
            if let Some((actor, seq, required)) = scan.waiting {
                if !slice.actor() {
                    page.continued = true;
                    break;
                }
                if holds(&view, &actor, seq) {
                    scan.waiting = None;
                    self.save_scan(id, scan, slice)?;
                } else {
                    if view.scope.unknown(&actor)
                        && !position(&mut page, actor, required, position_limit, slice)?
                    {
                        break;
                    }
                    self.advance(generation, id);
                    continue;
                }
            }
            if !records.contains(&id) && !slice.read() {
                page.continued = true;
                break;
            }
            let record = match records.take_slice(view.read, &id, slice) {
                Ok(Some(record)) => record,
                Ok(None) => {
                    self.mark_missing(id, slice)?;
                    self.advance(generation, id);
                    continue;
                }
                Err(LoadError::Yield) => {
                    page.continued = true;
                    break;
                }
                Err(LoadError::Failed(error)) => return Err(error),
            };
            let mut ready = true;
            let start = scan.after.map_or(Unbounded, Excluded);
            for dep in record.op.signed.body.deps.range((start, Unbounded)) {
                if !slice.edge() {
                    page.continued = true;
                    ready = false;
                    break;
                }
                if self.contains(dep) {
                    ready = false;
                    self.advance(generation, id);
                    break;
                }
                if self.emitted.contains(dep) {
                    scan.after = Some(*dep);
                    continue;
                }
                if !slice.read() {
                    page.continued = true;
                    ready = false;
                    break;
                }
                let Some(meta) = view.read.get_header(dep)? else {
                    self.mark_missing(*dep, slice)?;
                    ready = false;
                    self.advance(generation, id);
                    break;
                };
                if !holds(&view, &meta.actor_id, meta.actor_seq) {
                    if view.scope.unknown(&meta.actor_id)
                        && !position(
                            &mut page,
                            meta.actor_id,
                            meta.generation,
                            position_limit,
                            slice,
                        )?
                    {
                        ready = false;
                        break;
                    }
                    scan.waiting = Some((meta.actor_id, meta.actor_seq, meta.generation));
                    scan.after = Some(*dep);
                    self.advance(generation, id);
                    ready = false;
                    break;
                }
                scan.after = Some(*dep);
            }
            if !ready {
                self.save_scan(id, scan, slice)?;
                records.keep(record);
                if page.continued || self.after != Some((generation, id)) {
                    break;
                }
                continue;
            }
            let size = postcard::experimental::serialized_size(&record.op)?;
            if size > budget.bytes - bytes {
                if page.ops.is_empty() {
                    page.too_large = Some(id);
                }
                self.save_scan(id, scan, slice)?;
                records.keep(record);
                break;
            }
            reserve_set(&self.emitted, &id, slice)?;
            self.emitted.insert(id);
            self.remaining.remove(&id);
            self.scans.remove(&id);
            self.ordered.remove(&(generation, id));
            self.advance(generation, id);
            bytes += size;
            // The destination slot and output credit exist before the cache lease ends.
            page.ops.reserve(1);
            page.ops.push(record.into_op());
        }
        if !self.missing.is_empty() {
            slice.reserve(tree_bytes::<OpId, ()>(
                self.missing.len().min(MAX_PAGE_MISSING),
            ))?;
            let start = self.missing_after.map_or(Unbounded, Excluded);
            page.missing.extend(
                self.missing
                    .range((start, Unbounded))
                    .take(MAX_PAGE_MISSING)
                    .copied(),
            );
            self.missing_after = page.missing.last().copied();
            if self.missing_after.is_some_and(|last| {
                self.missing
                    .range((Excluded(last), Unbounded))
                    .next()
                    .is_some()
            }) {
                page.continued = true;
            } else {
                self.missing_after = None;
            }
        }
        Ok(page)
    }

    fn advance(&mut self, generation: u64, id: OpId) {
        self.after = Some((generation, id));
        self.selected = None;
    }

    fn prepare(&mut self, view: &RepairView<'_>, slice: &mut Slice) -> Result<bool> {
        while !self.prepared {
            let id = match self.preparing {
                Some(id) => id,
                None => {
                    let start = self.prepared_after.map_or(Unbounded, Excluded);
                    let Some(&id) = self.expected.range((start, Unbounded)).next() else {
                        self.prepared = true;
                        break;
                    };
                    if !slice.actor() {
                        return Ok(false);
                    }
                    self.preparing = Some(id);
                    id
                }
            };
            if !slice.read() {
                return Ok(false);
            }
            match view.read.get_header(&id)? {
                Some(header) if header.topic_id == *view.topic_id => {
                    let root = (header.generation, id);
                    reserve_set(&self.ordered, &root, slice)?;
                    self.ordered.insert(root);
                }
                _ => self.mark_missing(id, slice)?,
            }
            self.prepared_after = Some(id);
            self.preparing = None;
        }
        Ok(true)
    }

    fn save_scan(&mut self, id: OpId, scan: Scan, slice: &mut Slice) -> Result<()> {
        if !self.scans.contains_key(&id) {
            let count = self.scans.len();
            slice.reserve(tree_bytes::<OpId, Scan>(count + 1) - tree_bytes::<OpId, Scan>(count))?;
        }
        self.scans.insert(id, scan);
        Ok(())
    }

    fn mark_missing(&mut self, id: OpId, slice: &mut Slice) -> Result<()> {
        reserve_set(&self.missing, &id, slice)?;
        self.missing.insert(id);
        self.remaining.remove(&id);
        Ok(())
    }

    pub(super) fn confirms(&mut self, request: &SyncRequest) -> bool {
        if !request.wants.is_subset(&self.expected)
            || !self
                .expected
                .difference(&request.wants)
                .all(|id| self.emitted.contains(id))
        {
            return false;
        }
        self.expected.retain(|id| request.wants.contains(id));
        self.emitted.retain(|id| request.wants.contains(id));
        true
    }

    pub(super) fn offered(&self) -> bool {
        !self.emitted.is_empty()
    }

    pub(super) fn admit(&mut self, ops: &[Op], slice: &mut Slice) -> Result<()> {
        for op in ops {
            if self.remaining.contains(&op.id) {
                reserve_set(&self.emitted, &op.id, slice)?;
                self.emitted.insert(op.id);
                self.remaining.remove(&op.id);
                self.ordered.remove(&(op.signed.body.generation, op.id));
                self.scans.remove(&op.id);
            }
        }
        Ok(())
    }

    pub(super) fn contains(&self, id: &OpId) -> bool {
        self.remaining.contains(id) || (self.expected.contains(id) && self.missing.contains(id))
    }

    pub(super) fn pending(&self) -> bool {
        !self.remaining.is_empty() || self.missing_after.is_some()
    }

    pub(super) fn bytes(&self) -> usize {
        tree_bytes::<OpId, ()>(self.expected.len())
            + tree_bytes::<OpId, ()>(self.remaining.len())
            + tree_bytes::<OpId, ()>(self.emitted.len())
            + tree_bytes::<OpId, ()>(self.missing.len())
            + tree_bytes::<(u64, OpId), ()>(self.ordered.len())
            + tree_bytes::<OpId, Scan>(self.scans.len())
    }
}

fn holds(view: &RepairView<'_>, actor: &ActorId, seq: u64) -> bool {
    view.scope.holds_prefix(actor, seq)
        || (!view.scope.unknown(actor) && view.peer.get(actor) >= seq)
}

fn position(
    page: &mut RepairPage,
    actor: ActorId,
    generation: u64,
    limit: usize,
    slice: &mut Slice,
) -> Result<bool> {
    if !page.positions.contains_key(&actor) {
        let count = page.positions.len();
        if count >= limit {
            return Ok(false);
        }
        slice.reserve(tree_bytes::<ActorId, u64>(count + 1) - tree_bytes::<ActorId, u64>(count))?;
    }
    super::request::need(&mut page.positions, actor, generation);
    Ok(true)
}

fn reserve_set<T: Ord>(set: &BTreeSet<T>, value: &T, slice: &mut Slice) -> Result<()> {
    if !set.contains(value) {
        let count = set.len();
        slice.reserve(tree_bytes::<T, ()>(count + 1) - tree_bytes::<T, ()>(count))?;
    }
    Ok(())
}

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
