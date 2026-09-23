// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::storage::{OpMeta, SnapshotRead, Storage};
use crate::{Error, Op, Result, TopicId};

pub fn topological<S: Storage>(storage: &S, topic_id: &TopicId) -> Result<Vec<Op>> {
    Ok(topological_entries(storage, topic_id)?
        .into_iter()
        .map(|(op, _)| op)
        .collect())
}

pub(crate) fn topological_entries<S: Storage>(
    storage: &S,
    topic_id: &TopicId,
) -> Result<Vec<(Op, OpMeta)>> {
    storage.read_snapshot(|read| {
        let ids = read.list_op_ids(topic_id)?;
        subset_entries_in(read, &ids)
    })
}

/// Order the ops named by `ids` oldest-first. An op with missing records or a dependency
/// without metadata is a hole: it and what depends on it are left out, not discarded, and only
/// a cycle among fully present ops is an error.
pub fn topological_subset<S: Storage>(storage: &S, ids: &BTreeSet<crate::OpId>) -> Result<Vec<Op>> {
    Ok(topological_subset_entries(storage, ids)?
        .into_iter()
        .map(|(op, _)| op)
        .collect())
}

pub(crate) fn topological_subset_entries<S: Storage>(
    storage: &S,
    ids: &BTreeSet<crate::OpId>,
) -> Result<Vec<(Op, OpMeta)>> {
    storage.read_snapshot(|read| subset_entries_in(read, ids))
}

/// [`topological_subset`] over one snapshot a planner already holds.
pub(crate) fn subset_in(read: &dyn SnapshotRead, ids: &BTreeSet<crate::OpId>) -> Result<Vec<Op>> {
    Ok(subset_entries_in(read, ids)?
        .into_iter()
        .map(|(op, _)| op)
        .collect())
}

pub(crate) fn subset_entries_in(
    read: &dyn SnapshotRead,
    ids: &BTreeSet<crate::OpId>,
) -> Result<Vec<(Op, OpMeta)>> {
    topological_meta(read, ids)?
        .into_iter()
        .map(|meta| {
            let op = read
                .get_op(&meta.id)?
                .ok_or_else(|| Error::Storage(format!("missing op {}", meta.id)))?;
            Ok((op, meta))
        })
        .collect()
}

pub(crate) fn topological_ids<S: Storage>(
    storage: &S,
    ids: &BTreeSet<crate::OpId>,
) -> Result<Vec<crate::OpId>> {
    storage.read_snapshot(|read| {
        Ok(topological_meta(read, ids)?
            .into_iter()
            .map(|meta| meta.id)
            .collect())
    })
}

fn topological_meta(read: &dyn SnapshotRead, ids: &BTreeSet<crate::OpId>) -> Result<Vec<OpMeta>> {
    let mut present = BTreeMap::new();
    let mut children: BTreeMap<crate::OpId, BTreeSet<crate::OpId>> = BTreeMap::new();
    let mut blocked = BTreeSet::new();
    for id in ids {
        let Some(meta) = read.get_meta(id)? else {
            blocked.insert(*id);
            continue;
        };
        if !read.dep_resolvable(id)? {
            blocked.insert(*id);
            continue;
        }
        let mut deps_in_set = 0_usize;
        let mut dangling = false;
        for dep in &meta.deps {
            if ids.contains(dep) {
                deps_in_set += 1;
                children.entry(*dep).or_default().insert(*id);
            } else if !read.dep_resolvable(dep)? {
                dangling = true;
            }
        }
        if dangling {
            blocked.insert(*id);
        } else {
            present.insert(*id, (meta, deps_in_set));
        }
    }

    let mut frontier = blocked.iter().copied().collect::<Vec<_>>();
    while let Some(id) = frontier.pop() {
        for child in children.get(&id).into_iter().flatten() {
            if ids.contains(child) && blocked.insert(*child) {
                present.remove(child);
                frontier.push(*child);
            }
        }
    }
    if !blocked.is_empty() {
        tracing::debug!(
            deferred = blocked.len(),
            "deferred ops with unresolved dependencies"
        );
    }

    let mut ready = present
        .iter()
        .filter_map(|(id, (_, count))| (*count == 0).then_some(*id))
        .collect::<VecDeque<_>>();
    let expected = present.len();
    let mut out = Vec::with_capacity(expected);
    while let Some(id) = ready.pop_front() {
        let Some((meta, _)) = present.remove(&id) else {
            continue;
        };
        out.push(meta);
        for child in children.get(&id).into_iter().flatten() {
            if let Some((_, count)) = present.get_mut(child) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    ready.push_back(*child);
                }
            }
        }
    }

    if out.len() != expected {
        return Err(Error::Storage("cycle in op graph".into()));
    }
    Ok(out)
}

pub(crate) fn topological_ops(ops: Vec<Op>) -> Result<Vec<Op>> {
    let by_id = ops
        .into_iter()
        .map(|op| (op.id, op))
        .collect::<BTreeMap<_, _>>();
    let mut indeg = BTreeMap::new();
    let mut children: BTreeMap<crate::OpId, BTreeSet<crate::OpId>> = BTreeMap::new();
    for (id, op) in &by_id {
        let mut count = 0_usize;
        for dep in &op.signed.body.deps {
            if by_id.contains_key(dep) {
                count += 1;
                children.entry(*dep).or_default().insert(*id);
            }
        }
        indeg.insert(*id, count);
    }
    // Oldest generation first: ops unconnected inside the batch may still
    // depend on each other through stored ops, as repaired holes do.
    let generation = |id: &crate::OpId| by_id.get(id).map_or(0, |op| op.signed.body.generation);
    let mut ready = indeg
        .iter()
        .filter(|(_, count)| **count == 0)
        .map(|(id, _)| (generation(id), *id))
        .collect::<BTreeSet<_>>();
    let mut out = Vec::with_capacity(by_id.len());
    while let Some((_, id)) = ready.pop_first() {
        out.push(
            by_id
                .get(&id)
                .cloned()
                .ok_or_else(|| Error::Storage(format!("missing input op {id}")))?,
        );
        for child in children.get(&id).into_iter().flatten() {
            if let Some(count) = indeg.get_mut(child) {
                *count = (*count).saturating_sub(1);
                if *count == 0 {
                    ready.insert((generation(child), *child));
                }
            }
        }
    }
    if out.len() != by_id.len() {
        return Err(Error::Storage("cycle in input op batch".into()));
    }
    Ok(out)
}
