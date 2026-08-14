// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::storage::Storage;
use crate::{Error, Op, Result, TopicId};

pub fn topological<S: Storage>(storage: &S, topic_id: &TopicId) -> Result<Vec<Op>> {
    let ids = storage.list_op_ids(topic_id)?;
    topological_subset(storage, &ids)
}

/// Order the ops named by `ids` oldest-first.
///
/// An id whose records are absent, or that depends on an op with no metadata,
/// is treated as a hole still awaiting repair: it and everything in `ids`
/// reachable from it are left out instead of failing the whole traversal, so a
/// sync exchange still makes progress for the ops it can resolve. Nothing is
/// discarded - a deferred op stays admitted and reappears here once its
/// dependency is refetched. Only a cycle among fully present ops is an error.
pub fn topological_subset<S: Storage>(storage: &S, ids: &BTreeSet<crate::OpId>) -> Result<Vec<Op>> {
    let mut present = BTreeMap::new();
    let mut blocked = BTreeSet::new();
    for id in ids {
        let (Some(meta), Some(op)) = (storage.get_meta(id)?, storage.get_op(id)?) else {
            blocked.insert(*id);
            continue;
        };
        let mut deps_in_set = 0_usize;
        let mut dangling = false;
        for dep in &meta.deps {
            if ids.contains(dep) {
                deps_in_set += 1;
            } else if !storage.dep_resolvable(dep)? {
                dangling = true;
            }
        }
        if dangling {
            blocked.insert(*id);
        } else {
            present.insert(*id, (op, deps_in_set));
        }
    }

    let mut frontier = blocked.iter().copied().collect::<Vec<_>>();
    while let Some(id) = frontier.pop() {
        for child in storage.children(&id)? {
            if ids.contains(&child) && blocked.insert(child) {
                present.remove(&child);
                frontier.push(child);
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
    let mut out = Vec::with_capacity(present.len());
    while let Some(id) = ready.pop_front() {
        let Some((op, _)) = present.get(&id) else {
            continue;
        };
        out.push(op.clone());
        for child in storage.children(&id)? {
            if let Some((_, count)) = present.get_mut(&child) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    ready.push_back(child);
                }
            }
        }
    }

    if out.len() != present.len() {
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
    let mut ready = indeg
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(*id))
        .collect::<VecDeque<_>>();
    let mut out = Vec::with_capacity(by_id.len());
    while let Some(id) = ready.pop_front() {
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
                    ready.push_back(*child);
                }
            }
        }
    }
    if out.len() != by_id.len() {
        return Err(Error::Storage("cycle in input op batch".into()));
    }
    Ok(out)
}
