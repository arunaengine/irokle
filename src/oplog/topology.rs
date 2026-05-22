// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::storage::Storage;
use crate::{Error, Op, Result, TopicId};

pub fn topological<S: Storage>(storage: &S, topic_id: &TopicId) -> Result<Vec<Op>> {
    let ids = storage.list_op_ids(topic_id)?;
    topological_subset(storage, &ids)
}

pub fn topological_subset<S: Storage>(storage: &S, ids: &BTreeSet<crate::OpId>) -> Result<Vec<Op>> {
    let mut indeg = BTreeMap::new();
    for id in ids {
        let meta = storage
            .get_meta(id)?
            .ok_or_else(|| Error::Storage(format!("missing op meta for {id}")))?;
        indeg.insert(
            *id,
            meta.deps.iter().filter(|dep| ids.contains(dep)).count(),
        );
    }

    let mut ready = indeg
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(*id))
        .collect::<VecDeque<_>>();
    let mut out = Vec::with_capacity(ids.len());
    while let Some(id) = ready.pop_front() {
        let op = storage
            .get_op(&id)?
            .ok_or_else(|| Error::Storage(format!("missing op {id}")))?;
        out.push(op);
        for child in storage.children(&id)? {
            if let Some(count) = indeg.get_mut(&child) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    ready.push_back(child);
                }
            }
        }
    }

    if out.len() != ids.len() {
        return Err(Error::Storage(
            "cycle or missing dependency in op graph".into(),
        ));
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
