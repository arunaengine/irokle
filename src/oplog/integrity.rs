// SPDX-License-Identifier: MIT OR Apache-2.0
//! Holes of a topic: ids its stored records reference but cannot resolve, read
//! in bounded steps that resume where the last one stopped.

use std::collections::BTreeMap;

use crate::storage::{DependencyCursor, SnapshotRead};
use crate::{OpId, Result, TopicId};

/// Listed ids and dependency edges one scan step reads, like a page slice.
pub(crate) const STEP_READS: usize = 65_536;

/// Unresolved ids, each with its generation when its position is stored.
pub(crate) type Holes = BTreeMap<OpId, Option<u64>>;

/// Where a scan resumes: after the last id it finished, and inside the
/// dependencies of the next one when a step ended there.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Cursor {
    after: Option<OpId>,
    open: Option<(OpId, DependencyCursor)>,
}

/// What one step read: where the scan stands, its new holes, and whether it ended.
pub(crate) struct Step {
    pub(crate) cursor: Cursor,
    pub(crate) holes: Holes,
    pub(crate) done: bool,
}

/// Reads the scan of `topic_id` on from `cursor`: each listed id's position and
/// record presence, then each dependency edge, at most `reads` ids and edges. No
/// payload is decoded; a wide operation's dependencies may span several steps.
pub(crate) fn scan_step(
    read: &dyn SnapshotRead,
    topic_id: &TopicId,
    cursor: Cursor,
    reads: usize,
) -> Result<Step> {
    let mut step = Step {
        cursor,
        holes: Holes::new(),
        done: false,
    };
    let mut left = reads.max(1);
    if let Some((id, from)) = step.cursor.open.take()
        && !read_edges(read, id, from, &mut left, &mut step)?
    {
        return Ok(step);
    }
    if left == 0 {
        return Ok(step);
    }
    let asked = left;
    let ids = read.topic_ids_after(topic_id, step.cursor.after.as_ref(), asked)?;
    let listed = ids.len();
    for (index, id) in ids.into_iter().enumerate() {
        left -= 1;
        match read.get_header(&id)? {
            None => {
                step.holes.insert(id, None);
                step.cursor.after = Some(id);
            }
            Some(header) => {
                // The position is stored, so an unresolvable id lacks its record.
                if !read.dep_resolvable(&id)? {
                    step.holes.insert(id, Some(header.generation));
                }
                if !read_edges(read, id, DependencyCursor::default(), &mut left, &mut step)? {
                    return Ok(step);
                }
            }
        }
        if left == 0 && index + 1 < listed {
            return Ok(step);
        }
    }
    // A shorter listing than asked reached the end of the topic.
    step.done = listed < asked;
    Ok(step)
}

/// Checks the dependencies of `id` from `from` on, spending `left`. Returns whether
/// all were read; otherwise the cursor keeps where they resume.
fn read_edges(
    read: &dyn SnapshotRead,
    id: OpId,
    mut from: DependencyCursor,
    left: &mut usize,
    step: &mut Step,
) -> Result<bool> {
    loop {
        if *left == 0 {
            step.cursor.open = Some((id, from));
            return Ok(false);
        }
        let asked = *left;
        let Some(deps) = read.dependency_ids(&id, from, asked)? else {
            // Only a reset removes a position, and it starts a new scan; this is damage.
            step.holes.entry(id).or_insert(None);
            step.cursor.after = Some(id);
            return Ok(true);
        };
        *left -= deps.len();
        for dep in &deps {
            if !read.dep_resolvable(dep)? {
                step.holes.entry(*dep).or_insert(None);
            }
        }
        if deps.len() < asked {
            step.cursor.after = Some(id);
            return Ok(true);
        }
        from = DependencyCursor {
            offset: from.offset + deps.len(),
            after: deps.last().copied(),
        };
    }
}

#[cfg(test)]
#[path = "integrity_tests.rs"]
mod tests;
