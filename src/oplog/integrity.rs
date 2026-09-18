// SPDX-License-Identifier: MIT OR Apache-2.0
//! Holes of a topic: ids its stored records reference but cannot resolve. A scan
//! reads the topic in bounded steps, each in its own snapshot, and resumes where it
//! stopped. Contract and freshness rules: `src/sync/limits.md`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::storage::{DependencyCursor, SnapshotRead, TopicView};
use crate::{Error, OpId, Result, TopicId};

/// Listed ids and dependency edges one scan step reads, like a page slice.
pub(crate) const STEP_READS: usize = 65_536;

/// Unresolved ids, each with its generation when its position is stored.
pub(crate) type Holes = BTreeMap<OpId, Option<u64>>;

/// What is known about the holes of a topic on its current branch and data epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Integrity {
    /// No step has read the topic, and none could read it now.
    Unknown,
    /// A scan is under way and found these holes so far; more may follow.
    Scanning(Holes),
    /// A complete scan found these holes; admission removes the ones it fills.
    Incomplete(Holes),
    /// A complete scan found no hole, and admission never creates one.
    Whole,
}

impl Integrity {
    pub(crate) fn is_whole(&self) -> bool {
        matches!(self, Self::Whole)
    }

    /// Whether a scan reached the end of the topic.
    pub(crate) fn is_complete(&self) -> bool {
        matches!(self, Self::Whole | Self::Incomplete(_))
    }

    /// The holes known now: all of them once complete, those found so far before.
    pub(crate) fn holes(&self) -> Option<&Holes> {
        match self {
            Self::Scanning(holes) | Self::Incomplete(holes) => Some(holes),
            Self::Unknown | Self::Whole => None,
        }
    }

    /// The known holes of `view`'s topic with the dependencies its buffered ops wait for.
    pub(crate) fn unresolved(&self, view: &TopicView) -> Holes {
        let mut unresolved = view
            .pending_missing
            .iter()
            .map(|id| (*id, None))
            .collect::<Holes>();
        unresolved.extend(
            self.holes()
                .into_iter()
                .flatten()
                .map(|(id, at)| (*id, *at)),
        );
        unresolved
    }

    /// Whether `view`'s topic may be certified: scanned whole, with no buffered op waiting.
    pub(crate) fn certifies(&self, view: &TopicView) -> bool {
        self.is_whole() && view.pending_missing.is_empty()
    }
}

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

enum State {
    Scanning {
        cursor: Cursor,
        holes: Holes,
        /// The claim of the step that reads the topic now, cleared when it ends.
        stepping: Option<u64>,
    },
    Incomplete(Holes),
    Whole,
}

/// A topic's scan or verdict under one branch and data epoch.
struct Entry {
    key: (OpId, u64),
    state: State,
}

/// Integrity scans and verdicts of an oplog's topics. At most one step reads
/// a topic at a time, and its claim ends with the step, however it ends.
pub(crate) struct Inspections {
    topics: Mutex<BTreeMap<TopicId, Entry>>,
    released: Condvar,
    reads: AtomicUsize,
    claims: AtomicU64,
}

impl Default for Inspections {
    fn default() -> Self {
        Self {
            topics: Mutex::default(),
            released: Condvar::new(),
            reads: AtomicUsize::new(STEP_READS),
            claims: AtomicU64::new(0),
        }
    }
}

impl Inspections {
    /// What is known of `view`'s topic after one step of its scan read from
    /// `read`, which `view` came from. A scan another step reads now is not read.
    pub(crate) fn step(&self, read: &dyn SnapshotRead, view: &TopicView) -> Result<Integrity> {
        let topic_id = view.state.topic_id;
        let key = (view.state.genesis, view.epoch);
        let claim = self.claims.fetch_add(1, Ordering::Relaxed);
        let start = {
            let mut topics = self.topics()?;
            let entry = topics.entry(topic_id).or_insert_with(|| Entry::start(key));
            if entry.key != key {
                *entry = Entry::start(key);
            }
            match &mut entry.state {
                State::Whole => return Ok(Integrity::Whole),
                State::Incomplete(holes) => return Ok(Integrity::Incomplete(holes.clone())),
                State::Scanning {
                    cursor,
                    holes,
                    stepping: Some(_),
                } => {
                    if *cursor == Cursor::default() && holes.is_empty() {
                        return Ok(Integrity::Unknown);
                    }
                    return Ok(Integrity::Scanning(holes.clone()));
                }
                State::Scanning {
                    cursor, stepping, ..
                } => {
                    *stepping = Some(claim);
                    *cursor
                }
            }
        };
        let claim = Claim {
            inspections: self,
            topic_id,
            key,
            claim,
        };
        let step = scan_step(read, &topic_id, start, self.reads.load(Ordering::Relaxed))?;
        Ok(claim.finish(start, step))
    }

    /// Waits until no step reads `topic_id`. Steps are bounded, so the cap only
    /// turns a lost wakeup into another look.
    pub(crate) fn wait_idle(&self, topic_id: &TopicId) -> Result<()> {
        let topics = self.topics()?;
        let _ = self
            .released
            .wait_timeout_while(topics, Duration::from_secs(60), |topics| {
                topics.get(topic_id).is_some_and(|entry| {
                    matches!(
                        entry.state,
                        State::Scanning {
                            stepping: Some(_),
                            ..
                        }
                    )
                })
            })
            .map_err(|_| poisoned())?;
        Ok(())
    }

    /// The ids of `ops` that are holes of their topics now.
    pub(crate) fn listed<'a>(
        &self,
        ops: impl IntoIterator<Item = &'a crate::Op>,
    ) -> Result<Vec<(TopicId, OpId)>> {
        let topics = self.topics()?;
        Ok(ops
            .into_iter()
            .map(|op| (op.signed.body.topic_id, op.id))
            .filter(|(topic_id, id)| {
                topics
                    .get(topic_id)
                    .and_then(Entry::holes)
                    .is_some_and(|holes| holes.contains_key(id))
            })
            .collect())
    }

    /// Removes `filled` holes, now stored. Admission never creates a hole, so
    /// a complete verdict left without one is whole.
    pub(crate) fn fill(&self, filled: &[(TopicId, OpId)]) -> Result<()> {
        let mut topics = self.topics()?;
        for (topic_id, id) in filled {
            if let Some(entry) = topics.get_mut(topic_id) {
                entry.remove(id);
            }
        }
        Ok(())
    }

    /// Forgets every topic.
    pub(crate) fn clear(&self) -> Result<()> {
        self.topics()?.clear();
        self.released.notify_all();
        Ok(())
    }

    fn topics(&self) -> Result<MutexGuard<'_, BTreeMap<TopicId, Entry>>> {
        self.topics.lock().map_err(|_| poisoned())
    }
}

impl Entry {
    fn start(key: (OpId, u64)) -> Self {
        Self {
            key,
            state: State::Scanning {
                cursor: Cursor::default(),
                holes: Holes::new(),
                stepping: None,
            },
        }
    }

    fn holes(&self) -> Option<&Holes> {
        match &self.state {
            State::Scanning { holes, .. } | State::Incomplete(holes) => Some(holes),
            State::Whole => None,
        }
    }

    fn remove(&mut self, id: &OpId) {
        match &mut self.state {
            State::Scanning { holes, .. } => {
                holes.remove(id);
            }
            State::Incomplete(holes) => {
                holes.remove(id);
                if holes.is_empty() {
                    self.state = State::Whole;
                }
            }
            State::Whole => {}
        }
    }
}

/// The right to step one topic's scan. Dropping it without finishing, on an
/// error or a panic, keeps the saved cursor so the next step reads again from there.
struct Claim<'a> {
    inspections: &'a Inspections,
    topic_id: TopicId,
    key: (OpId, u64),
    claim: u64,
}

impl Claim<'_> {
    /// Saves `step` if this claim still holds the scan, which stands at `start`.
    /// A scan replaced meanwhile keeps its own state; `step` then counts as partial.
    fn finish(self, start: Cursor, step: Step) -> Integrity {
        let mut topics = self
            .inspections
            .topics
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let Some(entry) = topics.get_mut(&self.topic_id) else {
            return Integrity::Scanning(step.holes);
        };
        let State::Scanning {
            cursor,
            holes,
            stepping,
        } = &mut entry.state
        else {
            return Integrity::Scanning(step.holes);
        };
        if entry.key != self.key || *stepping != Some(self.claim) || *cursor != start {
            return Integrity::Scanning(step.holes);
        }
        *stepping = None;
        *cursor = step.cursor;
        for (id, generation) in step.holes {
            let known = holes.entry(id).or_insert(generation);
            *known = known.or(generation);
        }
        if !step.done {
            return Integrity::Scanning(holes.clone());
        }
        let holes = std::mem::take(holes);
        if holes.is_empty() {
            entry.state = State::Whole;
            return Integrity::Whole;
        }
        entry.state = State::Incomplete(holes.clone());
        Integrity::Incomplete(holes)
    }
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        let mut topics = self
            .inspections
            .topics
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(Entry {
            key,
            state: State::Scanning { stepping, .. },
        }) = topics.get_mut(&self.topic_id)
            && *key == self.key
            && *stepping == Some(self.claim)
        {
            *stepping = None;
        }
        drop(topics);
        self.inspections.released.notify_all();
    }
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

fn poisoned() -> Error {
    Error::Storage("topic integrity cache lock poisoned".into())
}

#[cfg(test)]
#[path = "integrity_tests.rs"]
mod tests;
