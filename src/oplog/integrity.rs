// SPDX-License-Identifier: MIT OR Apache-2.0
//! Holes of a topic: ids its stored records reference but cannot resolve. A scan
//! reads the topic in bounded steps, each in its own snapshot, and resumes where it
//! stopped; cached holes are checked again on use. Rules: `src/sync/limits.md`.

use std::collections::BTreeMap;
use std::ops::Bound;
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
    /// A complete scan found these holes, less those a question has since found stored.
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

/// Holes found so far, and the last one a question checked again for presence.
#[derive(Default)]
struct Found {
    holes: Holes,
    checked: Option<OpId>,
}

enum State {
    Scanning {
        cursor: Cursor,
        found: Found,
        /// The claim of the step that reads the topic now, cleared when it ends.
        stepping: Option<u64>,
    },
    Incomplete(Found),
    Whole,
}

/// A hole of a topic as its scan or verdict under one branch and data epoch knew it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Listed {
    topic_id: TopicId,
    key: (OpId, u64),
    pub(crate) id: OpId,
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
    /// `read`, which `view` came from, and one slice of its cached holes checked
    /// again there. A scan another step reads now is not read.
    pub(crate) fn step(&self, read: &dyn SnapshotRead, view: &TopicView) -> Result<Integrity> {
        let integrity = self.advance(read, view)?;
        if integrity.holes().is_some_and(|holes| !holes.is_empty()) {
            return self.recheck(read, view);
        }
        Ok(integrity)
    }

    /// One step of the scan, or the verdict when the scan is complete.
    fn advance(&self, read: &dyn SnapshotRead, view: &TopicView) -> Result<Integrity> {
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
                State::Scanning {
                    cursor,
                    stepping: stepping @ None,
                    ..
                } => {
                    *stepping = Some(claim);
                    *cursor
                }
                State::Scanning { .. } | State::Incomplete(_) | State::Whole => {
                    return Ok(entry.answer());
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

    /// Checks the next slice of `view`'s cached holes for presence in `read`, at most
    /// one step's reads, and drops the ones stored. A record stored on a branch and
    /// epoch stays stored there, so any snapshot that shows it proves it stored now.
    fn recheck(&self, read: &dyn SnapshotRead, view: &TopicView) -> Result<Integrity> {
        let topic_id = view.state.topic_id;
        let key = (view.state.genesis, view.epoch);
        let reads = self.reads.load(Ordering::Relaxed).max(1);
        let slice = {
            let mut topics = self.topics()?;
            let entry = topics.get_mut(&topic_id).filter(|entry| entry.key == key);
            match entry.and_then(Entry::found_mut) {
                Some(found) => found.next(reads),
                // A reset or recheck replaced the entry after the step answered.
                None => return Ok(Integrity::Unknown),
            }
        };
        let mut stored = Vec::new();
        for id in slice {
            if read.dep_resolvable(&id)? {
                stored.push(id);
            }
        }
        let mut topics = self.topics()?;
        let Some(entry) = topics.get_mut(&topic_id).filter(|entry| entry.key == key) else {
            return Ok(Integrity::Unknown);
        };
        for id in &stored {
            entry.remove(id);
        }
        Ok(entry.answer())
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

    /// The ids of `ops` that are holes of their topics now, with the branch and
    /// epoch of the scan or verdict that holds them.
    pub(crate) fn listed<'a>(
        &self,
        ops: impl IntoIterator<Item = &'a crate::Op>,
    ) -> Result<Vec<Listed>> {
        let topics = self.topics()?;
        Ok(ops
            .into_iter()
            .filter_map(|op| {
                let topic_id = op.signed.body.topic_id;
                let entry = topics.get(&topic_id)?;
                let listed = Listed {
                    topic_id,
                    key: entry.key,
                    id: op.id,
                };
                entry.holes()?.contains_key(&op.id).then_some(listed)
            })
            .collect())
    }

    /// Removes `filled` holes, now stored, from the branch and epoch that listed
    /// them. Admission never creates a hole, so a complete verdict left without one is whole.
    pub(crate) fn fill(&self, filled: &[Listed]) -> Result<()> {
        let mut topics = self.topics()?;
        for hole in filled {
            if let Some(entry) = topics.get_mut(&hole.topic_id)
                && entry.key == hole.key
            {
                entry.remove(&hole.id);
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

    #[cfg(test)]
    pub(crate) fn set_reads(&self, reads: usize) {
        self.reads.store(reads, Ordering::Relaxed);
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
                found: Found::default(),
                stepping: None,
            },
        }
    }

    /// The answer to a question that takes no step.
    fn answer(&self) -> Integrity {
        match &self.state {
            State::Whole => Integrity::Whole,
            State::Incomplete(found) => Integrity::Incomplete(found.holes.clone()),
            State::Scanning {
                cursor,
                found,
                stepping,
            } => {
                if stepping.is_some() && *cursor == Cursor::default() && found.holes.is_empty() {
                    return Integrity::Unknown;
                }
                Integrity::Scanning(found.holes.clone())
            }
        }
    }

    fn found_mut(&mut self) -> Option<&mut Found> {
        match &mut self.state {
            State::Scanning { found, .. } | State::Incomplete(found) => Some(found),
            State::Whole => None,
        }
    }

    fn holes(&self) -> Option<&Holes> {
        match &self.state {
            State::Scanning { found, .. } | State::Incomplete(found) => Some(&found.holes),
            State::Whole => None,
        }
    }

    fn remove(&mut self, id: &OpId) {
        match &mut self.state {
            State::Scanning { found, .. } => {
                found.holes.remove(id);
            }
            State::Incomplete(found) => {
                found.holes.remove(id);
                if found.holes.is_empty() {
                    self.state = State::Whole;
                }
            }
            State::Whole => {}
        }
    }
}

impl Found {
    /// Up to `reads` cached holes after the last one checked. A shorter slice
    /// reached the end, so the next question starts from the first hole again.
    fn next(&mut self, reads: usize) -> Vec<OpId> {
        let start = self.checked.map_or(Bound::Unbounded, Bound::Excluded);
        let slice = self
            .holes
            .range((start, Bound::Unbounded))
            .map(|(id, _)| *id)
            .take(reads)
            .collect::<Vec<_>>();
        self.checked = if slice.len() < reads {
            None
        } else {
            slice.last().copied()
        };
        slice
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
            found,
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
        // A hole found in an older snapshot may be stored now; the question's
        // recheck of this answer and later ones drop it.
        for (id, generation) in step.holes {
            let known = found.holes.entry(id).or_insert(generation);
            *known = known.or(generation);
        }
        if !step.done {
            return Integrity::Scanning(found.holes.clone());
        }
        let found = std::mem::take(found);
        if found.holes.is_empty() {
            entry.state = State::Whole;
            return Integrity::Whole;
        }
        let holes = found.holes.clone();
        entry.state = State::Incomplete(found);
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
