// SPDX-License-Identifier: MIT OR Apache-2.0
//! One admission per topic at a time within an oplog.

use std::collections::BTreeSet;
use std::sync::{Condvar, Mutex, PoisonError};

use crate::{Error, Result, TopicId};

/// Topics with an admission in progress. A second admission of the same topic
/// waits for its turn instead of validating against heads the first is about to
/// move, which would void its commit and repeat all of its validation.
#[derive(Default)]
pub(super) struct AdmissionTurns {
    state: Mutex<Busy>,
    freed: Condvar,
}

#[derive(Default)]
struct Busy {
    topics: BTreeSet<TopicId>,
    waiting: usize,
}

/// The turn of one admission, given back when dropped.
pub(super) struct AdmissionTurn<'a> {
    turns: &'a AdmissionTurns,
    topic_id: TopicId,
}

impl AdmissionTurns {
    pub(super) fn take(&self, topic_id: TopicId) -> Result<AdmissionTurn<'_>> {
        let poisoned = |_| Error::Storage("admission turn lock poisoned".into());
        let mut busy = self.state.lock().map_err(poisoned)?;
        while busy.topics.contains(&topic_id) {
            busy.waiting += 1;
            busy = self.freed.wait(busy).map_err(poisoned)?;
            busy.waiting -= 1;
        }
        busy.topics.insert(topic_id);
        Ok(AdmissionTurn {
            turns: self,
            topic_id,
        })
    }

    /// Admissions waiting for a turn now.
    #[cfg(test)]
    pub(super) fn waiting(&self) -> usize {
        self.state.lock().unwrap().waiting
    }
}

impl Drop for AdmissionTurn<'_> {
    fn drop(&mut self) {
        let mut busy = self
            .turns
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        busy.topics.remove(&self.topic_id);
        drop(busy);
        self.turns.freed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::AdmissionTurns;
    use crate::TopicId;

    /// A second turn on a topic starts only after the first is given back, while
    /// another topic takes its turn at once.
    #[test]
    fn turns_serialize_topic() {
        let turns = Arc::new(AdmissionTurns::default());
        let topic = TopicId::hash(b"turns-topic");
        let released = Arc::new(AtomicBool::new(false));
        let first = turns.take(topic).unwrap();
        drop(turns.take(TopicId::hash(b"turns-other")).unwrap());
        let second = {
            let (turns, released) = (Arc::clone(&turns), Arc::clone(&released));
            std::thread::spawn(move || {
                let _turn = turns.take(topic).unwrap();
                released.load(Ordering::SeqCst)
            })
        };
        released.store(true, Ordering::SeqCst);
        drop(first);
        assert!(
            second.join().unwrap(),
            "second turn started before the first ended"
        );
        assert!(turns.state.lock().unwrap().topics.is_empty());
    }
}
