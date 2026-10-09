// SPDX-License-Identifier: MIT OR Apache-2.0
//! One admission per topic at a time within an oplog.

use std::collections::BTreeSet;
use std::sync::{Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::{Error, Result, TopicId};

/// Longest wait for a turn. Past it the waiter fails with a retryable conflict
/// instead of validating beside the holder, and gives its thread back.
const TURN_PATIENCE: Duration = Duration::from_secs(60);

/// Topics with an admission in progress. A second admission of the same topic
/// waits for its turn instead of validating against heads the first is about to
/// move, which would void its commit and repeat all of its validation.
pub(super) struct AdmissionTurns {
    state: Mutex<Busy>,
    freed: Condvar,
    patience: Duration,
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

impl Default for AdmissionTurns {
    fn default() -> Self {
        Self::with_patience(TURN_PATIENCE)
    }
}

impl AdmissionTurns {
    fn with_patience(patience: Duration) -> Self {
        Self {
            state: Mutex::default(),
            freed: Condvar::new(),
            patience,
        }
    }

    /// Waits for the topic's turn; a wait past the patience fails with `AdmissionConflict`.
    pub(super) fn take(&self, topic_id: TopicId) -> Result<AdmissionTurn<'_>> {
        let deadline = Instant::now() + self.patience;
        let mut busy = self.state.lock().map_err(poisoned)?;
        while busy.topics.contains(&topic_id) {
            let Some(left) = deadline
                .checked_duration_since(Instant::now())
                .filter(|left| !left.is_zero())
            else {
                tracing::warn!(
                    %topic_id,
                    waited_ms = self.patience.as_millis() as u64,
                    "admission gave up waiting for its topic turn"
                );
                return Err(Error::AdmissionConflict);
            };
            busy.waiting += 1;
            busy = self.freed.wait_timeout(busy, left).map_err(poisoned)?.0;
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

fn poisoned<T>(_: PoisonError<T>) -> Error {
    Error::Storage("admission turn lock poisoned".into())
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
    use std::time::Duration;

    use super::AdmissionTurns;
    use crate::{Error, TopicId};

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

    /// A waiter past the patience fails with a retryable conflict and starts no
    /// admission beside the holder, which still gives its turn back.
    #[test]
    fn patience_ends_wait() {
        let turns = AdmissionTurns::with_patience(Duration::from_millis(10));
        let topic = TopicId::hash(b"turns-patience");
        let held = turns.take(topic).unwrap();
        assert!(matches!(turns.take(topic), Err(Error::AdmissionConflict)));
        assert!(turns.state.lock().unwrap().topics.contains(&topic));
        drop(held);
        assert!(turns.take(topic).is_ok());
    }
}
