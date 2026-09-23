// SPDX-License-Identifier: MIT OR Apache-2.0
//! Reducer traits and typed event records for application projections.

use crate::history::HistoryCursor;
use crate::{ActorClock, ActorId, OpId};

pub trait Reducer<E> {
    type State;
    type Error;
    fn apply(
        &mut self,
        state: &mut Self::State,
        record: &EventRecord<E>,
    ) -> Result<(), Self::Error>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpMeta {
    pub op_id: OpId,
    pub actor_id: ActorId,
    pub actor_seq: u64,
    pub observed_clock: ActorClock,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventRecord<E> {
    pub event: E,
    pub meta: OpMeta,
}

impl<E> EventRecord<E> {
    pub fn new(
        event: E,
        op_id: OpId,
        actor_id: ActorId,
        actor_seq: u64,
        observed_clock: ActorClock,
    ) -> Self {
        Self {
            event,
            meta: OpMeta {
                op_id,
                actor_id,
                actor_seq,
                observed_clock,
            },
        }
    }
}

/// One event of a typed history read that reports each record on its own. A
/// payload that does not decode as `E` names its op instead of failing the read.
#[derive(Debug)]
pub enum HistoryEntry<E> {
    Event(EventRecord<E>),
    Undecodable { meta: OpMeta, error: crate::Error },
}

impl<E> HistoryEntry<E> {
    /// The decoded record, or the error its payload failed with.
    pub fn into_record(self) -> crate::Result<EventRecord<E>> {
        match self {
            Self::Event(record) => Ok(record),
            Self::Undecodable { error, .. } => Err(error),
        }
    }
}

/// Events after a cursor, oldest first, read from one snapshot, with the cursor
/// that covers exactly these events. The next read passes `cursor`.
#[derive(Debug)]
pub struct HistoryPage<E> {
    pub entries: Vec<HistoryEntry<E>>,
    pub cursor: HistoryCursor,
}
