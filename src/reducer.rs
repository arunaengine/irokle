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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReadyBatch<E> {
    pub records: Vec<EventRecord<E>>,
}

pub fn apply_batch<E, R: Reducer<E>>(
    reducer: &mut R,
    state: &mut R::State,
    batch: &ReadyBatch<E>,
) -> Result<(), R::Error> {
    for record in &batch.records {
        reducer.apply(state, record)?;
    }
    Ok(())
}
