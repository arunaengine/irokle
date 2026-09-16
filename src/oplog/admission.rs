// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::BTreeSet;

use crate::storage::OpMeta;
use crate::{Error, Op, Result};

pub(super) fn next_actor_position(
    tip: Option<(u64, crate::OpId)>,
) -> Result<(u64, Option<crate::OpId>)> {
    match tip {
        Some((seq, id)) => Ok((checked_next(seq)?, Some(id))),
        None => Ok((1, None)),
    }
}

pub(super) fn checked_next(value: u64) -> Result<u64> {
    value.checked_add(1).ok_or(Error::InvalidOpId)
}

pub(super) fn ensure_event_type(expected: &str, actual: &str) -> Result<()> {
    if expected == actual {
        Ok(())
    } else {
        Err(Error::EventTypeMismatch {
            expected: expected.to_owned(),
            actual: actual.to_owned(),
        })
    }
}

/// Failures no later local state can turn into an admission: the signed op
/// itself is invalid. Discarding a pending op destroys signed work, so a
/// state dependent failure must never be classified here.
pub(super) fn is_permanent_rejection(err: &Error) -> bool {
    #[cfg(feature = "iroh")]
    if matches!(err, Error::OpTooLarge) {
        return true;
    }
    matches!(
        err,
        Error::InvalidSignature
            | Error::InvalidPublicKey
            | Error::WrongSigner
            | Error::ActorAuthorMismatch
            | Error::TopicMismatch
            | Error::GenerationMismatch { .. }
            | Error::RejectedOp(_)
    )
}

pub(super) fn is_admission_race(err: &Error) -> bool {
    // A generation mismatch here is a concurrent admission advancing
    // max_generation between the heads read and op validation, not immutable
    // invalidity: the retry recomputes the generation from fresh state.
    matches!(
        err,
        Error::AdmissionConflict
            | Error::ActorSeqGap { .. }
            | Error::ActorPrevMismatch
            | Error::ActorFork
            | Error::InvalidOpId
            | Error::GenerationMismatch { .. }
    )
}

pub(super) fn heads_after(current: &BTreeSet<crate::OpId>, op: &Op) -> BTreeSet<crate::OpId> {
    let mut heads = current.clone();
    for dep in &op.signed.body.deps {
        heads.remove(dep);
    }
    heads.insert(op.id);
    heads
}

pub(super) fn pending_meta_for(op: &Op, missing_deps: BTreeSet<crate::OpId>) -> OpMeta {
    let body = &op.signed.body;
    OpMeta {
        id: op.id,
        topic_id: body.topic_id,
        author: body.author,
        actor_id: body.actor_id,
        actor_seq: body.actor_seq,
        actor_prev: body.actor_prev,
        deps: body.deps.clone(),
        generation: body.generation,
        observed_clock: crate::ActorClock::new(),
        ready: false,
        missing_deps,
    }
}
