// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use crate::{Error, Op, Result};

use super::pending::{PendingVerdict, pending_meta_for};
use super::{
    Admitted, BuiltBatch, MAX_ADMISSION_RETRIES, MAX_CACHED_PROJECTIONS, MembershipCache, Oplog,
    ReceiveEffects, ResetPlan, TopicEviction, conflict_pause, is_structural_genesis,
};

/// Buffered ops one admission call visits without admitting any, and how many
/// it reads at once. A window that admits something starts another.
const MAX_DRAIN_OPS: usize = 4096;
const READY_SLICE: usize = 256;

impl<S: super::Storage> Oplog<S> {
    pub(super) fn receive_ops_admission(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
        verified: &BTreeSet<crate::OpId>,
        effects: Option<ReceiveEffects<'_>>,
    ) -> Result<Admitted> {
        let mut admitted = Admitted::default();
        let result = (|| -> Result<()> {
            let mut queue = VecDeque::new();
            if !ops.is_empty() {
                queue.push_back((source_peer, ops, false));
            }
            // Buffered ops come from the ready index in slices, so only ops
            // whose waits resolved are read, and one call does finite work.
            let mut drained = 0;
            let mut cursor = None;
            let mut pass_admitted = false;
            let mut window_admitted = false;
            // Ops retained in this call are not tried again until a later call.
            let mut held_back = BTreeSet::new();
            loop {
                if queue.is_empty() {
                    if drained >= MAX_DRAIN_OPS {
                        // Visits spent re-reading retained ops must not strand
                        // work released later: an admitting window starts another.
                        if window_admitted {
                            drained = 0;
                            window_admitted = false;
                            cursor = None;
                            pass_admitted = false;
                            continue;
                        }
                        admitted.ready_remaining =
                            !self.storage.ready_pending_after(None, 1)?.is_empty();
                        break;
                    }
                    let slice = self
                        .storage
                        .ready_pending_after(cursor.as_ref(), READY_SLICE)?;
                    let Some((_, last)) = slice.last() else {
                        if !pass_admitted {
                            break;
                        }
                        // Admissions of this pass may have readied ops the
                        // cursor already passed.
                        cursor = None;
                        pass_admitted = false;
                        continue;
                    };
                    cursor = Some(last.id);
                    drained += slice.len();
                    for (source, op) in slice {
                        if !held_back.contains(&op.id) {
                            queue.push_back((Some(source), vec![op], true));
                        }
                    }
                }
                let Some((batch_source_peer, ops, from_pending)) = queue.pop_front() else {
                    continue;
                };
                // A permanent rejection names the batch, not the op inside it,
                // so the retained copy lets one invalid record be isolated
                // without discarding the valid ops queued beside it.
                let retained = if from_pending {
                    ops.clone()
                } else {
                    Vec::new()
                };
                // Pending ops re-queued from storage are not in `verified`; they
                // get re-verified during admission like before.
                let (batch_accepted, batch_eviction) = match self.admit_with_retry(
                    batch_source_peer,
                    ops,
                    verified,
                    effects,
                ) {
                    Ok(outcome) => outcome,
                    Err(err) if from_pending && retained.len() == 1 => {
                        let op = &retained[0];
                        match self.pending_verdict(op, &err)? {
                            PendingVerdict::Reject => {
                                tracing::debug!(op_id = %op.id, error = %err, "rejecting pending subtree");
                                self.storage.reject_pending_subtree(&op.id)?;
                            }
                            // A repairable failure keeps the buffered record and
                            // must not fail the ops the caller actually sent.
                            PendingVerdict::Retain => {
                                tracing::debug!(op_id = %op.id, error = %err, "retaining pending op");
                                held_back.insert(op.id);
                            }
                        }
                        continue;
                    }
                    // Several buffered ops failed together: judge each alone, in
                    // dependency order, so only the offending subtree goes.
                    Err(_) if from_pending => {
                        for op in retained {
                            queue.push_back((batch_source_peer, vec![op], true));
                        }
                        continue;
                    }
                    Err(err) => return Err(err),
                };
                if let Some(eviction) = batch_eviction {
                    admitted.evictions.push(eviction);
                }
                pass_admitted |= !batch_accepted.is_empty();
                window_admitted |= !batch_accepted.is_empty();
                admitted.accepted.extend(batch_accepted.iter().copied());
            }

            Ok(())
        })();
        match result {
            Ok(()) => Ok(admitted),
            Err(error) => Err(admission_failure(admitted, error)),
        }
    }

    fn admit_with_retry(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
        verified: &BTreeSet<crate::OpId>,
        effects: Option<ReceiveEffects<'_>>,
    ) -> Result<(BTreeSet<crate::OpId>, Option<TopicEviction>)> {
        if let Some((topic, genesis)) = self.receive_genesis {
            for op in ops
                .iter()
                .filter(|op| op.signed.body.topic_id == topic && is_structural_genesis(op))
            {
                if op.id != genesis {
                    return Err(Error::StaleIncarnation);
                }
            }
        }
        let has_genesis = ops.iter().any(is_structural_genesis);
        // Signatures are immutable, so each is checked once for the whole job
        // rather than again on every retry.
        let mut checked = BTreeSet::new();
        for op in &ops {
            if !verified.contains(&op.id) {
                #[cfg(feature = "iroh")]
                op.validate_frame()?;
                op.validate()?;
            }
            checked.insert(op.id);
        }
        let verified = &checked;
        let topic_id = ops.first().map(|op| op.signed.body.topic_id);
        let heads = || {
            topic_id
                .map(|topic_id| self.storage.heads(&topic_id))
                .transpose()
        };
        for attempt in 0..MAX_ADMISSION_RETRIES {
            conflict_pause(attempt);
            let before = heads()?;
            if let Some((topic, genesis)) = self.receive_genesis
                && ops
                    .first()
                    .is_some_and(|op| op.signed.body.topic_id == topic)
                && self
                    .storage
                    .topic_state(&topic)?
                    .is_some_and(|state| state.genesis < genesis)
            {
                return Err(Error::StaleIncarnation);
            }
            let (ops_to_admit, reset, rejected_genesis) = if has_genesis {
                self.resolve_genesis_collision(ops.clone(), verified)?
            } else {
                (ops.clone(), None, None)
            };
            let eviction = reset.as_ref().map(|plan| plan.eviction.clone());
            if let Some(losing) = rejected_genesis {
                // A losing genesis is never admitted here, so its waiters can never become ready.
                self.storage.purge_pending_waiters(&losing)?;
            }
            // A won foreign genesis discards the local chain: admit the winner
            // batch against a fresh topic and fold the reset into the same
            // storage transaction as its admission (`reset_topic_and_admit`).
            match self.admit_ops_batch(source_peer, ops_to_admit, verified, reset, effects) {
                Err(Error::AdmissionConflict) => continue,
                // Validation reads the store op by op, so a commit landing meanwhile can
                // skip ops the store now holds and fake a gap; moved heads retry it.
                Err(err) if is_local_race(&err) && heads()? != before => continue,
                Ok(accepted) => return Ok((accepted, eviction)),
                Err(err) => return Err(err),
            }
        }
        Err(Error::AdmissionConflict)
    }

    pub(super) fn admit_ops_batch(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
        verified: &BTreeSet<crate::OpId>,
        reset_plan: Option<ResetPlan>,
        receive_effects: Option<ReceiveEffects<'_>>,
    ) -> Result<BTreeSet<crate::OpId>> {
        if reset_plan.is_some() {
            *self.membership_cache()? = MembershipCache::default();
        }
        let Some(BuiltBatch {
            accepted,
            batch,
            pending,
            projection_epoch,
            projections,
            projection_tips,
        }) = self.build_batch(
            source_peer,
            ops,
            verified,
            reset_plan.is_some(),
            receive_effects,
        )?
        else {
            return Ok(BTreeSet::new());
        };
        let topic_id = batch.topic_id;
        if let Some(plan) = &reset_plan {
            // Reset, winner admission, and the record of the discarded payloads
            // share one storage transaction, so a crash never leaves the topic
            // empty with the winner uninstalled or the payloads unrecorded.
            self.storage.reset_topic_and_admit(
                &topic_id,
                &plan.expected_state,
                batch,
                Some(&plan.eviction),
            )?;
            if let Ok(mut cache) = self.membership_cache() {
                *cache = MembershipCache::default();
            }
        } else if !batch.entries.is_empty() {
            self.storage.put_admitted_batch(batch)?;
        }

        // Buffer pending ops after admission, so a reset cannot wipe the descendants of a
        // partial winner batch. A pending op's missing deps are never in `entries`, so
        // ordering after admission cannot spuriously reject it.
        for (op, missing_deps) in pending {
            let source_peer = source_peer.unwrap_or(op.signed.body.author);
            let buffered = self.storage.put_pending_bound(
                source_peer,
                op.clone(),
                pending_meta_for(&op, missing_deps),
                self.receive_genesis
                    .filter(|(topic, _)| *topic == topic_id)
                    .map(|(_, genesis)| genesis),
            );
            // A descendant of a rejected op can never be admitted here.
            if let Err(Error::RejectedOp(rejected)) = &buffered {
                tracing::debug!(op_id = %op.id, %rejected, "dropping op behind a rejected op");
                continue;
            }
            buffered.map_err(|error| {
                admission_failure(
                    Admitted {
                        ready_remaining: false,
                        accepted: accepted.clone(),
                        evictions: reset_plan
                            .as_ref()
                            .map(|plan| plan.eviction.clone())
                            .into_iter()
                            .collect(),
                    },
                    error,
                )
            })?;
        }

        // A reset or integrity recheck must also invalidate in-flight cache writes.
        if let Ok(mut cache) = self.membership_cache()
            && Arc::ptr_eq(&projection_epoch, &cache.epoch)
        {
            for id in projection_tips {
                if let Some(state) = projections.get(&id)
                    && !cache.states.contains_key(&id)
                {
                    if cache.states.len() == MAX_CACHED_PROJECTIONS
                        && let Some(oldest) = cache.order.pop_front()
                    {
                        cache.states.remove(&oldest);
                    }
                    cache.states.insert(id, Arc::clone(state));
                    cache.order.push_back(id);
                }
            }
        }
        Ok(accepted)
    }
}

fn admission_failure(mut admitted: Admitted, error: Error) -> Error {
    let source = match error {
        Error::AdmissionCommitted {
            admitted: partial,
            source,
        } => {
            admitted.accepted.extend(partial.accepted);
            admitted.evictions.extend(partial.evictions);
            source
        }
        error => Box::new(error),
    };
    if admitted.accepted.is_empty() && admitted.evictions.is_empty() {
        *source
    } else {
        Error::AdmissionCommitted {
            admitted: Box::new(admitted),
            source,
        }
    }
}

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

pub(super) fn is_local_race(err: &Error) -> bool {
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
