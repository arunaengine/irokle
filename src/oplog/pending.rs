// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::storage::TopicState;
use crate::{Error, Op, OpId, Result, TopicPayload, actor_id_for};

use super::admission::{checked_next, is_permanent_rejection};
use super::{
    Admitted, BatchOverlay, MAX_DRAIN_OPS, MAX_PENDING_MISSING_DEPS, OpAdmission, Oplog,
    PendingVerdict, READY_SLICE, ReceiveEffects, StoredOp, admission_failure,
};

impl<S: super::Storage> Oplog<S> {
    /// Admit every buffered op whose dependencies resolved, in finite passes.
    pub fn reconcile_pending_ops(&self) -> Result<BTreeSet<crate::OpId>> {
        let mut accepted = BTreeSet::new();
        loop {
            let pass = self.receive_ops_admission(None, Vec::new(), &BTreeSet::new(), None)?;
            // A pass that admitted nothing met only retained ops; repeating it now spins.
            let progressed = !pass.accepted.is_empty();
            accepted.extend(pass.accepted);
            if !pass.ready_remaining || !progressed {
                return Ok(accepted);
            }
        }
    }

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
                // Pending ops re-queued from storage are not in verified; they
                // get re-verified during admission like before.
                let (batch_accepted, batch_eviction) = match self.admit_ops_batch_retry(
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

    /// Whether a buffered op that failed admission with error can never be
    /// admitted on this branch. Only immutable facts reject: its own signed
    /// content and the stored records of its dependencies and actor slots.
    fn pending_verdict(&self, op: &Op, error: &Error) -> Result<PendingVerdict> {
        let immutable = match error {
            error if is_permanent_rejection(error) => true,
            // Raised only once every dependency is known, so they describe the
            // op's causal frontier, which no later arrival changes.
            Error::NotTopicMember
            | Error::EventTypeMismatch { .. }
            | Error::InvalidGenesis
            | Error::InvalidOpId => true,
            Error::ActorPrevMismatch | Error::ActorSeqGap { .. } => self.position_impossible(op)?,
            Error::ActorFork => self.slot_taken(op, op.signed.body.actor_seq)?.is_some(),
            _ => false,
        };
        Ok(if immutable {
            PendingVerdict::Reject
        } else {
            PendingVerdict::Retain
        })
    }

    /// Whether the op actor position contradicts stored records: a known
    /// predecessor of another actor or sequence, or a slot before it that a
    /// different admitted op already holds.
    fn position_impossible(&self, op: &Op) -> Result<bool> {
        let body = &op.signed.body;
        let Some(prev) = body.actor_prev else {
            return Ok(body.actor_seq != 1);
        };
        if body.actor_seq < 2 || !body.deps.contains(&prev) {
            return Ok(true);
        }
        if let Some(meta) = self.storage.get_position(&prev)?
            && self.storage.dep_resolvable(&prev)?
            && (meta.topic_id != body.topic_id
                || meta.actor_id != body.actor_id
                || checked_next(meta.actor_seq)? != body.actor_seq)
        {
            return Ok(true);
        }
        Ok(self
            .slot_taken(op, body.actor_seq - 1)?
            .is_some_and(|holder| holder != prev))
    }

    /// The admitted op holding seq of the op actor, if it is not the op.
    fn slot_taken(&self, op: &Op, seq: u64) -> Result<Option<OpId>> {
        let body = &op.signed.body;
        let Some(holder) = self
            .storage
            .actor_index(&body.topic_id, &body.actor_id, seq)?
        else {
            return Ok(None);
        };
        Ok((holder != op.id && self.storage.dep_resolvable(&holder)?).then_some(holder))
    }

    pub(super) fn purge_losing_pending(&self, losing_genesis: OpId) -> Result<()> {
        self.storage
            .purge_pending_waiters(&losing_genesis)
            .map(drop)
    }

    pub(super) fn missing_deps_projected(
        &self,
        op: &Op,
        overlay_ops: &BTreeMap<crate::OpId, Op>,
        reset: bool,
    ) -> Result<BTreeSet<crate::OpId>> {
        let mut missing = BTreeSet::new();
        for dep in &op.signed.body.deps {
            if overlay_ops.contains_key(dep) {
                continue;
            }
            if reset || !self.storage.dep_resolvable(dep)? {
                missing.insert(*dep);
            }
        }
        Ok(missing)
    }

    pub(super) fn validate_pending_op(
        &self,
        op: &Op,
        missing_deps: &BTreeSet<crate::OpId>,
        overlay: &BatchOverlay<'_>,
        state: Option<&TopicState>,
    ) -> Result<OpAdmission> {
        let body = &op.signed.body;
        if missing_deps.len() > MAX_PENDING_MISSING_DEPS {
            return Err(Error::Storage(
                "pending op has too many missing deps".into(),
            ));
        }
        if body.actor_id != actor_id_for(body.topic_id, body.author) {
            return Err(Error::ActorAuthorMismatch);
        }
        if body.actor_seq == 0 {
            return Err(Error::ActorSeqGap {
                expected: 1,
                actual: 0,
            });
        }
        if let Some(existing) = self.stored_actor_index(body, overlay.reset)?.or_else(|| {
            overlay
                .index
                .get(&(body.topic_id, body.actor_id, body.actor_seq))
                .copied()
        }) {
            if existing != op.id {
                return Err(Error::ActorFork);
            }
            if self.is_admitted_duplicate(op)? {
                return Ok(OpAdmission::Duplicate);
            }
        }
        match &body.payload {
            TopicPayload::Genesis(_) => {
                if body.actor_seq != 1
                    || body.actor_prev.is_some()
                    || !body.deps.is_empty()
                    || state.is_some()
                {
                    return Err(Error::InvalidGenesis);
                }
            }
            TopicPayload::Event(envelope) => {
                if body.deps.is_empty() || body.generation == 0 {
                    return Err(Error::InvalidOpId);
                }
                // Latest membership says nothing about the op causal frontier,
                // which is unknown while a dependency is missing; the source
                // pending quota bounds what an unproven author can buffer.
                if let Some(state) = state {
                    super::admission::ensure_event_type(&state.event_type_id, &envelope.type_id)?;
                }
            }
            TopicPayload::Control(_) => {
                if body.deps.is_empty() || body.generation == 0 {
                    return Err(Error::InvalidOpId);
                }
            }
        }
        match (body.actor_seq, body.actor_prev) {
            (1, Some(_)) => return Err(Error::ActorPrevMismatch),
            (2.., None) => return Err(Error::ActorPrevMismatch),
            _ => {}
        }
        if let Some(prev) = body.actor_prev {
            if !body.deps.contains(&prev) {
                return Err(Error::ActorPrevMismatch);
            }
            if !missing_deps.contains(&prev) {
                let prev_meta = self.header_projected(&prev, overlay.meta)?;
                if prev_meta.topic_id != body.topic_id || prev_meta.actor_id != body.actor_id {
                    return Err(Error::ActorPrevMismatch);
                }
                if checked_next(prev_meta.actor_seq)? != body.actor_seq {
                    return Err(Error::ActorSeqGap {
                        expected: checked_next(prev_meta.actor_seq)?,
                        actual: body.actor_seq,
                    });
                }
            }
        }
        let expected = match overlay.tips.get(&(body.topic_id, body.actor_id)).copied() {
            Some(tip) => Some(tip),
            None => self.stored_actor_tip(body, overlay.reset)?,
        };
        if let Some((tip_seq, tip_id)) = expected {
            let next_seq = checked_next(tip_seq)?;
            if body.actor_seq <= tip_seq {
                if self.is_admitted_duplicate(op)? {
                    return Ok(OpAdmission::Duplicate);
                }
                return Err(Error::ActorSeqGap {
                    expected: next_seq,
                    actual: body.actor_seq,
                });
            }
            if body.actor_prev == Some(tip_id) && body.actor_seq != next_seq {
                return Err(Error::ActorSeqGap {
                    expected: next_seq,
                    actual: body.actor_seq,
                });
            }
        }
        for dep in &body.deps {
            if missing_deps.contains(dep) {
                continue;
            }
            let meta = self.header_projected(dep, overlay.meta)?;
            if meta.topic_id != body.topic_id {
                return Err(Error::TopicMismatch);
            }
            if meta.generation >= body.generation {
                return Err(Error::GenerationMismatch {
                    expected: checked_next(meta.generation)?,
                    actual: body.generation,
                });
            }
        }
        Ok(OpAdmission::Admit)
    }

    pub(super) fn stored_op_state(&self, op: &Op) -> Result<StoredOp> {
        let has_op = self.storage.get_op(&op.id)?.is_some();
        let has_meta = self.storage.get_position(&op.id)?.is_some();
        if has_op && has_meta {
            return Ok(StoredOp::Complete);
        }
        if has_op || has_meta {
            return Ok(StoredOp::Repair);
        }
        let body = &op.signed.body;
        if self
            .storage
            .actor_index(&body.topic_id, &body.actor_id, body.actor_seq)?
            == Some(op.id)
            || !self.storage.children(&op.id)?.is_empty()
        {
            return Ok(StoredOp::Repair);
        }
        Ok(StoredOp::Absent)
    }
}
