// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use crate::storage::{AdmissionEffects, AdmittedBatch, OpMeta, TopicState};
use crate::{Error, Op, OpId, Result, TopicId, TopicPayload, actor_id_for};

use super::membership::{apply_control, materialize_topic_state, merge_states};
use super::pending::{PendingVerdict, pending_meta_for};
use super::topology::topological_ops;
use super::{
    Admitted, BatchOverlay, MAX_ADMISSION_RETRIES, MAX_CACHED_PROJECTIONS, MembershipCache,
    OpAdmission, Oplog, ReceiveEffects, ResetPlan, TopicEviction, conflict_pause,
    is_structural_genesis,
};

/// Buffered ops one admission call visits without admitting any, and how many
/// it reads at once. A window that admits something starts another.
const MAX_DRAIN_OPS: usize = 4096;
const READY_SLICE: usize = 256;

/// How much of an op the local store holds. `Repair` means its id is already in
/// the chain but records are incomplete; refilling it is not an append.
enum StoredOp {
    Absent,
    Repair,
    Complete,
}

/// A validated admission not yet committed, with what follows its commit.
struct BuiltBatch {
    accepted: BTreeSet<OpId>,
    batch: AdmittedBatch,
    pending: Vec<(Op, BTreeSet<OpId>)>,
    projection_epoch: Arc<()>,
    projections: BTreeMap<OpId, Arc<TopicState>>,
    projection_tips: BTreeSet<OpId>,
}

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

    /// Validate `ops` against the stored topic, or against a fresh topic when
    /// `reset`, and build the batch admission would commit, without writing.
    fn build_batch(
        &self,
        source_peer: Option<crate::PeerId>,
        ops: Vec<Op>,
        verified: &BTreeSet<crate::OpId>,
        reset: bool,
        receive_effects: Option<ReceiveEffects<'_>>,
    ) -> Result<Option<BuiltBatch>> {
        let ops = topological_ops(ops)?;
        let mut accepted = BTreeSet::new();
        let Some(topic_id) = ops.first().map(|op| op.signed.body.topic_id) else {
            return Ok(None);
        };
        if ops.iter().any(|op| op.signed.body.topic_id != topic_id) {
            return Err(Error::TopicMismatch);
        }

        // On reset the wiped topic admits the self-contained winner batch atomically.
        // Heads and state come from one read: a membership verdict taken from a
        // state newer than the heads would reject an authorized op for good.
        let (expected_heads, expected_state) = if reset {
            (BTreeSet::new(), None)
        } else {
            let state = self.storage.topic_state(&topic_id)?;
            let heads = state
                .as_ref()
                .map(|state| state.heads.clone())
                .unwrap_or_default();
            (heads, state)
        };
        let mut heads = expected_heads.clone();
        let mut state = expected_state.clone();
        if let Some((topic, genesis)) = self.receive_genesis
            && topic == topic_id
            && expected_state
                .as_ref()
                .map(|state| state.genesis)
                .or_else(|| {
                    ops.iter()
                        .find(|op| is_structural_genesis(op))
                        .map(|op| op.id)
                })
                != Some(genesis)
        {
            return Err(Error::StaleIncarnation);
        }
        let mut topic_state_changed = false;
        let mut overlay_ops = BTreeMap::new();
        let mut overlay_meta = BTreeMap::new();
        let mut overlay_tips = BTreeMap::new();
        let mut overlay_index = BTreeMap::new();
        let mut admitted = Vec::new();
        let mut pending = Vec::new();
        // Reuse immutable causal states only within the current genesis.
        let (projection_epoch, mut projections) = {
            let cache = self.membership_cache()?;
            let projections = cache
                .states
                .iter()
                .filter(|(_, state)| {
                    expected_state.as_ref().is_some_and(|expected| {
                        state.topic_id == topic_id && state.genesis == expected.genesis
                    })
                })
                .map(|(id, state)| (*id, Arc::clone(state)))
                .collect();
            (Arc::clone(&cache.epoch), projections)
        };
        let mut projection_tips = BTreeSet::new();

        for op in ops {
            #[cfg(feature = "iroh")]
            op.validate_frame()?;
            if !verified.contains(&op.id) {
                op.validate()?;
            }
            let stored = if reset {
                StoredOp::Absent
            } else {
                self.stored_op_state(&op)?
            };
            if matches!(stored, StoredOp::Complete) {
                // Records of a topic without state exist only while an
                // activation is unfinished; that history is not this batch's.
                if state.is_none() {
                    return Err(Error::AdmissionConflict);
                }
                continue;
            }

            let missing_deps = self.missing_deps_projected(&op, &overlay_ops, reset)?;
            // Refilling an already-accounted id changes no head, clock entry or
            // topic state: only the records themselves are rewritten, once its
            // dependencies can be resolved again.
            if matches!(stored, StoredOp::Repair) {
                if missing_deps.is_empty() {
                    let meta = self.validate_repair(
                        &op,
                        state.as_ref(),
                        &overlay_ops,
                        &overlay_meta,
                        &mut projections,
                    )?;
                    overlay_meta.insert(op.id, meta);
                    accepted.insert(op.id);
                    admitted.push(op.id);
                    overlay_ops.insert(op.id, op);
                } else {
                    pending.push((op, missing_deps));
                }
                continue;
            }
            if !missing_deps.is_empty() {
                match self.validate_pending_op(
                    &op,
                    &missing_deps,
                    &BatchOverlay {
                        ops: &overlay_ops,
                        meta: &overlay_meta,
                        tips: &overlay_tips,
                        index: &overlay_index,
                        reset,
                    },
                    state.as_ref(),
                )? {
                    OpAdmission::Duplicate => {}
                    OpAdmission::Admit => pending.push((op, missing_deps)),
                }
                continue;
            }

            if let OpAdmission::Duplicate = self.validate_op_projected(
                &op,
                &BatchOverlay {
                    ops: &overlay_ops,
                    meta: &overlay_meta,
                    tips: &overlay_tips,
                    index: &overlay_index,
                    reset,
                },
                &heads,
                state.as_ref(),
                &mut projections,
            )? {
                continue;
            }
            projection_tips = op.signed.body.deps.clone();
            let meta = self.meta_for_projected(&op, &overlay_meta)?;
            for dep in &op.signed.body.deps {
                heads.remove(dep);
            }
            heads.insert(op.id);
            match &op.signed.body.payload {
                TopicPayload::Genesis(genesis) => {
                    state = Some(TopicState {
                        topic_id,
                        event_type_id: genesis.event_type_id.clone(),
                        genesis: op.id,
                        heads: BTreeSet::new(),
                        members: genesis.initial_peers.clone(),
                        replication_policy: genesis.replication_policy.clone(),
                        membership_controls: BTreeMap::new(),
                        replication_policy_control: None,
                    });
                    topic_state_changed = true;
                }
                TopicPayload::Event(_) => {}
                TopicPayload::Control(control) => {
                    let state = state.as_mut().ok_or(Error::TopicNotFound)?;
                    apply_control(state, &op, control);
                    topic_state_changed = true;
                }
            }

            overlay_index.insert((topic_id, meta.actor_id, meta.actor_seq), op.id);
            overlay_tips.insert((topic_id, meta.actor_id), (meta.actor_seq, op.id));
            overlay_meta.insert(op.id, meta);
            accepted.insert(op.id);
            admitted.push(op.id);
            overlay_ops.insert(op.id, op);
        }
        // Records move out of the overlay, so the batch holds one copy of each
        // op and its observed clock. Ops are unique by id, so each is there.
        let entries = admitted
            .into_iter()
            .map(|id| Some((overlay_ops.remove(&id)?, overlay_meta.remove(&id)?)))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| Error::Storage("admitted op left the batch overlay".into()))?;

        if let Some(state) = &mut state {
            state.heads = heads.clone();
        }
        // Effects commit with the entries under the same expected state, so a
        // reset cannot slip between the ops and the work they create.
        let effects = match (receive_effects, state.as_ref()) {
            (Some(compute), Some(state)) if !entries.is_empty() => {
                compute(source_peer, &entries, state)?
            }
            _ => AdmissionEffects::default(),
        };
        Ok(Some(BuiltBatch {
            accepted,
            batch: AdmittedBatch {
                topic_id,
                expected_heads,
                expected_topic_state: expected_state,
                entries,
                heads,
                topic_state: topic_state_changed.then(|| state.clone()).flatten(),
                effects,
            },
            pending,
            projection_epoch,
            projections,
            projection_tips,
        }))
    }

    /// Validate a refill of an id the chain already accounts for, once its
    /// dependencies resolve, and return its metadata. It must match any stored copy.
    fn validate_repair(
        &self,
        op: &Op,
        state: Option<&TopicState>,
        overlay_ops: &BTreeMap<OpId, Op>,
        overlay_meta: &BTreeMap<OpId, OpMeta>,
        projections: &mut BTreeMap<OpId, Arc<TopicState>>,
    ) -> Result<OpMeta> {
        let body = &op.signed.body;
        if body.actor_id != actor_id_for(body.topic_id, body.author) {
            return Err(Error::ActorAuthorMismatch);
        }
        if self
            .storage
            .actor_index(&body.topic_id, &body.actor_id, body.actor_seq)?
            .is_some_and(|id| id != op.id)
        {
            return Err(Error::ActorFork);
        }
        match &body.payload {
            TopicPayload::Genesis(_) => {
                if !is_structural_genesis(op) || state.is_some_and(|state| state.genesis != op.id) {
                    return Err(Error::InvalidGenesis);
                }
            }
            TopicPayload::Event(envelope) => {
                let state = state.ok_or(Error::TopicNotFound)?;
                ensure_event_type(&state.event_type_id, &envelope.type_id)?;
                if !self
                    .project_membership(
                        &body.topic_id,
                        &body.deps,
                        overlay_ops,
                        overlay_meta,
                        projections,
                    )?
                    .members
                    .contains(&body.author)
                {
                    return Err(Error::NotTopicMember);
                }
            }
            TopicPayload::Control(_) => {
                state.ok_or(Error::TopicNotFound)?;
                if !self
                    .project_membership(
                        &body.topic_id,
                        &body.deps,
                        overlay_ops,
                        overlay_meta,
                        projections,
                    )?
                    .members
                    .contains(&body.author)
                {
                    return Err(Error::NotTopicMember);
                }
            }
        }
        match (body.actor_seq, body.actor_prev) {
            (1, None) => {}
            (2.., Some(prev)) if body.deps.contains(&prev) => {
                let prev_meta = self.header_projected(&prev, overlay_meta)?;
                if prev_meta.topic_id != body.topic_id
                    || prev_meta.actor_id != body.actor_id
                    || checked_next(prev_meta.actor_seq)? != body.actor_seq
                {
                    return Err(Error::ActorPrevMismatch);
                }
            }
            _ => return Err(Error::ActorPrevMismatch),
        }
        let mut generation = 0;
        for id in &body.deps {
            let dep_meta = self.header_projected(id, overlay_meta)?;
            if dep_meta.topic_id != body.topic_id {
                return Err(Error::TopicMismatch);
            }
            generation = generation.max(checked_next(dep_meta.generation)?);
        }
        if body.generation != generation {
            return Err(Error::GenerationMismatch {
                expected: generation,
                actual: body.generation,
            });
        }
        let meta = self.meta_for_projected(op, overlay_meta)?;
        if self
            .storage
            .get_meta(&op.id)?
            .is_some_and(|stored| stored != meta)
        {
            return Err(Error::InvalidOpId);
        }
        Ok(meta)
    }

    fn stored_op_state(&self, op: &Op) -> Result<StoredOp> {
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

    pub(super) fn validate_op_projected(
        &self,
        op: &Op,
        overlay: &BatchOverlay<'_>,
        heads: &BTreeSet<crate::OpId>,
        state: Option<&TopicState>,
        projections: &mut BTreeMap<OpId, Arc<TopicState>>,
    ) -> Result<OpAdmission> {
        let body = &op.signed.body;
        if body.actor_id != actor_id_for(body.topic_id, body.author) {
            return Err(Error::ActorAuthorMismatch);
        }
        if let Some(prev) = body.actor_prev
            && !body.deps.contains(&prev)
        {
            return Err(Error::ActorPrevMismatch);
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
                let state = state.ok_or(Error::TopicNotFound)?;
                ensure_event_type(&state.event_type_id, &envelope.type_id)?;
                let author_is_member = if body.deps == *heads {
                    state.members.contains(&body.author)
                } else {
                    self.project_membership(
                        &body.topic_id,
                        &body.deps,
                        overlay.ops,
                        overlay.meta,
                        projections,
                    )?
                    .members
                    .contains(&body.author)
                };
                if !author_is_member {
                    return Err(Error::NotTopicMember);
                }
            }
            TopicPayload::Control(_) => {
                let state = state.ok_or(Error::TopicNotFound)?;
                let author_is_member = if body.deps == *heads {
                    state.members.contains(&body.author)
                } else {
                    self.project_membership(
                        &body.topic_id,
                        &body.deps,
                        overlay.ops,
                        overlay.meta,
                        projections,
                    )?
                    .members
                    .contains(&body.author)
                };
                if !author_is_member {
                    return Err(Error::NotTopicMember);
                }
            }
        }
        let expected = match overlay.tips.get(&(body.topic_id, body.actor_id)).copied() {
            Some(tip) => Some(tip),
            None => self.stored_actor_tip(body, overlay.reset)?,
        };
        let (expected_seq, expected_prev) = next_actor_position(expected)?;
        if body.actor_seq != expected_seq {
            if body.actor_seq < expected_seq && self.is_admitted_duplicate(op)? {
                return Ok(OpAdmission::Duplicate);
            }
            return Err(Error::ActorSeqGap {
                expected: expected_seq,
                actual: body.actor_seq,
            });
        }
        if body.actor_prev != expected_prev {
            return Err(Error::ActorPrevMismatch);
        }
        let mut generation = 0;
        for id in &body.deps {
            let meta = self.header_projected(id, overlay.meta)?;
            generation = generation.max(checked_next(meta.generation)?);
        }
        if body.generation != generation {
            return Err(Error::GenerationMismatch {
                expected: generation,
                actual: body.generation,
            });
        }
        Ok(OpAdmission::Admit)
    }

    fn project_membership(
        &self,
        topic_id: &TopicId,
        deps: &BTreeSet<OpId>,
        overlay_ops: &BTreeMap<OpId, Op>,
        overlay_meta: &BTreeMap<OpId, OpMeta>,
        projections: &mut BTreeMap<OpId, Arc<TopicState>>,
    ) -> Result<Arc<TopicState>> {
        let mut pending = deps.iter().map(|id| (*id, false)).collect::<Vec<_>>();
        let mut visiting = BTreeSet::new();
        while let Some((id, visited)) = pending.pop() {
            if projections.contains_key(&id) {
                continue;
            }
            let meta = self.meta_projected(&id, overlay_meta)?;
            if meta.topic_id != *topic_id {
                return Err(Error::TopicMismatch);
            }
            if !visited {
                if !visiting.insert(id) {
                    return Err(Error::Storage("cycle in op graph".into()));
                }
                pending.push((id, true));
                pending.extend(meta.deps.iter().map(|dep| (*dep, false)));
                continue;
            }
            let op = self.op_projected(&id, overlay_ops)?;
            let state = match &op.signed.body.payload {
                TopicPayload::Genesis(_) => {
                    Arc::new(materialize_topic_state(vec![op], BTreeSet::new())?)
                }
                TopicPayload::Event(_) => merge_states(&meta.deps, projections)?,
                TopicPayload::Control(control) => {
                    let mut state = (*merge_states(&meta.deps, projections)?).clone();
                    apply_control(&mut state, &op, control);
                    Arc::new(state)
                }
            };
            visiting.remove(&id);
            projections.insert(id, state);
        }
        merge_states(deps, projections)
    }

    fn meta_projected<'a>(
        &self,
        id: &crate::OpId,
        overlay_meta: &'a BTreeMap<crate::OpId, OpMeta>,
    ) -> Result<std::borrow::Cow<'a, OpMeta>> {
        match overlay_meta.get(id) {
            Some(meta) => Ok(std::borrow::Cow::Borrowed(meta)),
            None => self
                .storage
                .get_meta(id)?
                .ok_or(Error::MissingDependency(*id))
                .map(std::borrow::Cow::Owned),
        }
    }

    fn op_projected(
        &self,
        id: &crate::OpId,
        overlay_ops: &BTreeMap<crate::OpId, Op>,
    ) -> Result<Op> {
        overlay_ops.get(id).cloned().map(Ok).unwrap_or_else(|| {
            self.storage
                .get_op(id)?
                .ok_or(Error::MissingDependency(*id))
        })
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
