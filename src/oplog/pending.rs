// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use crate::storage::{OpMeta, TopicState};
use crate::{Error, Op, OpId, Result, TopicPayload, actor_id_for};

use crate::oplog::admission::checked_next;
use crate::oplog::{BatchOverlay, OpAdmission, Oplog};
use crate::storage::MAX_PENDING_MISSING_DEPS as MAX_MISSING_DEPS;

/// What happens to a buffered op whose admission failed.
pub(super) enum PendingVerdict {
    /// The failure is a property of immutable records: drop the op's subtree.
    Reject,
    /// A later arrival can still resolve it: keep the record.
    Retain,
}

impl<S: crate::oplog::Storage> Oplog<S> {
    /// Whether a buffered op that failed admission with `error` can never be
    /// admitted on this branch. Only immutable facts reject: its own signed
    /// content and the stored records of its dependencies and actor slots.
    pub(super) fn pending_verdict(&self, op: &Op, error: &Error) -> Result<PendingVerdict> {
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

    /// Whether the op's actor position contradicts stored records: a known
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

    /// The admitted op holding `seq` of the op's actor, if it is not the op.
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
        if missing_deps.len() > MAX_MISSING_DEPS {
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
                // Latest membership says nothing about the op's causal frontier,
                // which is unknown while a dependency is missing; the source's
                // pending quota bounds what an unproven author can buffer.
                if let Some(state) = state {
                    crate::oplog::admission::ensure_event_type(
                        &state.event_type_id,
                        &envelope.type_id,
                    )?;
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

/// Failures no later local state can turn into an admission: the signed op
/// itself is invalid. Discarding a pending op destroys signed work, so a
/// state dependent failure must never be classified here.
fn is_permanent_rejection(err: &Error) -> bool {
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::PendingVerdict;
    use crate::oplog::{BatchOverlay, OpAdmission, Oplog};
    use crate::storage::{MAX_PENDING_MISSING_DEPS, Storage, TopicState};
    use crate::{
        Ed25519Signer, Error, EventEnvelope, Op, OpBody, OpId, ReplicationPolicy, Result, Signer,
        TopicControl, TopicGenesis, TopicId, TopicPayload, actor_id_for,
    };

    /// A topic holding its genesis and bob's first event, plus carol without ops.
    struct Fixture {
        log: Oplog,
        topic: TopicId,
        genesis: Op,
        first: Op,
        bob: Ed25519Signer,
        carol: Ed25519Signer,
    }

    fn fixture(seed: u8) -> Fixture {
        let owner = Ed25519Signer::from_bytes(&[seed; 32]);
        let bob = Ed25519Signer::from_bytes(&[seed + 1; 32]);
        let carol = Ed25519Signer::from_bytes(&[seed + 2; 32]);
        let topic = TopicId::hash([seed]);
        let log = Oplog::new();
        let actor = actor_id_for(topic, owner.peer_id());
        let config = TopicGenesis::new("test.note", [bob.peer_id(), carol.peer_id()]);
        let genesis = log
            .create_topic_genesis(topic, actor, config, &owner)
            .unwrap();
        let first = sign(&bob, body(&bob, topic, 1, None, &[genesis.id]));
        log.receive_ops(vec![first.clone()]).unwrap();
        Fixture {
            log,
            topic,
            genesis,
            first,
            bob,
            carol,
        }
    }

    /// An event body of `signer` at `seq` behind `prev`, with the sequence as generation.
    fn body(
        signer: &Ed25519Signer,
        topic: TopicId,
        seq: u64,
        prev: Option<OpId>,
        deps: &[OpId],
    ) -> OpBody {
        OpBody {
            topic_id: topic,
            author: signer.peer_id(),
            actor_id: actor_id_for(topic, signer.peer_id()),
            actor_seq: seq,
            actor_prev: prev,
            deps: deps.iter().copied().collect(),
            generation: seq,
            payload: TopicPayload::Event(EventEnvelope {
                type_id: "test.note".into(),
                payload: vec![0].into(),
            }),
        }
    }

    fn sign(signer: &Ed25519Signer, body: OpBody) -> Op {
        Op::sign(body, signer).unwrap()
    }

    fn unknown(name: &str) -> OpId {
        OpId::hash(name.as_bytes())
    }

    fn rejects(log: &Oplog, op: &Op, error: Error) -> bool {
        matches!(
            log.pending_verdict(op, &error).unwrap(),
            PendingVerdict::Reject
        )
    }

    fn validate(
        log: &Oplog,
        op: &Op,
        missing: &[OpId],
        state: Option<&TopicState>,
    ) -> Result<OpAdmission> {
        let overlay = BatchOverlay {
            ops: &BTreeMap::new(),
            meta: &BTreeMap::new(),
            tips: &BTreeMap::new(),
            index: &BTreeMap::new(),
            reset: false,
        };
        let missing = missing.iter().copied().collect::<BTreeSet<_>>();
        log.validate_pending_op(op, &missing, &overlay, state)
    }

    /// A predecessor that is the stored previous slot of the same actor leaves
    /// the op waiting, and so does a first op without one.
    #[test]
    fn possible_positions_retained() {
        let f = fixture(10);
        let second = sign(
            &f.bob,
            body(&f.bob, f.topic, 2, Some(f.first.id), &[f.first.id]),
        );
        assert!(!rejects(&f.log, &second, Error::ActorPrevMismatch));
        let opening = sign(&f.carol, body(&f.carol, f.topic, 1, None, &[f.genesis.id]));
        assert!(!rejects(
            &f.log,
            &opening,
            Error::ActorSeqGap {
                expected: 1,
                actual: 1,
            }
        ));
    }

    /// Positions that no later arrival can repair reject the buffered op.
    #[test]
    fn impossible_positions_reject() {
        let f = fixture(20);
        let missing = unknown("missing");
        let cases = [
            (
                "later op without predecessor",
                body(&f.carol, f.topic, 2, None, &[missing]),
            ),
            (
                "first op with predecessor",
                body(
                    &f.carol,
                    f.topic,
                    1,
                    Some(missing),
                    &[f.genesis.id, missing],
                ),
            ),
            (
                "predecessor outside deps",
                body(&f.carol, f.topic, 2, Some(missing), &[f.genesis.id]),
            ),
            (
                "previous slot held by another op",
                body(&f.bob, f.topic, 2, Some(missing), &[missing]),
            ),
        ];
        for (case, body) in cases {
            let signer = if body.author == f.bob.peer_id() {
                &f.bob
            } else {
                &f.carol
            };
            let op = sign(signer, body);
            assert!(rejects(&f.log, &op, Error::ActorPrevMismatch), "{case}");
        }
    }

    /// A fork rejects only while another admitted op holds the slot.
    #[test]
    fn fork_needs_holder() {
        let f = fixture(30);
        let mut forked = body(&f.bob, f.topic, 1, None, &[f.genesis.id]);
        forked.generation += 1;
        let forked = sign(&f.bob, forked);
        assert!(rejects(&f.log, &forked, Error::ActorFork));
        assert!(!rejects(&f.log, &f.first, Error::ActorFork));
    }

    /// Failures raised against a fully known causal frontier reject the op.
    #[test]
    fn frontier_errors_reject() {
        let f = fixture(40);
        let errors = [
            Error::NotTopicMember,
            Error::EventTypeMismatch {
                expected: "test.note".into(),
                actual: "test.other".into(),
            },
            Error::InvalidGenesis,
            Error::InvalidOpId,
        ];
        for error in errors {
            let name = format!("{error:?}");
            assert!(rejects(&f.log, &f.first, error), "{name}");
        }
    }

    /// Deps in the batch count as present, and a reset treats stored deps as missing.
    #[test]
    fn missing_skips_overlay() {
        let f = fixture(50);
        let second = sign(
            &f.bob,
            body(&f.bob, f.topic, 2, Some(f.first.id), &[f.first.id]),
        );
        let missing = unknown("missing");
        let deps = [f.first.id, second.id, missing];
        let third = sign(&f.bob, body(&f.bob, f.topic, 3, Some(second.id), &deps));
        let overlay = BTreeMap::from([(second.id, second)]);
        let projected = |reset| {
            f.log
                .missing_deps_projected(&third, &overlay, reset)
                .unwrap()
        };
        assert_eq!(projected(false), [missing].into());
        assert_eq!(projected(true), [f.first.id, missing].into());
    }

    /// The missing dependency limit is inclusive.
    #[test]
    fn missing_limit_inclusive() {
        let f = fixture(60);
        let state = f.log.storage().topic_state(&f.topic).unwrap();
        let admits = |count: usize| {
            let missing = (0..count)
                .map(|index| unknown(&index.to_string()))
                .collect::<Vec<_>>();
            let mut deps = missing.clone();
            deps.push(f.first.id);
            let op = sign(&f.bob, body(&f.bob, f.topic, 2, Some(f.first.id), &deps));
            validate(&f.log, &op, &missing, state.as_ref())
        };
        assert!(matches!(
            admits(MAX_PENDING_MISSING_DEPS),
            Ok(OpAdmission::Admit)
        ));
        assert!(matches!(
            admits(MAX_PENDING_MISSING_DEPS + 1),
            Err(Error::Storage(_))
        ));
    }

    /// A buffered op whose slot another op holds is a fork.
    #[test]
    fn pending_fork_rejected() {
        let f = fixture(70);
        let state = f.log.storage().topic_state(&f.topic).unwrap();
        let missing = unknown("missing");
        let mut forked = body(&f.bob, f.topic, 1, None, &[f.genesis.id, missing]);
        forked.generation += 1;
        let forked = sign(&f.bob, forked);
        let verdict = validate(&f.log, &forked, &[missing], state.as_ref());
        assert!(
            matches!(verdict, Err(Error::ActorFork)),
            "{:?}",
            verdict.err()
        );
    }

    /// Only a first op without predecessor or deps opens a topic without state.
    #[test]
    fn genesis_shape_checked() {
        let f = fixture(80);
        let state = f.log.storage().topic_state(&f.topic).unwrap();
        let topic = TopicId::hash(b"fresh");
        let missing = unknown("missing");
        let opening = || OpBody {
            deps: BTreeSet::new(),
            generation: 0,
            payload: TopicPayload::Genesis(TopicGenesis::new("test.note", [])),
            ..body(&f.bob, topic, 1, None, &[])
        };
        let valid = sign(&f.bob, opening());
        assert!(matches!(
            validate(&f.log, &valid, &[], None),
            Ok(OpAdmission::Admit)
        ));
        let later = OpBody {
            actor_seq: 2,
            ..opening()
        };
        let behind = OpBody {
            actor_prev: Some(missing),
            ..opening()
        };
        let dependent = OpBody {
            deps: [missing].into(),
            ..opening()
        };
        let cases = [
            ("later sequence", later, None),
            ("predecessor", behind, None),
            ("dependency", dependent, None),
            ("existing state", opening(), state.as_ref()),
        ];
        for (case, body, state) in cases {
            let op = sign(&f.bob, body);
            let verdict = validate(&f.log, &op, &[missing], state);
            assert!(
                matches!(verdict, Err(Error::InvalidGenesis)),
                "{case}: {:?}",
                verdict.err()
            );
        }
    }

    /// Events and controls need a generation above zero even with deps.
    #[test]
    fn generation_zero_rejected() {
        let f = fixture(90);
        let state = f.log.storage().topic_state(&f.topic).unwrap();
        let missing = unknown("missing");
        let event = body(&f.bob, f.topic, 2, Some(f.first.id), &[f.first.id, missing]);
        let control = TopicPayload::Control(TopicControl::SetReplicationPolicy {
            policy: ReplicationPolicy::all(),
        });
        let payloads = [event.payload.clone(), control];
        for payload in payloads {
            let op = sign(
                &f.bob,
                OpBody {
                    generation: 0,
                    payload,
                    ..event.clone()
                },
            );
            let verdict = validate(&f.log, &op, &[missing], state.as_ref());
            assert!(
                matches!(verdict, Err(Error::InvalidOpId)),
                "{:?}",
                verdict.err()
            );
        }
    }

    /// The predecessor must match the sequence and, once known, the same actor.
    #[test]
    fn predecessor_shape_checked() {
        let f = fixture(100);
        let state = f.log.storage().topic_state(&f.topic).unwrap();
        let missing = unknown("missing");
        let cases = [
            (
                "first op with predecessor",
                &f.carol,
                body(
                    &f.carol,
                    f.topic,
                    1,
                    Some(missing),
                    &[f.genesis.id, missing],
                ),
            ),
            (
                "later op without predecessor",
                &f.carol,
                body(&f.carol, f.topic, 2, None, &[missing]),
            ),
            (
                "predecessor of another actor",
                &f.bob,
                body(
                    &f.bob,
                    f.topic,
                    2,
                    Some(f.genesis.id),
                    &[f.genesis.id, missing],
                ),
            ),
        ];
        for (case, signer, body) in cases {
            let op = sign(signer, body);
            let verdict = validate(&f.log, &op, &[missing], state.as_ref());
            assert!(
                matches!(verdict, Err(Error::ActorPrevMismatch)),
                "{case}: {:?}",
                verdict.err()
            );
        }
    }
}
