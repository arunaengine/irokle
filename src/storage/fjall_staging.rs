// SPDX-License-Identifier: MIT OR Apache-2.0
//! Bootstrap staging in Fjall: ops of topics this node does not hold yet.

use crate::{ActorClock, ActorId, Error, Op, PeerId, Result, TopicId};

use super::fjall::FjallStorage;
use super::{
    StagedSession, StagedTopic, check_staged_op, check_staged_session, pending_op_bytes,
    staged_clock,
};

type Tx = fjall::OptimisticWriteTx;
type Records = fjall::OptimisticTxKeyspace;

/// Session usage, `bm<source><topic>`. No other key begins with `b`.
const SESSION: &[u8] = b"bm";
/// Staged op, `bo<source><topic><actor><seq><op>` with a big-endian sequence.
const STAGED_OP: &[u8] = b"bo";

fn session_key(source: &PeerId, topic_id: &TopicId) -> Vec<u8> {
    [SESSION, source.as_ref(), topic_id.as_ref()].concat()
}

fn ops_prefix(source: &PeerId, topic_id: &TopicId) -> Vec<u8> {
    [STAGED_OP, source.as_ref(), topic_id.as_ref()].concat()
}

fn op_key(prefix: &[u8], op: &Op) -> Vec<u8> {
    let body = &op.signed.body;
    [
        prefix,
        body.actor_id.as_ref(),
        &body.actor_seq.to_be_bytes(),
        op.id.as_ref(),
    ]
    .concat()
}

fn bytes_at<const N: usize>(key: &[u8], offset: usize) -> Result<[u8; N]> {
    key.get(offset..offset + N)
        .and_then(|bytes| <[u8; N]>::try_from(bytes).ok())
        .ok_or_else(|| Error::Storage("corrupt fjall staging key".into()))
}

/// Source and topic named by a session key.
fn session_of(key: &[u8]) -> Result<(PeerId, TopicId)> {
    Ok((
        PeerId::from_bytes(bytes_at(key, SESSION.len())?),
        TopicId::from_bytes(bytes_at(key, SESSION.len() + PeerId::LEN)?),
    ))
}

/// Actor and sequence named by a staged op key.
fn slot_of(key: &[u8]) -> Result<(ActorId, u64)> {
    let offset = STAGED_OP.len() + PeerId::LEN + TopicId::LEN;
    Ok((
        ActorId::from_bytes(bytes_at(key, offset)?),
        u64::from_be_bytes(bytes_at(key, offset + ActorId::LEN)?),
    ))
}

impl FjallStorage {
    fn tx_sessions(
        tx: &impl fjall::Readable,
        records: &Records,
    ) -> Result<Vec<((PeerId, TopicId), StagedSession)>> {
        let mut sessions = Vec::new();
        for item in fjall::Readable::prefix(tx, records, SESSION) {
            let (key, value) = item.into_inner()?;
            sessions.push((session_of(&key)?, postcard::from_bytes(value.as_ref())?));
        }
        Ok(sessions)
    }

    fn tx_staged_clock(
        tx: &impl fjall::Readable,
        records: &Records,
        prefix: &[u8],
    ) -> Result<ActorClock> {
        let mut slots = Vec::new();
        for item in fjall::Readable::prefix(tx, records, prefix) {
            slots.push(slot_of(&item.key()?)?);
        }
        Ok(staged_clock(slots))
    }

    pub(super) fn staged_charges(topic_id: &TopicId, ops: &[Op]) -> Result<Vec<u64>> {
        ops.iter()
            .map(|op| {
                if op.signed.body.topic_id != *topic_id {
                    return Err(Error::TopicMismatch);
                }
                Ok(pending_op_bytes(op)? as u64)
            })
            .collect()
    }

    /// Stage `ops` with their precomputed `charges`, all or nothing.
    pub(super) fn tx_stage_ops(
        tx: &mut Tx,
        records: &Records,
        (source, topic_id): (PeerId, TopicId),
        ops: &[Op],
        charges: &[u64],
        now_ms: u64,
    ) -> Result<StagedTopic> {
        if fjall::Readable::contains_key(tx, records, Self::key_id(b"ts", &topic_id))? {
            return Err(Error::AdmissionConflict);
        }
        let sessions = Self::tx_sessions(tx, records)?;
        let current = sessions
            .iter()
            .find(|(key, _)| *key == (source, topic_id))
            .map(|(_, session)| *session);
        if current.is_none() {
            if ops.is_empty() {
                return Ok(StagedTopic::default());
            }
            let source_sessions = sessions.iter().filter(|((peer, _), _)| *peer == source);
            check_staged_session(sessions.len(), source_sessions.count())?;
        }
        let mut total_bytes = sessions
            .iter()
            .map(|(_, session)| session.bytes)
            .sum::<u64>();
        let mut session = current.unwrap_or_default();
        let prefix = ops_prefix(&source, &topic_id);
        for (op, charge) in ops.iter().zip(charges) {
            let key = op_key(&prefix, op);
            // The transaction reads its own writes, so a repeat inside `ops` is free too.
            if fjall::Readable::contains_key(tx, records, key.as_slice())? {
                continue;
            }
            check_staged_op(total_bytes, &session, *charge)?;
            session.ops += 1;
            session.bytes += charge;
            total_bytes += charge;
            Self::tx_put(tx, records, key, op)?;
        }
        session.updated_ms = session.updated_ms.max(now_ms);
        Self::tx_put(tx, records, session_key(&source, &topic_id), &session)?;
        Ok(StagedTopic {
            clock: Self::tx_staged_clock(tx, records, &prefix)?,
            ops: session.ops,
            bytes: session.bytes,
        })
    }

    pub(super) fn read_staged_ops(&self, source: &PeerId, topic_id: &TopicId) -> Result<Vec<Op>> {
        let read_tx = self.db.read_tx();
        let mut ops = Vec::new();
        for item in fjall::Readable::prefix(&read_tx, &self.records, ops_prefix(source, topic_id)) {
            ops.push(postcard::from_bytes(item.value()?.as_ref())?);
        }
        Ok(ops)
    }

    pub(super) fn read_staged_topic(
        &self,
        source: &PeerId,
        topic_id: &TopicId,
    ) -> Result<StagedTopic> {
        let read_tx = self.db.read_tx();
        let Some(session) =
            fjall::Readable::get(&read_tx, &self.records, session_key(source, topic_id))?
        else {
            return Ok(StagedTopic::default());
        };
        let session: StagedSession = postcard::from_bytes(session.as_ref())?;
        Ok(StagedTopic {
            clock: Self::tx_staged_clock(&read_tx, &self.records, &ops_prefix(source, topic_id))?,
            ops: session.ops,
            bytes: session.bytes,
        })
    }

    pub(super) fn tx_discard_session(
        tx: &mut Tx,
        records: &Records,
        source: &PeerId,
        topic_id: &TopicId,
    ) -> Result<usize> {
        tx.remove(records, session_key(source, topic_id));
        Self::tx_remove_prefix(tx, records, &ops_prefix(source, topic_id))
    }

    /// Drop every session of `topic_id`, whatever its source.
    pub(super) fn tx_discard_topic(
        tx: &mut Tx,
        records: &Records,
        topic_id: &TopicId,
    ) -> Result<()> {
        for ((source, staged_topic), _) in Self::tx_sessions(tx, records)? {
            if staged_topic == *topic_id {
                Self::tx_discard_session(tx, records, &source, &staged_topic)?;
            }
        }
        Ok(())
    }

    pub(super) fn tx_expire_sessions(
        tx: &mut Tx,
        records: &Records,
        older_than_ms: u64,
    ) -> Result<usize> {
        let mut expired = 0;
        for ((source, topic_id), session) in Self::tx_sessions(tx, records)? {
            if session.updated_ms < older_than_ms {
                Self::tx_discard_session(tx, records, &source, &topic_id)?;
                expired += 1;
            }
        }
        Ok(expired)
    }
}
