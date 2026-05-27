// SPDX-License-Identifier: MIT OR Apache-2.0
//! Signed operation envelopes and operation-id validation.

use crate::{
    crypto::{Signer, canonical_bytes, verify},
    error::{Error, Result},
    ids::{ActorId, OpId, PeerId, TopicId},
    topic::TopicPayload,
};
use ed25519_dalek::Signature;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpBody {
    pub topic_id: TopicId,
    pub author: PeerId,
    pub actor_id: ActorId,
    pub actor_seq: u64,
    pub actor_prev: Option<OpId>,
    pub deps: BTreeSet<OpId>,
    pub generation: u64,
    pub payload: TopicPayload,
}

const OP_SIGNING_DOMAIN: &[u8] = b"irokle/op/v1\0";

fn op_signing_message(body: &OpBody) -> Result<Vec<u8>> {
    let body_bytes = canonical_bytes(body)?;
    let mut msg = Vec::with_capacity(OP_SIGNING_DOMAIN.len() + body_bytes.len());
    msg.extend_from_slice(OP_SIGNING_DOMAIN);
    msg.extend_from_slice(&body_bytes);
    Ok(msg)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedOp {
    pub body: OpBody,
    pub signature: Signature,
}

impl SignedOp {
    pub fn sign(body: OpBody, signer: &impl Signer) -> Result<Self> {
        if body.author != signer.peer_id() {
            return Err(Error::WrongSigner);
        }
        let signature = signer.sign(&op_signing_message(&body)?)?;
        Ok(Self { body, signature })
    }

    pub fn verify(&self) -> Result<()> {
        verify(
            self.body.author,
            &op_signing_message(&self.body)?,
            &self.signature,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Op {
    pub id: OpId,
    pub signed: SignedOp,
}

impl Op {
    pub fn new(signed: SignedOp) -> Result<Self> {
        let id = Self::derive_id(&signed)?;
        Ok(Self { id, signed })
    }

    pub fn sign(body: OpBody, signer: &impl Signer) -> Result<Self> {
        Self::new(SignedOp::sign(body, signer)?)
    }

    pub fn derive_id(signed: &SignedOp) -> Result<OpId> {
        Ok(OpId::hash(canonical_bytes(signed)?))
    }

    pub fn validate(&self) -> Result<()> {
        if self.id != Self::derive_id(&self.signed)? {
            return Err(Error::InvalidOpId);
        }
        self.signed.verify()
    }
}
