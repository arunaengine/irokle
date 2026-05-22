// SPDX-License-Identifier: MIT OR Apache-2.0
//! Signing, verification, and canonical serialization helpers for signed ops.

use crate::{
    error::{Error, Result},
    ids::PeerId,
};
use ed25519_dalek::{Signature, Signer as DalekSigner, SigningKey, Verifier, VerifyingKey};
use serde::Serialize;

pub trait Signer: Send + Sync {
    fn peer_id(&self) -> PeerId;
    fn sign(&self, message: &[u8]) -> Result<Signature>;
}

#[derive(Clone)]
pub struct Ed25519Signer {
    key: SigningKey,
}

impl Ed25519Signer {
    pub fn from_signing_key(key: SigningKey) -> Self {
        Self { key }
    }

    pub fn generate() -> Self {
        let mut bytes = [0; 32];
        getrandom::fill(&mut bytes).expect("operating system random number generator failed");

        Self {
            key: SigningKey::from_bytes(&bytes),
        }
    }

    pub fn from_bytes(bytes: &[u8; 32]) -> Self {
        Self {
            key: SigningKey::from_bytes(bytes),
        }
    }

    pub fn verifying_key(&self) -> VerifyingKey {
        self.key.verifying_key()
    }

    #[cfg(feature = "iroh")]
    pub fn from_iroh_secret_key(secret_key: &iroh::SecretKey) -> Self {
        Self::from_bytes(&secret_key.to_bytes())
    }
}

#[cfg(feature = "iroh")]
impl From<&iroh::SecretKey> for Ed25519Signer {
    fn from(secret_key: &iroh::SecretKey) -> Self {
        Self::from_iroh_secret_key(secret_key)
    }
}

impl Signer for Ed25519Signer {
    fn peer_id(&self) -> PeerId {
        PeerId::from(self.verifying_key().to_bytes())
    }

    fn sign(&self, message: &[u8]) -> Result<Signature> {
        Ok(self.key.sign(message))
    }
}

pub fn verify(peer: PeerId, message: &[u8], signature: &Signature) -> Result<()> {
    let key = VerifyingKey::from_bytes(peer.as_bytes()).map_err(|_| Error::InvalidPublicKey)?;
    key.verify(message, signature)
        .map_err(|_| Error::InvalidSignature)
}

pub fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    postcard::to_allocvec(value).map_err(Into::into)
}
