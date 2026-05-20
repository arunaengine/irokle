// SPDX-License-Identifier: MIT OR Apache-2.0
//! Typed event encoding plus envelopes stored inside signed operations.

use bytes::Bytes;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::Result;

pub trait Event: Serialize + DeserializeOwned + Sized + Send + Sync + 'static {
    const TYPE_ID: &'static str;

    fn encode(&self) -> Result<Bytes> {
        Ok(Bytes::from(postcard::to_allocvec(self)?))
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        postcard::from_bytes(bytes).map_err(Into::into)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub type_id: String,
    pub payload: Bytes,
}

impl EventEnvelope {
    pub fn new<E: Event>(payload: Bytes) -> Self {
        Self {
            type_id: E::TYPE_ID.to_owned(),
            payload,
        }
    }

    pub fn encode_event<E: Event>(event: &E) -> Result<Self> {
        Ok(Self::new::<E>(event.encode()?))
    }

    pub fn decode_event<E: Event>(&self) -> Result<E> {
        if self.type_id != E::TYPE_ID {
            return Err(crate::Error::EventTypeMismatch {
                expected: E::TYPE_ID.to_owned(),
                actual: self.type_id.clone(),
            });
        }
        E::decode(&self.payload)
    }
}
