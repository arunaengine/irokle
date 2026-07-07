// SPDX-License-Identifier: MIT OR Apache-2.0
//! Public facade and re-exports for Irokle's signed topic operation log.

pub mod clock;
pub mod crypto;
pub mod error;
pub mod event;
pub mod history;
pub mod ids;
pub mod net;
pub mod node;
pub mod op;
/// Advanced operation-log internals. Prefer the `Irokle` facade for normal use.
pub mod oplog;
pub mod reducer;
pub mod storage;
/// Advanced sync planning internals and message types. Prefer `Irokle` sync facade methods.
pub mod sync;
pub mod topic;

extern crate self as irokle;

pub use clock::ActorClock;
pub use crypto::{Ed25519Signer, Signer, canonical_bytes, verify};
pub use error::{Error, Result};
pub use event::{Event, EventEnvelope};
pub use ids::{ActorId, IdParseError, OpId, PeerId, TopicId, actor_id_for};
pub use irokle_derive::Event;
pub use node::{Irokle, IrokleBuilder, NodeConfig, PublishOptions, RawTopic, Topic, WriteConcern};
pub use op::{Op, OpBody, SignedOp};
pub use oplog::{Admitted, EvictedOp, TopicEviction};
#[cfg(feature = "fjall")]
pub use storage::FjallStorage;
pub use storage::{MemoryStorage, Storage, SyncPeerState, SyncPeerStatus};
pub use topic::{
    ReplicationPolicy, TopicConfig, TopicControl, TopicGenesis, TopicInfo, TopicPayload,
};

#[cfg(test)]
mod tests;
