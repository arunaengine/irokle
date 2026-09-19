// SPDX-License-Identifier: MIT OR Apache-2.0
//! Error and result types shared across storage, admission, and sync APIs.

use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("postcard encoding failed: {0}")]
    Encode(#[from] postcard::Error),

    #[error("invalid signature")]
    InvalidSignature,

    #[error("missing signature")]
    MissingSignature,

    #[error("admission conflict")]
    AdmissionConflict,

    #[error("memory {domain:?} reservation needs {required} bytes, limit {limit}")]
    MemoryPressure {
        domain: crate::storage::MemoryDomain,
        required: u64,
        limit: u64,
    },

    #[error("invalid public key")]
    InvalidPublicKey,

    #[error("op id does not match signed op")]
    InvalidOpId,

    /// The signed generation does not equal one past the highest dependency.
    /// Both sides are fixed once every dependency is stored, so within one
    /// incarnation no later arrival can make such an op admissible.
    #[error("op generation {actual} does not match dependencies, expected {expected}")]
    GenerationMismatch { expected: u64, actual: u64 },

    #[error("signer does not match op author")]
    WrongSigner,

    #[error("event type mismatch: expected {expected}, got {actual}")]
    EventTypeMismatch { expected: String, actual: String },

    #[error("topic not found")]
    TopicNotFound,

    #[error("peer is not a member of topic")]
    NotTopicMember,

    #[error("peer {0} is not in the peer whitelist")]
    PeerNotWhitelisted(crate::ids::PeerId),

    #[error("invalid sync acknowledgement: {0}")]
    InvalidSyncAck(String),

    /// Evidence that names a topic incarnation other than the current one.
    /// Genesis replacement makes the discarded branch's proofs uncertifiable:
    /// the actor identities and sequence numbers repeat on the new branch.
    #[error("sync evidence belongs to a replaced topic incarnation")]
    StaleIncarnation,

    #[error("async replication requires a configured transport")]
    ReplicationUnavailable,

    #[cfg(feature = "iroh")]
    #[error("operation exceeds sync frame size limit")]
    OpTooLarge,

    #[error("admission committed but completion failed: {source}")]
    AdmissionCommitted {
        admitted: Box<crate::oplog::Admitted>,
        #[source]
        source: Box<Error>,
    },

    #[error("receive committed but completion failed: {source}")]
    ReceiveCommitted {
        ack: Box<crate::sync::SyncAck>,
        evictions: Vec<crate::oplog::TopicEviction>,
        #[source]
        source: Box<Error>,
    },

    #[error("actor sequence gap: expected {expected}, got {actual}")]
    ActorSeqGap { expected: u64, actual: u64 },

    #[error("actor previous op mismatch")]
    ActorPrevMismatch,

    #[error("actor fork detected")]
    ActorFork,

    #[error("actor id does not match op author")]
    ActorAuthorMismatch,

    #[error("op topic does not match sync topic")]
    TopicMismatch,

    #[error("missing dependency {0}")]
    MissingDependency(crate::ids::OpId),

    #[error("invalid genesis op")]
    InvalidGenesis,

    #[error("decode failed: {0}")]
    Decode(String),

    #[error("storage error: {0}")]
    Storage(String),

    /// One backend failure shared by every attempted item of a batch.
    #[error("{0}")]
    Shared(#[source] std::sync::Arc<Error>),

    #[cfg(feature = "fjall")]
    #[error("storage pressure: {0}")]
    StoragePressure(String),

    #[cfg(feature = "fjall")]
    #[error("storage buffer requires {required} bytes, limit {limit}")]
    StorageBuffer { required: u64, limit: u64 },

    #[cfg(feature = "fjall")]
    #[error("storage pressure probe failed: {0}")]
    StorageProbe(#[source] std::io::Error),

    #[error("sync planning capacity exhausted: {0}")]
    SyncCapacity(String),

    /// The op, or a dependency it waits on, was rejected on this branch.
    #[error("op {0} was rejected on this branch")]
    RejectedOp(crate::ids::OpId),

    /// Data for a topic this node does not hold was staged: its history does
    /// not yet make this node and the source members. Nothing was acknowledged.
    #[error("bootstrap data staged until its history proves membership")]
    BootstrapPending { staged: crate::ActorClock },

    /// Bootstrap staging reached a configured limit. Nothing of the refused
    /// data was kept; a later attempt may fit once staging drains or expires.
    #[error("bootstrap staging capacity exhausted: {0}")]
    StagingCapacity(String),

    #[error("eviction journal is full")]
    EvictionJournalFull,

    #[error("topic is sealed against reset")]
    TopicSealed,

    #[cfg(feature = "fjall")]
    #[error("fjall error: {0}")]
    Fjall(#[from] fjall::Error),

    /// A failed commit can appear after recovery; verify its outcome after reopening.
    #[cfg(feature = "fjall")]
    #[error("storage requires reopen and transaction outcome verification: {0}")]
    ReopenRequired(#[source] fjall::Error),
}

impl Error {
    /// Returns the error inside any `Shared` wrappers.
    pub fn cause(&self) -> &Self {
        let mut cause = self;
        while let Self::Shared(source) = cause {
            cause = source;
        }
        cause
    }
}
