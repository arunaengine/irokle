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

    #[error("invalid public key")]
    InvalidPublicKey,

    #[error("op id does not match signed op")]
    InvalidOpId,

    #[error("signer does not match op author")]
    WrongSigner,

    #[error("event type mismatch: expected {expected}, got {actual}")]
    EventTypeMismatch { expected: String, actual: String },

    #[error("topic not found")]
    TopicNotFound,

    #[error("peer is not a member of topic")]
    NotTopicMember,

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

    #[cfg(feature = "fjall")]
    #[error("fjall error: {0}")]
    Fjall(#[from] fjall::Error),
}
