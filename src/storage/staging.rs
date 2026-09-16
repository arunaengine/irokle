// SPDX-License-Identifier: MIT OR Apache-2.0
//! Staging limits, namespace records, and quota checks.

use serde::{Deserialize, Serialize};

use crate::{ActorClock, OpId, PeerId, Result, TopicId};

/// Limits of bootstrap staging: data for a topic this node does not hold yet,
/// kept per source and topic until its history proves membership.
pub const MAX_STAGED_BYTES_TOTAL: u64 = 64 * 1024 * 1024;
pub const MAX_STAGED_BYTES_PER_SESSION: u64 = 32 * 1024 * 1024;
pub const MAX_STAGED_SESSIONS: usize = 64;
pub const MAX_STAGED_SESSIONS_PER_SOURCE: usize = 8;
/// A session with no write for this long may be expired.
pub const MAX_STAGED_IDLE_MS: u64 = 10 * 60 * 1000;

/// Resources bootstrap staging may take before a history proves membership.
/// Each source and topic stages into its own namespace; bytes count every
/// serialized op a namespace holds, admitted or buffered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StagingLimits {
    pub total_bytes: u64,
    pub source_bytes: u64,
    pub namespace_bytes: u64,
    pub namespaces: usize,
    pub source_namespaces: usize,
}

impl StagingLimits {
    /// The envelope of a store that keeps staged history in memory.
    pub const MEMORY: Self = Self {
        total_bytes: MAX_STAGED_BYTES_TOTAL,
        source_bytes: MAX_STAGED_BYTES_PER_SESSION,
        namespace_bytes: MAX_STAGED_BYTES_PER_SESSION,
        namespaces: MAX_STAGED_SESSIONS,
        source_namespaces: MAX_STAGED_SESSIONS_PER_SOURCE,
    };

    /// The envelope of a store that keeps staged history on disk.
    pub const DISK: Self = Self {
        total_bytes: 16 * 1024 * 1024 * 1024,
        source_bytes: 4 * 1024 * 1024 * 1024,
        namespace_bytes: 4 * 1024 * 1024 * 1024,
        namespaces: MAX_STAGED_SESSIONS,
        source_namespaces: MAX_STAGED_SESSIONS_PER_SOURCE,
    };
}

/// Provisional history for a topic not yet held locally. The value scopes reads
/// to its session and writes until activation begins; data stays invisible until
/// membership is proven and activation completes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvisionalTopic {
    pub source: PeerId,
    pub topic_id: TopicId,
    /// The candidate branch this namespace holds.
    pub genesis: OpId,
    /// Durable identity of the namespace; a replacement gets a new one.
    pub session: u64,
    /// Last touch, for idle expiry. Wall-clock milliseconds, kept across a restart.
    pub updated_ms: u64,
    /// Activation began: the namespace is frozen at the state it validated.
    pub activating: bool,
    /// Grows with every write to the namespace, so a decision taken on an
    /// older observation of it is refused.
    pub revision: u64,
    /// Serialized op bytes the namespace holds, admitted and buffered.
    pub bytes: u64,
}

/// What one source staged for a topic this node does not hold. It is a
/// receipt, not an ack: it certifies nothing and clears no obligation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StagedTopic {
    /// The branch the namespace holds; `None` when nothing is staged.
    pub genesis: Option<OpId>,
    /// The namespace session this receipt belongs to.
    pub session: u64,
    /// Highest contiguous sequence staged per actor.
    pub clock: ActorClock,
    /// Serialized bytes the namespace holds, admitted and buffered.
    pub bytes: u64,
}

/// Refuse a new provisional namespace past the total or per-source count.
pub(crate) fn check_namespaces(
    limits: &StagingLimits,
    namespaces: usize,
    from_source: usize,
) -> Result<()> {
    if namespaces >= limits.namespaces {
        return Err(crate::Error::StagingCapacity(
            "bootstrap namespaces are full".into(),
        ));
    }
    if from_source >= limits.source_namespaces {
        return Err(crate::Error::StagingCapacity(
            "bootstrap namespace quota exceeded for source".into(),
        ));
    }
    Ok(())
}

/// Bytes one namespace may hold: its own limit and what the other namespaces
/// leave of the total and of its source's quota, read where the bytes commit.
#[derive(Clone, Copy, Debug)]
pub(crate) struct StagingQuota {
    namespace: u64,
    total: u64,
    source: u64,
}

impl StagingQuota {
    /// The quota of a namespace beside others holding `others` bytes in
    /// total, of which `source` belong to its own source.
    pub(crate) fn new(limits: &StagingLimits, others: u64, source: u64) -> Self {
        Self {
            namespace: limits.namespace_bytes,
            total: limits.total_bytes.saturating_sub(others),
            source: limits.source_bytes.saturating_sub(source),
        }
    }

    /// Refuse `charge` more bytes in a namespace holding `held`.
    pub(crate) fn check(&self, held: u64, charge: u64) -> Result<()> {
        if charge == 0 {
            return Ok(());
        }
        let refuse = |message: &str| Err(crate::Error::StagingCapacity(message.into()));
        let Some(after) = held.checked_add(charge) else {
            return refuse("bootstrap staging byte count overflow");
        };
        if after > self.namespace {
            return refuse("bootstrap namespace byte quota exceeded");
        }
        if after > self.total {
            return refuse("bootstrap staging byte budget is full");
        }
        if after > self.source {
            return refuse("bootstrap staging byte quota exceeded for source");
        }
        Ok(())
    }
}
