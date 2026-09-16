// SPDX-License-Identifier: MIT OR Apache-2.0
//! Iroh-backed sync framing, connection handling, and bounded resync loops.

#![allow(unexpected_cfgs)]

use std::io;

#[cfg(any(feature = "iroh", test))]
use crate::sync::SyncData;
use crate::sync::SyncMessage;
#[cfg(any(feature = "iroh", test))]
use crate::{Op, TopicId};

mod frame;
#[cfg(feature = "iroh")]
mod iroh;

pub use frame::{
    IROKLE_SYNC_ALPN, MAX_SYNC_DATA_OPS_PER_MESSAGE, decode_frame, decode_frames,
    decode_sync_message, decoded_message_bound, encode_frame, encode_frames, encode_sync_message,
    frame_decode_bound,
};
#[cfg(all(feature = "iroh", test))]
pub(crate) use iroh::StreamLimits;
#[cfg(feature = "iroh")]
pub use iroh::{
    IrohNet, IrohRuntimeConfig, OwnedBytes, OwnedClass, ShutdownOutcome, SyncResponse,
    SyncResponses, SyncResponsesIter,
};

#[cfg(test)]
pub(crate) fn sync_data_messages(topic_id: TopicId, ops: Vec<Op>) -> io::Result<Vec<SyncMessage>> {
    Ok(sync_data_page(topic_id, ops, usize::MAX, usize::MAX)?.messages)
}

#[cfg(any(feature = "iroh", test))]
/// Data messages for a causal prefix of some operations, within a wire budget.
#[cfg_attr(not(feature = "iroh"), allow(dead_code))]
pub(crate) struct DataPage {
    pub(crate) messages: Vec<SyncMessage>,
    /// Framed wire bytes of `messages`, prefixes, tags, topics and counts included.
    pub(crate) bytes: usize,
    /// Whether operations were left out to stay within the budget.
    pub(crate) cut: bool,
}

#[cfg(any(feature = "iroh", test))]
/// Frames the longest prefix of `ops` whose data messages fit `max_messages`
/// and `max_bytes` exactly as the stream writer encodes them. Each operation
/// is sized once; nothing is serialized twice to find the cut.
pub(crate) fn sync_data_page(
    topic_id: TopicId,
    ops: Vec<Op>,
    max_messages: usize,
    max_bytes: usize,
) -> io::Result<DataPage> {
    let count_len =
        |count: usize| postcard::experimental::serialized_size(&count).map_err(invalid_data);
    let empty = SyncMessage::Data(SyncData {
        topic_id,
        ops: Vec::new(),
    });
    let overhead = framed_message_len(&empty)? - count_len(0)?;
    let mut messages = Vec::new();
    let mut current = Vec::new();
    let mut current_len = 0;
    let mut closed_bytes = 0_usize;
    let mut cut = false;
    for op in ops {
        let op_len = postcard::experimental::serialized_size(&op).map_err(invalid_data)?;
        if overhead + count_len(1)? + op_len > frame::MAX_FRAME_LEN + 4 {
            return Err(invalid_data("operation exceeds sync frame size limit"));
        }
        let joined = overhead + count_len(current.len() + 1)? + current_len + op_len;
        let joins = !current.is_empty()
            && current.len() < MAX_SYNC_DATA_OPS_PER_MESSAGE
            && joined <= frame::MAX_FRAME_LEN + 4;
        let open_bytes = if current.is_empty() {
            0
        } else {
            overhead + count_len(current.len())? + current_len
        };
        let (count, bytes) = if joins {
            (messages.len() + 1, closed_bytes.saturating_add(joined))
        } else {
            (
                messages.len() + usize::from(!current.is_empty()) + 1,
                closed_bytes
                    .saturating_add(open_bytes)
                    .saturating_add(overhead + count_len(1)? + op_len),
            )
        };
        if count > max_messages || bytes > max_bytes {
            cut = true;
            break;
        }
        if !joins && !current.is_empty() {
            closed_bytes += open_bytes;
            current_len = 0;
            messages.push(SyncMessage::Data(SyncData {
                topic_id,
                ops: std::mem::take(&mut current),
            }));
        }
        current_len += op_len;
        current.push(op);
    }
    if !current.is_empty() {
        closed_bytes += overhead + count_len(current.len())? + current_len;
        messages.push(SyncMessage::Data(SyncData {
            topic_id,
            ops: current,
        }));
    }
    Ok(DataPage {
        messages,
        bytes: closed_bytes,
        cut,
    })
}

#[cfg(any(feature = "iroh", test))]
pub(crate) fn framed_message_len(message: &SyncMessage) -> io::Result<usize> {
    let len = postcard::experimental::serialized_size(message).map_err(invalid_data)?;
    if len > frame::MAX_FRAME_LEN {
        return Err(invalid_data("sync frame exceeds maximum length"));
    }
    Ok(4 + len)
}

#[cfg(feature = "iroh")]
pub(crate) fn validate_op(op: &Op) -> crate::Result<()> {
    let overhead = postcard::experimental::serialized_size(&SyncMessage::Data(SyncData {
        topic_id: op.signed.body.topic_id,
        ops: Vec::new(),
    }))?;
    if overhead + postcard::experimental::serialized_size(op)? > frame::MAX_FRAME_LEN {
        return Err(crate::Error::OpTooLarge);
    }
    Ok(())
}

pub fn _message_type_name(message: &SyncMessage) -> &'static str {
    match message {
        SyncMessage::Open(_) => "open",
        SyncMessage::Fingerprint(_) => "fingerprint",
        SyncMessage::Summary(_) => "summary",
        SyncMessage::Request(_) => "request",
        SyncMessage::Data(_) => "data",
        SyncMessage::Ack(_) => "ack",
        SyncMessage::Failure(_) => "failure",
        SyncMessage::Page(_) => "page",
        SyncMessage::Receipt(_) => "receipt",
    }
}

#[derive(Clone, Debug)]
struct SharedError(std::sync::Arc<dyn std::error::Error + Send + Sync>);

impl std::fmt::Display for SharedError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), formatter)
    }
}

impl std::error::Error for SharedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

impl SharedError {
    fn new(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self(std::sync::Arc::from(error.into()))
    }
}

fn invalid_data(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, SharedError::new(error))
}
