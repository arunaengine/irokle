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
    decode_sync_message, encode_frame, encode_frames, encode_sync_message,
};
#[cfg(feature = "iroh")]
pub use iroh::{IrohNet, IrohRuntimeConfig, ShutdownOutcome};

#[cfg(any(feature = "iroh", test))]
pub(crate) fn sync_data_messages(topic_id: TopicId, ops: Vec<Op>) -> io::Result<Vec<SyncMessage>> {
    let mut messages = Vec::new();
    let mut data = SyncData {
        topic_id,
        ops: Vec::new(),
    };
    let overhead = framed_message_len(&SyncMessage::Data(data.clone()))? - 1;
    let mut data_len = 0;
    for op in ops {
        let op_len = postcard::experimental::serialized_size(&op).map_err(invalid_data)?;
        if overhead + 1 + op_len > frame::MAX_FRAME_LEN + 4 {
            return Err(invalid_data("operation exceeds sync frame size limit"));
        }
        let count_len =
            postcard::experimental::serialized_size(&(data.ops.len() + 1)).map_err(invalid_data)?;
        if data.ops.len() == MAX_SYNC_DATA_OPS_PER_MESSAGE
            || overhead + count_len + data_len + op_len > frame::MAX_FRAME_LEN + 4
        {
            messages.push(SyncMessage::Data(SyncData {
                topic_id,
                ops: std::mem::take(&mut data.ops),
            }));
            data_len = 0;
        }
        data_len += op_len;
        data.ops.push(op);
    }
    if !data.ops.is_empty() {
        messages.push(SyncMessage::Data(data));
    }
    Ok(messages)
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
    }
}

fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
