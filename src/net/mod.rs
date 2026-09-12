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
pub use iroh::{IrohNet, IrohRuntimeConfig};

#[cfg(any(feature = "iroh", test))]
pub(crate) fn framed_message_len(message: &SyncMessage) -> io::Result<usize> {
    let payload_len = postcard::experimental::serialized_size(message).map_err(invalid_data)?;
    if payload_len > frame::MAX_FRAME_LEN {
        return Err(invalid_data("sync frame exceeds maximum length"));
    }
    payload_len
        .checked_add(4)
        .ok_or_else(|| invalid_data("sync frame length overflow"))
}

#[cfg(any(feature = "iroh", test))]
pub(crate) fn sync_data_messages(topic_id: TopicId, ops: Vec<Op>) -> io::Result<Vec<SyncMessage>> {
    use postcard::experimental::serialized_size;

    let mut data = SyncData {
        topic_id,
        ops: Vec::new(),
    };
    let header_size = serialized_size(&SyncMessage::Data(data.clone())).map_err(invalid_data)?
        - serialized_size(&0_usize).map_err(invalid_data)?;
    let mut batch_size = header_size;
    let mut messages = Vec::new();
    for op in ops {
        let op_size = serialized_size(&op).map_err(invalid_data)?;
        let count_size = serialized_size(&(data.ops.len() + 1)).map_err(invalid_data)?;
        if !data.ops.is_empty()
            && (data.ops.len() == MAX_SYNC_DATA_OPS_PER_MESSAGE
                || batch_size + op_size + count_size > frame::MAX_FRAME_LEN)
        {
            messages.push(SyncMessage::Data(SyncData {
                topic_id,
                ops: std::mem::take(&mut data.ops),
            }));
            batch_size = header_size;
        }
        let count_size = serialized_size(&(data.ops.len() + 1)).map_err(invalid_data)?;
        if batch_size + op_size + count_size > frame::MAX_FRAME_LEN {
            return Err(invalid_data("sync operation exceeds maximum frame length"));
        }
        batch_size += op_size;
        data.ops.push(op);
    }
    if !data.ops.is_empty() {
        messages.push(SyncMessage::Data(data));
    }
    Ok(messages)
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
    }
}

fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
