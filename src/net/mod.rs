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
pub(crate) fn sync_data_messages(topic_id: TopicId, ops: Vec<Op>) -> Vec<SyncMessage> {
    ops.chunks(MAX_SYNC_DATA_OPS_PER_MESSAGE)
        .map(|ops| {
            SyncMessage::Data(SyncData {
                topic_id,
                ops: ops.to_vec(),
            })
        })
        .collect()
}

pub fn _message_type_name(message: &SyncMessage) -> &'static str {
    match message {
        SyncMessage::Open(_) => "open",
        SyncMessage::Fingerprint(_) => "fingerprint",
        SyncMessage::Summary(_) => "summary",
        SyncMessage::Request(_) => "request",
        SyncMessage::Data(_) => "data",
        SyncMessage::Ack(_) => "ack",
    }
}

fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
