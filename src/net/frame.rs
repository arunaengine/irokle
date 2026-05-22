// SPDX-License-Identifier: MIT OR Apache-2.0

use std::io;

use crate::sync::SyncMessage;

use super::invalid_data;

pub(super) const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;
pub const MAX_SYNC_DATA_OPS_PER_MESSAGE: usize = 256;
pub const IROKLE_SYNC_ALPN: &[u8] = b"irokle/sync/1";

pub fn encode_sync_message(message: &SyncMessage) -> io::Result<Vec<u8>> {
    postcard::to_allocvec(message).map_err(invalid_data)
}

pub fn decode_sync_message(bytes: &[u8]) -> io::Result<SyncMessage> {
    postcard::from_bytes(bytes).map_err(invalid_data)
}

pub fn encode_frame(payload: &[u8]) -> io::Result<Vec<u8>> {
    if payload.len() > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "sync frame exceeds maximum length",
        ));
    }
    let len = payload.len() as u32;
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

pub fn decode_frame(input: &[u8]) -> io::Result<Option<(Vec<u8>, usize)>> {
    if input.len() < 4 {
        return Ok(None);
    }

    let len = u32::from_be_bytes([input[0], input[1], input[2], input[3]]) as usize;
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "sync frame exceeds maximum length",
        ));
    }

    let end = 4 + len;
    if input.len() < end {
        return Ok(None);
    }

    Ok(Some((input[4..end].to_vec(), end)))
}

pub fn encode_frames<'a>(payloads: impl IntoIterator<Item = &'a [u8]>) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    for payload in payloads {
        out.extend_from_slice(&encode_frame(payload)?);
    }
    Ok(out)
}

pub fn decode_frames(mut input: &[u8]) -> io::Result<Vec<Vec<u8>>> {
    let mut frames = Vec::new();
    while !input.is_empty() {
        let Some((frame, consumed)) = decode_frame(input)? else {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete sync frame",
            ));
        };
        frames.push(frame);
        input = &input[consumed..];
    }
    Ok(frames)
}
