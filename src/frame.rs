// SPDX-License-Identifier: MIT OR Apache-2.0

use std::io;

use crate::sync::SyncMessage;

use crate::net::invalid_data;

pub(super) const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;
pub const MAX_SYNC_DATA_OPS_PER_MESSAGE: usize = 256;
pub const IROKLE_SYNC_ALPN: &[u8] = crate::sync::SYNC_PROTOCOL.as_bytes();

/// Conservative heap reservation for a message decoded from the wire.
/// Shared allocations are reserved, not measured; caller-created backing slices
/// and spare capacities outside the wire decoder are owned by the caller.
pub fn decoded_message_bound(message: &SyncMessage) -> io::Result<usize> {
    let bytes = postcard::experimental::serialized_size(message).map_err(invalid_data)?;
    let ops = match message {
        SyncMessage::Data(data) => data.ops.len(),
        _ => 0,
    };
    let clock = |clock: &crate::ActorClock| crate::ActorClock::allocation_bound(clock.len());
    let clocks = match message {
        SyncMessage::Summary(summary) => clock(&summary.actor_clock).saturating_add(
            summary
                .staged
                .as_ref()
                .map_or(0, |receipt| clock(&receipt.clock)),
        ),
        SyncMessage::Ack(ack) => clock(&ack.clock),
        SyncMessage::Receipt(receipt) => clock(&receipt.clock),
        _ => 0,
    };
    Ok(bytes
        .saturating_mul(3)
        .saturating_add(ops.saturating_mul(size_of::<crate::Op>()))
        .saturating_add(2 * size_of::<SyncMessage>())
        .saturating_add(clocks))
}

/// Reservation while raw and decoded copies coexist. `tag` is the first wire byte.
/// Includes temporary map/trie construction and cautious vector preallocation on
/// malformed lengths, not only allocations left after successful decoding.
pub fn frame_decode_bound(bytes: usize, tag: u8) -> usize {
    let data = tag == 4;
    let factor = if data { 3 } else { 18 };
    let ops = if data {
        MAX_SYNC_DATA_OPS_PER_MESSAGE
    } else {
        0
    };
    bytes
        .saturating_mul(factor + 1)
        .saturating_add(2 * 1024 * 1024)
        .saturating_add(ops.saturating_mul(size_of::<crate::Op>()))
        .saturating_add(2 * size_of::<SyncMessage>())
}

/// The largest [`frame_decode_bound`] of a legal frame, of any kind up to
/// [`MAX_FRAME_LEN`]. A pool that fits it can admit every frame a peer may send.
#[cfg(feature = "iroh")]
pub(super) fn largest_decode_bound() -> usize {
    (0..=u8::MAX)
        .map(|tag| frame_decode_bound(MAX_FRAME_LEN, tag))
        .fold(0, usize::max)
}

/// The largest [`decoded_message_bound`] of a message of `bytes` wire bytes:
/// every operation a data frame may carry, or a clock entry per 33 bytes.
#[cfg(feature = "iroh")]
pub(super) fn retained_bound(bytes: usize) -> usize {
    let entries = bytes / (crate::ActorId::LEN + 1);
    let clocks = crate::ActorClock::allocation_bound(entries);
    let ops = MAX_SYNC_DATA_OPS_PER_MESSAGE.saturating_mul(size_of::<crate::Op>());
    bytes
        .saturating_mul(3)
        .saturating_add(clocks.max(ops))
        .saturating_add(2 * size_of::<SyncMessage>())
}

pub fn encode_sync_message(message: &SyncMessage) -> io::Result<Vec<u8>> {
    postcard::to_allocvec(message).map_err(invalid_data)
}

pub fn decode_sync_message(bytes: &[u8]) -> io::Result<SyncMessage> {
    let (message, remainder) = postcard::take_from_bytes(bytes).map_err(invalid_data)?;
    if !remainder.is_empty() {
        return Err(invalid_data("trailing bytes in sync message"));
    }
    if let SyncMessage::Data(data) = &message
        && data.ops.len() > MAX_SYNC_DATA_OPS_PER_MESSAGE
    {
        return Err(invalid_data("sync data exceeds maximum operation count"));
    }
    Ok(message)
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
