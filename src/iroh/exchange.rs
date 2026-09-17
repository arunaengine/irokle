// SPDX-License-Identifier: MIT OR Apache-2.0
//! One bounded wire exchange: stream limits, frames charged before they are
//! allocated, responses that keep their charge, and writers that encode once.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use crate::net::frame::MAX_FRAME_LEN;
use crate::net::{decode_sync_message, encode_sync_message, framed_message_len};
use crate::sync::SyncMessage;

use super::budget::{ByteBudget, Charge, DATA_TAG, OwnedClass, Pool};
use super::{StreamLimits, invalid_data, other, timed_out};

/// Refuses a reply the stream writer would refuse, before any of it is queued.
pub(super) fn reply_fits(messages: &[SyncMessage], stream_limits: StreamLimits) -> io::Result<()> {
    let mut limits = SyncReadLimits::new(stream_limits);
    for message in messages {
        limits.observe_frame(framed_message_len(message)? - 4)?;
    }
    Ok(())
}

pub(super) struct SyncReadLimits {
    limits: StreamLimits,
    messages: usize,
    bytes: usize,
}

impl SyncReadLimits {
    pub(super) fn new(limits: StreamLimits) -> Self {
        Self {
            limits,
            messages: 0,
            bytes: 0,
        }
    }

    pub(super) fn observe_frame(&mut self, frame_len: usize) -> io::Result<usize> {
        if self.messages >= self.limits.messages {
            return Err(invalid_data("sync stream has too many messages"));
        }
        self.bytes = self
            .bytes
            .checked_add(frame_len + 4)
            .ok_or_else(|| invalid_data("sync stream byte count overflow"))?;
        if self.bytes > self.limits.bytes {
            return Err(invalid_data("sync stream exceeds maximum byte length"));
        }
        let frame_index = self.messages;
        self.messages += 1;
        Ok(frame_index)
    }
}

/// Messages of one exchange, charged to the net's byte budget. `messages()` and `iter()`
/// borrow them. Consuming iteration yields [`SyncResponse`] items; the batch stays charged
/// until the iterator and every item it yielded are dropped.
pub struct SyncResponses {
    pub(super) messages: Vec<SyncMessage>,
    pub(super) charges: Vec<Charge>,
}

impl SyncResponses {
    pub fn messages(&self) -> &[SyncMessage] {
        &self.messages
    }

    pub fn iter(&self) -> std::slice::Iter<'_, SyncMessage> {
        self.messages.iter()
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub(super) fn into_parts(self) -> (Vec<SyncMessage>, Vec<Charge>) {
        (self.messages, self.charges)
    }

    pub(super) fn into_session_charge(
        self,
        budget: &Arc<ByteBudget>,
    ) -> io::Result<(Vec<SyncMessage>, Arc<Vec<Charge>>)> {
        let bytes = self.charges.iter().fold(0_usize, |bytes, charge| {
            bytes.saturating_add(charge.bytes())
        });
        // Retained replies must not occupy the pool their next exchange needs.
        let held = budget.try_take(Pool::Session, bytes, OwnedClass::Session)?;
        let (messages, charges) = self.into_parts();
        let charge = Arc::new(vec![held]);
        drop(charges);
        Ok((messages, charge))
    }
}

impl IntoIterator for SyncResponses {
    type Item = SyncResponse;
    type IntoIter = SyncResponsesIter;

    fn into_iter(self) -> SyncResponsesIter {
        SyncResponsesIter {
            messages: self.messages.into_iter(),
            charges: Arc::new(self.charges),
        }
    }
}

/// Consuming iterator over [`SyncResponses`] that yields [`SyncResponse`] items.
/// The batch stays charged until the iterator and every item it yielded are dropped.
pub struct SyncResponsesIter {
    messages: std::vec::IntoIter<SyncMessage>,
    charges: Arc<Vec<Charge>>,
}

impl Iterator for SyncResponsesIter {
    type Item = SyncResponse;

    fn next(&mut self) -> Option<SyncResponse> {
        self.messages.next().map(|message| SyncResponse {
            message,
            _charges: Arc::clone(&self.charges),
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.messages.size_hint()
    }
}

/// One returned message that keeps its whole batch charged until it is dropped.
/// Borrow through `AsRef` or dereferencing; caller-created clones and their shared
/// buffers are not charged. There is no detaching conversion.
pub struct SyncResponse {
    message: SyncMessage,
    _charges: Arc<Vec<Charge>>,
}

impl AsRef<SyncMessage> for SyncResponse {
    fn as_ref(&self) -> &SyncMessage {
        &self.message
    }
}

impl std::ops::Deref for SyncResponse {
    type Target = SyncMessage;

    fn deref(&self) -> &SyncMessage {
        &self.message
    }
}

/// Reads responses while charging each frame before allocation. Only the first
/// frame may wait for capacity; later exhaustion fails while held bytes remain
/// owned.
pub(super) async fn read_responses(
    recv: &mut iroh::endpoint::RecvStream,
    sync_io_timeout: Duration,
    stream_limits: StreamLimits,
    budget: &Arc<ByteBudget>,
) -> io::Result<SyncResponses> {
    let mut held: Option<Charge> = None;
    let mut messages = Vec::new();
    let mut limits = SyncReadLimits::new(stream_limits);
    while let Some((len, tag)) = read_frame_head(recv, sync_io_timeout).await? {
        let frame_index = limits.observe_frame(len)?;
        let data = tag == DATA_TAG;
        let bytes = ByteBudget::frame_charge(len, data);
        let mut charge = match held {
            None => {
                budget
                    .wait(Pool::Results, bytes, OwnedClass::Results)
                    .await?
            }
            Some(_) => budget.try_take(Pool::Results, bytes, OwnedClass::Results)?,
        };
        let message = read_frame_body(recv, len, tag, sync_io_timeout, frame_index).await?;
        let retained = crate::net::decoded_message_bound(&message)?;
        if retained > charge.bytes() {
            return Err(invalid_data("decoded message exceeded its reservation"));
        }
        charge.shrink(retained);
        match &mut held {
            Some(held) => held.merge(charge),
            None => held = Some(charge),
        }
        messages.push(message);
    }
    Ok(SyncResponses {
        messages,
        charges: held.into_iter().collect(),
    })
}

/// Writes `messages` as frames. With a `budget`, each encoding buffer is
/// charged while it exists; a served reply with an output grant passes none.
pub(super) async fn write_sync_messages(
    send: &mut iroh::endpoint::SendStream,
    messages: &[SyncMessage],
    sync_io_timeout: Duration,
    stream_limits: StreamLimits,
    budget: Option<&Arc<ByteBudget>>,
) -> io::Result<()> {
    reply_fits(messages, stream_limits)?;
    for message in messages {
        let _encoding = match budget {
            Some(budget) => {
                let len = framed_message_len(message)?;
                let pool = ByteBudget::frame_pool(len, matches!(message, SyncMessage::Data(_)));
                Some(budget.wait(pool, len, OwnedClass::Output).await?)
            }
            None => None,
        };
        let payload = encode_sync_message(message)?;
        if payload.len() > MAX_FRAME_LEN {
            return Err(invalid_data("sync frame exceeds maximum length"));
        }
        let prefix = (payload.len() as u32).to_be_bytes();
        tokio::time::timeout(sync_io_timeout, async {
            send.write_all(&prefix).await?;
            send.write_all(&payload).await
        })
        .await
        .map_err(|_| timed_out("sync write timed out"))?
        .map_err(other)?;
    }
    send.finish().map_err(other)
}

/// Reads the length of the next frame and its first payload byte, the message
/// kind, so the frame can be charged before the rest is allocated.
pub(super) async fn read_frame_head(
    recv: &mut iroh::endpoint::RecvStream,
    sync_io_timeout: Duration,
) -> io::Result<Option<(usize, u8)>> {
    let mut head = [0_u8; 5];
    let Some(first_read) = read_some(recv, &mut head[..1], sync_io_timeout).await? else {
        return Ok(None);
    };
    if first_read == 0 {
        return Ok(None);
    }

    let mut read = first_read;
    while read < 4 {
        let Some(n) = read_some(recv, &mut head[read..4], sync_io_timeout).await? else {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete sync frame length",
            ));
        };
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete sync frame length",
            ));
        }
        read += n;
    }

    let len = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) as usize;
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "sync frame exceeds maximum length",
        ));
    }
    if len == 0 {
        return Err(invalid_data("empty sync message frame"));
    }
    tokio::time::timeout(sync_io_timeout, recv.read_exact(&mut head[4..]))
        .await
        .map_err(|_| timed_out("sync read timed out"))?
        .map_err(other)?;
    Ok(Some((len, head[4])))
}

/// Reads the rest of a frame whose head was read, and decodes it.
pub(super) async fn read_frame_body(
    recv: &mut iroh::endpoint::RecvStream,
    len: usize,
    tag: u8,
    sync_io_timeout: Duration,
    frame_index: usize,
) -> io::Result<SyncMessage> {
    let mut payload = vec![0_u8; len];
    payload[0] = tag;
    tokio::time::timeout(sync_io_timeout, recv.read_exact(&mut payload[1..]))
        .await
        .map_err(|_| timed_out("sync read timed out"))?
        .map_err(other)?;
    decode_sync_message(&payload).map_err(|err| {
        invalid_data(format!(
            "invalid sync message frame {frame_index} ({len} bytes): {err}"
        ))
    })
}

async fn read_some(
    recv: &mut iroh::endpoint::RecvStream,
    buf: &mut [u8],
    sync_io_timeout: Duration,
) -> io::Result<Option<usize>> {
    tokio::time::timeout(sync_io_timeout, recv.read(buf))
        .await
        .map_err(|_| timed_out("sync read timed out"))?
        .map_err(other)
}

#[cfg(test)]
#[path = "exchange_tests.rs"]
mod tests;
