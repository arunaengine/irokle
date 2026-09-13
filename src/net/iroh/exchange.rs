// SPDX-License-Identifier: MIT OR Apache-2.0
//! One bounded wire exchange: stream limits, frames charged before they are
//! allocated, responses that keep their charge, and writers that encode once.

use std::io;

use crate::sync::SyncMessage;

use super::budget::Charge;
use super::{StreamLimits, invalid_data};

/// Refuses a reply the stream writer would refuse, before any of it is queued.
pub(super) fn reply_fits(messages: &[SyncMessage], stream_limits: StreamLimits) -> io::Result<()> {
    let mut limits = SyncReadLimits::new(stream_limits);
    for message in messages {
        limits.observe_frame(crate::net::framed_message_len(message)? - 4)?;
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

/// Messages of one exchange or embedded stream, charged to the net's byte
/// budget until this value, or the iterator it turns into, is dropped.
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
}

impl IntoIterator for SyncResponses {
    type Item = SyncMessage;
    type IntoIter = SyncResponsesIter;

    fn into_iter(self) -> SyncResponsesIter {
        SyncResponsesIter {
            messages: self.messages.into_iter(),
            _charges: self.charges,
        }
    }
}

/// Owned messages of one exchange or embedded stream. The charge of all of
/// them is released when the iterator is dropped.
pub struct SyncResponsesIter {
    messages: std::vec::IntoIter<SyncMessage>,
    _charges: Vec<Charge>,
}

impl Iterator for SyncResponsesIter {
    type Item = SyncMessage;

    fn next(&mut self) -> Option<SyncMessage> {
        self.messages.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.messages.size_hint()
    }
}
