// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::{Op, TopicPayload};

pub(crate) fn is_structural_genesis(op: &Op) -> bool {
    let body = &op.signed.body;
    matches!(body.payload, TopicPayload::Genesis(_))
        && body.actor_seq == 1
        && body.actor_prev.is_none()
        && body.deps.is_empty()
}
