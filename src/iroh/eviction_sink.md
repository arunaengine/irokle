<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->
Like [`Self::new_with_alpns_and_config`], but also wires an optional
eviction sink. When set, every [`TopicEviction`] produced while admitting
remote sync data (genesis tie-break resolution) is forwarded to the sink
so the embedder can re-emit the discarded payloads under the winning
genesis. The sink only makes recovery prompt: with or without it, the
payloads are journalled and drained through [`Irokle::pending_evictions`].
