<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->
Manually syncs `topic_ids` with one peer through the same batched page
exchange the resync loop uses, paging each topic while it advances up to
a page budget. Per topic: `Ok` when its goal completed, `WouldBlock`
when it advanced but the budget ran out and the rest is scheduled, and
the error of an exchange that failed or made no progress.
