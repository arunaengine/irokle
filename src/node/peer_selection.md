<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->
Sync targets for `local_peer` under the topic's replication policy and the
caller's `health` view.

`max_sync_peers` is the whole fanout budget of one node, not a quota per
class: preferred, ring and fallback slots are all taken out of it, so a
budget of one means one target at a time, including while failing over.
`selected_peers` is a policy restriction and not a hint. An empty set allows
every current member; a non-empty set allows only its intersection with
current membership. Every alternate stays inside that allowed scope, so a
peer outside the policy or outside current membership is never selected, not
even when all allowed peers are failing.

`health` carries failed attempt counts and the rotation epoch. A peer is
passed over only once its count reaches the retry limit, so a short delay
does not move targets. The epoch is injected instead of read from a clock so
rotation is reproducible; callers advance it when a target has exhausted its
retry budget. When the policy permits only peers past that budget, no
selection can make progress: the result is reported as blocked and the
allowed scope is never widened to compensate.
