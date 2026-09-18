# Page admission and progress

A page captures a finite goal on one branch. Ordinary appends are later work.
Authorization and destructive epochs are checked again on each resumed snapshot.
Output credit counts operations and serialized bytes independently of planning work.

| Domain | Default limit and unit |
| --- | --- |
| Traversal | 65,536 combined record/header/index visits and actor or waiter work items per slice |
| Dependencies | 65,536 examined edges per slice |
| Traversal workspace | 8 MiB of estimated allocation bytes per slice, and 4 MiB per kept plan |
| Payload decoding | 32 MiB per slice of encoded bytes admitted for decoding, counted at their upper bound |
| Snapshot authorization | 16 metadata read admissions per request |
| Snapshot capture | 32 MiB of admitted raw metadata envelopes per request |
| Preparation | 1,048,576 conservative entry units and 128 MiB of scratch allocation charges |
| Request bookkeeping | At most 1,083,392 weighted entry units, sharing the scratch allowance |
| Captured clocks | 128 MiB per reservation and 256 MiB across shared reservations |
| Record cache | 64 MiB per cache and 256 MiB across live record reservations |
| Retained goals | 16 per engine and 1 per Iroh session planner (`Continuations::fork`), with a 60-second idle lifetime |

Input bookkeeping charges 16 units per hint and eight per explicit want, plus
space for bounded page offers and missing requirements. These are conservative
work units, not elapsed-time or physical-I/O measurements. Storage counters count
logical backend operations; backend caches and filesystem I/O are separate costs.

Generated repair windows reserve room for independent forward work. Explicit
requests that exceed an admission envelope return `Error::SyncCapacity`; callers
must reduce the request or supply a backend with the required bounded operations.
Larger output credit does not increase traversal or workspace allowances.

A negotiation plans its push page and its request in one slice. Remote heads it
has no reads left to check stay unchecked, which hides no work: a head ahead of
the local clock is reached through actor ranges, and one at or behind it is held,
a hole the integrity scan finds, or a fork no page admits. Wants are ordered by the
generations the integrity scan read, and a want without a stored position is unknown.

A topic is certified only after an integrity scan found every stored record and
dependency edge resolvable. The scan lists the topic in id order and reads each
position, the presence of its record and each dependency edge, but decodes no
payload. One step reads at most 65,536 listed ids and edges in its own snapshot
and saves where it stopped, inside an operation's dependencies if needed, so the
next step resumes there. One step at a time may read a topic; its claim ends with
the step, including by error or panic. A planner inside a held snapshot takes at
most one step and plans an unfinished topic as not whole: it requests the holes
found so far, and its digest differs from the whole fingerprint. Facades without
a work budget finish the scan step by step, each step in its own snapshot, so
other storage users proceed between steps; an Iroh batch does so in the control
job that prepares its fingerprints.

A complete verdict is kept per branch and data epoch, holes included, so later
questions read nothing again. Admission never creates a hole, and appends made
during a scan do not restart it. When admission receives an operation named as a
hole, a new one or a copy already stored, and it is resolvable afterwards, it
leaves the verdict; a verdict without holes is whole. A reset changes the data
epoch and starts a new scan, `recheck_topics` drops every verdict and scan, and a
reopened store starts over. Damage written outside Irokle is found only by such a
new scan. A buffered operation's missing dependency is read fresh from each view.

Planning work that grows with a peer's summary is bounded by its frame. Iroh
decodes each message from one frame of at most 16 MiB, where a head takes 32
bytes, a clock entry at least 33 and a tip at least 65. A received summary thus
names at most 524,288 heads, 508,400 clock entries or 258,111 tips, and all of
them together fit those 16 MiB. Head checks stay within the slice. The tip loop,
the search for actors behind and request range building are linear in these
entries, and the filter of actors left out doubles in size at most 20 times up to
its 1 MiB limit. Page plans share the peer clock rather than copy it. A summary a
caller builds in process has no such bound.

Every Iroh message kind travels in one frame of at most 16 MiB (16,777,216
bytes) after a four-byte length prefix. The writer refuses a longer message as
`InvalidData` before sending it, and the reader refuses a longer prefix before
charging anything. Before a frame body is read, the frame is charged for its raw
copy and decoding: four bytes per wire byte plus 2 MiB and the most operations
for data, 19 bytes per wire byte plus 2 MiB for every other kind. A decoded
message that a session keeps is charged three bytes per wire byte plus its
operations or the allocation bound of its clock entries, at most one per 33 bytes.

| Pool | Configured | Raised to at least | Default capacity on 64-bit targets |
| --- | --- | --- | --- |
| Data | 256 MiB | The largest frame charge of any kind, and twice a largest page with its encoding buffer | 320,864,800 bytes |
| Results | 256 MiB | The largest frame charge of any kind | 320,864,800 bytes |
| Session | 128 MiB | Two largest kept messages: one read and a reply as large | 523,653,184 bytes with `fjall`, 458,577,984 without |
| Control | 16 MiB | Fixed; frames up to 64 KiB other than data, summaries and receipts | 16 MiB |

Capacities are semaphore permits, not allocated buffers. An idle endpoint admits
the largest legal frame of every kind in both directions. A stream waits only for
its first frame; later growth that finds its pool full fails with `OutOfMemory`,
which is temporary pressure rather than invalid input.

`handle_messages` serves messages an embedder already decoded. Before it handles
any of them, it checks the limits a served stream reads under: each message sized
as one frame of at most 16 MiB, four prefix bytes per message toward the stream
bytes, the stream's message count, and at most 256 operations per data message.
Refused input fails with `InvalidData` and changes nothing. The caller owns the
vector and any buffers its messages share; the library charges what it keeps and
replies. A served stream checks each frame as it arrives, so records admitted
before a refused frame stay committed and keep their obligations.

Fjall admits each authorization or clock record against a raw envelope just under
16 MiB before decoding. Its underlying read can allocate an oversized corrupt
value before returning its length. The envelope bounds admitted records and
subsequent decoding, not arbitrary backend allocations for unsupported records.

Progress assumes finite admitted goals, fair repeated service, sufficient output
credit for the next operation, available dependencies, successful delivery and
confirmation, and available execution and byte capacity. Callers must drop
returned values that hold reserved capacity once they no longer need them.
Sustained overload or permanently held capacity has no finite latency bound.

Continuations are server-owned and volatile. Resume them within their idle
lifetime on the same engine. Engine loss or expiry requires replanning; branch
replacement invalidates the old goal. Empty advancing plans are protected from
slot eviction; plans that have offered data may give up their slot. Unconfirmed
offers are replayed while their continuation remains available. When every plan
slot is protected, keeping another plan returns `Error::SyncCapacity` instead of
an empty page.

`more` asks for another page. `continued` marks a slice that sent no data but
kept its plan. `positions` lists actors the requester must name with their
positions in its next request, and `missing` reports unavailable records
separately from advancing work. `too_large` names an operation that alone
exceeds the page's byte budget. A false `more` flag alone does not certify that
unavailable records were received. High-level sync checks its complete goal.

Returned page output and planner caches have separate ownership. Items taken
from consumed Iroh responses share their batch's byte charge, which stays held
until the iterator and every item it yielded are dropped. Internal transfers
acquire their destination reservation before refunding the source. Cancellation
does not release reservations held by a started storage job; joined completion
does. An uncertain commit requires reopening and reconciliation.
