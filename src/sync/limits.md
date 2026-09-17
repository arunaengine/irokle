# Page admission and progress

A page captures a finite goal on one branch. Ordinary appends are later work.
Authorization and destructive epochs are checked again on each resumed snapshot.
Output credit counts operations and serialized bytes independently of planning work.

| Domain | Default limit and unit |
| --- | --- |
| Traversal | 65,536 combined record/header/index visits and actor or waiter work items per slice |
| Dependencies | 65,536 examined edges per slice |
| Traversal workspace | 8 MiB of conservative allocation charges per slice, 4 MiB retained per kept plan |
| Payload decoding | 32 MiB of admitted encoded upper-bound bytes per slice |
| Snapshot authorization | 16 metadata read admissions per request |
| Snapshot capture | 32 MiB of admitted raw metadata envelopes per request |
| Preparation | 1,048,576 conservative entry units and 128 MiB of scratch allocation charges |
| Request bookkeeping | At most 1,083,392 weighted entry units, sharing the scratch allowance |
| Captured clocks | 128 MiB per reservation and 256 MiB across shared reservations |
| Record cache | 64 MiB per cache and 256 MiB across live record reservations |
| Retained goals | 16 per engine, with a 60-second idle lifetime |

Input bookkeeping charges 16 units per hint and eight per explicit want, plus
space for bounded page offers and missing requirements. These are conservative
work units, not elapsed-time or physical-I/O measurements. Storage counters count
logical backend operations; backend caches and filesystem I/O are separate costs.

Generated repair windows reserve room for independent forward work. Explicit
requests that exceed an admission envelope return `Error::SyncCapacity`; callers
must reduce the request or supply a backend with the required bounded operations.
Larger output credit does not increase traversal or workspace allowances.

Fjall admits each authorization or clock record against a raw envelope just under
16 MiB before decoding. Its underlying read can allocate an oversized corrupt
value before returning its length. The envelope bounds admitted records and
subsequent decoding, not arbitrary backend allocations for unsupported records.

Progress assumes finite admitted goals, fair repeated service, sufficient output
credit for the next operation, available dependencies, successful delivery and
confirmation, and available execution and byte capacity. Callers must release returned owners when they no longer need
them. Sustained overload or permanently held capacity has no finite latency bound.

Continuations are server-owned and volatile. Resume them within their idle
lifetime on the same engine. Engine loss or expiry requires replanning; branch
replacement invalidates the old goal. Empty advancing plans are protected from
slot eviction; plans that have offered data may give up their slot. Unconfirmed
offers are replayed while their continuation remains available. A full protected registry reports capacity instead of empty success.

`more` requests another page; `continued` identifies a retained no-data slice.
`positions` requires requester position information, and `missing` reports
unavailable records separately from advancing work. `too_large` identifies a
local output allowance blocker. A false `more` flag alone does not certify that
unavailable records were received. High-level sync checks its complete goal.

Returned page output and planner caches have separate ownership. Iroh consuming
responses retain a shared whole-batch lease until the last original item drops.
Internal transfers acquire their destination reservation before refunding the
source. Cancellation does not release reservations held by a started storage job;
joined completion does. An uncertain commit requires reopening and reconciliation.
