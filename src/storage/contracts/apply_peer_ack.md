Backends must perform both writes in one durable operation so a crash between
them cannot leave the ack visible while obligations remain, or vice-versa.
Returns the number of cleared obligations.

The topic's current identity and membership must be read in that same operation
and the write conditioned on them, so evidence validated before a concurrent
reset or peer removal cannot still commit. Evidence naming another incarnation
is refused with [`crate::Error::StaleIncarnation`]; a removed peer's with
[`crate::Error::NotTopicMember`].

An uncertain backend commit may already have persisted these effects. Preserve
recovery work and reopen and reconcile when the typed error requires it.
