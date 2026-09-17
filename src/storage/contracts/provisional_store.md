It sees only that namespace. Each of its reads checks that the session is still
registered and each write, in the transaction that commits it, that the session
still stages; otherwise [`crate::Error::StaleIncarnation`] before any effect. A
write past the namespace, total or source byte limit is refused with
[`crate::Error::StagingCapacity`].
