The first step claims the topic's one activation for this session, which refuses
a claim of another session with [`crate::Error::AdmissionConflict`], and freezes
the namespace at `expected`. Nothing of the history is visible to a read of the
store until one transaction installs the state, heads, clock and `effects` and
ends every namespace of the topic. An interrupted activation resumes when called
again.
