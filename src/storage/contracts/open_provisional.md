The namespace of `source` for `topic_id`, opened empty for `genesis` when the
source has none. An existing namespace is returned unchanged, whatever genesis
it holds. Refuses an active topic with [`crate::Error::AdmissionConflict`] and a
namespace past the count limits with [`crate::Error::StagingCapacity`].
