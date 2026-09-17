Claim the topic's one activation for the session of `provisional` and freeze its
namespace at `expected`, in one transaction. The same session may claim again;
another session's claim or an active topic refuses. Returns the namespace
keyspace.
