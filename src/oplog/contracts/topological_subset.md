Order the ops named by `ids` oldest-first.

An id whose records are absent, or that depends on an op with no metadata, is treated as a hole still awaiting repair: it and everything in `ids` reachable from it are left out instead of failing the whole traversal, so a sync exchange still makes progress for the ops it can resolve. Nothing is discarded - a deferred op stays admitted and reappears here once its dependency is refetched. Only a cycle among fully present ops is an error.
