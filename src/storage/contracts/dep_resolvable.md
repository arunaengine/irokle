The DAG is traversed through metadata and served from op records, so either half
alone is a hole to refill, never a resolved edge. This is the one predicate
every caller must use; backends override it to read both keys from a single
snapshot.
