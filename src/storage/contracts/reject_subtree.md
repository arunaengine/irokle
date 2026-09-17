Atomically drop a permanently invalid pending `op_id` and every pending op
transitively waiting on it, or nothing at all once `op_id` is no longer
buffered, so a concurrent admission keeps its waiters. The count returned
includes `op_id`.

Required rather than defaulted: a composition of single removals is not atomic,
and a backend must not inherit that silently.
