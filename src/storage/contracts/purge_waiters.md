Genesis tie-break resolution uses this for a genesis that will never be admitted
here; a partial walk would strand waiters holding pending quota against a
dependency that can never arrive. Returns the number removed.

Required rather than defaulted: a composition of single removals is correct but
not atomic, and a backend must not inherit that silently.
