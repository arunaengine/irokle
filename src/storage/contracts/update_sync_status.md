Backends must read, apply and write in one lock or transaction: two outcomes
recorded at once would otherwise each miss the other's counter increment and the
later writer's state would win.

Required rather than defaulted: a composition of a read and a blind write is not
atomic, and a backend must not inherit that silently.
