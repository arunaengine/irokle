A provisional bootstrap: history one source served for a topic this store does not hold, kept in its own namespace and invisible to every topic query until it proves this node's membership and is activated.

The value is also the capability of the namespace: a store of it ([`crate::storage::Storage::provisional_store`]) reads and writes only while the backing store still registers `session`, and writes only until activation begins.
