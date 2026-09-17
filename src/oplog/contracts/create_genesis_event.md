Create a topic genesis op plus its first event op and admit both in a single
storage transaction. The event op chains off the genesis (actor_seq 2,
actor_prev/deps = genesis op). Returns `(genesis, event)`. Fails with
[`crate::Error::InvalidGenesis`] if the topic already exists, same as
[`Self::create_topic_genesis`].
