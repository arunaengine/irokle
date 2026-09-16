Upgrade a schema 1 database. Two things change:

Acknowledgements move into the current layout, which names the incarnation each one certifies. Their branch was never recorded, so they migrate as uncertified: the records and their clocks are preserved, but they prove nothing until the peer acknowledges the current branch.

Buffered pending payloads gain byte counters, seeded by measuring the records already stored so the budgets describe the whole pool rather than only what arrives afterwards.

One transaction carries every rewrite and the version bump. After interruption, reopening observes either schema 1 and retries, or the atomically completed schema 2. A commit error alone does not establish which outcome persisted.
