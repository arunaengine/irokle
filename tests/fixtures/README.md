# Fjall upgrade fixtures

Each directory holds a small Fjall database written by an old commit of
Irokle. The tests in `src/tests/upgrade.rs` copy a directory into a temporary
directory and open the copy with the current code. Never open these files in
place, because opening a database changes it.

| Directory | Commit | Layout |
| --- | --- | --- |
| `fjall-schema1-24417de` | 24417de220b2c9964f9622eec97834a9b68c5bf7 | schema 1 |
| `fjall-schema2-fb5ea2b` | fb5ea2b | schema 2 without pending byte counters |
| `fjall-schema2-54db4f9` | 54db4f95e8f7a654da4a75eafc1eebdbcebdb4c9 | schema 2 with pending byte counters |

## Contents

Every directory has:

- `db/`: the Fjall database.
- `manifest.json`: the topic, op and peer ids the tests need.
- `dep.op`: a postcard encoded op that the database never received.

The database was written only through the public API of that commit
(`Oplog`, `FjallStorage` and the `Storage` trait), with fixed signing keys
(`Ed25519Signer::from_bytes` with seeds 1, 2 and 3). It contains:

- Topic A with a genesis, events from two actors and an add-peer control.
- A stored ack from peer 2. At schema 2 the ack names the topic genesis.
- Five sync obligations in the old shape: ids with a clock, ids only, and
  one with neither.
- Two buffered ops from two different source peers. Both wait on the op in
  `dep.op`.
- A sync status record for peer 2.
- Topic B, where a smaller competing genesis from a member replaced the
  local branch. This left one record in the eviction journal.

## How they were made

For each commit, a detached scratch worktree was created and an uncommitted
example was added. It ran with `cargo run --example make_fixture --features
fjall`. After writing, the example flushed the records keyspace into a table
so no keyspace directory stays empty (git does not keep empty directories).
It then opened the database once more with the same commit, so Fjall trims
its preallocated 64 MiB journal down to the written content.

The ids are the same for all three commits, because the signing keys, the
signed content and the signing domain did not change between them.
