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
| `fjall-schema4-e112523` | e112523afc69764e4bfbba61f687f4498acf0772 | schema 4 with unscoped bootstrap staging |
| `fjall-schema5-68e4c19` | 68e4c19ed1b30847a96dca319eb90eeed81619dc | schema 5 with staging namespaces and an interrupted activation |
| `fjall-schema6-512b158` | 512b158a423d8bdf2d74cb1a6feac85863b940a7 | schema 6 with metadata holding every observed clock entry |

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

`fjall-schema4-e112523` was made the same way with its own example. Its
`manifest.json` names an active topic (`active`, with `genesis`, `e1`, `e2`)
and a topic staged from `source` through the old unscoped staging API
(`staged`, `staged_genesis`, `staged_event`). It has no `dep.op`.

`fjall-schema5-68e4c19` was made by an uncommitted ignored test in a scratch
worktree of its commit, because only that commit's test helper
`FjallStorage::interrupt_activation` stops an activation after its copies, as a
crash there would. Its `manifest.json` names an active topic (`active`, with
`genesis`, `e1`, `e2`), a namespace staged from `staged_source` for `staged`
holding four ops and one buffered op (`staged_waiting`, `staged_bytes`), and a
namespace of `activating_source` for `activating` whose activation claimed the
topic and copied its `activating_ops` ops (`activating_session`,
`activating_bytes`, `activating_last`). The local signer is seed 1 (`reader`).
It has no `dep.op`.

`fjall-schema6-512b158` was made the same way as the schema 5 fixture, with
that commit's test helpers. Its `manifest.json` names an active topic
(`active`, `e1`, `e2`), a reverse dependency chain of 40 single-op writers
(`chain`, `chain_ops`, `chain_head`, whose observed clock has 40 entries), a
namespace of `staged_source` for `staged` with four admitted ops and one
buffered op (`staged_session`, `staged_last`, `staged_bytes`), a namespace for
`activating` whose activation claimed the topic and copied its
`activating_ops` ops (`activating_session`, `activating_last`), and a
namespace for `cleared` that was discarded while clearing its slot failed
before the first delete (`cleared_session`). The local signer is seed 1
(`reader`). It has no `dep.op`.
