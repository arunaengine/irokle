# Development commands

Follow the canonical [STYLE.md](../STYLE.md). Run the checker and all script tests with:

```bash
python3 -B scripts/style.py
python3 -B -m unittest discover -s scripts -v
```

The checker validates identifier and filename term counts, wildcard imports, comment length, folder size, shared prefixes, unsupported file types and domain members. Its exact exceptions live in `style_rules.json`. Domain ownership, wording, reading order, meaningful term boundaries and external compatibility still need manual review.

## Reproducible checks

```text
checks.py REPOSITORY NEW_RESULT_DIRECTORY CASES_JSON
guard.py REPOSITORY NEW_LOG COMMAND [ARGUMENT ...]
matrix.sh REPOSITORY NEW_RESULT_DIRECTORY NEW_MANIFEST
```

Each JSON case contains `label`, `argv`, and a positive `timeout` in seconds. Optional `tests` names the exact Rust tests expected to execute; `required_logs` names nonempty repository-relative outputs; `allow_empty` lets a test command without exact `tests` execute no test, as the doc test run may. Arguments are executed directly, without an implicit shell. Source changes, incomplete runs, changed logs, nonzero exits and incorrect test cardinality fail the run. Existing result directories and manifests cannot be overwritten.

Heavy commands must run inside an enforced resource wrapper. `guard.py` puts the entire command tree in one Linux user scope with an 8 GiB memory cap, no swap, two build jobs, two test threads and reduced CPU/I/O priority. It refuses or stops work below 4 GiB available RAM or 8 GiB free filesystem space. Its JSON lines record resource samples and exit status. Check these limits against the actual host before running a campaign. Builds must use the selected checkout's `target/`.

For example, run the normal matrix under the guard, using fresh output paths outside the checkout (for example under a new directory in `/tmp` or a sibling path, so `checks.py`'s identity hashing of untracked, non-ignored files is not disturbed). The parent directory must already exist, since `guard.py` opens its log with mode "x":

```bash
mkdir -p /tmp/irokle-checks
python3 -B scripts/guard.py . /tmp/irokle-checks/resources.jsonl \
  bash scripts/matrix.sh . /tmp/irokle-checks/checks /tmp/irokle-checks/cases.json
```

## Peer compatibility

```text
compatibility.py CURRENT_REPO OLD_REPO CURRENT_BINARY OLD_BINARY FIXTURE_DIRECTORY NEW_RESULTS [CASE ...]
```

Run under `guard.py`. Both immutable test binaries must contain `tests::versions::peer_process`; each child log must show exactly that selected test. Generate the shared signed fixtures once using `tests::versions::write_fixtures` with `IROKLE_FIXTURES` naming a new directory. The driver verifies source and binary identities, complete process exits and signed ACK bytes in both initiator directions. A current encoding round trip does not establish historical compatibility.
