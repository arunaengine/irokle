# CONTRIBUTING

Thank you for your interest in contributing to the project. Issues, bug reports, and feature requests can be made via GitHub issues. For detailed developer information please see the sections below.

In any case please also acknowledge our [Code of Conduct](CODE_OF_CONDUCT.md).


## Developer Contributions Guidance

Follow the canonical [repository style and structure policy](STYLE.md) for all owned source, tests, assets, and tooling.

Please make sure that all contributions compile and do not produce any errors. These commands match CI:

```bash
python3 -B -m unittest discover -s scripts -v
python3 -B scripts/style.py
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --all-features --all-targets
cargo clippy --locked --all-features --all-targets -- -D warnings
cargo test --locked --all-features --lib --test derive
cargo test --locked --all-features --doc
RUSTDOCFLAGS='-D warnings' cargo doc --locked --all-features --no-deps
```

CI also runs the Iroh network tests in Linux network namespaces. They need `iproute2`, `nftables` and permission to create namespaces:

```bash
cargo test --locked --features iroh --test iroh_patchbay_sync -- --nocapture --test-threads=1
```

Changes to the wire format must change `irokle::sync::SYNC_PROTOCOL`, which is also the Iroh ALPN. Changes to the Fjall layout must raise the schema version and add a migration that rechecks the version inside its transaction.

### Workflow

Please make sure that you either create an issue or a PR draft first to give everyone an opportunity to discuss the best approach for your contribution.
