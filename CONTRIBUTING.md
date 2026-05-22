# CONTRIBUTING

Thank you for your interest in contributing to the project. Issues, bug reports, and feature requests can be made via GitHub issues. For detailed developer information please see the sections below.

In any case please also acknowledge our [Code of Conduct](CODE_OF_CONDUCT.md).


## Developer Contributions Guidance

Please make sure that all contributions compile and do not produce any errors. These commands match CI:

```bash
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --all-features --all-targets
cargo clippy --locked --all-features --all-targets -- -D warnings
cargo test --locked --all-features --lib --test derive
cargo test --locked --all-features --doc
cargo doc --locked --all-features --no-deps
```

### Workflow

Please make sure that you either create an issue or a PR draft first to give everyone an opportunity to discuss the best approach for your contribution.
