#!/usr/bin/env python3
"""Print the required serial check manifest; run it through checks.sh under guard.py."""

import json
import sys


def main():
    print(json.dumps(cases(), indent=2))



def cargo_subcommand(args):
    """Return the cargo subcommand, skipping a leading toolchain override."""
    return next(arg for arg in args if not arg.startswith("+"))


def cases():
    commands = [
        ("fmt", ["fmt", "--check"]),
        ("clippy-default", ["clippy", "--locked", "--all-targets", "--", "-D", "warnings"]),
        ("test-default", ["test", "--locked"]),
        ("check-all", ["check", "--locked", "--all-features", "--all-targets"]),
        ("clippy-all", ["clippy", "--locked", "--all-features", "--all-targets", "--", "-D", "warnings"]),
        ("test-all", ["test", "--locked", "--all-features", "--lib", "--test", "derive"]),
        ("test-doc", ["test", "--locked", "--all-features", "--doc"]),
        ("doc", ["doc", "--locked", "--all-features", "--no-deps"]),
        ("patchbay", ["test", "--locked", "--features", "iroh", "--test", "iroh_patchbay_sync",
                      "--", "--nocapture", "--test-threads=1"]),
        ("fmt-nightly", ["+nightly", "fmt", "--all", "--", "--check"]),
        ("clippy-nightly", ["+nightly", "clippy", "--locked", "--all-features", "--all-targets",
                            "--", "-D", "warnings"]),
    ]
    for feature in ("fjall", "iroh"):
        commands.extend([
            (f"test-{feature}", ["test", "--locked", "--features", feature, "--lib"]),
            (f"clippy-{feature}", ["clippy", "--locked", "--features", feature, "--all-targets",
                                   "--", "-D", "warnings"]),
        ])
    commands.extend((f"{name}-1.95", ["+1.95.0", *args]) for name, args in commands[:9])
    commands.append(("contracts", ["test", "--locked", "--all-features", "--test", "contracts"]))
    # The crate keeps its examples as tests, so its doc test run may find none.
    result = [{"label": name,
               "argv": (["env", "RUSTDOCFLAGS=-D warnings"] if cargo_subcommand(args) == "doc" else [])
                       + ["cargo", *args], "timeout": 7200,
               **({"allow_empty": True} if "--doc" in args else {})}
              for name, args in commands]
    result.insert(0, {"label": "style", "timeout": 120,
                      "argv": [sys.executable, "-B", "scripts/style.py"]})
    for name in ("pending::with_fjall::drains_complete", "planning::catch_up_costs",
                 "planning::pull_hundred_thousand", "progress::window_chain_boundary",
                 "requests::real_join_finishes",
                 "staging::with_fjall::invite_caps", "staging::memory_invite_caps"):
        exact = f"tests::{name}"
        result.append({"label": f"ignored-{name.split('::')[-1]}", "timeout": 7200,
                       "argv": ["cargo", "test", "--locked", "--all-features", "--lib", exact,
                                "--", "--exact", "--ignored", "--test-threads=1"], "tests": [exact]})
    for name in ("scan_allocation_bounds", "memory_store_bounds", "captured_clock_bounds",
                 "selected_clock_bounds", "decoded_clock_bounds", "malformed_frame_bounds",
                 "shared_cache_bounds", "tree_allocation_bounds", "vector_allocation_bounds",
                 "record_allocation_bounds"):
        result.append({"label": f"allocator-{name}", "timeout": 7200,
                       "argv": ["cargo", "test", "--locked", "--all-features", "--test",
                                "allocation_bounds", name, "--", "--exact", "--ignored",
                                "--nocapture", "--test-threads=1"], "tests": [name]})
    result.append({"label": "runner-tests", "timeout": 120,
                   "argv": [sys.executable, "-B", "-m", "unittest", "discover", "-s", "scripts", "-v"]})
    return result


if __name__ == "__main__":
    main()
