#!/usr/bin/env python3
"""Print the required serial check manifest; run it through checks.sh.

The launcher must enforce memory limits for the entire runner and its children.
"""

import json
import sys


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
    result = [{"label": name, "argv": ["cargo", *args], "timeout": 7200}
              for name, args in commands]
    for name in ("pending::fjall_drains_complete", "planning::catch_up_costs",
                 "planning::pull_hundred_thousand", "progress::window_chain_boundary",
                 "staging::fjall_invite_beyond_caps", "staging::memory_invite_beyond_caps"):
        exact = f"tests::{name}"
        result.append({"label": f"ignored-{name.split('::')[-1]}", "timeout": 7200,
                       "argv": ["cargo", "test", "--locked", "--all-features", "--lib", exact,
                                "--", "--exact", "--ignored", "--test-threads=1"], "tests": [exact]})
    result.append({"label": "runner-tests", "timeout": 120,
                   "argv": [sys.executable, "-B", "-m", "unittest", "discover", "-s", "scripts", "-v"]})
    return result


if __name__ == "__main__":
    print(json.dumps(cases(), indent=2))
