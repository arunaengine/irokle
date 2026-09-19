#!/usr/bin/env python3
"""Run immutable old/current binaries as independent sync/5 peers.
Run under guard.py; scripts/README.md describes fixtures and exact selections.
"""

import csv
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from checks import identity, inspect_log, save


TEST = "tests::versions::peer_process"
CASES = ("ordinary", "bootstrap", "window", "reconnect", "collision", "fallback", "branch")


def main():
    def interrupted(signum, _frame):
        raise InterruptedError(f"version campaign interrupted by signal {signum}")

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, interrupted)
    if len(sys.argv) < 7:
        raise ValueError(__doc__)
    current, old, current_binary, old_binary, fixtures = [Path(value).resolve(strict=True)
                                                        for value in sys.argv[1:6]]
    output = Path(sys.argv[6]).resolve()
    return run({"current": current, "old": old}, {"current": current_binary, "old": old_binary},
               fixtures, output, sys.argv[7:] or list(CASES))



def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def stop(process):
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=30)


def exchange(binaries, directory, fixtures, case, client, server, timeout=360):
    directory.mkdir()
    environment = dict(os.environ, IROKLE_FIXTURES=str(fixtures), IROKLE_PEER_CASE=case,
                       IROKLE_PEER_READY=str(directory / "ready.bin"),
                       IROKLE_PEER_ACK=str(directory / "ack.bin"))
    children, logs = [], []
    started = time.monotonic()
    deadline = started + timeout
    result = {"case": case, "client": client, "server": server, "status": "incomplete"}
    try:
        for role, variant in (("server", server), ("client", client)):
            log = (directory / f"{role}.log").open("xb")
            logs.append(log)
            child = subprocess.Popen([str(binaries[variant]), TEST, "--exact", "--ignored",
                                      "--show-output", "--test-threads=1"],
                                     env=dict(environment, IROKLE_PEER_ROLE=role),
                                     stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            children.append(child)
            if role == "server":
                while not (directory / "ready.bin").exists():
                    if child.poll() is not None:
                        raise RuntimeError(f"server exited before readiness: {child.returncode}")
                    if time.monotonic() >= deadline:
                        raise TimeoutError("server readiness timeout")
                    time.sleep(0.02)
        while any(child.poll() is None for child in children):
            if any(child.poll() not in (None, 0) for child in children):
                raise RuntimeError("peer process failed")
            if time.monotonic() >= deadline:
                raise TimeoutError("peer completion timeout")
            time.sleep(0.02)
        for role, child, log in zip(("server", "client"), children, logs):
            log.flush()
            reason = inspect_log(Path(log.name), {"tests": [TEST]})
            if child.returncode != 0 or reason:
                raise RuntimeError(f"{role}: exit {child.returncode}: {reason}")
        ack = directory / "ack.bin"
        if not ack.is_file() or ack.stat().st_size == 0:
            raise RuntimeError("missing signed ACK fixture")
        result.update(status="passed", ack_sha256=digest(ack), ack_bytes=ack.stat().st_size)
    except InterruptedError:
        raise
    except (OSError, RuntimeError) as error:
        result.update(status="failed", reason=str(error))
    finally:
        for child in children:
            stop(child)
        for log in logs:
            log.close()
        result.update(seconds=time.monotonic() - started,
                      exits=[child.returncode for child in children])
        save(directory / "result.json", result)
    return result


def run(repositories, binaries, fixtures, output, cases):
    if not cases or len(set(cases)) != len(cases) or any(case not in CASES for case in cases):
        raise ValueError("select unique known cases")
    output.mkdir()
    before = {name: identity(repo) for name, repo in repositories.items()}
    hashes = {name: digest(binary) for name, binary in binaries.items()}
    save(output / "before.json", {"sources": before, "binary_sha256": hashes,
                                  "fixtures": {p.name: digest(p) for p in sorted(fixtures.glob("*.bin"))}})
    rows, ack_hashes = [], {}
    try:
        for case in cases:
            for client, server in (("current", "current"), ("old", "current"), ("current", "old")):
                label = f"{case}-{client}-to-{server}"
                row = exchange(binaries, output / label, fixtures, case, client, server)
                if row["status"] == "passed":
                    expected = ack_hashes.setdefault(case, row["ack_sha256"])
                    if row["ack_sha256"] != expected:
                        row.update(status="failed", reason="cross-version signed ACK bytes differ")
                rows.append(dict(label=label, **row))
                print(f"{label}: {row['status']} {row.get('reason', '')}", flush=True)
    finally:
        after = {name: identity(repo) for name, repo in repositories.items()}
        stable = before == after and hashes == {name: digest(binary) for name, binary in binaries.items()}
        save(output / "after.json", {"sources": after, "stable": stable})
        complete = len(rows) == len(cases) * 3
        passed = complete and stable and all(row["status"] == "passed" for row in rows)
        save(output / "results.json", {"complete": complete, "passed": passed, "rows": rows})
        with (output / "CHECKS.csv").open("x", newline="") as table:
            writer = csv.DictWriter(table, fieldnames=("label", "status", "seconds", "reason"),
                                    extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
    return 0 if passed else 1




if __name__ == "__main__":
    sys.exit(main())
