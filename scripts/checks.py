#!/usr/bin/env python3
"""Run serial JSON checks with exact source, test, and log evidence.
See scripts/README.md for the manifest and enforced resource wrapper.
"""

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("cases", type=Path)
    args = parser.parse_args()
    try:
        return run(args.repo.resolve(strict=True), args.output.resolve(),
                   json.loads(args.cases.read_text()))
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(f"check runner failed: {error}", file=sys.stderr)
        return 1



def git(repo, *args):
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    return subprocess.check_output(["git", "-C", str(repo), *args], env=env)


def identity(repo):
    paths = git(repo, "ls-files", "-z", "--cached", "--others", "--exclude-standard")
    digest = hashlib.sha256()
    stats = hashlib.sha256()
    for name in sorted(set(paths.split(b"\0")) - {b""}):
        path = repo / os.fsdecode(name)
        digest.update(name + b"\0")
        if path.exists() or path.is_symlink():
            stat = path.lstat()
            stats.update(name + str((stat.st_ino, stat.st_mtime_ns, stat.st_ctime_ns)).encode())
        if path.is_symlink():
            digest.update(b"link\0" + os.fsencode(os.readlink(path)))
        elif path.is_file():
            digest.update(str(path.stat().st_mode & 0o777).encode() + b"\0")
            with path.open("rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
        else:
            digest.update(b"missing")
        digest.update(b"\0")
    return {
        "head": git(repo, "rev-parse", "HEAD").decode().strip(),
        "tree": git(repo, "rev-parse", "HEAD^{tree}").decode().strip(),
        "source_sha256": digest.hexdigest(),
        "stat_sha256": stats.hexdigest(),
        "status": git(repo, "status", "--porcelain=v1").decode(),
    }


def save(path, value):
    with path.open("x") as output:
        json.dump(value, output, indent=2)
        output.write("\n")


def validate(cases):
    if not isinstance(cases, list) or not cases:
        raise ValueError("a nonempty check list is required")
    labels = set()
    for case in cases:
        label = case["label"]
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", label) or label in labels:
            raise ValueError(f"invalid or duplicate label: {label}")
        labels.add(label)
        argv = case["argv"]
        if not argv or not all(isinstance(arg, str) for arg in argv):
            raise ValueError(f"invalid argv: {label}")
        if not 0 < case["timeout"] < float("inf"):
            raise ValueError(f"finite positive timeout required: {label}")
        if "tests" in case:
            tests = case["tests"]
            if not tests or len(set(tests)) != len(tests):
                raise ValueError(f"nonempty unique exact test names required: {label}")
        for log in case.get("required_logs", []):
            if Path(log).is_absolute() or ".." in Path(log).parts:
                raise ValueError(f"required log must be repository-relative: {log}")


def inspect_log(path, case):
    if not path.is_file() or path.stat().st_size == 0:
        return "missing or empty command log"
    text = path.read_text(errors="replace")
    counts = re.findall(r"^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;", text, re.MULTILINE)
    if "tests" in case:
        matches = re.findall(r"^test (\S+) \.\.\. ", text, re.MULTILINE)
        wanted = sorted(case["tests"])
        if sorted(matches) != wanted or counts != [(str(len(wanted)), "0", "0")]:
            return f"exact test cardinality mismatch: expected {wanted}, got {matches}, summaries {counts}"
    elif counts and not any(int(passed) for passed, _, _ in counts):
        return "test command matched no executed tests"
    return ""


def stop_group(process):
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def run(repo, output, cases):
    validate(cases)
    baseline = identity(repo)
    output.mkdir(parents=True, exist_ok=False)
    save(output / "cases.json", cases)
    save(output / "before.json", baseline)
    save(output / "state.json", {"status": "incomplete", "pid": os.getpid()})
    interrupted = 0

    def interrupt(signum, _frame):
        nonlocal interrupted
        interrupted = signum
        raise InterruptedError(f"runner interrupted by signal {signum}")

    handlers = {s: signal.signal(s, interrupt) for s in (signal.SIGTERM, signal.SIGINT)}
    fields = ["label", "exit", "status", "seconds", "source_before", "source_after", "log", "reason"]
    failed = False
    completed = 0
    artifacts = {}
    try:
        with (output / "CHECKS.csv").open("x", newline="") as table:
            writer = csv.DictWriter(table, fieldnames=fields)
            writer.writeheader()
            table.flush()
            for case in cases:
                label = case["label"]
                before = identity(repo)
                path = output / f"{label}.log"
                code, reason, process = None, "", None
                started = time.monotonic()
                try:
                    if before != baseline:
                        raise RuntimeError("source changed before command")
                    with path.open("x") as log:
                        log.write(json.dumps({"argv": case["argv"], "source": before,
                                              "started_utc": time.strftime("%FT%TZ", time.gmtime())}) + "\n")
                        log.flush()
                        process = subprocess.Popen(case["argv"], cwd=repo, stdout=log,
                                                   stderr=subprocess.STDOUT, start_new_session=True)
                        code = process.wait(timeout=case["timeout"])
                    if code != 0:
                        reason = f"child exit {code}"
                    reason = reason or inspect_log(path, case)
                    for required in case.get("required_logs", []):
                        required = repo / required
                        if not required.is_file() or required.stat().st_size == 0:
                            reason = reason or f"missing required log: {required}"
                except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                    reason = str(error)
                finally:
                    if process is not None:
                        stop_group(process)
                        code = process.returncode
                after = identity(repo)
                if after != baseline:
                    reason = reason or "source changed during command"
                status = "failed" if reason else "passed"
                failed |= bool(reason)
                for artifact in [path, *(repo / p for p in case.get("required_logs", []))]:
                    if artifact.is_file():
                        artifacts[str(artifact)] = hashlib.sha256(artifact.read_bytes()).hexdigest()
                writer.writerow(dict(label=label, exit=code, status=status,
                                     seconds=f"{time.monotonic() - started:.3f}",
                                     source_before=before["source_sha256"],
                                     source_after=after["source_sha256"], log=path.name, reason=reason))
                table.flush()
                save(output / f"{label}.json", {"before": before, "after": after,
                                               "exit": code, "status": status, "reason": reason})
                completed += 1
                print(f"{label}: {status} (exit {code}) {reason}", flush=True)
                if interrupted or after != baseline:
                    break
    except InterruptedError:
        failed = True
    finally:
        for signum, handler in handlers.items():
            signal.signal(signum, handler)
    after = identity(repo)
    save(output / "after.json", after)
    changed = [path for path, digest in artifacts.items()
               if not Path(path).is_file() or hashlib.sha256(Path(path).read_bytes()).hexdigest() != digest]
    failed |= completed != len(cases) or after != baseline
    failed |= bool(changed)
    status = "incomplete" if completed != len(cases) else "failed" if failed else "passed"
    save(output / "complete.json", {"status": status, "completed": completed,
                                   "expected": len(cases), "signal": interrupted,
                                   "changed_artifacts": changed, "artifacts_sha256": artifacts})
    return (128 + interrupted) if interrupted else int(failed)




if __name__ == "__main__":
    sys.exit(main())
