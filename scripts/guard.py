#!/usr/bin/env python3
"""Run a command in its own capped Linux user scope with a headroom watchdog.

Usage: guard.py REPOSITORY NEW_LOG COMMAND [ARGUMENT ...]
The entire command tree shares 8 GiB, no swap, low CPU/I/O priority, two build
jobs and two test threads. Refuse or stop below 4 GiB available RAM or 8 GiB
filesystem headroom. JSON lines record resource samples and the final exit.
"""

import json
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
import uuid


def resources(repo):
    memory = {line.split(":")[0]: int(line.split()[1]) * 1024
              for line in Path("/proc/meminfo").read_text().splitlines()}
    return {"available_memory": memory["MemAvailable"],
            "filesystem_free": shutil.disk_usage(repo).free,
            "load": Path("/proc/loadavg").read_text().strip()}


def healthy(sample):
    return sample["available_memory"] >= 4 * 1024**3 and sample["filesystem_free"] >= 8 * 1024**3


def main():
    if len(sys.argv) < 4:
        raise ValueError(__doc__)
    repo, log = Path(sys.argv[1]).resolve(strict=True), Path(sys.argv[2])
    unit = "irokle-check-" + uuid.uuid4().hex + ".scope"
    command = ["systemd-run", "--user", "--scope", "--quiet", f"--unit={unit}",
               "-p", "MemoryMax=8G", "-p", "MemorySwapMax=0", "env", "CARGO_BUILD_JOBS=2",
               "RUST_TEST_THREADS=2", "CARGO_INCREMENTAL=0", "CARGO_PROFILE_DEV_DEBUG=0",
               "PYTHONDONTWRITEBYTECODE=1", "nice", "-n", "19", "ionice", "-c3", *sys.argv[3:]]
    process = None
    with log.open("x") as output:
        def record(**event):
            output.write(json.dumps({"utc": time.strftime("%FT%TZ", time.gmtime()), **event}) + "\n")
            output.flush()

        def stop(signum, _frame):
            raise InterruptedError(f"signal {signum}")

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        try:
            sample = resources(repo)
            record(unit=unit, command=command, **sample)
            if not healthy(sample):
                raise RuntimeError("insufficient initial resource headroom")
            process = subprocess.Popen(command, cwd=repo)
            while process.poll() is None:
                sample = resources(repo)
                record(**sample)
                if not healthy(sample):
                    raise RuntimeError("resource headroom floor crossed")
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    pass
            record(exit=process.returncode)
            return process.returncode if process.returncode >= 0 else 128 - process.returncode
        except (OSError, RuntimeError) as error:
            record(failure=str(error))
            if process is not None:
                subprocess.run(["systemctl", "--user", "kill", "--kill-whom=all", "--signal=SIGKILL", unit],
                               check=False, timeout=30)
                process.wait(timeout=30)
            return 1


if __name__ == "__main__":
    sys.exit(main())
