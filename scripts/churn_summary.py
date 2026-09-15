#!/usr/bin/env python3
"""Report the largest recorded directory sample, never an instantaneous peak."""

import argparse
import hashlib
import json
from pathlib import Path
import re


def summarize(text, cycles):
    samples = [(int(cycle), int(size)) for cycle, size in re.findall(
        r"^cycle=(\d+) .*?directory_bytes=(\d+) ", text, re.MULTILINE)]
    if cycles < 10 or cycles % 10 or [cycle for cycle, _ in samples] != list(range(10, cycles + 1, 10)):
        raise ValueError("missing, duplicate, reordered or unexpected directory samples")
    if not re.search(r"^test result: ok\. 1 passed; 0 failed; 0 ignored;", text, re.MULTILINE):
        raise ValueError("no completed successful churn test")
    cycle, size = max(samples, key=lambda item: item[1])
    return {"largest_recorded_directory_sample_bytes": size, "cycle": cycle,
            "samples": len(samples), "sampling_interval_cycles": 10,
            "instantaneous_peak": "not measured"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path)
    parser.add_argument("cycles", type=int)
    args = parser.parse_args()
    content = args.log.read_bytes()
    result = summarize(content.decode(), args.cycles)
    result.update(log=str(args.log), sha256=hashlib.sha256(content).hexdigest())
    print(json.dumps(result, indent=2))
