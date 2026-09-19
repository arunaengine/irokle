#!/usr/bin/env python3
"""Failure propagation tests for the two-process version launcher."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import compatibility


FAKE = '''#!/usr/bin/env python3
import os
from pathlib import Path
import signal
import sys
import time
case = os.environ["IROKLE_PEER_CASE"]
ready = Path(os.environ["IROKLE_PEER_READY"])
ack = Path(os.environ["IROKLE_PEER_ACK"])
done = ack.with_suffix(".done")
server = os.environ["IROKLE_PEER_ROLE"] == "server"
if server:
    if case == "early":
        sys.exit(7)
    ready.write_bytes(b"ready")
    while not done.exists():
        time.sleep(0.01)
else:
    if case == "killed":
        os.kill(os.getpid(), signal.SIGKILL)
    if case == "timeout":
        time.sleep(60)
    if case == "client_exit":
        sys.exit(7)
    if case != "missing":
        ack.write_bytes(b"fixture")
    done.write_bytes(b"done")
if case == "zero":
    print("test result: ok. 0 passed; 0 failed; 0 ignored;")
else:
    print("test tests::versions::peer_process ... ok")
    print("test result: ok. 1 passed; 0 failed; 0 ignored;")
'''


class PeerLauncher(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.binary = self.root / "peer"
        self.binary.write_text(FAKE)
        self.binary.chmod(0o700)
        self.binaries = {"current": self.binary, "old": self.binary}

    def exchange(self, case, timeout=10):
        return compatibility.exchange(self.binaries, self.root / case, self.root,
                                      case, "current", "old", timeout)

    def test_early_recovery(self):
        failed = self.exchange("early")
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["exits"], [7])
        self.assertEqual(self.exchange("ordinary")["status"], "passed")

    def test_missing_ack(self):
        result = self.exchange("missing")
        self.assertEqual(result["status"], "failed")
        self.assertIn("missing signed ACK", result["reason"])

    def test_zero_matches(self):
        result = self.exchange("zero")
        self.assertEqual(result["status"], "failed")
        self.assertIn("cardinality", result["reason"])

    def test_failed_client(self):
        for case, expected in (("killed", -9), ("client_exit", 7)):
            with self.subTest(case=case):
                result = self.exchange(case)
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["exits"], [-15, expected])

    def test_timeout(self):
        result = self.exchange("timeout", timeout=0.2)
        self.assertEqual(result["status"], "failed")
        self.assertIn("timeout", result["reason"])
        self.assertTrue(all(code is not None for code in result["exits"]))

    def test_source_change(self):
        repos = {"current": self.root, "old": self.root}
        with patch.object(compatibility, "identity", side_effect=[1, 1, 2, 1]):
            result = compatibility.run(repos, self.binaries, self.root,
                                       self.root / "changed", ["ordinary"])
        self.assertEqual(result, 1)

    def test_duplicate_cases(self):
        with self.assertRaises(ValueError):
            compatibility.run({}, {}, self.root, self.root / "duplicate",
                              ["ordinary", "ordinary"])

    def test_all_success(self):
        with patch.object(compatibility, "identity", return_value=1):
            result = compatibility.run({"current": self.root}, self.binaries, self.root,
                                       self.root / "success", ["ordinary"])
        self.assertEqual(result, 0)

    def test_interrupted(self):
        with patch.object(compatibility, "inspect_log", side_effect=InterruptedError("signal")):
            with self.assertRaises(InterruptedError):
                self.exchange("ordinary")
        result = json.loads((self.root / "ordinary/result.json").read_text())
        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(result["exits"], [0, 0])

    def test_collect_failures(self):
        calls = []

        def exchange(*args):
            calls.append(args[3:])
            return {"status": "failed"} if len(calls) == 1 else {"status": "passed", "ack_sha256": "same"}

        with patch.object(compatibility, "identity", return_value=1), \
                patch.object(compatibility, "exchange", side_effect=exchange):
            result = compatibility.run({"current": self.root}, self.binaries, self.root,
                                       self.root / "collect", ["ordinary"])
        self.assertEqual(len(calls), 3)
        self.assertEqual(result, 1)


if __name__ == "__main__":
    unittest.main()
