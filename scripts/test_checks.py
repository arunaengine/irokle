"""Regression tests for collected failures and independently selected cases."""

import csv
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

RUNNER = Path(__file__).with_name("checks.sh").resolve()


class Checks(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.repo = self.base / "repo"
        subprocess.run(["git", "clone", "--quiet", "--shared", "--no-checkout",
                        str(RUNNER.parent.parent), str(self.repo)], check=True)
        (self.repo / "source").write_text("initial\n")
        self.output = self.base / "output"

    def case(self, label, code, **kwargs):
        return {"label": label, "argv": [sys.executable, "-c", code], "timeout": 30, **kwargs}

    def run_cases(self, cases):
        manifest = self.base / "cases.json"
        manifest.write_text(json.dumps(cases))
        result = subprocess.run(["bash", str(RUNNER), str(self.repo), str(self.output),
                                 str(manifest)], capture_output=True, text=True, timeout=60)
        rows = []
        if (self.output / "CHECKS.csv").exists():
            with (self.output / "CHECKS.csv").open() as source:
                rows = list(csv.DictReader(source))
        return result, rows

    def test_collect_failure(self):
        result, rows = self.run_cases([self.case("early", "raise SystemExit(7)"),
                                      self.case("later", "print('ok')")])
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([r["exit"] for r in rows], ["7", "0"])
        self.assertEqual([r["status"] for r in rows], ["failed", "passed"])

    def test_all_success(self):
        result, rows = self.run_cases([self.case("one", "print('ok')"),
                                      self.case("two", "print('ok')")])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(rows), 2)
        self.assertEqual(json.loads((self.output / "complete.json").read_text())["status"], "passed")

    def test_missing_log(self):
        result, rows = self.run_cases([self.case("missing", "pass", required_logs=["absent.log"])])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing required log", rows[0]["reason"])

    def test_removed_log(self):
        code = f"from pathlib import Path; Path({str(self.output / 'removed.log')!r}).unlink()"
        result, rows = self.run_cases([self.case("removed", code)])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing", rows[0]["reason"])

    def test_exact_zero(self):
        code = "print('test result: ok. 0 passed; 0 failed; 0 ignored;')"
        result, rows = self.run_cases([self.case("zero", code, tests=["tests::wanted"])])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cardinality", rows[0]["reason"])

    def test_exact_one(self):
        code = "print('test tests::wanted ... ok\\ntest result: ok. 1 passed; 0 failed; 0 ignored;')"
        result, _ = self.run_cases([self.case("one", code, tests=["tests::wanted"])])
        self.assertEqual(result.returncode, 0)

    def test_exact_extra(self):
        code = "print('test tests::wanted ... ok\\ntest tests::extra ... ok\\ntest result: ok. 2 passed; 0 failed; 0 ignored;')"
        result, _ = self.run_cases([self.case("extra", code, tests=["tests::wanted"])])
        self.assertNotEqual(result.returncode, 0)

    def test_killed_child(self):
        code = "import os,signal; os.kill(os.getpid(), signal.SIGKILL)"
        result, rows = self.run_cases([self.case("killed", code)])
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(rows[0]["exit"], "-9")

    def test_source_changed(self):
        code = "from pathlib import Path; Path('source').write_text('changed')"
        result, rows = self.run_cases([self.case("changed", code), self.case("later", "pass")])
        self.assertNotEqual(result.returncode, 0)
        self.assertNotEqual(rows[0]["source_before"], rows[0]["source_after"])
        self.assertEqual(len(rows), 1)

    def test_duplicate_labels(self):
        result, rows = self.run_cases([self.case("same", "pass"), self.case("same", "pass")])
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(rows, [])
        self.assertIn("duplicate", result.stderr)

    def test_missing_command(self):
        result, rows = self.run_cases([{"label": "missing", "argv": ["/no/such/command"], "timeout": 1}])
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(rows[0]["status"], "failed")

    def test_child_timeout(self):
        result, rows = self.run_cases([self.case("timeout", "import signal; signal.pause()", timeout=0.1)])
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(rows[0]["exit"], "-9")

    def test_runner_signal(self):
        code = "import os,signal; os.kill(os.getppid(), signal.SIGTERM); signal.pause()"
        result, rows = self.run_cases([self.case("signal", code), self.case("later", "pass")])
        self.assertEqual(result.returncode, 143)
        self.assertEqual(len(rows), 1)
        self.assertNotEqual(rows[0]["status"], "passed")
        self.assertEqual(json.loads((self.output / "complete.json").read_text())["status"], "incomplete")


if __name__ == "__main__":
    unittest.main()
