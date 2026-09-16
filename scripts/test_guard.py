import unittest
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

from guard import healthy, main


class Guard(unittest.TestCase):
    def test_existing_guard(self):
        with tempfile.TemporaryDirectory() as temporary:
            argv = ["guard.py", temporary, str(Path(temporary) / "resources.jsonl"), "cargo", "--version"]
            sample = {"available_memory": 8 * 1024**3, "filesystem_free": 16 * 1024**3}
            with patch.object(sys, "argv", argv), patch("guard.resources", return_value=sample), \
                    patch("guard.signal.signal"), patch("guard.subprocess.Popen") as launch:
                launch.return_value.poll.return_value = 0
                launch.return_value.returncode = 0
                self.assertEqual(main(), 0)
            command = launch.call_args.args[0]
            self.assertIn("CARGO_SAFE_ACTIVE=1", command[command.index("env") + 1:])
            self.assertIn("MemoryMax=8G", command[:command.index("env")])
            self.assertIn("MemorySwapMax=0", command[:command.index("env")])

    def test_headroom_floors(self):
        sample = {"available_memory": 4 * 1024**3, "filesystem_free": 8 * 1024**3}
        self.assertTrue(healthy(sample))
        for resource in sample:
            low = dict(sample)
            low[resource] -= 1
            self.assertFalse(healthy(low))
