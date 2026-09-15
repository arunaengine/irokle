import unittest
from guard import healthy


class Guard(unittest.TestCase):
    def test_headroom_floors(self):
        sample = {"available_memory": 4 * 1024**3, "filesystem_free": 8 * 1024**3}
        self.assertTrue(healthy(sample))
        for resource in sample:
            low = dict(sample)
            low[resource] -= 1
            self.assertFalse(healthy(low))
