import unittest
from churn_summary import summarize


class Churn(unittest.TestCase):
    def sample(self, cycle, size):
        return f"cycle={cycle} staged_bytes_before_discard=10 directory_bytes={size} journals=0\n"

    def complete(self, text):
        return text + "test result: ok. 1 passed; 0 failed; 0 ignored;\n"

    def test_maximum_sample(self):
        text = self.complete(self.sample(10, 588786045) + self.sample(20, 555854679))
        result = summarize(text, 20)
        self.assertEqual(result["largest_recorded_directory_sample_bytes"], 588786045)
        self.assertEqual(result["cycle"], 10)
        self.assertEqual(result["instantaneous_peak"], "not measured")

    def test_incomplete_refused(self):
        with self.assertRaises(ValueError):
            summarize(self.sample(10, 50), 10)

    def test_missing_refused(self):
        with self.assertRaises(ValueError):
            summarize(self.complete(self.sample(20, 50)), 20)

    def test_duplicate_refused(self):
        with self.assertRaises(ValueError):
            summarize(self.complete(self.sample(10, 50) * 2), 20)
