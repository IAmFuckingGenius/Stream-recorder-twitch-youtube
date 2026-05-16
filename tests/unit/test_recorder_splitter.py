import unittest

from streamrecorder.recorder import StreamRecorder
from streamrecorder.splitter import FileSplitter


class RecorderConfigTests(unittest.TestCase):
    def test_retry_values_use_configured_counts(self):
        recorder = StreamRecorder(retries=7, fragment_retries=8)

        self.assertEqual(recorder._retry_value(recorder.retries), "7")
        self.assertEqual(recorder._retry_value(recorder.fragment_retries), "8")


class SplitterValidationTests(unittest.TestCase):
    def test_rejects_non_positive_size(self):
        splitter = FileSplitter()

        with self.assertRaisesRegex(ValueError, "max_size_gb"):
            splitter.needs_splitting("missing.mp4", 0)

    def test_accepts_numeric_string_size(self):
        splitter = FileSplitter()

        self.assertFalse(splitter.needs_splitting("missing.mp4", "1.8"))


if __name__ == "__main__":
    unittest.main()
