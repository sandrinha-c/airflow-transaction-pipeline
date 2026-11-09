"""Tests for pipeline.run_date.file_date_from.

Run from the project root:
    python -m unittest tests.test_run_date -v
"""
import unittest
from datetime import date, datetime, timezone

from pipeline.run_date import file_date_from


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


class FileDateFromTest(unittest.TestCase):
    def test_scheduled_run_at_0030_taipei(self):
        # The 00:30 (UTC+8) run on 2026-06-03 is stored as 2026-06-02 16:30 UTC
        context = {"logical_date": utc(2026, 6, 2, 16, 30), "params": {}}
        self.assertEqual(file_date_from(context), date(2026, 6, 3))

    def test_utc_date_differs_from_taipei_date(self):
        # 23:59 UTC is already 07:59 the next day in Taipei
        context = {"logical_date": utc(2026, 6, 4, 23, 59), "params": {}}
        self.assertEqual(file_date_from(context), date(2026, 6, 5))

    def test_same_date_in_both_zones(self):
        context = {"logical_date": utc(2026, 6, 1, 3, 0), "params": {}}
        self.assertEqual(file_date_from(context), date(2026, 6, 1))


if __name__ == "__main__":
    unittest.main()
