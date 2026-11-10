"""Tests for waiting on the source file and the failure alert, run without Airflow.

Run from the project root:
    python -m unittest tests.test_file_arrival -v
"""
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from pipeline.alerts import task_failure_details
from pipeline.run_date import TAIPEI
from pipeline.settings import Paths, load_rules
from pipeline.validate import FileMissingError, check_file_arrived, file_deadline

# The scheduled 00:30 (UTC+8) run for 2026-06-06, as Airflow stores it
LOGICAL_DATE = datetime(2026, 6, 5, 16, 30, tzinfo=timezone.utc)


class FileDeadlineTest(unittest.TestCase):
    def test_scheduled_run_waits_until_0230_taipei(self):
        timeout = load_rules()["file_sensor"]["timeout_seconds"]
        deadline = file_deadline(LOGICAL_DATE, timeout)
        self.assertEqual(deadline.astimezone(TAIPEI), datetime(2026, 6, 6, 2, 30, tzinfo=TAIPEI))

    def test_deadline_is_scheduled_time_plus_timeout(self):
        # Based only on logical_date, so a backfill months later gets the same deadline
        self.assertEqual(file_deadline(LOGICAL_DATE, 60), LOGICAL_DATE + timedelta(seconds=60))


class CheckFileArrivedTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.path = self.tmp / "transactions_20260606.csv"
        self.deadline = file_deadline(LOGICAL_DATE, 7200)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_existing_file_passes(self):
        self.path.write_text("transaction_id\n", encoding="utf-8")
        self.assertTrue(check_file_arrived(self.path, self.deadline + timedelta(days=100), self.deadline))

    def test_missing_file_before_deadline_keeps_waiting(self):
        self.assertFalse(check_file_arrived(self.path, self.deadline - timedelta(seconds=1), self.deadline))

    def test_missing_file_at_deadline_fails_and_names_the_file(self):
        with self.assertRaisesRegex(FileMissingError, r"transactions_20260606\.csv not found.*2026-06-06 02:30"):
            check_file_arrived(self.path, self.deadline, self.deadline)

    def test_backfill_of_old_missing_date_fails_at_once(self):
        months_later = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)
        with self.assertRaises(FileMissingError):
            check_file_arrived(self.path, months_later, self.deadline)


class TaskFailureDetailsTest(unittest.TestCase):
    def test_alert_names_task_date_and_expected_file(self):
        paths = Paths(Path("/opt/airflow/data"))
        ti = SimpleNamespace(dag_id="daily_transactions", task_id="wait_for_file", try_number=1,
                             log_url="http://localhost:8080/log")
        context = {"task_instance": ti, "run_id": "backfill__2026-06-05T16:30:00+00:00",
                   "logical_date": LOGICAL_DATE, "exception": FileMissingError("missing")}
        details = task_failure_details(context, paths)
        self.assertEqual(details["task_id"], "wait_for_file")
        self.assertEqual(details["file_date"], "2026-06-06")
        self.assertEqual(Path(details["expected_file"]), paths.incoming / "transactions_20260606.csv")
        self.assertIn("FileMissingError", details["error"])
        self.assertEqual(details["try_number"], 1)


if __name__ == "__main__":
    unittest.main()
