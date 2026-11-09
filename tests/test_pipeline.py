"""End-to-end tests on the provided sample data, run without Airflow.

Run from the project root:
    python -m unittest tests.test_pipeline -v
"""
import csv
import json
import shutil
import tempfile
import unittest
from datetime import date
from pathlib import Path

from pipeline.runner import run_for_date
from pipeline.settings import PROJECT_ROOT, Paths, load_rules
from pipeline.validate import FileValidationError

SAMPLE_DIR = PROJECT_ROOT / "sample_data"
D = date.fromisoformat


class PipelineTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.paths = Paths(self.tmp)
        self.paths.incoming.mkdir(parents=True)
        for f in SAMPLE_DIR.glob("transactions_*.csv"):
            shutil.copyfile(f, self.paths.incoming / f.name)
        self.rules = load_rules()

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def run_date(self, d, run_id="test"):
        return run_for_date(D(d), run_id=run_id, paths=self.paths, rules=self.rules)

    def summary(self):
        with open(self.paths.output / "daily_summary.csv", newline="", encoding="utf-8") as f:
            return {r["transaction_date"]: (int(r["transaction_count"]), r["total_amount"]) for r in csv.DictReader(f)}

    def quarantine_reasons(self, file_date):
        with open(self.paths.quarantine / f"file_date={file_date}.csv", newline="", encoding="utf-8") as f:
            return {r["transaction_id"]: r["reason"] for r in csv.DictReader(f)}


class SampleDataTest(PipelineTestCase):
    def test_0601(self):
        audit = self.run_date("2026-06-01")
        # TXN0001 1200 + TXN0002 500 + TXN0003 800 + TXN0004 300 + TXN0005 -200
        self.assertEqual(self.summary(), {"2026-06-01": (5, "2600")})
        rec = audit["reconciliation"]
        self.assertEqual((rec["rows_read"], rec["counted"], rec["deduplicated"], rec["non_success"]), (7, 5, 1, 1))
        self.assertEqual(audit["flags"], {"missing_customer_id": 1, "negative_amount": 1})

    def test_0602_late_arrival_counts_toward_0601(self):
        self.run_date("2026-06-01")
        audit = self.run_date("2026-06-02")
        # TXN0009 (06-01 23:58) arrives in the 06-02 file and is added to 06-01
        self.assertEqual(self.summary(), {"2026-06-01": (6, "3050"), "2026-06-02": (2, "1800")})
        self.assertEqual(self.quarantine_reasons("20260602"), {"TXN0008": "missing_amount"})
        rec = audit["reconciliation"]
        self.assertEqual((rec["rows_read"], rec["counted"], rec["row_quarantined"], rec["deduplicated"],
                          rec["non_success"]), (6, 3, 1, 1, 1))

    def test_0603_conflict_corrects_0602(self):
        for d in ("2026-06-01", "2026-06-02"):
            self.run_date(d)
        audit = self.run_date("2026-06-03")
        # TXN0011 conflict (1100 vs 1200): removed from 06-02; TXN0014 / TXN0016 quarantined
        self.assertEqual(self.summary(), {
            "2026-06-01": (6, "3050"),
            "2026-06-02": (1, "700"),
            "2026-06-03": (3, "2600"),
        })
        self.assertEqual(self.quarantine_reasons("20260603"), {
            "TXN0014": "invalid_transaction_time",
            "TXN0016": "currency_not_allowed",
        })
        self.assertEqual(audit["conflicts_involving_this_file"], ["TXN0011"])
        self.assertEqual(audit["affected_dates"], ["2026-06-01", "2026-06-02", "2026-06-03"])
        self.assertIn({"transaction_date": "2026-06-02",
                       "before": {"transaction_count": "2", "total_amount": "1800"},
                       "after": {"transaction_count": "1", "total_amount": "700"}}, audit["summary_changes"])
        self.assertTrue(audit["reconciliation"]["balanced"])

    def test_0604_header_only_fails(self):
        with self.assertRaisesRegex(FileValidationError, "no data rows"):
            self.run_date("2026-06-04")

    def test_0605_too_small_fails(self):
        with self.assertRaisesRegex(FileValidationError, "below the minimum"):
            self.run_date("2026-06-05")

    def test_rerun_is_idempotent(self):
        for d in ("2026-06-01", "2026-06-02", "2026-06-03"):
            self.run_date(d)
        first = self.summary()
        self.run_date("2026-06-03", run_id="rerun")
        self.run_date("2026-06-02", run_id="rerun")
        self.assertEqual(self.summary(), first)

    def test_backfill_out_of_order_gives_same_result(self):
        for d in ("2026-06-03", "2026-06-01", "2026-06-02"):
            self.run_date(d)
        self.run_date("2026-06-03", run_id="final")
        self.assertEqual(self.summary(), {
            "2026-06-01": (6, "3050"),
            "2026-06-02": (1, "700"),
            "2026-06-03": (3, "2600"),
        })


class RuleTest(PipelineTestCase):
    def write_incoming(self, file_date, text):
        (self.paths.incoming / f"transactions_{file_date}.csv").write_text(text, encoding="utf-8")

    def test_unknown_column_fails_contract(self):
        self.write_incoming("20260610", "transaction_id,customer_id,transaction_time,amount,status,currency,channel\n"
                                        "TXN1,C1,2026-06-10 10:00:00,100,SUCCESS,TWD,APP\n")
        with self.assertRaisesRegex(FileValidationError, "unknown columns=\\['channel'\\]"):
            self.run_date("2026-06-10")

    def test_missing_currency_column_fails_v2_contract(self):
        self.write_incoming("20260610", "transaction_id,customer_id,transaction_time,amount,status\n"
                                        "TXN1,C1,2026-06-10 10:00:00,100,SUCCESS\n")
        with self.assertRaisesRegex(FileValidationError, "missing required columns=\\['currency'\\]"):
            self.run_date("2026-06-10")

    def test_late_arrival_window_and_currency_case(self):
        self.write_incoming("20260610", "transaction_id,customer_id,transaction_time,amount,status,currency\n"
                                        "TXN1,C1,2026-06-08 10:00:00,100,SUCCESS,twd\n"   # X-2: accepted, twd -> TWD
                                        "TXN2,C1,2026-06-07 10:00:00,100,SUCCESS,TWD\n"   # X-3: too old
                                        "TXN3,C1,2026-06-11 10:00:00,100,SUCCESS,TWD\n")  # future
        self.run_date("2026-06-10")
        self.assertEqual(self.quarantine_reasons("20260610"), {
            "TXN2": "transaction_date_too_old",
            "TXN3": "transaction_date_in_future",
        })
        self.assertEqual(self.summary(), {"2026-06-08": (1, "100")})

    def test_resent_file_archives_previous_version(self):
        self.run_date("2026-06-01")
        source = self.paths.incoming / "transactions_20260601.csv"
        source.write_text(source.read_text(encoding="utf-8").replace("1200", "1300"), encoding="utf-8")
        audit = self.run_date("2026-06-01", run_id="resent")
        self.assertIsNotNone(audit["archived_previous_version"])
        self.assertEqual(len(list(self.paths.archive.glob("file_date=20260601/replaced_at=*"))), 1)
        self.assertEqual(self.summary(), {"2026-06-01": (5, "2700")})

    def test_audit_written_to_history(self):
        audit = self.run_date("2026-06-01", run_id="scheduled__2026-06-01T00:30:00+08:00")
        saved = json.loads(Path(audit["audit_path"]).read_text(encoding="utf-8"))
        self.assertEqual(saved["source"]["sha256"], audit["source"]["sha256"])
        self.assertEqual(saved["rules"]["fingerprint"], audit["rules"]["fingerprint"])

    def test_rerun_with_same_run_id_keeps_both_snapshots(self):
        first = self.run_date("2026-06-01", run_id="same")
        second = self.run_date("2026-06-01", run_id="same")
        self.assertNotEqual(first["audit_path"], second["audit_path"])
        self.assertTrue(Path(first["audit_path"]).exists())
        self.assertTrue(Path(second["audit_path"]).exists())

    HEADER = "transaction_id,customer_id,transaction_time,amount,status,currency\n"

    def test_non_finite_amounts_are_quarantined(self):
        self.write_incoming("20260610", self.HEADER +
                            "TXN1,C1,2026-06-10 10:00:00,NaN,SUCCESS,TWD\n"
                            "TXN2,C1,2026-06-10 10:00:00,Infinity,SUCCESS,TWD\n"
                            "TXN3,C1,2026-06-10 10:00:00,-Infinity,SUCCESS,TWD\n"
                            "TXN4,C1,2026-06-10 10:00:00,100,SUCCESS,TWD\n")
        self.run_date("2026-06-10")
        self.assertEqual(self.quarantine_reasons("20260610"), {
            "TXN1": "invalid_amount", "TXN2": "invalid_amount", "TXN3": "invalid_amount",
        })
        self.assertEqual(self.summary(), {"2026-06-10": (1, "100")})

    def test_blank_row_with_too_few_columns_is_skipped(self):
        self.write_incoming("20260610", self.HEADER +
                            "TXN1,C1,2026-06-10 10:00:00,100,SUCCESS,TWD\n"
                            ",\n")
        audit = self.run_date("2026-06-10")
        self.assertEqual(audit["reconciliation"]["rows_read"], 1)
        self.assertEqual(self.summary(), {"2026-06-10": (1, "100")})

    def test_extra_columns_are_quarantined_unless_blank(self):
        self.write_incoming("20260610", self.HEADER +
                            "TXN1,C1,2026-06-10 10:00:00,100,SUCCESS,TWD,EXTRA\n"
                            "TXN2,C1,2026-06-10 10:00:00,200,SUCCESS,TWD,\n")
        audit = self.run_date("2026-06-10")
        self.assertEqual(self.quarantine_reasons("20260610"), {"TXN1": "too_many_columns"})
        self.assertTrue(audit["reconciliation"]["balanced"])
        self.assertEqual(self.summary(), {"2026-06-10": (1, "200")})

    def deliver(self, file_date, *rows):
        self.write_incoming(file_date, self.HEADER + "".join(f"{row},SUCCESS,TWD\n" for row in rows))
        return self.run_date(f"{file_date[:4]}-{file_date[4:6]}-{file_date[6:]}")

    def test_resent_file_without_its_own_date_removes_stale_total(self):
        self.deliver("20260610", "A1,C1,2026-06-10 10:00:00,100")
        self.deliver("20260611", "A2,C1,2026-06-11 10:00:00,70")
        self.deliver("20260611", "A9,C1,2026-06-10 12:00:00,5")
        self.assertEqual(self.summary(), {"2026-06-10": (2, "105")})

    def test_resent_file_recomputes_its_date_from_other_files(self):
        self.deliver("20260610", "A1,C1,2026-06-10 10:00:00,100")
        self.deliver("20260611", "A2,C1,2026-06-11 10:00:00,70")
        self.deliver("20260612", "A3,C1,2026-06-12 10:00:00,30", "A4,C1,2026-06-11 22:00:00,1")
        self.deliver("20260611", "A5,C1,2026-06-10 12:00:00,5")
        self.assertEqual(self.summary(), {
            "2026-06-10": (2, "105"),
            "2026-06-11": (1, "1"),
            "2026-06-12": (1, "30"),
        })

    def test_resent_file_removes_earlier_date_that_lost_all_rows(self):
        self.deliver("20260609", "A1,C1,2026-06-09 10:00:00,100")
        self.deliver("20260611", "A2,C1,2026-06-10 12:00:00,70", "A3,C1,2026-06-11 10:00:00,30")
        self.deliver("20260611", "A3,C1,2026-06-11 10:00:00,30")
        self.assertEqual(self.summary(), {"2026-06-09": (1, "100"), "2026-06-11": (1, "30")})

    OLD_RECORD = ("20260610", "TXN1,C1,2026-06-10 10:00:00,500")
    NEW_RECORD = ("20260618", "TXN1,C2,2026-06-17 10:00:00,300", "TXN2,C1,2026-06-18 10:00:00,70")

    def conflicts(self):
        with open(self.paths.quarantine / "conflicts.csv", newline="", encoding="utf-8") as f:
            return [(r["file_date"], r["kind"]) for r in csv.DictReader(f)]

    def test_id_collision_outside_window_keeps_old_record(self):
        self.deliver(*self.OLD_RECORD)
        audit = self.deliver(*self.NEW_RECORD)
        self.assertEqual(self.summary(), {"2026-06-10": (1, "500"), "2026-06-18": (1, "70")})
        self.assertEqual(self.conflicts(), [("2026-06-18", "id_collision_outside_window")])
        self.assertEqual(audit["conflicts_involving_this_file"], ["TXN1"])
        rec = audit["reconciliation"]
        self.assertEqual((rec["counted"], rec["conflict_quarantined"], rec["balanced"]), (1, 1, True))

    def test_id_collision_outside_window_is_order_independent(self):
        self.deliver(*self.NEW_RECORD)
        self.deliver(*self.OLD_RECORD)
        self.assertEqual(self.summary(), {"2026-06-10": (1, "500"), "2026-06-18": (1, "70")})
        self.assertEqual(self.conflicts(), [("2026-06-18", "id_collision_outside_window")])

    def test_id_collision_inside_window_quarantines_both(self):
        self.deliver("20260610", "TXN1,C1,2026-06-10 10:00:00,500")
        self.deliver("20260612", "TXN1,C2,2026-06-11 10:00:00,300", "TXN2,C1,2026-06-12 10:00:00,70")
        self.assertEqual(self.summary(), {"2026-06-12": (1, "70")})
        self.assertEqual(self.conflicts(), [("2026-06-10", "id_collision"), ("2026-06-12", "id_collision")])


if __name__ == "__main__":
    unittest.main()
