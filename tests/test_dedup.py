"""Tests for pipeline.dedup.classify_same_id_records.

Run from the project root:
    python -m unittest tests.test_dedup -v
"""
import unittest

from pipeline.dedup import DUPLICATE, ID_COLLISION, UNIQUE, VALUE_CONFLICT, classify_same_id_records


def record(**overrides):
    base = {
        "transaction_id": "TXN0011",
        "customer_id": "C008",
        "transaction_time": "2026-06-02 15:45:00",
        "amount": "1100",
        "currency": "TWD",
        "status": "SUCCESS",
    }
    return {**base, **overrides}


class ClassifySameIdRecordsTest(unittest.TestCase):
    def test_single_record_is_unique(self):
        self.assertEqual(classify_same_id_records([record()]), UNIQUE)

    def test_identical_records_are_duplicates(self):
        # TXN0002 / TXN0003: same transaction sent twice
        self.assertEqual(classify_same_id_records([record(), record()]), DUPLICATE)

    def test_three_identical_records_are_duplicates(self):
        self.assertEqual(classify_same_id_records([record(), record(), record()]), DUPLICATE)

    def test_different_amount_is_value_conflict(self):
        # TXN0011: 1100 in the 06-02 file, 1200 in the 06-03 file
        self.assertEqual(classify_same_id_records([record(amount="1100"), record(amount="1200")]), VALUE_CONFLICT)

    def test_different_status_is_value_conflict(self):
        self.assertEqual(classify_same_id_records([record(status="SUCCESS"), record(status="FAILED")]), VALUE_CONFLICT)

    def test_different_customer_is_id_collision(self):
        self.assertEqual(classify_same_id_records([record(customer_id="C008"), record(customer_id="C999")]), ID_COLLISION)

    def test_different_time_is_id_collision(self):
        self.assertEqual(
            classify_same_id_records([record(), record(transaction_time="2026-06-03 09:00:00")]), ID_COLLISION
        )

    def test_collision_wins_over_value_conflict(self):
        # identity differs AND amount differs: the more serious problem is reported
        self.assertEqual(classify_same_id_records([record(), record(customer_id="C999", amount="1")]), ID_COLLISION)

    def test_collision_in_later_record_wins_over_earlier_value_conflict(self):
        # record 2 differs in amount, record 3 differs in customer: every record's identity
        # must be checked before reporting a value conflict
        records = [record(), record(amount="1200"), record(customer_id="C999")]
        self.assertEqual(classify_same_id_records(records), ID_COLLISION)

    def test_conflict_detected_among_three_records(self):
        records = [record(), record(), record(amount="1200")]
        self.assertEqual(classify_same_id_records(records), VALUE_CONFLICT)


if __name__ == "__main__":
    unittest.main()
