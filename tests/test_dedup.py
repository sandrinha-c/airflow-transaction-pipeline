"""Tests for pipeline.dedup.classify_versions.

Run from the project root:
    python -m unittest tests.test_dedup -v
"""
import unittest

from pipeline.dedup import DUPLICATE, ID_COLLISION, UNIQUE, VALUE_CONFLICT, classify_versions


def version(**overrides):
    base = {
        "transaction_id": "TXN0011",
        "customer_id": "C008",
        "transaction_time": "2026-06-02 15:45:00",
        "amount": "1100",
        "currency": "TWD",
        "status": "SUCCESS",
    }
    return {**base, **overrides}


class ClassifyVersionsTest(unittest.TestCase):
    def test_single_version_is_unique(self):
        self.assertEqual(classify_versions([version()]), UNIQUE)

    def test_identical_versions_are_duplicates(self):
        # TXN0002 / TXN0003: same transaction sent twice
        self.assertEqual(classify_versions([version(), version()]), DUPLICATE)

    def test_three_identical_versions_are_duplicates(self):
        self.assertEqual(classify_versions([version(), version(), version()]), DUPLICATE)

    def test_different_amount_is_value_conflict(self):
        # TXN0011: 1100 in the 06-02 file, 1200 in the 06-03 file
        self.assertEqual(classify_versions([version(amount="1100"), version(amount="1200")]), VALUE_CONFLICT)

    def test_different_status_is_value_conflict(self):
        self.assertEqual(classify_versions([version(status="SUCCESS"), version(status="FAILED")]), VALUE_CONFLICT)

    def test_different_customer_is_id_collision(self):
        self.assertEqual(classify_versions([version(customer_id="C008"), version(customer_id="C999")]), ID_COLLISION)

    def test_different_time_is_id_collision(self):
        self.assertEqual(
            classify_versions([version(), version(transaction_time="2026-06-03 09:00:00")]), ID_COLLISION
        )

    def test_collision_wins_over_value_conflict(self):
        # identity differs AND amount differs: the more serious problem is reported
        self.assertEqual(classify_versions([version(), version(customer_id="C999", amount="1")]), ID_COLLISION)

    def test_conflict_detected_among_three_versions(self):
        versions = [version(), version(), version(amount="1200")]
        self.assertEqual(classify_versions(versions), VALUE_CONFLICT)


if __name__ == "__main__":
    unittest.main()
