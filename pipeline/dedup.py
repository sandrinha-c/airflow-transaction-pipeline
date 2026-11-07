"""Duplicate and conflict detection across every file received so far.

transaction_id is assumed to be globally unique (see DATA_OBSERVATION.md, assumption 4).
When the same id appears more than once, the versions are compared after normalization.
"""
from __future__ import annotations

# Fields that identify *which* transaction this is
IDENTITY_FIELDS = ("customer_id", "transaction_time")
# Fields that describe *what happened* in the transaction
VALUE_FIELDS = ("amount", "currency", "status")

UNIQUE = "unique"
DUPLICATE = "duplicate"
VALUE_CONFLICT = "value_conflict"
ID_COLLISION = "id_collision"


def classify_versions(versions: list[dict]) -> str:
    """Classify all staged versions of one transaction_id.

    Each version is a dict with at least IDENTITY_FIELDS and VALUE_FIELDS.

    Returns:
        UNIQUE          only one version
        DUPLICATE       every version is identical on IDENTITY_FIELDS and VALUE_FIELDS
        VALUE_CONFLICT  identity fields all match, but some value field differs
                        (e.g. TXN0011: same customer and time, amount 1100 vs 1200)
        ID_COLLISION    some identity field differs: likely two different transactions
                        sharing one id
    """
    raise NotImplementedError("TODO: implement classify_versions (see tests/test_dedup.py)")
