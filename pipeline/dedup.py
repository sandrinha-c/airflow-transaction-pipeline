"""Duplicate and conflict detection across every file received so far.

transaction_id is assumed to be globally unique (see DATA_OBSERVATION.md, assumption 4).
When the same id appears more than once, its records are compared after normalization.
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
# A newer record collides with one older than its file's late-arrival window
ID_COLLISION_OUTSIDE_WINDOW = "id_collision_outside_window"


def classify_same_id_records(records: list[dict]) -> str:
    """Classify all staged records that share one transaction_id.

    Each record is a dict with at least IDENTITY_FIELDS and VALUE_FIELDS.

    Returns:
        UNIQUE          only one record
        DUPLICATE       every record is identical on IDENTITY_FIELDS and VALUE_FIELDS
        VALUE_CONFLICT  identity fields all match, but some value field differs
                        (e.g. TXN0011: same customer and time, amount 1100 vs 1200)
        ID_COLLISION    some identity field differs: likely two different transactions
                        sharing one id
    """
    if len(records)== 1:
        return UNIQUE
    
    first = records[0]
        
    for record in records[1:]:
        for field in IDENTITY_FIELDS:
            if record[field] != first[field]:
                return ID_COLLISION

    for record in records[1:]:
        for field in VALUE_FIELDS:
            if record[field] != first[field]:
                return VALUE_CONFLICT

    return DUPLICATE

    
            

    
