"""Which file date a DAG run processes.

Never use datetime.now(): a rerun or backfill of 2026-06-02 executed on 2026-06-07
must still process transactions_20260602.csv.
"""
from __future__ import annotations

from datetime import date, timedelta, timezone

# The source system and the schedule are defined in UTC+8 (Taiwan has no daylight saving time)
TAIPEI = timezone(timedelta(hours=8))


def file_date_from(context: dict) -> date:
    """Return the file date for this run.

    context["logical_date"] is the time the run represents, stored in UTC.
    Example: 2026-06-02 16:30 UTC -> 2026-06-03 00:30 UTC+8 -> date 2026-06-03.
    """
    return context["logical_date"].astimezone(TAIPEI).date()
