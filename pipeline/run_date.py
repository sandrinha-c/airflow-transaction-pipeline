"""Which file date a DAG run processes.

The date comes from the run's logical_date, not the current time, so a rerun of
2026-06-02 executed on 2026-06-07 still processes transactions_20260602.csv.
"""
from __future__ import annotations

from datetime import date, timedelta, timezone

# The source system and the schedule are defined in UTC+8 (Taiwan has no daylight saving time)
TAIPEI = timezone(timedelta(hours=8))


def file_date_from(context: dict) -> date:
    """Return the UTC+8 date of the run's logical_date."""
    return context["logical_date"].astimezone(TAIPEI).date()
