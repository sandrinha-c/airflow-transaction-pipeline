"""Run the whole pipeline for one file date without Airflow (used by tests and for local debugging).

    python -m pipeline.runner 2026-06-03
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone

from pipeline.clean import clean_file, stage_file
from pipeline.settings import Paths, get_paths, load_rules
from pipeline.summarize import build_summary
from pipeline.validate import source_file_path, validate_file


def run_for_date(file_date: date, run_id: str | None = None, paths: Paths | None = None,
                 rules: dict | None = None) -> dict:
    paths = paths or get_paths()
    rules = rules or load_rules()
    run_id = run_id or f"local__{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"

    source = source_file_path(paths, file_date)
    validation = validate_file(source, file_date, rules)
    records, quarantined = clean_file(source, file_date, rules)
    stage_file(paths, file_date, source, validation, records, quarantined)
    return build_summary(paths, file_date, rules, run_id)


if __name__ == "__main__":
    audit = run_for_date(date.fromisoformat(sys.argv[1]))
    print(json.dumps(audit, indent=2, ensure_ascii=False))
