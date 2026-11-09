"""Resolve duplicates across all staged files, recompute affected dates, reconcile, and publish."""
from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal

from pipeline.clean import write_csv
from pipeline.dedup import DUPLICATE, ID_COLLISION, UNIQUE, VALUE_CONFLICT, classify_same_id_records
from pipeline.settings import Paths, rules_fingerprint

SUMMARY_FIELDS = ["transaction_date", "transaction_count", "total_amount"]
CONFLICT_FIELDS = ["transaction_id", "kind", "file_date", "row_number", "customer_id",
                   "transaction_time", "amount", "currency", "status"]
FLAG_FIELDS = ["flag", "file_date", "row_number", "transaction_id", "transaction_date", "amount", "customer_id"]

COUNTED = "counted"
DEDUPLICATED = "deduplicated"
CONFLICT = "conflict_quarantined"
NON_SUCCESS = "non_success"


class ReconciliationError(Exception):
    """Rows in != rows out: some rows were lost or double counted."""


def load_staged(paths: Paths) -> list[dict]:
    records = []
    for records_file in sorted(paths.staging.glob("file_date=*/records.csv")):
        with open(records_file, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                row["row_number"] = int(row["row_number"])
                records.append(row)
    records.sort(key=lambda r: (r["file_date"], r["row_number"]))
    return records


def row_key(record: dict) -> tuple[str, int]:
    return record["file_date"], record["row_number"]


def resolve(records: list[dict], rules: dict) -> tuple[dict, list[dict]]:
    """Give every staged record exactly one disposition. Returns (dispositions, conflict rows)."""
    by_id: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        by_id[record["transaction_id"]].append(record)

    dispositions: dict[tuple[str, int], str] = {}
    conflicts: list[dict] = []
    allowed_status = set(rules["status"]["allowed"])

    for transaction_id, same_id in by_id.items():
        kind = UNIQUE if len(same_id) == 1 else classify_same_id_records(same_id)
        if kind in (VALUE_CONFLICT, ID_COLLISION):
            for v in same_id:
                dispositions[row_key(v)] = CONFLICT
                conflicts.append({**{k: v[k] for k in CONFLICT_FIELDS if k in v}, "kind": kind})
            continue
        if kind not in (UNIQUE, DUPLICATE):
            raise ValueError(f"classify_same_id_records returned unexpected value {kind!r} for {transaction_id}")
        # Keep the earliest received record; later identical copies are duplicates
        first, *rest = same_id
        for v in rest:
            dispositions[row_key(v)] = DEDUPLICATED
        dispositions[row_key(first)] = COUNTED if first["status"] in allowed_status else NON_SUCCESS

    return dispositions, conflicts


def previous_trading_days(counted_dates: set[str], file_date: date, n: int) -> list[str]:
    before = sorted(d for d in counted_dates if d < file_date.isoformat())
    return before[-n:] if n > 0 else []


def summarize_dates(counted: list[dict], dates: set[str]) -> dict[str, dict]:
    totals: dict[str, dict] = {}
    for record in counted:
        d = record["transaction_date"]
        if d not in dates:
            continue
        entry = totals.setdefault(d, {"transaction_date": d, "transaction_count": 0, "total_amount": Decimal(0)})
        entry["transaction_count"] += 1
        entry["total_amount"] += Decimal(record["amount"])
    for entry in totals.values():
        entry["total_amount"] = format(entry["total_amount"].normalize(), "f")
    return totals


def read_summary(paths: Paths) -> dict[str, dict]:
    path = paths.output / "daily_summary.csv"
    if not path.exists():
        return {}
    with open(path, newline="", encoding="utf-8") as f:
        return {row["transaction_date"]: row for row in csv.DictReader(f)}


def collect_flags(records: list[dict], rules: dict) -> list[dict]:
    enabled = rules.get("flags", {})
    flags = []
    for r in records:
        amount = Decimal(r["amount"])
        found = []
        if enabled.get("negative_amount") and amount < 0:
            found.append("negative_amount")
        if enabled.get("zero_amount") and amount == 0:
            found.append("zero_amount")
        if enabled.get("missing_customer_id") and not r["customer_id"]:
            found.append("missing_customer_id")
        for flag in found:
            flags.append({"flag": flag, **{k: r[k] for k in FLAG_FIELDS if k in r}})
    return flags


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)


def build_summary(paths: Paths, file_date: date, rules: dict, run_id: str) -> dict:
    meta_path = paths.staging / f"file_date={file_date:%Y%m%d}" / "_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    records = load_staged(paths)
    dispositions, conflicts = resolve(records, rules)
    counted = [r for r in records if dispositions[row_key(r)] == COUNTED]
    current = [r for r in records if r["file_date"] == file_date.isoformat()]

    # 1. Reconcile: every data row of this file must end up in exactly one bucket
    buckets = {"row_quarantined": meta["row_quarantined"], COUNTED: 0, DEDUPLICATED: 0, CONFLICT: 0, NON_SUCCESS: 0}
    for r in current:
        buckets[dispositions[row_key(r)]] += 1
    accounted = sum(buckets.values())
    reconciliation = {"rows_read": meta["data_rows"], **buckets, "balanced": accounted == meta["data_rows"]}
    if not reconciliation["balanced"]:
        raise ReconciliationError(f"rows_read={meta['data_rows']} but rows accounted for={accounted}: {buckets}")

    # 2. Affected dates: dates touched by this file (including other records with the same ids)
    #    plus the previous N trading days
    current_ids = {r["transaction_id"] for r in current}
    involved = {r["transaction_date"] for r in records if r["transaction_id"] in current_ids}
    lookback = previous_trading_days({r["transaction_date"] for r in counted}, file_date,
                                     rules["lookback"]["trading_days"])
    affected = involved | set(lookback)

    # 3. Recompute affected dates from scratch and merge into the published summary
    before = read_summary(paths)
    recomputed = summarize_dates(counted, affected)
    after = {d: row for d, row in before.items() if d not in affected}
    after.update(recomputed)

    changes = []
    for d in sorted(affected):
        old, new = before.get(d), after.get(d)
        old_v = (str(old["transaction_count"]), str(old["total_amount"])) if old else None
        new_v = (str(new["transaction_count"]), str(new["total_amount"])) if new else None
        if old_v != new_v:
            changes.append({"transaction_date": d,
                            "before": dict(zip(["transaction_count", "total_amount"], old_v)) if old_v else None,
                            "after": dict(zip(["transaction_count", "total_amount"], new_v)) if new_v else None})

    summary_rows = [after[d] for d in sorted(after)]
    write_csv(paths.output / "daily_summary.csv", SUMMARY_FIELDS, summary_rows)
    write_csv(paths.quarantine / "conflicts.csv", CONFLICT_FIELDS, conflicts)

    flags = collect_flags([r for r in current if dispositions[row_key(r)] == COUNTED], rules)
    current_conflicts = sorted({c["transaction_id"] for c in conflicts if c["transaction_id"] in current_ids})

    audit = {
        "run_id": run_id,
        "file_date": file_date.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rules": {"version": rules.get("version"), "fingerprint": rules_fingerprint(rules)},
        "source": {k: meta[k] for k in ("file", "size_bytes", "sha256", "contract_version", "data_rows")},
        "archived_previous_version": meta.get("archived_previous_version"),
        "reconciliation": reconciliation,
        "row_quarantine_reasons": meta["row_quarantine_reasons"],
        "conflicts_involving_this_file": current_conflicts,
        "flags": {f: sum(1 for x in flags if x["flag"] == f) for f in sorted({x["flag"] for x in flags})},
        "affected_dates": sorted(affected),
        "lookback_dates": lookback,
        "summary_changes": changes,
    }

    # A rerun can reuse its run_id, so the timestamp keeps every run's snapshot
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    run_dir = paths.history / f"file_date={file_date:%Y%m%d}" / f"run={safe_name(run_id)}" / f"at={stamp}"
    write_csv(run_dir / "daily_summary.csv", SUMMARY_FIELDS, summary_rows)
    write_csv(run_dir / "flagged.csv", FLAG_FIELDS, flags)
    (run_dir / "audit.json").write_text(json.dumps(audit, indent=2, ensure_ascii=False), encoding="utf-8")
    audit["audit_path"] = str(run_dir / "audit.json")
    return audit
