"""Row-level cleaning: normalize each row, quarantine rows that cannot be counted, stage the rest."""
from __future__ import annotations

import csv
import json
import shutil
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from pipeline.settings import Paths
from pipeline.validate import is_blank_row

RECORD_FIELDS = [
    "file_date", "row_number", "transaction_id", "customer_id",
    "transaction_time", "transaction_date", "amount", "currency", "status",
]
QUARANTINE_FIELDS = ["file_date", "row_number", "transaction_id", "reason", "detail", "raw"]


class RowRejected(Exception):
    def __init__(self, reason: str, detail: str):
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


def parse_time(value: str, formats: list[str]) -> datetime:
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise RowRejected("invalid_transaction_time", f"cannot parse {value!r}")


def normalize_amount(value: str) -> str:
    if value == "":
        raise RowRejected("missing_amount", "amount is empty")
    try:
        amount = Decimal(value)
    except InvalidOperation:
        raise RowRejected("invalid_amount", f"cannot parse {value!r}") from None
    # 800, 800.0 and 800.00 all become "800" so duplicates compare equal
    return format(amount.normalize(), "f")


def clean_row(raw: dict, file_date: date, rules: dict, has_currency_column: bool) -> dict:
    value = {k.strip(): (v or "").strip() for k, v in raw.items() if k is not None}

    transaction_id = value.get("transaction_id", "")
    if not transaction_id:
        raise RowRejected("missing_transaction_id", "transaction_id is empty")

    ts = parse_time(value.get("transaction_time", ""), rules["transaction_time"]["formats"])
    txn_date = ts.date()

    window = rules["late_arrival"]
    earliest = file_date - timedelta(days=window["max_days_before_file_date"])
    if txn_date > file_date and not window["allow_future_dates"]:
        raise RowRejected("transaction_date_in_future", f"{txn_date} is after file date {file_date}")
    if txn_date < earliest:
        raise RowRejected("transaction_date_too_old", f"{txn_date} is before the accepted window start {earliest}")

    amount = normalize_amount(value.get("amount", ""))

    if has_currency_column:
        currency = value.get("currency", "").upper()
        if not currency:
            raise RowRejected("missing_currency", "currency is empty")
    else:
        currency = rules["currency"]["default"]
    if currency not in rules["currency"]["allowed"]:
        raise RowRejected("currency_not_allowed", f"{currency} is not in {rules['currency']['allowed']}")

    return {
        "transaction_id": transaction_id,
        "customer_id": value.get("customer_id", ""),
        "transaction_time": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "transaction_date": txn_date.isoformat(),
        "amount": amount,
        "currency": currency,
        "status": value.get("status", "").upper(),
    }


def clean_file(path: Path, file_date: date, rules: dict) -> tuple[list[dict], list[dict]]:
    """Return (records to stage, quarantined rows)."""
    records, quarantined = [], []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        header = [h.strip() for h in reader.fieldnames or []]
        has_currency_column = "currency" in header
        # row_number is the line number in the source file (header is line 1)
        for row_number, raw in enumerate(reader, start=2):
            if is_blank_row(list(raw.values())):
                continue
            base = {"file_date": file_date.isoformat(), "row_number": row_number}
            try:
                records.append({**base, **clean_row(raw, file_date, rules, has_currency_column)})
            except RowRejected as rejected:
                quarantined.append({
                    **base,
                    "transaction_id": (raw.get("transaction_id") or "").strip(),
                    "reason": rejected.reason,
                    "detail": rejected.detail,
                    "raw": json.dumps(raw, ensure_ascii=False),
                })
    return records, quarantined


def write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def staging_dir(paths: Paths, file_date: date) -> Path:
    return paths.staging / f"file_date={file_date:%Y%m%d}"


def stage_file(
    paths: Paths,
    file_date: date,
    source: Path,
    validation: dict,
    records: list[dict],
    quarantined: list[dict],
) -> dict:
    """Write staged records and row-level quarantine for one file.

    Rerunning the same file overwrites its staging (idempotent). If the source file was
    resent with different content, the previous staging is moved to archive first.
    """
    target = staging_dir(paths, file_date)
    meta_path = target / "_meta.json"
    archived_to = None
    if meta_path.exists():
        previous = json.loads(meta_path.read_text(encoding="utf-8"))
        if previous["sha256"] != validation["sha256"]:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            archived_to = paths.archive / target.name / f"replaced_at={stamp}"
            archived_to.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(target), str(archived_to))

    target.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target / "source.csv")
    write_csv(target / "records.csv", RECORD_FIELDS, records)
    write_csv(paths.quarantine / f"file_date={file_date:%Y%m%d}.csv", QUARANTINE_FIELDS, quarantined)

    reasons: dict[str, int] = {}
    for row in quarantined:
        reasons[row["reason"]] = reasons.get(row["reason"], 0) + 1

    meta = {
        **validation,
        "staged_records": len(records),
        "row_quarantined": len(quarantined),
        "row_quarantine_reasons": reasons,
        "archived_previous_version": str(archived_to) if archived_to else None,
    }
    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    return meta
