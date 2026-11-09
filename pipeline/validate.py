"""File-level checks. Any failure here fails the whole run."""
from __future__ import annotations

import csv
import hashlib
from datetime import date
from pathlib import Path

from pipeline.settings import Paths


class FileValidationError(Exception):
    """The file cannot be used at all (too small, no data, or schema contract violated)."""


def source_file_path(paths: Paths, file_date: date) -> Path:
    return paths.incoming / f"transactions_{file_date:%Y%m%d}.csv"


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def select_contract(rules: dict, file_date: date) -> dict:
    """Pick the newest schema contract whose effective_from is on or before the file date."""
    eligible = [
        c for c in rules["schema_contracts"]
        if date.fromisoformat(c["effective_from"]) <= file_date
    ]
    if not eligible:
        raise FileValidationError(f"No schema contract is effective for file date {file_date}")
    return max(eligible, key=lambda c: c["effective_from"])


def is_blank_row(row: list[str | None]) -> bool:
    # DictReader fills missing cells with None when a row is shorter than the header
    return not any((cell or "").strip() for cell in row)


def validate_file(path: Path, file_date: date, rules: dict) -> dict:
    checks = rules["file_checks"]

    size = path.stat().st_size
    if size < checks["min_size_bytes"]:
        raise FileValidationError(
            f"{path.name}: file size {size} bytes is below the minimum of {checks['min_size_bytes']} bytes"
        )

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        data_rows = sum(1 for row in reader if not is_blank_row(row))

    if not header:
        raise FileValidationError(f"{path.name}: file has no header")
    header = [col.strip() for col in header]

    if checks["require_data_rows"] and data_rows == 0:
        raise FileValidationError(f"{path.name}: file contains a header but no data rows")

    contract = select_contract(rules, file_date)
    known = set(contract["required"]) | set(contract["optional"])
    missing = [col for col in contract["required"] if col not in header]
    unknown = [col for col in header if col not in known]
    if missing or unknown:
        raise FileValidationError(
            f"{path.name}: schema contract {contract['version']} violated; "
            f"missing required columns={missing}, unknown columns={unknown}. "
            "Review with the source system before changing the contract."
        )

    return {
        "file": path.name,
        "file_date": file_date.isoformat(),
        "size_bytes": size,
        "sha256": file_sha256(path),
        "header": header,
        "data_rows": data_rows,
        "contract_version": contract["version"],
    }
