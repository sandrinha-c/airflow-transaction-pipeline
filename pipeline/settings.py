"""Paths and rule loading shared by all pipeline steps."""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Paths:
    root: Path

    @property
    def incoming(self) -> Path:
        return self.root / "incoming"

    @property
    def staging(self) -> Path:
        return self.root / "staging"

    @property
    def archive(self) -> Path:
        return self.root / "archive"

    @property
    def quarantine(self) -> Path:
        return self.root / "quarantine"

    @property
    def output(self) -> Path:
        return self.root / "output"

    @property
    def history(self) -> Path:
        return self.output / "history"

    @property
    def alerts(self) -> Path:
        return self.root / "alerts"


def get_paths(data_dir: str | Path | None = None) -> Paths:
    root = data_dir or os.environ.get("PIPELINE_DATA_DIR") or PROJECT_ROOT / "data"
    return Paths(Path(root))


def load_rules(path: str | Path | None = None) -> dict:
    path = path or os.environ.get("PIPELINE_CONFIG_PATH") or PROJECT_ROOT / "config" / "data_rules.yaml"
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def rules_fingerprint(rules: dict) -> str:
    """Short hash of the rules, so each run records exactly which rules produced its output."""
    canonical = json.dumps(rules, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]
