"""Alerting: every alert is appended to data/alerts/alerts.jsonl and logged.

If ALERT_WEBHOOK_URL is set (e.g. a Slack or Teams incoming webhook), the alert is also
POSTed there as JSON. No external service is contacted unless that variable is set.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import datetime, timezone

from pipeline.settings import Paths, get_paths

log = logging.getLogger("pipeline.alerts")


def send_alert(level: str, title: str, details: dict, paths: Paths | None = None) -> dict:
    paths = paths or get_paths()
    alert = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "level": level,
        "title": title,
        **details,
    }
    paths.alerts.mkdir(parents=True, exist_ok=True)
    with open(paths.alerts / "alerts.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(alert, ensure_ascii=False, default=str) + "\n")

    log.log(logging.ERROR if level == "ERROR" else logging.WARNING, "[ALERT %s] %s %s", level, title, details)

    url = os.environ.get("ALERT_WEBHOOK_URL")
    if url:
        body = json.dumps({"text": f"[{level}] {title}", "alert": alert}, default=str).encode("utf-8")
        request = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(request, timeout=10)
        except Exception:  # alerting must never break the pipeline itself
            log.exception("Failed to deliver alert to webhook")
    return alert
