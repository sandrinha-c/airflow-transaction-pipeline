"""Daily transaction batch: runs at 00:30 UTC+8 and processes that day's file.

Business logic lives in pipeline/ and rules in config/. This file only defines
the schedule, the task order, and what happens when a task fails.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.timetables.trigger import CronTriggerTimetable

from pipeline.alerts import send_alert
from pipeline.clean import clean_file, stage_file
from pipeline.run_date import file_date_from
from pipeline.settings import get_paths, load_rules
from pipeline.summarize import build_summary
from pipeline.validate import source_file_path, validate_file

RULES = load_rules()
PATHS = get_paths()
TAIPEI = "Asia/Taipei"


def alert_on_failure(context: dict) -> None:
    ti = context["task_instance"]
    send_alert(
        "ERROR",
        f"Task failed: {ti.dag_id}.{ti.task_id}",
        {
            "dag_id": ti.dag_id,
            "task_id": ti.task_id,
            "run_id": context["run_id"],
            "file_date": str(file_date_from(context)),
            "try_number": ti.try_number,
            "error": repr(context.get("exception")),
            "log_url": ti.log_url,
        },
    )


@dag(
    dag_id="daily_transactions",
    # CronTriggerTimetable makes logical_date the 00:30 fire time. The default cron
    # timetable would set it to the previous day and select the wrong file.
    schedule=CronTriggerTimetable("30 0 * * *", timezone=TAIPEI),
    start_date=pendulum.datetime(2026, 6, 1, tz=TAIPEI),
    # History is loaded on purpose with backfill, not automatically on deployment
    catchup=False,
    # Every run reads and rewrites the same staging and summary files
    max_active_runs=1,
    # File problems need a person, so retrying would only delay the alert
    default_args={"retries": 0, "on_failure_callback": alert_on_failure},
    tags=["transactions"],
)
def daily_transactions():
    @task.sensor(
        poke_interval=RULES["file_sensor"]["poke_interval_seconds"],
        timeout=RULES["file_sensor"]["timeout_seconds"],
        mode="reschedule",
    )
    def wait_for_file(**context) -> bool:
        return source_file_path(PATHS, file_date_from(context)).exists()

    @task
    def validate(**context) -> dict:
        file_date = file_date_from(context)
        return validate_file(source_file_path(PATHS, file_date), file_date, RULES)

    @task
    def clean_and_stage(validation: dict, **context) -> dict:
        file_date = file_date_from(context)
        source = source_file_path(PATHS, file_date)
        records, quarantined = clean_file(source, file_date, RULES)
        return stage_file(PATHS, file_date, source, validation, records, quarantined)

    @task
    def summarize(**context) -> dict:
        return build_summary(PATHS, file_date_from(context), RULES, run_id=context["run_id"])

    @task
    def report(audit: dict) -> None:
        """The run succeeded, but tell someone if any data needs review."""
        if (
            audit["reconciliation"]["row_quarantined"]
            or audit["conflicts_involving_this_file"]
            or audit["flags"]
        ):
            send_alert(
                "WARNING",
                "Data quality issues found",
                {
                    key: audit[key]
                    for key in (
                        "run_id",
                        "file_date",
                        "reconciliation",
                        "row_quarantine_reasons",
                        "conflicts_involving_this_file",
                        "flags",
                        "summary_changes",
                        "audit_path",
                    )
                },
            )

    validation = validate()
    staged = clean_and_stage(validation)
    audit = summarize()

    wait_for_file() >> validation
    staged >> audit
    report(audit)


daily_transactions()
