"""Structured pipeline observability and Airflow callbacks."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

EVENT_LOG = (
    PROJECT_ROOT
    / "metadata"
    / "pipeline_events.jsonl"
)

RUN_LOG = (
    PROJECT_ROOT
    / "metadata"
    / "pipeline_runs.jsonl"
)


def utc_now() -> str:
    """Return current UTC timestamp."""
    return datetime.now(
        timezone.utc
    ).isoformat()


def append_jsonl(
    path: Path,
    record: dict[str, Any],
) -> None:
    """Append one structured JSON record."""
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "a",
        encoding="utf-8",
    ) as file:
        file.write(
            json.dumps(
                record,
                sort_keys=True,
                default=str,
            )
        )
        file.write("\n")


def safe_duration(task_instance: Any) -> float | None:
    """Return task duration when available."""
    duration = getattr(
        task_instance,
        "duration",
        None,
    )

    if duration is None:
        return None

    try:
        return float(duration)
    except (TypeError, ValueError):
        return None


def task_success_callback(context: dict[str, Any]) -> None:
    """Persist a successful Airflow task record."""
    task_instance = context.get(
        "task_instance"
    )

    dag_run = context.get(
        "dag_run"
    )

    append_jsonl(
        EVENT_LOG,
        {
            "timestamp_utc": utc_now(),
            "pipeline_run_id": getattr(
                dag_run,
                "run_id",
                "unknown",
            ),
            "dag_id": getattr(
                dag_run,
                "dag_id",
                "unknown",
            ),
            "task_id": getattr(
                task_instance,
                "task_id",
                "unknown",
            ),
            "status": "SUCCESS",
            "try_number": getattr(
                task_instance,
                "try_number",
                None,
            ),
            "duration_seconds": safe_duration(
                task_instance
            ),
            "error_type": None,
            "error_message": None,
        },
    )


def task_failure_callback(context: dict[str, Any]) -> None:
    """Persist a failed Airflow task record."""
    task_instance = context.get(
        "task_instance"
    )

    dag_run = context.get(
        "dag_run"
    )

    exception = context.get(
        "exception"
    )

    append_jsonl(
        EVENT_LOG,
        {
            "timestamp_utc": utc_now(),
            "pipeline_run_id": getattr(
                dag_run,
                "run_id",
                "unknown",
            ),
            "dag_id": getattr(
                dag_run,
                "dag_id",
                "unknown",
            ),
            "task_id": getattr(
                task_instance,
                "task_id",
                "unknown",
            ),
            "status": "FAILED",
            "try_number": getattr(
                task_instance,
                "try_number",
                None,
            ),
            "duration_seconds": safe_duration(
                task_instance
            ),
            "error_type": (
                type(exception).__name__
                if exception
                else None
            ),
            "error_message": (
                str(exception)
                if exception
                else None
            ),
        },
    )


def artifact_counts() -> dict[str, int | None]:
    """Collect pipeline row counts for the run summary."""
    source_path = (
        PROJECT_ROOT
        / "data"
        / "source"
        / "Stroke_Prediction_Dataset.csv"
    )

    staging_path = (
        PROJECT_ROOT
        / "data"
        / "staging"
        / "stroke_cleaned.csv"
    )

    curated_path = (
        PROJECT_ROOT
        / "data"
        / "curated"
        / "stroke_model_ready.csv"
    )

    predictions_path = (
        PROJECT_ROOT
        / "outputs"
        / "predictions.csv"
    )

    split_path = (
        PROJECT_ROOT
        / "metadata"
        / "split_manifest.json"
    )

    counts: dict[str, int | None] = {
        "raw_rows": None,
        "staging_rows": None,
        "curated_rows": None,
        "train_rows": None,
        "test_rows": None,
        "prediction_rows": None,
    }

    if source_path.exists():
        counts["raw_rows"] = int(
            len(pd.read_csv(source_path))
        )

    if staging_path.exists():
        counts["staging_rows"] = int(
            len(pd.read_csv(staging_path))
        )

    if curated_path.exists():
        counts["curated_rows"] = int(
            len(pd.read_csv(curated_path))
        )

    if predictions_path.exists():
        counts["prediction_rows"] = int(
            len(pd.read_csv(predictions_path))
        )

    if split_path.exists():
        with split_path.open(
            "r",
            encoding="utf-8",
        ) as file:
            split = json.load(file)

        counts["train_rows"] = int(
            split.get(
                "train_rows",
                0,
            )
        )

        counts["test_rows"] = int(
            split.get(
                "test_rows",
                0,
            )
        )

    return counts


def write_run_record(
    context: dict[str, Any],
    status: str,
) -> None:
    """Persist one DAG-level pipeline summary."""
    dag_run = context.get(
        "dag_run"
    )

    exception = context.get(
        "exception"
    )

    started_at = getattr(
        dag_run,
        "start_date",
        None,
    )

    record = {
        "pipeline_run_id": getattr(
            dag_run,
            "run_id",
            "unknown",
        ),
        "dag_id": getattr(
            dag_run,
            "dag_id",
            "unknown",
        ),
        "started_at_utc": (
            started_at.isoformat()
            if started_at
            else None
        ),
        "ended_at_utc": utc_now(),
        "status": status,
        "error_type": (
            type(exception).__name__
            if exception
            else None
        ),
        "error_message": (
            str(exception)
            if exception
            else None
        ),
    }

    record.update(
        artifact_counts()
    )

    append_jsonl(
        RUN_LOG,
        record,
    )


def dag_success_callback(
    context: dict[str, Any],
) -> None:
    """Persist successful DAG completion."""
    write_run_record(
        context,
        "SUCCESS",
    )


def dag_failure_callback(
    context: dict[str, Any],
) -> None:
    """Persist failed DAG completion."""
    write_run_record(
        context,
        "FAILED",
    )