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


def safe_duration(
    task_instance: Any,
) -> float | None:
    """Return task duration when available."""
    duration = getattr(
        task_instance,
        "duration",
        None,
    )

    if duration is None:
        return None

    try:
        return float(
            duration
        )

    except (
        TypeError,
        ValueError,
    ):
        return None


def task_success_callback(
    context: dict[str, Any],
) -> None:
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


def task_failure_callback(
    context: dict[str, Any],
) -> None:
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
                type(
                    exception
                ).__name__
                if exception
                else None
            ),
            "error_message": (
                str(
                    exception
                )
                if exception
                else None
            ),
        },
    )


def artifact_counts() -> dict[str, int | None]:
    """Collect row counts for a successful pipeline run."""
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

    counts: dict[
        str,
        int | None,
    ] = {
        "raw_rows": None,
        "staging_rows": None,
        "curated_rows": None,
        "train_rows": None,
        "test_rows": None,
        "prediction_rows": None,
    }

    if source_path.exists():
        counts[
            "raw_rows"
        ] = int(
            len(
                pd.read_csv(
                    source_path
                )
            )
        )

    if staging_path.exists():
        counts[
            "staging_rows"
        ] = int(
            len(
                pd.read_csv(
                    staging_path
                )
            )
        )

    if curated_path.exists():
        counts[
            "curated_rows"
        ] = int(
            len(
                pd.read_csv(
                    curated_path
                )
            )
        )

    if predictions_path.exists():
        counts[
            "prediction_rows"
        ] = int(
            len(
                pd.read_csv(
                    predictions_path
                )
            )
        )

    if split_path.exists():
        with split_path.open(
            "r",
            encoding="utf-8",
        ) as file:
            split = json.load(
                file
            )

        counts[
            "train_rows"
        ] = int(
            split.get(
                "train_rows",
                0,
            )
        )

        counts[
            "test_rows"
        ] = int(
            split.get(
                "test_rows",
                0,
            )
        )

    return counts


def latest_failed_event(
    pipeline_run_id: str,
) -> dict[str, Any] | None:
    """Return the latest failed task event for one DAG run."""
    if not EVENT_LOG.exists():
        return None

    latest: dict[
        str,
        Any,
    ] | None = None

    with EVENT_LOG.open(
        "r",
        encoding="utf-8",
    ) as file:
        for line in file:
            line = line.strip()

            if not line:
                continue

            try:
                record = json.loads(
                    line
                )

            except json.JSONDecodeError:
                continue

            if (
                record.get(
                    "pipeline_run_id"
                )
                == pipeline_run_id
                and record.get(
                    "status"
                )
                == "FAILED"
            ):
                latest = record

    return latest


def write_database_run_record(
    record: dict[str, Any],
) -> bool:
    """
    Best-effort persistence of one DAG run to PostgreSQL.

    Observability must never cause the pipeline itself to fail,
    so database audit errors are intentionally contained here.
    """
    try:
        from src.storage import (
            initialize_database,
            postgres_connection,
        )

        initialize_database()

        with postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO audit.pipeline_runs (
                        pipeline_run_id,
                        started_at_utc,
                        ended_at_utc,
                        run_mode,
                        status,
                        raw_rows,
                        staging_rows,
                        curated_rows,
                        error_type,
                        error_message
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    )
                    ON CONFLICT (pipeline_run_id)
                    DO UPDATE SET
                        started_at_utc = EXCLUDED.started_at_utc,
                        ended_at_utc = EXCLUDED.ended_at_utc,
                        run_mode = EXCLUDED.run_mode,
                        status = EXCLUDED.status,
                        raw_rows = EXCLUDED.raw_rows,
                        staging_rows = EXCLUDED.staging_rows,
                        curated_rows = EXCLUDED.curated_rows,
                        error_type = EXCLUDED.error_type,
                        error_message = EXCLUDED.error_message;
                    """,
                    (
                        record[
                            "pipeline_run_id"
                        ],
                        (
                            record.get(
                                "started_at_utc"
                            )
                            or utc_now()
                        ),
                        record.get(
                            "ended_at_utc"
                        ),
                        record.get(
                            "run_mode"
                        ),
                        record[
                            "status"
                        ],
                        record.get(
                            "raw_rows"
                        ),
                        record.get(
                            "staging_rows"
                        ),
                        record.get(
                            "curated_rows"
                        ),
                        record.get(
                            "error_type"
                        ),
                        record.get(
                            "error_message"
                        ),
                    ),
                )

            connection.commit()

        return True

    except Exception:
        # Database audit logging is intentionally best-effort.
        # A logging failure must not fail an otherwise valid DAG.
        return False


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

    pipeline_run_id = getattr(
        dag_run,
        "run_id",
        "unknown",
    )

    started_at = getattr(
        dag_run,
        "start_date",
        None,
    )

    run_conf = getattr(
        dag_run,
        "conf",
        {},
    )

    failed_event = None

    if status == "FAILED":
        failed_event = (
            latest_failed_event(
                pipeline_run_id
            )
        )

    if exception is not None:
        error_type = (
            type(
                exception
            ).__name__
        )

        error_message = str(
            exception
        )

    elif failed_event is not None:
        error_type = (
            failed_event.get(
                "error_type"
            )
        )

        error_message = (
            failed_event.get(
                "error_message"
            )
        )

    else:
        error_type = None
        error_message = None

    record: dict[
        str,
        Any,
    ] = {
        "pipeline_run_id": (
            pipeline_run_id
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
        "run_mode": (
            run_conf.get(
                "run_mode",
                "full",
            )
            if isinstance(
                run_conf,
                dict,
            )
            else "full"
        ),
        "failure_scenario": (
            run_conf.get(
                "failure_scenario",
                "none",
            )
            if isinstance(
                run_conf,
                dict,
            )
            else "none"
        ),
        "error_type": (
            error_type
        ),
        "error_message": (
            error_message
        ),
        "failure_task_id": (
            failed_event.get(
                "task_id"
            )
            if failed_event
            else None
        ),
        "failure_try_number": (
            failed_event.get(
                "try_number"
            )
            if failed_event
            else None
        ),
    }

    if status == "SUCCESS":
        record.update(
            artifact_counts()
        )

    else:
        # A failed DAG may see files left by an older successful
        # execution. Do not attribute those stale artifacts to
        # the failed run.
        record.update(
            {
                "raw_rows": None,
                "staging_rows": None,
                "curated_rows": None,
                "train_rows": None,
                "test_rows": None,
                "prediction_rows": None,
            }
        )

    append_jsonl(
        RUN_LOG,
        record,
    )

    write_database_run_record(
        record
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