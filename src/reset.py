"""Safe reset utilities for generated pipeline state."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from src.config import (
    PROJECT_ROOT,
    get_configured_path,
    load_settings,
)
from src.storage import (
    initialize_database,
    postgres_connection,
)


def repository_path(
    path: Path,
) -> str:
    """Return a readable repository-relative path when possible."""
    try:
        return (
            path.resolve()
            .relative_to(
                PROJECT_ROOT.resolve()
            )
            .as_posix()
        )
    except ValueError:
        return str(
            path
        )


def remove_file(
    path: Path,
    removed: list[str],
    dry_run: bool,
) -> None:
    """Remove one generated file when it exists."""
    if not path.exists():
        return

    removed.append(
        repository_path(
            path
        )
    )

    if not dry_run:
        path.unlink()


def clear_directory(
    directory: Path,
    removed: list[str],
    dry_run: bool,
) -> None:
    """
    Remove generated contents while preserving .gitkeep placeholders.
    """
    if not directory.exists():
        return

    for child in sorted(
        directory.iterdir()
    ):
        if (
            child.name
            == ".gitkeep"
        ):
            continue

        removed.append(
            repository_path(
                child
            )
        )

        if dry_run:
            continue

        if child.is_dir():
            shutil.rmtree(
                child
            )
        else:
            child.unlink()


def reset_database_state(
    dry_run: bool = False,
) -> dict[str, Any]:
    """Reset project-owned PostgreSQL tables."""
    if dry_run:
        return {
            "status": "DRY_RUN",
            "tables": [
                "curated.stroke_model_ready",
                "audit.partition_loads",
                "audit.pipeline_runs",
            ],
        }

    initialize_database()

    with postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                TRUNCATE TABLE
                    curated.stroke_model_ready,
                    audit.partition_loads,
                    audit.pipeline_runs
                RESTART IDENTITY;
                """
            )

        connection.commit()

    return {
        "status": "RESET",
        "tables": [
            "curated.stroke_model_ready",
            "audit.partition_loads",
            "audit.pipeline_runs",
        ],
    }


def reset_generated(
    reset_database: bool = False,
    dry_run: bool = False,
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Reset generated pipeline state without touching canonical inputs.

    Preserved:
    - data/source/
    - source manifest
    - data contract
    - expected metrics
    - source code
    - tests
    - documentation/evidence
    """
    settings = (
        settings
        or load_settings()
    )

    removed: list[str] = []

    raw_dir = (
        get_configured_path(
            settings,
            "raw_dir",
        )
    )

    full_dir = (
        raw_dir
        / settings[
            "ingestion"
        ][
            "full_dirname"
        ]
    )

    incremental_dir = (
        raw_dir
        / settings[
            "ingestion"
        ][
            "incremental_dirname"
        ]
    )

    clear_directory(
        full_dir,
        removed,
        dry_run,
    )

    clear_directory(
        incremental_dir,
        removed,
        dry_run,
    )

    remove_file(
        get_configured_path(
            settings,
            "staging_file",
        ),
        removed,
        dry_run,
    )

    remove_file(
        get_configured_path(
            settings,
            "curated_file",
        ),
        removed,
        dry_run,
    )

    remove_file(
        get_configured_path(
            settings,
            "quarantine_file",
        ),
        removed,
        dry_run,
    )

    clear_directory(
        PROJECT_ROOT
        / "data"
        / "partitioned",
        removed,
        dry_run,
    )

    clear_directory(
        PROJECT_ROOT
        / "outputs"
        / "model",
        removed,
        dry_run,
    )

    clear_directory(
        PROJECT_ROOT
        / "outputs"
        / "storage",
        removed,
        dry_run,
    )

    remove_file(
        PROJECT_ROOT
        / "outputs"
        / "metrics.json",
        removed,
        dry_run,
    )

    remove_file(
        PROJECT_ROOT
        / "outputs"
        / "predictions.csv",
        removed,
        dry_run,
    )

    remove_file(
        get_configured_path(
            settings,
            "checkpoint_file",
        ),
        removed,
        dry_run,
    )

    remove_file(
        PROJECT_ROOT
        / "metadata"
        / "split_manifest.json",
        removed,
        dry_run,
    )

    database_result = None

    if reset_database:
        database_result = (
            reset_database_state(
                dry_run=dry_run
            )
        )

    return {
        "status": (
            "DRY_RUN"
            if dry_run
            else "RESET"
        ),
        "removed_count": len(
            removed
        ),
        "generated_paths": (
            removed
        ),
        "canonical_source_preserved": True,
        "database": (
            database_result
        ),
    }