"""Interrupted incremental-ingestion recovery demonstration."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import (
    datetime,
    timezone,
)
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd

import src.ingest as ingest_module
from src.config import (
    PROJECT_ROOT,
    get_configured_path,
    load_settings,
)


def read_json(
    path: Path,
) -> dict[str, Any]:
    """Read a JSON object."""
    with path.open(
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(
            file
        )


def summarize_ingestion_result(
    result: dict[str, Any],
) -> dict[str, Any]:
    """Remove temporary-machine paths from saved evidence."""
    return {
        key: result[
            key
        ]
        for key in [
            "status",
            "mode",
            "pipeline_run_id",
            "processed_batches",
            "rows_processed",
            "last_completed_batch",
            "total_batches",
        ]
        if key in result
    }


def resolve_output_path(
    output_path: str | Path,
) -> Path:
    """Resolve the permanent evidence-log destination."""
    path = Path(
        output_path
    ).expanduser()

    if not path.is_absolute():
        path = (
            PROJECT_ROOT
            / path
        )

    return path.resolve()


def run_interrupted_ingestion_demo(
    output_path: str | Path = (
        "docs/interrupted_run_log.json"
    ),
) -> dict[str, Any]:
    """
    Demonstrate crash-safe incremental recovery.

    Sequence:
    1. Process batches 1 and 2 normally.
    2. Write batch 3, then simulate failure before checkpoint commit.
    3. Verify checkpoint remains at batch 2.
    4. Resume from checkpoint.
    5. Verify 5,110 rows and 5,110 unique IDs.
    """
    base_settings = (
        load_settings()
    )

    with TemporaryDirectory(
        prefix=(
            "dss150p_interrupted_ingestion_"
        )
    ) as temp_directory:
        temp_root = Path(
            temp_directory
        )

        settings = deepcopy(
            base_settings
        )

        settings[
            "paths"
        ][
            "raw_dir"
        ] = str(
            temp_root
            / "data"
            / "raw"
        )

        settings[
            "paths"
        ][
            "checkpoint_file"
        ] = str(
            temp_root
            / "metadata"
            / "ingestion_checkpoint.json"
        )

        settings[
            "paths"
        ][
            "audit_log"
        ] = str(
            temp_root
            / "metadata"
            / "ingestion_audit.csv"
        )

        first_run = (
            ingest_module
            .ingest_incremental(
                max_batches=2,
                ingest_all=False,
                settings=settings,
            )
        )

        checkpoint_path = (
            get_configured_path(
                settings,
                "checkpoint_file",
            )
        )

        checkpoint_after_first = (
            read_json(
                checkpoint_path
            )
        )

        if (
            int(
                checkpoint_after_first[
                    "last_completed_batch"
                ]
            )
            != 2
        ):
            raise RuntimeError(
                "Recovery demo setup failed: "
                "checkpoint should be 2."
            )

        original_atomic_write = (
            ingest_module
            .atomic_write_json
        )

        interruption_message = ""

        def crash_before_batch_three_checkpoint(
            path: Path,
            payload: dict[str, Any],
        ) -> None:
            """
            Simulate process death immediately before checkpoint 3
            becomes durable.
            """
            if (
                Path(
                    path
                ).resolve()
                == checkpoint_path.resolve()
                and int(
                    payload.get(
                        "last_completed_batch",
                        0,
                    )
                )
                == 3
            ):
                raise RuntimeError(
                    "Controlled interruption after "
                    "batch 3 file write and before "
                    "checkpoint persistence."
                )

            original_atomic_write(
                path,
                payload,
            )

        ingest_module.atomic_write_json = (
            crash_before_batch_three_checkpoint
        )

        try:
            ingest_module.ingest_incremental(
                max_batches=1,
                ingest_all=False,
                settings=settings,
            )

            raise RuntimeError(
                "Expected interrupted ingestion "
                "did not fail."
            )

        except RuntimeError as exc:
            interruption_message = str(
                exc
            )

            if (
                "Controlled interruption"
                not in interruption_message
            ):
                raise

        finally:
            ingest_module.atomic_write_json = (
                original_atomic_write
            )

        checkpoint_after_failure = (
            read_json(
                checkpoint_path
            )
        )

        checkpoint_value_after_failure = int(
            checkpoint_after_failure[
                "last_completed_batch"
            ]
        )

        incremental_dir = (
            get_configured_path(
                settings,
                "raw_dir",
            )
            / settings[
                "ingestion"
            ][
                "incremental_dirname"
            ]
        )

        orphan_batch_three = (
            incremental_dir
            / "batch_0003.csv"
        )

        if (
            checkpoint_value_after_failure
            != 2
        ):
            raise RuntimeError(
                "Checkpoint advanced despite "
                "the simulated interruption."
            )

        if not (
            orphan_batch_three.exists()
        ):
            raise RuntimeError(
                "Batch 3 was not written before "
                "the interruption."
            )

        resumed_run = (
            ingest_module
            .ingest_incremental(
                ingest_all=True,
                settings=settings,
            )
        )

        final_checkpoint = (
            read_json(
                checkpoint_path
            )
        )

        batch_files = sorted(
            incremental_dir.glob(
                "batch_*.csv"
            )
        )

        frames = [
            pd.read_csv(
                path
            )
            for path in batch_files
        ]

        combined = pd.concat(
            frames,
            ignore_index=True,
        )

        final_rows = int(
            len(
                combined
            )
        )

        unique_ids = int(
            combined[
                "id"
            ]
            .nunique()
        )

        duplicate_ids = int(
            final_rows
            - unique_ids
        )

        final_completed_batch = int(
            final_checkpoint[
                "last_completed_batch"
            ]
        )

        total_batches = int(
            final_checkpoint[
                "total_batches"
            ]
        )

        if final_rows != 5110:
            raise RuntimeError(
                "Interrupted-run recovery produced "
                f"{final_rows} rows instead of 5110."
            )

        if unique_ids != 5110:
            raise RuntimeError(
                "Interrupted-run recovery produced "
                "duplicate/missing IDs."
            )

        if duplicate_ids != 0:
            raise RuntimeError(
                "Interrupted-run recovery produced "
                f"{duplicate_ids} duplicate IDs."
            )

        if (
            final_completed_batch
            != total_batches
        ):
            raise RuntimeError(
                "Recovery did not finish all batches."
            )

        result = {
            "status": "PASS",
            "generated_at_utc": (
                datetime.now(
                    timezone.utc
                ).isoformat()
            ),
            "scenario": (
                "batch_3_written_then_checkpoint_write_interrupted"
            ),
            "first_run": (
                summarize_ingestion_result(
                    first_run
                )
            ),
            "interrupted_run": {
                "attempted_batch": 3,
                "checkpoint_before": 2,
                "checkpoint_after": (
                    checkpoint_value_after_failure
                ),
                "batch_3_file_exists": bool(
                    orphan_batch_three.exists()
                ),
                "error_type": (
                    "RuntimeError"
                ),
                "error_message": (
                    interruption_message
                ),
            },
            "resumed_run": (
                summarize_ingestion_result(
                    resumed_run
                )
            ),
            "final_state": {
                "batch_files": len(
                    batch_files
                ),
                "last_completed_batch": (
                    final_completed_batch
                ),
                "total_batches": (
                    total_batches
                ),
                "rows": final_rows,
                "unique_ids": (
                    unique_ids
                ),
                "duplicate_ids": (
                    duplicate_ids
                ),
            },
        }

    destination = (
        resolve_output_path(
            output_path
        )
    )

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with destination.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            result,
            file,
            indent=2,
            sort_keys=True,
        )

        file.write(
            "\n"
        )

    try:
        evidence_file = (
            destination
            .relative_to(
                PROJECT_ROOT.resolve()
            )
            .as_posix()
        )
    except ValueError:
        evidence_file = str(
            destination
        )

    result[
        "evidence_file"
    ] = evidence_file

    return result