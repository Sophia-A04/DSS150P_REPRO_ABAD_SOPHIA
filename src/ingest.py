"""Raw ingestion for the DSS150P reproducibility pipeline."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from math import ceil
from pathlib import Path
from typing import Any

import yaml

from src.config import get_configured_path, load_settings


AUDIT_FIELDS = [
    "pipeline_run_id",
    "ingested_at_utc",
    "mode",
    "status",
    "source_file",
    "source_sha256",
    "sequence_start",
    "sequence_end",
    "rows_processed",
    "batch_number",
    "output_file",
    "output_sha256",
    "message",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def load_source_manifest(settings: dict[str, Any]) -> dict[str, Any]:
    manifest_path = get_configured_path(settings, "source_manifest")

    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Source manifest not found: {manifest_path}"
        )

    with manifest_path.open("r", encoding="utf-8") as file:
        manifest = yaml.safe_load(file)

    if not isinstance(manifest, dict):
        raise ValueError("Source manifest is empty or invalid.")

    return manifest


def inspect_csv(path: Path) -> tuple[list[str], list[list[str]]]:
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as file:
        reader = csv.reader(file)

        try:
            header = next(reader)
        except StopIteration as exc:
            raise ValueError(f"Source CSV is empty: {path}") from exc

        rows = list(reader)

    return header, rows


def verify_source_integrity(
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    settings = settings or load_settings()

    source_path = get_configured_path(settings, "source_file")
    manifest = load_source_manifest(settings)

    if not source_path.exists():
        raise FileNotFoundError(
            f"Source dataset not found: {source_path}"
        )

    expected_sha256 = str(
        manifest["integrity"]["sha256"]
    ).strip().lower()

    actual_sha256 = sha256_file(source_path)

    if actual_sha256 != expected_sha256:
        raise ValueError(
            "SOURCE CHECKSUM FAILURE\n"
            f"Expected: {expected_sha256}\n"
            f"Actual:   {actual_sha256}"
        )

    header, rows = inspect_csv(source_path)

    actual_rows = len(rows)
    actual_columns = len(header)

    expected_rows = int(manifest["source_file"]["rows"])
    expected_columns = int(manifest["source_file"]["columns"])

    if actual_rows != expected_rows:
        raise ValueError(
            f"Source row-count mismatch: expected "
            f"{expected_rows}, found {actual_rows}"
        )

    if actual_columns != expected_columns:
        raise ValueError(
            f"Source column-count mismatch: expected "
            f"{expected_columns}, found {actual_columns}"
        )

    return {
        "status": "PASS",
        "source_file": str(source_path),
        "sha256": actual_sha256,
        "rows": actual_rows,
        "columns": actual_columns,
        "header": header,
    }


def append_audit(
    settings: dict[str, Any],
    record: dict[str, Any],
) -> None:
    audit_path = get_configured_path(settings, "audit_log")
    audit_path.parent.mkdir(parents=True, exist_ok=True)

    exists = audit_path.exists()

    normalized = {
        field: record.get(field, "")
        for field in AUDIT_FIELDS
    }

    with audit_path.open(
        "a",
        encoding="utf-8",
        newline="",
    ) as file:
        writer = csv.DictWriter(file, fieldnames=AUDIT_FIELDS)

        if not exists:
            writer.writeheader()

        writer.writerow(normalized)


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = path.with_name(path.name + ".tmp")

    with temporary_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, sort_keys=True)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())

    os.replace(temporary_path, path)


def ingest_full(
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    settings = settings or load_settings()
    verification = verify_source_integrity(settings)

    source_path = get_configured_path(settings, "source_file")
    raw_dir = get_configured_path(settings, "raw_dir")

    full_dir = raw_dir / settings["ingestion"]["full_dirname"]
    full_dir.mkdir(parents=True, exist_ok=True)

    output_path = (
        full_dir / settings["ingestion"]["full_filename"]
    )

    run_id = f"full-{uuid.uuid4().hex[:12]}"
    timestamp = utc_now()

    if output_path.exists():
        existing_sha256 = sha256_file(output_path)

        if existing_sha256 != verification["sha256"]:
            append_audit(
                settings,
                {
                    "pipeline_run_id": run_id,
                    "ingested_at_utc": timestamp,
                    "mode": "full",
                    "status": "FAILED",
                    "source_file": source_path.name,
                    "source_sha256": verification["sha256"],
                    "sequence_start": 1,
                    "sequence_end": verification["rows"],
                    "rows_processed": 0,
                    "output_file": str(output_path),
                    "output_sha256": existing_sha256,
                    "message": "Existing raw snapshot differs from source.",
                },
            )

            raise ValueError(
                "Existing raw full snapshot does not match source."
            )

        append_audit(
            settings,
            {
                "pipeline_run_id": run_id,
                "ingested_at_utc": timestamp,
                "mode": "full",
                "status": "SKIPPED",
                "source_file": source_path.name,
                "source_sha256": verification["sha256"],
                "sequence_start": 1,
                "sequence_end": verification["rows"],
                "rows_processed": 0,
                "output_file": str(output_path),
                "output_sha256": existing_sha256,
                "message": "Identical raw snapshot already exists.",
            },
        )

        return {
            "status": "SKIPPED",
            "mode": "full",
            "rows": verification["rows"],
            "output": str(output_path),
            "sha256": existing_sha256,
        }

    temporary_path = output_path.with_name(
        output_path.name + ".tmp"
    )

    shutil.copyfile(source_path, temporary_path)
    os.replace(temporary_path, output_path)

    output_sha256 = sha256_file(output_path)

    if output_sha256 != verification["sha256"]:
        output_path.unlink(missing_ok=True)
        raise ValueError(
            "Full ingestion checksum verification failed."
        )

    append_audit(
        settings,
        {
            "pipeline_run_id": run_id,
            "ingested_at_utc": timestamp,
            "mode": "full",
            "status": "SUCCESS",
            "source_file": source_path.name,
            "source_sha256": verification["sha256"],
            "sequence_start": 1,
            "sequence_end": verification["rows"],
            "rows_processed": verification["rows"],
            "output_file": str(output_path),
            "output_sha256": output_sha256,
            "message": "Full raw snapshot created successfully.",
        },
    )

    return {
        "status": "SUCCESS",
        "mode": "full",
        "rows": verification["rows"],
        "output": str(output_path),
        "sha256": output_sha256,
    }


def initialize_checkpoint(
    source_sha256: str,
    total_rows: int,
    batch_size: int,
) -> dict[str, Any]:
    return {
        "version": 1,
        "strategy": "immutable_source_row_sequence",
        "laboratory_simulation": True,
        "source_sha256": source_sha256,
        "batch_size": batch_size,
        "total_rows": total_rows,
        "total_batches": ceil(total_rows / batch_size),
        "last_completed_batch": 0,
        "completed_batches": {},
    }


def load_or_create_checkpoint(
    settings: dict[str, Any],
    source_sha256: str,
    total_rows: int,
    batch_size: int,
) -> tuple[Path, dict[str, Any]]:
    checkpoint_path = get_configured_path(
        settings,
        "checkpoint_file",
    )

    if not checkpoint_path.exists():
        checkpoint = initialize_checkpoint(
            source_sha256,
            total_rows,
            batch_size,
        )
        atomic_write_json(checkpoint_path, checkpoint)
        return checkpoint_path, checkpoint

    with checkpoint_path.open("r", encoding="utf-8") as file:
        checkpoint = json.load(file)

    if checkpoint.get("source_sha256") != source_sha256:
        raise ValueError(
            "Checkpoint belongs to a different source checksum."
        )

    if int(checkpoint.get("batch_size", -1)) != batch_size:
        raise ValueError(
            "Configured batch size differs from existing checkpoint."
        )

    if int(checkpoint.get("total_rows", -1)) != total_rows:
        raise ValueError(
            "Checkpoint row count differs from source."
        )

    return checkpoint_path, checkpoint


def write_batch_file(
    header: list[str],
    rows: list[list[str]],
    output_path: Path,
) -> tuple[str, str]:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = output_path.with_name(
        output_path.name + ".tmp"
    )

    with temporary_path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as file:
        writer = csv.writer(file, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
        file.flush()
        os.fsync(file.fileno())

    intended_sha256 = sha256_file(temporary_path)

    if output_path.exists():
        existing_sha256 = sha256_file(output_path)

        if existing_sha256 != intended_sha256:
            temporary_path.unlink(missing_ok=True)
            raise ValueError(
                f"Existing batch differs from expected batch: "
                f"{output_path}"
            )

        temporary_path.unlink(missing_ok=True)

        return "RECOVERED_EXISTING", existing_sha256

    os.replace(temporary_path, output_path)

    return "SUCCESS", intended_sha256


def ingest_incremental(
    max_batches: int | None = 1,
    ingest_all: bool = False,
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    settings = settings or load_settings()
    verification = verify_source_integrity(settings)

    source_path = get_configured_path(settings, "source_file")
    raw_dir = get_configured_path(settings, "raw_dir")

    incremental_dir = (
        raw_dir / settings["ingestion"]["incremental_dirname"]
    )
    incremental_dir.mkdir(parents=True, exist_ok=True)

    batch_size = int(settings["ingestion"]["batch_size"])

    if batch_size <= 0:
        raise ValueError("Batch size must be greater than zero.")

    header, rows = inspect_csv(source_path)

    checkpoint_path, checkpoint = load_or_create_checkpoint(
        settings,
        verification["sha256"],
        len(rows),
        batch_size,
    )

    total_batches = int(checkpoint["total_batches"])
    last_completed = int(checkpoint["last_completed_batch"])

    if last_completed >= total_batches:
        append_audit(
            settings,
            {
                "pipeline_run_id": f"incremental-{uuid.uuid4().hex[:12]}",
                "ingested_at_utc": utc_now(),
                "mode": "incremental",
                "status": "SKIPPED",
                "source_file": source_path.name,
                "source_sha256": verification["sha256"],
                "rows_processed": 0,
                "message": "All batches already ingested.",
            },
        )

        return {
            "status": "SKIPPED",
            "mode": "incremental",
            "message": "No remaining batches.",
            "last_completed_batch": last_completed,
            "total_batches": total_batches,
            "rows_processed": 0,
        }

    remaining = total_batches - last_completed

    if ingest_all:
        number_to_process = remaining
    else:
        if max_batches is None or max_batches <= 0:
            raise ValueError(
                "max_batches must be greater than zero."
            )

        number_to_process = min(max_batches, remaining)

    run_id = f"incremental-{uuid.uuid4().hex[:12]}"
    processed_rows = 0
    processed_batches = []

    start_batch = last_completed + 1
    end_batch = last_completed + number_to_process

    for batch_number in range(start_batch, end_batch + 1):
        start_index = (batch_number - 1) * batch_size
        end_index = min(
            start_index + batch_size,
            len(rows),
        )

        batch_rows = rows[start_index:end_index]

        sequence_start = start_index + 1
        sequence_end = end_index

        output_path = (
            incremental_dir
            / f"batch_{batch_number:04d}.csv"
        )

        batch_status, batch_sha256 = write_batch_file(
            header,
            batch_rows,
            output_path,
        )

        checkpoint["completed_batches"][str(batch_number)] = {
            "sequence_start": sequence_start,
            "sequence_end": sequence_end,
            "rows": len(batch_rows),
            "sha256": batch_sha256,
            "file": (
                f"data/raw/incremental/"
                f"batch_{batch_number:04d}.csv"
            ),
        }

        checkpoint["last_completed_batch"] = batch_number

        atomic_write_json(
            checkpoint_path,
            checkpoint,
        )

        append_audit(
            settings,
            {
                "pipeline_run_id": run_id,
                "ingested_at_utc": utc_now(),
                "mode": "incremental",
                "status": batch_status,
                "source_file": source_path.name,
                "source_sha256": verification["sha256"],
                "sequence_start": sequence_start,
                "sequence_end": sequence_end,
                "rows_processed": len(batch_rows),
                "batch_number": batch_number,
                "output_file": str(output_path),
                "output_sha256": batch_sha256,
                "message": "Incremental batch completed.",
            },
        )

        processed_rows += len(batch_rows)
        processed_batches.append(batch_number)

    return {
        "status": "SUCCESS",
        "mode": "incremental",
        "pipeline_run_id": run_id,
        "processed_batches": processed_batches,
        "rows_processed": processed_rows,
        "last_completed_batch": checkpoint[
            "last_completed_batch"
        ],
        "total_batches": total_batches,
        "checkpoint": str(checkpoint_path),
    }