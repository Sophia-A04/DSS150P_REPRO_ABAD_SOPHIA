"""Integration tests for storage and PostgreSQL rerun behavior."""

from src.storage import (
    create_partitions,
    export_storage_formats,
    load_partition,
    load_postgres,
    verify_storage_equivalence,
)


def test_storage_formats_are_equivalent():
    """CSV, JSONL, and Parquet must contain the same logical row set."""
    export_result = export_storage_formats()
    verify_result = verify_storage_equivalence()

    assert export_result["status"] == "PASS"
    assert verify_result["status"] == "PASS"
    assert verify_result["rows"] == 3265

    hashes = verify_result["hashes"]

    assert hashes["csv"] == hashes["jsonl"]
    assert hashes["csv"] == hashes["parquet"]


def test_postgres_rerun_and_selected_partition_are_idempotent():
    """Full and selected-partition reruns must not duplicate rows."""
    first = load_postgres()
    second = load_postgres()

    assert first["table_rows"] == 3265
    assert second["table_rows"] == 3265
    assert first["distinct_ids"] == 3265
    assert second["distinct_ids"] == 3265

    partitions = create_partitions()

    assert partitions["status"] == "PASS"

    selected = load_partition(
        age_score_risk=1
    )

    assert selected["status"] == "PASS"
    assert selected["partition_rows"] == 1779
    assert selected["table_rows_after"] == 3265