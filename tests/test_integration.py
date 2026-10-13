"""Integration tests for storage, PostgreSQL, and recovery behavior."""

from src.recovery_demo import (
    run_interrupted_ingestion_demo,
)
from src.storage import (
    create_partitions,
    export_storage_formats,
    load_partition,
    load_postgres,
    verify_storage_equivalence,
)


def test_storage_formats_are_equivalent():
    """CSV, JSONL, and Parquet must contain the same logical row set."""
    export_result = (
        export_storage_formats()
    )

    verify_result = (
        verify_storage_equivalence()
    )

    assert (
        export_result[
            "status"
        ]
        == "PASS"
    )

    assert (
        verify_result[
            "status"
        ]
        == "PASS"
    )

    assert (
        verify_result[
            "rows"
        ]
        == 3265
    )

    hashes = (
        verify_result[
            "hashes"
        ]
    )

    assert (
        hashes[
            "csv"
        ]
        == hashes[
            "jsonl"
        ]
    )

    assert (
        hashes[
            "csv"
        ]
        == hashes[
            "parquet"
        ]
    )


def test_postgres_rerun_and_selected_partition_are_idempotent():
    """Full and selected-partition reruns must not duplicate rows."""
    first = (
        load_postgres()
    )

    second = (
        load_postgres()
    )

    assert (
        first[
            "table_rows"
        ]
        == 3265
    )

    assert (
        second[
            "table_rows"
        ]
        == 3265
    )

    assert (
        first[
            "distinct_ids"
        ]
        == 3265
    )

    assert (
        second[
            "distinct_ids"
        ]
        == 3265
    )

    partitions = (
        create_partitions()
    )

    assert (
        partitions[
            "status"
        ]
        == "PASS"
    )

    selected = (
        load_partition(
            age_score_risk=1
        )
    )

    assert (
        selected[
            "status"
        ]
        == "PASS"
    )

    assert (
        selected[
            "partition_rows"
        ]
        == 1779
    )

    assert (
        selected[
            "table_rows_after"
        ]
        == 3265
    )


def test_incremental_ingestion_recovers_after_interruption(
    tmp_path,
):
    """A written-but-uncheckpointed batch must recover without duplicates."""
    evidence_path = (
        tmp_path
        / "interrupted_run_log.json"
    )

    result = (
        run_interrupted_ingestion_demo(
            output_path=(
                evidence_path
            )
        )
    )

    assert (
        result[
            "status"
        ]
        == "PASS"
    )

    assert (
        result[
            "interrupted_run"
        ][
            "checkpoint_before"
        ]
        == 2
    )

    assert (
        result[
            "interrupted_run"
        ][
            "checkpoint_after"
        ]
        == 2
    )

    assert (
        result[
            "interrupted_run"
        ][
            "batch_3_file_exists"
        ]
        is True
    )

    assert (
        result[
            "final_state"
        ][
            "batch_files"
        ]
        == 11
    )

    assert (
        result[
            "final_state"
        ][
            "rows"
        ]
        == 5110
    )

    assert (
        result[
            "final_state"
        ][
            "unique_ids"
        ]
        == 5110
    )

    assert (
        result[
            "final_state"
        ][
            "duplicate_ids"
        ]
        == 0
    )

    assert (
        evidence_path.exists()
    )