"""Repeated storage benchmarking with equivalence verification."""

from __future__ import annotations

import json
import platform
import statistics
import time
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from src.config import get_configured_path, load_settings
from src.storage import (
    canonical_dataframe,
    initialize_database,
    logical_hash,
    postgres_connection,
    postgres_dataframe,
    upsert_dataframe,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BENCHMARK_DIR = (
    PROJECT_ROOT
    / "outputs"
    / "benchmark"
)

RESULTS_CSV = (
    PROJECT_ROOT
    / "docs"
    / "benchmark_results.csv"
)

CONTEXT_JSON = (
    PROJECT_ROOT
    / "docs"
    / "benchmark_context.json"
)

REPEATS_DEFAULT = 5


def summarize(
    values: list[float],
) -> dict[str, float]:
    """Return descriptive timing statistics."""
    if not values:
        raise ValueError(
            "Cannot summarize an empty timing list."
        )

    return {
        "median_seconds": float(
            statistics.median(values)
        ),
        "mean_seconds": float(
            statistics.mean(values)
        ),
        "stdev_seconds": float(
            statistics.stdev(values)
            if len(values) > 1
            else 0.0
        ),
        "min_seconds": float(
            min(values)
        ),
        "max_seconds": float(
            max(values)
        ),
    }


def timed_call(
    function: Callable[[], Any],
) -> tuple[float, Any]:
    """Execute a callable and return elapsed seconds and result."""
    start = time.perf_counter()

    result = function()

    elapsed = (
        time.perf_counter()
        - start
    )

    return elapsed, result


def read_csv_file(
    path: Path,
) -> pd.DataFrame:
    """Read CSV benchmark representation."""
    return pd.read_csv(path)


def read_jsonl_file(
    path: Path,
) -> pd.DataFrame:
    """Read JSON Lines benchmark representation."""
    return pd.read_json(
        path,
        orient="records",
        lines=True,
    )


def read_parquet_file(
    path: Path,
) -> pd.DataFrame:
    """Read Parquet benchmark representation."""
    return pd.read_parquet(path)


def benchmark_file_format(
    format_name: str,
    source_df: pd.DataFrame,
    repeats: int,
) -> dict[str, Any]:
    """Benchmark repeated write, full-read and filtered-read operations."""
    BENCHMARK_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    extension_map = {
        "csv": "csv",
        "jsonl": "jsonl",
        "parquet": "parquet",
    }

    if format_name not in extension_map:
        raise ValueError(
            f"Unsupported format: {format_name}"
        )

    path = (
        BENCHMARK_DIR
        / (
            "stroke_model_ready."
            + extension_map[format_name]
        )
    )

    write_times: list[float] = []
    full_read_times: list[float] = []
    filtered_read_times: list[float] = []

    expected_full_hash = logical_hash(
        source_df
    )

    expected_filtered = (
        source_df[
            source_df["stroke"] == 1
        ]
        .copy()
    )

    expected_filtered_hash = logical_hash(
        expected_filtered
    )

    for _ in range(repeats):

        if format_name == "csv":

            def write_operation():
                source_df.to_csv(
                    path,
                    index=False,
                )

            reader = lambda: read_csv_file(
                path
            )

            filtered_reader = lambda: (
                read_csv_file(path)
                .query("stroke == 1")
                .copy()
            )

        elif format_name == "jsonl":

            def write_operation():
                source_df.to_json(
                    path,
                    orient="records",
                    lines=True,
                )

            reader = lambda: read_jsonl_file(
                path
            )

            filtered_reader = lambda: (
                read_jsonl_file(path)
                .query("stroke == 1")
                .copy()
            )

        else:

            def write_operation():
                source_df.to_parquet(
                    path,
                    index=False,
                    compression="snappy",
                )

            reader = lambda: read_parquet_file(
                path
            )

            filtered_reader = lambda: (
                pd.read_parquet(
                    path,
                    filters=[
                        (
                            "stroke",
                            "==",
                            1,
                        )
                    ],
                )
            )

        write_elapsed, _ = timed_call(
            write_operation
        )

        write_times.append(
            write_elapsed
        )

        read_elapsed, full_df = timed_call(
            reader
        )

        full_read_times.append(
            read_elapsed
        )

        if logical_hash(full_df) != expected_full_hash:
            raise ValueError(
                f"{format_name} full row-set hash mismatch."
            )

        filter_elapsed, filtered_df = timed_call(
            filtered_reader
        )

        filtered_read_times.append(
            filter_elapsed
        )

        if len(filtered_df) != 246:
            raise ValueError(
                f"{format_name} filtered row count mismatch: "
                f"{len(filtered_df)}"
            )

        if (
            logical_hash(filtered_df)
            != expected_filtered_hash
        ):
            raise ValueError(
                f"{format_name} filtered row-set hash mismatch."
            )

    return {
        "storage": format_name,
        "rows": len(source_df),
        "filtered_rows": len(
            expected_filtered
        ),
        "size_bytes": path.stat().st_size,
        "full_logical_hash": expected_full_hash,
        "filtered_logical_hash": expected_filtered_hash,
        "write": summarize(
            write_times
        ),
        "full_read": summarize(
            full_read_times
        ),
        "filtered_read": summarize(
            filtered_read_times
        ),
    }


def postgres_relation_size() -> int:
    """Return PostgreSQL total relation size in bytes."""
    with postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT pg_total_relation_size(
                    'curated.stroke_model_ready'
                );
                """
            )

            return int(
                cursor.fetchone()[0]
            )


def postgres_filtered_dataframe() -> pd.DataFrame:
    """Read stroke-positive rows directly from PostgreSQL."""
    query = """
        SELECT
            id,
            gender,
            age,
            hypertension,
            heart_disease,
            "Residence_type",
            avg_glucose_level,
            bmi,
            smoking_status,
            stroke,
            gender_score_risk,
            age_score_risk,
            glucose_score_risk,
            bmi_score_risk,
            smoking_score_risk,
            residence_score_risk,
            "VAAF",
            "TGI"
        FROM curated.stroke_model_ready
        WHERE stroke = 1
        ORDER BY id;
    """

    with postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)

            rows = cursor.fetchall()

            columns = [
                description.name
                for description
                in cursor.description
            ]

    return pd.DataFrame(
        rows,
        columns=columns,
    )


def benchmark_postgres(
    source_df: pd.DataFrame,
    repeats: int,
) -> dict[str, Any]:
    """Benchmark repeated PostgreSQL UPSERTs and queries."""
    initialize_database()

    write_times: list[float] = []
    full_read_times: list[float] = []
    filtered_read_times: list[float] = []

    expected_full_hash = logical_hash(
        source_df
    )

    expected_filtered = (
        source_df[
            source_df["stroke"] == 1
        ]
        .copy()
    )

    expected_filtered_hash = logical_hash(
        expected_filtered
    )

    for _ in range(repeats):

        def database_write():
            with postgres_connection() as connection:
                upsert_dataframe(
                    connection,
                    source_df,
                )

                connection.commit()

        write_elapsed, _ = timed_call(
            database_write
        )

        write_times.append(
            write_elapsed
        )

        read_elapsed, full_df = timed_call(
            postgres_dataframe
        )

        full_read_times.append(
            read_elapsed
        )

        if logical_hash(full_df) != expected_full_hash:
            raise ValueError(
                "PostgreSQL full row-set hash mismatch."
            )

        filter_elapsed, filtered_df = timed_call(
            postgres_filtered_dataframe
        )

        filtered_read_times.append(
            filter_elapsed
        )

        if len(filtered_df) != 246:
            raise ValueError(
                "PostgreSQL filtered row count mismatch: "
                f"{len(filtered_df)}"
            )

        if (
            logical_hash(filtered_df)
            != expected_filtered_hash
        ):
            raise ValueError(
                "PostgreSQL filtered row-set hash mismatch."
            )

    return {
        "storage": "postgresql",
        "rows": len(source_df),
        "filtered_rows": len(
            expected_filtered
        ),
        "size_bytes": postgres_relation_size(),
        "full_logical_hash": expected_full_hash,
        "filtered_logical_hash": expected_filtered_hash,
        "write": summarize(
            write_times
        ),
        "full_read": summarize(
            full_read_times
        ),
        "filtered_read": summarize(
            filtered_read_times
        ),
    }


def flatten_result(
    result: dict[str, Any],
    repeats: int,
) -> dict[str, Any]:
    """Flatten benchmark result for CSV reporting."""
    return {
        "storage": result["storage"],
        "repeats": repeats,
        "rows": result["rows"],
        "filtered_rows": result[
            "filtered_rows"
        ],
        "size_bytes": result[
            "size_bytes"
        ],
        "median_write_s": result[
            "write"
        ]["median_seconds"],
        "stdev_write_s": result[
            "write"
        ]["stdev_seconds"],
        "median_full_read_s": result[
            "full_read"
        ]["median_seconds"],
        "stdev_full_read_s": result[
            "full_read"
        ]["stdev_seconds"],
        "median_filtered_read_s": result[
            "filtered_read"
        ]["median_seconds"],
        "stdev_filtered_read_s": result[
            "filtered_read"
        ]["stdev_seconds"],
        "full_logical_hash": result[
            "full_logical_hash"
        ],
        "filtered_logical_hash": result[
            "filtered_logical_hash"
        ],
    }


def run_benchmark(
    repeats: int = REPEATS_DEFAULT,
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run repeated benchmarks for all required storage representations."""
    if repeats < 2:
        raise ValueError(
            "Benchmark repeats must be at least 2."
        )

    settings = settings or load_settings()

    curated_path = get_configured_path(
        settings,
        "curated_file",
    )

    if not curated_path.exists():
        raise FileNotFoundError(
            "Curated dataset not found."
        )

    source_df = canonical_dataframe(
        pd.read_csv(curated_path)
    )

    if len(source_df) != 3265:
        raise ValueError(
            "Benchmark source must contain 3265 rows."
        )

    results = []

    for format_name in [
        "csv",
        "jsonl",
        "parquet",
    ]:
        results.append(
            benchmark_file_format(
                format_name=format_name,
                source_df=source_df,
                repeats=repeats,
            )
        )

    results.append(
        benchmark_postgres(
            source_df=source_df,
            repeats=repeats,
        )
    )

    full_hashes = {
        result[
            "full_logical_hash"
        ]
        for result in results
    }

    filtered_hashes = {
        result[
            "filtered_logical_hash"
        ]
        for result in results
    }

    if len(full_hashes) != 1:
        raise ValueError(
            "Full logical hashes differ across storage representations."
        )

    if len(filtered_hashes) != 1:
        raise ValueError(
            "Filtered logical hashes differ across storage representations."
        )

    RESULTS_CSV.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    table = pd.DataFrame(
        [
            flatten_result(
                result,
                repeats,
            )
            for result in results
        ]
    )

    table.to_csv(
        RESULTS_CSV,
        index=False,
    )

    context = {
        "status": "PASS",
        "benchmark_repeats": repeats,
        "filter_workload": "stroke == 1",
        "expected_rows": 3265,
        "expected_filtered_rows": 246,
        "full_logical_hash": next(
            iter(full_hashes)
        ),
        "filtered_logical_hash": next(
            iter(filtered_hashes)
        ),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "notes": [
            (
                "Write, full-read, and filtered-read/query "
                "operations were each repeated."
            ),
            (
                "Median and standard deviation are reported "
                "to reduce dependence on one timing observation."
            ),
            (
                "All storage representations were verified "
                "against canonical full-row and filtered-row hashes."
            ),
            (
                "PostgreSQL write timing measures rerun-safe UPSERT "
                "plus transaction commit."
            ),
            (
                "Performance results describe this machine and "
                "environment and are not universal rankings."
            ),
        ],
    }

    with CONTEXT_JSON.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            context,
            file,
            indent=2,
            sort_keys=True,
        )

        file.write("\n")

    return {
        "status": "PASS",
        "repeats": repeats,
        "rows": 3265,
        "filtered_rows": 246,
        "full_equivalence": True,
        "filtered_equivalence": True,
        "results_file": str(
            RESULTS_CSV
        ),
        "context_file": str(
            CONTEXT_JSON
        ),
        "results": [
            flatten_result(
                result,
                repeats,
            )
            for result in results
        ],
    }