"""Storage, partitioning, equivalence, and PostgreSQL utilities."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg

from src.config import get_configured_path, load_settings


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_ROOT / "sql"
STORAGE_DIR = PROJECT_ROOT / "outputs" / "storage"
PARTITION_DIR = PROJECT_ROOT / "data" / "partitioned"

SCHEMA_SQL_PATH = SQL_DIR / "schema.sql"
TRANSFORMATIONS_SQL_PATH = SQL_DIR / "transformations.sql"

TABLE_COLUMNS = [
    "id",
    "gender",
    "age",
    "hypertension",
    "heart_disease",
    "Residence_type",
    "avg_glucose_level",
    "bmi",
    "smoking_status",
    "stroke",
    "gender_score_risk",
    "age_score_risk",
    "glucose_score_risk",
    "bmi_score_risk",
    "smoking_score_risk",
    "residence_score_risk",
    "VAAF",
    "TGI",
]


def postgres_connection():
    """Create a PostgreSQL connection from environment variables."""
    return psycopg.connect(
        host=os.getenv(
            "POSTGRES_HOST",
            "localhost",
        ),
        port=int(
            os.getenv(
                "POSTGRES_PORT",
                "5432",
            )
        ),
        dbname=os.getenv(
            "POSTGRES_DB",
            "dss150p",
        ),
        user=os.getenv(
            "POSTGRES_USER",
            "dss150p",
        ),
        password=os.getenv(
            "POSTGRES_PASSWORD",
            "change_me",
        ),
    )


def read_sql_file(path: Path) -> str:
    """Read a required SQL file and reject missing or empty files."""
    if not path.exists():
        raise FileNotFoundError(
            f"Required SQL file not found: {path}"
        )

    sql = path.read_text(
        encoding="utf-8"
    ).strip()

    if not sql:
        raise ValueError(
            f"Required SQL file is empty: {path}"
        )

    return sql


def canonical_dataframe(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Normalize a dataframe for deterministic comparisons."""
    missing = set(
        TABLE_COLUMNS
    ).difference(
        df.columns
    )

    if missing:
        raise ValueError(
            "Missing storage columns: "
            + ", ".join(
                sorted(missing)
            )
        )

    frame = df[
        TABLE_COLUMNS
    ].copy()

    frame = (
        frame
        .sort_values("id")
        .reset_index(drop=True)
    )

    integer_columns = [
        "id",
        "hypertension",
        "heart_disease",
        "stroke",
        "gender_score_risk",
        "age_score_risk",
        "glucose_score_risk",
        "bmi_score_risk",
        "smoking_score_risk",
        "residence_score_risk",
    ]

    float_columns = [
        "age",
        "avg_glucose_level",
        "bmi",
        "VAAF",
        "TGI",
    ]

    for column in integer_columns:
        frame[column] = (
            pd.to_numeric(
                frame[column],
                errors="raise",
            )
            .astype("int64")
        )

    for column in float_columns:
        frame[column] = (
            pd.to_numeric(
                frame[column],
                errors="raise",
            )
            .astype(float)
            .round(8)
        )

    return frame


def logical_hash(
    df: pd.DataFrame,
) -> str:
    """Calculate a storage-format-independent logical hash."""
    frame = canonical_dataframe(
        df
    )

    payload = frame.to_csv(
        index=False,
        lineterminator="\n",
        float_format="%.8f",
    )

    return hashlib.sha256(
        payload.encode("utf-8")
    ).hexdigest()


def export_storage_formats(
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Export curated data as CSV, JSONL, and Parquet."""
    settings = (
        settings
        or load_settings()
    )

    curated_path = (
        get_configured_path(
            settings,
            "curated_file",
        )
    )

    if not curated_path.exists():
        raise FileNotFoundError(
            "Curated dataset not found. "
            "Run transform-all first."
        )

    df = canonical_dataframe(
        pd.read_csv(
            curated_path
        )
    )

    STORAGE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    csv_path = (
        STORAGE_DIR
        / "stroke_model_ready.csv"
    )

    jsonl_path = (
        STORAGE_DIR
        / "stroke_model_ready.jsonl"
    )

    parquet_path = (
        STORAGE_DIR
        / "stroke_model_ready.parquet"
    )

    df.to_csv(
        csv_path,
        index=False,
    )

    df.to_json(
        jsonl_path,
        orient="records",
        lines=True,
    )

    df.to_parquet(
        parquet_path,
        index=False,
        compression="snappy",
    )

    return {
        "status": "PASS",
        "rows": len(df),
        "logical_hash": (
            logical_hash(df)
        ),
        "files": {
            "csv": {
                "path": str(
                    csv_path
                ),
                "size_bytes": (
                    csv_path
                    .stat()
                    .st_size
                ),
            },
            "jsonl": {
                "path": str(
                    jsonl_path
                ),
                "size_bytes": (
                    jsonl_path
                    .stat()
                    .st_size
                ),
            },
            "parquet": {
                "path": str(
                    parquet_path
                ),
                "size_bytes": (
                    parquet_path
                    .stat()
                    .st_size
                ),
            },
        },
    }


def verify_storage_equivalence() -> dict[str, Any]:
    """Verify full logical equivalence across storage formats."""
    csv_path = (
        STORAGE_DIR
        / "stroke_model_ready.csv"
    )

    jsonl_path = (
        STORAGE_DIR
        / "stroke_model_ready.jsonl"
    )

    parquet_path = (
        STORAGE_DIR
        / "stroke_model_ready.parquet"
    )

    for path in [
        csv_path,
        jsonl_path,
        parquet_path,
    ]:
        if not path.exists():
            raise FileNotFoundError(
                "Storage representation "
                f"missing: {path}"
            )

    frames = {
        "csv": pd.read_csv(
            csv_path
        ),
        "jsonl": pd.read_json(
            jsonl_path,
            orient="records",
            lines=True,
        ),
        "parquet": pd.read_parquet(
            parquet_path
        ),
    }

    hashes = {
        name: logical_hash(
            frame
        )
        for name, frame
        in frames.items()
    }

    row_counts = {
        name: len(frame)
        for name, frame
        in frames.items()
    }

    if (
        len(
            set(
                row_counts.values()
            )
        )
        != 1
    ):
        raise ValueError(
            "Storage row counts "
            "are not equivalent."
        )

    if (
        len(
            set(
                hashes.values()
            )
        )
        != 1
    ):
        raise ValueError(
            "Storage logical hashes "
            "are not equivalent."
        )

    return {
        "status": "PASS",
        "rows": next(
            iter(
                row_counts.values()
            )
        ),
        "logical_hash": next(
            iter(
                hashes.values()
            )
        ),
        "row_counts": (
            row_counts
        ),
        "hashes": hashes,
    }


def initialize_database() -> None:
    """
    Initialize PostgreSQL objects from sql/schema.sql.

    Database object definitions intentionally live in SQL rather
    than being duplicated inside Python.
    """
    schema_sql = read_sql_file(
        SCHEMA_SQL_PATH
    )

    with postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                schema_sql
            )

        connection.commit()


def python_value(
    value: Any,
) -> Any:
    """Convert numpy scalar values into normal Python values."""
    if hasattr(
        value,
        "item",
    ):
        return value.item()

    return value


def upsert_dataframe(
    connection,
    df: pd.DataFrame,
) -> int:
    """
    Idempotently UPSERT dataframe records into PostgreSQL.

    The SQL statement is loaded from sql/transformations.sql.
    """
    frame = canonical_dataframe(
        df
    )

    upsert_sql = read_sql_file(
        TRANSFORMATIONS_SQL_PATH
    )

    rows = []

    for row in frame.itertuples(
        index=False,
        name=None,
    ):
        rows.append(
            tuple(
                python_value(
                    value
                )
                for value in row
            )
        )

    with connection.cursor() as cursor:
        cursor.executemany(
            upsert_sql,
            rows,
        )

    return len(rows)


def postgres_dataframe() -> pd.DataFrame:
    """Read the full curated PostgreSQL table."""
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
        ORDER BY id;
    """

    with postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query
            )

            rows = (
                cursor.fetchall()
            )

            columns = [
                description.name
                for description
                in cursor.description
            ]

    return pd.DataFrame(
        rows,
        columns=columns,
    )


def load_postgres(
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Load curated data into PostgreSQL using rerun-safe UPSERT."""
    settings = (
        settings
        or load_settings()
    )

    curated_path = (
        get_configured_path(
            settings,
            "curated_file",
        )
    )

    if not curated_path.exists():
        raise FileNotFoundError(
            "Curated dataset does not exist."
        )

    initialize_database()

    source_df = pd.read_csv(
        curated_path
    )

    with postgres_connection() as connection:
        rows_attempted = (
            upsert_dataframe(
                connection,
                source_df,
            )
        )

        connection.commit()

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT id)
                FROM
                    curated.stroke_model_ready;
                """
            )

            (
                total_rows,
                distinct_ids,
            ) = cursor.fetchone()

    total_rows = int(
        total_rows
    )

    distinct_ids = int(
        distinct_ids
    )

    if total_rows != 3265:
        raise ValueError(
            "Unexpected PostgreSQL "
            f"row count: {total_rows}"
        )

    if (
        total_rows
        != distinct_ids
    ):
        raise ValueError(
            "Duplicate patient IDs "
            "detected in PostgreSQL."
        )

    database_df = (
        postgres_dataframe()
    )

    source_hash = logical_hash(
        source_df
    )

    database_hash = logical_hash(
        database_df
    )

    if (
        source_hash
        != database_hash
    ):
        raise ValueError(
            "PostgreSQL row-set differs "
            "from curated source."
        )

    return {
        "status": "PASS",
        "rows_attempted": (
            rows_attempted
        ),
        "table_rows": (
            total_rows
        ),
        "distinct_ids": (
            distinct_ids
        ),
        "logical_hash": (
            database_hash
        ),
    }


def create_partitions(
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create independently loadable age-risk partitions."""
    settings = (
        settings
        or load_settings()
    )

    curated_path = (
        get_configured_path(
            settings,
            "curated_file",
        )
    )

    if not curated_path.exists():
        raise FileNotFoundError(
            "Curated dataset does not exist."
        )

    df = pd.read_csv(
        curated_path
    )

    PARTITION_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    partitions: dict[
        str,
        dict[str, Any],
    ] = {}

    for value, group in df.groupby(
        "age_score_risk"
    ):
        value = int(
            value
        )

        partition_directory = (
            PARTITION_DIR
            / (
                "age_score_risk="
                f"{value}"
            )
        )

        partition_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        partition_file = (
            partition_directory
            / "data.parquet"
        )

        frame = canonical_dataframe(
            group
        )

        frame.to_parquet(
            partition_file,
            index=False,
            compression="snappy",
        )

        partitions[
            str(value)
        ] = {
            "rows": len(
                frame
            ),
            "path": str(
                partition_file
            ),
            "logical_hash": (
                logical_hash(
                    frame
                )
            ),
        }

    if (
        set(
            partitions.keys()
        )
        != {"0", "1"}
    ):
        raise ValueError(
            "Expected partitions "
            "0 and 1."
        )

    return {
        "status": "PASS",
        "partition_column": (
            "age_score_risk"
        ),
        "partitions": (
            partitions
        ),
    }


def load_partition(
    age_score_risk: int,
) -> dict[str, Any]:
    """Load a selected partition independently into PostgreSQL."""
    if (
        age_score_risk
        not in {0, 1}
    ):
        raise ValueError(
            "age_score_risk must "
            "be 0 or 1."
        )

    partition_file = (
        PARTITION_DIR
        / (
            "age_score_risk="
            f"{age_score_risk}"
        )
        / "data.parquet"
    )

    if not partition_file.exists():
        raise FileNotFoundError(
            "Partition not found: "
            f"{partition_file}"
        )

    df = pd.read_parquet(
        partition_file
    )

    if not (
        df[
            "age_score_risk"
        ]
        == age_score_risk
    ).all():
        raise ValueError(
            "Partition contains "
            "unexpected cohort values."
        )

    initialize_database()

    with postgres_connection() as connection:
        loaded_rows = (
            upsert_dataframe(
                connection,
                df,
            )
        )

        connection.commit()

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM
                    curated.stroke_model_ready;
                """
            )

            total_rows = int(
                cursor.fetchone()[0]
            )

            cursor.execute(
                """
                INSERT INTO audit.partition_loads (
                    partition_column,
                    partition_value,
                    source_rows,
                    table_rows_after
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s
                );
                """,
                (
                    "age_score_risk",
                    str(
                        age_score_risk
                    ),
                    loaded_rows,
                    total_rows,
                ),
            )

        connection.commit()

    return {
        "status": "PASS",
        "partition_column": (
            "age_score_risk"
        ),
        "partition_value": (
            age_score_risk
        ),
        "partition_rows": (
            loaded_rows
        ),
        "table_rows_after": (
            total_rows
        ),
    }


def storage_all(
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run all required storage operations."""
    settings = (
        settings
        or load_settings()
    )

    export_result = (
        export_storage_formats(
            settings
        )
    )

    equivalence_result = (
        verify_storage_equivalence()
    )

    partition_result = (
        create_partitions(
            settings
        )
    )

    postgres_result = (
        load_postgres(
            settings
        )
    )

    return {
        "status": "PASS",
        "export": (
            export_result
        ),
        "equivalence": (
            equivalence_result
        ),
        "partitions": (
            partition_result
        ),
        "postgres": (
            postgres_result
        ),
    }