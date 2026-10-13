"""Environment and data-quality validation."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import get_configured_path, load_settings
from src.ingest import verify_source_integrity
from src.storage import (
    postgres_connection,
    verify_storage_equivalence,
)


REQUIRED_PACKAGES = {
    "PyYAML": "6.0.2",
    "pandas": "2.2.3",
    "numpy": "2.0.2",
    "scikit-learn": "1.5.2",
    "joblib": "1.4.2",
    "pyarrow": "17.0.0",
    "psycopg": "3.1.18",
    "pytest": "8.3.5",
}


def validate_required_packages() -> dict[str, Any]:
    """Verify required Python packages are installed."""
    packages: dict[str, Any] = {}
    failures: list[str] = []

    for package, expected_version in REQUIRED_PACKAGES.items():
        try:
            actual_version = version(package)
            matches = actual_version == expected_version

            packages[package] = {
                "expected": expected_version,
                "actual": actual_version,
                "pass": bool(matches),
            }

            if not matches:
                failures.append(package)

        except PackageNotFoundError:
            packages[package] = {
                "expected": expected_version,
                "actual": None,
                "pass": False,
            }

            failures.append(package)

    if failures:
        raise ValueError(
            "Package validation failed: "
            + ", ".join(failures)
        )

    return {
        "status": "PASS",
        "packages": packages,
    }


def validate_required_files() -> dict[str, Any]:
    """Verify critical repository files exist."""
    root = Path(__file__).resolve().parents[1]

    required_files = [
        "README.md",
        "requirements.txt",
        "Dockerfile",
        "docker-compose.yml",
        "config/settings.yml",
        "metadata/source_manifest.yml",
        "metadata/data_contract.yml",
        "metadata/expected_metrics.json",
        "data/source/Stroke_Prediction_Dataset.csv",
        "sql/schema.sql",
        "src/config.py",
        "src/ingest.py",
        "src/transform.py",
        "src/storage.py",
        "src/train.py",
        "src/evaluate.py",
        "src/validate.py",
        "src/cli.py",
    ]

    results = {
        relative_path: bool(
            (root / relative_path).exists()
        )
        for relative_path in required_files
    }

    missing = [
        path
        for path, exists in results.items()
        if not exists
    ]

    if missing:
        raise FileNotFoundError(
            "Missing required project files: "
            + ", ".join(missing)
        )

    return {
        "status": "PASS",
        "files_checked": len(required_files),
        "files": results,
    }


def validate_database() -> dict[str, Any]:
    """Verify PostgreSQL connectivity and curated table state."""
    with postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1;")
            connectivity = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT id)
                FROM curated.stroke_model_ready;
                """
            )

            total_rows, distinct_ids = cursor.fetchone()

    total_rows = int(total_rows)
    distinct_ids = int(distinct_ids)

    if connectivity != 1:
        raise ValueError(
            "PostgreSQL connectivity check failed."
        )

    if total_rows != 3265:
        raise ValueError(
            f"Unexpected PostgreSQL row count: {total_rows}"
        )

    if distinct_ids != 3265:
        raise ValueError(
            "Unexpected PostgreSQL distinct ID count: "
            f"{distinct_ids}"
        )

    return {
        "status": "PASS",
        "connectivity": True,
        "table_rows": total_rows,
        "distinct_ids": distinct_ids,
    }


def validate_data_quality(
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute deterministic staging and curated quality assertions."""
    settings = settings or load_settings()

    staging_path = get_configured_path(
        settings,
        "staging_file",
    )

    curated_path = get_configured_path(
        settings,
        "curated_file",
    )

    if not staging_path.exists():
        raise FileNotFoundError(
            "Staging dataset is missing."
        )

    if not curated_path.exists():
        raise FileNotFoundError(
            "Curated dataset is missing."
        )

    staging = pd.read_csv(staging_path)
    curated = pd.read_csv(curated_path)

    assertions = {
        "staging_row_count_3265": bool(
            len(staging) == 3265
        ),
        "staging_unique_ids": bool(
            staging["id"].nunique() == len(staging)
        ),
        "staging_no_missing_bmi": bool(
            staging["bmi"].isna().sum() == 0
        ),
        "staging_age_between_35_and_90": bool(
            staging["age"]
            .between(
                35,
                90,
                inclusive="both",
            )
            .all()
        ),
        "curated_row_count_3265": bool(
            len(curated) == 3265
        ),
        "curated_unique_ids": bool(
            curated["id"].nunique() == len(curated)
        ),
        "curated_no_nulls": bool(
            curated.isna().sum().sum() == 0
        ),
        "target_binary": bool(
            set(curated["stroke"].unique())
            <= {0, 1}
        ),
        "stroke_class_counts_match": bool(
            int(
                (curated["stroke"] == 0).sum()
            )
            == 3019
            and int(
                (curated["stroke"] == 1).sum()
            )
            == 246
        ),
        "age_partition_domain": bool(
            set(
                curated[
                    "age_score_risk"
                ].unique()
            )
            == {0, 1}
        ),
    }

    failed = [
        name
        for name, passed in assertions.items()
        if not passed
    ]

    if failed:
        raise ValueError(
            "Data-quality assertions failed: "
            + ", ".join(failed)
        )

    return {
        "status": "PASS",
        "assertions_passed": int(
            len(assertions)
        ),
        "assertions": assertions,
    }


def validate_all(
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run complete reproducibility validation."""
    settings = settings or load_settings()

    source = verify_source_integrity(settings)
    packages = validate_required_packages()
    files = validate_required_files()
    quality = validate_data_quality(settings)
    storage = verify_storage_equivalence()
    database = validate_database()

    return {
        "status": "PASS",
        "source": source,
        "packages": packages,
        "files": files,
        "data_quality": quality,
        "storage": storage,
        "database": database,
    }