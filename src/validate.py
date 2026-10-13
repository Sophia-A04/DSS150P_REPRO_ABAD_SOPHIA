"""Environment, raw-layer, and data-quality validation."""

from __future__ import annotations

from importlib.metadata import (
    PackageNotFoundError,
    version,
)
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import (
    get_configured_path,
    load_settings,
)
from src.contract import (
    contract_row_violations,
    load_data_contract,
    summarize_contract_violations,
)
from src.ingest import (
    sha256_file,
    verify_source_integrity,
)
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

    for (
        package,
        expected_version,
    ) in REQUIRED_PACKAGES.items():
        try:
            actual_version = version(
                package
            )

            matches = (
                actual_version
                == expected_version
            )

            packages[
                package
            ] = {
                "expected": (
                    expected_version
                ),
                "actual": (
                    actual_version
                ),
                "pass": bool(
                    matches
                ),
            }

            if not matches:
                failures.append(
                    package
                )

        except PackageNotFoundError:
            packages[
                package
            ] = {
                "expected": (
                    expected_version
                ),
                "actual": None,
                "pass": False,
            }

            failures.append(
                package
            )

    if failures:
        raise ValueError(
            "Package validation failed: "
            + ", ".join(
                failures
            )
        )

    return {
        "status": "PASS",
        "packages": (
            packages
        ),
    }


def validate_required_files() -> dict[str, Any]:
    """
    Verify critical repository files exist and are non-empty.

    This intentionally checks the external SQL and contract modules so
    placeholder zero-byte files cannot pass validation.
    """
    root = (
        Path(__file__)
        .resolve()
        .parents[1]
    )

    required_files = [
        "README.md",
        "requirements.txt",
        "Dockerfile",
        "Dockerfile.airflow",
        "docker-compose.yml",
        "config/settings.yml",
        "metadata/source_manifest.yml",
        "metadata/data_contract.yml",
        "metadata/expected_metrics.json",
        (
            "data/source/"
            "Stroke_Prediction_Dataset.csv"
        ),
        "sql/schema.sql",
        "sql/transformations.sql",
        "src/config.py",
        "src/contract.py",
        "src/ingest.py",
        "src/transform.py",
        "src/storage.py",
        "src/train.py",
        "src/evaluate.py",
        "src/validate.py",
        "src/cli.py",
        "tests/test_unit.py",
        "tests/test_integration.py",
        (
            "dags/"
            "predictive_repro_pipeline.py"
        ),
    ]

    results: dict[
        str,
        bool,
    ] = {}

    for relative_path in (
        required_files
    ):
        path = (
            root
            / relative_path
        )

        results[
            relative_path
        ] = bool(
            path.exists()
            and path.is_file()
            and path.stat().st_size > 0
        )

    missing_or_empty = [
        relative_path
        for (
            relative_path,
            valid,
        ) in results.items()
        if not valid
    ]

    if missing_or_empty:
        raise FileNotFoundError(
            "Missing or empty required "
            "project files: "
            + ", ".join(
                missing_or_empty
            )
        )

    return {
        "status": "PASS",
        "files_checked": len(
            required_files
        ),
        "files": results,
    }


def raw_snapshot_path(
    settings: dict[str, Any],
) -> Path:
    """Return the configured full raw-snapshot path."""
    raw_dir = (
        get_configured_path(
            settings,
            "raw_dir",
        )
    )

    return (
        raw_dir
        / settings[
            "ingestion"
        ][
            "full_dirname"
        ]
        / settings[
            "ingestion"
        ][
            "full_filename"
        ]
    )


def validate_raw_layer(
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Validate the actual ingested raw snapshot.

    Structural contract violations fail immediately inside
    contract_row_violations(). Row-level contract violations are
    reported as QUARANTINE_REQUIRED so staging can preserve them in the
    quarantine layer instead of silently allowing them into modeling.
    """
    settings = (
        settings
        or load_settings()
    )

    source_result = (
        verify_source_integrity(
            settings
        )
    )

    raw_file = (
        raw_snapshot_path(
            settings
        )
    )

    if not raw_file.exists():
        raise FileNotFoundError(
            "Full raw snapshot is missing: "
            f"{raw_file}"
        )

    raw_sha256 = (
        sha256_file(
            raw_file
        )
    )

    source_sha256 = (
        source_result[
            "sha256"
        ]
    )

    if (
        raw_sha256
        != source_sha256
    ):
        raise ValueError(
            "Raw snapshot checksum does not "
            "match the verified source.\n"
            f"Source: {source_sha256}\n"
            f"Raw:    {raw_sha256}"
        )

    raw = pd.read_csv(
        raw_file
    )

    contract = (
        load_data_contract()
    )

    reasons = (
        contract_row_violations(
            raw,
            contract,
        )
    )

    summary = (
        summarize_contract_violations(
            reasons
        )
    )

    invalid_rows = int(
        summary[
            "invalid_rows"
        ]
    )

    status = (
        "PASS"
        if invalid_rows == 0
        else "QUARANTINE_REQUIRED"
    )

    return {
        "status": status,
        "raw_file": str(
            raw_file
        ),
        "rows": int(
            len(
                raw
            )
        ),
        "columns": int(
            len(
                raw.columns
            )
        ),
        "sha256": (
            raw_sha256
        ),
        "source_sha256": (
            source_sha256
        ),
        "checksum_matches_source": (
            raw_sha256
            == source_sha256
        ),
        "contract_invalid_rows": (
            invalid_rows
        ),
        "contract_valid_rows": int(
            summary[
                "valid_rows"
            ]
        ),
        "contract_violation_reasons": (
            summary[
                "reasons"
            ]
        ),
        "requires_quarantine": bool(
            invalid_rows > 0
        ),
    }


def validate_database() -> dict[str, Any]:
    """Verify PostgreSQL connectivity and curated table state."""
    with postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1;"
            )

            connectivity = (
                cursor.fetchone()[0]
            )

            cursor.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT id)
                FROM curated.stroke_model_ready;
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

    if connectivity != 1:
        raise ValueError(
            "PostgreSQL connectivity "
            "check failed."
        )

    if total_rows != 3265:
        raise ValueError(
            "Unexpected PostgreSQL "
            f"row count: {total_rows}"
        )

    if distinct_ids != 3265:
        raise ValueError(
            "Unexpected PostgreSQL "
            "distinct ID count: "
            f"{distinct_ids}"
        )

    return {
        "status": "PASS",
        "connectivity": True,
        "table_rows": (
            total_rows
        ),
        "distinct_ids": (
            distinct_ids
        ),
    }


def validate_data_quality(
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute staging, quarantine, and curated assertions."""
    settings = (
        settings
        or load_settings()
    )

    staging_path = (
        get_configured_path(
            settings,
            "staging_file",
        )
    )

    curated_path = (
        get_configured_path(
            settings,
            "curated_file",
        )
    )

    quarantine_path = (
        get_configured_path(
            settings,
            "quarantine_file",
        )
    )

    if not staging_path.exists():
        raise FileNotFoundError(
            "Staging dataset is missing."
        )

    if not curated_path.exists():
        raise FileNotFoundError(
            "Curated dataset is missing."
        )

    if not quarantine_path.exists():
        raise FileNotFoundError(
            "Quarantine dataset is missing."
        )

    staging = pd.read_csv(
        staging_path
    )

    curated = pd.read_csv(
        curated_path
    )

    quarantine = pd.read_csv(
        quarantine_path
    )

    minimum_age = int(
        settings[
            "transformation"
        ][
            "minimum_age"
        ]
    )

    maximum_age = int(
        settings[
            "transformation"
        ][
            "maximum_age"
        ]
    )

    expected_staging_rows = int(
        settings[
            "transformation"
        ][
            "expected_staging_rows"
        ]
    )

    quarantine_reason_present = bool(
        "rejection_reason"
        in quarantine.columns
        and quarantine[
            "rejection_reason"
        ]
        .notna()
        .all()
        and quarantine[
            "rejection_reason"
        ]
        .astype(str)
        .str.strip()
        .ne("")
        .all()
    )

    assertions = {
        "staging_expected_row_count": bool(
            len(
                staging
            )
            == expected_staging_rows
        ),

        "staging_unique_ids": bool(
            staging[
                "id"
            ]
            .nunique()
            == len(
                staging
            )
        ),

        "staging_no_missing_bmi": bool(
            staging[
                "bmi"
            ]
            .isna()
            .sum()
            == 0
        ),

        "staging_age_in_modeling_scope": bool(
            staging[
                "age"
            ]
            .between(
                minimum_age,
                maximum_age,
                inclusive="both",
            )
            .all()
        ),

        "staging_hypertension_binary": bool(
            staging[
                "hypertension"
            ]
            .isin(
                [
                    0,
                    1,
                ]
            )
            .all()
        ),

        "staging_heart_disease_binary": bool(
            staging[
                "heart_disease"
            ]
            .isin(
                [
                    0,
                    1,
                ]
            )
            .all()
        ),

        "staging_target_binary": bool(
            staging[
                "stroke"
            ]
            .isin(
                [
                    0,
                    1,
                ]
            )
            .all()
        ),

        "staging_glucose_nonnegative": bool(
            (
                staging[
                    "avg_glucose_level"
                ]
                >= 0
            )
            .all()
        ),

        "staging_bmi_nonnegative": bool(
            (
                staging[
                    "bmi"
                ]
                >= 0
            )
            .all()
        ),

        "quarantine_has_rejection_reasons": (
            quarantine_reason_present
        ),

        "row_conservation": bool(
            len(
                staging
            )
            + len(
                quarantine
            )
            == 5110
        ),

        "curated_row_count_3265": bool(
            len(
                curated
            )
            == 3265
        ),

        "curated_unique_ids": bool(
            curated[
                "id"
            ]
            .nunique()
            == len(
                curated
            )
        ),

        "curated_no_nulls": bool(
            curated
            .isna()
            .sum()
            .sum()
            == 0
        ),

        "curated_target_binary": bool(
            set(
                curated[
                    "stroke"
                ]
                .unique()
            )
            <= {
                0,
                1,
            }
        ),

        "stroke_class_counts_match": bool(
            int(
                (
                    curated[
                        "stroke"
                    ]
                    == 0
                )
                .sum()
            )
            == 3019
            and int(
                (
                    curated[
                        "stroke"
                    ]
                    == 1
                )
                .sum()
            )
            == 246
        ),

        "age_partition_domain": bool(
            set(
                curated[
                    "age_score_risk"
                ]
                .unique()
            )
            == {
                0,
                1,
            }
        ),
    }

    failed = [
        name
        for (
            name,
            passed,
        ) in assertions.items()
        if not passed
    ]

    if failed:
        raise ValueError(
            "Data-quality assertions failed: "
            + ", ".join(
                failed
            )
        )

    return {
        "status": "PASS",
        "assertions_passed": int(
            len(
                assertions
            )
        ),
        "staging_rows": int(
            len(
                staging
            )
        ),
        "quarantine_rows": int(
            len(
                quarantine
            )
        ),
        "curated_rows": int(
            len(
                curated
            )
        ),
        "assertions": (
            assertions
        ),
    }


def validate_all(
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run complete reproducibility validation."""
    settings = (
        settings
        or load_settings()
    )

    source = (
        verify_source_integrity(
            settings
        )
    )

    raw = (
        validate_raw_layer(
            settings
        )
    )

    # A full reproducibility validation must use a clean raw dataset.
    # Row-level violations may be quarantined by staging, but the
    # baseline reproduction itself is not considered clean/passable.
    if (
        raw[
            "status"
        ]
        != "PASS"
    ):
        raise ValueError(
            "Raw data contains contract "
            "violations requiring quarantine: "
            f"{raw['contract_invalid_rows']} row(s)."
        )

    packages = (
        validate_required_packages()
    )

    files = (
        validate_required_files()
    )

    quality = (
        validate_data_quality(
            settings
        )
    )

    storage = (
        verify_storage_equivalence()
    )

    database = (
        validate_database()
    )

    return {
        "status": "PASS",
        "source": source,
        "raw": raw,
        "packages": packages,
        "files": files,
        "data_quality": quality,
        "storage": storage,
        "database": database,
    }