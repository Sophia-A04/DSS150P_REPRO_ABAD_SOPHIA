"""Deterministic staging and curated transformations.

This module reproduces the cleaning and feature-engineering logic used in
the original DSS141P Predictive Analytics project while enforcing the
repository data contract before records enter the modeling pipeline.
"""

from __future__ import annotations

from typing import Any

import numpy as np
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


def _add_reason(
    reasons: pd.Series,
    mask: pd.Series,
    message: str,
) -> None:
    """Append a quarantine reason to selected rows."""
    mask = mask.fillna(False)

    first = (
        mask
        & reasons.eq("")
    )

    additional = (
        mask
        & reasons.ne("")
    )

    reasons.loc[
        first
    ] = message

    reasons.loc[
        additional
    ] = (
        reasons.loc[
            additional
        ]
        + "; "
        + message
    )


def calculate_vascular_age(
    row: pd.Series,
) -> float:
    """Reproduce the original adapted FRS-BMI-HnT vascular-age mapping."""
    age = row["age"]

    is_male = (
        row[
            "gender_score_risk"
        ]
        == 1
    )

    smokes = (
        row[
            "smoking_score_risk"
        ]
        == 3
    )

    has_hypertension = (
        row[
            "hypertension"
        ]
        == 1
    )

    has_diabetes = (
        row[
            "avg_glucose_level"
        ]
        >= 126.0
    )

    points = 0

    if age < 35:
        points += 0
    elif age < 40:
        points += 2
    elif age < 45:
        points += (
            5
            if is_male
            else 4
        )
    elif age < 50:
        points += (
            6
            if is_male
            else 5
        )
    elif age < 55:
        points += (
            8
            if is_male
            else 7
        )
    elif age < 60:
        points += (
            10
            if is_male
            else 8
        )
    elif age < 65:
        points += (
            11
            if is_male
            else 9
        )
    elif age < 70:
        points += (
            12
            if is_male
            else 10
        )
    elif age < 75:
        points += (
            14
            if is_male
            else 11
        )
    else:
        points += (
            15
            if is_male
            else 12
        )

    if smokes:
        points += (
            4
            if is_male
            else 3
        )

    if has_hypertension:
        points += (
            3
            if is_male
            else 4
        )
    else:
        points += (
            0
            if is_male
            else -1
        )

    if has_diabetes:
        points += (
            3
            if is_male
            else 4
        )

    if row["bmi"] >= 25.0:
        points += 2

    if is_male:
        if points <= -1:
            vascular_age = 28
        elif points == 0:
            vascular_age = 30
        elif points <= 2:
            vascular_age = 33
        elif points <= 4:
            vascular_age = 37
        elif points <= 6:
            vascular_age = 41
        elif points <= 8:
            vascular_age = 46
        elif points <= 10:
            vascular_age = 52
        elif points <= 12:
            vascular_age = 58
        elif points <= 14:
            vascular_age = 66
        elif points == 15:
            vascular_age = 72
        elif points == 16:
            vascular_age = 76
        else:
            vascular_age = 82

    else:
        if points <= 1:
            vascular_age = 28
        elif points <= 3:
            vascular_age = 32
        elif points <= 5:
            vascular_age = 38
        elif points <= 7:
            vascular_age = 43
        elif points <= 9:
            vascular_age = 49
        elif points <= 11:
            vascular_age = 57
        elif points <= 13:
            vascular_age = 66
        elif points == 14:
            vascular_age = 76
        else:
            vascular_age = 82

    return float(
        vascular_age
    )


def build_staging(
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Create cleaned staging data with contract-driven quarantine.

    Fatal schema/key problems stop the pipeline. Row-level value
    violations are preserved in quarantine with explicit reasons.
    """
    settings = (
        settings
        or load_settings()
    )

    raw_dir = (
        get_configured_path(
            settings,
            "raw_dir",
        )
    )

    raw_file = (
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

    staging_file = (
        get_configured_path(
            settings,
            "staging_file",
        )
    )

    quarantine_file = (
        get_configured_path(
            settings,
            "quarantine_file",
        )
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

    if not raw_file.exists():
        raise FileNotFoundError(
            "Full raw snapshot does not exist. "
            "Run ingest-full first."
        )

    df = pd.read_csv(
        raw_file
    )

    contract = (
        load_data_contract()
    )

    contract_reasons = (
        contract_row_violations(
            df,
            contract,
        )
    )

    contract_summary = (
        summarize_contract_violations(
            contract_reasons
        )
    )

    rejection_reason = (
        contract_reasons.copy()
    )

    age_numeric = (
        pd.to_numeric(
            df["age"],
            errors="coerce",
        )
    )

    outside_age_scope = (
        age_numeric.notna()
        & ~age_numeric.between(
            minimum_age,
            maximum_age,
            inclusive="both",
        )
    )

    _add_reason(
        rejection_reason,
        outside_age_scope,
        (
            "modeling_scope: "
            f"age_outside_{minimum_age}_{maximum_age}"
        ),
    )

    rejected = (
        rejection_reason
        .ne("")
    )

    quarantine = (
        df.loc[
            rejected
        ]
        .copy()
    )

    quarantine[
        "rejection_reason"
    ] = (
        rejection_reason.loc[
            rejected
        ]
        .values
    )

    staging = (
        df.loc[
            ~rejected
        ]
        .copy()
    )

    # Downstream normalization occurs only after contract enforcement.
    gender_mode = (
        staging[
            "gender"
        ]
        .mode(
            dropna=True
        )
        .iloc[0]
    )

    staging[
        "gender"
    ] = (
        staging[
            "gender"
        ]
        .replace(
            "Other",
            gender_mode,
        )
    )

    children_unknown = (
        (
            staging[
                "work_type"
            ]
            == "children"
        )
        & (
            staging[
                "smoking_status"
            ]
            == "Unknown"
        )
    )

    staging.loc[
        children_unknown,
        "smoking_status",
    ] = "never smoked"

    # Original project calculated median after age/model eligibility.
    bmi_median = float(
        staging[
            "bmi"
        ]
        .median()
    )

    staging[
        "bmi"
    ] = (
        staging[
            "bmi"
        ]
        .fillna(
            bmi_median
        )
    )

    staging = (
        staging
        .reset_index(
            drop=True
        )
    )

    quarantine = (
        quarantine
        .reset_index(
            drop=True
        )
    )

    if (
        len(staging)
        + len(quarantine)
        != len(df)
    ):
        raise ValueError(
            "Staging/quarantine row conservation failed."
        )

    # For the unchanged canonical source, preserve the exact
    # reproducibility target. If true contract violations are present,
    # they are allowed to reduce staging because they were quarantined.
    if (
        contract_summary[
            "invalid_rows"
        ]
        == 0
        and len(staging)
        != expected_staging_rows
    ):
        raise ValueError(
            "Unexpected staging row count: "
            f"{len(staging)}; expected "
            f"{expected_staging_rows}."
        )

    if (
        staging[
            "bmi"
        ]
        .isna()
        .any()
    ):
        raise ValueError(
            "BMI imputation failed."
        )

    if not (
        staging[
            "age"
        ]
        .between(
            minimum_age,
            maximum_age,
            inclusive="both",
        )
        .all()
    ):
        raise ValueError(
            "Staging contains records "
            "outside age scope."
        )

    if (
        staging[
            "gender"
        ]
        .isin(
            [
                "Other",
            ]
        )
        .any()
    ):
        raise ValueError(
            "Gender normalization failed."
        )

    if (
        staging[
            "id"
        ]
        .duplicated()
        .any()
    ):
        raise ValueError(
            "Duplicate IDs detected after staging."
        )

    # No invalid binary clinical values may reach staging.
    if not (
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
    ):
        raise ValueError(
            "Invalid hypertension value "
            "reached staging."
        )

    if not (
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
    ):
        raise ValueError(
            "Invalid heart_disease value "
            "reached staging."
        )

    if not (
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
    ):
        raise ValueError(
            "Invalid stroke value reached staging."
        )

    staging_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    quarantine_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    staging.to_csv(
        staging_file,
        index=False,
    )

    quarantine.to_csv(
        quarantine_file,
        index=False,
    )

    return {
        "status": "PASS",
        "input_rows": int(
            len(df)
        ),
        "staging_rows": int(
            len(staging)
        ),
        "quarantined_rows": int(
            len(quarantine)
        ),
        "contract_invalid_rows": int(
            contract_summary[
                "invalid_rows"
            ]
        ),
        "modeling_scope_excluded_rows": int(
            outside_age_scope.sum()
        ),
        "bmi_median": (
            bmi_median
        ),
        "remaining_missing_bmi": int(
            staging[
                "bmi"
            ]
            .isna()
            .sum()
        ),
        "remaining_duplicate_ids": int(
            staging[
                "id"
            ]
            .duplicated()
            .sum()
        ),
        "staging_file": str(
            staging_file
        ),
        "quarantine_file": str(
            quarantine_file
        ),
        "contract": (
            contract_summary
        ),
    }


def build_curated(
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create the model-ready feature-engineered curated dataset."""
    settings = (
        settings
        or load_settings()
    )

    staging_file = (
        get_configured_path(
            settings,
            "staging_file",
        )
    )

    curated_file = (
        get_configured_path(
            settings,
            "curated_file",
        )
    )

    if not staging_file.exists():
        raise FileNotFoundError(
            "Staging dataset does not exist. "
            "Run transform-staging first."
        )

    df = pd.read_csv(
        staging_file
    )

    # Defense in depth: bad binary clinical values can never
    # silently enter feature engineering.
    for column in [
        "hypertension",
        "heart_disease",
        "stroke",
    ]:
        if not (
            df[
                column
            ]
            .isin(
                [
                    0,
                    1,
                ]
            )
            .all()
        ):
            raise ValueError(
                "Invalid binary value in staging: "
                f"{column}"
            )

    df[
        "gender_score_risk"
    ] = (
        df[
            "gender"
        ]
        .map(
            {
                "Female": 0,
                "Male": 1,
            }
        )
        .astype(int)
    )

    df[
        "age_score_risk"
    ] = (
        pd.cut(
            df[
                "age"
            ],
            bins=[
                34,
                54,
                90,
            ],
            labels=[
                0,
                1,
            ],
        )
        .astype(int)
    )

    df[
        "glucose_score_risk"
    ] = (
        pd.cut(
            df[
                "avg_glucose_level"
            ],
            bins=[
                0,
                140,
                200,
                np.inf,
            ],
            labels=[
                0,
                1,
                2,
            ],
            right=False,
        )
        .astype(int)
    )

    df[
        "bmi_score_risk"
    ] = (
        pd.cut(
            df[
                "bmi"
            ],
            bins=[
                0,
                23,
                np.inf,
            ],
            labels=[
                0,
                1,
            ],
            right=False,
        )
        .astype(int)
    )

    df[
        "smoking_score_risk"
    ] = (
        df[
            "smoking_status"
        ]
        .map(
            {
                "never smoked": 0,
                "Unknown": 1,
                "formerly smoked": 2,
                "smokes": 3,
            }
        )
        .astype(int)
    )

    df[
        "residence_score_risk"
    ] = (
        df[
            "Residence_type"
        ]
        .map(
            {
                "Rural": 1,
                "Urban": 2,
            }
        )
        .astype(int)
    )

    df = df.drop(
        columns=[
            "ever_married",
            "work_type",
        ]
    )

    vascular_age = (
        df.apply(
            calculate_vascular_age,
            axis=1,
        )
    )

    df[
        "VAAF"
    ] = (
        vascular_age
        - df[
            "age"
        ]
    )

    df[
        "TGI"
    ] = (
        df[
            "bmi"
        ]
        * df[
            "avg_glucose_level"
        ]
    )

    if (
        df
        .isna()
        .any()
        .any()
    ):
        columns = (
            df.columns[
                df
                .isna()
                .any()
            ]
            .tolist()
        )

        raise ValueError(
            "Curated dataset contains "
            "unexpected nulls: "
            f"{columns}"
        )

    # Exact baseline row count is intentionally retained here.
    # If new invalid source rows are quarantined, model reproduction
    # must stop rather than silently train on a different population.
    if len(df) != 3265:
        raise ValueError(
            "Unexpected curated row count: "
            f"{len(df)}. "
            "Check quarantine for contract violations."
        )

    if (
        df[
            "id"
        ]
        .duplicated()
        .any()
    ):
        raise ValueError(
            "Duplicate IDs detected in curated data."
        )

    curated_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_csv(
        curated_file,
        index=False,
    )

    return {
        "status": "PASS",
        "rows": int(
            len(df)
        ),
        "columns": int(
            len(
                df.columns
            )
        ),
        "stroke_0": int(
            (
                df[
                    "stroke"
                ]
                == 0
            )
            .sum()
        ),
        "stroke_1": int(
            (
                df[
                    "stroke"
                ]
                == 1
            )
            .sum()
        ),
        "curated_file": str(
            curated_file
        ),
    }


def transform_all(
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run staging and curated transformations."""
    settings = (
        settings
        or load_settings()
    )

    staging_result = (
        build_staging(
            settings
        )
    )

    curated_result = (
        build_curated(
            settings
        )
    )

    return {
        "status": "PASS",
        "staging": (
            staging_result
        ),
        "curated": (
            curated_result
        ),
    }