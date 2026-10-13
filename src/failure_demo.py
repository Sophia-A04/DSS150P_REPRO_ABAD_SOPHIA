"""Controlled data-quality failure demonstrations.

These helpers create temporary corrupted copies of the ingested raw
dataset. Canonical source and raw data are never modified.
"""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
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


class ControlledDataQualityFailure(ValueError):
    """Expected failure raised by a controlled validation demonstration."""


def get_full_raw_path(
    settings: dict[str, Any],
) -> Path:
    """Return the configured full raw snapshot."""
    raw_dir = get_configured_path(
        settings,
        "raw_dir",
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


def run_contract_failure_demo(
    scenario: str = "invalid-binary",
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Demonstrate a genuine contract failure on a temporary raw copy.

    Supported scenarios:
    - invalid-binary: sets hypertension=7 in one otherwise eligible row.
    - missing-column: removes the hypertension column.

    A correctly working demonstration intentionally raises
    ControlledDataQualityFailure so Airflow records a real failed task.
    """
    settings = (
        settings
        or load_settings()
    )

    raw_file = get_full_raw_path(
        settings
    )

    if not raw_file.exists():
        raise FileNotFoundError(
            "Full raw snapshot is missing: "
            f"{raw_file}"
        )

    scenario = (
        scenario
        .strip()
        .lower()
    )

    if scenario not in {
        "invalid-binary",
        "missing-column",
    }:
        raise ValueError(
            "scenario must be "
            "'invalid-binary' or "
            "'missing-column'."
        )

    raw = pd.read_csv(
        raw_file
    )

    if raw.empty:
        raise ValueError(
            "Raw dataset is empty."
        )

    corrupted = raw.copy()

    if (
        scenario
        == "invalid-binary"
    ):
        eligible = (
            corrupted[
                "age"
            ]
            .between(
                35,
                90,
                inclusive="both",
            )
        )

        if not eligible.any():
            raise ValueError(
                "No eligible row is available "
                "for failure demonstration."
            )

        row_index = (
            corrupted[
                eligible
            ]
            .index[0]
        )

        record_id = int(
            corrupted.loc[
                row_index,
                "id",
            ]
        )

        corrupted.loc[
            row_index,
            "hypertension",
        ] = 7

        mutation = {
            "record_id": record_id,
            "column": "hypertension",
            "original_rule": "allowed_values=[0, 1]",
            "injected_value": 7,
        }

    else:
        corrupted = (
            corrupted.drop(
                columns=[
                    "hypertension",
                ]
            )
        )

        mutation = {
            "removed_column": (
                "hypertension"
            ),
        }

    contract = (
        load_data_contract()
    )

    with TemporaryDirectory(
        prefix="dss150p_contract_failure_"
    ) as temp_directory:
        temporary_file = (
            Path(
                temp_directory
            )
            / "corrupted_raw.csv"
        )

        corrupted.to_csv(
            temporary_file,
            index=False,
        )

        demonstration_data = (
            pd.read_csv(
                temporary_file
            )
        )

        try:
            reasons = (
                contract_row_violations(
                    demonstration_data,
                    contract,
                )
            )

        except ValueError as exc:
            raise ControlledDataQualityFailure(
                "Controlled data-quality failure "
                "successfully detected. "
                f"Scenario={scenario}. "
                f"Mutation={mutation}. "
                f"Contract error: {exc}"
            ) from exc

        summary = (
            summarize_contract_violations(
                reasons
            )
        )

        if (
            summary[
                "invalid_rows"
            ]
            < 1
        ):
            raise RuntimeError(
                "Failure demonstration did not "
                "produce a contract violation."
            )

        raise ControlledDataQualityFailure(
            "Controlled data-quality failure "
            "successfully detected. "
            f"Scenario={scenario}. "
            f"Mutation={mutation}. "
            "Invalid rows="
            f"{summary['invalid_rows']}. "
            "Reasons="
            f"{summary['reasons']}."
        )