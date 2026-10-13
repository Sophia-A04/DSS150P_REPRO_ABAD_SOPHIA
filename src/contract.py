"""Data-contract loading and enforcement utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from src.config import PROJECT_ROOT


DEFAULT_CONTRACT_PATH = (
    PROJECT_ROOT
    / "metadata"
    / "data_contract.yml"
)


def load_data_contract(
    path: Path | str | None = None,
) -> dict[str, Any]:
    """Load and validate the repository data contract."""
    contract_path = (
        Path(path).expanduser().resolve()
        if path is not None
        else DEFAULT_CONTRACT_PATH
    )

    if not contract_path.exists():
        raise FileNotFoundError(
            f"Data contract not found: {contract_path}"
        )

    with contract_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        contract = yaml.safe_load(file)

    if not isinstance(
        contract,
        dict,
    ):
        raise ValueError(
            "Data contract is empty or invalid."
        )

    if "schema" not in contract:
        raise ValueError(
            "Data contract is missing schema."
        )

    if not isinstance(
        contract["schema"],
        dict,
    ):
        raise ValueError(
            "Data contract schema must be a mapping."
        )

    return contract


def _blank_mask(
    series: pd.Series,
) -> pd.Series:
    """Return a mask for null or blank-string values."""
    return series.isna() | series.map(
        lambda value: (
            isinstance(
                value,
                str,
            )
            and value.strip() == ""
        )
    )


def _add_reason(
    reasons: pd.Series,
    mask: pd.Series,
    message: str,
) -> None:
    """Append a rejection reason to selected rows."""
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


def _numeric_series(
    series: pd.Series,
) -> pd.Series:
    """Convert a series to numeric values without mutating source data."""
    return pd.to_numeric(
        series,
        errors="coerce",
    )


def _validate_primary_key(
    df: pd.DataFrame,
    contract: dict[str, Any],
) -> None:
    """Fail on invalid source primary-key structure."""
    primary_key = (
        contract
        .get(
            "dataset",
            {},
        )
        .get(
            "primary_key",
            [],
        )
    )

    if isinstance(
        primary_key,
        str,
    ):
        primary_key = [
            primary_key
        ]

    for column in primary_key:
        if column not in df.columns:
            raise ValueError(
                "Primary-key column is missing: "
                f"{column}"
            )

        series = df[
            column
        ]

        if _blank_mask(
            series
        ).any():
            raise ValueError(
                "Source primary key contains "
                f"null/blank values: {column}"
            )

        rule = (
            contract[
                "schema"
            ]
            .get(
                column,
                {},
            )
        )

        expected_type = rule.get(
            "type"
        )

        if expected_type in {
            "integer",
            "float",
        }:
            numeric = (
                _numeric_series(
                    series
                )
            )

            if numeric.isna().any():
                raise ValueError(
                    "Source primary key contains "
                    "non-numeric values: "
                    f"{column}"
                )

            if (
                expected_type
                == "integer"
            ):
                fractional = (
                    numeric
                    .mod(1)
                    .ne(0)
                )

                if fractional.any():
                    raise ValueError(
                        "Source primary key contains "
                        "non-integer values: "
                        f"{column}"
                    )

        if series.duplicated().any():
            raise ValueError(
                "Duplicate primary-key values "
                f"detected: {column}"
            )


def contract_row_violations(
    df: pd.DataFrame,
    contract: dict[str, Any] | None = None,
) -> pd.Series:
    """
    Return row-level contract violations.

    Missing/unexpected schema and invalid source keys fail
    immediately. Other invalid records receive explicit reasons
    so staging can quarantine them.
    """
    contract = (
        contract
        or load_data_contract()
    )

    schema = contract[
        "schema"
    ]

    expected_columns = set(
        schema.keys()
    )

    actual_columns = set(
        df.columns
    )

    missing_columns = (
        expected_columns
        - actual_columns
    )

    unexpected_columns = (
        actual_columns
        - expected_columns
    )

    if missing_columns:
        raise ValueError(
            "Missing required source columns: "
            + ", ".join(
                sorted(
                    missing_columns
                )
            )
        )

    if unexpected_columns:
        raise ValueError(
            "Unexpected source columns: "
            + ", ".join(
                sorted(
                    unexpected_columns
                )
            )
        )

    _validate_primary_key(
        df,
        contract,
    )

    if df.duplicated().any():
        raise ValueError(
            "Exact duplicate source rows detected."
        )

    reasons = pd.Series(
        "",
        index=df.index,
        dtype="string",
    )

    for column, rule in schema.items():
        series = df[
            column
        ]

        blank = _blank_mask(
            series
        )

        nullable = bool(
            rule.get(
                "nullable",
                True,
            )
        )

        if not nullable:
            _add_reason(
                reasons,
                blank,
                (
                    f"{column}: "
                    "null_or_blank"
                ),
            )

        expected_type = (
            rule.get(
                "type"
            )
        )

        numeric = None

        if expected_type in {
            "integer",
            "float",
        }:
            numeric = (
                _numeric_series(
                    series
                )
            )

            invalid_numeric = (
                ~blank
                & numeric.isna()
            )

            _add_reason(
                reasons,
                invalid_numeric,
                (
                    f"{column}: "
                    "invalid_numeric_type"
                ),
            )

            if (
                expected_type
                == "integer"
            ):
                fractional = (
                    ~blank
                    & numeric.notna()
                    & numeric.mod(1).ne(0)
                )

                _add_reason(
                    reasons,
                    fractional,
                    (
                        f"{column}: "
                        "non_integer_value"
                    ),
                )

        elif (
            expected_type
            == "string"
        ):
            invalid_string = (
                ~blank
                & ~series.map(
                    lambda value: isinstance(
                        value,
                        str,
                    )
                )
            )

            _add_reason(
                reasons,
                invalid_string,
                (
                    f"{column}: "
                    "invalid_string_type"
                ),
            )

        allowed_values = (
            rule.get(
                "allowed_values"
            )
        )

        if allowed_values is not None:
            if expected_type in {
                "integer",
                "float",
            }:
                assert (
                    numeric
                    is not None
                )

                invalid_allowed = (
                    ~blank
                    & numeric.notna()
                    & ~numeric.isin(
                        allowed_values
                    )
                )

            else:
                invalid_allowed = (
                    ~blank
                    & ~series.isin(
                        allowed_values
                    )
                )

            _add_reason(
                reasons,
                invalid_allowed,
                (
                    f"{column}: "
                    "value_not_allowed"
                ),
            )

        minimum = rule.get(
            "minimum"
        )

        if minimum is not None:
            if numeric is None:
                numeric = (
                    _numeric_series(
                        series
                    )
                )

            below_minimum = (
                ~blank
                & numeric.notna()
                & (
                    numeric
                    < float(
                        minimum
                    )
                )
            )

            _add_reason(
                reasons,
                below_minimum,
                (
                    f"{column}: "
                    "below_minimum"
                ),
            )

        maximum = rule.get(
            "maximum"
        )

        if maximum is not None:
            if numeric is None:
                numeric = (
                    _numeric_series(
                        series
                    )
                )

            above_maximum = (
                ~blank
                & numeric.notna()
                & (
                    numeric
                    > float(
                        maximum
                    )
                )
            )

            _add_reason(
                reasons,
                above_maximum,
                (
                    f"{column}: "
                    "above_maximum"
                ),
            )

        if bool(
            rule.get(
                "unique",
                False,
            )
        ):
            duplicates = (
                series
                .duplicated(
                    keep=False
                )
            )

            primary_key = (
                contract
                .get(
                    "dataset",
                    {},
                )
                .get(
                    "primary_key",
                    [],
                )
            )

            if isinstance(
                primary_key,
                str,
            ):
                primary_key = [
                    primary_key
                ]

            if (
                column
                not in primary_key
            ):
                _add_reason(
                    reasons,
                    duplicates,
                    (
                        f"{column}: "
                        "duplicate_value"
                    ),
                )

    return reasons


def summarize_contract_violations(
    reasons: pd.Series,
) -> dict[str, Any]:
    """Summarize contract violations for diagnostics."""
    invalid = reasons.ne("")

    unique_reasons = sorted(
        {
            reason
            for reason
            in reasons.loc[
                invalid
            ].tolist()
            if reason
        }
    )

    return {
        "status": (
            "PASS"
            if not invalid.any()
            else "VIOLATIONS_FOUND"
        ),
        "rows_checked": int(
            len(
                reasons
            )
        ),
        "invalid_rows": int(
            invalid.sum()
        ),
        "valid_rows": int(
            (~invalid).sum()
        ),
        "reasons": (
            unique_reasons
        ),
    }