"""Unit tests for deterministic pipeline behavior."""

from copy import deepcopy

import pandas as pd

from src.config import load_settings
from src.contract import (
    contract_row_violations,
    load_data_contract,
)
from src.ingest import verify_source_integrity
from src.storage import logical_hash
from src.train import prepare_features
from src.transform import build_staging


def test_source_integrity():
    """Canonical source must match its manifest."""
    result = (
        verify_source_integrity()
    )

    assert (
        result[
            "status"
        ]
        == "PASS"
    )

    assert (
        result[
            "rows"
        ]
        == 5110
    )

    assert (
        result[
            "columns"
        ]
        == 12
    )


def test_logical_hash_ignores_row_order():
    """Logical hashing should be independent of row order."""
    df = pd.read_csv(
        "data/curated/stroke_model_ready.csv"
    )

    shuffled = df.sample(
        frac=1.0,
        random_state=42,
    )

    assert (
        logical_hash(
            df
        )
        == logical_hash(
            shuffled
        )
    )


def test_model_feature_preparation_is_deterministic():
    """Repeated feature preparation must be identical."""
    df = pd.read_csv(
        "data/curated/stroke_model_ready.csv"
    )

    X1, y1 = (
        prepare_features(
            df
        )
    )

    X2, y2 = (
        prepare_features(
            df
        )
    )

    pd.testing.assert_frame_equal(
        X1,
        X2,
    )

    pd.testing.assert_series_equal(
        y1,
        y2,
    )


def test_contract_detects_invalid_binary_values():
    """Invalid clinical binary values must be contract violations."""
    df = pd.read_csv(
        "data/source/Stroke_Prediction_Dataset.csv"
    ).head(
        2
    ).copy()

    df.loc[
        df.index[0],
        "hypertension",
    ] = 7

    df.loc[
        df.index[1],
        "heart_disease",
    ] = -1

    contract = (
        load_data_contract()
    )

    reasons = (
        contract_row_violations(
            df,
            contract,
        )
    )

    assert (
        "hypertension: value_not_allowed"
        in reasons.iloc[0]
    )

    assert (
        "heart_disease: value_not_allowed"
        in reasons.iloc[1]
    )


def test_staging_quarantines_invalid_contract_record(
    tmp_path,
):
    """A contract-invalid record must not reach staging."""
    source = pd.read_csv(
        "data/source/Stroke_Prediction_Dataset.csv"
    )

    eligible_index = (
        source[
            source[
                "age"
            ]
            .between(
                35,
                90,
                inclusive="both",
            )
        ]
        .index[0]
    )

    invalid_id = int(
        source.loc[
            eligible_index,
            "id",
        ]
    )

    source.loc[
        eligible_index,
        "hypertension",
    ] = 7

    raw_dir = (
        tmp_path
        / "raw"
    )

    full_dir = (
        raw_dir
        / "full"
    )

    full_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    source.to_csv(
        full_dir
        / "Stroke_Prediction_Dataset.csv",
        index=False,
    )

    settings = deepcopy(
        load_settings()
    )

    settings[
        "paths"
    ][
        "raw_dir"
    ] = str(
        raw_dir
    )

    settings[
        "paths"
    ][
        "staging_file"
    ] = str(
        tmp_path
        / "staging.csv"
    )

    settings[
        "paths"
    ][
        "quarantine_file"
    ] = str(
        tmp_path
        / "quarantine.csv"
    )

    result = (
        build_staging(
            settings
        )
    )

    staging = pd.read_csv(
        settings[
            "paths"
        ][
            "staging_file"
        ]
    )

    quarantine = pd.read_csv(
        settings[
            "paths"
        ][
            "quarantine_file"
        ]
    )

    assert (
        result[
            "status"
        ]
        == "PASS"
    )

    assert (
        result[
            "contract_invalid_rows"
        ]
        == 1
    )

    assert (
        invalid_id
        not in set(
            staging[
                "id"
            ]
        )
    )

    rejected = (
        quarantine[
            quarantine[
                "id"
            ]
            == invalid_id
        ]
    )

    assert (
        len(
            rejected
        )
        == 1
    )

    assert (
        "hypertension: value_not_allowed"
        in rejected[
            "rejection_reason"
        ]
        .iloc[0]
    )