"""Unit tests for deterministic pipeline behavior."""

import pandas as pd

from src.ingest import verify_source_integrity
from src.storage import logical_hash
from src.train import prepare_features


def test_source_integrity():
    """Canonical source must match its manifest."""
    result = verify_source_integrity()

    assert result["status"] == "PASS"
    assert result["rows"] == 5110
    assert result["columns"] == 12


def test_logical_hash_ignores_row_order():
    """Logical hashing should be independent of row order."""
    df = pd.read_csv(
        "data/curated/stroke_model_ready.csv"
    )

    shuffled = df.sample(
        frac=1.0,
        random_state=42,
    )

    assert logical_hash(df) == logical_hash(shuffled)


def test_model_feature_preparation_is_deterministic():
    """Repeated feature preparation must be identical."""
    df = pd.read_csv(
        "data/curated/stroke_model_ready.csv"
    )

    X1, y1 = prepare_features(df)
    X2, y2 = prepare_features(df)

    pd.testing.assert_frame_equal(X1, X2)
    pd.testing.assert_series_equal(y1, y2)