"""Deterministic model training for predictive reproducibility.

This module reproduces the Baseline Logistic Regression experiment from
the original DSS141P predictive analytics project.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.config import get_configured_path, load_settings


RANDOM_STATE = 42
TEST_SIZE = 0.20
TARGET = "stroke"

BASELINE_FEATURES = [
    "gender",
    "age",
    "hypertension",
    "heart_disease",
    "Residence_type",
    "avg_glucose_level",
    "bmi",
    "smoking_status",
]


def sha256_text(value: str) -> str:
    """Return SHA-256 for a UTF-8 string."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def prepare_features(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series]:
    """Reproduce original baseline feature preparation."""
    missing = set(BASELINE_FEATURES + [TARGET]).difference(df.columns)

    if missing:
        raise ValueError(
            "Missing required model columns: "
            + ", ".join(sorted(missing))
        )

    X = df[BASELINE_FEATURES].copy()
    y = df[TARGET].astype(int).copy()

    # Exact encoding approach used in the original notebook.
    X_encoded = pd.get_dummies(
        X,
        drop_first=True,
        dtype=int,
    )

    return X_encoded, y


def create_split(
    df: pd.DataFrame,
    y: pd.Series,
) -> tuple[pd.Index, pd.Index]:
    """Create the original deterministic stratified split."""
    train_idx, test_idx = train_test_split(
        df.index,
        test_size=TEST_SIZE,
        random_state=RANDOM_STATE,
        stratify=y,
    )

    return train_idx, test_idx


def write_split_manifest(
    df: pd.DataFrame,
    train_idx: pd.Index,
    test_idx: pd.Index,
    path: Path,
) -> dict[str, Any]:
    """Persist deterministic train/test membership."""
    train_ids = (
        df.loc[train_idx, "id"]
        .astype(int)
        .tolist()
    )

    test_ids = (
        df.loc[test_idx, "id"]
        .astype(int)
        .tolist()
    )

    train_id_text = ",".join(map(str, train_ids))
    test_id_text = ",".join(map(str, test_ids))

    payload = {
        "strategy": "stratified_train_test_split",
        "random_state": RANDOM_STATE,
        "test_size": TEST_SIZE,
        "train_rows": len(train_ids),
        "test_rows": len(test_ids),
        "train_id_sha256": sha256_text(train_id_text),
        "test_id_sha256": sha256_text(test_id_text),
        "train_ids": train_ids,
        "test_ids": test_ids,
    }

    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)
        file.write("\n")

    return payload


def train_model(
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Train and persist the reproducible Logistic Regression model."""
    settings = settings or load_settings()

    curated_path = get_configured_path(
        settings,
        "curated_file",
    )

    split_manifest_path = (
        Path(__file__).resolve().parents[1]
        / "metadata"
        / "split_manifest.json"
    )

    model_path = (
        Path(__file__).resolve().parents[1]
        / "outputs"
        / "model"
        / "baseline_logistic_regression.joblib"
    )

    if not curated_path.exists():
        raise FileNotFoundError(
            "Curated dataset not found. Run transform-all first."
        )

    df = pd.read_csv(curated_path)

    if len(df) != 3265:
        raise ValueError(
            f"Expected 3265 curated records, found {len(df)}."
        )

    X, y = prepare_features(df)

    train_idx, test_idx = create_split(df, y)

    X_train = X.loc[train_idx]
    X_test = X.loc[test_idx]

    y_train = y.loc[train_idx]
    y_test = y.loc[test_idx]

    pipeline = Pipeline(
        steps=[
            (
                "scaler",
                StandardScaler(),
            ),
            (
                "model",
                LogisticRegression(
                    random_state=RANDOM_STATE,
                    class_weight="balanced",
                    max_iter=1000,
                ),
            ),
        ]
    )

    pipeline.fit(X_train, y_train)

    model_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    artifact = {
        "pipeline": pipeline,
        "feature_columns": X.columns.tolist(),
        "raw_features": BASELINE_FEATURES,
        "target": TARGET,
        "random_state": RANDOM_STATE,
        "test_size": TEST_SIZE,
        "model_name": "Baseline Logistic Regression",
    }

    joblib.dump(
        artifact,
        model_path,
    )

    split_manifest = write_split_manifest(
        df=df,
        train_idx=train_idx,
        test_idx=test_idx,
        path=split_manifest_path,
    )

    return {
        "status": "PASS",
        "model": "Baseline Logistic Regression",
        "curated_rows": len(df),
        "encoded_features": len(X.columns),
        "train_rows": len(X_train),
        "test_rows": len(X_test),
        "train_stroke_cases": int(y_train.sum()),
        "test_stroke_cases": int(y_test.sum()),
        "model_file": str(model_path),
        "split_manifest": str(split_manifest_path),
        "test_id_sha256": split_manifest[
            "test_id_sha256"
        ],
    }