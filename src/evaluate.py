"""Evaluation and reproducibility comparison."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

from src.config import (
    get_configured_path,
    load_settings,
)
from src.train import (
    prepare_features,
    sha256_text,
)
from src.validate import (
    validate_data_quality,
    validate_raw_layer,
)


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)


def load_json(
    path: Path,
) -> dict[str, Any]:
    """Load a JSON document."""
    with path.open(
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(
            file
        )


def validate_split_manifest(
    df: pd.DataFrame,
    split_manifest: dict[str, Any],
    expected_document: dict[str, Any],
) -> dict[str, Any]:
    """
    Verify deterministic train/test membership.

    The split must:
    - contain the expected number of train and test IDs;
    - contain no duplicates within either split;
    - contain no train/test overlap;
    - cover the complete curated dataset exactly; and
    - match the frozen canonical split hashes.
    """
    if (
        "expected_split"
        not in expected_document
    ):
        raise ValueError(
            "Expected split metadata is missing "
            "from expected_metrics.json."
        )

    expected_split = (
        expected_document[
            "expected_split"
        ]
    )

    train_ids = [
        int(value)
        for value in (
            split_manifest[
                "train_ids"
            ]
        )
    ]

    test_ids = [
        int(value)
        for value in (
            split_manifest[
                "test_ids"
            ]
        )
    ]

    train_set = set(
        train_ids
    )

    test_set = set(
        test_ids
    )

    curated_ids = set(
        df[
            "id"
        ]
        .astype(int)
        .tolist()
    )

    if (
        len(train_ids)
        != len(train_set)
    ):
        raise ValueError(
            "Duplicate IDs detected "
            "inside the training split."
        )

    if (
        len(test_ids)
        != len(test_set)
    ):
        raise ValueError(
            "Duplicate IDs detected "
            "inside the test split."
        )

    overlap = (
        train_set
        & test_set
    )

    if overlap:
        raise ValueError(
            "Train/test leakage detected: "
            f"{len(overlap)} ID(s) appear "
            "in both splits."
        )

    if (
        train_set
        | test_set
    ) != curated_ids:
        raise ValueError(
            "Train/test split does not exactly "
            "cover the curated dataset."
        )

    train_rows = len(
        train_ids
    )

    test_rows = len(
        test_ids
    )

    if (
        train_rows
        != int(
            split_manifest[
                "train_rows"
            ]
        )
    ):
        raise ValueError(
            "Training row count does not match "
            "the split manifest."
        )

    if (
        test_rows
        != int(
            split_manifest[
                "test_rows"
            ]
        )
    ):
        raise ValueError(
            "Test row count does not match "
            "the split manifest."
        )

    if (
        train_rows
        != int(
            expected_split[
                "train_rows"
            ]
        )
    ):
        raise ValueError(
            "Training split row count differs "
            "from the frozen expected split."
        )

    if (
        test_rows
        != int(
            expected_split[
                "test_rows"
            ]
        )
    ):
        raise ValueError(
            "Test split row count differs "
            "from the frozen expected split."
        )

    train_id_text = ",".join(
        map(
            str,
            train_ids,
        )
    )

    test_id_text = ",".join(
        map(
            str,
            test_ids,
        )
    )

    calculated_train_hash = (
        sha256_text(
            train_id_text
        )
    )

    calculated_test_hash = (
        sha256_text(
            test_id_text
        )
    )

    manifest_train_hash = (
        split_manifest[
            "train_id_sha256"
        ]
    )

    manifest_test_hash = (
        split_manifest[
            "test_id_sha256"
        ]
    )

    expected_train_hash = (
        expected_split[
            "train_id_sha256"
        ]
    )

    expected_test_hash = (
        expected_split[
            "test_id_sha256"
        ]
    )

    if (
        calculated_train_hash
        != manifest_train_hash
    ):
        raise ValueError(
            "Training ID hash does not match "
            "the IDs stored in the split manifest."
        )

    if (
        calculated_test_hash
        != manifest_test_hash
    ):
        raise ValueError(
            "Test ID hash does not match "
            "the IDs stored in the split manifest."
        )

    if (
        calculated_train_hash
        != expected_train_hash
    ):
        raise ValueError(
            "Training split differs from "
            "the frozen canonical split."
        )

    if (
        calculated_test_hash
        != expected_test_hash
    ):
        raise ValueError(
            "Test split differs from "
            "the frozen canonical split."
        )

    if (
        split_manifest.get(
            "strategy"
        )
        != expected_split.get(
            "strategy"
        )
    ):
        raise ValueError(
            "Split strategy differs from "
            "the frozen expected configuration."
        )

    if (
        int(
            split_manifest.get(
                "random_state"
            )
        )
        != int(
            expected_split[
                "random_state"
            ]
        )
    ):
        raise ValueError(
            "Split random_state differs from "
            "the frozen expected configuration."
        )

    if (
        float(
            split_manifest.get(
                "test_size"
            )
        )
        != float(
            expected_split[
                "test_size"
            ]
        )
    ):
        raise ValueError(
            "Split test_size differs from "
            "the frozen expected configuration."
        )

    return {
        "status": "PASS",
        "train_rows": (
            train_rows
        ),
        "test_rows": (
            test_rows
        ),
        "overlap_rows": 0,
        "train_id_sha256": (
            calculated_train_hash
        ),
        "test_id_sha256": (
            calculated_test_hash
        ),
    }


def evaluate_model(
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Evaluate the regenerated model and compare with historical metrics.

    Evaluation is blocked unless the current raw, staging, quarantine,
    and curated layers all pass validation. The persisted train/test
    membership must also match the frozen canonical split.
    """
    settings = (
        settings
        or load_settings()
    )

    raw_validation = (
        validate_raw_layer(
            settings
        )
    )

    if (
        raw_validation[
            "status"
        ]
        != "PASS"
    ):
        raise ValueError(
            "Evaluation blocked because "
            "the current raw layer "
            "does not pass validation."
        )

    validate_data_quality(
        settings
    )

    curated_path = (
        get_configured_path(
            settings,
            "curated_file",
        )
    )

    model_path = (
        PROJECT_ROOT
        / "outputs"
        / "model"
        / "baseline_logistic_regression.joblib"
    )

    split_manifest_path = (
        PROJECT_ROOT
        / "metadata"
        / "split_manifest.json"
    )

    expected_path = (
        PROJECT_ROOT
        / "metadata"
        / "expected_metrics.json"
    )

    metrics_path = (
        PROJECT_ROOT
        / "outputs"
        / "metrics.json"
    )

    predictions_path = (
        PROJECT_ROOT
        / "outputs"
        / "predictions.csv"
    )

    required_paths = [
        curated_path,
        model_path,
        split_manifest_path,
        expected_path,
    ]

    for path in (
        required_paths
    ):
        if not path.exists():
            raise FileNotFoundError(
                "Required evaluation artifact "
                f"missing: {path}"
            )

    df = pd.read_csv(
        curated_path
    )

    X, y = (
        prepare_features(
            df
        )
    )

    artifact = joblib.load(
        model_path
    )

    pipeline = (
        artifact[
            "pipeline"
        ]
    )

    feature_columns = (
        artifact[
            "feature_columns"
        ]
    )

    if (
        X.columns.tolist()
        != feature_columns
    ):
        raise ValueError(
            "Encoded feature schema differs "
            "from training schema."
        )

    split_manifest = (
        load_json(
            split_manifest_path
        )
    )

    expected_document = (
        load_json(
            expected_path
        )
    )

    split_validation = (
        validate_split_manifest(
            df,
            split_manifest,
            expected_document,
        )
    )

    test_ids = set(
        split_manifest[
            "test_ids"
        ]
    )

    test_mask = (
        df[
            "id"
        ]
        .astype(int)
        .isin(
            test_ids
        )
    )

    X_test = X.loc[
        test_mask
    ]

    y_test = y.loc[
        test_mask
    ]

    ids_test = (
        df.loc[
            test_mask,
            "id",
        ]
        .astype(int)
    )

    if (
        len(
            X_test
        )
        != split_manifest[
            "test_rows"
        ]
    ):
        raise ValueError(
            "Test split row count does not "
            "match split manifest."
        )

    probabilities = (
        pipeline
        .predict_proba(
            X_test
        )[:, 1]
    )

    predictions = (
        pipeline
        .predict(
            X_test
        )
    )

    cm = confusion_matrix(
        y_test,
        predictions,
        labels=[
            0,
            1,
        ],
    )

    (
        tn,
        fp,
        fn,
        tp,
    ) = (
        cm.ravel()
    )

    metrics = {
        "model_name": (
            artifact[
                "model_name"
            ]
        ),
        "rows_evaluated": (
            int(
                len(
                    y_test
                )
            )
        ),
        "accuracy": (
            float(
                accuracy_score(
                    y_test,
                    predictions,
                )
            )
        ),
        "precision": (
            float(
                precision_score(
                    y_test,
                    predictions,
                    zero_division=0,
                )
            )
        ),
        "recall": (
            float(
                recall_score(
                    y_test,
                    predictions,
                    zero_division=0,
                )
            )
        ),
        "f1": (
            float(
                f1_score(
                    y_test,
                    predictions,
                    zero_division=0,
                )
            )
        ),
        "roc_auc": (
            float(
                roc_auc_score(
                    y_test,
                    probabilities,
                )
            )
        ),
        "confusion_matrix": {
            "true_negative": (
                int(
                    tn
                )
            ),
            "false_positive": (
                int(
                    fp
                )
            ),
            "false_negative": (
                int(
                    fn
                )
            ),
            "true_positive": (
                int(
                    tp
                )
            ),
        },
    }

    predictions_df = (
        pd.DataFrame(
            {
                "id": (
                    ids_test.values
                ),
                "actual_stroke": (
                    y_test.values
                ),
                "predicted_stroke": (
                    predictions
                ),
                "stroke_probability": (
                    probabilities
                ),
            }
        )
        .sort_values(
            "id"
        )
    )

    predictions_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    predictions_df.to_csv(
        predictions_path,
        index=False,
    )

    expected = (
        expected_document[
            "expected_metrics"
        ]
    )

    tolerance = (
        expected_document[
            "tolerance"
        ]
    )

    comparisons: dict[
        str,
        Any,
    ] = {}

    overall_pass = True

    for (
        metric_name,
        expected_value,
    ) in (
        expected.items()
    ):
        actual_value = float(
            metrics[
                metric_name
            ]
        )

        allowed_difference = float(
            tolerance[
                metric_name
            ]
        )

        absolute_difference = abs(
            actual_value
            - float(
                expected_value
            )
        )

        metric_pass = (
            absolute_difference
            <= allowed_difference
        )

        comparisons[
            metric_name
        ] = {
            "expected": (
                float(
                    expected_value
                )
            ),
            "actual": (
                actual_value
            ),
            "absolute_difference": (
                absolute_difference
            ),
            "tolerance": (
                allowed_difference
            ),
            "pass": (
                metric_pass
            ),
        }

        overall_pass = (
            overall_pass
            and metric_pass
        )

    output = {
        "status": (
            "PASS"
            if overall_pass
            else "FAIL"
        ),
        "model": (
            metrics
        ),
        "split_validation": (
            split_validation
        ),
        "reproducibility_comparison": (
            comparisons
        ),
    }

    metrics_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with metrics_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            output,
            file,
            indent=2,
            sort_keys=True,
        )

        file.write("\n")

    return output