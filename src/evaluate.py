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
from src.train import prepare_features
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


def evaluate_model(
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Evaluate the regenerated model and compare with historical metrics.

    Evaluation is blocked unless the current raw, staging, quarantine,
    and curated layers all pass validation. This prevents stale model
    outputs from being evaluated after an upstream pipeline failure.
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