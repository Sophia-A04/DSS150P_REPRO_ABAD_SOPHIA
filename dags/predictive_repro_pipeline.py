"""Airflow orchestration for DSS150P Lab Activity #4."""

from __future__ import annotations

import json
from datetime import (
    datetime,
    timedelta,
)
from pathlib import Path

from airflow import DAG
from airflow.operators.python import (
    PythonOperator,
)

from src.evaluate import (
    evaluate_model,
)
from src.failure_demo import (
    run_contract_failure_demo,
)
from src.ingest import (
    ingest_full,
    ingest_incremental,
)
from src.observe import (
    dag_failure_callback,
    dag_success_callback,
    task_failure_callback,
    task_success_callback,
)
from src.storage import (
    load_postgres,
)
from src.train import (
    train_model,
)
from src.transform import (
    build_curated,
    build_staging,
)
from src.validate import (
    validate_data_quality,
    validate_raw_layer,
)


PROJECT_ROOT = Path(
    "/opt/airflow/project"
)


def ingest_source(
    **context,
):
    """Run full or incremental ingestion based on DAG parameter."""
    params = (
        context[
            "params"
        ]
    )

    run_mode = (
        params.get(
            "run_mode",
            "full",
        )
    )

    if (
        run_mode
        == "full"
    ):
        return ingest_full()

    if (
        run_mode
        == "incremental"
    ):
        return (
            ingest_incremental(
                ingest_all=True
            )
        )

    raise ValueError(
        "run_mode must be "
        "'full' or 'incremental'."
    )


def validate_raw(
    **context,
):
    """
    Validate the actual raw layer.

    Optional failure scenarios use a temporary corrupted copy so the
    canonical source and raw data remain untouched.
    """
    params = (
        context[
            "params"
        ]
    )

    scenario = str(
        params.get(
            "failure_scenario",
            "none",
        )
    ).strip().lower()

    # Backward compatibility with the earlier demonstration command.
    legacy_injection = bool(
        params.get(
            "inject_failure",
            False,
        )
    )

    if (
        legacy_injection
        and scenario == "none"
    ):
        scenario = (
            "invalid-binary"
        )

    if scenario not in {
        "none",
        "invalid-binary",
        "missing-column",
    }:
        raise ValueError(
            "failure_scenario must be "
            "'none', 'invalid-binary', "
            "or 'missing-column'."
        )

    if (
        scenario
        != "none"
    ):
        return (
            run_contract_failure_demo(
                scenario=(
                    scenario
                )
            )
        )

    result = (
        validate_raw_layer()
    )

    if (
        result[
            "status"
        ]
        != "PASS"
    ):
        raise ValueError(
            "Raw-layer validation "
            "did not pass: "
            f"{result}"
        )

    return result


def transform_staging():
    """Build deterministic staging data."""
    return build_staging()


def build_curated_task():
    """Build deterministic curated data."""
    return build_curated()


def load_postgres_task():
    """Load curated records into PostgreSQL."""
    return load_postgres()


def validate_curated():
    """Execute curated data-quality checks."""
    return (
        validate_data_quality()
    )


def train_model_task():
    """Train deterministic predictive model."""
    return train_model()


def evaluate_model_task():
    """Evaluate regenerated model."""
    return evaluate_model()


def reproducibility_check():
    """Require expected-vs-actual metric comparison to pass."""
    metrics_path = (
        PROJECT_ROOT
        / "outputs"
        / "metrics.json"
    )

    if not (
        metrics_path.exists()
    ):
        raise FileNotFoundError(
            "outputs/metrics.json "
            "was not produced."
        )

    with metrics_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        result = (
            json.load(
                file
            )
        )

    if (
        result.get(
            "status"
        )
        != "PASS"
    ):
        raise ValueError(
            "Model reproducibility "
            "comparison failed."
        )

    return {
        "status": "PASS",
        "metrics_file": str(
            metrics_path
        ),
    }


def publish_outputs():
    """Verify final deliverable artifacts exist."""
    required = [
        (
            PROJECT_ROOT
            / "data"
            / "curated"
            / "stroke_model_ready.csv"
        ),
        (
            PROJECT_ROOT
            / "outputs"
            / "metrics.json"
        ),
        (
            PROJECT_ROOT
            / "outputs"
            / "predictions.csv"
        ),
    ]

    missing = [
        str(
            path
        )
        for path in required
        if not path.exists()
    ]

    if missing:
        raise FileNotFoundError(
            "Missing final outputs: "
            + ", ".join(
                missing
            )
        )

    return {
        "status": "PASS",
        "outputs": [
            str(
                path
            )
            for path in required
        ],
    }


default_args = {
    "owner": (
        "sophia_abad"
    ),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(
        seconds=10
    ),
    "execution_timeout": timedelta(
        minutes=10
    ),
    "on_success_callback": (
        task_success_callback
    ),
    "on_failure_callback": (
        task_failure_callback
    ),
}


with DAG(
    dag_id=(
        "dss150p_predictive_"
        "repro_pipeline"
    ),
    description=(
        "Reproducible stroke predictive "
        "analytics pipeline for DSS150P Lab 4"
    ),
    start_date=datetime(
        2026,
        1,
        1,
    ),
    schedule=None,
    catchup=False,
    default_args=(
        default_args
    ),
    max_active_runs=1,
    max_active_tasks=2,
    dagrun_timeout=timedelta(
        minutes=30
    ),
    params={
        "run_mode": (
            "full"
        ),
        "failure_scenario": (
            "none"
        ),
        # Legacy parameter retained only so old documented
        # commands do not break. When true, it now invokes
        # a genuine invalid-row contract failure.
        "inject_failure": False,
    },
    on_success_callback=(
        dag_success_callback
    ),
    on_failure_callback=(
        dag_failure_callback
    ),
    tags=[
        "dss150p",
        "lab4",
        "reproducibility",
    ],
) as dag:

    ingest_source_task = (
        PythonOperator(
            task_id=(
                "ingest_source"
            ),
            python_callable=(
                ingest_source
            ),
        )
    )

    validate_raw_task = (
        PythonOperator(
            task_id=(
                "validate_raw"
            ),
            python_callable=(
                validate_raw
            ),
        )
    )

    transform_staging_task = (
        PythonOperator(
            task_id=(
                "transform_staging"
            ),
            python_callable=(
                transform_staging
            ),
        )
    )

    build_curated_operator = (
        PythonOperator(
            task_id=(
                "build_curated"
            ),
            python_callable=(
                build_curated_task
            ),
        )
    )

    load_postgres_operator = (
        PythonOperator(
            task_id=(
                "load_postgres"
            ),
            python_callable=(
                load_postgres_task
            ),
        )
    )

    validate_curated_task = (
        PythonOperator(
            task_id=(
                "validate_curated"
            ),
            python_callable=(
                validate_curated
            ),
        )
    )

    train_model_operator = (
        PythonOperator(
            task_id=(
                "train_model"
            ),
            python_callable=(
                train_model_task
            ),
        )
    )

    evaluate_model_operator = (
        PythonOperator(
            task_id=(
                "evaluate_model"
            ),
            python_callable=(
                evaluate_model_task
            ),
        )
    )

    reproducibility_operator = (
        PythonOperator(
            task_id=(
                "reproducibility_check"
            ),
            python_callable=(
                reproducibility_check
            ),
        )
    )

    publish_outputs_operator = (
        PythonOperator(
            task_id=(
                "publish_outputs"
            ),
            python_callable=(
                publish_outputs
            ),
        )
    )

    (
        ingest_source_task
        >> validate_raw_task
        >> transform_staging_task
        >> build_curated_operator
        >> load_postgres_operator
        >> validate_curated_task
        >> train_model_operator
        >> evaluate_model_operator
        >> reproducibility_operator
        >> publish_outputs_operator
    )