"""CLI for the DSS150P reproducibility pipeline."""

from __future__ import annotations

import argparse
import json
import sys

from src.benchmark import run_benchmark
from src.evaluate import evaluate_model
from src.failure_demo import run_contract_failure_demo
from src.ingest import (
    ingest_full,
    ingest_incremental,
    verify_source_integrity,
)
from src.recovery_demo import (
    run_interrupted_ingestion_demo,
)
from src.reset import reset_generated
from src.storage import (
    create_partitions,
    export_storage_formats,
    load_partition,
    load_postgres,
    storage_all,
    verify_storage_equivalence,
)
from src.train import train_model
from src.transform import (
    build_curated,
    build_staging,
    transform_all,
)
from src.validate import (
    validate_all,
    validate_data_quality,
    validate_database,
    validate_raw_layer,
)


def build_parser() -> argparse.ArgumentParser:
    """Build command-line interface."""
    parser = argparse.ArgumentParser(
        description=(
            "DSS150P reproducible stroke pipeline"
        )
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    # ------------------------------------------------------------------
    # Source and ingestion
    # ------------------------------------------------------------------

    subparsers.add_parser(
        "verify-source"
    )

    subparsers.add_parser(
        "ingest-full"
    )

    incremental = subparsers.add_parser(
        "ingest-incremental"
    )

    incremental.add_argument(
        "--max-batches",
        type=int,
        default=1,
    )

    incremental.add_argument(
        "--all",
        action="store_true",
    )

    # ------------------------------------------------------------------
    # Transformation
    # ------------------------------------------------------------------

    subparsers.add_parser(
        "transform-staging"
    )

    subparsers.add_parser(
        "transform-curated"
    )

    subparsers.add_parser(
        "transform-all"
    )

    # ------------------------------------------------------------------
    # Modeling
    # ------------------------------------------------------------------

    subparsers.add_parser(
        "train"
    )

    subparsers.add_parser(
        "evaluate"
    )

    # ------------------------------------------------------------------
    # Storage
    # ------------------------------------------------------------------

    subparsers.add_parser(
        "storage-export"
    )

    subparsers.add_parser(
        "storage-verify"
    )

    subparsers.add_parser(
        "create-partitions"
    )

    subparsers.add_parser(
        "load-postgres"
    )

    subparsers.add_parser(
        "storage-all"
    )

    partition_parser = (
        subparsers.add_parser(
            "load-partition"
        )
    )

    partition_parser.add_argument(
        "--age-score-risk",
        type=int,
        required=True,
        choices=[
            0,
            1,
        ],
    )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    subparsers.add_parser(
        "validate-raw"
    )

    subparsers.add_parser(
        "validate-data"
    )

    subparsers.add_parser(
        "validate-database"
    )

    subparsers.add_parser(
        "validate-all"
    )

    # ------------------------------------------------------------------
    # Controlled contract-failure demonstration
    # ------------------------------------------------------------------

    failure_parser = (
        subparsers.add_parser(
            "demo-contract-failure"
        )
    )

    failure_parser.add_argument(
        "--scenario",
        choices=[
            "invalid-binary",
            "missing-column",
        ],
        default="invalid-binary",
    )

    # ------------------------------------------------------------------
    # Interrupted-ingestion recovery demonstration
    # ------------------------------------------------------------------

    recovery_parser = (
        subparsers.add_parser(
            "demo-interrupted-ingest"
        )
    )

    recovery_parser.add_argument(
        "--output",
        default=(
            "docs/interrupted_run_log.json"
        ),
        help=(
            "Path where interrupted-run evidence "
            "will be written."
        ),
    )

    # ------------------------------------------------------------------
    # Generated-state reset
    # ------------------------------------------------------------------

    reset_parser = (
        subparsers.add_parser(
            "reset-generated"
        )
    )

    reset_parser.add_argument(
        "--database",
        action="store_true",
        help=(
            "Also reset project-owned "
            "PostgreSQL tables."
        ),
    )

    reset_parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Show generated state that would "
            "be removed without deleting it."
        ),
    )

    # ------------------------------------------------------------------
    # Benchmark
    # ------------------------------------------------------------------

    benchmark_parser = (
        subparsers.add_parser(
            "benchmark"
        )
    )

    benchmark_parser.add_argument(
        "--repeats",
        type=int,
        default=5,
    )

    return parser


def main() -> int:
    """Execute selected pipeline command."""
    parser = build_parser()
    args = parser.parse_args()

    try:
        if (
            args.command
            == "verify-source"
        ):
            result = (
                verify_source_integrity()
            )

        elif (
            args.command
            == "ingest-full"
        ):
            result = (
                ingest_full()
            )

        elif (
            args.command
            == "ingest-incremental"
        ):
            result = (
                ingest_incremental(
                    max_batches=(
                        args.max_batches
                    ),
                    ingest_all=(
                        args.all
                    ),
                )
            )

        elif (
            args.command
            == "transform-staging"
        ):
            result = (
                build_staging()
            )

        elif (
            args.command
            == "transform-curated"
        ):
            result = (
                build_curated()
            )

        elif (
            args.command
            == "transform-all"
        ):
            result = (
                transform_all()
            )

        elif (
            args.command
            == "train"
        ):
            result = (
                train_model()
            )

        elif (
            args.command
            == "evaluate"
        ):
            result = (
                evaluate_model()
            )

        elif (
            args.command
            == "storage-export"
        ):
            result = (
                export_storage_formats()
            )

        elif (
            args.command
            == "storage-verify"
        ):
            result = (
                verify_storage_equivalence()
            )

        elif (
            args.command
            == "create-partitions"
        ):
            result = (
                create_partitions()
            )

        elif (
            args.command
            == "load-postgres"
        ):
            result = (
                load_postgres()
            )

        elif (
            args.command
            == "load-partition"
        ):
            result = (
                load_partition(
                    age_score_risk=(
                        args.age_score_risk
                    )
                )
            )

        elif (
            args.command
            == "storage-all"
        ):
            result = (
                storage_all()
            )

        elif (
            args.command
            == "validate-raw"
        ):
            result = (
                validate_raw_layer()
            )

        elif (
            args.command
            == "validate-data"
        ):
            result = (
                validate_data_quality()
            )

        elif (
            args.command
            == "validate-database"
        ):
            result = (
                validate_database()
            )

        elif (
            args.command
            == "validate-all"
        ):
            result = (
                validate_all()
            )

        elif (
            args.command
            == "demo-contract-failure"
        ):
            result = (
                run_contract_failure_demo(
                    scenario=(
                        args.scenario
                    )
                )
            )

        elif (
            args.command
            == "demo-interrupted-ingest"
        ):
            result = (
                run_interrupted_ingestion_demo(
                    output_path=(
                        args.output
                    )
                )
            )

        elif (
            args.command
            == "reset-generated"
        ):
            result = (
                reset_generated(
                    reset_database=(
                        args.database
                    ),
                    dry_run=(
                        args.dry_run
                    ),
                )
            )

        elif (
            args.command
            == "benchmark"
        ):
            result = (
                run_benchmark(
                    repeats=(
                        args.repeats
                    )
                )
            )

        else:
            parser.error(
                "Unknown command: "
                f"{args.command}"
            )

            return 2

        print(
            json.dumps(
                result,
                indent=2,
                sort_keys=True,
            )
        )

        # Reproducibility evaluation is an acceptance check.
        # A returned FAIL must also produce a nonzero shell exit code
        # so CI, Airflow, and other automation can detect the failure.
        if (
            args.command
            == "evaluate"
            and isinstance(
                result,
                dict,
            )
            and result.get(
                "status"
            )
            != "PASS"
        ):
            return 1

        return 0

    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": (
                        "FAILED"
                    ),
                    "error_type": (
                        type(
                            exc
                        ).__name__
                    ),
                    "message": (
                        str(
                            exc
                        )
                    ),
                },
                indent=2,
            ),
            file=sys.stderr,
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(
        main()
    )