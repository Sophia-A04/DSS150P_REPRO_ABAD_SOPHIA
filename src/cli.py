"""CLI for the DSS150P reproducibility pipeline."""

from __future__ import annotations

import argparse
import json
import sys

from src.evaluate import evaluate_model
from src.ingest import (
    ingest_full,
    ingest_incremental,
    verify_source_integrity,
)
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


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""
    parser = argparse.ArgumentParser(
        description="DSS150P reproducible stroke pipeline"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    # Source and ingestion commands
    subparsers.add_parser(
        "verify-source",
        help="Verify source checksum and dimensions.",
    )

    subparsers.add_parser(
        "ingest-full",
        help="Perform rerun-safe full ingestion.",
    )

    incremental = subparsers.add_parser(
        "ingest-incremental",
        help="Perform resumable incremental ingestion.",
    )

    incremental.add_argument(
        "--max-batches",
        type=int,
        default=1,
        help="Maximum number of new batches to process.",
    )

    incremental.add_argument(
        "--all",
        action="store_true",
        help="Process all remaining batches.",
    )

    # Transformation commands
    subparsers.add_parser(
        "transform-staging",
        help="Create deterministic cleaned staging data.",
    )

    subparsers.add_parser(
        "transform-curated",
        help="Create feature-engineered curated data.",
    )

    subparsers.add_parser(
        "transform-all",
        help="Run staging and curated transformations.",
    )

    # Model commands
    subparsers.add_parser(
        "train",
        help="Train the reproducible baseline model.",
    )

    subparsers.add_parser(
        "evaluate",
        help="Evaluate model reproducibility.",
    )

    # Storage commands
    subparsers.add_parser(
        "storage-export",
        help="Export curated data to CSV, JSONL, and Parquet.",
    )

    subparsers.add_parser(
        "storage-verify",
        help="Verify logical equivalence across storage formats.",
    )

    subparsers.add_parser(
        "create-partitions",
        help="Create deterministic age-risk partitions.",
    )

    subparsers.add_parser(
        "load-postgres",
        help="Load curated data into PostgreSQL using rerun-safe UPSERT.",
    )

    subparsers.add_parser(
        "storage-all",
        help=(
            "Export storage formats, verify equivalence, "
            "create partitions, and load PostgreSQL."
        ),
    )

    partition_parser = subparsers.add_parser(
        "load-partition",
        help="Load one selected partition independently.",
    )

    partition_parser.add_argument(
        "--age-score-risk",
        type=int,
        required=True,
        choices=[0, 1],
        help="Age-risk partition to load.",
    )

    return parser


def main() -> int:
    """Execute a CLI command."""
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.command == "verify-source":
            result = verify_source_integrity()

        elif args.command == "ingest-full":
            result = ingest_full()

        elif args.command == "ingest-incremental":
            result = ingest_incremental(
                max_batches=args.max_batches,
                ingest_all=args.all,
            )

        elif args.command == "transform-staging":
            result = build_staging()

        elif args.command == "transform-curated":
            result = build_curated()

        elif args.command == "transform-all":
            result = transform_all()

        elif args.command == "train":
            result = train_model()

        elif args.command == "evaluate":
            result = evaluate_model()

        elif args.command == "storage-export":
            result = export_storage_formats()

        elif args.command == "storage-verify":
            result = verify_storage_equivalence()

        elif args.command == "create-partitions":
            result = create_partitions()

        elif args.command == "load-postgres":
            result = load_postgres()

        elif args.command == "load-partition":
            result = load_partition(
                age_score_risk=args.age_score_risk
            )

        elif args.command == "storage-all":
            result = storage_all()

        else:
            parser.error(f"Unknown command: {args.command}")
            return 2

        print(
            json.dumps(
                result,
                indent=2,
                sort_keys=True,
            )
        )

        return 0

    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                },
                indent=2,
            ),
            file=sys.stderr,
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(main())