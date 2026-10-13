"""CLI for the DSS150P reproducibility pipeline."""

from __future__ import annotations

import argparse
import json
import sys

from src.ingest import (
    ingest_full,
    ingest_incremental,
    verify_source_integrity,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="DSS150P reproducible stroke pipeline"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    subparsers.add_parser("verify-source")
    subparsers.add_parser("ingest-full")

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

    return parser


def main() -> int:
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

        else:
            parser.error("Unknown command.")
            return 2

        print(json.dumps(result, indent=2, sort_keys=True))
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