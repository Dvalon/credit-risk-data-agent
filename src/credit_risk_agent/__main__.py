"""Command line entry point: ``python -m credit_risk_agent``."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import duckdb

from credit_risk_agent.config import DEFAULT_WAREHOUSE, SNAPSHOT_DATE
from credit_risk_agent.download import DownloadError, download_accepted
from credit_risk_agent.pipeline import (
    PipelineError,
    SourceNotFoundError,
    build_warehouse,
    resolve_source,
    warn_if_unexpected_row_count,
)
from credit_risk_agent.quality import format_issues, run_quality_checks


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="credit_risk_agent",
        description="Build the Phase 1 LendingClub accepted-loans star schema.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build", help="Rebuild the DuckDB warehouse from scratch")
    build_parser.add_argument("--source", type=Path, help="Accepted-loans CSV. Default: data/raw extract")
    build_parser.add_argument("--warehouse", type=Path, default=DEFAULT_WAREHOUSE)
    build_parser.add_argument(
        "--snapshot",
        type=date.fromisoformat,
        default=SNAPSHOT_DATE,
        help="As-of date for is_matured, YYYY-MM-DD (default: 2019-03-31)",
    )
    build_parser.add_argument(
        "--no-download",
        action="store_true",
        help="Do not try to download the Kaggle extract when the raw file is missing",
    )

    subparsers.add_parser("download", help="Download the accepted-loans file from Kaggle")

    check_parser = subparsers.add_parser("check", help="Run quality checks on an existing warehouse")
    check_parser.add_argument("--warehouse", type=Path, required=True)

    args = parser.parse_args(argv)
    try:
        if args.command == "download":
            path = download_accepted()
            print(f"Downloaded accepted loans to {path}")
            return 0
        if args.command == "check":
            return _check(args.warehouse)
        return _build(args.source, args.warehouse, args.snapshot, allow_download=not args.no_download)
    except (SourceNotFoundError, DownloadError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except PipelineError as exc:
        print(str(exc), file=sys.stderr)
        return 1


def _build(source: Path | None, warehouse: Path, snapshot: date, *, allow_download: bool) -> int:
    resolved = resolve_source(source, allow_download=allow_download)
    report = build_warehouse(resolved, warehouse, snapshot)
    warn_if_unexpected_row_count(report.source_name, report.source_rows)
    print(report.summary())
    print("Quality checks passed.")
    return 0


def _check(warehouse: Path) -> int:
    if not warehouse.is_file():
        print(f"Warehouse not found: {warehouse}", file=sys.stderr)
        return 2
    connection = duckdb.connect(str(warehouse), read_only=True)
    try:
        issues = run_quality_checks(connection)
    finally:
        connection.close()
    print(format_issues(issues))
    return 1 if issues else 0


if __name__ == "__main__":
    raise SystemExit(main())
