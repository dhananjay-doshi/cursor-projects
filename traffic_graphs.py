#!/usr/bin/env python3
"""CLI for plotting traffic KPI graphs from a SQLite database."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from src.database import TIMESTAMP_FORMAT, list_columns, list_tables, validate_table_columns
from src.plots import plot_traffic_matplotlib, plot_traffic_plotly
from src.seed_data import (
    DEMO_KPI_COLUMNS,
    DEMO_TABLE,
    DEMO_TIME_COLUMN,
    seed_demo_table,
)

DEFAULT_DB = Path("data/traffic_kpis.db")


def _parse_datetime(value: str) -> datetime:
    for fmt in (TIMESTAMP_FORMAT, "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"Invalid datetime '{value}'. Use YYYY-MM-DD HH:MM:SS."
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Plot traffic KPI graphs from an existing SQLite3 table. "
            "The first column must be a timestamp (YYYY-MM-DD HH:MM:SS); "
            "remaining columns are KPI series to plot."
        )
    )
    parser.add_argument(
        "--db",
        type=Path,
        required=True,
        help="Path to the SQLite database file",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    seed_parser = subparsers.add_parser(
        "seed",
        help="Create a demo database table for testing (optional)",
    )
    seed_parser.add_argument("--hours", type=int, default=24, help="Hours of history")
    seed_parser.add_argument(
        "--interval-minutes",
        type=int,
        default=5,
        help="Sampling interval in minutes",
    )

    plot_parser = subparsers.add_parser("plot", help="Generate traffic KPI graphs")
    plot_parser.add_argument(
        "--table",
        required=True,
        help="Table name within the database",
    )
    plot_parser.add_argument(
        "--columns",
        nargs="+",
        required=True,
        metavar="COLUMN",
        help=(
            "Columns to read. First column is the timestamp "
            "(YYYY-MM-DD HH:MM:SS); remaining columns are KPI metrics."
        ),
    )
    plot_parser.add_argument(
        "--format",
        choices=("png", "html", "both"),
        default="both",
        help="Output format: static PNG, interactive HTML, or both",
    )
    plot_parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Directory for generated graph files",
    )
    plot_parser.add_argument("--start", type=_parse_datetime, help="Start time filter")
    plot_parser.add_argument("--end", type=_parse_datetime, help="End time filter")
    plot_parser.add_argument(
        "--title",
        help="Chart title (default: Traffic KPIs · <table>)",
    )
    plot_parser.add_argument(
        "--output-name",
        help="Base output filename without extension (default: <table>)",
    )

    info_parser = subparsers.add_parser(
        "info", help="List tables and columns in the database"
    )
    info_parser.add_argument(
        "--table",
        help="Show columns for a specific table",
    )

    return parser


def cmd_seed(args: argparse.Namespace) -> int:
    count = seed_demo_table(
        args.db,
        hours=args.hours,
        interval_minutes=args.interval_minutes,
    )
    print(f"Seeded {count} rows into {args.db} table {DEMO_TABLE!r}")
    print(
        "Plot with:\n"
        f"  python3 traffic_graphs.py --db {args.db} plot "
        f"--table {DEMO_TABLE} --columns {DEMO_TIME_COLUMN} "
        f"{' '.join(DEMO_KPI_COLUMNS)}"
    )
    return 0


def cmd_info(args: argparse.Namespace) -> int:
    if not args.db.exists():
        print(f"Database not found: {args.db}", file=sys.stderr)
        return 1

    print(f"Database: {args.db}")

    if args.table:
        columns = list_columns(args.db, args.table)
        print(f"Table: {args.table}")
        print(f"Columns ({len(columns)}): {', '.join(columns)}")
        return 0

    tables = list_tables(args.db)
    print(f"Tables ({len(tables)}): {', '.join(tables) or 'none'}")
    for table in tables:
        columns = list_columns(args.db, table)
        print(f"  {table}: {', '.join(columns)}")
    return 0


def cmd_plot(args: argparse.Namespace) -> int:
    if not args.db.exists():
        print(f"Database not found: {args.db}", file=sys.stderr)
        return 1

    if len(args.columns) < 2:
        print(
            "Provide at least two columns: one timestamp column and one KPI column.",
            file=sys.stderr,
        )
        return 1

    time_column, *kpi_columns = args.columns

    try:
        validate_table_columns(args.db, args.table, args.columns)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)
    base_name = args.output_name or args.table
    outputs: list[Path] = []

    try:
        if args.format in ("png", "both"):
            png_path = plot_traffic_matplotlib(
                args.db,
                args.table,
                time_column,
                kpi_columns,
                args.output_dir / f"{base_name}.png",
                start=args.start,
                end=args.end,
                title=args.title,
            )
            outputs.append(png_path)

        if args.format in ("html", "both"):
            html_path = plot_traffic_plotly(
                args.db,
                args.table,
                time_column,
                kpi_columns,
                args.output_dir / f"{base_name}.html",
                start=args.start,
                end=args.end,
                title=args.title,
            )
            outputs.append(html_path)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    for path in outputs:
        print(f"Wrote {path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "seed":
        return cmd_seed(args)
    if args.command == "info":
        return cmd_info(args)
    if args.command == "plot":
        return cmd_plot(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
