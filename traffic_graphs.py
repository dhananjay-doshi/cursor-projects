#!/usr/bin/env python3
"""CLI for plotting traffic KPI graphs from a SQLite database."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from src.database import list_metrics, list_sources
from src.plots import plot_traffic_matplotlib, plot_traffic_plotly
from src.seed_data import seed_traffic_kpis

DEFAULT_DB = Path("data/traffic_kpis.db")


def _parse_datetime(value: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"Invalid datetime '{value}'. Use YYYY-MM-DD or YYYY-MM-DD HH:MM."
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Plot traffic KPI graphs from a SQLite3 database."
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help=f"Path to SQLite database (default: {DEFAULT_DB})",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    seed_parser = subparsers.add_parser(
        "seed", help="Initialize the database and load sample traffic KPI data"
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
    plot_parser.add_argument(
        "--metrics",
        nargs="+",
        help="Metric names to plot (default: all available)",
    )
    plot_parser.add_argument(
        "--sources",
        nargs="+",
        help="Traffic sources/interfaces to include (default: all)",
    )
    plot_parser.add_argument("--start", type=_parse_datetime, help="Start time filter")
    plot_parser.add_argument("--end", type=_parse_datetime, help="End time filter")
    plot_parser.add_argument(
        "--title",
        default="Traffic KPI Overview",
        help="Chart title",
    )

    info_parser = subparsers.add_parser(
        "info", help="List available sources and metrics in the database"
    )

    return parser


def cmd_seed(args: argparse.Namespace) -> int:
    count = seed_traffic_kpis(
        args.db,
        hours=args.hours,
        interval_minutes=args.interval_minutes,
    )
    print(f"Seeded {count} KPI records into {args.db}")
    return 0


def cmd_info(args: argparse.Namespace) -> int:
    if not args.db.exists():
        print(f"Database not found: {args.db}", file=sys.stderr)
        return 1

    sources = list_sources(args.db)
    metrics = list_metrics(args.db)
    print(f"Database: {args.db}")
    print(f"Sources ({len(sources)}): {', '.join(sources) or 'none'}")
    print(f"Metrics ({len(metrics)}): {', '.join(metrics) or 'none'}")
    return 0


def cmd_plot(args: argparse.Namespace) -> int:
    if not args.db.exists():
        print(
            f"Database not found: {args.db}. Run `python traffic_graphs.py seed` first.",
            file=sys.stderr,
        )
        return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)
    outputs: list[Path] = []

    if args.format in ("png", "both"):
        png_path = plot_traffic_matplotlib(
            args.db,
            args.output_dir / "traffic_kpis.png",
            metrics=args.metrics,
            sources=args.sources,
            start=args.start,
            end=args.end,
            title=args.title,
        )
        outputs.append(png_path)

    if args.format in ("html", "both"):
        html_path = plot_traffic_plotly(
            args.db,
            args.output_dir / "traffic_kpis.html",
            metrics=args.metrics,
            sources=args.sources,
            start=args.start,
            end=args.end,
            title=args.title,
        )
        outputs.append(html_path)

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
