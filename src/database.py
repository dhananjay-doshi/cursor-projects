"""SQLite access layer for reading KPI data from user-provided tables."""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def quote_ident(name: str) -> str:
    """Validate and quote a SQLite identifier."""
    if not IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return f'"{name}"'


def connect(db_path: str | Path, *, read_only: bool = False) -> sqlite3.Connection:
    path = Path(db_path)
    if not read_only and not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

    if not path.exists():
        raise FileNotFoundError(f"Database not found: {path}")

    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=read_only) if read_only else sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def list_tables(db_path: str | Path) -> list[str]:
    with connect(db_path, read_only=True) as conn:
        cursor = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        )
        return [row["name"] for row in cursor.fetchall()]


def list_columns(db_path: str | Path, table: str) -> list[str]:
    quoted_table = quote_ident(table)
    with connect(db_path, read_only=True) as conn:
        if table not in list_tables(db_path):
            raise ValueError(f"Table not found: {table}")

        cursor = conn.execute(f"PRAGMA table_info({quoted_table})")
        return [row["name"] for row in cursor.fetchall()]


def validate_table_columns(
    db_path: str | Path, table: str, columns: list[str]
) -> None:
    if not columns:
        raise ValueError("At least two columns are required: time column plus one KPI column.")

    available_tables = list_tables(db_path)
    if table not in available_tables:
        raise ValueError(
            f"Table not found: {table}. Available tables: {', '.join(available_tables) or 'none'}"
        )

    available_columns = set(list_columns(db_path, table))
    missing = [column for column in columns if column not in available_columns]
    if missing:
        raise ValueError(
            f"Column(s) not found in {table}: {', '.join(missing)}. "
            f"Available columns: {', '.join(sorted(available_columns))}"
        )


def _format_timestamp(value: datetime) -> str:
    return value.strftime(TIMESTAMP_FORMAT)


def fetch_kpi_table(
    db_path: str | Path,
    table: str,
    time_column: str,
    kpi_columns: list[str],
    *,
    start: datetime | None = None,
    end: datetime | None = None,
) -> pd.DataFrame:
    """Read KPI rows from a wide-format table.

    The first selected column must be a timestamp formatted as YYYY-MM-DD HH:MM:SS.
    Remaining columns are numeric KPI series to plot.
    """
    columns = [time_column, *kpi_columns]
    validate_table_columns(db_path, table, columns)

    quoted_columns = ", ".join(quote_ident(column) for column in columns)
    quoted_table = quote_ident(table)
    quoted_time = quote_ident(time_column)

    clauses: list[str] = []
    params: list[str] = []
    if start is not None:
        clauses.append(f"{quoted_time} >= ?")
        params.append(_format_timestamp(start))
    if end is not None:
        clauses.append(f"{quoted_time} <= ?")
        params.append(_format_timestamp(end))

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"""
        SELECT {quoted_columns}
        FROM {quoted_table}
        {where}
        ORDER BY {quoted_time} ASC
    """

    with connect(db_path, read_only=True) as conn:
        frame = pd.read_sql_query(query, conn, params=params)

    if frame.empty:
        return frame

    frame[time_column] = pd.to_datetime(
        frame[time_column], format=TIMESTAMP_FORMAT, errors="coerce"
    )
    invalid_times = frame[time_column].isna().sum()
    if invalid_times:
        raise ValueError(
            f"{invalid_times} row(s) in {time_column!r} do not match "
            f"YYYY-MM-DD HH:MM:SS (e.g. 2026-02-05 09:45:30)."
        )

    for column in kpi_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    return frame
