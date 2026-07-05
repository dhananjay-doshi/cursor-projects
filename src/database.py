"""SQLite access layer for traffic KPI data."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"

# Supported KPI metrics and their display units.
METRIC_UNITS = {
    "throughput_in_mbps": "Mbps",
    "throughput_out_mbps": "Mbps",
    "packet_rate_in_pps": "pps",
    "packet_rate_out_pps": "pps",
    "latency_ms": "ms",
    "packet_loss_pct": "%",
    "active_connections": "count",
    "error_rate": "errors/s",
}


@dataclass(frozen=True)
class KpiRecord:
    recorded_at: datetime
    source: str
    metric_name: str
    metric_value: float
    unit: str | None = None


def connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: str | Path) -> None:
    schema = SCHEMA_PATH.read_text(encoding="utf-8")
    with connect(db_path) as conn:
        conn.executescript(schema)
        conn.commit()


def insert_kpis(db_path: str | Path, records: Iterable[KpiRecord]) -> int:
    rows = [
        (
            record.recorded_at.isoformat(),
            record.source,
            record.metric_name,
            record.metric_value,
            record.unit or METRIC_UNITS.get(record.metric_name),
        )
        for record in records
    ]
    if not rows:
        return 0

    with connect(db_path) as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO traffic_kpis
                (recorded_at, source, metric_name, metric_value, unit)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    return len(rows)


def list_sources(db_path: str | Path) -> list[str]:
    with connect(db_path) as conn:
        cursor = conn.execute(
            "SELECT DISTINCT source FROM traffic_kpis ORDER BY source"
        )
        return [row["source"] for row in cursor.fetchall()]


def list_metrics(db_path: str | Path, source: str | None = None) -> list[str]:
    query = "SELECT DISTINCT metric_name FROM traffic_kpis"
    params: tuple = ()
    if source:
        query += " WHERE source = ?"
        params = (source,)
    query += " ORDER BY metric_name"

    with connect(db_path) as conn:
        cursor = conn.execute(query, params)
        return [row["metric_name"] for row in cursor.fetchall()]


def fetch_kpi_series(
    db_path: str | Path,
    metric_names: list[str] | None = None,
    sources: list[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> list[dict]:
    """Return KPI rows as dicts ordered by time."""
    clauses: list[str] = []
    params: list = []

    if metric_names:
        placeholders = ", ".join("?" * len(metric_names))
        clauses.append(f"metric_name IN ({placeholders})")
        params.extend(metric_names)

    if sources:
        placeholders = ", ".join("?" * len(sources))
        clauses.append(f"source IN ({placeholders})")
        params.extend(sources)

    if start:
        clauses.append("recorded_at >= ?")
        params.append(start.isoformat())

    if end:
        clauses.append("recorded_at <= ?")
        params.append(end.isoformat())

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"""
        SELECT recorded_at, source, metric_name, metric_value, unit
        FROM traffic_kpis
        {where}
        ORDER BY recorded_at ASC
    """

    with connect(db_path) as conn:
        cursor = conn.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]
