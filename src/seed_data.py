"""Generate a sample KPI table for development and demos."""

from __future__ import annotations

import math
import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from .database import TIMESTAMP_FORMAT, quote_ident

DEMO_TABLE = "traffic_metrics"
DEMO_TIME_COLUMN = "recorded_at"
DEMO_KPI_COLUMNS = (
    "throughput_in_mbps",
    "throughput_out_mbps",
    "latency_ms",
    "packet_loss_pct",
)


def _metric_value(column: str, hour: float) -> float:
    phase = hour / 24.0 * 2 * math.pi
    noise = random.uniform(-0.08, 0.08)

    if column == "throughput_in_mbps":
        return max(0.0, 850 * (1 + 0.25 * math.sin(phase)) * (1 + noise))
    if column == "throughput_out_mbps":
        return max(0.0, 620 * (1 + 0.25 * math.sin(phase + 0.5)) * (1 + noise))
    if column == "latency_ms":
        return max(1.0, 12 + 8 * math.sin(phase * 2) + random.uniform(-2, 2))
    if column == "packet_loss_pct":
        return max(0.0, 0.05 + 0.4 * abs(math.sin(phase * 3)) + random.uniform(0, 0.1))

    return random.uniform(0, 100)


def seed_demo_table(
    db_path: str | Path,
    *,
    hours: int = 24,
    interval_minutes: int = 5,
    table: str = DEMO_TABLE,
    time_column: str = DEMO_TIME_COLUMN,
    kpi_columns: tuple[str, ...] = DEMO_KPI_COLUMNS,
) -> int:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    quoted_table = quote_ident(table)
    quoted_time = quote_ident(time_column)
    quoted_kpis = ", ".join(quote_ident(column) for column in kpi_columns)
    column_defs = ", ".join(f"{quote_ident(column)} REAL NOT NULL" for column in kpi_columns)

    end = datetime.now().replace(second=0, microsecond=0)
    start = end - timedelta(hours=hours)
    step = timedelta(minutes=interval_minutes)

    rows: list[tuple] = []
    current = start
    while current <= end:
        hour = current.hour + current.minute / 60.0
        values = tuple(round(_metric_value(column, hour), 4) for column in kpi_columns)
        rows.append((current.strftime(TIMESTAMP_FORMAT), *values))
        current += step

    with sqlite3.connect(path) as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {quoted_table} (
                {quoted_time} TEXT NOT NULL,
                {column_defs}
            )
            """
        )
        conn.execute(f"DELETE FROM {quoted_table}")

        placeholders = ", ".join("?" * (1 + len(kpi_columns)))
        conn.executemany(
            f"INSERT INTO {quoted_table} ({quoted_time}, {quoted_kpis}) VALUES ({placeholders})",
            rows,
        )
        conn.commit()

    return len(rows)
