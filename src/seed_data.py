"""Generate sample traffic KPI data for development and demos."""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta

from .database import KpiRecord, METRIC_UNITS, init_db, insert_kpis

DEFAULT_SOURCES = ("eth0", "eth1", "wan0")
DEFAULT_METRICS = tuple(METRIC_UNITS.keys())


def _metric_value(metric_name: str, hour: float, source_index: int) -> float:
    """Produce realistic-looking synthetic KPI values."""
    phase = hour / 24.0 * 2 * math.pi
    noise = random.uniform(-0.08, 0.08)
    base = 1.0 + 0.25 * math.sin(phase + source_index)

    if metric_name.startswith("throughput"):
        peak = 850 if "in" in metric_name else 620
        return max(0.0, peak * base * (1 + noise))

    if metric_name.startswith("packet_rate"):
        peak = 120_000 if "in" in metric_name else 95_000
        return max(0.0, peak * base * (1 + noise))

    if metric_name == "latency_ms":
        return max(1.0, 12 + 8 * math.sin(phase * 2) + random.uniform(-2, 2))

    if metric_name == "packet_loss_pct":
        return max(0.0, 0.05 + 0.4 * abs(math.sin(phase * 3)) + random.uniform(0, 0.1))

    if metric_name == "active_connections":
        return max(0.0, 4_000 + 2_500 * base + random.uniform(-200, 200))

    if metric_name == "error_rate":
        return max(0.0, 3 + 5 * abs(math.sin(phase * 4)) + random.uniform(0, 2))

    return random.uniform(0, 100)


def seed_traffic_kpis(
    db_path: str,
    *,
    hours: int = 24,
    interval_minutes: int = 5,
    sources: tuple[str, ...] = DEFAULT_SOURCES,
    metrics: tuple[str, ...] = DEFAULT_METRICS,
) -> int:
    init_db(db_path)

    end = datetime.now().replace(second=0, microsecond=0)
    start = end - timedelta(hours=hours)
    step = timedelta(minutes=interval_minutes)

    records: list[KpiRecord] = []
    current = start
    while current <= end:
        hour = current.hour + current.minute / 60.0
        for source_index, source in enumerate(sources):
            for metric_name in metrics:
                records.append(
                    KpiRecord(
                        recorded_at=current,
                        source=source,
                        metric_name=metric_name,
                        metric_value=round(
                            _metric_value(metric_name, hour, source_index), 4
                        ),
                    )
                )
        current += step

    return insert_kpis(db_path, records)
