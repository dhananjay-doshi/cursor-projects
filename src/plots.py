"""Traffic KPI graph generation from SQLite data."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .database import METRIC_UNITS, fetch_kpi_series

METRIC_LABELS = {
    "throughput_in_mbps": "Inbound Throughput",
    "throughput_out_mbps": "Outbound Throughput",
    "packet_rate_in_pps": "Inbound Packet Rate",
    "packet_rate_out_pps": "Outbound Packet Rate",
    "latency_ms": "Latency",
    "packet_loss_pct": "Packet Loss",
    "active_connections": "Active Connections",
    "error_rate": "Error Rate",
}


def _to_dataframe(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(
            columns=["recorded_at", "source", "metric_name", "metric_value", "unit"]
        )

    frame = pd.DataFrame(rows)
    frame["recorded_at"] = pd.to_datetime(frame["recorded_at"])
    return frame


def _series_label(source: str, metric_name: str) -> str:
    label = METRIC_LABELS.get(metric_name, metric_name)
    return f"{source} · {label}"


def _unit_for_metric(metric_name: str, frame: pd.DataFrame) -> str:
    units = frame.loc[frame["metric_name"] == metric_name, "unit"].dropna().unique()
    if len(units):
        return str(units[0])
    return METRIC_UNITS.get(metric_name, "")


def plot_traffic_matplotlib(
    db_path: str | Path,
    output_path: str | Path,
    *,
    metrics: list[str] | None = None,
    sources: list[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    title: str = "Traffic KPI Overview",
) -> Path:
    rows = fetch_kpi_series(db_path, metrics, sources, start, end)
    frame = _to_dataframe(rows)
    if frame.empty:
        raise ValueError("No KPI data found for the selected filters.")

    metric_names = sorted(frame["metric_name"].unique())
    figure, axes = plt.subplots(
        len(metric_names), 1, figsize=(12, 3.2 * len(metric_names)), sharex=True
    )
    if len(metric_names) == 1:
        axes = [axes]

    for axis, metric_name in zip(axes, metric_names):
        subset = frame[frame["metric_name"] == metric_name]
        for source in sorted(subset["source"].unique()):
            series = subset[subset["source"] == source]
            axis.plot(
                series["recorded_at"],
                series["metric_value"],
                label=_series_label(source, metric_name),
                linewidth=1.8,
            )

        unit = _unit_for_metric(metric_name, subset)
        y_label = METRIC_LABELS.get(metric_name, metric_name)
        if unit:
            y_label = f"{y_label} ({unit})"
        axis.set_ylabel(y_label)
        axis.grid(True, alpha=0.3)
        axis.legend(loc="upper right", fontsize=8)

    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    figure.autofmt_xdate()
    figure.suptitle(title, fontsize=14, y=0.995)
    figure.tight_layout()

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output, dpi=150, bbox_inches="tight")
    plt.close(figure)
    return output


def plot_traffic_plotly(
    db_path: str | Path,
    output_path: str | Path,
    *,
    metrics: list[str] | None = None,
    sources: list[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    title: str = "Traffic KPI Overview",
) -> Path:
    rows = fetch_kpi_series(db_path, metrics, sources, start, end)
    frame = _to_dataframe(rows)
    if frame.empty:
        raise ValueError("No KPI data found for the selected filters.")

    metric_names = sorted(frame["metric_name"].unique())
    figure = make_subplots(
        rows=len(metric_names),
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        subplot_titles=[
            METRIC_LABELS.get(name, name) for name in metric_names
        ],
    )

    for row_index, metric_name in enumerate(metric_names, start=1):
        subset = frame[frame["metric_name"] == metric_name]
        unit = _unit_for_metric(metric_name, subset)
        for source in sorted(subset["source"].unique()):
            series = subset[subset["source"] == source]
            figure.add_trace(
                go.Scatter(
                    x=series["recorded_at"],
                    y=series["metric_value"],
                    mode="lines",
                    name=_series_label(source, metric_name),
                    legendgroup=source,
                ),
                row=row_index,
                col=1,
            )

        y_title = METRIC_LABELS.get(metric_name, metric_name)
        if unit:
            y_title = f"{y_title} ({unit})"
        figure.update_yaxes(title_text=y_title, row=row_index, col=1)

    figure.update_layout(
        title=title,
        height=max(320, 260 * len(metric_names)),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    figure.update_xaxes(title_text="Time", row=len(metric_names), col=1)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    figure.write_html(str(output), include_plotlyjs="cdn")
    return output
