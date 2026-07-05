"""Traffic KPI graph generation from SQLite table data."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .database import fetch_kpi_table


def plot_traffic_matplotlib(
    db_path: str | Path,
    table: str,
    time_column: str,
    kpi_columns: list[str],
    output_path: str | Path,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    title: str | None = None,
) -> Path:
    frame = fetch_kpi_table(
        db_path, table, time_column, kpi_columns, start=start, end=end
    )
    if frame.empty:
        raise ValueError("No KPI data found for the selected filters.")

    chart_title = title or f"Traffic KPIs · {table}"
    figure, axes = plt.subplots(
        len(kpi_columns), 1, figsize=(12, 3.2 * len(kpi_columns)), sharex=True
    )
    if len(kpi_columns) == 1:
        axes = [axes]

    for axis, column in zip(axes, kpi_columns):
        axis.plot(
            frame[time_column],
            frame[column],
            label=column,
            linewidth=1.8,
        )
        axis.set_ylabel(column)
        axis.grid(True, alpha=0.3)
        axis.legend(loc="upper right", fontsize=8)

    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
    figure.autofmt_xdate()
    figure.suptitle(chart_title, fontsize=14, y=0.995)
    figure.tight_layout()

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output, dpi=150, bbox_inches="tight")
    plt.close(figure)
    return output


def plot_traffic_plotly(
    db_path: str | Path,
    table: str,
    time_column: str,
    kpi_columns: list[str],
    output_path: str | Path,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    title: str | None = None,
) -> Path:
    frame = fetch_kpi_table(
        db_path, table, time_column, kpi_columns, start=start, end=end
    )
    if frame.empty:
        raise ValueError("No KPI data found for the selected filters.")

    chart_title = title or f"Traffic KPIs · {table}"
    figure = make_subplots(
        rows=len(kpi_columns),
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        subplot_titles=kpi_columns,
    )

    for row_index, column in enumerate(kpi_columns, start=1):
        figure.add_trace(
            go.Scatter(
                x=frame[time_column],
                y=frame[column],
                mode="lines",
                name=column,
            ),
            row=row_index,
            col=1,
        )
        figure.update_yaxes(title_text=column, row=row_index, col=1)

    figure.update_layout(
        title=chart_title,
        height=max(320, 260 * len(kpi_columns)),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    figure.update_xaxes(title_text=time_column, row=len(kpi_columns), col=1)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    figure.write_html(str(output), include_plotlyjs="cdn")
    return output
