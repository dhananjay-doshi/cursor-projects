# Traffic KPI Graphs

Plot traffic graphs from KPIs stored in a SQLite3 database.

## Features

- SQLite schema for time-series traffic KPIs (throughput, packet rate, latency, packet loss, connections, errors)
- Sample data seeding for quick demos
- Static PNG charts (matplotlib) and interactive HTML charts (plotly)
- Filter by source/interface, metric, and time range

## Setup

```bash
pip install -r requirements.txt
```

## Quick start

```bash
# Create database and load 24 hours of sample traffic KPIs
python traffic_graphs.py seed

# List available sources and metrics
python traffic_graphs.py info

# Generate PNG and HTML traffic graphs
python traffic_graphs.py plot
```

Outputs are written to `output/traffic_kpis.png` and `output/traffic_kpis.html`.

## Database schema

KPIs are stored in a single `traffic_kpis` table:

| Column        | Type   | Description                          |
|---------------|--------|--------------------------------------|
| recorded_at   | TEXT   | ISO timestamp                        |
| source        | TEXT   | Interface or traffic source (e.g. eth0) |
| metric_name   | TEXT   | KPI identifier                       |
| metric_value  | REAL   | Numeric value                        |
| unit          | TEXT   | Display unit (Mbps, ms, %, etc.)     |

Supported metrics:

- `throughput_in_mbps`, `throughput_out_mbps`
- `packet_rate_in_pps`, `packet_rate_out_pps`
- `latency_ms`
- `packet_loss_pct`
- `active_connections`
- `error_rate`

## Inserting your own data

Use the Python API or insert rows directly:

```python
from datetime import datetime
from src.database import init_db, insert_kpis, KpiRecord

init_db("data/traffic_kpis.db")
insert_kpis("data/traffic_kpis.db", [
    KpiRecord(
        recorded_at=datetime.now(),
        source="eth0",
        metric_name="throughput_in_mbps",
        metric_value=512.4,
    ),
])
```

## Plot options

```bash
# PNG only
python traffic_graphs.py plot --format png

# Specific metrics and sources
python traffic_graphs.py plot \
  --metrics throughput_in_mbps latency_ms \
  --sources eth0 wan0

# Time range filter
python traffic_graphs.py plot \
  --start "2026-07-04 00:00" \
  --end "2026-07-05 12:00"
```

## Project layout

```
traffic_graphs.py   # CLI entry point
src/
  schema.sql        # SQLite schema
  database.py       # DB access layer
  seed_data.py      # Sample data generator
  plots.py          # Graph generation
data/               # SQLite database (created at runtime)
output/             # Generated charts
```
