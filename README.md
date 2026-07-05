# Traffic KPI Graphs

Plot traffic graphs from KPIs stored in an existing SQLite3 database table.

You provide:

1. **Database file** path
2. **Table name** within that database
3. **Column names** — the first column is the timestamp (`YYYY-MM-DD HH:MM:SS`, e.g. `2026-02-05 09:45:30`); remaining columns are KPI metrics to plot

## Setup

```bash
pip install -r requirements.txt
```

## Plot from your database

```bash
python3 traffic_graphs.py --db /path/to/your.db plot \
  --table your_table_name \
  --columns recorded_at throughput_mbps latency_ms packet_loss_pct
```

In this example:

- `recorded_at` — timestamp column (must be first)
- `throughput_mbps`, `latency_ms`, `packet_loss_pct` — KPI columns to graph

Outputs are written to `output/<table>.png` and `output/<table>.html`.

## Inspect your database

```bash
# List all tables and their columns
python3 traffic_graphs.py --db /path/to/your.db info

# List columns for one table
python3 traffic_graphs.py --db /path/to/your.db info --table your_table_name
```

## Plot options

```bash
# PNG only
python3 traffic_graphs.py --db your.db plot \
  --table your_table --columns ts col1 col2 --format png

# Filter by time range
python3 traffic_graphs.py --db your.db plot \
  --table your_table --columns ts col1 col2 \
  --start "2026-02-05 00:00:00" \
  --end "2026-02-05 23:59:59"

# Custom title and output filename
python3 traffic_graphs.py --db your.db plot \
  --table your_table --columns ts col1 col2 \
  --title "WAN Link KPIs" \
  --output-name wan_traffic
```

## Expected table format

Your table should be in **wide** format — one row per timestamp, with KPI values in separate columns:

| recorded_at         | throughput_mbps | latency_ms | packet_loss_pct |
|---------------------|-----------------|------------|-----------------|
| 2026-02-05 09:45:30 | 512.4           | 12.3       | 0.05            |
| 2026-02-05 09:50:30 | 498.1           | 13.1       | 0.04            |

Column names are yours; only the timestamp format and column order matter.

## Demo data (optional)

To try the tool without your own database:

```bash
python3 traffic_graphs.py --db data/demo.db seed
python3 traffic_graphs.py --db data/demo.db plot \
  --table traffic_metrics \
  --columns recorded_at throughput_in_mbps throughput_out_mbps latency_ms packet_loss_pct
```

## Project layout

```
traffic_graphs.py   # CLI entry point
src/
  database.py       # Read any SQLite table/columns
  plots.py          # Graph generation
  seed_data.py      # Optional demo table generator
output/             # Generated charts
```
