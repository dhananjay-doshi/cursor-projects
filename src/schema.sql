-- Traffic KPI metrics stored at regular intervals per interface/source.

CREATE TABLE IF NOT EXISTS traffic_kpis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at TEXT NOT NULL,
    source TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value REAL NOT NULL,
    unit TEXT,
    UNIQUE (recorded_at, source, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_traffic_kpis_time
    ON traffic_kpis (recorded_at);

CREATE INDEX IF NOT EXISTS idx_traffic_kpis_source_metric
    ON traffic_kpis (source, metric_name);
