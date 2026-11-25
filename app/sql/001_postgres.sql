CREATE TABLE IF NOT EXISTS daily_runs (
    metric_date date PRIMARY KEY,
    computed_at timestamptz NOT NULL,
    exported_version timestamptz,
    events_processed bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_metrics (
    metric_date date NOT NULL REFERENCES daily_runs (metric_date),
    metric text NOT NULL,
    dimension text NOT NULL DEFAULT '',
    value double precision,
    computed_at timestamptz NOT NULL,
    PRIMARY KEY (metric_date, metric, dimension)
);
