CREATE TABLE IF NOT EXISTS default.raw_events
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3, 'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id String,
    progress_seconds Nullable(Int32),
    event_date Date32 MATERIALIZED toDate32(timestamp)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id, timestamp, event_id);

CREATE TABLE IF NOT EXISTS default.movie_events_kafka
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3, 'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id String,
    progress_seconds Nullable(Int32)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events-v1',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 3,
    kafka_thread_per_consumer = 1,
    kafka_flush_interval_ms = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS default.movie_events_ingestion
TO default.raw_events AS
SELECT event_id, user_id, movie_id, event_type, timestamp, device_type, session_id, progress_seconds
FROM default.movie_events_kafka;

CREATE TABLE IF NOT EXISTS default.daily_metrics
(
    metric_date Date32,
    metric LowCardinality(String),
    dimension String,
    value Nullable(Float64),
    computed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY metric_date
ORDER BY (metric_date, metric, dimension);

CREATE TABLE IF NOT EXISTS default.daily_metrics_stage AS default.daily_metrics;

CREATE USER IF NOT EXISTS grafana_reader IDENTIFIED WITH sha256_password BY 'grafana-local';

GRANT SELECT ON default.daily_metrics TO grafana_reader;

ALTER USER grafana_reader SETTINGS readonly = 1, max_execution_time CHANGEABLE_IN_READONLY;
