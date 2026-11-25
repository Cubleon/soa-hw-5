import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import UTC, date, timedelta

import psycopg
from fastapi import FastAPI, HTTPException

from app.common import BUCKET, SQL_DIR, clickhouse, configure_logging, object_store, postgres, utc_now

log = logging.getLogger(__name__)
DAILY_SQL = (SQL_DIR / "daily.sql").read_text()
TOP_SQL = (SQL_DIR / "top_movies.sql").read_text()
RETENTION_SQL = (SQL_DIR / "retention.sql").read_text()


def query_rows(sql, **parameters):
    return [json.loads(line) for line in clickhouse(sql, **parameters).text.splitlines()]


def calculate(day):
    parameters = {"day": day.isoformat()}
    daily, = query_rows(DAILY_SQL, **parameters)
    top_movies = query_rows(TOP_SQL, **parameters)
    retention = query_rows(RETENTION_SQL, today=utc_now().date().isoformat(), **parameters)

    metrics = {
        ("dau", ""): int(daily["dau"]),
        ("avg_watch_seconds", ""): daily["avg_watch_seconds"],
        ("conversion", ""): daily["conversion"],
        ("cohort_size", ""): int(retention[0]["cohort_size"]),
        ("retention_d1", ""): retention[1]["rate"],
        ("retention_d7", ""): retention[7]["rate"],
    }
    metrics.update({("movie_views", row["movie_id"]): int(row["views"]) for row in top_movies})
    metrics.update({("cohort_retention", str(row["age"])): row["rate"] for row in retention})
    return int(daily["events_processed"]), metrics


def lock_day(connection, day):
    connection.execute("SELECT pg_advisory_xact_lock(%s, %s)", (53005, day.toordinal()))


def write_clickhouse(day, metrics, version):
    parameters = {"day": day.isoformat()}
    rows = (
        {
            "metric_date": parameters["day"],
            "metric": name,
            "dimension": dimension,
            "value": value,
            "computed_at": version,
        }
        for (name, dimension), value in metrics.items()
    )
    payload = "".join(json.dumps(row, allow_nan=False) + "\n" for row in rows)
    clickhouse("ALTER TABLE default.daily_metrics_stage DROP PARTITION {day:Date32}", **parameters)
    clickhouse("INSERT INTO default.daily_metrics_stage FORMAT JSONEachRow", data=payload)
    clickhouse(
        "ALTER TABLE default.daily_metrics REPLACE PARTITION {day:Date32} FROM default.daily_metrics_stage",
        **parameters,
    )


def save_day(day, force):
    with postgres() as connection:
        lock_day(connection, day)
        events, metrics = calculate(day)
        previous = connection.execute(
            "SELECT computed_at, events_processed FROM daily_runs WHERE metric_date = %s", (day,)
        ).fetchone()
        if not force and previous and events == previous[1]:
            existing = connection.execute(
                "SELECT metric, dimension, value FROM daily_metrics WHERE metric_date = %s", (day,)
            ).fetchall()
            if metrics == {(name, dimension): value for name, dimension, value in existing}:
                version = previous[0].astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                snapshot, = query_rows(
                    """SELECT maxOrNull(computed_at) AS version FROM default.daily_metrics
                       WHERE metric_date = {day:Date32} FORMAT JSONEachRow""",
                    day=day.isoformat(),
                )
                if snapshot["version"] != version:
                    write_clickhouse(day, metrics, version)
                return events, len(metrics), False

        now = utc_now()
        version = now.replace(microsecond=now.microsecond // 1000 * 1000)
        connection.execute(
            """INSERT INTO daily_runs (metric_date, computed_at, events_processed)
               VALUES (%s, %s, %s)
               ON CONFLICT (metric_date) DO UPDATE
               SET computed_at = EXCLUDED.computed_at, events_processed = EXCLUDED.events_processed,
                   exported_version = NULL""",
            (day, version, events),
        )
        connection.execute("DELETE FROM daily_metrics WHERE metric_date = %s", (day,))
        with connection.cursor() as cursor:
            cursor.executemany(
                """INSERT INTO daily_metrics (metric_date, metric, dimension, value, computed_at)
                   VALUES (%s, %s, %s, %s, %s)""",
                [(day, name, dimension, value, version) for (name, dimension), value in metrics.items()],
            )
        write_clickhouse(day, metrics, version.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
        return events, len(metrics), True


def recompute(day, *, force=False):
    started = time.monotonic()
    log.info("Recompute started date=%s", day)
    for attempt in range(3):
        try:
            events, metrics, changed = save_day(day, force)
            break
        except psycopg.OperationalError:
            log.exception("PostgreSQL error date=%s attempt=%s/3", day, attempt + 1)
            if attempt == 2:
                raise
            time.sleep(0.5 * 2**attempt)
    log.info(
        "Recompute finished date=%s events=%s metrics=%s updated=%s elapsed=%.3fs",
        day, events, metrics, changed, time.monotonic() - started,
    )
    return {"date": day, "events_processed": events, "metrics": metrics, "updated": changed}


def export_day(day, *, force=False):
    try:
        with postgres() as connection:
            lock_day(connection, day)
            run = connection.execute(
                "SELECT computed_at, exported_version FROM daily_runs WHERE metric_date = %s", (day,)
            ).fetchone()
            if run is None or (not force and run[0] == run[1]):
                return None
            rows = connection.execute(
                """SELECT metric, dimension, value, computed_at FROM daily_metrics
                   WHERE metric_date = %s ORDER BY metric, dimension""",
                (day,),
            ).fetchall()
            records = [
                {"date": day.isoformat(), "metric": name, "dimension": dimension,
                 "value": value, "computed_at": computed_at.isoformat()}
                for name, dimension, value, computed_at in rows
            ]
            key = f"daily/{day}/aggregates.json"
            object_store().put_object(
                Bucket=BUCKET, Key=key, ContentType="application/json",
                Body=json.dumps(records, ensure_ascii=False, allow_nan=False).encode("utf-8"),
            )
            connection.execute(
                "UPDATE daily_runs SET exported_version = %s WHERE metric_date = %s", (run[0], day)
            )
        log.info("Exported date=%s metrics=%s key=%s", day, len(records), key)
        return {"date": day, "metrics": len(records), "object": f"s3://{BUCKET}/{key}"}
    except Exception:
        log.exception("Export failed date=%s; pending exports are retried on the next cycle", day)
        raise


def scheduled_days(today):
    yesterday = today - timedelta(days=1)
    raw_counts = {
        date.fromisoformat(row["event_date"]): int(row["events_processed"])
        for row in query_rows(
            """SELECT event_date, count() AS events_processed FROM default.raw_events FINAL
               WHERE event_date < {day:Date32} GROUP BY event_date FORMAT JSONEachRow""",
            day=yesterday.isoformat(),
        )
    }
    window_start = today - timedelta(days=8)
    with postgres() as connection:
        completed = dict(connection.execute("SELECT metric_date, events_processed FROM daily_runs"))
    recently_active = {day for day in raw_counts if day >= window_start}
    changed = {day for day, count in raw_counts.items() if count != completed.get(day)}
    affected_cohorts = {day - timedelta(days=age) for day in changed for age in range(8)}
    return {today, yesterday} | recently_active | (affected_cohorts & raw_counts.keys())


def scheduled_exports(today):
    with postgres() as connection:
        return [
            row[0] for row in connection.execute(
                """SELECT metric_date FROM daily_runs WHERE metric_date < %s
                   AND exported_version IS DISTINCT FROM computed_at ORDER BY metric_date""",
                (today,),
            )
        ]


def run_scheduler(stop):
    interval = max(1, float(os.getenv("AGGREGATION_INTERVAL_SECONDS", "30")))
    pending_dates = set()
    while not stop.is_set():
        today = utc_now().date()
        started = time.monotonic()
        processed = dates = 0
        log.info("Aggregation cycle started utc_date=%s", today)
        try:
            pending_dates.update(scheduled_days(today))
        except Exception:
            log.exception("Could not list dates for aggregation")
        for day in sorted(pending_dates):
            if stop.is_set():
                return
            try:
                processed += recompute(day)["events_processed"]
                dates += 1
                pending_dates.remove(day)
            except Exception:
                log.exception("Scheduled recompute failed date=%s", day)
        log.info("Aggregation cycle finished dates=%s events=%s elapsed=%.3fs", dates, processed, time.monotonic() - started)

        try:
            for day in scheduled_exports(today):
                if stop.is_set():
                    return
                try:
                    export_day(day)
                except Exception:
                    continue  # export_day logs the error and leaves the version pending.
        except Exception:
            log.exception("Could not list dates for export")
        stop.wait(interval)


@asynccontextmanager
async def lifespan(_api):
    configure_logging()
    stop = threading.Event()
    worker = threading.Thread(target=run_scheduler, args=(stop,), daemon=True)
    worker.start()
    try:
        yield
    finally:
        stop.set()
        worker.join(timeout=2)


api = FastAPI(title="Aggregation Service", lifespan=lifespan)


@api.get("/health")
def health():
    try:
        clickhouse("SELECT 1 FROM default.daily_metrics LIMIT 1")
        with postgres() as connection:
            connection.execute("SELECT 1 FROM daily_runs LIMIT 1")
    except Exception as exc:
        raise HTTPException(503, "ClickHouse or PostgreSQL unavailable") from exc
    return {"status": "ok"}


def execute_for_day(action, day):
    if day.year < 1900 or day > utc_now().date():
        raise HTTPException(422, "date must be between 1900-01-01 and today in UTC")
    try:
        return action(day, force=True)
    except Exception as exc:
        log.exception("Manual operation failed date=%s", day)
        raise HTTPException(503, "Analytics storage operation failed") from exc


@api.post("/recompute/{day}")
def manual_recompute(day: date):
    return execute_for_day(recompute, day)


@api.post("/export/{day}")
def manual_export(day: date):
    result = execute_for_day(export_day, day)
    if result is None:
        raise HTTPException(404, "Metrics for this date have not been computed")
    return result
