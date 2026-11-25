import json
import os
import time
import unittest
from datetime import UTC, date, datetime, timedelta, timezone
from uuid import uuid4

import requests
from confluent_kafka.admin import AdminClient, ConfigResource

from app.common import BUCKET, SCHEMA_SUBJECT, SCHEMA_VERSION, TOPIC, clickhouse, kafka_config
from app.common import object_store, postgres, registry_request
from app.generator import historical_sessions


class PipelineTest(unittest.TestCase):
    def test_schema_and_replicated_topic(self):
        schema = registry_request("GET", f"/subjects/{SCHEMA_SUBJECT}/versions/{SCHEMA_VERSION}")
        self.assertEqual(schema["version"], SCHEMA_VERSION)
        self.assertEqual(schema["subject"], SCHEMA_SUBJECT)

        admin = AdminClient(kafka_config())
        partitions = admin.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions.values()
        self.assertGreaterEqual(len(partitions), 3)
        self.assertTrue(all(len(partition.replicas) >= 2 for partition in partitions))

        resource = ConfigResource("TOPIC", TOPIC)
        config = admin.describe_configs([resource], request_timeout=10)[resource].result()
        self.assertGreaterEqual(int(config["min.insync.replicas"].value), 1)

    def test_generator_sessions_are_ordered_and_reproducible(self):
        sessions = [list(session) for session in historical_sessions(date(2026, 5, 12), 8)]
        repeat = [list(session) for session in historical_sessions(date(2026, 5, 12), 8)]
        self.assertEqual(sessions, repeat)
        self.assertTrue(sessions)
        for session in sessions:
            self.assertEqual({event.session_id for event in session}, {session[0].session_id})
            self.assertEqual({event.device_type for event in session}, {session[0].device_type})
            timestamps = [event.timestamp for event in session]
            self.assertEqual(timestamps, sorted(timestamps))
            views = [event for event in session if event.event_type.startswith("VIEW_")]
            self.assertEqual(views[0].event_type, "VIEW_STARTED")
            progress = [event.progress_seconds for event in views]
            self.assertEqual(progress, sorted(set(progress)))

    def test_http_kafka_clickhouse_postgres_s3_and_grafana(self):
        producer = os.environ["PRODUCER_URL"]
        aggregation = os.environ["AGGREGATION_URL"]
        namespace = f"e2e-{uuid4().hex}"
        # Old randomized dates keep repeated runs away from the dashboard demo and other test batches.
        with postgres() as connection:
            occupied = {row[0] for row in connection.execute(
                "SELECT metric_date FROM daily_runs WHERE metric_date < '1999-01-01'"
            )}
        offset = int(namespace[-8:], 16) % 10_000
        day = date(1971, 1, 1) + timedelta(days=offset)
        while any(day + timedelta(days=age) in occupied for age in range(-3, 8)):
            day += timedelta(days=11)

        def event(user, kind, movie, day_offset=0, progress=None, minute=0):
            when = datetime.combine(
                day + timedelta(days=day_offset),
                datetime.min.time(),
                timezone(timedelta(hours=3)),
            ) + timedelta(hours=15, minutes=minute, milliseconds=123)
            return {
                "event_id": str(uuid4()),
                "user_id": f"{namespace}-{user}",
                "movie_id": f"{namespace}-{movie}",
                "event_type": kind,
                "timestamp": when.isoformat(),
                "device_type": "TV",
                "session_id": f"{namespace}-{user}-{day_offset}",
                "progress_seconds": progress,
            }

        events = [
            event("e", "VIEW_STARTED", "x", -3, 0),
            event("a", "VIEW_STARTED", "x", progress=0),
            event("a", "VIEW_FINISHED", "x", progress=1800, minute=30),
            event("b", "VIEW_STARTED", "x", progress=0),
            event("c", "VIEW_STARTED", "y", progress=0),
            event("c", "VIEW_FINISHED", "y", progress=3600, minute=60),
            event("d", "SEARCHED", "y"),
            event("e", "LIKED", "x"),
            event("a", "SEARCHED", "x", 1),
            event("c", "VIEW_STARTED", "y", 1, 0),
            event("b", "VIEW_STARTED", "x", 7, 0),
        ]
        invalid = {**events[1], "progress_seconds": None}
        self.assertEqual(requests.post(f"{producer}/events", json=invalid, timeout=10).status_code, 422)

        for item in [*events, events[1]]:
            response = requests.post(f"{producer}/events", json=item, timeout=18)
            self.assertEqual(response.status_code, 201, response.text)
            self.assertEqual(response.json(), {"event_id": item["event_id"]})

        deadline = time.monotonic() + 45
        while True:
            output = clickhouse(
                """SELECT event_id, user_id, movie_id, event_type, timestamp,
                          device_type, session_id, progress_seconds
                   FROM default.raw_events FINAL
                   WHERE startsWith(user_id, {prefix:String}) FORMAT JSONEachRow""",
                prefix=namespace,
            )
            ingested = [json.loads(line) for line in output.text.splitlines()]
            if len(ingested) >= len(events) or time.monotonic() >= deadline:
                break
            time.sleep(0.5)
        self.assertEqual(len(ingested), len(events), ingested)
        expected_by_id = {item["event_id"]: item for item in events}
        for row in ingested:
            expected = expected_by_id[row["event_id"]].copy()
            expected["timestamp"] = datetime.fromisoformat(expected["timestamp"]).astimezone(UTC)
            row["timestamp"] = datetime.fromisoformat(row["timestamp"]).replace(tzinfo=UTC)
            self.assertEqual(row, expected)

        for _ in range(2):
            response = requests.post(f"{aggregation}/recompute/{day}", timeout=60)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["events_processed"], 7)

        expected = {
            ("dau", ""): 5,
            ("avg_watch_seconds", ""): 2700,
            ("conversion", ""): 2 / 3,
            ("cohort_size", ""): 3,
            ("retention_d1", ""): 2 / 3,
            ("retention_d7", ""): 1 / 3,
            ("movie_views", f"{namespace}-x"): 2,
            ("movie_views", f"{namespace}-y"): 1,
            **{("cohort_retention", str(age)): value for age, value in enumerate(
                [1, 2 / 3, 0, 0, 0, 0, 0, 1 / 3]
            )},
        }
        with postgres() as connection:
            rows = connection.execute(
                """SELECT metric, dimension, value, computed_at FROM daily_metrics
                   WHERE metric_date = %s ORDER BY metric, dimension""",
                (day,),
            ).fetchall()

        pg_values = {(name, dimension): value for name, dimension, value, _ in rows}
        self.assertEqual(len(rows), len(expected))
        self.assertEqual(pg_values, expected)

        output = clickhouse(
            """SELECT metric, dimension, value FROM default.daily_metrics
               WHERE metric_date = {day:Date32} FORMAT JSONEachRow""",
            day=day.isoformat(),
        )
        snapshots = [json.loads(line) for line in output.text.splitlines()]
        self.assertEqual(len(snapshots), len(expected))
        self.assertEqual({(row["metric"], row["dimension"]): row["value"] for row in snapshots}, expected)

        key = f"daily/{day}/aggregates.json"
        for _ in range(2):
            result = requests.post(f"{aggregation}/export/{day}", timeout=30)
            self.assertEqual(result.status_code, 200, result.text)
            self.assertEqual(result.json()["object"], f"s3://{BUCKET}/{key}")

        s3 = object_store()
        objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"daily/{day}/")
        self.assertEqual([item["Key"] for item in objects["Contents"]], [key])
        with s3.get_object(Bucket=BUCKET, Key=key)["Body"] as body:
            records = json.loads(body.read())
        self.assertEqual(records, [
            {"date": day.isoformat(), "metric": name, "dimension": dimension, "value": value,
             "computed_at": computed_at.isoformat()}
            for name, dimension, value, computed_at in rows
        ])

        grafana = os.environ["GRAFANA_URL"]
        auth = ("admin", "cinema-local")
        response = requests.get(f"{grafana}/api/dashboards/uid/movie-analytics", auth=auth, timeout=10)
        self.assertEqual(response.status_code, 200, response.text)
        panels = response.json()["dashboard"]["panels"]
        self.assertGreaterEqual(len(panels), 3)
        start = datetime.combine(day, datetime.min.time(), UTC)
        for panel in panels:
            query = {
                "queries": panel["targets"],
                "from": str(int(start.timestamp() * 1000)),
                "to": str(int((start + timedelta(hours=23)).timestamp() * 1000)),
            }
            response = requests.post(f"{grafana}/api/ds/query", json=query, auth=auth, timeout=30)
            self.assertEqual(response.status_code, 200, response.text)
            result = response.json()["results"]["A"]
            self.assertNotIn("error", result)
            frames = result["frames"]
            self.assertTrue(frames, panel["title"])
            if panel["title"] == "Retention Cohort Heatmap":
                frame, = frames
                self.assertEqual(frame["data"]["values"], [
                    [day.isoformat()], [1], [2 / 3], [0], [0], [0], [0], [0], [1 / 3],
                ])


if __name__ == "__main__":
    unittest.main()
