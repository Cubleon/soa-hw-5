import logging
import time

from botocore.exceptions import ClientError
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from app.common import (
    BUCKET,
    SCHEMA_PATH,
    SCHEMA_SUBJECT,
    SCHEMA_VERSION,
    SQL_DIR,
    TOPIC,
    clickhouse,
    configure_logging,
    kafka_config,
    object_store,
    postgres,
    registry_request,
)

log = logging.getLogger(__name__)


def ensure_topic():
    admin = AdminClient(kafka_config())
    topic = NewTopic(TOPIC, num_partitions=3, replication_factor=2, config={"min.insync.replicas": "1"})
    try:
        admin.create_topics([topic], request_timeout=10)[TOPIC].result()
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise
    log.info("Kafka topic ready: %s", TOPIC)


def ensure_schema():
    registry_request("PUT", f"/config/{SCHEMA_SUBJECT}", json={"compatibility": "BACKWARD_TRANSITIVE"})
    schema = registry_request(
        "POST",
        f"/subjects/{SCHEMA_SUBJECT}/versions",
        json={"schemaType": "AVRO", "schema": SCHEMA_PATH.read_text()},
    )
    registered = registry_request("GET", f"/subjects/{SCHEMA_SUBJECT}/versions/{SCHEMA_VERSION}")
    if schema["id"] != registered["id"]:
        raise RuntimeError(f"Unexpected schema at {SCHEMA_SUBJECT} v{SCHEMA_VERSION}")
    log.info("Avro schema ready: %s v%s id=%s", SCHEMA_SUBJECT, SCHEMA_VERSION, schema["id"])


def ensure_bucket():
    s3 = object_store()
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
            raise
        s3.create_bucket(Bucket=BUCKET)
    log.info("S3 bucket ready: %s", BUCKET)


def setup():
    ensure_topic()
    ensure_schema()
    for statement in (SQL_DIR / "001_clickhouse.sql").read_text().split(";"):
        if statement.strip():
            clickhouse(statement)
    with postgres() as connection:
        connection.execute((SQL_DIR / "001_postgres.sql").read_text())
    ensure_bucket()


if __name__ == "__main__":
    configure_logging()
    for attempt in range(5):
        try:
            setup()
            break
        except Exception:
            log.exception("Bootstrap failed (attempt %s/5)", attempt + 1)
            if attempt == 4:
                raise
            time.sleep(2**attempt)
