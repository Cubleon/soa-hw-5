import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import boto3
import psycopg
import requests
from botocore.config import Config

SQL_DIR = Path(__file__).parent / "sql"
TOPIC = "movie-events"
SCHEMA_SUBJECT = f"{TOPIC}-value"
SCHEMA_VERSION = 1
SCHEMA_PATH = Path(__file__).parents[1] / "schemas" / "movie-event-v1.avsc"
BUCKET = "movie-analytics"


def utc_now():
    return datetime.now(UTC)


def configure_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def kafka_config():
    return {"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]}


def registry_request(method, path, **kwargs):
    media_type = "application/vnd.schemaregistry.v1+json"
    response = requests.request(
        method, os.environ["SCHEMA_REGISTRY_URL"] + path, timeout=(3, 10),
        headers={"Accept": media_type, "Content-Type": media_type}, **kwargs,
    )
    response.raise_for_status()
    return response.json()


def clickhouse(sql, data="", **parameters):
    query_parameters = {f"param_{key}": value for key, value in parameters.items()}
    response = requests.post(
        os.environ["CLICKHOUSE_URL"],
        params={"query": sql, "wait_end_of_query": 1, **query_parameters},
        data=data.encode("utf-8"),
        auth=(os.environ["CLICKHOUSE_USER"], os.environ["CLICKHOUSE_PASSWORD"]),
        timeout=(3, 60),
    )
    if not response.ok:
        raise RuntimeError(f"ClickHouse HTTP {response.status_code}: {response.text.strip()}")
    return response


def postgres():
    return psycopg.connect(os.environ["POSTGRES_DSN"], connect_timeout=4)


def object_store():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=3,
            read_timeout=8,
            retries={"total_max_attempts": 1},
        ),
    )
