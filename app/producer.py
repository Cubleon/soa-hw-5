import io
import logging
import os
import struct
import threading
from contextlib import asynccontextmanager
from time import monotonic
from uuid import UUID

from confluent_kafka import KafkaException, Producer
from fastapi import FastAPI, HTTPException, Request
from fastavro import schemaless_writer

from app.common import SCHEMA_SUBJECT, SCHEMA_VERSION, TOPIC, configure_logging, kafka_config, registry_request
from app.events import AVRO_SCHEMA, MovieEvent
from app.generator import run_generator

log = logging.getLogger(__name__)


class Publisher:
    def __init__(self):
        schema = registry_request("GET", f"/subjects/{SCHEMA_SUBJECT}/versions/{SCHEMA_VERSION}")
        self.header = struct.pack(">BI", 0, schema["id"])
        self.producer = Producer(
            {
                **kafka_config(),
                "enable.idempotence": True,
                "acks": "all",
                "retry.backoff.ms": 100,
                "retry.backoff.max.ms": 2000,
                "message.timeout.ms": 12000,
                "linger.ms": 1,
                "partitioner": "murmur2_random",
            }
        )

    def publish(self, event):
        with io.BytesIO() as payload:
            payload.write(self.header)
            schemaless_writer(payload, AVRO_SCHEMA, event.model_dump(), strict=True)
            data = payload.getvalue()

        delivered = threading.Event()
        delivery_error = None

        def on_delivery(error, _message):
            nonlocal delivery_error
            delivery_error = error
            delivered.set()

        self.producer.produce(TOPIC, key=event.user_id.encode(), value=data, on_delivery=on_delivery)
        deadline = monotonic() + 14
        while not delivered.is_set():
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError(f"No Kafka delivery report for {event.event_id}")
            self.producer.poll(min(0.1, remaining))
        if delivery_error is not None:
            raise KafkaException(delivery_error)

        log.info("Published event_id=%s event_type=%s timestamp=%s", event.event_id, event.event_type, event.timestamp)


@asynccontextmanager
async def lifespan(api):
    configure_logging()
    api.state.publisher = Publisher()
    stop = threading.Event()
    worker = None
    if os.getenv("GENERATOR_ENABLED", "true").lower() in {"true", "1"}:
        worker = threading.Thread(target=run_generator, args=(api.state.publisher.publish, stop), daemon=True)
        worker.start()
    try:
        yield
    finally:
        stop.set()
        if worker:
            worker.join(timeout=15)
        api.state.publisher.producer.flush(2)


api = FastAPI(title="Movie Service", lifespan=lifespan)


@api.get("/health")
def health(request: Request):
    try:
        metadata = request.app.state.publisher.producer.list_topics(timeout=2)
        topic = metadata.topics.get(TOPIC)
        if topic is None or topic.error or len(topic.partitions) < 3:
            raise RuntimeError("Movie topic is unavailable")
        if any(partition.leader < 0 for partition in topic.partitions.values()):
            raise RuntimeError("Movie topic has unavailable partitions")
    except (KafkaException, RuntimeError) as exc:
        raise HTTPException(503, "Kafka unavailable") from exc
    return {"status": "ok"}


@api.post("/events", status_code=201)
def accept(event: MovieEvent, request: Request) -> dict[str, UUID]:
    try:
        request.app.state.publisher.publish(event)
    except (KafkaException, BufferError, TimeoutError) as exc:
        log.exception("Publish failed event_id=%s", event.event_id)
        raise HTTPException(503, "Kafka publication failed") from exc
    return {"event_id": event.event_id}
