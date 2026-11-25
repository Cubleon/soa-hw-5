import json
import re
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated
from uuid import UUID

from fastavro import parse_schema
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from app.common import SCHEMA_PATH

AVRO_SCHEMA = parse_schema(json.loads(SCHEMA_PATH.read_text()))
IDENTIFIER = Annotated[str, StringConstraints(strict=True, min_length=1, pattern=r"^[^\x00]+$")]
RFC3339 = re.compile(r"\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:\d{2})")


class EventType(StrEnum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(StrEnum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


class MovieEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    user_id: IDENTIFIER
    movie_id: IDENTIFIER
    event_type: EventType
    timestamp: AwareDatetime
    device_type: DeviceType
    session_id: IDENTIFIER
    progress_seconds: Annotated[int, Field(strict=True, ge=0, le=2**31 - 1)] | None = None

    @field_validator("timestamp", mode="before")
    @classmethod
    def require_rfc3339(cls, value):
        if isinstance(value, datetime) or (isinstance(value, str) and RFC3339.fullmatch(value)):
            return value
        raise ValueError("timestamp must be RFC 3339 with an explicit offset")

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value):
        if not 1899 <= value.year <= 2300:
            raise ValueError("timestamp outside the ClickHouse DateTime64 range")
        utc = value.astimezone(UTC)
        if not 1900 <= utc.year <= 2299:
            raise ValueError("timestamp outside the ClickHouse DateTime64 range")
        return utc.replace(microsecond=utc.microsecond // 1000 * 1000)

    @model_validator(mode="after")
    def validate_progress(self):
        if self.event_type.startswith("VIEW_") and self.progress_seconds is None:
            raise ValueError("progress_seconds is required for view events")
        return self
