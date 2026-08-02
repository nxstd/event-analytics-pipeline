from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Self
from uuid import UUID

from pydantic import BaseModel, Field, StringConstraints, field_serializer, field_validator


class EventStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    PENDING = "pending"


EventType = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)]
Currency = Annotated[str, StringConstraints(pattern=r"^[A-Z]{3}$")]


class Event(BaseModel):
    event_id: UUID
    type: EventType
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    status: EventStatus
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def created_at_must_have_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        return value.astimezone(UTC)

    @field_serializer("amount")
    def serialize_amount(self, value: Decimal) -> str:
        return format(value, "f")

    @field_serializer("created_at")
    def serialize_created_at(self, value: datetime) -> str:
        return value.isoformat().replace("+00:00", "Z")

    def kafka_payload(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_kafka(cls, payload: bytes) -> Self:
        return cls.model_validate_json(payload)


class EventAccepted(BaseModel):
    event_id: UUID
    status: str = "accepted"


class StatsGroup(BaseModel):
    type: str
    status: EventStatus
    currency: str
    events_count: int
    total_amount: Decimal

    @field_serializer("total_amount")
    def serialize_total(self, value: Decimal) -> str:
        return format(value, ".2f")


class StatsResponse(BaseModel):
    generated_at: datetime
    groups: list[StatsGroup]
