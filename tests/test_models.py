from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.models import Event


def test_valid_event_preserves_decimal_as_string(event_payload: dict) -> None:
    event = Event.model_validate(event_payload)

    assert event.amount == Decimal("1490.5")
    assert '"amount":"1490.5"' in event.kafka_payload().decode()


def test_rejects_negative_amount(event_payload: dict) -> None:
    event_payload["amount"] = -1

    with pytest.raises(ValidationError):
        Event.model_validate(event_payload)


def test_rejects_invalid_currency(event_payload: dict) -> None:
    event_payload["currency"] = "rub"

    with pytest.raises(ValidationError):
        Event.model_validate(event_payload)


def test_rejects_timezone_naive_created_at(event_payload: dict) -> None:
    event_payload["created_at"] = "2026-08-02T16:30:00"

    with pytest.raises(ValidationError, match="timezone-aware"):
        Event.model_validate(event_payload)


def test_normalizes_created_at_to_utc(event_payload: dict) -> None:
    event_payload["created_at"] = "2026-08-02T19:30:00+03:00"

    event = Event.model_validate(event_payload)

    assert '"created_at":"2026-08-02T16:30:00Z"' in event.kafka_payload().decode()
