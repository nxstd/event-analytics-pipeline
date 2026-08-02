from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from app.models import Event, EventStatus


@pytest.fixture
def event() -> Event:
    return Event(
        event_id=UUID("a49c3274-2c53-46a9-98d7-283d49feb951"),
        type="transaction.completed",
        amount=Decimal("1490.50"),
        currency="RUB",
        status=EventStatus.SUCCESS,
        created_at=datetime(2026, 8, 2, 16, 30, tzinfo=UTC),
    )


@pytest.fixture
def event_payload() -> dict[str, str | float]:
    return {
        "event_id": "a49c3274-2c53-46a9-98d7-283d49feb951",
        "type": "transaction.completed",
        "amount": 1490.50,
        "currency": "RUB",
        "status": "success",
        "created_at": "2026-08-02T16:30:00Z",
    }
