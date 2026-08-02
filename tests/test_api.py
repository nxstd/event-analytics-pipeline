import json
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import create_app
from app.metrics import EVENTS_PUBLISH_ERRORS
from app.models import EventStatus, StatsGroup


@pytest.fixture
def producer() -> AsyncMock:
    producer = AsyncMock()
    producer.ping.return_value = True
    return producer


@pytest.fixture
def redis() -> AsyncMock:
    client = AsyncMock()
    client.ping.return_value = True
    return client


@pytest.fixture
def clickhouse() -> AsyncMock:
    store = AsyncMock()
    store.ping.return_value = True
    return store


@pytest.fixture
async def client(producer: AsyncMock, redis: AsyncMock, clickhouse: AsyncMock):
    app = create_app(
        producer=producer,
        redis_client=redis,
        clickhouse=clickhouse,
        manage_lifespan=False,
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as value:
        yield value


async def test_post_events_returns_accepted(
    client: AsyncClient, producer: AsyncMock, event_payload: dict
) -> None:
    response = await client.post("/events", json=event_payload)

    assert response.status_code == 202
    assert response.json() == {"event_id": event_payload["event_id"], "status": "accepted"}
    producer.publish.assert_awaited_once()
    assert producer.publish.await_args.args[0] == event_payload["event_id"]
    sent_payload = producer.publish.await_args.args[1].decode()
    assert '"amount":"1490.5"' in sent_payload


async def test_post_events_reports_publish_error(
    client: AsyncClient, producer: AsyncMock, event_payload: dict
) -> None:
    producer.publish.side_effect = RuntimeError("Kafka unavailable")
    errors_before = EVENTS_PUBLISH_ERRORS._value.get()

    response = await client.post("/events", json=event_payload)

    assert response.status_code == 503
    assert response.json() == {"detail": "Event broker is unavailable"}
    assert EVENTS_PUBLISH_ERRORS._value.get() == errors_before + 1


async def test_live_health(client: AsyncClient) -> None:
    response = await client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_live_health_does_not_check_dependencies(
    client: AsyncClient, producer: AsyncMock, redis: AsyncMock, clickhouse: AsyncMock
) -> None:
    await client.get("/health/live")

    producer.ping.assert_not_awaited()
    redis.ping.assert_not_awaited()
    clickhouse.ping.assert_not_awaited()


async def test_ready_health_reports_dependency_failure(
    client: AsyncClient, redis: AsyncMock
) -> None:
    redis.ping.return_value = False

    response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["dependencies"]["redis"] == "unavailable"


async def test_stats_cache_hit(
    client: AsyncClient, redis: AsyncMock, clickhouse: AsyncMock
) -> None:
    redis.get.return_value = (
        '{"generated_at":"2026-08-02T16:30:00Z","groups":'
        '[{"type":"transaction.completed","status":"success","currency":"RUB",'
        '"events_count":3,"total_amount":"4471.50"}]}'
    )

    response = await client.get("/stats")

    assert response.status_code == 200
    assert response.json()["generated_at"] == "2026-08-02T16:30:00Z"
    assert response.json()["groups"][0]["total_amount"] == "4471.50"
    clickhouse.get_stats.assert_not_awaited()


async def test_stats_cache_miss(
    client: AsyncClient, redis: AsyncMock, clickhouse: AsyncMock
) -> None:
    redis.get.return_value = None
    clickhouse.get_stats.return_value = [
        StatsGroup(
            type="transaction.completed",
            status=EventStatus.SUCCESS,
            currency="RUB",
            events_count=1,
            total_amount=Decimal("1490.50"),
        )
    ]

    response = await client.get("/stats")

    assert response.status_code == 200
    assert response.json()["groups"][0]["total_amount"] == "1490.50"
    redis.set.assert_awaited_once()
    assert redis.set.await_args.kwargs == {"ex": 30}
    assert json.loads(redis.set.await_args.args[1]) == response.json()
