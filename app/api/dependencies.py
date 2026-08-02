from typing import Annotated, Protocol, cast

from fastapi import Depends, Request
from redis.asyncio import Redis

from app.infrastructure.clickhouse import ClickHouseStore


class EventPublisher(Protocol):
    async def publish(self, event_id: str, payload: bytes) -> None: ...

    async def ping(self) -> bool: ...


def get_producer(request: Request) -> EventPublisher:
    return cast(EventPublisher, request.app.state.producer)


def get_redis(request: Request) -> Redis:
    return cast(Redis, request.app.state.redis)


def get_clickhouse(request: Request) -> ClickHouseStore:
    return cast(ClickHouseStore, request.app.state.clickhouse)


ProducerDependency = Annotated[EventPublisher, Depends(get_producer)]
RedisDependency = Annotated[Redis, Depends(get_redis)]
ClickHouseDependency = Annotated[ClickHouseStore, Depends(get_clickhouse)]
