from unittest.mock import AsyncMock

import pytest

from app.core.config import Settings
from app.infrastructure.redis import STATS_CACHE_KEY
from app.worker import RELEASE_LOCK_SCRIPT, ProcessingResult, process_event


@pytest.fixture
def settings() -> Settings:
    return Settings(
        idempotency_ttl_seconds=120,
        idempotency_lock_ttl_seconds=10,
    )


async def test_processes_event(event, settings: Settings) -> None:
    operations: list[str] = []
    redis = AsyncMock()

    def exists(key: str) -> int:
        operations.append(f"exists:{key}")
        return 0

    def set_value(key: str, value: str, **kwargs: object) -> bool:
        operations.append(f"set:{key}")
        return True

    def delete(key: str) -> int:
        operations.append(f"delete:{key}")
        return 1

    def release_lock(script: str, key_count: int, key: str, token: str) -> int:
        operations.append(f"release:{key}")
        assert script == RELEASE_LOCK_SCRIPT
        assert key_count == 1
        assert token == redis.set.await_args_list[0].args[1]
        return 1

    redis.exists.side_effect = exists
    redis.set.side_effect = set_value
    redis.delete.side_effect = delete
    redis.eval.side_effect = release_lock
    clickhouse = AsyncMock()
    clickhouse.insert_event.side_effect = lambda _: operations.append("clickhouse:insert")
    commit_offset = AsyncMock()
    commit_offset.side_effect = lambda: operations.append("kafka:commit")

    result = await process_event(event, redis, clickhouse, settings, commit_offset)

    assert result is ProcessingResult.PROCESSED
    clickhouse.insert_event.assert_awaited_once_with(event)
    redis.set.assert_any_await(
        f"event:processed:{event.event_id}", "1", ex=settings.idempotency_ttl_seconds
    )
    redis.delete.assert_any_await(STATS_CACHE_KEY)
    commit_offset.assert_awaited_once_with()
    redis.eval.assert_awaited_once()
    assert redis.eval.await_args.args[:3] == (
        RELEASE_LOCK_SCRIPT,
        1,
        f"event:lock:{event.event_id}",
    )
    processed_key = f"event:processed:{event.event_id}"
    lock_key = f"event:lock:{event.event_id}"
    assert operations == [
        f"exists:{processed_key}",
        f"set:{lock_key}",
        f"exists:{processed_key}",
        "clickhouse:insert",
        f"set:{processed_key}",
        f"delete:{STATS_CACHE_KEY}",
        "kafka:commit",
        f"release:{lock_key}",
    ]


async def test_skips_duplicate(event, settings: Settings) -> None:
    redis = AsyncMock()
    redis.exists.return_value = 1
    clickhouse = AsyncMock()
    commit_offset = AsyncMock()

    result = await process_event(event, redis, clickhouse, settings, commit_offset)

    assert result is ProcessingResult.DUPLICATE
    clickhouse.insert_event.assert_not_awaited()
    commit_offset.assert_awaited_once_with()


async def test_does_not_mark_processed_when_clickhouse_fails(event, settings: Settings) -> None:
    redis = AsyncMock()
    redis.exists.return_value = 0
    redis.set.return_value = True
    clickhouse = AsyncMock()
    clickhouse.insert_event.side_effect = RuntimeError("write failed")
    commit_offset = AsyncMock()

    with pytest.raises(RuntimeError, match="write failed"):
        await process_event(event, redis, clickhouse, settings, commit_offset)

    processed_key = f"event:processed:{event.event_id}"
    assert all(call.args[0] != processed_key for call in redis.set.await_args_list)
    commit_offset.assert_not_awaited()
    redis.eval.assert_awaited_once()


async def test_does_not_commit_when_lock_is_busy(event, settings: Settings) -> None:
    redis = AsyncMock()
    redis.exists.return_value = 0
    redis.set.return_value = False
    clickhouse = AsyncMock()
    commit_offset = AsyncMock()

    result = await process_event(event, redis, clickhouse, settings, commit_offset)

    assert result is ProcessingResult.LOCKED
    clickhouse.insert_event.assert_not_awaited()
    commit_offset.assert_not_awaited()
    redis.eval.assert_not_awaited()
