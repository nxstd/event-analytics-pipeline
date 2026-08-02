import asyncio
import logging
import signal
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, TopicPartition
from prometheus_client import start_http_server
from redis.asyncio import Redis

from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.infrastructure.clickhouse import ClickHouseStore, create_clickhouse_client
from app.infrastructure.redis import STATS_CACHE_KEY, create_redis
from app.metrics import (
    EVENT_PROCESSING_DURATION,
    EVENTS_DUPLICATES,
    EVENTS_PROCESSED,
    EVENTS_PROCESSING_ERRORS,
)
from app.models import Event

logger = logging.getLogger(__name__)

RELEASE_LOCK_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


class ProcessingResult(StrEnum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    LOCKED = "locked"


async def process_event(
    event: Event,
    redis: Redis,
    clickhouse: ClickHouseStore,
    settings: Settings,
    commit_offset: Callable[[], Awaitable[None]],
) -> ProcessingResult:
    processed_key = f"event:processed:{event.event_id}"
    lock_key = f"event:lock:{event.event_id}"

    if await redis.exists(processed_key):
        logger.info("Duplicate event event_id=%s", event.event_id)
        EVENTS_DUPLICATES.inc()
        await commit_offset()
        return ProcessingResult.DUPLICATE

    lock_token = uuid4().hex
    lock_acquired = await redis.set(
        lock_key, lock_token, ex=settings.idempotency_lock_ttl_seconds, nx=True
    )
    if not lock_acquired:
        return ProcessingResult.LOCKED

    try:
        if await redis.exists(processed_key):
            logger.info("Duplicate event after lock event_id=%s", event.event_id)
            EVENTS_DUPLICATES.inc()
            await commit_offset()
            return ProcessingResult.DUPLICATE

        await clickhouse.insert_event(event)
        await redis.set(processed_key, "1", ex=settings.idempotency_ttl_seconds)
        await redis.delete(STATS_CACHE_KEY)
        await commit_offset()
        EVENTS_PROCESSED.labels(type=event.type, status=event.status.value).inc()
        logger.info("Event written event_id=%s", event.event_id)
        return ProcessingResult.PROCESSED
    finally:
        await redis.eval(RELEASE_LOCK_SCRIPT, 1, lock_key, lock_token)


@dataclass
class WorkerResources:
    consumer: AIOKafkaConsumer
    redis: Redis
    clickhouse: ClickHouseStore


async def create_resources(settings: Settings) -> WorkerResources:
    redis = create_redis(settings.redis_url)
    clickhouse: ClickHouseStore | None = None
    consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    consumer_started = False
    try:
        clickhouse_client = await create_clickhouse_client(
            settings.clickhouse_host,
            settings.clickhouse_port,
            settings.clickhouse_database,
            settings.clickhouse_username,
            settings.clickhouse_password,
        )
        clickhouse = ClickHouseStore(clickhouse_client)
        await consumer.start()
        consumer_started = True
        if not await redis.ping():
            raise ConnectionError("Redis ping failed")
        if not await clickhouse.ping():
            raise ConnectionError("ClickHouse ping failed")
        return WorkerResources(consumer, redis, clickhouse)
    except Exception:
        if consumer_started:
            await consumer.stop()
        await redis.aclose()
        if clickhouse is not None:
            await clickhouse.close()
        raise


async def start_resources_with_retry(settings: Settings) -> WorkerResources:
    last_error: Exception | None = None
    for attempt in range(1, settings.dependency_retry_attempts + 1):
        try:
            resources = await create_resources(settings)
            logger.info("Worker consumer started")
            return resources
        except Exception as exc:
            last_error = exc
            logger.warning(
                "Worker dependencies unavailable; retry %s/%s",
                attempt,
                settings.dependency_retry_attempts,
            )
            if attempt < settings.dependency_retry_attempts:
                await asyncio.sleep(settings.dependency_retry_delay_seconds)
    assert last_error is not None
    raise last_error


async def run_worker() -> None:
    settings = get_settings()
    start_http_server(settings.worker_metrics_port)
    resources = await start_resources_with_retry(settings)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop_event.set)
        except NotImplementedError:
            pass

    try:
        while not stop_event.is_set():
            batch = await resources.consumer.getmany(timeout_ms=1000, max_records=10)
            for messages in batch.values():
                for message in messages:
                    started = time.perf_counter()
                    try:
                        event = Event.from_kafka(message.value)
                        logger.info("Event received event_id=%s", event.event_id)

                        async def commit_offset(
                            topic: str = message.topic,
                            partition: int = message.partition,
                            offset: int = message.offset,
                        ) -> None:
                            topic_partition = TopicPartition(topic, partition)
                            await resources.consumer.commit(
                                {topic_partition: offset + 1}
                            )

                        result = await process_event(
                            event,
                            resources.redis,
                            resources.clickhouse,
                            settings,
                            commit_offset,
                        )
                        if result is ProcessingResult.LOCKED:
                            raise RuntimeError(f"Event lock is busy: {event.event_id}")
                    except Exception:
                        EVENTS_PROCESSING_ERRORS.inc()
                        logger.exception("Event processing failed")
                        resources.consumer.seek(
                            TopicPartition(message.topic, message.partition), message.offset
                        )
                        await asyncio.sleep(settings.dependency_retry_delay_seconds)
                        break
                    finally:
                        EVENT_PROCESSING_DURATION.observe(time.perf_counter() - started)
    finally:
        await resources.consumer.stop()
        await resources.redis.aclose()
        await resources.clickhouse.close()
        logger.info("Worker stopped")


def main() -> None:
    configure_logging()
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
