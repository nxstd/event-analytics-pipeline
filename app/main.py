import asyncio
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.api.routes import events, health, stats
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.infrastructure.clickhouse import ClickHouseStore, create_clickhouse_client
from app.infrastructure.kafka import KafkaEventProducer
from app.infrastructure.redis import create_redis
from app.metrics import HTTP_REQUEST_DURATION

logger = logging.getLogger(__name__)


async def retry_start(
    name: str,
    operation: Callable[[], Awaitable[None]],
    settings: Settings,
) -> None:
    for attempt in range(1, settings.dependency_retry_attempts + 1):
        try:
            await operation()
            return
        except Exception:
            if attempt == settings.dependency_retry_attempts:
                logger.exception("%s unavailable after %s attempts", name, attempt)
                raise
            logger.warning(
                "%s unavailable; retry %s/%s",
                name,
                attempt,
                settings.dependency_retry_attempts,
            )
            await asyncio.sleep(settings.dependency_retry_delay_seconds)


def create_app(
    *,
    producer: object | None = None,
    redis_client: object | None = None,
    clickhouse: object | None = None,
    manage_lifespan: bool = True,
) -> FastAPI:
    settings = get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if not manage_lifespan:
            yield
            return

        async def start_dependencies() -> None:
            redis_client = create_redis(settings.redis_url)
            clickhouse_store: ClickHouseStore | None = None
            kafka_producer: KafkaEventProducer | None = None
            try:
                client = await create_clickhouse_client(
                    settings.clickhouse_host,
                    settings.clickhouse_port,
                    settings.clickhouse_database,
                    settings.clickhouse_username,
                    settings.clickhouse_password,
                )
                clickhouse_store = ClickHouseStore(client)
                if not await redis_client.ping():
                    raise ConnectionError("Redis ping failed")
                if not await clickhouse_store.ping():
                    raise ConnectionError("ClickHouse ping failed")
                kafka_producer = KafkaEventProducer(
                    settings.kafka_bootstrap_servers, settings.kafka_topic
                )
                await kafka_producer.start()
            except Exception:
                if kafka_producer is not None:
                    await kafka_producer.stop()
                if clickhouse_store is not None:
                    await clickhouse_store.close()
                await redis_client.aclose()
                raise
            app.state.redis = redis_client
            app.state.clickhouse = clickhouse_store
            app.state.producer = kafka_producer

        await retry_start("Application dependencies", start_dependencies, settings)
        try:
            yield
        finally:
            await app.state.producer.stop()
            await app.state.redis.aclose()
            await app.state.clickhouse.close()

    application = FastAPI(
        title="Event Analytics Pipeline",
        version="0.1.0",
        lifespan=lifespan,
    )
    if producer is not None:
        application.state.producer = producer
    if redis_client is not None:
        application.state.redis = redis_client
    if clickhouse is not None:
        application.state.clickhouse = clickhouse

    @application.middleware("http")
    async def measure_requests(request: Request, call_next: Callable) -> Response:
        started = time.perf_counter()
        response: Response | None = None
        try:
            response = await call_next(request)
            return response
        finally:
            route = request.scope.get("route")
            endpoint = getattr(route, "path", request.url.path)
            status_code = response.status_code if response is not None else 500
            HTTP_REQUEST_DURATION.labels(
                method=request.method,
                endpoint=endpoint,
                status=str(status_code),
            ).observe(time.perf_counter() - started)

    @application.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    application.include_router(events.router)
    application.include_router(stats.router)
    application.include_router(health.router)
    return application


configure_logging()
app = create_app()
