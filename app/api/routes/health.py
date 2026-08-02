import asyncio

from fastapi import APIRouter, Response, status

from app.api.dependencies import ClickHouseDependency, ProducerDependency, RedisDependency

router = APIRouter(tags=["health"])
READINESS_TIMEOUT_SECONDS = 2.0


@router.get("/health/live")
async def live() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/ready")
async def ready(
    response: Response,
    producer: ProducerDependency,
    redis: RedisDependency,
    clickhouse: ClickHouseDependency,
) -> dict[str, object]:
    checks = await asyncio.gather(
        asyncio.wait_for(producer.ping(), timeout=READINESS_TIMEOUT_SECONDS),
        asyncio.wait_for(redis.ping(), timeout=READINESS_TIMEOUT_SECONDS),
        asyncio.wait_for(clickhouse.ping(), timeout=READINESS_TIMEOUT_SECONDS),
        return_exceptions=True,
    )
    names = ("kafka", "redis", "clickhouse")
    dependencies = {
        name: "ok" if result is True else "unavailable"
        for name, result in zip(names, checks, strict=True)
    }
    is_ready = all(value == "ok" for value in dependencies.values())
    if not is_ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "ready" if is_ready else "not_ready", "dependencies": dependencies}
