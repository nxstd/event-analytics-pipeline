from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends

from app.api.dependencies import ClickHouseDependency, RedisDependency
from app.core.config import Settings, get_settings
from app.infrastructure.redis import STATS_CACHE_KEY
from app.models import StatsResponse

router = APIRouter(tags=["analytics"])
SettingsDependency = Annotated[Settings, Depends(get_settings)]


@router.get("/stats", response_model=StatsResponse)
async def get_stats(
    redis: RedisDependency,
    clickhouse: ClickHouseDependency,
    settings: SettingsDependency,
) -> StatsResponse:
    cached = await redis.get(STATS_CACHE_KEY)
    if cached is not None:
        return StatsResponse.model_validate_json(cached)

    response = StatsResponse(
        generated_at=datetime.now(UTC),
        groups=await clickhouse.get_stats(),
    )
    payload = response.model_dump_json()
    await redis.set(STATS_CACHE_KEY, payload, ex=settings.stats_cache_ttl_seconds)
    return response
