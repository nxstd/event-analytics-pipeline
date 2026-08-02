from redis.asyncio import Redis

STATS_CACHE_KEY = "stats:all"


def create_redis(url: str) -> Redis:
    return Redis.from_url(url, decode_responses=True)
