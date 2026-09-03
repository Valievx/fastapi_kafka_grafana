import redis.asyncio as redis

from common.settings import settings


class RedisClient:

    def __init__(self):
        self.client = redis.from_url(settings.REDIS_URL, decode_responses=True)

    async def close(self):
        await self.client.aclose()


redis_client = RedisClient()
