from uuid import UUID


class TaskRepository:

    def __init__(self, redis):
        self.redis = redis

    @staticmethod
    def _key(request_id: UUID) -> str:
        return f"task:{request_id}"

    async def create(self, request_id: UUID):
        await self.redis.hset(
            self._key(request_id),
            mapping={
                "status": "pending",
            },
        )

    async def set_processing(self, request_id: UUID):
        await self.redis.hset(
            self._key(request_id),
            mapping={
                "status": "processing",
            },
        )

    async def set_completed(self, request_id: UUID, result: int):
        await self.redis.hset(
            self._key(request_id),
            mapping={
                "status": "completed",
                "result": str(result),
            },
        )

    async def set_failed(self, request_id: UUID, error: str):
        await self.redis.hset(
            self._key(request_id),
            mapping={
                "status": "failed",
                "error": error,
            },
        )

    async def get(self, request_id: UUID):
        return await self.redis.hgetall(self._key(request_id))
