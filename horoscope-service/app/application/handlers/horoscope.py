import asyncio
import random
import logging
from uuid import UUID


logger = logging.getLogger(__name__)


class HoroscopeHandler:

    def __init__(self, repository):
        self.repository = repository

    async def handle(self, event: dict):
        request_id = UUID(event["request_id"])

        await self.repository.set_processing(request_id)

        try:
            await asyncio.sleep(15)

            result = random.randint(1, 100)

            await self.repository.set_completed(request_id=request_id, result=result)

        except Exception as e:
            await self.repository.set_failed(request_id=request_id, error=str(e))
            raise
