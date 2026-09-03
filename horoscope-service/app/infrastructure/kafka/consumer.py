import asyncio
import json
import logging
from collections.abc import Callable

from aiokafka import AIOKafkaConsumer

from common.settings import settings


logger = logging.getLogger(__name__)


class KafkaConsumer:

    def __init__(self, group_id: str, handler: Callable):

        self.consumer = AIOKafkaConsumer(
            "horoscope-event",
            bootstrap_servers=settings.KAFKA_URL,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )

        self.handler = handler
        self.task = None

    async def start(self):
        await self.consumer.start()
        self.task = asyncio.create_task(self._consume())

    async def stop(self):
        if self.task:
            self.task.cancel()

            try:
                await self.task
            except asyncio.CancelledError:
                pass

            self.task = None

        await self.consumer.stop()

    async def _consume(self):
        async for message in self.consumer:
            await self.handler(message.value)
