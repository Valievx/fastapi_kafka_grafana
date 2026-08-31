from collections.abc import Callable
import asyncio
import json

from aiokafka import AIOKafkaConsumer

from common.settings import settings


class KafkaConsumer:

    def __init__(self, topic: str, group_id: str, handler: Callable):
        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=settings.KAFKA_URL,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode()),
            auto_offset_reset='earliest',  # Читать с начала, если нет сохраненного оффсета
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

        await self.consumer.stop()

    async def _consume(self):
        async for message in self.consumer:
            await self.handler(message.value)
