import time
from collections.abc import Callable
import asyncio
import logging
import json

from aiokafka import AIOKafkaConsumer

from common.settings import settings

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

class KafkaConsumer:

    def __init__(self, topic: str, consumer_id: int, group_id: str, handler: Callable):
        self.consumer_id = consumer_id

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

        self.processed_messages = 0
        self.total_time = 0.0
        self.last_time = 0.0
        self.max_time = 0.0

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
            started = time.perf_counter()

            try:
                await self.handler(message.value)

                elapsed = time.perf_counter() - started
                self.processed_messages += 1
                self.total_time += elapsed
                self.last_time = elapsed
                self.max_time = max(self.max_time, elapsed)

            except Exception as e:
                logger.error(f"Consumer {self.consumer_id} failed: {type(e).__name__}: {e}")

    async def get_metrics(self) -> dict:
        avg = self.total_time / self.processed_messages if self.processed_messages else 0

        return {
            "consumer_id": self.consumer_id,
            "processed": self.processed_messages,
            "avg_time_ms": round(avg * 1000, 2),
            "last_time_ms": round(self.last_time * 1000, 2),
            "max_time_ms": round(self.max_time * 1000, 2),
        }
