import time
from collections.abc import Callable
import asyncio
import logging
import json

from aiokafka import AIOKafkaConsumer

from common.settings import settings
from common.metrics import kafka_messages_processed_total

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

class KafkaConsumer:

    def __init__(self, topic: str, consumer_id: int, group_id: str, handler: Callable):
        self.consumer_id = consumer_id
        self.topic = topic

        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=settings.KAFKA_URL,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode()),
            auto_offset_reset='earliest',  # Читать с начала, если нет сохраненного оффсета
            enable_auto_commit=False,
        )
        self.handler = handler
        self.task = None

        self.batch_size = 1000

        # metrics
        self.processed_messages = 0
        self.start_time = None

    async def start(self):
        await self.consumer.start()
        self.start_time = time.perf_counter()
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
        while True:
            result = await self.consumer.getmany(timeout_ms=100, max_records=self.batch_size)

            for topic_partition, messages in result.items():
                if not messages:
                    continue

                batch = [message.value for message in messages]
                await self._process(batch)

    async def _process(self, batch: list[dict]):
        try:
            await self.handler(batch)
            await self.consumer.commit()

            self.processed_messages += len(batch)
            kafka_messages_processed_total.labels(
                topic=self.topic, consumer_id=str(self.consumer_id)
            ).inc(len(batch))
        except Exception as e:
            logger.error(f"Consumer {self.consumer_id} failed: {type(e).__name__}: {e}")

    async def get_metrics(self) -> dict:
        elapsed = time.perf_counter() - self.start_time if self.start_time else 0
        rps = self.processed_messages / elapsed if elapsed else 0

        return {
            "consumer_id": self.consumer_id,
            "processed": self.processed_messages,
            "rps": round(rps, 2),
            "elapsed_s": round(elapsed, 2),
        }
