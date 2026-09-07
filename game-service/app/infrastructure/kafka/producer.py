import time

from aiokafka import AIOKafkaProducer

from common.settings import settings
from schemas.event import EventSchema


class KafkaProducer:

    def __init__(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_URL)

        # metrics
        self.sent_messages = 0
        self.start_time = None


    async def start(self):
        await self.producer.start()
        self.start_time = time.perf_counter()

    async def stop(self):
        await self.producer.stop()

    async def send(self, event: EventSchema, topic: str):
        await self.producer.send(
            topic=topic,
            value=event.model_dump_json().encode("utf-8")
        )
        self.sent_messages += 1


    async def get_metrics(self) -> dict:
        elapsed = time.perf_counter() - self.start_time if self.start_time else 0
        rps = self.sent_messages / elapsed if elapsed else 0

        return {
            "sent": self.sent_messages,
            "rps": round(rps, 2),
            "elapsed_s": round(elapsed, 2),
        }


kafka_producer = KafkaProducer()
