import time

from aiokafka import AIOKafkaProducer

from common.settings import settings
from schemas.event import EventSchema


class KafkaProducer:

    def __init__(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_URL)

        self.sent_messages = 0
        self.total_time = 0.0
        self.last_time = 0.0
        self.max_time = 0.0
        self.start_time = None


    async def start(self):
        await self.producer.start()
        self.start_time = time.perf_counter()

    async def stop(self):
        await self.producer.stop()

    async def send(self, event: EventSchema, topic: str):
        started = time.perf_counter()

        try:
            await self.producer.send(
                topic=topic,
                value=event.model_dump_json().encode("utf-8")
            )

            elapsed = time.perf_counter() - started

            self.sent_messages += 1
            self.total_time += elapsed
            self.last_time = elapsed
            self.max_time = max(self.max_time, elapsed)

        except Exception:
            raise

    async def get_metrics(self) -> dict:
        wall_elapsed = time.perf_counter() - self.start_time if self.start_time else 0
        rps = self.sent_messages / wall_elapsed if wall_elapsed else 0

        return {
            "sent": self.sent_messages,
            "rps": round(rps, 2),
            "avg_time_ms": round(self.total_time / self.sent_messages * 1000, 2) if self.sent_messages else 0,
            "last_time_ms": round(self.last_time * 1000, 2),
            "max_time_ms": round(self.max_time * 1000, 2),
            "elapsed_s": round(wall_elapsed, 2),
        }


kafka_producer = KafkaProducer()
