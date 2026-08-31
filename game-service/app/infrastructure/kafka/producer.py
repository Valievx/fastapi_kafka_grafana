from aiokafka import AIOKafkaProducer

from common.settings import settings
from schemas.event import EventSchema


class KafkaProducer:

    def __init__(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_URL)

    async def start(self):
        await self.producer.start()

    async def stop(self):
        await self.producer.stop()

    async def send(self, event: EventSchema):
        await self.producer.send_and_wait(
            topic="game-events",
            value=event.model_dump_json().encode("utf-8")
        )


kafka_producer = KafkaProducer()
