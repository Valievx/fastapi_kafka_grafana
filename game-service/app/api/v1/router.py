from fastapi import  APIRouter
from fastapi.responses import JSONResponse

from schemas.event import EventSchema
from infrastructure.kafka.producer import kafka_producer
from lifespan import game_consumers

router = APIRouter(prefix="/api/v1")


@router.post(path="/send-event")
async def send_event():
    # 2h 38min 5.04s
    # 1103301

    # 14.24s
    for _ in range(1000000):
        event = EventSchema(event_type="game-event", text="test event")
        await kafka_producer.send(event=event, topic="game-events")

    return JSONResponse(content={"success": True}, status_code=200)


@router.get(path="/metrics")
async def get_metrics():
    return [await consumer.get_metrics() for consumer in game_consumers]


