from fastapi import  APIRouter
from fastapi.responses import JSONResponse

from schemas.event import EventSchema
from infrastructure.kafka.producer import kafka_producer
from lifespan import game_consumers

router = APIRouter(prefix="/api/v1")


@router.post(path="/send-event")
async def send_event(data: EventSchema):
    await kafka_producer.send(event=data, topic="game-events")
    return JSONResponse(
        content={"event_id": str(data.event_id)},
        status_code=200
    )


@router.get(path="/metrics")
async def get_metrics():
    return [await consumer.get_metrics() for consumer in game_consumers]
